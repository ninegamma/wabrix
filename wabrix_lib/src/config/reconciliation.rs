use crate::config::hot_config::SharedSystemState;
use crate::toml_parser::{NodeConfig, PipelineConfig, WabricksConfig};
use hashbrown::HashMap as WasmHashMap;
use postcard::to_allocvec;
use std::collections::HashMap;
use std::sync::Arc;
use wabrix_api::{HotWasmPayload, StandardOpcode};
use wabrix_base::ControlEvent;

// StandardOpcode::UpdateConfiguration assigned to the State & Lifecycle block
const OPCODE_HOT_WASM_UPDATE: u32 = StandardOpcode::UpdateConfiguration as u32;

pub enum ReconciliationResult {
    FullRestart,
    LiveUpdated(Vec<Arc<ControlEvent>>),
    NoAction,
    Error(String),
}

pub fn reconcile_config(
    system_state: Arc<SharedSystemState>,
    current: &WabricksConfig,
    target: &WabricksConfig,
) -> ReconciliationResult {
    if current.pipelines.len() != target.pipelines.len() {
        return ReconciliationResult::FullRestart;
    }

    let mut requires_rcu_update = false;
    let mut generated_events = Vec::new();
    let empty_map = HashMap::new();

    // Transform current and target pipelines into HashMaps
    let current_pipelines: HashMap<&String, &PipelineConfig> =
        current.pipelines.iter().map(|p| (&p.name, p)).collect();

    let target_pipelines: HashMap<&String, &PipelineConfig> =
        target.pipelines.iter().map(|p| (&p.name, p)).collect();

    // Detect additions or structural deletions
    if current_pipelines.len() != target_pipelines.len() {
        return ReconciliationResult::FullRestart;
    }

    for (name, target_pipeline) in &target_pipelines {
        let current_pipeline = match current_pipelines.get(name) {
            Some(p) => p,
            None => return ReconciliationResult::FullRestart, // New pipeline added
        };

        if current_pipeline.nodes.len() != target_pipeline.nodes.len() {
            return ReconciliationResult::FullRestart;
        }

        // Transform nodes into HashMaps for deterministic ID-based lookup
        let current_nodes: HashMap<&String, &NodeConfig> =
            current_pipeline.nodes.iter().map(|n| (&n.id, n)).collect();

        let target_nodes: HashMap<&String, &NodeConfig> =
            target_pipeline.nodes.iter().map(|n| (&n.id, n)).collect();

        // Iterate over target nodes and look up matching current node by ID
        for (node_id, target_node) in &target_nodes {
            let current_node = match current_nodes.get(node_id) {
                Some(n) => n,
                None => return ReconciliationResult::FullRestart, // New node added
            };

            // Structural changes require a full restart
            if current_node.node_type != target_node.node_type {
                return ReconciliationResult::FullRestart;
            }

            // Static arguments trigger a full restart if modified
            if current_node.args != target_node.args {
                return ReconciliationResult::FullRestart;
            }

            // Detect changes in RCU-backed declarative state
            if current_node.hot_args != target_node.hot_args {
                requires_rcu_update = true;
            }

            // Detect changes in Wasm plugin runtime parameters and generate imperative events
            if current_node.hot_wasm_data != target_node.hot_wasm_data {
                let current_map = current_node.hot_wasm_data.as_ref().unwrap_or(&empty_map);
                let target_map = target_node.hot_wasm_data.as_ref().unwrap_or(&empty_map);

                let mut diff_updates = WasmHashMap::new();

                // Identify keys that were removed and mark them with an empty string tombstone
                for key in current_map.keys() {
                    if !target_map.contains_key(key) {
                        diff_updates.insert(key.clone(), String::new());
                    }
                }

                // Insert all active target keys
                for (key, value) in target_map.iter() {
                    diff_updates.insert(key.clone(), value.clone());
                }

                let payload = HotWasmPayload {
                    updates: diff_updates,
                };

                match to_allocvec(&payload) {
                    Ok(bytes) => {
                        let event = ControlEvent {
                            address_pattern: format!(
                                "*/{}/{}/{}",
                                target_pipeline.name, target_node.id, target_node.node_type
                            ),
                            opcode: OPCODE_HOT_WASM_UPDATE,
                            payload: bytes,
                        };
                        generated_events.push(Arc::new(event));
                    }
                    Err(e) => {
                        return ReconciliationResult::Error(format!(
                            "Failed to serialize hot_wasm_data for node {}: {}",
                            target_node.id, e
                        ));
                    }
                }
            }
        }

        // Pipeline lifecycle transitions trigger RCU updates without structural rebuilds
        if current_pipeline.lifecycle_state != target_pipeline.lifecycle_state {
            requires_rcu_update = true;
        }
    }

    if !requires_rcu_update && generated_events.is_empty() {
        return ReconciliationResult::NoAction;
    }

    // Apply RCU mutations if required
    if requires_rcu_update {
        for target_pipeline in &target.pipelines {
            let rcu = match system_state.pipelines.get(&target_pipeline.name) {
                Some(r) => r,
                None => {
                    return ReconciliationResult::Error(format!(
                        "Pipeline not found in system state: {}",
                        target_pipeline.name
                    ))
                }
            };

            let current_ptr = rcu.load();
            let mut offline_config = (**current_ptr).clone();

            offline_config.lifecycle_state = target_pipeline.lifecycle_state.clone();

            for target_node in &target_pipeline.nodes {
                if let Some(idx) = offline_config.name_to_index.get(&target_node.id) {
                    let dynamic_state = &mut offline_config.fast_nodes[*idx].dynamic_state;
                    dynamic_state.update_from_strings(&target_node.hot_args);
                } else {
                    return ReconciliationResult::Error(format!(
                        "Node index not found for: {}",
                        target_node.id
                    ));
                }
            }

            rcu.store(Arc::new(offline_config));
        }
    }

    ReconciliationResult::LiveUpdated(generated_events)
}
