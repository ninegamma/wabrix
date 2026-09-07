use crate::config::hot_config::SharedSystemState;
use crate::toml_parser::{NodeConfig, PipelineConfig, PipelineLifecycle, WabricksConfig};
use hashbrown::HashMap as WasmHashMap;
use postcard::to_allocvec;
use std::collections::HashMap;
use std::sync::Arc;
use wabrix_api::{HotWasmPayload, StandardOpcode};
use wabrix_base::ControlEvent;

// StandardOpcode::UpdateConfiguration assigned to the State & Lifecycle block
const OPCODE_HOT_WASM_UPDATE: u32 = StandardOpcode::UpdateConfiguration as u32;

/// Node types whose runtime contract accepts the standard configuration update
/// event. Startup `node_data` remains restart-only for every node type.
fn supports_hot_node_data(node_type: &str) -> bool {
    matches!(node_type, "TcpNode" | "WasmEngine")
}

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

            // Startup data is consumed during construction and cannot be
            // replaced safely on a running node.
            if current_node.node_data != target_node.node_data {
                return ReconciliationResult::FullRestart;
            }

            if current_node.desired_state != target_node.desired_state {
                let Some(desired_state) = target_node.desired_state.as_ref() else {
                    return ReconciliationResult::FullRestart;
                };
                let state = match desired_state {
                    PipelineLifecycle::Running | PipelineLifecycle::RunningDelayed(_) => 1,
                    PipelineLifecycle::Paused | PipelineLifecycle::Stopping => 0,
                };
                generated_events.push(Arc::new(ControlEvent {
                    address_pattern: format!(
                        "*/{}/{}/{}",
                        target_pipeline.name, target_node.id, target_node.node_type
                    ),
                    opcode: StandardOpcode::SetOperationalState as u32,
                    payload: vec![state],
                }));
            }

            // Detect changes in RCU-backed declarative state
            if current_node.hot_args != target_node.hot_args {
                requires_rcu_update = true;
            }

            // Detect changes in Wasm plugin runtime parameters and generate imperative events
            if current_node.hot_node_data != target_node.hot_node_data {
                if !supports_hot_node_data(&target_node.node_type) {
                    return ReconciliationResult::FullRestart;
                }
                let current_map = current_node.hot_node_data.as_ref().unwrap_or(&empty_map);
                let target_map = target_node.hot_node_data.as_ref().unwrap_or(&empty_map);

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
                            "Failed to serialize hot_node_data for node {}: {}",
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

            match target_pipeline.lifecycle_state.as_ref() {
                Some(PipelineLifecycle::RunningDelayed(delay_ms)) => {
                    offline_config.lifecycle_state = Some(PipelineLifecycle::Paused);
                    offline_config.startup_delay_ms = Some(*delay_ms);
                    offline_config.delayed_start_at = None;
                }
                state => {
                    offline_config.lifecycle_state = state.cloned();
                    offline_config.startup_delay_ms = None;
                    offline_config.delayed_start_at = None;
                }
            }

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

#[cfg(test)]
mod tests {
    use super::{reconcile_config, ReconciliationResult};
    use crate::config::hot_config::SharedSystemState;
    use crate::toml_parser::{NodeConfig, PipelineConfig, PipelineLifecycle, WabricksConfig};
    use std::collections::HashMap;
    use std::sync::Arc;
    use wabrix_api::{HotWasmPayload, StandardOpcode};

    fn config(
        node_type: &str,
        node_data: Option<HashMap<String, String>>,
        hot_node_data: Option<HashMap<String, String>>,
    ) -> WabricksConfig {
        WabricksConfig {
            pipelines: vec![PipelineConfig {
                name: "p0".into(),
                workers: vec![0],
                nodes: vec![NodeConfig {
                    id: "n0".into(),
                    node_type: node_type.into(),
                    routes: Vec::new(),
                    queues: None,
                    labels: None,
                    args: HashMap::new(),
                    hot_args: HashMap::new(),
                    node_data,
                    hot_node_data,
                    desired_state: None,
                }],
                poll_frequency: 1,
                lifecycle_state: None,
            }],
            ..WabricksConfig::default()
        }
    }

    #[test]
    fn startup_node_data_changes_require_restart() {
        let mut node_data = HashMap::new();
        node_data.insert("syn_rate".into(), "1k".into());
        let mut changed = node_data.clone();
        changed.insert("syn_rate".into(), "2k".into());

        let result = reconcile_config(
            Arc::new(SharedSystemState::default()),
            &config("TcpNode", Some(node_data), None),
            &config("TcpNode", Some(changed), None),
        );
        assert!(matches!(result, ReconciliationResult::FullRestart));
    }

    #[test]
    fn supported_hot_data_change_generates_update_event() {
        let mut hot_data = HashMap::new();
        hot_data.insert("syn_rate".into(), "2k".into());
        let result = reconcile_config(
            Arc::new(SharedSystemState::default()),
            &config("TcpNode", None, None),
            &config("TcpNode", None, Some(hot_data)),
        );

        let ReconciliationResult::LiveUpdated(events) = result else {
            panic!("expected a live configuration event");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].opcode, StandardOpcode::UpdateConfiguration as u32);
        let payload = postcard::from_bytes::<HotWasmPayload>(&events[0].payload)
            .expect("event payload should decode");
        assert_eq!(payload.updates.get("syn_rate"), Some(&"2k".to_string()));
    }

    #[test]
    fn unsupported_hot_data_change_requires_restart() {
        let mut hot_data = HashMap::new();
        hot_data.insert("signature".into(), "abc".into());
        let result = reconcile_config(
            Arc::new(SharedSystemState::default()),
            &config("StaticNode", None, None),
            &config("StaticNode", None, Some(hot_data)),
        );
        assert!(matches!(result, ReconciliationResult::FullRestart));
    }

    #[test]
    fn node_operational_state_change_generates_control_event() {
        let current = config("TcpNode", None, None);
        let mut target = config("TcpNode", None, None);
        target.pipelines[0].nodes[0].desired_state = Some(PipelineLifecycle::Running);

        let result = reconcile_config(Arc::new(SharedSystemState::default()), &current, &target);
        let ReconciliationResult::LiveUpdated(events) = result else {
            panic!("expected an operational-state event");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].opcode, StandardOpcode::SetOperationalState as u32);
        assert_eq!(events[0].payload, vec![1]);
    }
}
