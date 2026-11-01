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

fn args_equal_except_wasm_path(
    current: &HashMap<String, String>,
    target: &HashMap<String, String>,
) -> bool {
    current
        .iter()
        .filter(|(key, _)| key.as_str() != "wasm_file_path")
        .all(|(key, value)| target.get(key) == Some(value))
        && target
            .iter()
            .filter(|(key, _)| key.as_str() != "wasm_file_path")
            .all(|(key, value)| current.get(key) == Some(value))
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
    // Worker policies, including an explicit turbo request, are applied at startup.
    if current.wabrix.workers != target.wabrix.workers {
        return ReconciliationResult::FullRestart;
    }

    if current.pipelines.len() != target.pipelines.len() {
        return ReconciliationResult::FullRestart;
    }

    let mut rcu_updates = Vec::new();
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

        let lifecycle_changed = current_pipeline.lifecycle_state != target_pipeline.lifecycle_state;
        let mut changed_hot_nodes = Vec::new();

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

            let mut diff_updates = WasmHashMap::new();

            // A WasmEngine artifact path is an executable update and is staged
            // on the workers. Other argument changes still require rebuilding
            // the node graph and therefore a full restart.
            if current_node.args != target_node.args
                && (target_node.node_type != "WasmEngine"
                    || !args_equal_except_wasm_path(&current_node.args, &target_node.args))
            {
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
                changed_hot_nodes.push(*target_node);
            }

            if target_node.node_type == "WasmEngine"
                && current_node.effective_arg("wasm_file_path")
                    != target_node.effective_arg("wasm_file_path")
            {
                let Some(path) = target_node.effective_arg("wasm_file_path") else {
                    return ReconciliationResult::FullRestart;
                };
                diff_updates.insert("wasm_file_path".to_string(), path.clone());
            }

            // Detect changes in Wasm plugin runtime parameters and generate imperative events
            if current_node.hot_node_data != target_node.hot_node_data {
                if !supports_hot_node_data(&target_node.node_type) {
                    return ReconciliationResult::FullRestart;
                }
                let current_map = current_node.hot_node_data.as_ref().unwrap_or(&empty_map);
                let target_map = target_node.hot_node_data.as_ref().unwrap_or(&empty_map);

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
            }

            if !diff_updates.is_empty() {
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
                            "Failed to serialize hot update for node {}: {}",
                            target_node.id, e
                        ));
                    }
                }
            }
        }

        // Publish only pipelines whose declarative RCU state actually changed.
        if lifecycle_changed || !changed_hot_nodes.is_empty() {
            rcu_updates.push((*target_pipeline, lifecycle_changed, changed_hot_nodes));
        }
    }

    if rcu_updates.is_empty() && generated_events.is_empty() {
        return ReconciliationResult::NoAction;
    }

    // Apply RCU mutations if required
    for (target_pipeline, lifecycle_changed, changed_hot_nodes) in rcu_updates {
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

        // Workers advance delayed startup independently of the TOML baseline.
        // Preserve that runtime state unless a lifecycle transition was requested.
        if lifecycle_changed {
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
        }

        for target_node in changed_hot_nodes {
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

    ReconciliationResult::LiveUpdated(generated_events)
}

#[cfg(test)]
mod tests {
    use super::{reconcile_config, ReconciliationResult};
    use crate::config::hot_config::{
        HotNodeData, HotPipelineConfig, NodeDynamicState, SharedSystemState,
    };
    use crate::toml_parser::{NodeConfig, PipelineConfig, PipelineLifecycle, WabricksConfig};
    use arc_swap::ArcSwap;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
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
        node_data.insert("generation_rate".into(), "1k".into());
        let mut changed = node_data.clone();
        changed.insert("generation_rate".into(), "2k".into());

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
        hot_data.insert("generation_rate".into(), "2k".into());
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
        assert_eq!(
            payload.updates.get("generation_rate"),
            Some(&"2k".to_string())
        );
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

    #[test]
    fn worker_turbo_policy_change_requires_restart() {
        for (previous, requested) in [
            (None, Some(false)),
            (None, Some(true)),
            (Some(true), Some(false)),
            (Some(false), None),
            (Some(true), None),
        ] {
            let mut current = config("TcpNode", None, None);
            current.wabrix.workers[0].no_turbo = previous;
            let mut target = current.clone();
            target.wabrix.workers[0].no_turbo = requested;

            assert!(matches!(
                reconcile_config(Arc::new(SharedSystemState::default()), &current, &target),
                ReconciliationResult::FullRestart
            ));
        }
    }

    fn wasm_config() -> WabricksConfig {
        let mut current = config("WasmEngine", None, None);
        current.pipelines[0].lifecycle_state = Some(PipelineLifecycle::RunningDelayed(2));
        current.pipelines[0].nodes[0]
            .hot_args
            .insert("wasm_file_path".into(), "old.wasm".into());
        current
    }

    fn wasm_shared_state(config: &WabricksConfig) -> Arc<SharedSystemState> {
        let pipelines = config
            .pipelines
            .iter()
            .map(|pipeline| {
                let hot = HotPipelineConfig {
                    fast_nodes: pipeline
                        .nodes
                        .iter()
                        .map(|node| HotNodeData {
                            dynamic_state: NodeDynamicState::WasmEngine {
                                wasm_file_path: node
                                    .effective_arg("wasm_file_path")
                                    .unwrap()
                                    .clone(),
                            },
                            ..HotNodeData::default()
                        })
                        .collect(),
                    name_to_index: pipeline
                        .nodes
                        .iter()
                        .enumerate()
                        .map(|(idx, node)| (node.id.clone(), idx))
                        .collect(),
                    // The workers have already completed their configured startup delay.
                    lifecycle_state: Some(PipelineLifecycle::Running),
                    poll_frequency: pipeline.poll_frequency,
                    ..HotPipelineConfig::default()
                };
                (pipeline.name.clone(), Arc::new(ArcSwap::from_pointee(hot)))
            })
            .collect();
        SharedSystemState::new(pipelines)
    }

    #[test]
    fn wasm_path_update_preserves_running_lifecycle_and_other_pipelines() {
        let mut current = wasm_config();
        let mut generator = current.pipelines[0].clone();
        generator.name = "generator_pipe".into();
        current.pipelines.push(generator);
        let state = wasm_shared_state(&current);
        let old_generator = state.pipelines["generator_pipe"].load_full();
        let old_inspector = state.pipelines["p0"].load_full();
        let mut target = current.clone();
        target.pipelines[0].nodes[0]
            .hot_args
            .insert("wasm_file_path".into(), "new.wasm".into());

        let ReconciliationResult::LiveUpdated(events) =
            reconcile_config(state.clone(), &current, &target)
        else {
            panic!("expected a live Wasm update");
        };

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].address_pattern, "*/p0/n0/WasmEngine");
        assert_eq!(events[0].opcode, StandardOpcode::UpdateConfiguration as u32);
        let payload = postcard::from_bytes::<HotWasmPayload>(&events[0].payload).unwrap();
        assert_eq!(payload.updates.get("wasm_file_path").unwrap(), "new.wasm");
        let new_generator = state.pipelines["generator_pipe"].load_full();
        assert!(Arc::ptr_eq(&old_generator, &new_generator));
        let new_inspector = state.pipelines["p0"].load_full();
        assert!(!Arc::ptr_eq(&old_inspector, &new_inspector));
        assert_eq!(
            new_inspector.lifecycle_state,
            Some(PipelineLifecycle::Running)
        );
        assert_eq!(new_inspector.startup_delay_ms, None);
        assert_eq!(new_inspector.delayed_start_at, None);
        assert!(matches!(
            &new_inspector.fast_nodes[0].dynamic_state,
            NodeDynamicState::WasmEngine { wasm_file_path } if wasm_file_path == "new.wasm"
        ));
    }

    #[test]
    fn hot_update_preserves_pending_startup_deadline() {
        let current = wasm_config();
        let state = wasm_shared_state(&current);
        let deadline = Instant::now() + Duration::from_millis(2);
        let mut waiting = (*state.pipelines["p0"].load_full()).clone();
        waiting.lifecycle_state = Some(PipelineLifecycle::Paused);
        waiting.startup_delay_ms = Some(2);
        waiting.delayed_start_at = Some(deadline);
        state.pipelines["p0"].store(Arc::new(waiting));
        let mut target = current.clone();
        target.pipelines[0].nodes[0]
            .hot_args
            .insert("wasm_file_path".into(), "new.wasm".into());

        assert!(matches!(
            reconcile_config(state.clone(), &current, &target),
            ReconciliationResult::LiveUpdated(_)
        ));

        let hot = state.pipelines["p0"].load_full();
        assert_eq!(hot.lifecycle_state, Some(PipelineLifecycle::Paused));
        assert_eq!(hot.startup_delay_ms, Some(2));
        assert_eq!(hot.delayed_start_at, Some(deadline));
    }

    #[test]
    fn explicit_lifecycle_change_applies_new_state_and_delay() {
        for desired in [
            Some(PipelineLifecycle::RunningDelayed(5)),
            Some(PipelineLifecycle::Running),
            Some(PipelineLifecycle::Paused),
            None,
        ] {
            let current = wasm_config();
            let state = wasm_shared_state(&current);
            let mut target = current.clone();
            target.pipelines[0].lifecycle_state = desired.clone();

            let ReconciliationResult::LiveUpdated(events) =
                reconcile_config(state.clone(), &current, &target)
            else {
                panic!("expected a lifecycle update");
            };

            assert!(events.is_empty());
            let hot = state.pipelines["p0"].load_full();
            match desired {
                Some(PipelineLifecycle::RunningDelayed(delay_ms)) => {
                    assert_eq!(hot.lifecycle_state, Some(PipelineLifecycle::Paused));
                    assert_eq!(hot.startup_delay_ms, Some(delay_ms));
                }
                state => {
                    assert_eq!(hot.lifecycle_state, state);
                    assert_eq!(hot.startup_delay_ms, None);
                }
            }
            assert_eq!(hot.delayed_start_at, None);
        }
    }

    #[test]
    fn hot_update_preserves_unchanged_node_runtime_state() {
        let mut current = wasm_config();
        let mut other_node = current.pipelines[0].nodes[0].clone();
        other_node.id = "other".into();
        current.pipelines[0].nodes.push(other_node);
        let state = wasm_shared_state(&current);
        let mut hot = (*state.pipelines["p0"].load_full()).clone();
        hot.fast_nodes[1].dynamic_state = NodeDynamicState::WasmEngine {
            wasm_file_path: "runtime.wasm".into(),
        };
        state.pipelines["p0"].store(Arc::new(hot));
        let mut target = current.clone();
        target.pipelines[0].nodes[0]
            .hot_args
            .insert("wasm_file_path".into(), "new.wasm".into());

        assert!(matches!(
            reconcile_config(state.clone(), &current, &target),
            ReconciliationResult::LiveUpdated(_)
        ));

        let hot = state.pipelines["p0"].load_full();
        assert!(matches!(
            &hot.fast_nodes[1].dynamic_state,
            NodeDynamicState::WasmEngine { wasm_file_path } if wasm_file_path == "runtime.wasm"
        ));
    }

    #[test]
    fn unchanged_config_does_not_publish_rcu_state() {
        let current = wasm_config();
        let state = wasm_shared_state(&current);
        let before = state.pipelines["p0"].load_full();

        assert!(matches!(
            reconcile_config(state.clone(), &current, &current),
            ReconciliationResult::NoAction
        ));

        assert!(Arc::ptr_eq(&before, &state.pipelines["p0"].load_full()));
    }
}
