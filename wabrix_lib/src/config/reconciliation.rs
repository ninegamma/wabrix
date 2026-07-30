use std::sync::Arc;
use crate::config::hot_config::SharedSystemState;
use crate::toml_parser::WabricksConfig;

pub enum ReconciliationResult {
    FullRestart,
    LiveUpdated,
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

    let mut requires_live_update = false;

    for (current_pipeline, target_pipeline) in current.pipelines.iter().zip(target.pipelines.iter()) {
        if current_pipeline.name != target_pipeline.name || current_pipeline.workers != target_pipeline.workers {
            return ReconciliationResult::FullRestart;
        }

        if current_pipeline.nodes.len() != target_pipeline.nodes.len() {
            return ReconciliationResult::FullRestart;
        }

        for (current_node, target_node) in current_pipeline.nodes.iter().zip(target_pipeline.nodes.iter()) {
            if current_node.id != target_node.id || current_node.node_type != target_node.node_type {
                return ReconciliationResult::FullRestart;
            }

            if current_node.args != target_node.args {
                requires_live_update = true;
            }
        }

        // Pipeline lifecycle transitions can trigger live updates without structural rebuilds
        if current_pipeline.lifecycle_state != target_pipeline.lifecycle_state {
            requires_live_update = true;
        }
    }

    if !requires_live_update {
        return ReconciliationResult::NoAction;
    }

    for target_pipeline in &target.pipelines {
        let rcu = match system_state.pipelines.get(&target_pipeline.name) {
            Some(r) => r,
            None => return ReconciliationResult::Error(format!("Pipeline not found in system state: {}", target_pipeline.name)),
        };

        let current_ptr = rcu.load();
        let mut offline_config = (**current_ptr).clone();

        offline_config.lifecycle_state = target_pipeline.lifecycle_state.clone();

        for target_node in &target_pipeline.nodes {
            if let Some(idx) = offline_config.name_to_index.get(&target_node.id) {
                let dynamic_state = &mut offline_config.fast_nodes[*idx].dynamic_state;
                dynamic_state.update_from_strings(&target_node.hot_args);
            } else {
                return ReconciliationResult::Error(format!("Node index not found for: {}", target_node.id));
            }
        }

        rcu.store(Arc::new(offline_config));
    }

    ReconciliationResult::LiveUpdated
}