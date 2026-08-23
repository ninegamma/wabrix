use crate::generator_node::GenState;
use crate::toml_parser::PipelineLifecycle;
use arc_swap::ArcSwap;
use macaddr::MacAddr6;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;
use wabrix_api::helpers::parse_suffixed_number;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::NodeId;

/// Strongly typed states for specific node types
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum NodeDynamicState {
    Generator {
        state: GenState,
        pps_limit: u64,
        packet_limit: u64,
        dst_ip: Ipv4Addr,
        dst_mac: MacAddr6,
        batch_size: u32,
    },
    Inspector {
        drop_probability: f32,
    },
    WasmEngine {
        wasm_file_path: String,
    },
    #[default]
    Static,
}

// In wabrix_lib/config/hot_config.rs

impl NodeDynamicState {
    /// Takes the current typed state and applies updates from the weakly-typed hot_args map.
    pub fn update_from_strings(&mut self, updates: &HashMap<String, String>) {
        match self {
            NodeDynamicState::Generator {
                state,
                pps_limit,
                packet_limit,
                batch_size,
                ..
            } => {
                if let Some(cmd) = updates.get("state") {
                    // Replaces the old 'cmd' logic
                    *state = match cmd.as_str() {
                        "start" => GenState::Generating,
                        "stop" => GenState::Idle,
                        _ => state.clone(), // Ignore invalid commands
                    };
                }
                if let Some(pps_str) = updates.get("pps") {
                    match parse_suffixed_number(pps_str) {
                        Ok(v) => {
                            *pps_limit = v;
                        }
                        Err(e) => {
                            log::warn!("hot_args pps parse error: {}", e);
                        }
                    }
                }
                if let Some(count_str) = updates.get("packet_limit") {
                    match parse_suffixed_number(count_str) {
                        Ok(v) => {
                            *packet_limit = v;
                        }
                        Err(e) => {
                            log::warn!("hot_args packet_count parse error: {}", e);
                        }
                    }
                }
                if let Some(bs_str) = updates.get("batch_size") {
                    match bs_str.parse::<u32>() {
                        Ok(v) if v > 0 && v <= MAX_BATCH_SIZE as u32 => {
                            *batch_size = v;
                        }
                        Ok(0) => {
                            log::warn!("hot_args batch_size must be > 0");
                        }
                        Ok(v) => {
                            log::warn!(
                                "hot_args batch_size {} exceeds MAX_BATCH_SIZE {}",
                                v,
                                MAX_BATCH_SIZE
                            );
                        }
                        Err(e) => {
                            log::warn!("hot_args batch_size parse error: {}", e);
                        }
                    }
                }
            }
            NodeDynamicState::Inspector { drop_probability } => {
                if let Some(prob_str) = updates.get("drop_probability") {
                    if let Ok(p) = prob_str.parse::<f32>() {
                        *drop_probability = p;
                    }
                }
            }
            NodeDynamicState::WasmEngine { wasm_file_path } => {
                if let Some(path_str) = updates.get("wasm_file_path") {
                    *wasm_file_path = path_str.to_string();
                }
            }
            NodeDynamicState::Static => {}
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HotNodeData {
    pub routes: Vec<NodeId>,
    pub dynamic_state: NodeDynamicState,
}

/// The overarching lock-free configuration that workers read
#[derive(Debug, Clone, Default)]
pub struct HotPipelineConfig {
    // ⚡ FAST PATH: Workers use this.
    // Index matches usize::from(&node_id)
    pub fast_nodes: Vec<HotNodeData>,
    // SLOW PATH: Axum API uses this to translate UI strings into the Vec index
    pub name_to_index: HashMap<String, usize>,
    /// Hot-changeable — workers read this on each iteration.
    pub lifecycle_state: Option<PipelineLifecycle>,
    /// Startup delay. While present, lifecycle_state is Paused until the
    /// monotonic deadline is reached.
    pub startup_delay_ms: Option<u64>,
    pub delayed_start_at: Option<Instant>,
    /// Timer poll frequency in Hz. - Hot-changeable
    pub poll_frequency: u64,
}

/// This is what we will pass to both the Axum API and the Worker Threads
/// The global state holds an RCU pointer FOR EACH pipeline
#[derive(Default)]
pub struct SharedSystemState {
    // Key: pipeline.name
    pub pipelines: HashMap<String, Arc<ArcSwap<HotPipelineConfig>>>,
}

impl SharedSystemState {
    /// Creates a new SharedSystemState from an initialized HashMap of pipeline RCUs
    pub fn new(pipelines: HashMap<String, Arc<ArcSwap<HotPipelineConfig>>>) -> Arc<Self> {
        Arc::new(Self { pipelines })
    }
}
