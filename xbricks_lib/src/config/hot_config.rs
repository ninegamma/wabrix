use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use macaddr::{MacAddr6};
use serde::{Deserialize, Serialize};
use xbricks_base::NodeId;
use crate::generator_node::GenState;

/// Strongly typed states for specific node types
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum NodeDynamicState {
    Generator {
        state: GenState,
        pps_limit: u64,
        packet_limit: usize,
        dst_ip: Ipv4Addr,
        dst_mac: MacAddr6,
        batch_size: usize,
    },
    Inspector {
        drop_probability: f32,
    },
    #[default]
    Static,
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

    // 🐌 SLOW PATH: Axum API uses this to translate UI strings into the Vec index
    pub name_to_index: HashMap<String, usize>,

    /// Timer poll frequency in Hz. Hot-changeable — workers read this on each iteration.
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