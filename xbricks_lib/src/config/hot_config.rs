use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use xbricks_base::NodeId;

/// Represents the mutable parameters of a single pipeline node
#[derive(Debug, Clone, Default)]
pub struct HotNodeData {
    pub routes: Vec<NodeId>,
    pub default_route: Option<NodeId>,
    // For things like changing telemetry buckets, or specific node args on the fly
    pub args: HashMap<String, String>,
}

/// The overarching lock-free configuration that workers read
#[derive(Debug, Clone, Default)]
pub struct HotPipelineConfig {
    // ⚡ FAST PATH: Workers use this.
    // Index matches usize::from(&node_id)
    pub fast_nodes: Vec<HotNodeData>,

    // 🐌 SLOW PATH: Axum API uses this to translate UI strings into the Vec index
    pub name_to_index: HashMap<String, usize>,
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