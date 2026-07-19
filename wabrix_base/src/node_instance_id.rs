use core::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeInstanceId {
    /// The numeric index used for ultra-fast DAG array routing
    pub index: u32,

    // --- The Structural Identity ---
    pub worker_id: u32,
    pub pipeline: String,
    pub node_name: String,
    pub node_type: String,
}

impl NodeInstanceId {
    pub fn new(index: u32, worker_id: u32, pipeline: String, node_name: String, node_type: String) -> Self {
        Self {
            index,
            worker_id,
            pipeline,
            node_name,
            node_type,
        }
    }

    /// Evaluates a target pattern like "w0/*/rx_node" or "*/*/*/WasmInspectorNode"
    /// The format is: {worker}/{pipeline}/{node_name}/{node_type}
    /// Missing trailing segments are implicitly treated as wildcards.
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        // Global Broadcast
        if pattern == "*" || pattern.is_empty() {
            return true;
        }

        let parts: Vec<&str> = pattern.split('/').collect();

        // 1. Match Worker (handles both "0" and "w0" syntax, and "*")
        if let Some(&w_pat) = parts.first() {
            if w_pat != "*" {
                let w_target = w_pat.trim_start_matches('w');
                if w_target != self.worker_id.to_string() {
                    return false;
                }
            }
        }

        // 2. Match Pipeline
        if let Some(&p_pat) = parts.get(1) {
            if p_pat != "*" && p_pat != self.pipeline {
                return false;
            }
        }

        // 3. Match Node Name
        if let Some(&n_pat) = parts.get(2) {
            if n_pat != "*" && n_pat != self.node_name {
                return false;
            }
        }

        // 4. Match Node Type
        if let Some(&t_pat) = parts.get(3) {
            if t_pat != "*" && t_pat != self.node_type {
                return false;
            }
        }

        true // All provided segments matched!
    }
}

// Keep your existing conversions for DAG routing
impl From<NodeInstanceId> for u32 {
    fn from(node_id: NodeInstanceId) -> Self {
        node_id.index
    }
}
impl From<&NodeInstanceId> for u32 {
    fn from(node_id: &NodeInstanceId) -> Self {
        node_id.index
    }
}
impl From<NodeInstanceId> for usize {
    fn from(node_id: NodeInstanceId) -> Self {
        node_id.index as usize
    }
}
impl From<&NodeInstanceId> for usize {
    fn from(node_id: &NodeInstanceId) -> Self {
        node_id.index as usize
    }
}
impl TryFrom<NodeInstanceId> for u16 {
    type Error = std::num::TryFromIntError;
    fn try_from(node_id: NodeInstanceId) -> Result<Self, Self::Error> {
        u16::try_from(node_id.index)
    }
}

// Make the logs look beautiful and fully qualified
impl Display for NodeInstanceId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = format!(
            "w{}/{}/{} [{}]",
            self.worker_id, self.pipeline, self.node_name, self.node_type
        );
        f.pad(&output)
    }
}
