extern crate alloc;
extern crate core;

pub mod interface;
pub mod frame_registry;
pub mod system;
pub mod common;
pub mod telemetry;
pub mod node_instance_id;

use core::fmt::Display;
use crossbeam_channel::Sender;
use xsk_rs::Umem;
use crate::frame_registry::MAX_UMEM;
use crate::interface::batch::Batch;

/// Configuration specific to the shared memory region (UMEM).
/// All sockets in the same group MUST use identical values here.
#[derive(Debug, Clone)]
pub struct RuntimeUmemConfig {
    pub group_id: usize,
    pub frame_size: u32,
    /// log2(frame_size) for storage in u8
    pub frame_size_log2: u8,
    /// total frames in this group
    pub frame_count: u32,
    pub head_room: u32,
    pub fq_size: u32,
    pub cq_size: u32,
    ///the offset of the global frame index for this group
    pub group_start_frame_idx: u32,
    /// Number of partitions this UMEM is divided into
    pub partitions: u32,
    /// Frames at the start of this UMEM exclusively reserved for WASM plugins.
    /// Worker partitions are allocated starting after these frames.
    pub reserved_frames: u32,
}

impl Drop for RuntimeUmemConfig {
    fn drop(&mut self) {
        // This will ONLY print when the strong_count hits ZERO
        println!("!!! KERNEL NOTIFY: Umem Group {} is physically DEALLOCATING now !!!", self.group_id);
    }
}


#[derive(Debug, Clone, Copy)]
pub struct UmemPartition {
    pub worker_id: usize,     // Worker thread that owns this partition
    pub umem_group_id: usize, // Memory domain (UMEM group)
    pub start_idx: u32,       // Absolute global frame index (used by system_builder for layout math)
    // end_idx is non-inclusive: [start_idx, end_idx)
    pub end_idx: u32,         // Absolute global frame index
    /// UMEM-relative start: local_start = start_idx - group_start_frame_idx
    pub local_start: u32,
    /// UMEM-relative end (non-inclusive): local_end = end_idx - group_start_frame_idx
    pub local_end: u32,
}

impl UmemPartition {
    /// Check ownership against an **encoded** global frame index
    /// (format: `(umem_id << 24) | local_idx`).
    #[inline]
    pub fn owns_encoded(&self, encoded_frame_idx: u32) -> bool {
        let (umem_id, local_idx) = crate::frame_registry::decode_global_frame(encoded_frame_idx);
        umem_id == self.umem_group_id
            && local_idx >= self.local_start
            && local_idx < self.local_end
    }
}

// 1. Define the Command Enum
#[derive(Debug, Clone, Copy)]
pub enum BusCommand {
    Retain(u32),  // Increment RefCount
    Release(u32), // Decrement RefCount (and free if 0)
}

// 2. Update SystemBus to carry BusCommand
#[derive(Clone)]
pub struct SystemBus {
    pub bus_senders: Vec<Sender<BusCommand>>,
    /// All UMEM partitions across all workers, used by determine_owner to map
    /// a frame index to the worker that owns it.
    pub umem_partitions: Vec<UmemPartition>,
    pub umem_addr: [Option<usize>; MAX_UMEM] ,
}

///This is safe because crossbeam_channel::Sender<u32> is already Send, so a Vec of them is also Send.
unsafe impl Send for SystemBus {}



/// Directives tell the scheduler where the batch should go next.
pub enum Directive {
    /// Pass the entire batch to the next node in the DAG.
    Next(NodeId),
    /// The batch has been split (e.g., by a Router).
    /// The Vec contains (NextNodeId, SubBatch).
    Split(Vec<(NodeId, Batch)>),
    /// The batch reached a sink (TX) or was fully dropped.
    Stop,
}



pub trait Node: Send + std::any::Any {
    fn id(&self) -> &NodeId;

    /// The core logic of the node.
    /// It modifies the batch in-place (RTC efficiency).
    fn process(&mut self, batch: &mut Batch) -> Directive;

    /// Control-path for timers (defaults to doing nothing)
    fn requires_timers(&self) -> bool { false }
    fn poll_timers(&mut self, batch: &mut Batch, current_tsc: u64) -> Directive {
        Directive::Stop // Default implementation does nothing
    }

    /// do we have a control channel (defaults to doing nothing)
    fn requires_control_events(&self) -> bool { false }
    /// called by the scheduler when a control event is received  (defaults to doing nothing)
    fn poll_control_events(&mut self) { }

    /// called before startup, e.g., rx nodes can replenish the fill queue
    fn prepare(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn processed_packets(&self) -> usize;

    fn full_batch_count(&self) -> Option<usize> {
        None
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub index: u32,
    pub id: String,
}

impl NodeId {
    pub fn new(index: u32, id: String) -> Self {
        Self { index, id }
    }
}

impl From<NodeId> for u32 {
    fn from(node_id: NodeId) -> Self {
        node_id.index
    }
}

impl From<&NodeId> for u32 {
    fn from(node_id: &NodeId) -> Self {
        node_id.index
    }
}

impl From<NodeId> for usize {
    fn from(node_id: NodeId) -> Self {
        node_id.index as usize
    }
}

impl From<&NodeId> for usize {
    fn from(node_id: &NodeId) -> Self {
        node_id.index as usize
    }
}

impl TryFrom<NodeId> for u16 {
    type Error = std::num::TryFromIntError;

    fn try_from(node_id: NodeId) -> Result<Self, Self::Error> {
        u16::try_from(node_id.index)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        // 1. Build your custom string
        let output = format!("{}: {}", self.index, self.id);

        // 2. Let the formatter apply the, e.g., {:20} or {:>20} rules!
        f.pad(&output)
    }
}

#[derive(Clone, Debug)]
pub struct ControlEvent {
    pub opcode: u32,
    pub payload: Vec<u8>,
}
