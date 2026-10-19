extern crate alloc;
extern crate core;

pub mod common;
pub mod frame_registry;
pub mod interface;
pub mod mailbox;
pub mod node_instance_id;
pub mod system;
pub mod telemetry;

use crate::frame_registry::{decode_global_frame, encode_global_frame, MAX_PARTITIONS, MAX_UMEM};
use crate::interface::batch::Batch;
use core::fmt::Display;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

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
}

impl Drop for RuntimeUmemConfig {
    fn drop(&mut self) {
        // This will ONLY print when the strong_count hits ZERO
        println!(
            "!!! KERNEL NOTIFY: Umem Group {} is physically DEALLOCATING now !!!",
            self.group_id
        );
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(align(64))]
pub struct UmemPartition {
    /// Stable index carried by every frame token in this partition.
    pub partition_id: usize,
    pub worker_id: usize,     // Worker thread that owns this partition
    pub umem_group_id: usize, // Memory domain (UMEM group)
    pub start_idx: u32, // Absolute global frame index (used by system_builder for layout math)
    // end_idx is non-inclusive: [start_idx, end_idx)
    pub end_idx: u32, // Absolute global frame index
    /// UMEM-relative start: local_start = start_idx - group_start_frame_idx
    pub local_start: u32,
    /// UMEM-relative end (non-inclusive): local_end = end_idx - group_start_frame_idx
    pub local_end: u32,
}

impl UmemPartition {
    /// Check ownership against an **encoded** global frame index
    /// (format: partition ID plus partition-relative offset).
    #[inline]
    pub fn owns_encoded(&self, encoded_frame_idx: u32) -> bool {
        let (partition_id, offset) = decode_global_frame(encoded_frame_idx);
        partition_id == self.partition_id
            && offset < self.local_end.saturating_sub(self.local_start)
    }
}

/// Per-worker lockless return queue: packed u32 commands.
/// Bit 31 = command (0 = Release, 1 = Retain); bits 30-23 = partition ID;
/// bits 22-0 = partition-relative offset.
/// See frame_registry::{CMD_BIT, is_retain, extract_global_idx} for helpers.
#[derive(Clone)]
#[repr(align(64))]
pub struct SystemBus {
    /// One ArrayQueue<u32> per worker. Senders push packed u32 commands;
    /// the owning worker pops them in process_returns().
    pub bus_senders: Vec<Arc<ArrayQueue<u32>>>,
    /// Directly indexed partition metadata for frame-token resolution.
    pub partition_directory: Vec<Option<UmemPartition>>,
    /// Direct local-frame-to-token lookup used when RX descriptors carry only an address.
    pub frame_tokens: Vec<Vec<Option<u32>>>,
    pub umem_addr: [Option<usize>; MAX_UMEM],
}

impl SystemBus {
    /// Builds the fixed-index directory for the supplied stable partitions.
    pub fn partition_directory(partitions: &[UmemPartition]) -> Vec<Option<UmemPartition>> {
        let mut directory = vec![None; MAX_PARTITIONS];
        for partition in partitions {
            assert!(
                partition.partition_id < MAX_PARTITIONS,
                "partition ID exceeds token capacity"
            );
            assert!(
                partition.local_end >= partition.local_start,
                "partition range is inverted"
            );
            assert!(
                partition.local_end - partition.local_start
                    <= crate::frame_registry::MAX_PARTITION_FRAMES,
                "partition exceeds token offset capacity"
            );
            assert!(
                directory[partition.partition_id].is_none(),
                "duplicate partition ID"
            );
            directory[partition.partition_id] = Some(*partition);
        }
        directory
    }

    pub fn frame_tokens(
        partitions: &[UmemPartition],
        frame_counts: &[u32],
    ) -> Vec<Vec<Option<u32>>> {
        let mut tokens = frame_counts
            .iter()
            .map(|&count| vec![None; count as usize])
            .collect::<Vec<_>>();
        for partition in partitions {
            let Some(group_tokens) = tokens.get_mut(partition.umem_group_id) else {
                continue;
            };
            for local_idx in partition.local_start..partition.local_end {
                if let Some(slot) = group_tokens.get_mut(local_idx as usize) {
                    *slot = Some(encode_global_frame(
                        partition.partition_id,
                        local_idx - partition.local_start,
                    ));
                }
            }
        }
        tokens
    }
}

/// SAFETY: Arc<ArrayQueue<u32>> is Send, so Vec<Arc<ArrayQueue<u32>>> is also Send.
unsafe impl Send for SystemBus {}

/// Directives tell the scheduler where the batch should go next.
pub enum Directive {
    /// Pass the entire batch to the next node in the DAG.
    Next(NodeIndex),
    /// The batch has been split (e.g., by a Router).
    /// The Vec contains (next node index, sub-batch).
    Split(Vec<(NodeIndex, Batch)>),
    /// The batch reached a sink (TX) or was fully dropped.
    Stop,
}

/// Compact, allocation-free reference to a node used by the scheduler.
///
/// `NodeId` remains the owning identity used for configuration and logging;
/// directives only need its stable numeric index to dispatch a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct NodeIndex(pub u32);

impl From<u32> for NodeIndex {
    fn from(index: u32) -> Self {
        Self(index)
    }
}

impl From<NodeIndex> for usize {
    fn from(index: NodeIndex) -> Self {
        index.0 as usize
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

impl From<NodeId> for NodeIndex {
    fn from(node_id: NodeId) -> Self {
        Self(node_id.index)
    }
}

impl From<&NodeId> for NodeIndex {
    fn from(node_id: &NodeId) -> Self {
        Self(node_id.index)
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

impl Display for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        // 1. Build your custom string
        let output = format!("{}: {}", self.index, self.id);

        // 2. Let the formatter apply the, e.g., {:20} or {:>20} rules!
        f.pad(&output)
    }
}

/// Result of handling a control-plane event on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEventResult {
    /// The node accepted and applied the event.
    Handled,
    /// The opcode is supported, but the payload failed validation.
    InvalidPayload,
    /// The node does not implement the opcode.
    Unsupported,
}

pub trait Node: Send + std::any::Any {
    fn id(&self) -> &NodeId;

    /// The core logic of the node.
    /// It modifies the batch in-place (RTC efficiency).
    fn process(&mut self, batch: &mut Batch) -> Directive;

    /// Control-path for timers (defaults to doing nothing)
    fn requires_timers(&self) -> bool {
        false
    }
    fn poll_timers(&mut self, _batch: &mut Batch, _current_tsc: u64) -> Directive {
        Directive::Stop // Default implementation does nothing
    }

    /// do we have a control channel (defaults to doing nothing)
    fn requires_control_events(&self) -> bool {
        false
    }

    /// called by the scheduler when a control event is received for this node (defaults to doing nothing)
    fn handle_control_event(&mut self, _event: &ControlEvent) -> ControlEventResult {
        ControlEventResult::Unsupported
    }

    /// called before startup, e.g., rx nodes can replenish the fill queue
    fn prepare(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn processed_packets(&self) -> u64;

    fn full_batch_count(&self) -> Option<u32> {
        None
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        0
    }
}

///no support of cloning here, because we use Arc
#[derive(Debug)]
pub struct ControlEvent {
    /// address pattern like "w0/*/rx_node" or "*/*/*/WasmEngine"
    /// The format is: {worker}/{pipeline}/{node_name}/{node_type}
    /// Missing trailing segments are implicitly treated as wildcards.
    pub address_pattern: String,
    pub opcode: u32,
    pub payload: Vec<u8>,
}
