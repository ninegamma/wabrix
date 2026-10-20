use crate::interface::Pdu;
use crate::{RuntimeUmemConfig, SystemBus, UmemPartition};
use core::cell::Cell;
use core::mem::MaybeUninit;
use crossbeam_queue::ArrayQueue;
use log::debug;
use std::sync::Arc;
use wabrix_api::MAX_BATCH_SIZE;
use xsk_core::Umem;

/// Identifies a frame by its UMEM group and UMEM-local frame index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct FrameToken {
    // The high bit of umem_id is reserved for the queue-only retain command.
    umem_id: u32,
    frame_index: u32,
}

const RETAIN_COMMAND_BIT: u32 = 1 << 31;

impl FrameToken {
    #[inline(always)]
    pub fn new(umem_id: u32, frame_index: u32) -> Self {
        assert!(
            umem_id < MAX_UMEM as u32,
            "UMEM ID uses reserved command bits"
        );
        Self {
            umem_id: umem_id & !RETAIN_COMMAND_BIT,
            frame_index,
        }
    }

    #[inline(always)]
    pub fn umem_id(self) -> u32 {
        self.umem_id & !RETAIN_COMMAND_BIT
    }

    #[inline(always)]
    pub fn frame_index(self) -> u32 {
        self.frame_index
    }

    #[inline(always)]
    pub(crate) fn with_retain_command(mut self) -> Self {
        self.umem_id |= RETAIN_COMMAND_BIT;
        self
    }

    #[inline(always)]
    pub(crate) fn is_retain_command(self) -> bool {
        self.umem_id & RETAIN_COMMAND_BIT != 0
    }

    #[inline(always)]
    pub(crate) fn without_command(mut self) -> Self {
        self.umem_id &= !RETAIN_COMMAND_BIT;
        self
    }
}

impl core::fmt::Display for FrameToken {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "umem {} frame {}",
            self.umem_id(),
            self.frame_index()
        )
    }
}

// Thread-local storage for the worker's frame registry pointer
// This allows XSocketRx/Tx to access the worker's registry without storing it in each socket
thread_local! {
    static WORKER_FRAME_REGISTRY: Cell<*mut FrameRegistry> = const { Cell::new(std::ptr::null_mut()) };
    static TSC_FREQUENCY: Cell<u64> = const { Cell::new(0) };
    static TSC_START: Cell<u64> = const { Cell::new(0) };
}

/// Set the TSC frequency and start timestamp for the current thread
pub fn set_tsc_frequency(freq: u64, start_tsc: u64) {
    TSC_FREQUENCY.with(|f| f.set(freq));
    TSC_START.with(|s| s.set(start_tsc));
}

/// Get the TSC frequency for the current thread
pub fn get_tsc_frequency() -> u64 {
    TSC_FREQUENCY.with(|f| f.get())
}

/// Get the TSC start timestamp for the current thread
pub fn get_tsc_start() -> u64 {
    TSC_START.with(|s| s.get())
}

/// Set the worker's frame registry for the current thread
/// Must be called before any socket operations
pub fn set_worker_frame_registry(registry: *mut FrameRegistry) {
    WORKER_FRAME_REGISTRY.with(|r| r.set(registry));
}

/// Get the worker's frame registry for the current thread
pub fn get_worker_frame_registry() -> *mut FrameRegistry {
    WORKER_FRAME_REGISTRY.with(|r| r.get())
}

/// Number of addressable frame-registry partitions.
pub const MAX_PARTITIONS: usize = 1 << 8;

/// Capacity of each worker's return queue.
/// Must be large enough to absorb cross-worker frame returns without dropping.
pub const RETURN_QUEUE_CAPACITY: usize = 65536;

/// Maximum number of distinct UMEM groups supported by fixed UMEM-group-indexed tables.
pub const MAX_UMEM: usize = 128;

/// Per-UMEM partition data within a worker's registry.
/// `free_stash` holds UMEM group and UMEM-local frame-index pairs.
/// `refcounts` is indexed by local_idx - `refcount_start`.
pub struct PartitionData {
    /// First UMEM-relative frame represented by `refcounts`.
    refcount_start: u32,
    /// One counter per frame owned by this partition. Keeping this partition-local avoids
    /// allocating a counter for every frame in the UMEM when a worker owns only a subrange.
    refcounts: Vec<u16>,
    /// Frame tokens available for allocation.
    free_stash: Vec<FrameToken>,
}

impl PartitionData {
    /// Converts a UMEM-relative frame index to an index in this partition's compact
    /// refcount array. Returns `None` for a frame that does not belong to the partition.
    #[inline(always)]
    fn refcount_index(&self, local_idx: u32) -> Option<usize> {
        let idx = local_idx.checked_sub(self.refcount_start)? as usize;
        (idx < self.refcounts.len()).then_some(idx)
    }
}

/// Per-worker frame registry information
/// One registry per worker thread, shared by all sockets in that worker
/// Supports multiple UMEM groups per worker as per Multi-Umem Static Partitioning model
pub struct FrameRegistry {
    pub worker_id: usize,
    /// Indexed by umem_id (0..MAX_UMEM). None = this worker has no partition in that UMEM.
    pub registry_map: Vec<Option<PartitionData>>,
    /// UMEM groups with a registry entry, used to avoid scanning empty slots in hot-path totals.
    active_umem_ids: Vec<usize>,
    /// Lockless return queue; the high bit of `umem_id` marks retain commands.
    return_queue: Arc<ArrayQueue<FrameToken>>,
    /// System bus for frame lifecycle management
    pub bus: Arc<SystemBus>,
    /// Indexed by umem_id (0..MAX_UMEM).
    pub umem_configs: Vec<Option<Arc<RuntimeUmemConfig>>>,
    /// Indexed by umem_id (0..MAX_UMEM). Runtime physical memory mappings.
    pub active_umems: Vec<Option<Arc<Umem>>>,
}

impl FrameRegistry {
    /// Create a new FrameRegistry for a worker with multiple UMEM partitions
    /// partitions: Vec of (umem_group_id, UmemPartition) tuples
    pub fn new(
        worker_id: usize,
        partitions: Vec<(usize, UmemPartition)>,
        return_queue: Arc<ArrayQueue<FrameToken>>,
        bus: Arc<SystemBus>,
        umem_configs: Vec<Option<Arc<RuntimeUmemConfig>>>,
    ) -> Self {
        let mut registry_map: Vec<Option<PartitionData>> = (0..MAX_UMEM).map(|_| None).collect();

        debug!(
            "Creating FrameRegistry for worker {} with {} UMEM partition(s)",
            worker_id,
            partitions.len()
        );

        for (umem_group_id, partition) in partitions {
            let umem_config = umem_configs[umem_group_id]
                .as_ref()
                .expect("FrameRegistry::new: missing RuntimeUmemConfig for UMEM group");

            let group_start = umem_config.group_start_frame_idx;
            let frame_count = umem_config.frame_count as usize;

            // Convert absolute partition range to UMEM-relative local indices
            let local_start = partition.start_idx - group_start;
            let local_end = partition.end_idx - group_start;
            let partition_size = (local_end - local_start) as usize;

            let free_stash: Vec<FrameToken> = (local_start..local_end)
                .map(|frame_index| FrameToken::new(umem_group_id as u32, frame_index))
                .collect();

            debug!(
                "  Worker {}: UMEM group {} partition - {} frames, local range [{}, {}), refcounts reduced from full UMEM size {}",
                worker_id, umem_group_id, partition_size, local_start, local_end, frame_count
            );

            registry_map[umem_group_id] = Some(PartitionData {
                refcount_start: local_start,
                refcounts: vec![0u16; partition_size],
                free_stash,
            });
        }

        // Build this once so total_free_frames() only visits groups assigned to this worker.
        let active_umem_ids = registry_map
            .iter()
            .enumerate()
            .filter_map(|(umem_id, partition)| partition.as_ref().map(|_| umem_id))
            .collect();

        Self {
            worker_id,
            registry_map,
            active_umem_ids,
            return_queue,
            bus,
            umem_configs,
            active_umems: vec![None; MAX_UMEM],
        }
    }

    #[inline]
    pub fn process_returns(&mut self) {
        // Track TOTAL processed items (both releases and retains)
        let mut processed_count = 0;

        // Strictly bound the drain loop to prevent latency spikes
        const MAX_PROCESS_PER_TICK: usize = 512;

        while processed_count < MAX_PROCESS_PER_TICK {
            if let Some(queued_token) = self.return_queue.pop() {
                let is_retain = queued_token.is_retain_command();
                let frame_token = queued_token.without_command();
                if is_retain {
                    self.retain_local(frame_token);
                } else {
                    self.release_local(frame_token);
                }
                processed_count += 1;
            } else {
                // 🚨 CRITICAL: The queue is empty. Break the loop! 🚨
                break;
            }
        }

        if processed_count > 0 && log::log_enabled!(log::Level::Trace) {
            let total_free: usize = self
                .registry_map
                .iter()
                .filter_map(|e| e.as_ref())
                .map(|p| p.free_stash.len())
                .sum();
            log::trace!(
                "Worker {}: Processed {} frame commands, total free frames: {}",
                self.worker_id,
                processed_count,
                total_free
            );
        }
    }

    fn retain_local(&mut self, frame_token: FrameToken) {
        let umem_id = frame_token.umem_id() as usize;
        let Some(partition_data) = self.registry_map.get_mut(umem_id).and_then(Option::as_mut)
        else {
            log::error!(
                "Worker {}: retain for unassigned UMEM group {}",
                self.worker_id,
                umem_id
            );
            return;
        };
        let Some(refcount_idx) = partition_data.refcount_index(frame_token.frame_index()) else {
            log::error!(
                "Worker {}: retain for frame {:?} outside its partition",
                self.worker_id,
                frame_token
            );
            return;
        };
        // SAFETY: refcount_index verifies the frame belongs to this partition.
        unsafe { *partition_data.refcounts.get_unchecked_mut(refcount_idx) += 1 };
    }

    /// Releases a frame back to the worker's frame registry.
    ///
    /// # Parameters
    /// * `frame_token` - UMEM group and UMEM-local frame index.
    #[inline]
    pub fn release_local(&mut self, frame_token: FrameToken) {
        let umem_id = frame_token.umem_id() as usize;
        // 2. Direct Vec index — O(1) with no hash computation
        if let Some(partition_data) = self.registry_map.get_mut(umem_id).and_then(Option::as_mut) {
            let Some(idx) = partition_data.refcount_index(frame_token.frame_index()) else {
                log::error!(
                    "Worker {}: frame local_idx={} is outside UMEM {} partition",
                    self.worker_id,
                    frame_token.frame_index(),
                    umem_id
                );
                return;
            };

            unsafe {
                // SAFETY: refcount_index verifies that idx is within this partition's
                // compact refcounts vector.
                let refcount = partition_data.refcounts.get_unchecked_mut(idx);

                if *refcount == 0 {
                    log::error!(
                        "🔥 CRITICAL: Prevented Double-Free of frame local_idx={} in UMEM {}!",
                        frame_token.frame_index(),
                        umem_id
                    );
                    return;
                }

                *refcount -= 1;
                if *refcount == 0 {
                    partition_data.free_stash.push(frame_token);
                }
            }
        } else {
            log::error!(
                "Worker {}: partition data not found for UMEM group {}!",
                self.worker_id,
                umem_id
            );
        }
    }

    /// Allocates up to `dst.len()` frames from the specific UMEM partition.
    /// Writes frame token directly into the provided slice.
    /// Returns the number of frames successfully allocated.
    pub fn allocate_batch_into(&mut self, umem_id: usize, dst: &mut [FrameToken]) -> usize {
        if let Some(registry) = self.registry_map.get_mut(umem_id).and_then(Option::as_mut) {
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, dst.len());

            if to_take == 0 {
                log::warn!(
                    "Worker {}: Starvation! No frames available in UMEM {}",
                    self.worker_id,
                    umem_id
                );
                return 0;
            }

            // FAST PATH: Bulk memory copy
            let start_idx = available - to_take;
            dst[..to_take].copy_from_slice(&stash[start_idx..]);

            // Mark all allocated frames as actively owned (refcount = 1).
            for &frame_token in &dst[..to_take] {
                let refcount_idx = (frame_token.frame_index() - registry.refcount_start) as usize;
                unsafe {
                    // SAFETY: `dst` was copied from this partition's free_stash.
                    *registry.refcounts.get_unchecked_mut(refcount_idx) = 1;
                }
            }

            stash.truncate(start_idx);

            to_take
        } else {
            log::error!(
                "Worker {} tried to alloc from unassigned UMEM {}",
                self.worker_id,
                umem_id
            );
            0
        }
    }

    /// Allocates up to `count` frames from the specific UMEM partition.
    /// Returns a vector of frame tokens.
    pub fn allocate_batch(&mut self, umem_id: usize, count: usize) -> Vec<FrameToken> {
        if let Some(registry) = self.registry_map.get_mut(umem_id).and_then(Option::as_mut) {
            let mut frame_tokens = Vec::with_capacity(count);
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, count);

            for _ in 0..to_take {
                let frame_token = stash.pop().unwrap();
                frame_tokens.push(frame_token);

                let refcount_idx = frame_token.frame_index() - registry.refcount_start;
                registry.refcounts[refcount_idx as usize] = 1;
            }

            if frame_tokens.is_empty() {
                log::trace!(
                    "Worker {}: Starvation! No frames available in UMEM {}",
                    self.worker_id,
                    umem_id
                );
            }

            frame_tokens
        } else {
            log::error!(
                "Worker {} tried to alloc from unassigned UMEM {}",
                self.worker_id,
                umem_id
            );
            Vec::new()
        }
    }

    /// Get total number of free frames across all UMEM partitions
    pub fn total_free_frames(&self) -> usize {
        self.active_umem_ids
            .iter()
            .map(|&umem_id| {
                self.registry_map[umem_id]
                    .as_ref()
                    .map_or(0, |partition| partition.free_stash.len())
            })
            .sum()
    }

    /* Disabled legacy API; retained for reference while allocation uses caller-provided slots.
        /// Allocates frames and creates PDUs for them
        ///
        /// # Arguments
        /// * `umem_id` - The UMEM group ID to allocate from
        /// * `count` - Number of PDUs to create
        ///
        /// # Returns
        /// A vector of PDUs ready to be filled with packet data
        pub fn allocate_pdus(&mut self, umem_id: usize, count: usize) -> Vec<crate::interface::Pdu> {
            use crate::interface::Pdu;
            use xsk_core::FrameDesc;

            let umem_config = if let Some(config) = self.umem_configs[umem_id].as_ref() {
                config.clone()
            } else {
                log::error!(
                    "Worker {} tried to allocate PDUs from unknown UMEM {}",
                    self.worker_id,
                    umem_id
                );
                return Vec::new();
            };

            let frame_tokens = self.allocate_batch(umem_id, count);
            if frame_tokens.is_empty() {
                return Vec::new();
            }

            let active_umem = match self.active_umems[umem_id].as_ref() {
                Some(handle) => handle,
                None => {
                    log::error!(
                        "Worker {} lacks physical memory mapping for UMEM {}",
                        self.worker_id,
                        umem_id
                    );
                    return Vec::new();
                }
            };

            let _umem_base_ptr = active_umem.as_ptr();
            let system_bus = &*self.bus as *const SystemBus;
            let frame_size_log2 = umem_config.frame_size_log2;
            let headroom = umem_config.head_room as u64;

            let mut pdus = Vec::with_capacity(frame_tokens.len());

            for &frame_token in &frame_tokens {
                let local_idx = frame_token.frame_index();
                let frame_addr = (local_idx as u64) << frame_size_log2;
                let desc = FrameDesc::new(frame_addr + headroom);

                let pdu = Pdu::new(
                    desc,
                    frame_token.umem_id(),
                    self.worker_id as u8,
                    system_bus,
                    frame_size_log2,
                );

                pdus.push(pdu);
            }

            pdus
        }
    */

    /// Allocates up to `dst.len()` frames from the specific UMEM partition and creates PDUs.
    /// Writes the PDUs directly into the provided slice.
    /// Returns the number of PDUs successfully allocated.
    pub fn allocate_pdus_into(&mut self, umem_id: usize, dst: &mut [MaybeUninit<Pdu>]) -> usize {
        use crate::interface::Pdu;
        use xsk_core::FrameDesc;

        // Extract values before the mutable borrow in allocate_batch_into
        let (_umem_base_ptr, frame_size_log2, headroom) = {
            let umem_config = if let Some(config) = self.umem_configs[umem_id].as_ref() {
                config
            } else {
                log::error!(
                    "Worker {} tried to allocate PDUs from unknown UMEM {}",
                    self.worker_id,
                    umem_id
                );
                return 0;
            };

            let active_umem = match self.active_umems[umem_id].as_ref() {
                Some(handle) => handle,
                None => {
                    log::error!(
                        "Worker {} lacks physical memory mapping for UMEM {}",
                        self.worker_id,
                        umem_id
                    );
                    return 0;
                }
            };

            (
                active_umem.as_ptr(),
                umem_config.frame_size_log2,
                umem_config.head_room as u64,
            )
        };

        // STACK ALLOCATION: zero overhead, stays in L1 cache
        #[repr(align(64))]
        struct AlignedBatch([FrameToken; MAX_BATCH_SIZE]);
        let mut aligned_buffer = AlignedBatch([FrameToken::new(0, 0); MAX_BATCH_SIZE]);
        let frame_tokens = &mut aligned_buffer.0;

        let req_len = std::cmp::min(dst.len(), MAX_BATCH_SIZE);
        let allocated = self.allocate_batch_into(umem_id, &mut frame_tokens[..req_len]);

        if allocated == 0 {
            return 0;
        }

        let system_bus = &*self.bus as *const SystemBus;

        let dst_slice = &mut dst[..allocated];
        let src_slice = &frame_tokens[..allocated];

        for (dst_slot, &frame_token) in dst_slice.iter_mut().zip(src_slice.iter()) {
            let local_idx = frame_token.frame_index();
            let frame_addr = (local_idx as u64) << frame_size_log2;
            let desc = FrameDesc::new(frame_addr + headroom);

            dst_slot.write(Pdu::new(
                desc,
                frame_token.umem_id(),
                self.worker_id as u8,
                system_bus,
                frame_size_log2,
            ));
        }

        allocated
    }

    /// Pre-faults all frames in the registry to eliminate demand paging latency
    pub fn warm_up_umem_partitions(&mut self) {
        for partition_data in self.registry_map.iter().flatten() {
            // Iterate over all frames in the free stash
            // Use volatile writes to prevent the compiler from optimizing the loop away
            for &frame_token in &partition_data.free_stash {
                let umem_id = frame_token.umem_id() as usize;
                let local_idx = frame_token.frame_index();

                let Some(umem_config) = self.umem_configs.get(umem_id).and_then(Option::as_ref)
                else {
                    debug!(
                        "Worker {} cannot warm up UMEM {}: no runtime configuration",
                        self.worker_id, umem_id
                    );
                    continue;
                };

                let Some(umem) = self.active_umems.get(umem_id).and_then(Option::as_ref) else {
                    debug!(
                        "Worker {} cannot warm up UMEM {}: no active mapping",
                        self.worker_id, umem_id
                    );
                    continue;
                };

                // Calculate physical UMEM offset
                let offset = ((local_idx as u64) << umem_config.frame_size_log2)
                    + umem_config.head_room as u64;

                unsafe {
                    let frame_ptr = umem.as_ptr().cast_mut().add(offset as usize);
                    core::ptr::write_volatile(frame_ptr, 0x00);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_token_command_bit_preserves_identity_and_size() {
        let token = FrameToken::new(17, u32::MAX);
        let queued_retain = token.with_retain_command();

        assert_eq!(core::mem::size_of::<FrameToken>(), 8);
        assert_eq!(token.umem_id(), 17);
        assert!(!token.is_retain_command());
        assert_eq!(queued_retain.umem_id(), 17);
        assert_eq!(queued_retain.frame_index(), u32::MAX);
        assert!(queued_retain.is_retain_command());
        assert_eq!(queued_retain.without_command(), token);
    }

    #[test]
    fn queued_retain_command_updates_refcount() {
        let partition = UmemPartition {
            partition_id: 0,
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 1,
            local_start: 0,
            local_end: 1,
        };
        let queue = Arc::new(ArrayQueue::new(8));
        let config = Arc::new(RuntimeUmemConfig {
            group_id: 0,
            frame_size: 2048,
            frame_size_log2: 11,
            frame_count: 1,
            head_room: 128,
            fq_size: 1,
            cq_size: 1,
            group_start_frame_idx: 0,
            partitions: 1,
        });
        let partitions = [partition];
        let bus = Arc::new(SystemBus {
            bus_senders: vec![queue.clone()],
            partition_directory: SystemBus::partition_directory(&partitions),
            frame_partition_maps: SystemBus::frame_partition_maps(&partitions, &[1], &[1]),
            umem_addr: [None; MAX_UMEM],
        });
        let mut registry = FrameRegistry::new(
            0,
            vec![(0, partition)],
            queue.clone(),
            bus,
            vec![Some(config)],
        );

        let frame_token = registry.allocate_batch(0, 1)[0];
        assert_eq!(registry.total_free_frames(), 0);

        queue
            .push(frame_token.with_retain_command())
            .expect("retain command should fit in the test queue");
        registry.process_returns();

        registry.release_local(frame_token);
        assert_eq!(registry.total_free_frames(), 0);
        registry.release_local(frame_token);
        assert_eq!(registry.total_free_frames(), 1);
    }
}
