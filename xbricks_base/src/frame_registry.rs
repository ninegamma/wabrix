use std::sync::Arc;
use core::cell::Cell;
use core::mem::MaybeUninit;
use crossbeam_queue::ArrayQueue;
use xsk_rs::Umem;
use log::debug;
use crate::{RuntimeUmemConfig, SystemBus, UmemPartition};
use crate::interface::batch::MAX_BATCH_SIZE;
use crate::interface::Pdu;

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

const LOCAL_IDX_MASK: u32 = 0x00FF_FFFF;

/// Capacity of each worker's return queue.
/// Must be large enough to absorb cross-worker frame returns without dropping.
pub const RETURN_QUEUE_CAPACITY: usize = 65536;

/// Command bit packed into the high bit of a return-queue u32.
/// 0 = Release (decrement refcount, free if zero), 1 = Retain (increment refcount).
/// Bits 30-24 = umem_id (7 bits, max 128 UMEMs), bits 23-0 = local_idx.
pub const CMD_BIT: u32 = 0x8000_0000;

/// Returns true if the packed command is a Retain.
#[inline(always)]
pub fn is_retain(cmd: u32) -> bool {
    (cmd & CMD_BIT) != 0
}

/// Strips the command bit to recover the plain global frame index.
#[inline(always)]
pub fn extract_global_idx(cmd: u32) -> u32 {
    cmd & !CMD_BIT
}

/// Maximum number of distinct UMEM groups supported.
/// UMEM IDs occupy bits 24-30 of the encoded frame index (7 bits), so at most 128 groups.
pub const MAX_UMEM: usize = 128;

/// Packs a UMEM ID and a UMEM-relative local frame index into a single 32-bit global index.
/// Bits 30-24 = umem_id (7 bits), bits 23-0 = local_idx (0-based from UMEM mmap start).
/// Bit 31 is reserved for the return-queue command bit and is always 0 here.
/// local_idx maps directly to: frame_addr = local_idx << frame_size_log2
#[inline(always)]
pub fn encode_global_frame(umem_id: usize, local_idx: u32) -> u32 {
    debug_assert!(umem_id < 128, "UMEM ID exceeds 7 bits!");
    debug_assert!(local_idx <= LOCAL_IDX_MASK, "Local index exceeds 24 bits!");

    ((umem_id as u32) << 24) | (local_idx & LOCAL_IDX_MASK)
}

/// Unpacks a 32-bit global index into (umem_id, local_idx).
/// Masks off bit 31 (the command bit) so this is safe to call on packed queue values too.
/// local_idx is UMEM-relative: frame_addr = local_idx << frame_size_log2
#[inline(always)]
pub fn decode_global_frame(global_idx: u32) -> (usize, u32) {
    let umem_id = ((global_idx & 0x7F00_0000) >> 24) as usize;
    let local_idx = global_idx & LOCAL_IDX_MASK;

    (umem_id, local_idx)
}


/// Per-UMEM partition data within a worker's registry.
/// `free_stash` holds encoded global frame indices (encode_global_frame format).
/// `refcounts` is indexed directly by UMEM-relative local_idx — no subtraction needed.
pub struct PartitionData {
    /// Indexed by UMEM-relative local_idx. Sized to the full UMEM frame count so that
    /// local_idx (from decode_global_frame) is a direct array index with no arithmetic.
    refcounts: Vec<u16>,
    /// Encoded global frame indices available for allocation.
    free_stash: Vec<u32>,
}

/// Per-worker frame registry information
/// One registry per worker thread, shared by all sockets in that worker
/// Supports multiple UMEM groups per worker as per Multi-Umem Static Partitioning model
pub struct FrameRegistry {
    pub worker_id: usize,
    /// Indexed by umem_id (0..MAX_UMEM). None = this worker has no partition in that UMEM.
    pub registry_map: Vec<Option<PartitionData>>,
    /// Lockless return queue: other threads push packed u32 commands; this worker pops them.
    /// Packed format: bit 31 = command (0=Release, 1=Retain), bits 30-24 = umem_id, bits 23-0 = local_idx.
    return_queue: Arc<ArrayQueue<u32>>,
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
        return_queue: Arc<ArrayQueue<u32>>,
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

            // Convert absolute partition range to UMEM-relative local indices.
            let local_start = partition.start_idx - group_start;
            let local_end   = partition.end_idx   - group_start;
            let partition_size = (local_end - local_start) as usize;

            // Pre-encode every frame in the partition.
            // Encoding: (umem_group_id << 24) | local_idx
            // decode_global_frame(encoded).1 == local_idx, so refcounts[local_idx] is a
            // direct array access without any further arithmetic.
            let free_stash: Vec<u32> = (local_start..local_end)
                .map(|local_idx| encode_global_frame(umem_group_id, local_idx))
                .collect();

            debug!(
                "  Worker {}: UMEM group {} partition - {} frames, local range [{}, {}), \
                 refcounts sized to full UMEM ({})",
                worker_id, umem_group_id, partition_size, local_start, local_end, frame_count
            );

            registry_map[umem_group_id] = Some(PartitionData {
                // Size to the full UMEM so local_idx (from decode_global_frame) is a
                // direct zero-overhead index — no subtraction from partition.start_idx.
                refcounts: vec![0u16; frame_count],
                free_stash,
            });
        }

        Self {
            worker_id,
            registry_map,
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
            if let Some(packed_cmd) = self.return_queue.pop() {
                let global_idx = extract_global_idx(packed_cmd);

                if is_retain(packed_cmd) {
                    // O(1): decode umem_id and local_idx directly
                    let (umem_id, local_idx) = decode_global_frame(global_idx);

                    if let Some(partition_data) = self.registry_map.get_mut(umem_id).and_then(|p| p.as_mut()) {
                        unsafe {
                            // SAFETY: local_idx is bounds-checked by the stash initializer
                            *partition_data.refcounts.get_unchecked_mut(local_idx as usize) += 1;
                        }
                    }
                } else {
                    // Note: Removed the inner loop `trace!` macro to save cycles.
                    // String interpolation inside a hot loop is dangerous even if disabled.
                    self.release_local(global_idx);
                }

                processed_count += 1;

            } else {
                // 🚨 CRITICAL: The queue is empty. Break the loop! 🚨
                break;
            }
        }

        if processed_count > 0 && log::log_enabled!(log::Level::Trace) {
            let total_free: usize = self.registry_map.iter().filter_map(|e| e.as_ref()).map(|p| p.free_stash.len()).sum();
            log::trace!("Worker {}: Processed {} frame commands, total free frames: {}", self.worker_id, processed_count, total_free);
        }
    }

    /// Releases a frame back to the worker's frame registry.
    ///
    /// # Parameters
    /// * `global_idx` - Encoded global frame index: (umem_id << 24) | local_idx.
    ///   The umem_id selects the partition; local_idx is the direct refcounts array index.
    #[inline]
    pub fn release_local(&mut self, global_idx: u32) {
        // 1. O(1) decode: 1 shift + 1 mask
        let (umem_id, local_idx) = decode_global_frame(global_idx);

        // 2. Direct Vec index — O(1) with no hash computation
        if let Some(partition_data) = self.registry_map[umem_id].as_mut() {
            let idx = local_idx as usize;

            unsafe {
                // SAFETY: local_idx is guaranteed to be within UMEM frame bounds
                // because it is mapped 1:1 during stash initialization.
                let refcount = partition_data.refcounts.get_unchecked_mut(idx);

                if *refcount == 0 {
                    log::error!("🔥 CRITICAL: Prevented Double-Free of frame local_idx={} in UMEM {}!", idx, umem_id);
                    return;
                }

                *refcount -= 1;
                if *refcount == 0 {
                    partition_data.free_stash.push(global_idx);
                }
            }
        } else {
            log::error!("Worker {}: partition data not found for UMEM group {}!", self.worker_id, umem_id);
        }
    }


    /// Allocates up to `dst.len()` frames from the specific UMEM partition.
    /// Writes encoded global indices directly into the provided slice.
    /// Returns the number of frames successfully allocated.
    pub fn allocate_batch_into(&mut self, umem_id: usize, dst: &mut [u32]) -> usize {
        if let Some(registry) = self.registry_map[umem_id].as_mut() {
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
            // decode_global_frame gives local_idx directly — no subtraction.
            for &frame_idx in &dst[..to_take] {
                let (_umem_id, local_idx) = decode_global_frame(frame_idx);
                unsafe {
                    *registry.refcounts.get_unchecked_mut(local_idx as usize) = 1;
                }
            }

            stash.truncate(start_idx);

            to_take
        } else {
            log::error!("Worker {} tried to alloc from unassigned UMEM {}", self.worker_id, umem_id);
            0
        }
    }

    /// Allocates up to `count` frames from the specific UMEM partition.
    /// Returns a vector of encoded global indices.
    pub fn allocate_batch(&mut self, umem_id: usize, count: usize) -> Vec<u32> {
        if let Some(registry) = self.registry_map[umem_id].as_mut() {
            let mut frames = Vec::with_capacity(count);
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, count);

            for _ in 0..to_take {
                let frame_idx = stash.pop().unwrap();
                frames.push(frame_idx);

                // Direct array access — no subtraction from partition.start_idx.
                let (_umem_id, local_idx) = decode_global_frame(frame_idx);
                registry.refcounts[local_idx as usize] = 1;
            }

            if frames.is_empty() {
                log::trace!(
                    "Worker {}: Starvation! No frames available in UMEM {}",
                    self.worker_id,
                    umem_id
                );
            }

            frames
        } else {
            log::error!("Worker {} tried to alloc from unassigned UMEM {}", self.worker_id, umem_id);
            Vec::new()
        }
    }

    /// Get total number of free frames across all UMEM partitions
    pub fn total_free_frames(&self) -> usize {
        self.registry_map.iter().filter_map(|e| e.as_ref()).map(|p| p.free_stash.len()).sum()
    }

    /// Allocates frames and creates PDUs for them
    ///
    /// # Arguments
    /// * `umem_id` - The UMEM group ID to allocate from
    /// * `count` - Number of PDUs to create
    ///
    /// # Returns
    /// A vector of PDUs ready to be filled with packet data
    pub fn allocate_pdus(&mut self, umem_id: usize, count: usize) -> Vec<crate::interface::Pdu> {
        use xsk_rs::FrameDesc;
        use crate::interface::Pdu;

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

        let frame_indices = self.allocate_batch(umem_id, count);
        if frame_indices.is_empty() {
            return Vec::new();
        }

        let active_umem = match self.active_umems[umem_id].as_ref() {
            Some(handle) => handle,
            None => {
                log::error!("Worker {} lacks physical memory mapping for UMEM {}", self.worker_id, umem_id);
                return Vec::new();
            }
        };

        let _umem_base_ptr = active_umem.as_ptr();
        let system_bus = &*self.bus as *const SystemBus;
        let frame_size_log2 = umem_config.frame_size_log2;
        let headroom = umem_config.head_room as usize;

        let mut pdus = Vec::with_capacity(frame_indices.len());

        for &frame_idx in &frame_indices {
            // local_idx is UMEM-relative: frame_addr = local_idx << frame_size_log2
            let (_umem_id, local_idx) = decode_global_frame(frame_idx);
            let frame_addr = (local_idx as usize) << frame_size_log2;
            let desc = FrameDesc::new(frame_addr + headroom);

            let pdu = Pdu::new(
                desc,
                frame_idx,
                self.worker_id as u8,
                system_bus,
                frame_size_log2,
            );

            pdus.push(pdu);
        }

        pdus
    }

    /// Allocates up to `dst.len()` frames from the specific UMEM partition and creates PDUs.
    /// Writes the PDUs directly into the provided slice.
    /// Returns the number of PDUs successfully allocated.
    pub fn allocate_pdus_into(&mut self, umem_id: usize, dst: &mut [MaybeUninit<Pdu>]) -> usize {
        use xsk_rs::FrameDesc;
        use crate::interface::Pdu;

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
                    log::error!("Worker {} lacks physical memory mapping for UMEM {}", self.worker_id, umem_id);
                    return 0;
                }
            };

            (
                active_umem.as_ptr(),
                umem_config.frame_size_log2,
                umem_config.head_room as usize,
            )
        };

        // STACK ALLOCATION: zero overhead, stays in L1 cache
        #[repr(align(64))]
        struct AlignedBatch([u32; MAX_BATCH_SIZE]);
        let mut aligned_buffer = AlignedBatch([0u32; MAX_BATCH_SIZE]);
        let global_frame_indices = &mut aligned_buffer.0;

        let req_len = std::cmp::min(dst.len(), MAX_BATCH_SIZE);
        let allocated = self.allocate_batch_into(umem_id, &mut global_frame_indices[..req_len]);

        if allocated == 0 {
            return 0;
        }

        let system_bus = &*self.bus as *const SystemBus;

        let dst_slice = &mut dst[..allocated];
        let src_slice = &global_frame_indices[..allocated];

        for (dst_slot, &global_frame_idx) in dst_slice.iter_mut().zip(src_slice.iter()) {
            // local_idx is UMEM-relative: frame_addr = local_idx << frame_size_log2
            // No group_start subtraction needed — the encoding carries that information.
            let (_umem_id, local_idx) = decode_global_frame(global_frame_idx);
            let frame_addr = (local_idx as usize) << frame_size_log2;
            let desc = FrameDesc::new(frame_addr + headroom);

            dst_slot.write(Pdu::new(
                desc,
                global_frame_idx,
                self.worker_id as u8,
                system_bus,
                frame_size_log2,
            ));
        }

        allocated
    }
}
