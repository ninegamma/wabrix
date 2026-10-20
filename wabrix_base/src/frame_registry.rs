use crate::interface::Pdu;
use crate::{RuntimeUmemConfig, SystemBus, UmemPartition};
use core::cell::Cell;
use core::mem::MaybeUninit;
use crossbeam_queue::ArrayQueue;
use log::debug;
use std::sync::Arc;
use wabrix_api::MAX_BATCH_SIZE;
use xsk_core::Umem;

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

/// Number of bits reserved for the partition-relative frame offset.
pub const PARTITION_OFFSET_BITS: u32 = 23;
/// Number of addressable partition identities in a frame token.
pub const MAX_PARTITIONS: usize = 1 << 8;
/// Maximum number of frames in one partition.
pub const MAX_PARTITION_FRAMES: u32 = 1 << PARTITION_OFFSET_BITS;
const PARTITION_OFFSET_MASK: u32 = MAX_PARTITION_FRAMES - 1;

/// Capacity of each worker's return queue.
/// Must be large enough to absorb cross-worker frame returns without dropping.
pub const RETURN_QUEUE_CAPACITY: usize = 65536;

/// Command bit packed into the high bit of a return-queue u32.
/// 0 = Release (decrement refcount, free if zero), 1 = Retain (increment refcount).
/// Bits 30-23 = partition ID (8 bits), bits 22-0 = partition-relative offset.
pub const CMD_BIT: u32 = 0x8000_0000;

/// Returns true if the packed command is a Retain.
#[inline(always)]
pub fn is_retain(cmd: u32) -> bool {
    (cmd & CMD_BIT) != 0
}

/// Strips the command bit to recover the frame token.
#[inline(always)]
pub fn extract_frame_token(cmd: u32) -> u32 {
    cmd & !CMD_BIT
}

/// Maximum number of distinct UMEM groups supported by fixed UMEM-group-indexed tables.
/// This is independent of the frame-token bit layout.
pub const MAX_UMEM: usize = 128;

/// Packs a partition ID and partition-relative frame offset into a frame token.
/// Bits 30-23 = partition ID, bits 22-0 = offset within that partition.
/// Bit 31 is reserved for the return-queue command bit and is always 0 here.
#[inline(always)]
pub fn encode_frame_token(partition_id: usize, offset: u32) -> u32 {
    debug_assert!(
        partition_id < MAX_PARTITIONS,
        "partition ID exceeds 8 bits!"
    );
    debug_assert!(
        offset < MAX_PARTITION_FRAMES,
        "partition offset exceeds 23 bits!"
    );

    ((partition_id as u32) << PARTITION_OFFSET_BITS) | (offset & PARTITION_OFFSET_MASK)
}

/// Unpacks a frame token into (partition ID, partition-relative offset).
/// Masks off bit 31 (the command bit) so this is safe to call on packed queue values too.
#[inline(always)]
pub fn decode_frame_token(frame_token: u32) -> (usize, u32) {
    let partition_id = ((frame_token & !CMD_BIT) >> PARTITION_OFFSET_BITS) as usize;
    let offset = frame_token & PARTITION_OFFSET_MASK;

    (partition_id, offset)
}

/// Resolved metadata for a valid frame token.
/// local_idx maps directly to: frame_addr = local_idx << frame_size_log2
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedFrameToken {
    pub partition_id: usize,
    pub worker_id: usize,
    pub umem_group_id: usize,
    pub local_idx: u32,
}

/// Resolves a frame token through the indexed partition directory without scanning ranges.
#[inline(always)]
pub fn resolve_frame_token(
    frame_token: u32,
    directory: &[Option<UmemPartition>],
) -> Option<ResolvedFrameToken> {
    let (partition_id, offset) = decode_frame_token(frame_token);
    let entry = directory.get(partition_id)?.as_ref()?;
    let partition_len = entry.local_end.checked_sub(entry.local_start)?;
    if offset >= partition_len {
        return None;
    }

    Some(ResolvedFrameToken {
        partition_id,
        worker_id: entry.worker_id,
        umem_group_id: entry.umem_group_id,
        local_idx: entry.local_start + offset,
    })
}

/// Per-UMEM partition data within a worker's registry.
/// `free_stash` holds frame tokens (`encode_frame_token` format).
/// `refcounts` is indexed by local_idx - `refcount_start`.
pub struct PartitionData {
    /// First UMEM-relative frame represented by `refcounts`.
    refcount_start: u32,
    /// One counter per frame owned by this partition. Keeping this partition-local avoids
    /// allocating a counter for every frame in the UMEM when a worker owns only a subrange.
    refcounts: Vec<u16>,
    /// Frame tokens available for allocation.
    free_stash: Vec<u32>,
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
    /// Lockless return queue: other threads push packed u32 commands; this worker pops them.
    /// Packed format: bit 31 = command, bits 30-23 = partition ID, bits 22-0 = offset.
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

            // Convert absolute partition range to UMEM-relative local indices
            let local_start = partition.start_idx - group_start;
            let local_end = partition.end_idx - group_start;
            let partition_size = (local_end - local_start) as usize;

            // Pre-encode every frame using the stable partition ID and a local offset.
            let free_stash: Vec<u32> = (local_start..local_end)
                .map(|local_idx| {
                    encode_frame_token(partition.partition_id, local_idx - local_start)
                })
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
            if let Some(packed_cmd) = self.return_queue.pop() {
                let frame_token = extract_frame_token(packed_cmd);

                if is_retain(packed_cmd) {
                    let Some(resolved) =
                        resolve_frame_token(frame_token, &self.bus.partition_directory)
                    else {
                        log::error!(
                            "Worker {}: invalid frame token {} for retain",
                            self.worker_id,
                            frame_token
                        );
                        processed_count += 1;
                        continue;
                    };
                    let umem_id = resolved.umem_group_id;
                    let local_idx = resolved.local_idx;

                    if let Some(partition_data) =
                        self.registry_map.get_mut(umem_id).and_then(|p| p.as_mut())
                    {
                        if let Some(refcount_idx) = partition_data.refcount_index(local_idx) {
                            unsafe {
                                // SAFETY: refcount_index verifies that the index belongs to
                                // this partition and is within the compact refcounts vector.
                                *partition_data.refcounts.get_unchecked_mut(refcount_idx) += 1;
                            }
                        } else {
                            log::error!(
                                "Worker {}: retain for frame local_idx={} outside UMEM {} partition",
                                self.worker_id,
                                local_idx,
                                umem_id
                            );
                        }
                    }
                } else {
                    // Note: Removed the inner loop `trace!` macro to save cycles.
                    // String interpolation inside a hot loop is dangerous even if disabled.
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

    /// Releases a frame back to the worker's frame registry.
    ///
    /// # Parameters
    /// * `frame_token` - Frame token resolved through the system directory.
    #[inline]
    pub fn release_local(&mut self, frame_token: u32) {
        let Some(resolved) = resolve_frame_token(frame_token, &self.bus.partition_directory) else {
            log::error!(
                "Worker {}: invalid frame token {} for release",
                self.worker_id,
                frame_token
            );
            return;
        };
        let umem_id = resolved.umem_group_id;
        let local_idx = resolved.local_idx;

        // 2. Direct Vec index — O(1) with no hash computation
        if let Some(partition_data) = self.registry_map[umem_id].as_mut() {
            let Some(idx) = partition_data.refcount_index(local_idx) else {
                log::error!(
                    "Worker {}: frame local_idx={} is outside UMEM {} partition",
                    self.worker_id,
                    local_idx,
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
                        idx,
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
            for &frame_token in &dst[..to_take] {
                // Tokens in this registry's free stash encode offsets from `refcount_start`,
                // so the token offset is already the compact refcount-array index.
                let (_, refcount_idx) = decode_frame_token(frame_token);
                unsafe {
                    // SAFETY: `dst` was copied from this partition's free_stash, whose tokens
                    // are encoded from [refcount_start, refcount_start + refcounts.len()).
                    // Thus each decoded token offset is a valid refcount index.
                    *registry.refcounts.get_unchecked_mut(refcount_idx as usize) = 1;
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
    pub fn allocate_batch(&mut self, umem_id: usize, count: usize) -> Vec<u32> {
        if let Some(registry) = self.registry_map[umem_id].as_mut() {
            let mut frame_tokens = Vec::with_capacity(count);
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, count);

            for _ in 0..to_take {
                let frame_token = stash.pop().unwrap();
                frame_tokens.push(frame_token);

                let Some(local_idx) =
                    resolve_frame_token(frame_token, &self.bus.partition_directory)
                        .map(|resolved| resolved.local_idx)
                else {
                    continue;
                };
                let refcount_idx = local_idx - registry.refcount_start;
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
            // local_idx is UMEM-relative: frame_addr = local_idx << frame_size_log2
            let Some(local_idx) = resolve_frame_token(frame_token, &self.bus.partition_directory)
                .map(|resolved| resolved.local_idx)
            else {
                continue;
            };
            let frame_addr = (local_idx as u64) << frame_size_log2;
            let desc = FrameDesc::new(frame_addr + headroom);

            let pdu = Pdu::new(
                desc,
                frame_token,
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
        struct AlignedBatch([u32; MAX_BATCH_SIZE]);
        let mut aligned_buffer = AlignedBatch([0u32; MAX_BATCH_SIZE]);
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
            let Some(resolved) = resolve_frame_token(frame_token, &self.bus.partition_directory)
            else {
                continue;
            };
            let local_idx = resolved.local_idx;
            let frame_addr = (local_idx as u64) << frame_size_log2;
            let desc = FrameDesc::new(frame_addr + headroom);

            dst_slot.write(Pdu::new(
                desc,
                frame_token,
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
            // Iterate over all pre-encoded frames in the free stash
            // Use volatile writes to prevent the compiler from optimizing the loop away
            for &frame_token in &partition_data.free_stash {
                let Some(resolved) =
                    resolve_frame_token(frame_token, &self.bus.partition_directory)
                else {
                    continue;
                };
                let umem_id = resolved.umem_group_id;
                let local_idx = resolved.local_idx;

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
    fn frame_token_round_trips_boundaries_and_command_bit() {
        for (partition_id, offset) in [(0, 0), (MAX_PARTITIONS - 1, MAX_PARTITION_FRAMES - 1)] {
            let frame_token = encode_frame_token(partition_id, offset);
            assert_eq!(decode_frame_token(frame_token), (partition_id, offset));
            assert_eq!(
                decode_frame_token(frame_token | CMD_BIT),
                (partition_id, offset)
            );
            assert_eq!(extract_frame_token(frame_token | CMD_BIT), frame_token);
        }
    }

    #[test]
    fn directory_resolution_checks_partition_and_offset() {
        let directory = vec![
            None,
            Some(UmemPartition {
                partition_id: 1,
                worker_id: 4,
                umem_group_id: 2,
                start_idx: 10,
                end_idx: 14,
                local_start: 10,
                local_end: 14,
            }),
        ];

        assert_eq!(
            resolve_frame_token(encode_frame_token(1, 0), &directory),
            Some(ResolvedFrameToken {
                partition_id: 1,
                worker_id: 4,
                umem_group_id: 2,
                local_idx: 10,
            })
        );
        assert!(resolve_frame_token(encode_frame_token(1, 4), &directory).is_none());
        assert!(resolve_frame_token(encode_frame_token(2, 0), &directory).is_none());
    }
}
