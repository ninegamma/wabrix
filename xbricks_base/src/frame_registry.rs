use std::collections::HashMap;
use std::sync::Arc;
use core::cell::Cell;
use core::mem::MaybeUninit;
use crossbeam_channel::Receiver;
use xsk_rs::Umem;
use log::{debug, trace};
use xbricks_protocol::wbatch::MAX_BATCH_SIZE;
use crate::{BusCommand, RuntimeUmemConfig, SystemBus, UmemPartition};
use crate::interface::Pdu;

// Thread-local storage for the worker's frame registry pointer
// This allows XSocketRx/Tx to access the worker's registry without storing it in each socket
thread_local! {
    static WORKER_FRAME_REGISTRY: Cell<*mut FrameRegistry> = Cell::new(std::ptr::null_mut());
    static TSC_FREQUENCY: Cell<u64> = Cell::new(0);
    static TSC_START: Cell<u64> = Cell::new(0);
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


/// Per-UMEM partition data within a worker's registry
pub struct PartitionData {
    partition: UmemPartition,
    refcounts: Vec<u16>,
    free_stash: Vec<u32>,
}

/// Per-worker frame registry information
/// One registry per worker thread, shared by all sockets in that worker
/// Supports multiple UMEM groups per worker as per Multi-Umem Static Partitioning model
pub struct FrameRegistry {
    pub worker_id: usize,
    /// Map: umem_group_id -> PartitionData
    /// Each worker can own partitions from multiple UMEM groups
    pub registry_map: HashMap<usize, PartitionData>,
    return_rx: Receiver<BusCommand>,
    /// System bus for frame lifecycle management
    pub bus: Arc<SystemBus>,
    /// Map: umem_group_id -> RuntimeUmemConfig
    pub umem_configs: HashMap<usize, Arc<RuntimeUmemConfig>>,
    // Add this: Runtime state (the physical memory mappings)
    pub active_umems: HashMap<usize, Arc<Umem>>,
}

impl FrameRegistry {
    /// Create a new FrameRegistry for a worker with multiple UMEM partitions
    /// partitions: Vec of (umem_group_id, UmemPartition) tuples
    pub fn new(
        worker_id: usize,
        partitions: Vec<(usize, UmemPartition)>,
        return_rx: Receiver<BusCommand>,
        bus: Arc<SystemBus>,
        umem_configs: HashMap<usize, Arc<RuntimeUmemConfig>>,
    ) -> Self {
        let mut partition_map = HashMap::new();

        debug!(
            "Creating FrameRegistry for worker {} with {} UMEM partition(s)",
            worker_id,
            partitions.len()
        );

        for (umem_group_id, partition) in partitions {
            let size = (partition.end_idx - partition.start_idx) as usize;
            // Indices are Global relative to the Group.
            // But since groups are isolated, index 0 in Group A is NOT index 0 in Group B.
            // We use absolute indices relative to the specific UMEM start.
            let free_stash: Vec<u32> = (partition.start_idx..partition.end_idx).collect();

            debug!(
                "  Worker {}: UMEM group {} partition - size: {} frames, range: [{}, {})",
                worker_id, umem_group_id, size, partition.start_idx, partition.end_idx
            );

            partition_map.insert(
                umem_group_id,
                PartitionData {
                    partition,
                    refcounts: vec![0; size],
                    free_stash,
                },
            );
        }

        Self {
            worker_id,
            registry_map: partition_map,
            return_rx,
            bus,
            umem_configs,
            active_umems: HashMap::new(),
        }
    }

    /// Find which UMEM group owns a given frame index
    pub fn find_umem_group_for_frame(&self, frame_idx: u32) -> Option<usize> {
        for (umem_group_id, partition_data) in &self.registry_map {
            if frame_idx >= partition_data.partition.start_idx && frame_idx < partition_data.partition.end_idx {
                return Some(*umem_group_id);
            }
        }
        None
    }

    #[inline]
    pub fn process_returns(&mut self) {
        let mut returned_count = 0;
        while let Ok(cmd) = self.return_rx.try_recv() {
            match cmd {
                BusCommand::Release(frame_idx) => {
                    returned_count += 1;
                    trace!("process_returns: Frame {} returned to registry", frame_idx);
                    self.release_local(frame_idx);
                }
                BusCommand::Retain(frame_idx) => {
                    // Find which partition owns this frame
                    if let Some(umem_group_id) = self.find_umem_group_for_frame(frame_idx) {
                        if let Some(partition_data) = self.registry_map.get_mut(&umem_group_id) {
                            let offset = (frame_idx - partition_data.partition.start_idx) as usize;
                            if offset < partition_data.refcounts.len() {
                                partition_data.refcounts[offset] += 1;
                            }
                        }
                    }
                }
            }
        }
        if returned_count > 0 {
            let total_free: usize = self.registry_map.values().map(|p| p.free_stash.len()).sum();
            log::trace!(
                "Worker {}: Processed {} frame returns, total free frames across all UMEMs: {}",
                self.worker_id,
                returned_count,
                total_free
            );
        }
    }

    /// Releases a frame back to the worker's frame registry.
    ///
    /// # Parameters
    /// * `frame_idx` - Global frame index across the entire system. This is an absolute frame index
    ///   that uniquely identifies a frame across all UMEM groups. Frame indices are sequential
    ///   starting from 0 for the first UMEM group and continuing through all groups.
    ///   For example, if UMEM 0 has 4096 frames, then UMEM 1 starts at frame_idx 4096.
    ///
    /// The method automatically determines which UMEM group owns this frame and releases it
    /// to the appropriate partition's free_stash.
    #[inline]
    pub fn release_local(&mut self, frame_idx: u32) {
        // Find which partition owns this frame
        if let Some(umem_group_id) = self.find_umem_group_for_frame(frame_idx) {
            if let Some(partition_data) = self.registry_map.get_mut(&umem_group_id) {
                let offset = (frame_idx - partition_data.partition.start_idx) as usize;
                if offset >= partition_data.refcounts.len() {
                    return;
                }

                // THE SHIELD: If refcount is already 0, it is ALREADY in the free stash!
                if partition_data.refcounts[offset] == 0 {
                    log::error!("🔥 CRITICAL: Prevented Double-Free of UMEM frame {} in UMEM group {}!", frame_idx, umem_group_id);
                    return; // Ignore the duplicate release entirely
                }
                partition_data.refcounts[offset] -= 1;

                if partition_data.refcounts[offset] == 0 {
                    partition_data.free_stash.push(frame_idx);
                }
            } else {
                log::error!("Worker {}: partition data not found for UMEM group {}!", self.worker_id, umem_group_id);
            }
        } else {
            log::error!("Worker {} tried to release frame {} from unknown UMEM group!", self.worker_id, frame_idx);
        }
    }

    /// Allocates up to `dst.len()` frames from the specific UMEM partition.
    /// Writes the GLOBAL indices directly into the provided slice.
    /// Returns the number of frames successfully allocated.
    pub fn allocate_batch_into(&mut self, umem_id: usize, dst: &mut [u32]) -> usize {
        if let Some(registry) = self.registry_map.get_mut(&umem_id) {
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, dst.len());

            if to_take == 0 {
                log::trace!(
                    "Worker {}: Starvation! No frames available in UMEM {}",
                    self.worker_id,
                    umem_id
                );
                return 0;
            }

            // FAST PATH: Bulk memory copy!
            // Instead of a slow `for` loop pushing one by one, we use a highly
            // optimized slice copy to grab the exact chunk we need in a single CPU instruction,
            // and then truncate the stash to remove them.
            let start_idx = available - to_take;
            dst[..to_take].copy_from_slice(&stash[start_idx..]);
            // Mark all allocated frames as actively owned (refcount = 1)
            for &frame_idx in &dst[..to_take] {
                let offset = (frame_idx - registry.partition.start_idx) as usize;
                registry.refcounts[offset] = 1;
            }

            stash.truncate(start_idx);

            to_take
        } else {
            log::error!("Worker {} tried to alloc from unassigned UMEM {}", self.worker_id, umem_id);
            0
        }
    }

    /// Allocates up to `count` frames from the specific UMEM partition.
    /// Returns a vector of GLOBAL indices.
    pub fn allocate_batch(&mut self, umem_id: usize, count: usize) -> Vec<u32> {
        if let Some(registry) = self.registry_map.get_mut(&umem_id) {
            let mut frames = Vec::with_capacity(count);
            let stash = &mut registry.free_stash;

            let available = stash.len();
            let to_take = std::cmp::min(available, count);

            for _ in 0..to_take {
                // Safety: we checked len() via to_take
                let frame_idx = stash.pop().unwrap();
                frames.push(frame_idx);

                // Mark the allocated frame as actively owned (refcount = 1)
                let offset = (frame_idx - registry.partition.start_idx) as usize;
                registry.refcounts[offset] = 1;
            }

            // Optional: Trace if we are running low/empty
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
        self.registry_map.values().map(|p| p.free_stash.len()).sum()
    }

    /// Allocates frames and creates PDUs for them
    ///
    /// # Arguments
    /// * `umem_id` - The UMEM group ID to allocate from
    /// * `count` - Number of PDUs to create
    ///
    /// # Returns
    /// A vector of PDUs ready to be filled with packet data
    ///
    /// # Notes
    /// Uses the RuntimeUmemConfig and SystemBus stored in the FrameRegistry
    pub fn allocate_pdus(&mut self, umem_id: usize, count: usize) -> Vec<crate::interface::Pdu> {
        use xsk_rs::FrameDesc;
        use crate::interface::Pdu;

        // Get the UMEM config
        let umem_config = if let Some(config) = self.umem_configs.get(&umem_id) {
            config.clone()
        } else {
            log::error!(
                "Worker {} tried to allocate PDUs from unknown UMEM {}",
                self.worker_id,
                umem_id
            );
            return Vec::new();
        };

        let group_start_frame_idx = umem_config.group_start_frame_idx;

        // Allocate frame indices
        let frame_indices = self.allocate_batch(umem_id, count);

        if frame_indices.is_empty() {
            return Vec::new();
        }

        let active_umem = match self.active_umems.get(&umem_id) {
            Some(handle) => handle,
            None => {
                log::error!("Worker {} lacks physical memory mapping for UMEM {}", self.worker_id, umem_id);
                return Vec::new();
            }
        };

        // Get UMEM base pointer and system bus from stored data
        let umem_base_ptr = active_umem.as_ptr();
        let system_bus = &*self.bus as *const SystemBus;
        let frame_size_log2 = umem_config.frame_size_log2;

        // Create PDUs for each allocated frame
        let mut pdus = Vec::with_capacity(frame_indices.len());

        for &frame_idx in &frame_indices {
            // Calculate frame address (local index * frame_size)
            let local_idx = frame_idx - group_start_frame_idx;
            let frame_addr = (local_idx as usize) << frame_size_log2;

            // Create frame descriptor with headroom offset
            // The address in FrameDesc should point to the start of the data area (after headroom)
            let desc = FrameDesc::new(frame_addr + umem_config.head_room as usize);

            // Create PDU
            // Must use GLOBAL frame_idx so PDU can be properly released later
            let pdu = Pdu::new(
                desc,
                frame_idx,
                self.worker_id as u8,
                umem_base_ptr,
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
    ///
    /// # Arguments
    /// * `umem_id` - The UMEM group ID to allocate from
    /// * `dst` - Slice to write PDUs into
    ///
    /// # Returns
    /// Number of PDUs successfully allocated and written to dst
    ///
    /// # Notes
    /// Uses the RuntimeUmemConfig and SystemBus stored in the FrameRegistry
    pub fn allocate_pdus_into(&mut self, umem_id: usize, dst: &mut [MaybeUninit<Pdu>]) -> usize {
        use xsk_rs::FrameDesc;
        use crate::interface::Pdu;

        // Extract values we need before the mutable borrow in allocate_batch_into
        // This avoids borrow checker conflicts
        let (umem_base_ptr, frame_size_log2, headroom, group_start_frame_idx) = {
            // Get the UMEM config
            let umem_config = if let Some(config) = self.umem_configs.get(&umem_id) {
                config
            } else {
                log::error!(
                    "Worker {} tried to allocate PDUs from unknown UMEM {}",
                    self.worker_id,
                    umem_id
                );
                return 0;
            };



            let group_start_frame_idx = umem_config.group_start_frame_idx;

            let active_umem = match self.active_umems.get(&umem_id) {
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
                group_start_frame_idx,
            )
        };

        // STACK ALLOCATION (Zero Overhead)
        // Allocate the temporary buffer directly on the CPU stack.
        // This costs 0 CPU cycles and stays inside the L1 cache.

        let mut global_frame_indices = [0u32; MAX_BATCH_SIZE];

        // Ensure we don't ask for more than our stack array can hold
        let req_len = std::cmp::min(dst.len(), MAX_BATCH_SIZE);

        // Pass a slice of our stack array to the allocator
        let allocated = self.allocate_batch_into(umem_id, &mut global_frame_indices[..req_len]);

        if allocated == 0 {
            return 0;
        }

        // Get system bus pointer
        let system_bus = &*self.bus as *const SystemBus;

        // ZERO-COST ITERATORS (Eliminates Bounds Checking)
        // Zipping the destination slice and the source slice together guarantees to the
        // compiler that we will never go out of bounds. The compiler will strip the panic checks.
        let dst_slice = &mut dst[..allocated];
        let src_slice = &global_frame_indices[..allocated];

        for (dst_slot, &global_frame_idx) in dst_slice.iter_mut().zip(src_slice.iter()) {
            let frame_addr = ((global_frame_idx - group_start_frame_idx) as usize) << frame_size_log2;

            let desc = FrameDesc::new(frame_addr + headroom);

            dst_slot.write(Pdu::new(
                desc,
                global_frame_idx, // Must be GLOBAL frame index, not local!
                self.worker_id as u8,
                umem_base_ptr,
                system_bus,
                frame_size_log2,
            ));
        }

        allocated
    }
}
