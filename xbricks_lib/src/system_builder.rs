// src/system_builder.rs

use crossbeam_channel::{unbounded, Sender, Receiver};
use std::sync::Arc;
use std::collections::HashMap;
use core_affinity::CoreId;
use xsk_rs::{Umem, };
use log::{warn, debug, info, error};
use xsk_rs::config::{FrameSize, QueueSize};
//use xbricks_wasm::wasm_manager::WasmManager;
use xbricks_base::{RuntimeUmemConfig, SystemBus, UmemPartition};
use xbricks_base::frame_registry::FrameRegistry;
use crate::xsocket::{XSocket, XSocketRx, XSocketTx, XdpSocketConfig};
use crate::{BatchRx, BatchTx, MAX_BATCH_SIZE};
use xbricks_base::system::PlatformData;
#[cfg(feature = "wasm")]
use xbricks_wasm::wasm_manager::WasmManager;
use crate::system_runner::XBricksSystem;
use crate::toml_parser::{PartitionRequest, WorkerBuilderConfig};
use crate::xscrubber_skel::XscrubberSkel;

pub struct SystemBuilder {
    pub(crate) workers: Vec<WorkerBuilderConfig>,
    pub(crate) xdp_socket_configs: Vec<XdpSocketConfig>,
    pub runtime_umems: HashMap<usize, Arc<RuntimeUmemConfig>>, // umem_id to config
    #[cfg(feature = "wasm")]
    pub(crate) wasm_managers: HashMap<usize, Arc<WasmManager>>, // umem_id to wasm_manager
    pub(crate) worker_partitions: HashMap<usize, Vec<PartitionRequest>>, // worker_id to partition requests
}




impl SystemBuilder {
    /// Build the XDP socket system
    /// Builds:
    /// 1. Vec<XSocket> - Sockets for BPF registration and pipeline construction
    /// 2. Vec<FrameRegistry> - One frame registry per worker, sorted by worker id
    /// 3. XSocketRegistry - Holds Rx and Tx sides of each XSocket with keys (interface, queue_id)
    /// 4. TelemetryRegistry - Holds references to all telemetry nodes
    /// 5. WorkerRunners - Holds pipeline runners for each worker
    pub fn build(
        mut self,
        interface_skels: Option<&HashMap<String, Arc<XscrubberSkel<'static>>>>,
        platform_data: PlatformData,
    ) -> Result<Box<XBricksSystem>, Box<dyn std::error::Error>> {

        let mut xbricks_system = Box::new(XBricksSystem::default());
        xbricks_system.platform_data = platform_data;
        // 1. Initialize Channels for the System Bus
        // We need one channel per worker to allow them to return frames to each other.
        let mut bus_senders = Vec::new();
        let mut bus_receivers = Vec::new();

        for _ in 0..self.workers.len() {
            let (tx, rx) = unbounded();
            bus_senders.push(tx);
            bus_receivers.push(Some(rx)); // Wrap in Option to move out later
        }

        // system_bus is created after step 3 once umem_partitions are known

        // 2. Group XDP Configs by UMEM Group ID
        let mut umem_groups: HashMap<usize, Vec<&XdpSocketConfig>> = HashMap::new();
        for config in &self.xdp_socket_configs {
            umem_groups.entry(config.umem_config.group_id).or_default().push(config);
        }

        // 3. Calculate partition allocation for each worker
        // Map: worker_id -> Vec<(umem_group_id, UmemPartition)>
        let mut worker_partitions: HashMap<usize, Vec<(usize, UmemPartition)>> = HashMap::new();

        // Track next available partition index for each UMEM
        // Map: umem_group_id -> next_partition_index
        let mut umem_partition_counters: HashMap<usize, usize> = HashMap::new();

        // Initialize counters based on UMEM partition counts
        for (umem_id, _umem_conf) in &self.runtime_umems {
            umem_partition_counters.insert(*umem_id, 0);
        }

        // Process workers in order of their IDs for deterministic allocation
        let mut sorted_worker_ids: Vec<usize> = self.worker_partitions.keys().copied().collect();
        sorted_worker_ids.sort();

        for worker_id in &sorted_worker_ids {
            let mut worker_parts = Vec::new();

            // Get partition requests for this worker (if any)
            let partition_reqs = self.worker_partitions.get(worker_id).cloned().unwrap_or_default();

            // Process each partition request for this worker
            for partition_req in &partition_reqs {
                let umem_group_id = partition_req.umem;
                let umem_conf = self
                    .runtime_umems
                    .get(&umem_group_id)
                    .ok_or_else(|| format!("Worker {}: requested partitions from unknown UMEM {}", worker_id, umem_group_id))?;

                // Calculate partition size from usable (non-reserved) frames only
                let usable_frames = umem_conf.frame_count - umem_conf.reserved_frames;
                let partition_size = usable_frames / umem_conf.partitions;
                // Worker partitions begin after the reserved region
                let usable_start = umem_conf.group_start_frame_idx + umem_conf.reserved_frames;

                // Get the starting partition index for this allocation
                let partition_idx = umem_partition_counters
                    .get_mut(&umem_group_id)
                    .ok_or_else(|| format!("UMEM {} not initialized", umem_group_id))?;

                let first_partition_idx = *partition_idx;

                // Calculate frame range for the merged partition
                // Since partitions are consecutive, we can merge them into one larger partition
                let start_idx = usable_start + (first_partition_idx as u32 * partition_size);
                let end_idx = start_idx + (partition_req.count as u32 * partition_size);

                let partition = UmemPartition {
                    worker_id: *worker_id,
                    umem_group_id,
                    start_idx,
                    end_idx,
                };

                worker_parts.push((umem_group_id, partition));

                // Update partition counter by the number of partitions allocated
                *partition_idx += partition_req.count;

                debug!(
                    "Allocated merged partition for worker {}: UMEM {} partitions [{}-{}) covering frames [{}-{}) (reserved: [0-{}))",
                    worker_id,
                    umem_group_id,
                    first_partition_idx,
                    first_partition_idx + partition_req.count,
                    start_idx,
                    end_idx,
                    umem_conf.reserved_frames
                );
            }

            worker_partitions.insert(*worker_id, worker_parts);
        }

        // Collect all partitions into a flat Vec for determine_owner lookups
        let all_partitions: Vec<UmemPartition> = worker_partitions
            .values()
            .flat_map(|parts| parts.iter().map(|(_umem_id, p)| *p))
            .collect();

        let system_bus = Arc::new(SystemBus {
            bus_senders: bus_senders.clone(),
            umem_partitions: all_partitions,
        });

        // 4. Create FrameRegistry for each worker
        for worker_id in &sorted_worker_ids {
            // Get the partitions for this worker
            let partitions = worker_partitions.remove(worker_id).unwrap_or_else(Vec::new); // If no partitions requested, create empty registry

            // Get the receiver for this worker
            let return_rx = bus_receivers[*worker_id]
                .take()
                .ok_or_else(|| format!("No bus receiver for worker {}", worker_id))?;

            // Create the registry
            let frame_registry = FrameRegistry::new(
                *worker_id,
                partitions,
                return_rx,
                system_bus.clone(),
                self.runtime_umems.clone(),
            );
            xbricks_system.frame_registries.insert(*worker_id, frame_registry);
        }

        // Initialize UMEMs and Fill/Comp Queues
        // We store the initialized UMEM objects to be shared by sockets in the same group.
        // Map: group_id -> (Umem, FrameCount, StartIndex)
        let mut initialized_umems: HashMap<usize, (Arc<Umem>, Vec<xsk_rs::FrameDesc>)> = HashMap::new();

        // We iterate over the keys to avoid borrow checker conflicts with the values
        let group_ids: Vec<usize> = self.runtime_umems.keys().cloned().collect();

        for group_id in group_ids {
            // 1. Get the config from the builder
            let umem_conf = self.runtime_umems.get_mut(&group_id).unwrap();

            let xsk_umem_config = xsk_rs::config::UmemConfig::builder()
                .frame_size(FrameSize::new(umem_conf.frame_size)?)
                .fill_queue_size(QueueSize::try_from(umem_conf.fq_size)?)
                .comp_queue_size(QueueSize::try_from(umem_conf.cq_size)?)
                .frame_headroom(umem_conf.head_room)
                .build()
                .map_err(|e| format!("Failed to create UMEM for group {}: {}", group_id, e))?;

            // xsk-rs returns the umem and the pre-calculated descriptors
            let (umem, umem_descs) = Umem::new(
                xsk_umem_config,
                std::num::NonZeroU32::new(umem_conf.frame_count).unwrap(),
                true,
            )?;

            let shared_umem = Arc::from(umem);
            // Store UMEM for sharing (cloning Umem is cheap, it's a handle)
            initialized_umems.insert(
                group_id,
                (shared_umem, umem_descs),
            );
        }

        // UPDATE REGISTRIES WITH INITIALIZED CONFIGS ---
        // Now every worker has the SAME Arc handle that the Builder has.
        for frame_registry in xbricks_system.frame_registries.values_mut() {
            for (group_id, (umem,_ ) ) in &initialized_umems {
                // Give it the physical UMEM handle (cheap Arc clone)
                frame_registry.active_umems.insert(*group_id, umem.clone());
            }
        }

        // --- 6. CREATE SOCKETS ---
        let mut xsockets = Vec::new();
        for (group_id, (umem, umem_descs)) in &initialized_umems {
            let mut queue_ring_map = HashMap::new();
            let umem_conf = self.runtime_umems.get(group_id).unwrap();

            for config in umem_groups.get(group_id).unwrap_or(&Vec::new()).iter() {
                let ring_key = (config.interface.clone(), config.queue_id);
                let shared_rings = queue_ring_map.get(&ring_key).cloned();

                let mut initial_descs = Vec::new();
                if shared_rings.is_none() {
                    if let Some(registry) = xbricks_system.frame_registries.get_mut(&config.worker_id) {
                        let mut global_frames = vec![0u32; umem_conf.fq_size as usize];
                        let allocated = registry.allocate_batch_into(*group_id, &mut global_frames);
                        let offset_idx = umem_conf.group_start_frame_idx;

                        for i in 0..allocated {
                            let local_idx = (global_frames[i] - offset_idx) as usize;
                            initial_descs.push(umem_descs[local_idx]);
                        }
                    }
                }

                let socket_result = XSocket::new(
                    (*config).clone(),
                    system_bus.clone(),
                    umem.clone(), // This is now a clean clone of the Arc
                    shared_rings,
                    initial_descs,
                );

                let (xs, fq, cq) = match socket_result {
                    Ok(s) => {
                        // In Rust, casting a reference to a raw pointer and printing it
                        // will show you where the struct lives in memory.
                        log::debug!(
                            "Queue pointers created for Group {} -> FQ: {:p}, CQ: {:p}, RX: {:p}, TX: {:p}",
                            group_id,
                            &s.1,
                            &s.2,
                            &s.0.rx_queue, // Assuming your XSocket struct exposes rx
                            &s.0.tx_queue  // Assuming your XSocket struct exposes tx
                        );
                        s
                    },
                    Err(e) => {
                        // Grab the exact kernel errno that triggered the failure
                        let os_err = std::io::Error::last_os_error();
                        let raw_code = os_err.raw_os_error().unwrap_or(0);

                        log::error!("========================================================");
                        log::error!("🔥 FATAL: Kernel rejected AF_XDP Socket Creation!");
                        log::error!("Interface: {}, Queue: {}", config.interface, config.queue_id);
                        log::error!("xsk-rs error : {}", e);
                        log::error!("OS Error     : {} (Errno: {})", os_err, raw_code);

                        // Cheat sheet for the most common AF_XDP failures:
                        match raw_code {
                            16 => log::error!("💡 EBUSY (16): The hardware queue is STILL bound. The BPF map teardown failed or another process is using it."),
                            12 => log::error!("💡 ENOMEM (12): Out of memory! Usually means RLIMIT_MEMLOCK is too low. Run 'ulimit -l unlimited'."),
                            22 => log::error!("💡 EINVAL (22): Invalid argument. Check if the queue ID exists, or if Zerocopy is forced on an unsupported driver."),
                            95 => log::error!("💡 EOPNOTSUPP (95): Operation not supported. Driver might not support Native XDP."),
                            _  => log::error!("💡 Check standard Linux errnos for code {}", raw_code),
                        }
                        log::error!("========================================================");

                        return Err(e.into());
                    }
                };

                if !queue_ring_map.contains_key(&ring_key) {
                    queue_ring_map.insert(ring_key, (fq, cq));
                }
                xsockets.push(xs);
            }
        }
        #[cfg(feature = "wasm")]
        for (umem_id, config) in &self.runtime_umems {
            if let Some((umem, _ )) = &initialized_umems.get(umem_id) {
                let umem_conf = self.runtime_umems.get(umem_id).unwrap();
                self.wasm_managers.insert(
                    *umem_id,
                    Arc::new(WasmManager::new(umem_conf.frame_size, umem_conf.reserved_frames, umem_conf.frame_count, umem.clone())?),
                );
            } else {
                log::error!("🔥 Failed to initialize WASM Manager for UMEM {}. Umem not found.", umem_id);
            }
        }

        // Register all sockets in the BPF maps
        if interface_skels.is_some() {
            crate::config::configure_bpf::register_sockets_in_bpf(&xsockets, interface_skels.unwrap())?;
        } else {
            error!("🔥 No interface skels provided. Cannot register sockets in BPF.");
        }

        let num_sockets = xsockets.len();
        let num_workers = xbricks_system.frame_registries.len();
        info!(
            "Initialized {} XDP sockets and {} worker frame registries from configuration",
            num_sockets, num_workers
        );

        // Create socket registry for pipeline construction
        xbricks_system.xsocket_registry = crate::system_builder::XSocketRegistry::new(xsockets);
        // Create telemetry registry
        let mut telemetry_registry = crate::telemetry_logger::TelemetryRegistry::new();
        // Create dst port stats registry
        let mut dst_port_stats_registry: Vec<std::sync::Arc<crate::nodes::DstPortStats>> = Vec::new();

        // ==========================================================
        // NEW: INITIALIZE RCU SHARED SYSTEM STATE
        // ==========================================================
        use crate::config::hot_config::{SharedSystemState, HotPipelineConfig};
        use arc_swap::ArcSwap;

        let mut rcu_pipelines = HashMap::new();

        // Scan all workers to find unique pipeline names
        for worker in &self.workers {
            for pipeline in &worker.pipelines {
                if !rcu_pipelines.contains_key(&pipeline.name) {
                    // Initialize an empty RCU pointer for this pipeline
                    rcu_pipelines.insert(
                        pipeline.name.clone(),
                        Arc::new(ArcSwap::from_pointee(HotPipelineConfig::default()))
                    );
                }
            }
        }

        let shared_state = SharedSystemState::new(rcu_pipelines);

        // Attach it to the system so main.rs can retrieve it later for the Axum API
        xbricks_system.shared_state = shared_state.clone();
        // ==========================================================

        // Build pipeline runners for each worker
        for worker in self.workers {
            let worker_id = worker.id as usize;
            let num_pipelines = worker.pipelines.len();
            let core_id = CoreId {
                id: worker.core_id as usize,
            };
            debug!("Worker {}: creating {} pipeline runners", worker_id, num_pipelines);

            let mut runners = Vec::new();
            for pipeline_instance_cfg in &worker.pipelines {
                // Build the node DAG for this pipeline instance
                let nodes = crate::pipeline::PipelineFactory::build(
                    pipeline_instance_cfg,
                    worker_id,
                    &mut xbricks_system.xsocket_registry,
                    Some(&mut telemetry_registry),
                    Some(&mut dst_port_stats_registry),
                    &xbricks_system.platform_data,
                    #[cfg(feature = "wasm")]
                    &self.wasm_managers,
                    shared_state.clone(),
                )
                .map_err(|e| anyhow::anyhow!(e))?;
                // Create the pipeline runner
                let runner = crate::pipeline::PipelineRunner::new(pipeline_instance_cfg.name.clone(), worker_id, nodes)
                    .map_err(|e| anyhow::anyhow!(e))?;

                runners.push(runner);
            }
            xbricks_system.worker_runners.insert(worker_id, (core_id, runners));
        }

        xbricks_system.telemetry_registry = Arc::new(telemetry_registry);
        xbricks_system.dst_port_stats = dst_port_stats_registry;

        Ok(xbricks_system)
    }
}

/// Manager for XSockets that implements the XSocketManager trait from pipeline.rs
/// This allows pipelines to access sockets by (queue_id, interface) pairs
/// Sockets are split into RX and TX components for independent ownership
#[derive(Default)]
pub struct XSocketRegistry {
    /// Key: (interface, queue_id) -> (Option<XSocketRx>, Option<XSocketTx>)
    /// Options allow independent taking of RX and TX sides
    sockets: HashMap<(String, usize), (Option<XSocketRx>, Option<XSocketTx>)>,
}

impl XSocketRegistry {
    /// Create a new registry from a vector of XSockets
    /// Each socket is split into RX and TX components
    pub fn new(sockets: Vec<XSocket>) -> Self {
        let mut map = HashMap::new();

        for socket in sockets {
            let key = (socket.config.interface.clone(), socket.config.queue_id as usize);
            if map.contains_key(&key) {
                warn!(
                    "Duplicate socket for interface {} queue {}",
                    socket.config.interface, socket.config.queue_id
                );
            }
            let (rx, tx) = socket.split();
            map.insert(key, (Some(rx), Some(tx)));
        }

        Self { sockets: map }
    }
}

impl crate::pipeline::XSocketRxTxProvider for XSocketRegistry {
    fn take_rx_socket(&mut self, queue_id: usize, interface: &str) -> Option<Box<dyn BatchRx + Send>> {
        let key = (interface.to_string(), queue_id);
        self.sockets
            .get_mut(&key)
            .and_then(|(rx_opt, _tx_opt)| rx_opt.take())
            .map(|rx| Box::new(rx) as Box<dyn BatchRx + Send>)
    }

    fn take_tx_socket(&mut self, queue_id: usize, interface: &str) -> Option<Box<dyn BatchTx + Send>> {
        let key = (interface.to_string(), queue_id);
        self.sockets
            .get_mut(&key)
            .and_then(|(_rx_opt, tx_opt)| tx_opt.take())
            .map(|tx| Box::new(tx) as Box<dyn BatchTx + Send>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test SystemBus and configs
    fn create_test_bus_and_configs() -> (Arc<SystemBus>, HashMap<usize, Arc<RuntimeUmemConfig>>) {
        let (tx, _rx) = crossbeam_channel::bounded(10);
        let bus = Arc::new(SystemBus { bus_senders: vec![tx], umem_partitions: vec![] });

        let mut configs = HashMap::new();
        configs.insert(
            0,
            Arc::new(RuntimeUmemConfig {
                group_id: 0,
                frame_size: 2048,
                frame_size_log2: 11,
                frame_count: 1024,
                head_room: 128,
                fq_size: 512,
                cq_size: 512,
                group_start_frame_idx: 0,
                partitions: 2,
                reserved_frames: 0,
            }),
        );

        (bus, configs)
    }

    /// Test basic frame indexing with a single UMEM and single partition
    #[test]
    fn test_single_umem_single_partition() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        let partition = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 100,
        };

        let registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus, configs);

        // Check that we have 100 frames available
        assert_eq!(registry.total_free_frames(), 100);

        // Check frame ownership
        assert_eq!(registry.find_umem_group_for_frame(0), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(50), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(99), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(100), None); // Out of range
    }

    /// Test frame indexing with multiple UMEM groups
    #[test]
    fn test_multi_umem_frame_indexing() {
        let (tx, rx) = crossbeam_channel::bounded(10);

        // UMEM 0: frames 0-4095 (4096 frames)
        let partition0 = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 4096,
        };

        // UMEM 1: frames 4096-6143 (2048 frames)
        let partition1 = UmemPartition {
            worker_id: 0,
            umem_group_id: 1,
            start_idx: 4096,
            end_idx: 6144,
        };

        let (bus, mut configs) = create_test_bus_and_configs();
        // Add config for UMEM 1
        configs.insert(
            1,
            Arc::new(RuntimeUmemConfig {
                group_id: 1,
                frame_size: 2048,
                frame_size_log2: 11,
                frame_count: 2048,
                head_room: 128,
                fq_size: 512,
                cq_size: 512,
                group_start_frame_idx: 4096,
                partitions: 1,
                reserved_frames: 0,
            }),
        );

        let registry = FrameRegistry::new(0, vec![(0, partition0), (1, partition1)], rx, bus, configs);

        // Check total frames
        assert_eq!(registry.total_free_frames(), 6144);

        // Check frame ownership for UMEM 0
        assert_eq!(registry.find_umem_group_for_frame(0), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(2047), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(4095), Some(0));

        // Check frame ownership for UMEM 1
        assert_eq!(registry.find_umem_group_for_frame(4096), Some(1));
        assert_eq!(registry.find_umem_group_for_frame(5120), Some(1));
        assert_eq!(registry.find_umem_group_for_frame(6143), Some(1));

        // Check out of range
        assert_eq!(registry.find_umem_group_for_frame(6144), None);
    }

    /// Test frame allocation from specific UMEM group
    #[test]
    fn test_allocate_from_specific_umem() {
        let (tx, rx) = crossbeam_channel::bounded(10);

        let partition0 = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 100,
        };

        let partition1 = UmemPartition {
            worker_id: 0,
            umem_group_id: 1,
            start_idx: 100,
            end_idx: 200,
        };

        let (bus, mut configs) = create_test_bus_and_configs();
        // Add config for UMEM 1
        configs.insert(
            1,
            Arc::new(RuntimeUmemConfig {
                group_id: 1,
                frame_size: 2048,
                frame_size_log2: 11,
                frame_count: 100,
                head_room: 128,
                fq_size: 512,
                cq_size: 512,
                group_start_frame_idx: 100,
                partitions: 1,
                reserved_frames: 0,
            }),
        );

        let mut registry = FrameRegistry::new(0, vec![(0, partition0), (1, partition1)], rx, bus, configs);

        // Allocate 10 frames from UMEM 0
        let frames = registry.allocate_batch(0, 10);
        assert_eq!(frames.len(), 10);
        for &frame_idx in &frames {
            assert!(frame_idx < 100, "Frame {} should be from UMEM 0", frame_idx);
        }

        // Allocate 5 frames from UMEM 1
        let frames = registry.allocate_batch(1, 5);
        assert_eq!(frames.len(), 5);
        for &frame_idx in &frames {
            assert!(
                frame_idx >= 100 && frame_idx < 200,
                "Frame {} should be from UMEM 1",
                frame_idx
            );
        }

        // Check remaining frames
        assert_eq!(registry.total_free_frames(), 185); // 200 - 15
    }

    /// Test frame release and refcounting
    #[test]
    fn test_frame_release_and_refcount() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        let partition = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 100,
        };

        let mut registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus.clone(), configs.clone());

        // Allocate 10 frames
        let frames = registry.allocate_batch(0, 10);
        assert_eq!(frames.len(), 10);
        assert_eq!(registry.total_free_frames(), 90);

        // Release first frame
        registry.release_local(frames[0]);
        assert_eq!(registry.total_free_frames(), 91);

        // Release same frame again (refcount is 0, will add duplicate - this is a caller bug)
        // In real usage, this shouldn't happen as the frame is already back in free_stash.
        // As double free is caught by release_local, we don't need to worry about it.

        registry.release_local(frames[0]);
        assert_eq!(registry.total_free_frames(), 91); // Double Free Caught

        // Note: The above is a bug scenario - callers should not release frames twice
        // The refcount mechanism is for tracking shallow copies (retain/release pairs)

        // Release remaining frames (note: we now have 92 due to duplicate)
        for &frame_idx in &frames[1..] {
            registry.release_local(frame_idx);
        }
        assert_eq!(registry.total_free_frames(), 100); // 91 + 9 = 100
    }

    /// Test merged partitions (worker requests multiple consecutive partitions)
    #[test]
    fn test_merged_partitions() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        // Worker gets 4 consecutive partitions merged into one
        // Partitions 0-3 from UMEM 0, each 256 frames = 1024 total frames
        let merged_partition = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 1024, // 4 partitions * 256 frames
        };

        let registry = FrameRegistry::new(0, vec![(0, merged_partition)], rx, bus.clone(), configs.clone());

        // Check total frames
        assert_eq!(registry.total_free_frames(), 1024);

        // Check frame ownership across the merged partition
        assert_eq!(registry.find_umem_group_for_frame(0), Some(0));
        assert_eq!(registry.find_umem_group_for_frame(255), Some(0)); // End of partition 0
        assert_eq!(registry.find_umem_group_for_frame(256), Some(0)); // Start of partition 1
        assert_eq!(registry.find_umem_group_for_frame(512), Some(0)); // Start of partition 2
        assert_eq!(registry.find_umem_group_for_frame(768), Some(0)); // Start of partition 3
        assert_eq!(registry.find_umem_group_for_frame(1023), Some(0)); // End of partition 3
        assert_eq!(registry.find_umem_group_for_frame(1024), None); // Out of range
    }

    /// Test allocation exhaustion
    #[test]
    fn test_allocation_exhaustion() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        let partition = UmemPartition {
            worker_id: 0,
            umem_group_id: 0,
            start_idx: 0,
            end_idx: 10, // Only 10 frames
        };

        let mut registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus.clone(), configs.clone());

        // Allocate all frames
        let frames = registry.allocate_batch(0, 20); // Request more than available
        assert_eq!(frames.len(), 10); // Should only get 10
        assert_eq!(registry.total_free_frames(), 0);

        // Try to allocate more (should get empty)
        let frames = registry.allocate_batch(0, 5);
        assert_eq!(frames.len(), 0);
    }

    /// Test frame indexing with non-zero start indices
    #[test]
    fn test_non_zero_start_indices() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        // Worker owns partitions 2-3 from UMEM 0 (frames 512-1023)
        let partition = UmemPartition {
            worker_id: 1,
            umem_group_id: 0,
            start_idx: 512,
            end_idx: 1024,
        };

        let registry = FrameRegistry::new(1, vec![(0, partition)], rx, bus.clone(), configs.clone());

        // Check total frames
        assert_eq!(registry.total_free_frames(), 512);

        // Check frame ownership
        assert_eq!(registry.find_umem_group_for_frame(511), None); // Before range
        assert_eq!(registry.find_umem_group_for_frame(512), Some(0)); // Start
        assert_eq!(registry.find_umem_group_for_frame(768), Some(0)); // Middle
        assert_eq!(registry.find_umem_group_for_frame(1023), Some(0)); // End
        assert_eq!(registry.find_umem_group_for_frame(1024), None); // After range
    }
}
