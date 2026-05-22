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
use xbricks_base::frame_registry::{FrameRegistry, MAX_UMEM};
use crate::xsocket::{XSocket, XSocketRx, XSocketTx, XdpSocketConfig};
use crate::{BatchRx, BatchTx};
use xbricks_base::system::PlatformData;
#[cfg(feature = "wasm")]
use xbricks_wasm::wasm_manager::WasmManager;
use crate::system_runner::XBricksSystem;
use xbricks_base::telemetry::TelemetryRegistry;
use crate::toml_parser::{PartitionRequest, WorkerBuilderConfig};
use crate::xscrubber_skel::XscrubberSkel;

pub struct SystemBuilder {
    pub(crate) workers: Vec<WorkerBuilderConfig>,
    pub(crate) xdp_socket_configs: Vec<XdpSocketConfig>,
    /// Indexed by umem_id (0..MAX_UMEM). None = not configured.
    pub runtime_umems: Vec<Option<Arc<RuntimeUmemConfig>>>,
    #[cfg(feature = "wasm")]
    /// Indexed by umem_id (0..MAX_UMEM). None = no WASM manager for that UMEM.
    pub(crate) wasm_managers: Vec<Option<Arc<WasmManager>>>,
    /// Indexed by worker_id (0..workers.len()). None = no partitions requested.
    pub(crate) worker_partitions: Vec<Option<Vec<PartitionRequest>>>,
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

        use crate::config::hot_config::SharedSystemState;
        let num_workers = self.workers.len();
        let mut xbricks_system = Box::new(XBricksSystem {
            xsocket_registry: XSocketRegistry::default(),
            frame_registries: (0..num_workers).map(|_| None).collect(),
            telemetry_registry: Arc::new(TelemetryRegistry::default()),
            worker_runners: (0..num_workers).map(|_| None).collect(),
            platform_data,
            shared_state: SharedSystemState::new(HashMap::new()),
            umems: vec![None; MAX_UMEM],
        });
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
        let mut umem_groups: Vec<Option<Vec<&XdpSocketConfig>>> = vec![None; MAX_UMEM];
        for config in &self.xdp_socket_configs {
            umem_groups[config.umem_config.group_id].get_or_insert_with(Vec::new).push(config);
        }

        // Pre-scan: count WASM instances per umem_id and adjust reserved_frames to be
        // evenly divisible before partition layout and WasmManager creation.
        #[cfg(feature = "wasm")]
        {
            let mut wasm_counts = vec![0usize; MAX_UMEM];
            for worker in &self.workers {
                for pipeline in &worker.pipelines {
                    for node in &pipeline.nodes {
                        if node.args.contains_key("wasm_file_path") {
                            if let Some(id_str) = node.args.get("umem") {
                                if let Ok(uid) = id_str.parse::<usize>() {
                                    if uid < MAX_UMEM {
                                        wasm_counts[uid] += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (uid, &count) in wasm_counts.iter().enumerate() {
                if count == 0 { continue; }
                if let Some(umem_conf_arc) = self.runtime_umems[uid].as_ref() {
                    let orig = umem_conf_arc.reserved_frames;
                    let frame_size = umem_conf_arc.frame_size;
                    let adjusted = (orig / count as u32) * count as u32;
                    if adjusted != orig {
                        let mut new_conf = (**umem_conf_arc).clone();
                        new_conf.reserved_frames = adjusted;
                        self.runtime_umems[uid] = Some(Arc::new(new_conf));
                        log::info!(
                            "UMEM {}: reserved_frames reduced from {} to {} to be evenly divisible by {} WASM instance(s)",
                            uid, orig, adjusted, count
                        );
                    }
                    let frames_per_instance = adjusted / count as u32;
                    let mb_per_instance = frames_per_instance as f64 * frame_size as f64 / (1024.0 * 1024.0);
                    log::info!(
                        "UMEM {}: each of {} WASM node(s) gets {} reserved frame(s) ({:.2} MB)",
                        uid, count, frames_per_instance, mb_per_instance
                    );
                }
            }
        }

        // 3. Calculate partition allocation for each worker
        // Indexed by worker_id (0..num_workers).
        let mut worker_partitions: Vec<Vec<(usize, UmemPartition)>> = vec![Vec::new(); num_workers];

        // Track next available partition index for each UMEM. Indexed by umem_id (0..MAX_UMEM).
        let mut umem_partition_counters: Vec<usize> = vec![0; MAX_UMEM];

        // Process workers in order of their IDs for deterministic allocation
        let mut sorted_worker_ids: Vec<usize> = (0..num_workers)
            .filter(|&id| self.worker_partitions[id].is_some())
            .collect();

        for worker_id in &sorted_worker_ids {
            let mut worker_parts = Vec::new();

            // Get partition requests for this worker (if any)
            let partition_reqs = self.worker_partitions[*worker_id].as_deref().unwrap_or(&[]);

            // Process each partition request for this worker
            for partition_req in partition_reqs {
                let umem_group_id = partition_req.umem;
                let umem_conf = self
                    .runtime_umems[umem_group_id]
                    .as_ref()
                    .ok_or_else(|| format!("Worker {}: requested partitions from unknown UMEM {}", worker_id, umem_group_id))?;

                // Calculate partition size from usable (non-reserved) frames only
                let usable_frames = umem_conf.frame_count - umem_conf.reserved_frames;
                let partition_size = usable_frames / umem_conf.partitions;
                // Worker partitions begin after the reserved region
                let usable_start = umem_conf.group_start_frame_idx + umem_conf.reserved_frames;

                // Get the starting partition index for this allocation
                let partition_idx = &mut umem_partition_counters[umem_group_id];

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
                    local_start: start_idx - umem_conf.group_start_frame_idx,
                    local_end:   end_idx   - umem_conf.group_start_frame_idx,
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

            worker_partitions[*worker_id] = worker_parts;
        }

        // Collect all partitions into a flat Vec for determine_owner lookups
        let all_partitions: Vec<UmemPartition> = worker_partitions
            .iter()
            .flat_map(|parts| parts.iter().map(|(_umem_id, p)| *p))
            .collect();

        // Initialize UMEMs and Fill/Comp Queues
        // Indexed by group_id (0..MAX_UMEM). None = this group_id not configured.
        let mut initialized_umems: Vec<Option<(Arc<Umem>, Vec<xsk_rs::FrameDesc>)>> = vec![None; MAX_UMEM];

        for (group_id, umem_conf_opt) in self.runtime_umems.iter().enumerate() {
            let umem_conf = match umem_conf_opt {
                Some(c) => c,
                None => continue,
            };

            let xsk_umem_config = xsk_rs::config::UmemConfig::builder()
                .frame_size(FrameSize::new(umem_conf.frame_size)?)
                .frame_count(std::num::NonZeroU32::new(umem_conf.frame_count).unwrap())
                .fill_queue_size(QueueSize::try_from(umem_conf.fq_size)?)
                .comp_queue_size(QueueSize::try_from(umem_conf.cq_size)?)
                .frame_headroom(umem_conf.head_room)
                .build_opts()
                .map_err(|e| format!("Failed to create UMEM for group {}: {}", group_id, e))?;

            // xsk-rs returns the umem and the pre-calculated descriptors
            let (umem, umem_descs) = Umem::new_with_opts(
                xsk_umem_config,
                true,
            )?;

            let shared_umem = Arc::from(umem);
            initialized_umems[group_id] = Some((shared_umem, umem_descs));
        }
        let mut umem_addr= [None; MAX_UMEM];
        // Store all UMEM handles in XBricksSystem and propagate to each worker's registry.
        for (group_id, entry) in initialized_umems.iter().enumerate() {
            if let Some((umem, _)) = entry {
                xbricks_system.umems[group_id] = Some(umem.clone());
                umem_addr[group_id] = Some(umem.as_ptr() as usize);
            }
        }

        let system_bus = Arc::new(SystemBus {
            bus_senders: bus_senders.clone(),
            umem_partitions: all_partitions,
            umem_addr,
        });

        // Create FrameRegistry for each worker
        for worker_id in &sorted_worker_ids {
            // Get the partitions for this worker
            let partitions = std::mem::take(&mut worker_partitions[*worker_id]);

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
            xbricks_system.frame_registries[*worker_id] = Some(frame_registry);
        }

        // Give every worker registry the physical UMEM handle (cheap Arc clone).
        for frame_registry_opt in xbricks_system.frame_registries.iter_mut() {
            if let Some(frame_registry) = frame_registry_opt {
                for (group_id, entry) in initialized_umems.iter().enumerate() {
                    if let Some((umem, _)) = entry {
                        frame_registry.active_umems[group_id] = Some(umem.clone());
                    }
                }
            }
        }

        // --- 6. CREATE SOCKETS ---
        let mut xsockets = Vec::new();
        for (group_id, entry) in initialized_umems.iter().enumerate() {
            let (umem, umem_descs) = match entry {
                Some(v) => v,
                None => continue,
            };
            let mut queue_ring_map = HashMap::new();
            let umem_conf = self.runtime_umems[group_id].as_ref().unwrap();

            for config in umem_groups[group_id].as_deref().unwrap_or(&[]).iter() {
                let ring_key = (config.interface.clone(), config.queue_id);
                let shared_rings = queue_ring_map.get(&ring_key).cloned();

                let mut initial_descs = Vec::new();
                if shared_rings.is_none() {
                    if let Some(registry) = xbricks_system.frame_registries[config.worker_id].as_mut() {
                        let mut global_frames = vec![0u32; umem_conf.fq_size as usize];
                        let allocated = registry.allocate_batch_into(group_id, &mut global_frames);

                        for i in 0..allocated {
                            let (_umem_id, local_idx) = xbricks_base::frame_registry::decode_global_frame(global_frames[i]);
                            initial_descs.push(umem_descs[local_idx as usize]);
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
        for (umem_id, umem_conf_opt) in self.runtime_umems.iter().enumerate() {
            if let Some(umem_conf) = umem_conf_opt {
                if umem_conf.reserved_frames > 0 {
                    if let Some((umem, _)) = &initialized_umems[umem_id] {
                        self.wasm_managers[umem_id] = Some(Arc::new(
                            WasmManager::new(umem_conf.frame_size, umem_conf.reserved_frames, umem_conf.frame_count, umem.clone())?
                        ));
                    } else {
                        log::error!("🔥 Failed to initialize WASM Manager for UMEM {}. Umem not found.", umem_id);
                    }
                }
            }
        }

        // Register all sockets in the BPF maps
        if interface_skels.is_some() {
            crate::config::configure_bpf::register_sockets_in_bpf(&xsockets, interface_skels.unwrap())?;
        } else {
            error!("🔥 No interface skels provided. Cannot register sockets in BPF.");
        }

        let num_sockets = xsockets.len();
        let num_active_workers = xbricks_system.frame_registries.iter().filter(|e| e.is_some()).count();
        info!(
            "Initialized {} XDP sockets and {} worker frame registries from configuration",
            num_sockets, num_active_workers
        );

        // Create socket registry for pipeline construction
        xbricks_system.xsocket_registry = crate::system_builder::XSocketRegistry::new(xsockets);
        // Create telemetry registry
        let mut telemetry_registry = TelemetryRegistry::new();
        // Create dst port stats registry

        // ==========================================================
        // NEW: INITIALIZE RCU SHARED SYSTEM STATE
        // ==========================================================
        use crate::config::hot_config::HotPipelineConfig;
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

        // Pre-scan: count WASM instances per umem_id so we can
        // divide the reserved region into equal partitions.
        #[cfg(feature = "wasm")]
        let wasm_instance_totals: Vec<usize> = {
            let mut totals = vec![0usize; MAX_UMEM];
            for worker in &self.workers {
                for pipeline in &worker.pipelines {
                    for node in &pipeline.nodes {

                        // GENERIC WASM DETECTOR: Does this node load a WASM plugin?
                        if node.args.contains_key("wasm_file_path") {

                            // If it uses WASM, it MUST be bound to a UMEM
                            if let Some(id_str) = node.args.get("umem") {
                                if let Ok(uid) = id_str.parse::<usize>() {
                                    if uid < MAX_UMEM {
                                        totals[uid] += 1;
                                    }
                                }
                            } else {
                                log::warn!(
                                    "Node '{}' has a 'wasm_file_path' but no 'umem' argument!",
                                    node.id
                                );
                            }
                        }

                    }
                }
            }
            totals
        };
        #[cfg(feature = "wasm")]
        let mut wasm_instance_counters= vec![0u32; MAX_UMEM];

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
                let (nodes, source_indices) = crate::pipeline::PipelineFactory::build(
                    pipeline_instance_cfg,
                    worker_id,
                    &mut xbricks_system.xsocket_registry,
                    Some(&mut telemetry_registry),
                    &xbricks_system.platform_data,
                    #[cfg(feature = "wasm")]
                    &self.wasm_managers,
                    shared_state.clone(),
                    #[cfg(feature = "wasm")]
                    &wasm_instance_totals,
                    #[cfg(feature = "wasm")]
                    &mut wasm_instance_counters,
                )
                .map_err(|e| anyhow::anyhow!(e))?;
                // Create the pipeline runner
                let pipeline_rcu = shared_state.pipelines
                    .get(&pipeline_instance_cfg.name)
                    .expect("Pipeline RCU missing for runner")
                    .clone();
                let runner = crate::pipeline::PipelineRunner::new(pipeline_instance_cfg.name.clone(), worker_id, nodes, source_indices, xbricks_system.platform_data.tsc_frequency, pipeline_rcu)
                    .map_err(|e| anyhow::anyhow!(e))?;

                runners.push(runner);
            }
            xbricks_system.worker_runners[worker_id] = Some((core_id, runners));
        }

        xbricks_system.telemetry_registry = Arc::new(telemetry_registry);

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
    use xbricks_base::frame_registry::decode_global_frame;

    // Helper to create test SystemBus and configs
    fn create_test_bus_and_configs() -> (Arc<SystemBus>, Vec<Option<Arc<RuntimeUmemConfig>>>) {
        let (tx, _rx) = crossbeam_channel::bounded(10);
        let bus = Arc::new(SystemBus { bus_senders: vec![tx], umem_partitions: vec![], umem_addr: [None; MAX_UMEM] });

        let mut configs: Vec<Option<Arc<RuntimeUmemConfig>>> = vec![None; MAX_UMEM];
        configs[0] = Some(Arc::new(RuntimeUmemConfig {
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
        }));

        (bus, configs)
    }

    /// Test basic frame indexing with a single UMEM and single partition
    #[test]
    fn test_single_umem_single_partition() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        // UMEM 0, group_start=0 → local range == absolute range
        let partition = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 100,
            local_start: 0, local_end: 100,
        };

        let mut registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus, configs);

        assert_eq!(registry.total_free_frames(), 100);

        // All frames should decode to umem_id 0
        let frames = registry.allocate_batch(0, 100);
        assert_eq!(frames.len(), 100);
        for &f in &frames {
            assert_eq!(decode_global_frame(f).0, 0, "frame {f:#010x} must belong to UMEM 0");
        }
    }

    /// Test frame indexing with multiple UMEM groups
    #[test]
    fn test_multi_umem_frame_indexing() {
        let (tx, rx) = crossbeam_channel::bounded(10);

        // UMEM 0: 4096 frames, group_start=0
        let partition0 = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 4096,
            local_start: 0, local_end: 4096,
        };

        // UMEM 1: 2048 frames, group_start=4096 → local range [0, 2048)
        let partition1 = UmemPartition {
            worker_id: 0, umem_group_id: 1,
            start_idx: 4096, end_idx: 6144,
            local_start: 0, local_end: 2048,
        };

        let (bus, mut configs) = create_test_bus_and_configs();
        // Override UMEM 0 to have 4096 frames to match partition0
        configs[0] = Some(Arc::new(RuntimeUmemConfig {
            group_id: 0, frame_size: 2048, frame_size_log2: 11,
            frame_count: 4096, head_room: 128, fq_size: 512, cq_size: 512,
            group_start_frame_idx: 0, partitions: 2, reserved_frames: 0,
        }));
        configs[1] = Some(Arc::new(RuntimeUmemConfig {
            group_id: 1, frame_size: 2048, frame_size_log2: 11,
            frame_count: 2048, head_room: 128, fq_size: 512, cq_size: 512,
            group_start_frame_idx: 4096, partitions: 1, reserved_frames: 0,
        }));

        let mut registry = FrameRegistry::new(0, vec![(0, partition0), (1, partition1)], rx, bus, configs);

        assert_eq!(registry.total_free_frames(), 6144);

        // All frames from UMEM 0 must decode to umem_id 0
        let frames0 = registry.allocate_batch(0, 4096);
        assert_eq!(frames0.len(), 4096);
        for &f in &frames0 {
            assert_eq!(decode_global_frame(f).0, 0, "frame {f:#010x} must belong to UMEM 0");
        }

        // All frames from UMEM 1 must decode to umem_id 1
        let frames1 = registry.allocate_batch(1, 2048);
        assert_eq!(frames1.len(), 2048);
        for &f in &frames1 {
            assert_eq!(decode_global_frame(f).0, 1, "frame {f:#010x} must belong to UMEM 1");
        }
    }

    /// Test frame allocation from specific UMEM group
    #[test]
    fn test_allocate_from_specific_umem() {
        let (tx, rx) = crossbeam_channel::bounded(10);

        // UMEM 0: 100 frames, group_start=0
        let partition0 = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 100,
            local_start: 0, local_end: 100,
        };

        // UMEM 1: 100 frames, group_start=100 → local range [0, 100)
        let partition1 = UmemPartition {
            worker_id: 0, umem_group_id: 1,
            start_idx: 100, end_idx: 200,
            local_start: 0, local_end: 100,
        };

        let (bus, mut configs) = create_test_bus_and_configs();
        configs[1] = Some(Arc::new(RuntimeUmemConfig {
            group_id: 1, frame_size: 2048, frame_size_log2: 11,
            frame_count: 100, head_room: 128, fq_size: 512, cq_size: 512,
            group_start_frame_idx: 100, partitions: 1, reserved_frames: 0,
        }));

        let mut registry = FrameRegistry::new(0, vec![(0, partition0), (1, partition1)], rx, bus, configs);

        // Allocate 10 frames from UMEM 0 — must all decode to umem_id 0
        let frames = registry.allocate_batch(0, 10);
        assert_eq!(frames.len(), 10);
        for &f in &frames {
            assert_eq!(decode_global_frame(f).0, 0, "frame {f:#010x} must be from UMEM 0");
        }

        // Allocate 5 frames from UMEM 1 — must all decode to umem_id 1
        let frames = registry.allocate_batch(1, 5);
        assert_eq!(frames.len(), 5);
        for &f in &frames {
            assert_eq!(decode_global_frame(f).0, 1, "frame {f:#010x} must be from UMEM 1");
        }

        assert_eq!(registry.total_free_frames(), 185); // 200 - 15
    }

    /// Test frame release and refcounting
    #[test]
    fn test_frame_release_and_refcount() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        let partition = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 100,
            local_start: 0, local_end: 100,
        };

        let mut registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus.clone(), configs.clone());

        let frames = registry.allocate_batch(0, 10);
        assert_eq!(frames.len(), 10);
        assert_eq!(registry.total_free_frames(), 90);

        registry.release_local(frames[0]);
        assert_eq!(registry.total_free_frames(), 91);

        // Double-free is caught by release_local
        registry.release_local(frames[0]);
        assert_eq!(registry.total_free_frames(), 91);

        for &frame_idx in &frames[1..] {
            registry.release_local(frame_idx);
        }
        assert_eq!(registry.total_free_frames(), 100);
    }

    /// Test merged partitions (worker requests multiple consecutive partitions)
    #[test]
    fn test_merged_partitions() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        // 4 consecutive partitions merged into one — UMEM 0, group_start=0
        let merged_partition = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 1024,
            local_start: 0, local_end: 1024,
        };

        let mut registry = FrameRegistry::new(0, vec![(0, merged_partition)], rx, bus.clone(), configs.clone());

        assert_eq!(registry.total_free_frames(), 1024);

        // All 1024 frames must decode to umem_id 0
        let frames = registry.allocate_batch(0, 1024);
        assert_eq!(frames.len(), 1024);
        for &f in &frames {
            assert_eq!(decode_global_frame(f).0, 0);
        }
    }

    /// Test allocation exhaustion
    #[test]
    fn test_allocation_exhaustion() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        let partition = UmemPartition {
            worker_id: 0, umem_group_id: 0,
            start_idx: 0, end_idx: 10,
            local_start: 0, local_end: 10,
        };

        let mut registry = FrameRegistry::new(0, vec![(0, partition)], rx, bus.clone(), configs.clone());

        let frames = registry.allocate_batch(0, 20); // request more than available
        assert_eq!(frames.len(), 10);
        assert_eq!(registry.total_free_frames(), 0);

        let frames = registry.allocate_batch(0, 5);
        assert_eq!(frames.len(), 0);
    }

    /// Test frame indexing with non-zero start indices (worker owns partition [512, 1024) of UMEM 0)
    #[test]
    fn test_non_zero_start_indices() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let (bus, configs) = create_test_bus_and_configs();

        // UMEM 0, group_start=0 → local_start=512, local_end=1024
        let partition = UmemPartition {
            worker_id: 1, umem_group_id: 0,
            start_idx: 512, end_idx: 1024,
            local_start: 512, local_end: 1024,
        };

        let mut registry = FrameRegistry::new(1, vec![(0, partition)], rx, bus.clone(), configs.clone());

        assert_eq!(registry.total_free_frames(), 512);

        // All allocated frames must decode to umem_id=0 and local_idx in [512, 1024)
        let frames = registry.allocate_batch(0, 512);
        assert_eq!(frames.len(), 512);
        for &f in &frames {
            let (uid, lidx) = decode_global_frame(f);
            assert_eq!(uid, 0, "frame {f:#010x} must be from UMEM 0");
            assert!(lidx >= 512 && lidx < 1024, "local_idx {lidx} must be in [512, 1024)");
        }
    }
}
