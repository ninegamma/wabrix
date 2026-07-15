use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use arc_swap::ArcSwap;
use core_affinity::CoreId;
use log::{debug, info};
use crate::toml_parser::PipelineInstance;
pub(crate) use crate::nodes::{
    BatchRxNode, BatchTxNode, DropNode, FastParserNode,
};

pub(crate) use crate::{BatchRx, BatchTx};
use wabrix_base::system::PlatformData;
use separator::Separatable;
use tokio::sync::broadcast::{Receiver, Sender};
use wabrix_api::helpers::parse_suffixed_number;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::frame_registry::{set_tsc_frequency, set_worker_frame_registry};
use wabrix_base::{ControlEvent, Directive, Node, NodeId};
use crate::config::hot_config::{HotNodeData, HotPipelineConfig, NodeDynamicState};
#[cfg(feature = "wasm")]
use wabrix_wasm::wasm_manager::WasmManager;
use wabrix_base::telemetry::TelemetryRegistry;
use wabrix_base::node_instance_id::NodeInstanceId;
use crate::generator_node::{GenState, GeneratorNode};
use crate::tcp_node::TcpNode;

/// Trait for managing XDP socket access
/// This abstracts away the details of how XDP sockets are stored and retrieved
/// Note: XSockets implement both BatchRx and BatchTx
pub trait XSocketRxTxProvider {
    /// Get an owned XSocket for RX operations on a specific queue and interface
    /// The socket implements both BatchRx and BatchTx
    fn take_rx_socket(&mut self, queue_id: usize, interface: &str) -> Option<Box<dyn BatchRx + Send>>;

    /// Get an owned XSocket for TX operations on a specific queue and interface
    /// The socket implements both BatchRx and BatchTx
    fn take_tx_socket(&mut self, queue_id: usize, interface: &str) -> Option<Box<dyn BatchTx + Send>>;
}

pub struct PipelineFactory;

impl PipelineFactory {
    /// returns a vector of nodes, and a vector of the indices of the source nodes in the pipeline
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub fn build(
        pipeline_instance: &PipelineInstance,
        worker_id: usize,
        xsocket_mgr: &mut dyn XSocketRxTxProvider,
        mut telemetry_registry: Option<&mut TelemetryRegistry>,
        ctrl_tx: Option<Sender<Arc<ControlEvent>>>,
        platform_data: &PlatformData,
        #[cfg(feature = "wasm")]
        wasm_managers: &[Option<Arc<WasmManager>>],
        shared_state: Arc<crate::config::hot_config::SharedSystemState>,
        #[cfg(feature = "wasm")]
        wasm_instance_totals: &[usize],
        #[cfg(feature = "wasm")]
        wasm_instance_counters: &mut [u32],
    ) -> Result<(Vec<Box<dyn Node>>, Vec<usize>), String> {
        // --- PASS 1: ID Resolution ---
        // Map "string_id" -> NodeId
        // The vector index will be the NodeId index.
        let mut id_map: HashMap<String, NodeId> = HashMap::new();
        for (index, node_conf) in pipeline_instance.node_instances.iter().enumerate() {
            if id_map.contains_key(&node_conf.node_config.id) {
                return Err(format!("Duplicate Node ID: {}", node_conf.node_config.id));
            }
            id_map.insert(node_conf.node_config.id.clone(), NodeId::new(index as u32, node_conf.node_config.id.clone()));
        }

        // =========================================================
        // --- PASS 1.2: Populate Initial Hot Configuration ---
        // =========================================================
        // Grab the specific RCU pointer for this pipeline
        let pipeline_rcu = shared_state.pipelines
            .get(&pipeline_instance.name)
            .expect("Pipeline RCU state not initialized!")
            .clone();

        // Create a vector pre-filled with defaults, sized exactly to our node count
        let mut fast_nodes = vec![HotNodeData::default(); pipeline_instance.node_instances.len()];
        let mut name_to_index = HashMap::new();
        // Resolve string routes to fast NodeIds
        let mut resolved_routes = Vec::new();

        for (index, node_conf) in pipeline_instance.node_instances.iter().enumerate() {
            name_to_index.insert(node_conf.node_config.id.clone(), index);


            for r_str in &node_conf.node_config.routes {
                let rid = id_map.get(r_str)
                    .ok_or(format!("Route target '{}' not found", r_str))?
                    .clone();
                resolved_routes.push(rid);
            }
        }

        // =========================================================
        // --- PASS 1.5: Collect interface information from nodes ---
        // Map node_id -> interface name (for BatchTxNode and BatchRxNode)
        let mut node_interfaces: HashMap<String, String> = HashMap::new();
        for node_conf in &pipeline_instance.node_instances {
            if node_conf.node_config.node_type == "BatchTxNode" || node_conf.node_config.node_type == "BatchRxNode" {
                if let Some(iface) = node_conf.node_config.args.get("interface") {
                    node_interfaces.insert(node_conf.node_config.id.clone(), iface.clone());
                }
            }
        }

        // Validate that we have a node list
        let mut built_nodes: Vec<Option<Box<dyn Node>>> = Vec::with_capacity(pipeline_instance.node_instances.len());
        // Fill with None initially so we can insert at specific indices
        for _ in 0..pipeline_instance.node_instances.len() {
            built_nodes.push(None);
        }

        // --- PASS 2: Instantiation ---
        for node_instance in &pipeline_instance.node_instances {
            let my_id = id_map[&node_instance.node_config.id].clone();

            let node_impl: Box<dyn Node> = match node_instance.node_config.node_type.as_str() {
                // === 1. RX Node ===
                "BatchRxNode" => {
                    let next_str = node_instance
                        .node_config.routes
                        .first()
                        .ok_or(format!("Node {} missing routes[0]", node_instance.node_config.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_instance.node_config.id, next_str))?
                        .clone();

                    let iface = node_instance
                        .node_config.args
                        .get("interface")
                        .ok_or(format!("RxNode {} missing 'interface' arg", node_instance.node_config.id))?;

                    assert!(node_instance.node_config.queues.is_some());
                    assert_eq!(node_instance.node_config.queues.as_ref().unwrap().len(), 1);
                    let queue_id = node_instance.node_config.queues.as_ref().unwrap()[0] as usize;

                    // Parse batch_size from args (default: 64, max: MAX_BATCH_SIZE)
                    let batch_size = node_instance
                        .node_config.effective_arg("batch_size")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(64);
                    debug!("BatchRxNode {}: batch_size={}", node_instance.node_config.id, batch_size);

                    // Validate batch_size
                    if batch_size == 0 {
                        return Err(format!("BatchRxNode {}: batch_size must be > 0", node_instance.node_config.id));
                    }
                    if batch_size > MAX_BATCH_SIZE {
                        return Err(format!(
                            "BatchRxNode {}: batch_size ({}) exceeds maximum of {}",
                            node_instance.node_config.id, batch_size, MAX_BATCH_SIZE
                        ));
                    }
                    // Take ownership of the RX component of the socket
                    let rx_source = xsocket_mgr
                        .take_rx_socket(queue_id, iface)
                        .ok_or(format!("Could not find RX socket for queue {} / iface {}", queue_id, iface))?;

                    Box::new(BatchRxNode::new(my_id.clone(), next_id, rx_source, batch_size))
                }

                // === 2. PacketGenerator Node ===
                "PacketGenerator" => {
                    let next_str = node_instance
                        .node_config.routes
                        .first()
                        .ok_or(format!("Node {} missing routes[0]", node_instance.node_config.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_instance.node_config.id, next_str))?
                        .clone();

                    // Get UMEM ID
                    let umem_group_id = node_instance
                        .node_config.args
                        .get("umem")
                        .ok_or(format!("PacketGenerator {} missing 'umem' in args", node_instance.node_config.id))?
                        .parse::<usize>()
                        .map_err(|_| format!("PacketGenerator {} 'umem' must be a number", node_instance.node_config.id))?;
                    // e.g. args = { pps = "1M" }
                    let pps: u64 = if let Some(pps_str) = node_instance.node_config.hot_args.get("pps") {
                        parse_suffixed_number(pps_str)
                            .map_err(|e| format!("PacketGenerator {}: invalid pps: {}", node_instance.node_config.id, e))?
                    } else {
                        1_000
                    };

                    // Get packets per iteration (default to 256)
                    let batch_size = node_instance
                        .node_config.effective_arg("batch_size")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(256);

                    // Validate batch size
                    if batch_size > MAX_BATCH_SIZE {
                        return Err(format!(
                            "PacketGenerator {}: batch_size ({}) exceeds maximum of {}",
                            node_instance.node_config.id, batch_size, MAX_BATCH_SIZE
                        ));
                    }

                    // Get packets_to_generate: accepts "packets_to_generate" (raw number) or
                    // "no-packets" (human-readable like ipblower's --no-packets, e.g. "100K")
                    let packets_to_generate = node_instance
                        .node_config.effective_arg("packet_count")
                        .and_then(|s| parse_suffixed_number(s).ok())
                        .or_else(|| {
                            node_instance
                                .node_config.args
                                .get("no-packets")
                                .and_then(|s| parse_suffixed_number(s).ok())
                        })
                        .unwrap_or(0);


                    // Find the eventual sink node by traversing the chain
                    use macaddr::MacAddr6;
                    let src_mac = {
                        let mut current_node_id = next_str.clone();
                        let mut visited = std::collections::HashSet::new();
                        let mut found_interface: Option<String> = None;

                        // Traverse the chain to find a BatchTxNode
                        while let Some(current_node) = pipeline_instance.node_instances.iter().find(|n| n.node_config.id == current_node_id) {
                            if visited.contains(&current_node_id) {
                                debug!("PacketGenerator {}: Cycle detected in node chain", node_instance.node_config.id);
                                break;
                            }
                            visited.insert(current_node_id.clone());

                            // Check if this node has an interface (BatchTxNode or BatchRxNode)
                            if let Some(iface) = node_interfaces.get(&current_node_id) {
                                found_interface = Some(iface.clone());
                                break;
                            }

                            // Move to next node if it exists
                            if let Some(next) = current_node.node_config.routes.first() {
                                current_node_id = next.clone();
                            } else {
                                // Reached end of chain
                                break;
                            }
                        }

                        // Try to get MAC address from found interface
                        if let Some(iface) = found_interface {
                            use wabrix_base::system::get_mac_from_ifname;
                            match get_mac_from_ifname(&iface) {
                                Ok(mac) => {
                                    debug!("PacketGenerator {}: Using MAC {} from interface {}", node_instance.node_config.id, mac, iface);
                                    mac
                                }
                                Err(e) => {
                                    debug!(
                                        "Failed to get MAC address for interface {}: {}, using default 00:00:00:00:00:00",
                                        iface, e
                                    );
                                    MacAddr6::new(0, 0, 0, 0, 0, 0)
                                }
                            }
                        } else {
                            // Default MAC (all zeros) if no sink node found with interface
                            debug!(
                                "PacketGenerator {}: No sink node with interface found in chain, using default MAC 00:00:00:00:00:00",
                                node_instance.node_config.id
                            );
                            MacAddr6::new(0, 0, 0, 0, 0, 0)
                        }
                    };
                    // Parse combined "dst" arg: "ip:mac" format (e.g. "192.168.177.1:08:bf:b8:0a:0b:0c"),
                    // matching ipblower's --dst option but using ':' as the ip/mac separator.
                    // Individual "dst_ip" and "dst_mac" args take precedence if also present.
                    let (dst_from_combined_ip, dst_from_combined_mac) = node_instance
                        .node_config.effective_arg("dst")
                        .and_then(|s| {
                            // The IP is dotted-decimal; split on the first ':' to separate it from the MAC.
                            s.find(':').map(|pos| (&s[..pos], &s[pos + 1..]))
                        })
                        .map(|(ip_str, mac_str)| {
                            let ip = ip_str.parse::<std::net::Ipv4Addr>().ok();
                            let mac = {
                                let parts: Vec<&str> = mac_str.split(':').collect();
                                if parts.len() == 6 {
                                    let bytes: Result<Vec<u8>, _> =
                                        parts.iter().map(|p| u8::from_str_radix(p, 16)).collect();
                                    bytes.ok().map(|b| MacAddr6::new(b[0], b[1], b[2], b[3], b[4], b[5]))
                                } else {
                                    None
                                }
                            };
                            (ip, mac)
                        })
                        .unwrap_or((None, None));

                    // Parse destination IP from args (default to 192.168.177.1)
                    let dst_ip = node_instance
                        .node_config.effective_arg("dst_ip")
                        .and_then(|s| s.parse::<std::net::Ipv4Addr>().ok())
                        .or(dst_from_combined_ip)
                        .unwrap_or(std::net::Ipv4Addr::new(192, 168, 177, 1));

                    // Parse destination MAC from args (default to 08:bf:b8:0a:0b:0c)
                    let dst_mac = node_instance
                        .node_config.effective_arg("dst_mac")
                        .and_then(|s| {
                            let parts: Vec<&str> = s.split(':').collect();
                            if parts.len() == 6 {
                                let bytes: Result<Vec<u8>, _> = parts.iter().map(|p| u8::from_str_radix(p, 16)).collect();
                                bytes.ok().map(|b| MacAddr6::new(b[0], b[1], b[2], b[3], b[4], b[5]))
                            } else {
                                None
                            }
                        })
                        .or(dst_from_combined_mac)
                        .unwrap_or(MacAddr6::new(0x08, 0xbf, 0xb8, 0x0a, 0x0b, 0x0c));

                    // 3. Inject it into the fast_nodes array
                    let index = *name_to_index.get(&node_instance.node_config.id).ok_or(format!("Node {} not found", node_instance.node_config.id))?;
                    fast_nodes[index] = HotNodeData {
                        routes: vec![next_id.clone()],
                        dynamic_state: NodeDynamicState::Generator {
                            state: GenState::Idle,
                            pps_limit: pps,
                            packet_limit: packets_to_generate,
                            batch_size: batch_size as u32,
                            dst_ip,
                            dst_mac
                        },
                    };

                    const PATCH_CYCLES: u64 = 2000;
                    let min_interbatch_gap =
                        (platform_data.tsc_frequency / pps as u64 * batch_size as u64).saturating_sub(PATCH_CYCLES);
                    debug!(
                        "PacketGenerator {}: pps={}, batch_size={}, packets_to_generate={}, min_interbatch_gap={}, src_mac={}, dst_ip={}, dst_mac={}",
                        node_instance.node_config.id, pps, batch_size, packets_to_generate, min_interbatch_gap, src_mac, dst_ip, dst_mac
                    );

                    // Detect if this generator should start immediately (ipblower)
                    // or start idle (wabrix GUI). Default to false. ipblower sets autostart="true".
                    let autostart = node_instance.node_config.args.get("autostart")
                        .map(|s| s.to_lowercase() == "true")
                        .unwrap_or(false);

                    let tm_total = Arc::new(AtomicU64::new(0));
                    let tm_this_run = Arc::new(AtomicU64::new(0));
                    let tm_avg_cycles = Arc::new(AtomicU64::new(0));
                    let tm_pps = Arc::new(AtomicU64::new(0));
                    let tm_is_running = Arc::new(AtomicBool::new(false));
                    let tm_force_sync = Arc::new(AtomicBool::new(false));
                    if let Some(ref mut reg) = telemetry_registry {
                        reg.register(
                            format!("{}/{}", pipeline_instance.name, node_instance.node_config.id),
                            vec![
                                "total_generated".to_string(),
                                "pkts_this_run".to_string(),
                                "total_cycles".to_string(),
                                "pps_last_run".to_string(),
                            ],
                            vec![tm_total.clone(), tm_this_run.clone(), tm_avg_cycles.clone(), tm_pps.clone()],
                            vec![Arc::new(AtomicU64::new(0))],
                            Some(tm_is_running.clone()),
                            Some(tm_force_sync.clone()),
                        );
                    }

                    // =========================================================
                    // NEW: Resolve WASM properties if the feature is enabled
                    // =========================================================
                    #[cfg(feature = "wasm")]
                    let (wasm_mgr_opt, wasm_path_opt, partition_start_bytes) = if let Some(path) = node_instance.node_config.args.get("wasm_file_path") {
                        let manager = wasm_managers[umem_group_id]
                            .as_ref()
                            .ok_or(format!("PacketGenerator {}: no WasmManager for umem {}", node_instance.node_config.id, umem_group_id))?;

                        // Compute this instance's scratchpad partition.
                        // Scratchpads occupy [0, reserved_bytes - control_block_bytes); the
                        // Control Block is at the end of the reserved region, so partition_start
                        // starts at UMEM offset 0 for instance 0.
                        let instance_idx = wasm_instance_counters[umem_group_id];
                        let total_instances = wasm_instance_totals[umem_group_id].max(1);
                        let umem_info = manager.umem();
                        let cb_frames = wabrix_base::mailbox::control_block_frames(total_instances as u32, umem_info.frame_size);
                        let scratchpad_frames = umem_info.reserved_frames.saturating_sub(cb_frames);
                        let frames_per_instance = if total_instances > 0 { scratchpad_frames / total_instances as u32 } else { 0 };
                        let p_start = instance_idx * frames_per_instance * umem_info.frame_size;

                        // Increment the counter so the next plugin gets the next memory slice
                        wasm_instance_counters[umem_group_id] += 1;

                        (Some(manager.as_ref()), Some(path.as_str()), p_start)
                    } else {
                        (None, None, 0)
                    };

                    Box::new(GeneratorNode::new(
                        my_id.clone(),
                        next_id,
                        umem_group_id,
                        worker_id,
                        batch_size as u32,
                        min_interbatch_gap,
                        packets_to_generate,
                        src_mac,
                        dst_ip,
                        dst_mac,
                        shared_state.clone(),
                        pipeline_instance.name.clone(),
                        autostart,
                        tm_total,
                        tm_this_run,
                        tm_avg_cycles,
                        tm_pps,
                        tm_is_running,
                        // Inject the conditionally compiled WASM arguments
                        #[cfg(feature = "wasm")]
                        wasm_mgr_opt,
                        #[cfg(feature = "wasm")]
                        wasm_path_opt,
                        #[cfg(feature = "wasm")]
                        partition_start_bytes,
                        #[cfg(feature = "wasm")]
                        node_instance.node_config.wasm_data.clone().unwrap_or_default(),
                        platform_data.clone(),
                    ))
                }

                // === 3. TX Node ===
                "BatchTxNode" => {
                    let iface = node_instance
                        .node_config.args
                        .get("interface")
                        .ok_or(format!("TxNode {} missing 'interface' arg", node_instance.node_config.id))?;

                    assert!(node_instance.node_config.queues.is_some());
                    assert_eq!(node_instance.node_config.queues.as_ref().unwrap().len(), 1);
                    let queue_id = node_instance.node_config.queues.as_ref().unwrap()[0] as usize;

                    // Take ownership of the TX component of the socket
                    let tx_sink = xsocket_mgr
                        .take_tx_socket(queue_id, iface)
                        .ok_or(format!("Could not find TX socket for queue {} / iface {}", queue_id, iface))?;

                    Box::new(BatchTxNode::new(my_id.clone(), tx_sink))
                }

                // === 4. Drop Node ===
                "DropNode" => {
                    let dropped_pkts = Arc::new(AtomicU64::new(0));
                    if let Some(ref mut reg) = telemetry_registry {
                        reg.register(
                            format!("{}/{}", pipeline_instance.name, node_instance.node_config.id),
                            vec!["dropped".to_string()],
                            vec![dropped_pkts.clone()],
                            vec![Arc::new(AtomicU64::new(0))],
                            None,
                            None,
                        );
                    }
                    Box::new(DropNode::new(my_id.clone(), Some(dropped_pkts)))
                }

                // === 5. FastParser Node ===
                "FastParserNode" => {
                    let next_str = node_instance
                        .node_config.routes
                        .first()
                        .ok_or(format!("Node {} missing routes[0]", node_instance.node_config.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_instance.node_config.id, next_str))?
                        .clone();

                    Box::new(FastParserNode::new(my_id.clone(), next_id))
                }

                // === 6. Native TcpNode ===
                "TcpNode" => {
                    let routing_table: Vec<NodeId> = node_instance
                        .node_config.routes
                        .iter()
                        .map(|r| {
                            id_map
                                .get(r)
                                .ok_or_else(|| format!("TcpNode {} points to unknown route '{}'", node_instance.node_config.id, r))
                                .cloned()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if routing_table.is_empty() {
                        return Err(format!("TcpNode {} has no routes defined", node_instance.node_config.id));
                    }

                    let umem_group_id = node_instance
                        .node_config.args
                        .get("umem")
                        .ok_or(format!("TcpNode {} missing 'umem' in args", node_instance.node_config.id))?
                        .parse::<usize>()
                        .map_err(|_| format!("TcpNode {} 'umem' must be a number", node_instance.node_config.id))?;

                    let config_data: Vec<(String, String)> = node_instance
                        .node_config
                        .wasm_data
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .collect();

                    let labels = node_instance.node_config.labels.clone();
                    let num_buckets = labels.as_ref().map_or(0, |l| l.len());
                    let counters_pkts: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();
                    let counters_bytes: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();
                    let tm_force_sync = Arc::new(AtomicBool::new(false));
                    if let Some(registry) = telemetry_registry.as_mut() {
                        registry.register(
                            format!("{}/{}", pipeline_instance.name, node_instance.node_config.id),
                            labels.clone().unwrap_or_default(),
                            counters_pkts.clone(),
                            counters_bytes,
                            None,
                            Some(tm_force_sync.clone()),
                        );
                    }

                    Box::new(TcpNode::new(
                        my_id.clone(),
                        routing_table,
                        umem_group_id,
                        config_data,
                        platform_data.tsc_frequency,
                        counters_pkts,
                        tm_force_sync,
                    ))
                }

                // === 7. Wasm Nodes (Inspector & Engine) ===
                #[cfg(feature = "wasm")]
                "WasmInspectorNode" | "WasmEngine" => {
                    let node_type = node_instance.node_config.node_type.as_str();

                    let wasm_path = node_instance
                        .node_config.args
                        .get("wasm_file_path")
                        .ok_or_else(|| format!("{} {} missing 'wasm_file_path' in args", node_type, node_instance.node_config.id))?;

                    let umem_id = node_instance
                        .node_config.args
                        .get("umem")
                        .ok_or_else(|| format!("{} {} missing 'umem' in args", node_type, node_instance.node_config.id))?
                        .parse::<usize>()
                        .map_err(|_| format!("{} {} 'umem' must be a number", node_type, node_instance.node_config.id))?;

                    let manager = wasm_managers[umem_id]
                        .as_ref()
                        .ok_or_else(|| format!(
                            "{} {}: no WasmManager for umem {}",
                            node_type, node_instance.node_config.id, umem_id
                        ))?;

                    if node_instance.node_config.routes.len() > 255 {
                        return Err(format!(
                            "{} {} cannot have more than 255 routes",
                            node_type, node_instance.node_config.id
                        ));
                    }
                    // ==========================================================
                    // DYNAMIC ROUTING TABLE BUILDER
                    // Loop through however many routes the user provided in TOML
                    // ==========================================================
                    let mut routing_table = Vec::with_capacity(node_instance.node_config.routes.len());
                    for route_name in &node_instance.node_config.routes {
                        let target_id = id_map
                            .get(route_name)
                            .ok_or_else(|| format!(
                                "{} {}: route target '{}' not found",
                                node_type, node_instance.node_config.id, route_name
                            ))?
                            .clone();
                        routing_table.push(target_id);
                    }

                    let labels = node_instance.node_config.labels.clone();
                    let num_buckets = labels.as_ref().map_or(0, |l| l.len());
                    let counters_pkts: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();
                    let counters_bytes: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();
                    let tm_force_sync = Arc::new(AtomicBool::new(false));

                    if let Some(registry) = telemetry_registry.as_mut() {
                        registry.register(
                            format!("{}/{}", pipeline_instance.name, node_instance.node_config.id),
                            labels.clone().unwrap_or_default(),
                            counters_pkts.clone(),
                            counters_bytes.clone(),
                            None,
                            Some(tm_force_sync.clone()),
                        );
                    }

                    let telemetry_metrics = wabrix_base::telemetry::TelemetryMetrics {
                        node_id: node_instance.node_config.id.clone(),
                        labels: labels.unwrap_or_default(),
                        counters_pkts,
                        counters_bytes,
                        active_flag: None,
                        force_sync_flag: Some(tm_force_sync.clone()),
                    };

                    // Compute this instance's scratchpad partition.
                    // Scratchpads occupy [0, reserved_bytes - control_block_bytes); the
                    // Control Block is at the end of the reserved region, so partition_start
                    // starts at UMEM offset 0 for instance 0.
                    let instance_idx = wasm_instance_counters[umem_id];
                    let total_instances = wasm_instance_totals[umem_id].max(1);
                    let umem_info = manager.umem();
                    let cb_frames = wabrix_base::mailbox::control_block_frames(total_instances as u32, umem_info.frame_size);
                    let scratchpad_frames = umem_info.reserved_frames.saturating_sub(cb_frames);
                    let frames_per_instance = if total_instances > 0 { scratchpad_frames / total_instances as u32 } else { 0 };
                    let mailbox_offset = umem_info.mailbox_offset(instance_idx);
                    let partition_start_bytes = instance_idx * frames_per_instance * umem_info.frame_size;
                    wasm_instance_counters[umem_id] += 1;

                    if node_type == "WasmInspectorNode" {
                        use wabrix_wasm::WasmInspectorNode;
                        Box::new(WasmInspectorNode::new(
                            my_id.clone(),
                            manager,
                            wasm_path,
                            routing_table,
                            None,
                            telemetry_metrics,
                            partition_start_bytes,
                            umem_id as u8,
                            node_instance.node_config.wasm_data.clone().unwrap_or_default()
                        ))
                    } else {
                        use wabrix_wasm::WasmEngine;
                        let _worker_rx = ctrl_tx.as_ref().map(|tx| tx.subscribe());
                        Box::new(WasmEngine::new(
                            my_id.clone(),
                            manager,
                            wasm_path,
                            routing_table,
                            telemetry_metrics,
                            partition_start_bytes,
                            umem_id as u8,
                            node_instance.node_config.wasm_data.clone().unwrap_or_default(),
                            platform_data,
                            mailbox_offset,
                        ))
                    }
                }
                _ => return Err(format!("Unknown Node Type: {}", node_instance.node_config.node_type)),
            };

            // Place the built node into the vector at the index corresponding to its NodeId
            built_nodes[usize::from(&my_id)] = Some(node_impl);
        }

        // Unwrap Options (we guaranteed one for every index)
        let final_nodes = built_nodes.into_iter().map(|n| n.unwrap()).collect();

        // Store the fully-populated hot config now that all nodes have been initialised.
        pipeline_rcu.store(Arc::new(HotPipelineConfig { fast_nodes, name_to_index, poll_frequency: pipeline_instance.poll_frequency }));

        // =========================================================
        // --- PASS 3: Topological Source Detection (In-Degree = 0)
        // =========================================================
        // Create an array to count how many times each node is targeted
        let mut in_degrees = vec![0; pipeline_instance.node_instances.len()];

        for node_conf in &pipeline_instance.node_instances {
            // Count routes
            for route_str in &node_conf.node_config.routes {
                if let Some(target_id) = id_map.get(route_str) {
                    in_degrees[target_id.index as usize] += 1;
                }
            }
        }

        // Any node with an in-degree of 0 is mathematically a Source Node
        let source_indices: Vec<usize> = in_degrees
            .into_iter()
            .enumerate()
            .filter(|(_, degree)| *degree == 0)
            .map(|(index, _)| index)
            .collect();

        if source_indices.is_empty() {
            log::warn!("Pipeline '{}' forms a closed loop with no entry points! It will never execute.", pipeline_instance.name);
        }

        // Return both the nodes and the source indices
        Ok((final_nodes, source_indices))
    }
}

/// PipelineRunner executes a pipeline DAG
///
/// This is the core execution engine that:
/// 1. Maintains the node DAG (directed acyclic graph)
/// 2. Executes nodes by following Directive instructions
/// 3. Integrates with Tokio for async coordination
/// 4. Manages batch lifecycle and memory reuse
///
/// Execution model:
/// - Start at the root node (typically a BatchRxNode at index 0)
/// - Process returns a Directive (Next, Split, or Stop)
/// - Follow the directive to continue execution through the DAG
/// - Reuse batch memory across iterations for cache efficiency
/// - Runs in tight synchronous loop within spawn_blocking
pub struct PipelineRunner {
    /// The pipeline name (for logging/debugging)
    name: String,
    /// Worker ID this pipeline belongs to
    worker_id: usize,
    /// Vector of nodes indexed by NodeId
    /// nodes[0] is always the root (entry point)
    nodes: Vec<Box<dyn Node>>,
    /// Tracks the source node IDs for each entry point
    source_indices: Vec<usize>,
    /// The parallel array of rich identities for routing
    node_identities: Vec<NodeInstanceId>,
    /// The runner holds the control channel, not the nodes
    control_rx: Option<Receiver<Arc<ControlEvent>>>,
    /// Reusable batch container to avoid allocations
    /// Cleared and reused on each execution
    working_batch: crate::Batch,
    /// Statistics
    packets_processed: u64,
    runs_with_work: u64,
    runs_idle: u64,
    full_batch_count: usize,
    /// Cache the indices of nodes that need timers
    timer_indices: Vec<usize>,
    /// Cache the indices of nodes that need control events
    #[allow(dead_code)]
    control_indices: Vec<usize>,
    last_timer_poll: u64,
    tsc_freq: u64,
    /// RCU handle for this pipeline's hot config — poll_frequency is read from here at runtime
    pipeline_rcu: Arc<ArcSwap<HotPipelineConfig>>,
}

impl PipelineRunner {
    /// Create a new PipelineRunner from a built node DAG
    ///
    /// # Arguments
    /// * `name` - Pipeline name for identification
    /// * `worker_id` - Worker ID this pipeline belongs to
    /// * `nodes` - Vector of nodes where nodes[0] is the root entry point
    /// * `source_indices` - Vector of source node indices (0-based)
    /// # Returns
    /// Result containing the PipelineRunner or an error if nodes is empty
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        worker_id: usize,
        nodes: Vec<Box<dyn Node>>,
        source_indices: Vec<usize>,
        node_identities: Vec<NodeInstanceId>,
        control_rx: Option<Receiver<Arc<ControlEvent>>>,
        tsc_freq: u64,
        pipeline_rcu: Arc<ArcSwap<HotPipelineConfig>>,
    ) -> Result<Self, String> {
        if nodes.is_empty() {
            return Err("Pipeline must have at least one node".to_string());
        }

        // We can just use the batch count of the first source node (or fallback to max)
        let first_source = source_indices.first().copied().unwrap_or(0);
        let full_batch_count = nodes[first_source].full_batch_count().unwrap_or(MAX_BATCH_SIZE as u32) as usize;

        // Pre-calculate which nodes want timers during setup
        let timer_indices: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.requires_timers())
            .map(|(index, _)| index)
            .collect();

        // Pre-calculate which nodes want timers during setup
        let control_indices: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.requires_control_events())
            .map(|(index, _)| index)
            .collect();

        Ok(PipelineRunner {
            name,
            worker_id,
            nodes,
            source_indices,
            node_identities,
            control_rx,
            working_batch: crate::Batch::new(),
            packets_processed: 0,
            runs_with_work: 0,
            runs_idle: 0,
            full_batch_count,
            last_timer_poll: 0,
            timer_indices,
            control_indices,
            tsc_freq,
            pipeline_rcu,
        })
    }

    /// PRIVATE HELPER: Resolves the Directive chain through the DAG.
    /// This allows us to use the exact same routing logic for normal traffic AND timer traffic.
    #[inline(always)]
    fn execute_directive_chain(&mut self, mut directive: Directive) {
        loop {
            match directive {
                Directive::Next(next_id) => {
                    let next_idx = usize::from(next_id);
                    if next_idx >= self.nodes.len() {
                        log::error!("Pipeline '{}': Invalid NodeId {}", self.name, next_idx);
                        break; // Abort this branch
                    }
                    log::trace!("Pipeline '{}': Executing Next Node {}", self.name, next_idx);
                    directive = self.nodes[next_idx].process(&mut self.working_batch);
                }
                Directive::Split(branches) => {
                    for (next_id, mut sub_batch) in branches {
                        let next_idx = usize::from(next_id);
                        if next_idx >= self.nodes.len() { continue; }

                        log::trace!("Pipeline '{}': Executing Split Branch to Node {}", self.name, next_idx);
                        let mut current_directive = self.nodes[next_idx].process(&mut sub_batch);

                        loop {
                            match current_directive {
                                Directive::Next(branch_next_id) => {
                                    let branch_idx = usize::from(branch_next_id);
                                    if branch_idx >= self.nodes.len() { break; }
                                    log::trace!("Pipeline '{}': Executing Next Node in split branch {}", self.name, branch_idx);
                                    current_directive = self.nodes[branch_idx].process(&mut sub_batch);
                                }
                                Directive::Split(nested_branches) => {
                                    for (nested_id, mut nested_batch) in nested_branches {
                                        let nested_idx = usize::from(nested_id);
                                        if nested_idx < self.nodes.len() {
                                            log::trace!("Pipeline '{}': Executing nested Split Branch to Node {}", self.name, nested_idx);
                                            let _ = self.nodes[nested_idx].process(&mut nested_batch);
                                        }
                                    }
                                    break;
                                }
                                Directive::Stop => break,
                            }
                        }
                    }
                    break;
                }
                Directive::Stop => break,
            }
        }
    }

    /// Execute one iteration of the pipeline
    ///
    /// This follows the Run-To-Completion (RTC) model:
    /// 1. Iterate over all entry points in the DAG (source nodes)
    /// 2. Process batch and get directive
    /// 3. Follow directive to next node(s)
    /// 4. Continue until Stop directive or error
    ///
    /// Returns (packets_processed, status_code)
    /// - packets_processed: number of packets handled in this run
    /// - status_code: 0 for success, negative for errors, positive for specific statuses
    pub fn run_iteration(&mut self) -> (u32, i32) {
        let mut total_packets_this_iteration = 0;
        let mut timer_work_done = false;

        // =========================================================
        //    INGRESS: Process all entry points in the DAG
        // =========================================================
        for i in 0..self.source_indices.len() {
            let source_idx = self.source_indices[i];

            self.working_batch.clear();
            log::trace!("Pipeline '{}': Processing batch from source {}", self.name, source_idx);
            let directive = self.nodes[source_idx].process(&mut self.working_batch);

            let initial_packet_count = self.working_batch.len() as u32;
            total_packets_this_iteration += initial_packet_count;

            // Route the packets through the rest of the DAG
            self.execute_directive_chain(directive);
        }


        let now = unsafe { std::arch::x86_64::_rdtsc() };

        let poll_frequency = self.pipeline_rcu.load().poll_frequency.max(1);
        if now - self.last_timer_poll > self.tsc_freq / poll_frequency {
            // =========================================================
            //    TIMERS: Process asynchronous events (e.g., TCP Retransmits)
            // =========================================================
            self.last_timer_poll = now;

            for i in 0..self.timer_indices.len() {
                let idx = self.timer_indices[i];

                self.working_batch.clear();

                // Call the timer, which may allocate new packets into working_batch
                let directive = self.nodes[idx].poll_timers(&mut self.working_batch, now);

                // WasmEngine::poll_timers drains working_batch into Directive::Split
                // sub-batches, so guard on the directive, not working_batch.len().
                if !matches!(directive, Directive::Stop) {
                    // We generated packets via a timer! Do not go to sleep!
                    timer_work_done = true;
                    self.execute_directive_chain(directive);
                }
            }
            // =========================================================
            //    CONTROL EVENTS: Runner acts as the Router at poll_frequency
            // =========================================================
            if let Some(rx) = &mut self.control_rx {
                // Drain all pending commands from the Axum API
                while let Ok(event) = rx.try_recv() {

                    // Evaluate the wildcard pattern against every node in this pipeline
                    for (idx, identity) in self.node_identities.iter().enumerate() {

                        // Assuming you moved the matches_pattern() method onto NodeInstanceId
                        if identity.matches_pattern(&event.address_pattern) {

                            log::info!("Runner routing event opcode {} to node {}", event.opcode, identity.node_name);

                            // Directly invoke the handler on the matched node
                            self.nodes[idx].handle_control_event(&event);
                        }
                    }
                }
            }

        }

        // =========================================================
        //    STATISTICS & ADAPTIVE POLLING
        // =========================================================
        if total_packets_this_iteration > 0 || timer_work_done {
            self.packets_processed += total_packets_this_iteration as u64;
            self.runs_with_work += 1;
            (total_packets_this_iteration, 0)
        } else {
            self.runs_idle += 1;
            (0,1)
        }


    }

    // Inside PipelineRunner implementation
    pub fn prepare_nodes(&mut self) -> anyhow::Result<()> {
        for node in &mut self.nodes {
            node.prepare()?;
        }
        Ok(())
    }

    /// Get the pipeline name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get worker ID
    pub fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Get statistics
    pub fn stats(&mut self) -> PipelineStats {
        use std::any::Any;

        // Update profiling data from sources before collecting stats
        #[cfg(feature = "profiling")]
        for node in &mut self.nodes {
            let node_any = &mut **node as &mut dyn Any;
            if let Some(rx_node) = node_any.downcast_mut::<crate::nodes::BatchRxNode>() {
                rx_node.update_profiling_from_source();
            }
        }

        // Collect per-node statistics
        let node_stats: Vec<NodeStats> = self
            .nodes
            .iter()
            .map(|node| {
                // Check if this is a PacketGenNode to get burst duration
                let node_ref = &**node as &dyn Any;
                let (burst_duration_cycles, burst_duration) =
                    if let Some(gen_node) = node_ref.downcast_ref::<GeneratorNode>() {
                        (Some(gen_node.get_burst_duration_cycles()), gen_node.get_burst_duration())
                    } else {
                        (None, None)
                    };

                {
                    #[cfg(feature = "profiling")]
                    let (
                        rx_loaded_cycles,
                        rx_empty_cycles,
                        rx_replenish_avg_cycles,
                        rx_replenish_total_cycles,
                        rx_replenish_calls,
                    ) = if let Some(rx_node) = node_ref.downcast_ref::<crate::nodes::BatchRxNode>() {
                        (
                            Some(rx_node.loaded_cycles()),
                            Some(rx_node.empty_cycles()),
                            Some(rx_node.replenish_avg_cycles()),
                            Some(rx_node.replenish_total_cycles()),
                            Some(rx_node.replenish_calls()),
                        )
                    } else {
                        (None, None, None, None, None)
                    };

                    NodeStats {
                        node_id: node.id().clone(),
                        packets_processed: node.processed_packets(),
                        burst_duration_cycles,
                        burst_duration,
                        #[cfg(feature = "profiling")]
                        profiling_cycles: node.profiling_cycles(),
                        #[cfg(feature = "profiling")]
                        rx_loaded_cycles,
                        #[cfg(feature = "profiling")]
                        rx_empty_cycles,
                        #[cfg(feature = "profiling")]
                        rx_replenish_avg_cycles,
                        #[cfg(feature = "profiling")]
                        rx_replenish_total_cycles,
                        #[cfg(feature = "profiling")]
                        rx_replenish_calls,
                    }
                }
            })
            .collect();

        PipelineStats {
            pipeline_name: self.name.clone(),
            packets_processed: self.packets_processed,
            runs_with_work: self.runs_with_work,
            runs_idle: self.runs_idle,
            node_stats,
        }
    }

    /// Check if this pipeline contains a PacketGenerator
    pub fn has_generator(&self) -> bool {
        use std::any::Any;
        for node in &self.nodes {
            let node_ref = &**node as &dyn Any;
            if let Some(_gen_node) = node_ref.downcast_ref::<GeneratorNode>() {
                return true;
            }
        }
        false
    }

    /// Check if all PacketGenerators in this pipeline are finished
    /// Returns true only if we found at least one generator and all are finished
    pub fn has_all_finished_generators(&self) -> bool {
        use std::any::Any;
        let mut found_generator = false;
        for node in &self.nodes {
            // Try to downcast to PacketGenNode
            let node_ref = &**node as &dyn Any;
            if let Some(gen_node) = node_ref.downcast_ref::<GeneratorNode>() {
                found_generator = true;
                if !gen_node.is_finished() {
                    return false; // Found an unfinished generator
                }
            }
        }
        // Return true only if we found at least one generator and all are finished
        found_generator
    }
}

/// Statistics for a pipeline runner
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub pipeline_name: String,
    pub packets_processed: u64,
    pub runs_with_work: u64,
    pub runs_idle: u64,
    pub node_stats: Vec<NodeStats>,
}

impl PipelineStats {
    pub fn print_stats(&self, worker_idx: usize) {
        info!("Worker {} pipeline '{}' stopped:", worker_idx, self.pipeline_name);
        info!(
            "  {} packets, {} runs ({} idle)",
            self.packets_processed.separated_string(),
            self.runs_with_work.separated_string(),
            self.runs_idle.separated_string()
        );

        for node_stat in &self.node_stats {
            node_stat.print_stats();
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeStats {
    pub node_id: NodeId,
    pub packets_processed: u64,
    pub burst_duration_cycles: Option<u64>,
    pub burst_duration: Option<std::time::Duration>,
    #[cfg(feature = "profiling")]
    pub profiling_cycles: u64,
    #[cfg(feature = "profiling")]
    pub rx_loaded_cycles: Option<u64>,
    #[cfg(feature = "profiling")]
    pub rx_empty_cycles: Option<u64>,
    #[cfg(feature = "profiling")]
    pub rx_replenish_avg_cycles: Option<u64>,
    #[cfg(feature = "profiling")]
    pub rx_replenish_total_cycles: Option<u64>,
    #[cfg(feature = "profiling")]
    pub rx_replenish_calls: Option<u64>,
}

impl NodeStats {
    pub fn print_stats(&self) {
        #[cfg(feature = "profiling")]
        {
            if let Some(cycles_per_packet) = self.profiling_cycles.checked_div(self.packets_processed) {
                info!(
                    "    Node {:20}: {} cycles/packet ({} total cycles, {} packets)",
                    self.node_id,
                    cycles_per_packet.separated_string(),
                    self.profiling_cycles.separated_string(),
                    self.packets_processed.separated_string()
                );
            } else {
                info!("    Node {:20}: 0 packets processed", self.node_id);
            }

            // For BatchRxNode, also display loaded vs empty cycles
            if let (Some(loaded), Some(empty)) = (self.rx_loaded_cycles, self.rx_empty_cycles) {
                if let Some(loaded_per_packet) = loaded.checked_div(self.packets_processed) {
                    info!(
                        "      RX loaded runs: {} cycles/packet ({} total cycles)",
                        loaded_per_packet.separated_string(),
                        loaded.separated_string()
                    );
                }
                info!("      RX empty runs: {} total cycles", empty.separated_string());
            }

            // For BatchRxNode, also display replenish profiling
            if let Some(replenish_avg) = self.rx_replenish_avg_cycles {
                if let (Some(total), Some(calls)) = (self.rx_replenish_total_cycles, self.rx_replenish_calls) {
                    info!(
                        "      RX replenish_fill_queue: {} avg cycles/call ({} total cycles, {} calls)",
                        replenish_avg.separated_string(),
                        total.separated_string(),
                        calls.separated_string()
                    );
                }
            }
        }
        #[cfg(not(feature = "profiling"))]
        {
            info!(
                "    Node {:20}: {} packets processed",
                self.node_id,
                self.packets_processed.separated_string()
            );
        }
    }
}

/// Spawn multiple pipeline runners for a worker
///
/// When a worker has multiple pipelines assigned, this spawns them all
/// and runs them cooperatively (round-robin style) on the same core.
///
/// # Arguments
/// * `runners` - Vector of PipelineRunners for this worker
/// * `core_id` - Logical core ID to pin to
/// * `frame_registry` - The worker's frame registry (one per worker)
/// * `shutdown` - Shutdown signal
/// * `platform_data` - Platform data including TSC frequency
///
/// # Returns
/// JoinHandle that resolves to aggregated statistics
pub fn spawn_worker_pipelines(
    mut runners: Vec<PipelineRunner>,
    core_id: CoreId,
    mut frame_registry: wabrix_base::frame_registry::FrameRegistry,
    shutdown: Arc<AtomicBool>,
    workers_completed: Arc<AtomicUsize>,
    platform_data: &PlatformData,
    is_one_shot: bool,
) -> tokio::task::JoinHandle<Result<Vec<PipelineStats>, String>> {
    let tsc_freq = platform_data.tsc_frequency;

    // Capture the TSC start timestamp
    let tsc_start: u64;
    unsafe {
        std::arch::asm!(
            "rdtsc",
            "shl rdx, 32",
            "or rax, rdx",
            out("rax") tsc_start,
            out("rdx") _,
            options(nostack, nomem)
        );
    }
    tokio::task::spawn_blocking(move || {
        // Manually construct the core ID and force the thread onto it
        let core = core_affinity::CoreId { id: core_id.id };

        if core_affinity::set_for_current(core) {
            println!("Successfully pinned worker to isolated core {}", core_id.id);
        } else {
            eprintln!("Failed to pin to core {}. Are you running with sudo?", core_id.id);
        }

        let worker_id = if !runners.is_empty() { runners[0].worker_id() } else { 0 };

        debug!(
            "Worker {} starting with {} pipeline(s) on core {}",
            worker_id,
            runners.len(),
            core_id.id
        );

        // Set the worker's frame registry in thread-local storage
        // This allows all XSocketRx/Tx in this worker to access the registry
        set_worker_frame_registry(&mut frame_registry as *mut _);

        // Set the TSC frequency and start timestamp for timing measurements
        set_tsc_frequency(tsc_freq, tsc_start);

        // Prepare all nodes for startup
        for runner in &mut runners {
            let _ = runner.prepare_nodes();
            log::debug!("Pipeline '{}': full_batch_count: {}", runner.name(), runner.full_batch_count);
        }

        // Note: The actual fill happens on first receive_into_batch call
        // We just log here for visibility

        let mut iteration_count = 0u64;

        log::trace!("Worker {} entering main execution loop", worker_id);

        let num_pipelines = runners.len();

        // State trackers for adaptive polling
        // idle_streaks tracks how many times consecutively a pipeline returned 0
        let mut idle_streaks = vec![0u32; num_pipelines];
        // skip_counters tracks how many loop iterations we should skip this pipeline
        let mut skip_counters = vec![0u32; num_pipelines];
        // Thread-level idle tracker
        let _global_idle_spins = 0u32;
        // The maximum number of consecutive loops to skip a dead pipeline.
        // 32 means we check the veth at least once every 32 physical loops.
        const MAX_SKIP_PENALTY: u32 = 512;
        // How many times to greedily re-run a busy pipeline before yielding
        const MAX_CONSECUTIVE_BURSTS: u32 = 8;

        loop {
            // 1. Check shutdown signal
            if shutdown.load(Ordering::Acquire) {
                debug!( "Worker {} received shutdown signal after {} iterations",
                    worker_id, iteration_count
                );
                break;
            }

            // 2. ONLY shut down automatically if we are in one-shot mode (ipblower)
            if is_one_shot {
                // Check if all generators in this worker are finished
                let mut any_generator = false;
                let mut all_generators_finished = true;
                for runner in &runners {
                    if runner.has_generator() {
                        any_generator = true;
                        if !runner.has_all_finished_generators() {
                            all_generators_finished = false;
                            break;
                        }
                    }
                }

                if any_generator && all_generators_finished {
                    debug!("Worker {}: All PacketGenerators finished, worker shutting down", worker_id);
                    break;
                }
            }

            // 2. Process frame returns from the bus (Keep this!)
            frame_registry.process_returns();

            // 3. Adaptive Execution of pipelines
            let mut _work_done_this_tick = false;

            for i in 0..num_pipelines {
                // If this pipeline is being penalized for being empty, skip it
                if skip_counters[i] > 0 {
                    skip_counters[i] -= 1;
                    continue;
                }

                // Run the iteration and see how much work it did
                let mut packets_processed = runners[i].run_iteration().0 as usize;

                if packets_processed > 0 {
                    _work_done_this_tick = true; // Mark that the core did actual work
                    // TRAFFIC DETECTED! Reset all penalties.
                    idle_streaks[i] = 0;
                    skip_counters[i] = 0;

                    // GREEDY DRAIN: If it pulled a FULL batch, there is likely more traffic
                    // waiting in the NIC ring right now. Don't wait for the next loop!
                    // Keep draining it to prevent hardware drops.

                    let mut bursts = 1;

                    while packets_processed == runners[i].full_batch_count && bursts < MAX_CONSECUTIVE_BURSTS {
                        // CRITICAL: Empty the return channel so we don't deadlock!
                        frame_registry.process_returns();
                        packets_processed = runners[i].run_iteration().0 as usize;
                        bursts += 1;
                    }
                } else {
                    // PIPELINE IS EMPTY. Apply exponential backoff.
                    // 1st empty -> skip 1 loop. 2nd empty -> skip 2 loops. 3rd -> skip 4...
                    idle_streaks[i] += 1;

                    // Calculate penalty (powers of 2), capped at MAX_SKIP_PENALTY
                    let penalty = 1 << std::cmp::min(idle_streaks[i], 9); // 1, 2, 4, 8, 16, 32, ...
                    skip_counters[i] = std::cmp::min(penalty, MAX_SKIP_PENALTY);
                }
            }
/*
            // =======================================================
            // GLOBAL CPU BACKOFF
            // =======================================================
            if work_done_this_tick {
                global_idle_spins = 0;
            } else {
                global_idle_spins += 1;

                if global_idle_spins < 1000 {
                    // Phase 1: Pause the CPU pipeline (nanoseconds).
                    // Drops thermal load without losing thread context.
                    std::hint::spin_loop();
                } else {
                    // Phase 2: Deep idle (microseconds).
                    // The whole core is dead. Yield to Linux.
                    std::thread::yield_now();
                }
            }
*/
            iteration_count += 1;
        }

        // Collect final statistics
        let stats: Vec<PipelineStats> = runners.iter_mut().map(|r| r.stats()).collect();

        log::trace!("Worker {} stopped after {} iterations", worker_id, iteration_count);

        // CRITICAL SHUTDOWN SEQUENCE:
        // 1. Clear thread-local pointer FIRST to prevent any remaining operations from using it
        log::trace!("Worker {}: Clearing thread-local frame registry pointer", worker_id);
        set_worker_frame_registry(std::ptr::null_mut());

        // 2. Drop runners (and their nodes/sockets) - they can no longer access frame_registry
        log::debug!("Worker {}: Dropping pipeline runners", worker_id);
        drop(runners);

        // ==========================================================
        // 🚨 YIELD TO LINUX KERNEL 🚨
        // It gives the Mellanox/Intel driver's asynchronous background workqueue
        // enough time to execute xsk_destruct() and physically detach the hardware
        // pool before we destroy the memory it relies on.
        // ==========================================================
        std::thread::sleep(std::time::Duration::from_millis(50));

        // 3. Finally drop the frame registry safely
        log::debug!("Worker {}: Dropping frame registry", worker_id);
        drop(frame_registry);

        // 4. Increment the workers completed counter (all workers increment, not just generator workers)
        workers_completed.fetch_add(1, Ordering::SeqCst);

        info!("Worker {}: Shutdown complete", worker_id);
        Ok(stats)
    })
}
