use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use core_affinity::CoreId;
use log::{debug, error, info};
use crate::toml_parser::PipelineInstance;
use xbricks_base::interface::Pdu;
pub(crate) use crate::nodes::{
    BatchRxNode, BatchTxNode, BucketedTelemetryNode, ClassifierNode, DropNode, DstPortCntNode, FastParserNode,
    L4ClassifierNode, PacketGenNode, get_telemetry_mapper, ExceptionRouterNode,
};
pub(crate) use crate::{BatchRx, BatchTx, MAX_BATCH_SIZE};
use xbricks_base::system::PlatformData;
use separator::Separatable;
use xbricks_base::frame_registry::{set_tsc_frequency, set_worker_frame_registry};
use xbricks_base::{Directive, Node, NodeId};
use crate::config::hot_config::{HotNodeData, HotPipelineConfig};
#[cfg(feature = "wasm")]
use xbricks_wasm::wasm_manager::WasmManager;
use xbricks_base::telemetry::TelemetryRegistry;

type ClassifierFn = Box<dyn Fn(&Pdu) -> usize + Send>;

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

pub fn get_classifier_algorithm(name: &str) -> ClassifierFn {
    match name {
        "ipv4_filter" => Box::new(|pdu| {
            // Route 0 = IPv4 packets, everything else goes to default
            if pdu.is_ipv4() { 0 } else { 99 }
        }),
        "ipv4_split" => Box::new(|pdu| {
            // Example: Route 0 = IPv4, Route 1 = IPv6, Default = Other
            if pdu.is_ipv4() {
                0
            } else if pdu.is_ipv6() {
                1
            } else {
                99
            }
        }),
        "mac_filter" => Box::new(|_pdu| {
            // Example check
            0
        }),
        // Add more logic here
        _ => panic!("Unknown classifier algorithm: {}", name),
    }
}

pub struct PipelineFactory;

impl PipelineFactory {
    pub fn build(
        pipeline_instance: &PipelineInstance,
        worker_id: usize,
        xsocket_mgr: &mut dyn XSocketRxTxProvider,
        mut telemetry_registry: Option<&mut TelemetryRegistry>,
        mut dst_port_stats_registry: Option<&mut Vec<Arc<crate::nodes::DstPortStats>>>,
        platform_data: &PlatformData,
        #[cfg(feature = "wasm")]
        wasm_managers: &HashMap<usize, Arc<WasmManager>>,
        // NEW: Pass the shared state so nodes can hold a reference to it
        shared_state: Arc<crate::config::hot_config::SharedSystemState>,
    ) -> Result<Vec<Box<dyn Node>>, String> {
        // --- PASS 1: ID Resolution ---
        // Map "string_id" -> NodeId
        // The vector index will be the NodeId index.
        let mut id_map: HashMap<String, NodeId> = HashMap::new();
        for (index, node_conf) in pipeline_instance.nodes.iter().enumerate() {
            if id_map.contains_key(&node_conf.id) {
                return Err(format!("Duplicate Node ID: {}", node_conf.id));
            }
            id_map.insert(node_conf.id.clone(), NodeId::new(index as u32, node_conf.id.clone()));
        }

        // =========================================================
        // --- NEW PASS 1.2: Populate Initial Hot Configuration ---
        // =========================================================
        // Grab the specific RCU pointer for this pipeline
        let pipeline_rcu = shared_state.pipelines
            .get(&pipeline_instance.name)
            .expect("Pipeline RCU state not initialized!")
            .clone();

        // Create a vector pre-filled with defaults, sized exactly to our node count
        let mut fast_nodes = vec![HotNodeData::default(); pipeline_instance.nodes.len()];
        let mut name_to_index = HashMap::new();

        for (index, node_conf) in pipeline_instance.nodes.iter().enumerate() {
            name_to_index.insert(node_conf.id.clone(), index);

            // Resolve string routes to fast NodeIds
            let mut resolved_routes = Vec::new();
            for r_str in &node_conf.routes {
                let rid = id_map.get(r_str)
                    .ok_or(format!("Route target '{}' not found", r_str))?
                    .clone();
                resolved_routes.push(rid);
            }

            let resolved_default = if let Some(def_str) = &node_conf.default {
                Some(id_map.get(def_str).unwrap().clone())
            } else {
                None
            };

            // Place it directly at the index!
            fast_nodes[index] = HotNodeData {
                routes: resolved_routes,
                default_route: resolved_default,
                args: node_conf.args.clone(),
            };
        }

        let initial_hot_config = HotPipelineConfig {
            fast_nodes,
            name_to_index
        };
        pipeline_rcu.store(Arc::new(initial_hot_config));


        // =========================================================
        // --- PASS 1.5: Collect interface information from nodes ---
        // Map node_id -> interface name (for BatchTxNode and BatchRxNode)
        let mut node_interfaces: HashMap<String, String> = HashMap::new();
        for node_conf in &pipeline_instance.nodes {
            if node_conf.node_type == "BatchTxNode" || node_conf.node_type == "BatchRxNode" {
                if let Some(iface) = node_conf.args.get("interface") {
                    node_interfaces.insert(node_conf.id.clone(), iface.clone());
                }
            }
        }

        // Validate that we have a node list
        let mut built_nodes: Vec<Option<Box<dyn Node>>> = Vec::with_capacity(pipeline_instance.nodes.len());
        // Fill with None initially so we can insert at specific indices
        for _ in 0..pipeline_instance.nodes.len() {
            built_nodes.push(None);
        }

        // --- PASS 2: Instantiation ---
        for node_conf in &pipeline_instance.nodes {
            let my_id = id_map[&node_conf.id].clone();

            let node_impl: Box<dyn Node> = match node_conf.node_type.as_str() {
                // === 1. RX Node ===
                "BatchRxNode" => {
                    let next_str = node_conf
                        .next
                        .as_ref()
                        .ok_or(format!("Node {} missing 'next'", node_conf.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_conf.id, next_str))?
                        .clone();

                    let iface = node_conf
                        .args
                        .get("interface")
                        .ok_or(format!("RxNode {} missing 'interface' arg", node_conf.id))?;

                    assert_eq!(node_conf.queues.len(), 1);
                    let queue_id = node_conf.queues[0] as usize;

                    // Parse batch_size from args (default: 64, max: MAX_BATCH_SIZE)
                    let batch_size = node_conf
                        .args
                        .get("batch_size")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(64);
                    debug!("BatchRxNode {}: batch_size={}", node_conf.id, batch_size);

                    // Validate batch_size
                    if batch_size == 0 {
                        return Err(format!("BatchRxNode {}: batch_size must be > 0", node_conf.id));
                    }
                    if batch_size > MAX_BATCH_SIZE {
                        return Err(format!(
                            "BatchRxNode {}: batch_size ({}) exceeds maximum of {}",
                            node_conf.id, batch_size, MAX_BATCH_SIZE
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
                    let next_str = node_conf
                        .next
                        .as_ref()
                        .ok_or(format!("Node {} missing 'next'", node_conf.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_conf.id, next_str))?
                        .clone();

                    // Get UMEM ID
                    if node_conf.umem.is_empty() {
                        return Err(format!("PacketGenerator {} missing 'umem' field", node_conf.id));
                    }
                    let umem_group_id = node_conf.umem[0];
                    let pps = node_conf.pps.unwrap_or(1_000);
                    // Get packets per iteration (default to 32, max 128)
                    let batch_size = node_conf
                        .args
                        .get("batch_size")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(32);

                    // Validate batch size
                    if batch_size > MAX_BATCH_SIZE {
                        return Err(format!(
                            "PacketGenerator {}: batch_size ({}) exceeds maximum of {}",
                            node_conf.id, batch_size, MAX_BATCH_SIZE
                        ));
                    }

                    // Get packets_to_generate (default to 0 = unlimited)
                    let packets_to_generate = node_conf
                        .args
                        .get("packets_to_generate")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);

                    // Find the eventual sink node by traversing the chain
                    use macaddr::MacAddr6;
                    let src_mac = {
                        let mut current_node_id = next_str.clone();
                        let mut visited = std::collections::HashSet::new();
                        let mut found_interface: Option<String> = None;

                        // Traverse the chain to find a BatchTxNode
                        while let Some(current_node) = pipeline_instance.nodes.iter().find(|n| n.id == current_node_id) {
                            if visited.contains(&current_node_id) {
                                debug!("PacketGenerator {}: Cycle detected in node chain", node_conf.id);
                                break;
                            }
                            visited.insert(current_node_id.clone());

                            // Check if this node has an interface (BatchTxNode or BatchRxNode)
                            if let Some(iface) = node_interfaces.get(&current_node_id) {
                                found_interface = Some(iface.clone());
                                break;
                            }

                            // Move to next node if it exists
                            if let Some(next) = &current_node.next {
                                current_node_id = next.clone();
                            } else {
                                // Reached end of chain
                                break;
                            }
                        }

                        // Try to get MAC address from found interface
                        if let Some(iface) = found_interface {
                            use xbricks_base::system::get_mac_from_ifname;
                            match get_mac_from_ifname(&iface) {
                                Ok(mac) => {
                                    debug!("PacketGenerator {}: Using MAC {} from interface {}", node_conf.id, mac, iface);
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
                                node_conf.id
                            );
                            MacAddr6::new(0, 0, 0, 0, 0, 0)
                        }
                    };
                    // Parse destination IP from args (default to 192.168.177.1)
                    let dst_ip = node_conf
                        .args
                        .get("dst_ip")
                        .and_then(|s| s.parse::<std::net::Ipv4Addr>().ok())
                        .unwrap_or(std::net::Ipv4Addr::new(192, 168, 177, 1));

                    // Parse destination MAC from args (default to 08:bf:b8:0a:0b:0c)
                    let dst_mac = node_conf
                        .args
                        .get("dst_mac")
                        .and_then(|s| {
                            let parts: Vec<&str> = s.split(':').collect();
                            if parts.len() == 6 {
                                let bytes: Result<Vec<u8>, _> = parts.iter().map(|p| u8::from_str_radix(p, 16)).collect();
                                bytes.ok().map(|b| MacAddr6::new(b[0], b[1], b[2], b[3], b[4], b[5]))
                            } else {
                                None
                            }
                        })
                        .unwrap_or(MacAddr6::new(0x08, 0xbf, 0xb8, 0x0a, 0x0b, 0x0c));

                    const PATCH_CYCLES: u64 = 2000;
                    let min_interbatch_gap =
                        (platform_data.tsc_frequency / pps as u64 * batch_size as u64).saturating_sub(PATCH_CYCLES);
                    debug!(
                        "PacketGenerator {}: pps={}, batch_size={}, packets_to_generate={}, min_interbatch_gap={}, src_mac={}, dst_ip={}, dst_mac={}",
                        node_conf.id, pps, batch_size, packets_to_generate, min_interbatch_gap, src_mac, dst_ip, dst_mac
                    );
                    Box::new(PacketGenNode::new(
                        my_id.clone(),
                        next_id,
                        umem_group_id,
                        worker_id,
                        batch_size,
                        min_interbatch_gap,
                        packets_to_generate,
                        src_mac,
                        dst_ip,
                        dst_mac,
                    ))
                }

                // === 3. TX Node ===
                "BatchTxNode" => {
                    let iface = node_conf
                        .args
                        .get("interface")
                        .ok_or(format!("TxNode {} missing 'interface' arg", node_conf.id))?;

                    assert_eq!(node_conf.queues.len(), 1);
                    let queue_id = node_conf.queues[0] as usize;

                    // Take ownership of the TX component of the socket
                    let tx_sink = xsocket_mgr
                        .take_tx_socket(queue_id, iface)
                        .ok_or(format!("Could not find TX socket for queue {} / iface {}", queue_id, iface))?;

                    Box::new(BatchTxNode::new(my_id.clone(), tx_sink))
                }

                // === 4. Drop Node ===
                "DropNode" => Box::new(DropNode::new(my_id.clone())),

                // === 5. FastParser Node ===
                "FastParserNode" => {
                    let next_str = node_conf
                        .next
                        .as_ref()
                        .ok_or(format!("Node {} missing 'next'", node_conf.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_conf.id, next_str))?
                        .clone();

                    Box::new(FastParserNode::new(my_id.clone(), next_id))
                }

                // === 6. L4Classifier Node ===
                "L4ClassifierNode" => {
                    // This node requires routes array with exactly 2 entries
                    if node_conf.routes.len() != 2 {
                        return Err(format!(
                            "L4ClassifierNode {} requires exactly 2 routes (tcp_udp and other)",
                            node_conf.id
                        ));
                    }

                    // Route 0: TCP/UDP traffic
                    let tcp_udp_route_str = &node_conf.routes[0];
                    let tcp_udp_route = id_map
                        .get(tcp_udp_route_str)
                        .ok_or(format!(
                            "L4ClassifierNode {}: tcp_udp route '{}' not found",
                            node_conf.id, tcp_udp_route_str
                        ))?
                        .clone();

                    // Route 1: Other traffic
                    let other_route_str = &node_conf.routes[1];
                    let other_route = id_map
                        .get(other_route_str)
                        .ok_or(format!(
                            "L4ClassifierNode {}: other route '{}' not found",
                            node_conf.id, other_route_str
                        ))?
                        .clone();

                    Box::new(L4ClassifierNode::new(my_id.clone(), tcp_udp_route, other_route))
                }

                // === 6. L4Classifier Node ===
                "ExceptionRouterNode" => {
                    // This node requires routes array with exactly 2 entries
                    if node_conf.routes.len() != 2 {
                        return Err(format!(
                            "ExceptionRouterNode {} requires exactly 2 routes (tcp_udp and other)",
                            node_conf.id
                        ));
                    }

                    // Route 0: TCP/UDP traffic
                    let fast_path_route_str = &node_conf.routes[0];
                    let fast_path_route = id_map
                        .get(fast_path_route_str)
                        .ok_or(format!(
                            "ExceptionRouterNode {}: tcp_udp route '{}' not found",
                            node_conf.id, fast_path_route_str
                        ))?
                        .clone();

                    // Route 1: Other traffic
                    let exception_route_str = &node_conf.routes[1];
                    let exception_route = id_map
                        .get(exception_route_str)
                        .ok_or(format!(
                            "ExceptionRouterNode {}: other route '{}' not found",
                            node_conf.id, exception_route_str
                        ))?
                        .clone();

                    Box::new(ExceptionRouterNode::new(my_id.clone(), fast_path_route, exception_route))
                }

                // === 7. DstPortCnt Node ===
                "DstPortCntNode" => {
                    let next_str = node_conf
                        .next
                        .as_ref()
                        .ok_or(format!("Node {} missing 'next'", node_conf.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_conf.id, next_str))?
                        .clone();

                    let port_ranges = node_conf
                        .args
                        .get("port_ranges")
                        .ok_or(format!("DstPortCntNode {} missing 'port_ranges' arg", node_conf.id))?;

                    let node = DstPortCntNode::new(my_id.clone(), next_id, port_ranges, node_conf.id.clone());

                    // Register stats for later printing
                    if let Some(registry) = dst_port_stats_registry.as_mut() {
                        registry.push(node.get_stats());
                    }

                    Box::new(node)
                }

                // === 7. Classifier Node ===
                "ClassifierNode" => {
                    // Resolve Routes: ["A", "B"] -> [NodeId(1), NodeId(5)]
                    if node_conf.routes.is_empty() {
                        return Err(format!("Classifier {} missing 'routes'", node_conf.id));
                    }

                    let mut route_ids = Vec::new();
                    for r_str in &node_conf.routes {
                        let rid = id_map
                            .get(r_str)
                            .ok_or(format!("Route target '{}' not found", r_str))?
                            .clone();
                        route_ids.push(rid);
                    }

                    // Resolve Default Route
                    let default_id = if let Some(def_str) = &node_conf.default {
                        Some(
                            id_map
                                .get(def_str)
                                .ok_or(format!("Default target '{}' not found", def_str))?
                                .clone(),
                        )
                    } else {
                        None
                    };

                    // Resolve Algorithm
                    let algo_name = node_conf
                        .args
                        .get("algo")
                        .ok_or(format!("Classifier {} missing 'algo' arg", node_conf.id))?;
                    let closure = get_classifier_algorithm(algo_name);

                    Box::new(ClassifierNode::new(my_id.clone(), route_ids, default_id, closure))
                }

                // === 8. Telemetry Node ===
                "TelemetryNode" => {
                    let next_str = node_conf
                        .next
                        .as_ref()
                        .ok_or(format!("Node {} missing 'next'", node_conf.id))?;
                    let next_id = id_map
                        .get(next_str)
                        .ok_or(format!("Node {} points to unknown next '{}'", node_conf.id, next_str))?
                        .clone();

                    // Get algorithm name
                    let algo_name = node_conf
                        .args
                        .get("algo")
                        .ok_or(format!("TelemetryNode {} missing 'algo' arg", node_conf.id))?;

                    // Get number of buckets
                    let num_buckets = node_conf
                        .args
                        .get("buckets")
                        .ok_or(format!("TelemetryNode {} missing 'buckets' arg", node_conf.id))?
                        .parse::<usize>()
                        .map_err(|_| format!("TelemetryNode {} 'buckets' must be a number", node_conf.id))?;

                    // Parse labels if provided
                    let labels = if let Some(labels_str) = node_conf.args.get("labels") {
                        labels_str.split(',').map(|s| s.trim().to_string()).collect()
                    } else {
                        // Generate default labels (Bucket 0, Bucket 1, etc.)
                        (0..num_buckets).map(|i| format!("Bucket {}", i)).collect()
                    };

                    // Get the mapper function
                    let mapper = get_telemetry_mapper(algo_name);

                    // Create the telemetry node
                    let telemetry_node = BucketedTelemetryNode::new(my_id.clone(), next_id, num_buckets, mapper);

                    // Register with telemetry registry if provided
                    if let Some(registry) = telemetry_registry.as_mut() {
                        let (counters_pkts, counters_bytes) = telemetry_node.get_counters();
                        registry.register(node_conf.id.clone(), labels, counters_pkts, counters_bytes);
                    }

                    Box::new(telemetry_node)
                }

                // === 9. Wasm Inspector Node ===
                #[cfg(feature = "wasm")]
                "WasmInspectorNode" => {
                    use xbricks_wasm::WasmInspectorNode;
                    let wasm_path = node_conf
                        .args
                        .get("wasm_file_path")
                        .ok_or(format!("WasmInspectorNode {} missing 'wasm_file_path' in args", node_conf.id))?;

                    let umem_id = node_conf
                        .args
                        .get("umem")
                        .ok_or(format!("WasmInspectorNode {} missing 'umem' in args", node_conf.id))?
                        .parse::<usize>()
                        .map_err(|_| format!("WasmInspectorNode {} 'umem' must be a number", node_conf.id))?;

                    let manager = wasm_managers
                        .get(&umem_id)
                        .ok_or(format!(
                            "WasmInspectorNode {}: no WasmManager for umem {}",
                            node_conf.id, umem_id
                        ))?;

                    if node_conf.routes.len() != 2 {
                        return Err(format!(
                            "WasmInspectorNode {} requires exactly 2 routes (fast_path and exception_path)",
                            node_conf.id
                        ));
                    }

                    let fast_path_id = id_map
                        .get(&node_conf.routes[0])
                        .ok_or(format!(
                            "WasmInspectorNode {}: fast_path route '{}' not found",
                            node_conf.id, &node_conf.routes[0]
                        ))?
                        .clone();

                    let exception_path_id = id_map
                        .get(&node_conf.routes[1])
                        .ok_or(format!(
                            "WasmInspectorNode {}: exception_path route '{}' not found",
                            node_conf.id, &node_conf.routes[1]
                        ))?
                        .clone();
                    let routing_table = vec![fast_path_id, exception_path_id];

                    let labels = node_conf.labels.clone();
                    let num_buckets = labels.len();
                    let counters_pkts: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();
                    let counters_bytes: Vec<Arc<AtomicU64>> = (0..num_buckets)
                        .map(|_| Arc::new(AtomicU64::new(0)))
                        .collect();

                    if let Some(registry) = telemetry_registry.as_mut() {
                        registry.register(
                            node_conf.id.clone(),
                            labels.clone(),
                            counters_pkts.clone(),
                            counters_bytes.clone(),
                        );
                    }

                    let telemetry_metrics = xbricks_base::telemetry::TelemetryMetrics {
                        node_id: node_conf.id.clone(),
                        labels,
                        counters_pkts,
                        counters_bytes,
                    };

                    Box::new(WasmInspectorNode::new(my_id.clone(), manager, wasm_path, routing_table, None, telemetry_metrics))
                }

                _ => return Err(format!("Unknown Node Type: {}", node_conf.node_type)),
            };

            // Place the built node into the vector at the index corresponding to its NodeId
            built_nodes[usize::from(&my_id)] = Some(node_impl);
        }

        // Unwrap Options (we guaranteed one for every index)
        let final_nodes = built_nodes.into_iter().map(|n| n.unwrap()).collect();
        Ok(final_nodes)
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
    /// Reusable batch container to avoid allocations
    /// Cleared and reused on each execution
    working_batch: crate::Batch,
    /// Statistics
    packets_processed: u64,
    runs_with_work: u64,
    runs_idle: u64,
    full_batch_count: usize,
}

impl PipelineRunner {
    /// Create a new PipelineRunner from a built node DAG
    ///
    /// # Arguments
    /// * `name` - Pipeline name for identification
    /// * `worker_id` - Worker ID this pipeline belongs to
    /// * `nodes` - Vector of nodes where nodes[0] is the root entry point
    ///
    /// # Returns
    /// Result containing the PipelineRunner or an error if nodes is empty
    pub fn new(name: String, worker_id: usize, nodes: Vec<Box<dyn Node>>) -> Result<Self, String> {
        if nodes.is_empty() {
            return Err("Pipeline must have at least one node".to_string());
        }

        let full_batch_count = nodes[0].full_batch_count().unwrap_or(MAX_BATCH_SIZE);

        Ok(PipelineRunner {
            name,
            worker_id,
            nodes,
            working_batch: crate::Batch::new(),
            packets_processed: 0,
            runs_with_work: 0,
            runs_idle: 0,
            full_batch_count,
        })
    }

    /// Execute one iteration of the pipeline
    ///
    /// This follows the Run-To-Completion (RTC) model:
    /// 1. Start at root node (index 0)
    /// 2. Process batch and get directive
    /// 3. Follow directive to next node(s)
    /// 4. Continue until Stop directive or error
    ///
    /// Returns (packets_processed, status_code)
    /// - packets_processed: number of packets handled in this run
    /// - status_code: 0 for success, negative for errors, positive for specific statuses
    fn run_iteration(&mut self) -> (u32, i32) {

        // Clear the working batch from previous iteration
        self.working_batch.clear();

        // Start execution at the root node (index 0)
        let root_node = &mut self.nodes[0];
        let mut directive = root_node.process(&mut self.working_batch);

        // Track packets in the initial batch
        let initial_packet_count = self.working_batch.len() as u32;

        // Follow the directive chain through the DAG
        loop {
            match directive {
                // Continue to next node with current batch
                Directive::Next(next_id) => {
                    let next_idx = usize::from(next_id);

                    if next_idx >= self.nodes.len() {
                        log::error!(
                            "Pipeline '{}': Invalid NodeId {} (max {})",
                            self.name,
                            next_idx,
                            self.nodes.len() - 1
                        );
                        return (initial_packet_count, -1);
                    }

                    let next_node = &mut self.nodes[next_idx];
                    directive = next_node.process(&mut self.working_batch);
                }

                // Batch was split - process each sub-batch recursively
                Directive::Split(branches) => {
                    for (next_id, mut sub_batch) in branches {
                        let next_idx = usize::from(next_id);

                        if next_idx >= self.nodes.len() {
                            log::error!(
                                "Pipeline '{}': Invalid split target NodeId {} (max {})",
                                self.name,
                                next_idx,
                                self.nodes.len() - 1
                            );
                            // Drop sub_batch automatically returns frames to UMEM
                            continue;
                        }

                        // Execute the sub-branch
                        let mut current_directive = self.nodes[next_idx].process(&mut sub_batch);

                        // Follow this sub-branch to completion
                        loop {
                            match current_directive {
                                Directive::Next(branch_next_id) => {
                                    let branch_idx = usize::from(branch_next_id);
                                    if branch_idx >= self.nodes.len() {
                                        log::error!("Pipeline '{}': Invalid branch NodeId {}", self.name, branch_idx);
                                        break;
                                    }
                                    current_directive = self.nodes[branch_idx].process(&mut sub_batch);
                                }
                                Directive::Split(nested_branches) => {
                                    // Nested splits - could happen with multiple classifiers
                                    log::warn!("Pipeline '{}': Nested split detected - processing sequentially", self.name);
                                    // For simplicity, process nested branches sequentially
                                    for (nested_id, mut nested_batch) in nested_branches {
                                        let nested_idx = usize::from(nested_id);
                                        if nested_idx < self.nodes.len() {
                                            let _ = self.nodes[nested_idx].process(&mut nested_batch);
                                            // nested_batch drops here, returning frames
                                        }
                                    }
                                    break;
                                }
                                Directive::Stop => {
                                    // This branch is complete
                                    break;
                                }
                            }
                        }
                        // sub_batch drops here, automatically returning any remaining frames
                    }

                    // All branches processed, this iteration is complete
                    break;
                }

                // Pipeline execution complete (reached sink or drop node)
                Directive::Stop => {
                    break;
                }
            }
        }

        // Update statistics
        if initial_packet_count > 0 {
            self.packets_processed += initial_packet_count as u64;
            self.runs_with_work += 1;
        } else {
            self.runs_idle += 1;
        }

        // Return (packets_processed, status_code)
        // Status 0 = success, 1 = idle (no packets)
        (initial_packet_count, if initial_packet_count > 0 { 0 } else { 1 })
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
                    if let Some(gen_node) = node_ref.downcast_ref::<crate::nodes::PacketGenNode>() {
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
            if let Some(_gen_node) = node_ref.downcast_ref::<crate::nodes::PacketGenNode>() {
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
            if let Some(gen_node) = node_ref.downcast_ref::<crate::nodes::PacketGenNode>() {
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
    pub packets_processed: usize,
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
            if self.packets_processed > 0 {
                let cycles_per_packet = self.profiling_cycles / self.packets_processed as u64;
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
                if self.packets_processed > 0 {
                    let loaded_per_packet = loaded / self.packets_processed as u64;
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
    mut frame_registry: xbricks_base::frame_registry::FrameRegistry,
    shutdown: Arc<AtomicBool>,
    workers_completed: Arc<AtomicUsize>,
    platform_data: &PlatformData,
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
        // The maximum number of consecutive loops to skip a dead pipeline.
        // 32 means we check the veth at least once every 32 physical loops.
        const MAX_SKIP_PENALTY: u32 = 512;
        // How many times to greedily re-run a busy pipeline before yielding
        const MAX_CONSECUTIVE_BURSTS: u32 = 8;

        loop {
            // 1. Check shutdown signal
            if shutdown.load(Ordering::Acquire) {
                debug!(
                    "Worker {} received shutdown signal after {} iterations",
                    worker_id, iteration_count
                );
                break;
            }

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

            // 2. Process frame returns from the bus (Keep this!)
            frame_registry.process_returns();

            // 3. Adaptive Execution of pipelines

            for i in 0..num_pipelines {
                // If this pipeline is being penalized for being empty, skip it
                if skip_counters[i] > 0 {
                    skip_counters[i] -= 1;
                    continue;
                }

                // Run the iteration and see how much work it did
                let mut packets_processed = runners[i].run_iteration().0 as usize;

                if packets_processed > 0 {
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
                    let penalty = 1 << std::cmp::min(idle_streaks[i], 5); // 1, 2, 4, 8, 16, 32
                    skip_counters[i] = std::cmp::min(penalty, MAX_SKIP_PENALTY);
                }
            }

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
