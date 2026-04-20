use std::arch::x86_64::_rdtsc;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use xbricks_base::interface::batch::Batch;

pub struct BatchRxNode {
    id: NodeId,
    next_node: NodeId,
    rx_source: Box<dyn BatchRx + Send>,
    received_packets: usize,
    batch_size: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    #[cfg(feature = "profiling")]
    loaded_cycles: u64,
    #[cfg(feature = "profiling")]
    empty_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_avg_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_total_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_calls: u64,
}

impl BatchRxNode {
    pub fn new(id: NodeId, next_node: NodeId, rx_source: Box<dyn BatchRx + Send>, batch_size: usize) -> Self {
        Self {
            id,
            next_node,
            rx_source,
            received_packets: 0,
            batch_size,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            #[cfg(feature = "profiling")]
            loaded_cycles: 0,
            #[cfg(feature = "profiling")]
            empty_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_avg_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_total_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_calls: 0,
        }
    }
}

impl Node for BatchRxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // 1. Ensure the batch is empty to maximize burst capacity.
        // The Scheduler provides a reused batch container, so we must reset it.
        // Note: internal Pdu pointers were already dropped/returned by the previous run's clean-up.
        batch.clear();

        // 2. Pull from hardware/socket
        let result = match self.rx_source.receive_into_batch(batch, self.batch_size) {
            Ok(count) => {
                if count > 0 {
                    // trace!("Source {}: received {} packets", self.id, count);
                    self.received_packets += count;

                    #[cfg(feature = "profiling")]
                    {
                        let end_cycles = unsafe { _rdtsc() };
                        let elapsed = end_cycles - start_cycles;
                        self.total_cycles += elapsed;
                        self.loaded_cycles += elapsed;
                    }

                    // 3. We have work to do -> Push to the pipeline
                    Directive::Next(self.next_node.clone())
                } else {
                    #[cfg(feature = "profiling")]
                    {
                        let end_cycles = unsafe { _rdtsc() };
                        let elapsed = end_cycles - start_cycles;
                        self.total_cycles += elapsed;
                        self.empty_cycles += elapsed;
                    }

                    // 4. No packets available -> Stop this run immediately
                    Directive::Stop
                }
            }
            Err(e) => {
                // 5. Hardware/Polling error
                // We log it but don't crash the thread. We just stop processing this batch.
                warn!("Source Node {} [{}] error: {}", self.id, self.rx_source.source_id(), e);

                #[cfg(feature = "profiling")]
                {
                    let end_cycles = unsafe { _rdtsc() };
                    let elapsed = end_cycles - start_cycles;
                    self.total_cycles += elapsed;
                    self.empty_cycles += elapsed;
                }

                Directive::Stop
            }
        };

        result
    }

    fn prepare(&mut self) -> anyhow::Result<()> {
        self.rx_source.replenish(1024)
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.received_packets
    }

    fn full_batch_count(&self) -> Option<usize> {
        Some(self.batch_size)
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

impl BatchRxNode {
    #[cfg(feature = "profiling")]
    pub fn loaded_cycles(&self) -> u64 {
        self.loaded_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn empty_cycles(&self) -> u64 {
        self.empty_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_avg_cycles(&self) -> u64 {
        self.replenish_avg_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_total_cycles(&self) -> u64 {
        self.replenish_total_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_calls(&self) -> u64 {
        self.replenish_calls
    }

    #[cfg(feature = "profiling")]
    pub fn update_profiling_from_source(&mut self) {
        self.replenish_avg_cycles = self.rx_source.get_replenish_avg_cycles();
        self.replenish_total_cycles = self.rx_source.get_replenish_total_cycles();
        self.replenish_calls = self.rx_source.get_replenish_calls();
    }
}

pub struct BatchTxNode {
    id: NodeId,
    tx_sink: Box<dyn BatchTx + Send>,
    sent_packets: u64,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl BatchTxNode {
    /// Creates a new BatchTxNode.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this node.
    /// * `tx_sink` - The implementation of the transmitter (e.g., an XSocket).
    pub fn new(id: NodeId, tx_sink: Box<dyn BatchTx + Send>) -> Self {
        Self {
            id,
            tx_sink,
            sent_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for BatchTxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // 1. Check if there is anything to send
        if batch.len() == 0 {
            return Directive::Stop;
        }

        // 2. Attempt transmission
        // The sink is responsible for:
        // - Checking ring capacity (nb_free)
        // - Creating descriptors
        // - Consuming (removing) successful packets from the batch
        let result = match self.tx_sink.transmit_batch(batch) {
            Ok(sent_count) => {
                self.sent_packets += sent_count as u64;
                // Telemetry (optional)
                // trace!("Node {}: Sent {} packets", self.id, sent_count);

                // 3. Handle Partial Sends / Congestion
                // If sent_count < original batch.len(), the remaining packets
                // match the "Tail Drop" behavior.
                //
                // For performance reasons we bulk drop here:
                // Instantly recycle all tail-dropped packets in ~5 cycles!
                batch.bulk_drop();
                Directive::Stop
            }
            Err(e) => {
                // 4. Handle Errors (e.g., Driver down, Ring broken)
                warn!("Node {}: TX Error on sink {}: {}", self.id, self.tx_sink.sink_id(), e);

                // Explicitly clear to ensure resources are returned immediately
                // rather than waiting for Scheduler cleanup.
                batch.clear();

                Directive::Stop
            }
        };

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        result
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.sent_packets as usize
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

use crossbeam_channel::Sender;
use log::{debug, info, warn};
use macaddr::MacAddr6;
use network_types::eth::EtherType;
use crate::{BatchRx, BatchTx};
use xbricks_base::interface::Pdu;
use xbricks_protocol::headers::{EndOffset, Header, HeaderKind, IpHeader, MacHeader, TcpHeader};

pub struct QueueNode {
    id: NodeId,
    /// The sender side of a channel leading to another Scheduler/Core
    tx: Sender<Batch>,
    /// The ID of the node that should process this batch on the destination side
    next_node_id: NodeId,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl QueueNode {
    pub fn new(id: NodeId, tx: Sender<Batch>, next_node_id: NodeId) -> Self {
        Self {
            id,
            tx,
            next_node_id,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for QueueNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // We take ownership of the batch data to move it into the channel.
        // We replace the local batch with an empty one.
        let batch_to_send = std::mem::replace(batch, Batch::new());
        let packet_count = batch_to_send.len();
        // Attempt to send the batch to the next processing stage
        let result = match self.tx.try_send(batch_to_send) {
            Ok(_) => {
                // Return Stop because this thread's RTC path for this batch ends here.
                self.processed_packets += packet_count;
                Directive::Stop
            }
            Err(e) => {
                // If the queue is full, we must drop the batch.
                // Because Batch contains Pdus, and Pdus have a Drop impl,
                // the frames will automatically return to the original UMEM.
                warn!("QueueNode {}: Drop due to congestion: {}", self.id, e);
                Directive::Stop
            }
        };

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        result
    }

    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// A classification function takes a Pdu and returns an index 0..K-1
/// representing the target output branch.
pub type ClassifierFn = Box<dyn Fn(&Pdu) -> usize + Send>;

pub struct ClassifierNode {
    id: NodeId,
    /// Maps class index (0..K-1) to the next NodeId in the DAG
    routes: Vec<NodeId>,
    /// Optional: If None, leftovers are implicitly dropped
    default_route: Option<NodeId>,
    /// The classification logic
    classify: ClassifierFn,
    /// Persistent output buffers (one per route).
    /// We keep them purely to reuse their allocated memory.
    buffers: Vec<Batch>,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl ClassifierNode {
    pub fn new(id: NodeId, routes: Vec<NodeId>, default_route: Option<NodeId>, classify: ClassifierFn) -> Self {
        // Pre-allocate one batch per route
        let buffers = (0..routes.len()).map(|_| Batch::new()).collect();

        Self {
            id,
            routes,
            default_route,
            classify,
            buffers,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for ClassifierNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        let num_classes = self.routes.len();
        self.processed_packets += batch.len();

        // 1. Distribute moves packets from 'batch' -> 'outputs'
        batch.distribute(&mut self.buffers, &self.classify);

        // ... Convert to Directive::Split ...
        let mut split_branches = Vec::with_capacity(num_classes);
        for (i, buffer) in self.buffers.iter_mut().enumerate() {
            if buffer.len() > 0 {
                // Swap out the full buffer to send it down the pipeline
                let mut batch_to_send = Batch::new();
                std::mem::swap(buffer, &mut batch_to_send);
                split_branches.push((self.routes[i].clone(), batch_to_send));
            }
        }

        // 4. Add Default Route (The "Left-Over" Packets)
        if let Some(def_id) = &self.default_route {
            if batch.len() > 0 {
                // We must move the leftovers out of input_batch because
                // input_batch is about to die (end of scope in scheduler).
                // We use replace to swap in an empty batch and take the valid one.
                let leftovers = std::mem::replace(batch, Batch::new());
                split_branches.push((def_id.clone(), leftovers));
            }
        }
        // Else: leftovers stay in input_batch and get Dropped automatically
        // when the scheduler finishes this iteration.

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Split(split_branches)
    }
    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// A sink node that intentionally discards all packets it receives.
/// Useful for:
/// 1. Terminating "unmatched" traffic from a Classifier (Default Route).
/// 2. Implementing firewall "Deny" rules.
/// 3. Debugging/Testing throughput (Blackhole).
pub struct DropNode {
    id: NodeId,
    // Optional: Add a counter for telemetry
    dropped_count: u64,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl DropNode {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            dropped_count: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for DropNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        let count = batch.len();
        if count > 0 {
            self.dropped_count += count as u64;

            // 🚀 BOOM: 1-Call Amortized Bulk Drop!
            batch.bulk_drop();
        }

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Stop
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.dropped_count as usize
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

pub fn get_telemetry_mapper(name: &str) -> Box<dyn Fn(&Pdu) -> usize + Send> {
    match name {
        // Example: Count by Ethertype
        "ethertype" => Box::new(|pdu| {
            // Requires Pdu to parse/expose ethertype
            match pdu.ether_type() {
                0x0800 => 0, // IPv4
                0x86DD => 1, // IPv6
                0x0806 => 2, // ARP
                _ => 3,      // Other
            }
        }),

        // Example: Count specific subnets
        "subnet_monitor" => Box::new(|pdu| {
            if let Some(ip) = pdu.ipv4_src() {
                // 192.168.177.0/24 (Integer match is faster than string parse)
                if (ip & 0xFFFFFF00) == 0xC0A8B100 {
                    return 0;
                }
                // 192.168.176.0/24
                if (ip & 0xFF000000) == 0xC0A8B000 {
                    return 1;
                }
            }
            2 // Default/Internet
        }),

        _ => panic!("Unknown telemetry mapper: {}", name),
    }
}

/// A flexible telemetry node that categorizes packets into buckets
/// and updates specific counters accordingly.
pub struct BucketedTelemetryNode {
    id: NodeId,
    next_node: NodeId,
    processed_packets: usize,

    // Logic to map a PDU to a bucket index (0..N-1)
    mapper: Box<dyn Fn(&Pdu) -> usize + Send>,

    // Vector of counters (Size N)
    // We store Arcs so the Control Plane can hold references to read them
    counters_pkts: Vec<Arc<AtomicU64>>,
    counters_bytes: Vec<Arc<AtomicU64>>,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

use rand::{TryRng};
use rand::rngs::SmallRng;
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::mem::MaybeUninit;
use separator::Separatable;
use xbricks_base::frame_registry::get_worker_frame_registry;
use xbricks_base::{Directive, Node, NodeId};

/// Destination port counter node.
/// Counts packets by destination port based on configured port ranges.
/// Each port in the specified ranges gets its own counter, plus an "other" category for unmatched ports.
pub struct DstPortCntNode {
    id: NodeId,
    next_node: NodeId,
    processed_packets: usize,

    // O(1) Fast-Path lookup tables
    is_tracked: Vec<bool>,              // 64 KB array to instantly check if a port is tracked
    port_counters: Arc<Vec<AtomicU64>>, // 512 KB array of actual packet counters
    other_counter: Arc<AtomicU64>,

    stats: Arc<DstPortStats>,

    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

/// Shared statistics structure for DstPortCntNode
pub struct DstPortStats {
    pub node_id: String,
    pub tracked_ports: Vec<u16>,
    // The shared flat array of 65,536 counters
    pub port_counters: Arc<Vec<AtomicU64>>,
    pub other_counter: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct PortRange {
    start: u16,
    end: u16, // inclusive
}

impl PortRange {
    fn new(start: u16, end: u16) -> Self {
        Self { start, end }
    }

    fn ports(&self) -> Vec<u16> {
        (self.start..=self.end).collect()
    }
}

impl DstPortCntNode {
    pub fn new(id: NodeId, next_node: NodeId, port_ranges_str: &str, node_id_str: String) -> Self {
        let port_ranges = Self::parse_port_ranges(port_ranges_str);

        // 1. Allocate O(1) flat arrays for all 65,536 possible ports
        let mut is_tracked = vec![false; 65536];

        // Initialize exactly 65,536 AtomicU64s to zero
        let counters_vec: Vec<AtomicU64> = std::iter::repeat_with(|| AtomicU64::new(0)).take(65536).collect();
        let port_counters = Arc::new(counters_vec);

        // 2. Mark the tracked ports
        let mut tracked_ports_set = std::collections::HashSet::new();
        for range in &port_ranges {
            for port in range.ports() {
                is_tracked[port as usize] = true;
                tracked_ports_set.insert(port);
            }
        }

        let mut tracked_ports: Vec<u16> = tracked_ports_set.into_iter().collect();
        tracked_ports.sort();

        let other_counter = Arc::new(AtomicU64::new(0));

        // 3. Create the shared stats object
        let stats = Arc::new(DstPortStats {
            node_id: node_id_str,
            tracked_ports,
            port_counters: port_counters.clone(),
            other_counter: other_counter.clone(),
        });

        Self {
            id,
            next_node,
            processed_packets: 0,
            is_tracked,    // <--- NEW: O(1) Tracker
            port_counters, // <--- NEW: O(1) Counters
            other_counter,
            stats,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }

    pub fn get_stats(&self) -> Arc<DstPortStats> {
        self.stats.clone()
    }

    fn parse_port_ranges(ranges_str: &str) -> Vec<PortRange> {
        let mut ranges = Vec::new();

        for part in ranges_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if part.contains('-') {
                // Range format: "start-end"
                let parts: Vec<&str> = part.split('-').collect();
                if parts.len() == 2 {
                    if let (Ok(start), Ok(end)) = (parts[0].trim().parse::<u16>(), parts[1].trim().parse::<u16>()) {
                        if start <= end {
                            ranges.push(PortRange::new(start, end));
                        }
                    }
                }
            } else {
                // Single port
                if let Ok(port) = part.parse::<u16>() {
                    ranges.push(PortRange::new(port, port));
                }
            }
        }

        ranges
    }
}

impl DstPortStats {
    pub fn print_statistics(&self) {
        println!("\n=== Destination Port Statistics ===");
        println!("Node ID: {}", self.node_id);
        println!("\nPer-Port Counters:");

        let mut total = 0u64;

        // Iterate through the sorted list of ports we care about
        for &port in &self.tracked_ports {
            // O(1) direct array lookup!
            let count = self.port_counters[port as usize].load(Ordering::Relaxed);

            // only print the tracked ports if they have traffic
            // (Or you can remove the `if count > 0` check if you want to see the 0s!)
            if count > 0 {
                total += count;
                println!("  Port {}: {} packets", port, count.separated_string());
            } else {
                // If you want it to match your previous logs exactly:
                println!("  Port {}: 0 packets", port);
            }
        }

        let other_count = self.other_counter.load(Ordering::Relaxed);
        total += other_count;

        println!("  Other ports: {} packets", other_count.separated_string());
        println!("\nTotal packets: {}", total.separated_string());
        println!("====================================\n");
    }
}

impl Node for DstPortCntNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { core::arch::x86_64::_rdtsc() };

        let batch_len = batch.len();
        if batch_len == 0 {
            return Directive::Stop;
        }

        let mut local_other_count = 0;

        for pdu in batch.iter() {
            let mut dst_port = None;
            let header_count = pdu.headers().count();
            let umem_addr =  pdu.umem_addr();
            // FAST PATH: Iterate backwards. L4 headers (TCP/UDP) are usually last!
            for i in (0..header_count).rev() {
                match pdu.headers().get(umem_addr, i).kind() {
                    HeaderKind::Tcp => {
                        dst_port = Some(pdu.headers().tcp(umem_addr, i).dst_port());
                        break; // Found it, stop scanning!
                    }
                    HeaderKind::Udp => {
                        dst_port = Some(pdu.headers().udp(umem_addr, i).dst_port());
                        break;
                    }
                    _ => {} // Ignore IP, MAC, etc.
                }
            }

            if let Some(port) = dst_port {
                let port_idx = port as usize;

                // O(1) ARRAY LOOKUP - No HashMaps!
                if self.is_tracked[port_idx] {
                    self.port_counters[port_idx].fetch_add(1, Ordering::Relaxed);
                } else {
                    local_other_count += 1;
                }
            } else {
                local_other_count += 1;
            }
        }

        // Batch-flush the 'other' counter once per batch to save atomic instructions
        if local_other_count > 0 {
            self.other_counter.fetch_add(local_other_count, Ordering::Relaxed);
        }

        self.processed_packets += batch_len;

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { core::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Next(self.next_node.clone())
    }
    
    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// Fast parser node with prefetching optimization.
/// Parses packet headers with data prefetching to minimize cache misses.
pub struct FastParserNode {
    id: NodeId,
    next_node: NodeId,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl FastParserNode {
    pub fn new(id: NodeId, next_node: NodeId) -> Self {
        Self {
            id,
            next_node,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for FastParserNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        let prefetch_dist = 4;

        for i in 0..len {
            // 1. Pre-fetch the data for a future PDU in the batch
            if i + prefetch_dist < len {
                let future_pdu = batch.get(i + prefetch_dist);
                unsafe {
                    let future_addr = future_pdu.data_address(0);
                    _mm_prefetch(future_addr as *const i8, _MM_HINT_T0);
                }
            }

            // 2. Parse the current PDU (using your 7-cycle parse_fast!)
            let current_pdu = batch.get_mut(i);
            current_pdu.parse_fast();

            log::trace!("Current Pdu: {}", current_pdu);
        }

        self.processed_packets += len;

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Next(self.next_node.clone())
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// L4ClassifierNode - Layer 4 Protocol Classifier
///
/// Routes IPv4 packets based on transport layer protocol:
/// - Route 0: IPv4 packets with TCP (protocol 6) or UDP (protocol 17)
/// - Route 1: All other packets (non-IPv4, IPv4 with other protocols, etc.)
///
/// This is a simple binary classifier optimized for separating TCP/UDP traffic
/// from everything else for further processing.
pub struct L4ClassifierNode {
    id: NodeId,
    tcp_udp_route: NodeId,
    other_route: NodeId,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl L4ClassifierNode {
    pub fn new(id: NodeId, tcp_udp_route: NodeId, other_route: NodeId) -> Self {
        Self {
            id,
            tcp_udp_route,
            other_route,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for L4ClassifierNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        self.processed_packets += len;

        // 1. Create empty output batches directly on the stack (Zero cost)
        let mut outputs = [Batch::new(), Batch::new()];

        // 2. ZERO-COPY ROUTING: Steal the PDUs out of the incoming batch
        //    and instantly drop them into the correct output batch.
        batch.distribute(&mut outputs, |pdu| {
            if pdu.is_tcp() || pdu.is_udp() {
                0 // Index 0 goes to TCP/UDP route
            } else {
                1 // Index 1 goes to Other route
            }
        });
        // Note: The incoming `batch` is now completely empty. No double-frees!

        // 3. Extract the filled batches and yield them
        let mut split_branches = Vec::with_capacity(2);

        // Take ownership of the newly filled batches out of the array
        let tcp_udp_batch = std::mem::replace(&mut outputs[0], Batch::new());
        if !tcp_udp_batch.is_empty() {
            split_branches.push((self.tcp_udp_route.clone(), tcp_udp_batch));
        }

        let other_batch = std::mem::replace(&mut outputs[1], Batch::new());
        if !other_batch.is_empty() {
            split_branches.push((self.other_route.clone(), other_batch));
        }

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        if split_branches.is_empty() {
            Directive::Stop
        } else {
            Directive::Split(split_branches)
        }
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

pub struct ExceptionRouterNode {
    id: NodeId,
    fast_path_route: NodeId,
    exception_route: NodeId,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl ExceptionRouterNode {
    pub fn new(id: NodeId, fast_path_route: NodeId, exception_route: NodeId) -> Self {
        Self {
            id,
            fast_path_route,
            exception_route,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for ExceptionRouterNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        self.processed_packets += len;
        let mut outputs = [Batch::new(), Batch::new()];

        // ZERO-COPY ROUTING
        batch.distribute(&mut outputs, |pdu| {
            // Check for Exception Traffic (ARP or ICMP)
            let mut is_exception = false;
            let umem_addr = pdu.umem_addr();
            for i in (0..pdu.headers().count()).rev() {
                let kind = pdu.headers().get(umem_addr, i).kind();
                if kind == HeaderKind::ArpIpv4 || kind == HeaderKind::Icmp {
                    is_exception = true;
                    break;
                }

                // Optional: Route SSH (Port 22) to the namespace
                if kind == HeaderKind::Tcp && pdu.headers().tcp(umem_addr, i).dst_port() == 22 {
                    is_exception = true;
                    break;
                }
            }

            if is_exception {
                1 // Send to Exception Route (veth_sink)
            } else {
                0 // Send to Fast Path (dst_port_counter)
            }
        });

        let mut split_branches = Vec::with_capacity(2);

        let fast_batch = std::mem::replace(&mut outputs[0], Batch::new());
        if !fast_batch.is_empty() {
            split_branches.push((self.fast_path_route.clone(), fast_batch));
        }

        let exception_batch = std::mem::replace(&mut outputs[1], Batch::new());
        if !exception_batch.is_empty() {
            split_branches.push((self.exception_route.clone(), exception_batch));
        }

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        if split_branches.is_empty() {
            Directive::Stop
        } else {
            Directive::Split(split_branches)
        }
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// A packet generator node that creates packets based on a prototype PDU.
/// This is a source node that generates synthetic traffic.
pub struct PacketGenNode {
    id: NodeId,
    next_node: NodeId,
    /// The UMEM group ID to allocate frames from
    umem_group_id: usize,
    /// Worker ID for this generator instance
    worker_id: usize,
    /// Number of packets to generate per batch
    batch_size: usize,
    /// minimum interbatch gap in tsc cycles
    min_interbatch_gap: u64,
    lastbatch_timestamp: u64,
    start_delay: u64,
    start_time: u64,
    packets_to_generate: usize,
    /// Total packets generated per worker
    total_generated: usize,
    /// Flag indicating if generation is complete
    finished: bool,
    /// Timestamp of first packet burst (TSC cycles)
    first_burst_timestamp: u64,
    /// Timestamp of last packet burst (TSC cycles)
    last_burst_timestamp: u64,
    /// Instant of first packet burst (std::time)
    first_burst_instant: Option<std::time::Instant>,
    /// Instant of last packet burst (std::time)
    last_burst_instant: Option<std::time::Instant>,
    rng: SmallRng,
    /// Source MAC address (from interface)
    src_mac: MacAddr6,
    /// Destination IP address
    dst_ip: Ipv4Addr,
    /// Destination MAC address
    dst_mac: MacAddr6,
    next_log_threshold: usize,
    scratch_pdus: [core::mem::MaybeUninit<Pdu>; crate::MAX_BATCH_SIZE],
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl BucketedTelemetryNode {
    pub fn new(id: NodeId, next_node: NodeId, num_buckets: usize, mapper: Box<dyn Fn(&Pdu) -> usize + Send>) -> Self {
        // Initialize N counters
        let mut pkts = Vec::with_capacity(num_buckets);
        let mut bytes = Vec::with_capacity(num_buckets);

        for _ in 0..num_buckets {
            pkts.push(Arc::new(AtomicU64::new(0)));
            bytes.push(Arc::new(AtomicU64::new(0)));
        }

        Self {
            id,
            next_node,
            mapper,
            counters_pkts: pkts,
            counters_bytes: bytes,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }

    // Allow the metrics registry to retrieve the handles
    pub fn get_counters(&self) -> (Vec<Arc<AtomicU64>>, Vec<Arc<AtomicU64>>) {
        (self.counters_pkts.clone(), self.counters_bytes.clone())
    }
}

impl Node for BucketedTelemetryNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        if batch.len() == 0 {
            return Directive::Stop;
        }

        // Iterate through the batch and classify each packet
        for pdu in batch.iter() {
            let bucket_idx = (self.mapper)(pdu);

            // Safety check: ignore invalid indices to prevent panic
            if bucket_idx < self.counters_pkts.len() {
                // Relaxed ordering is sufficient for statistics
                self.counters_pkts[bucket_idx].fetch_add(1, Ordering::Relaxed);

                // Assuming pdu.data_len() gives the packet size
                self.counters_bytes[bucket_idx].fetch_add(pdu.w.data_len() as u64, Ordering::Relaxed);
            }
        }
        self.processed_packets += batch.len();

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Next(self.next_node.clone())
    }
    #[inline]
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

impl PacketGenNode {
    pub fn new(
        id: NodeId,
        next_node: NodeId,
        umem_group_id: usize,
        worker_id: usize,
        batch_size: usize,
        min_interbatch_gap: u64,    // minimum interbatch_gap in tsc cycles
        packets_to_generate: usize, // 0: no limit
        src_mac: MacAddr6,
        dst_ip: Ipv4Addr,
        dst_mac: MacAddr6,
    ) -> Self {
        Self {
            id,
            next_node,
            umem_group_id,
            worker_id,
            min_interbatch_gap,
            lastbatch_timestamp: 0,
            start_delay: 0,
            start_time: 0,
            batch_size: batch_size,
            packets_to_generate,
            total_generated: 0,
            finished: false,
            first_burst_timestamp: 0,
            last_burst_timestamp: 0,
            first_burst_instant: None,
            last_burst_instant: None,
            rng: rand::make_rng(),
            src_mac,
            dst_mac,
            dst_ip,
            next_log_threshold: 0,
            scratch_pdus: unsafe { MaybeUninit::uninit().assume_init() },
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }

    /// Initializes PDU by writing headers directly into the frame at the current data pointer,
    /// without consuming headroom. This leaves headroom available for future header additions.
    fn initialize_pdu(&mut self, pdu: &mut Pdu, seqn: u32, dst_port: u16) -> anyhow::Result<()> {
        // Create headers
        let mut mac = MacHeader::new();
        mac.src = self.src_mac;
        mac.dst = self.dst_mac;
        mac.set_ethertype(EtherType::Ipv4);

        let mut ip = IpHeader::new();
        ip.set_src(self.rng.try_next_u32()?);
        ip.set_dst_address(self.dst_ip);
        ip.set_ttl(128);
        ip.set_version(4);
        ip.set_protocol(6); //tcp
        ip.set_ihl(5);
        ip.set_length(40);
        ip.set_flags(0x2); // DF=1, MF=0 flag: don't fragment

        let mut tcp = TcpHeader::new();
        tcp.set_syn_flag();
        tcp.set_src_port(111);
        tcp.set_dst_port(dst_port);
        tcp.set_data_offset(5);
        tcp.set_seq_num(seqn);

        // Calculate total packet size
        let mac_size = MacHeader::size();
        let ip_size = IpHeader::size();
        let tcp_size = TcpHeader::size();
        let total_size = mac_size + ip_size + tcp_size;

        let umem_addr = pdu.umem_addr();
        // Write headers directly into the frame at current data pointer
        let data_ptr = pdu.data_ptr(umem_addr);

        unsafe {
            // Write MAC header
            let mac_ptr = data_ptr as *mut MacHeader;
            std::ptr::copy_nonoverlapping(&mac as *const MacHeader, mac_ptr, 1);

            // Write IP header
            let ip_ptr = data_ptr.add(mac_size) as *mut IpHeader;
            std::ptr::copy_nonoverlapping(&ip as *const IpHeader, ip_ptr, 1);

            // Write TCP header
            let tcp_ptr = data_ptr.add(mac_size + ip_size) as *mut TcpHeader;
            std::ptr::copy_nonoverlapping(&tcp as *const TcpHeader, tcp_ptr, 1);


            // Initialize HeaderStack with the correct offsets
            pdu.headers_mut().push(umem_addr, Header::Mac(&mut *mac_ptr));
            pdu.headers_mut().push(umem_addr, Header::Ip(&mut *ip_ptr));
            pdu.headers_mut().push(umem_addr, Header::Tcp(&mut *tcp_ptr));
        }

        // Update PDU data length
        pdu.w.set_data_length(total_size as u32);
        // log::trace!("Initialized PDU with seqn {}, dst_port {}: {}", seqn, dst_port, pdu);
        Ok(())
    }

    pub fn get_stats(&self) -> usize {
        self.total_generated
    }

    /// Check if the generator has finished generating all required packets
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Get the duration of actual packet burst generation in TSC cycles
    pub fn get_burst_duration_cycles(&self) -> u64 {
        if self.last_burst_timestamp > self.first_burst_timestamp {
            self.last_burst_timestamp - self.first_burst_timestamp
        } else {
            0
        }
    }

    /// Get the duration of actual packet burst generation using std::time
    pub fn get_burst_duration(&self) -> Option<std::time::Duration> {
        if let (Some(first), Some(last)) = (self.first_burst_instant, self.last_burst_instant) {
            Some(last.duration_since(first))
        } else {
            None
        }
    }
}

impl Node for PacketGenNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // Check if already finished
        if self.finished {
            return Directive::Stop;
        }

        let now = unsafe { _rdtsc() };
        if self.start_time == 0 {
            self.start_time = now;
        }

        // Check if we've reached the packet limit
        if self.packets_to_generate > 0 && self.total_generated >= self.packets_to_generate {
            self.finished = true;
            self.last_burst_instant = Some(std::time::Instant::now());
            info!(
                "PacketGenerator {}: Finished generating {} packets",
                self.id, self.total_generated
            );
            return Directive::Stop;
        }

        // Check timing constraints
        if !((now - self.lastbatch_timestamp) >= self.min_interbatch_gap && (now - self.start_time) >= self.start_delay) {
            return Directive::Stop;
        }

        // Ensure the batch is empty
        batch.clear();

        // Access the worker's frame registry from thread-local storage
        let registry_ptr = get_worker_frame_registry();
        if registry_ptr.is_null() {
            warn!("PacketGenNode {}: Frame registry not available", self.id);
            return Directive::Stop;
        }
        let frame_registry = unsafe { &mut *registry_ptr };

        // Allocate frames and get PDUs
        // Calculate how many packets we can still generate to not exceed the limit
        let remaining = if self.packets_to_generate > 0 {
            self.packets_to_generate.saturating_sub(self.total_generated)
        } else {
            usize::MAX
        };
        let frames_to_generate = std::cmp::min(std::cmp::min(self.batch_size, remaining), crate::MAX_BATCH_SIZE);

        // Don't allocate if we've reached the limit
        if frames_to_generate == 0 {
            self.finished = true;
            info!(
                "PacketGenerator {}: Finished generating {} packets",
                self.id, self.total_generated
            );
            return Directive::Stop;
        }

        // here we allocate pdus using descriptors from the umem group
        let allocated = frame_registry.allocate_pdus_into(self.umem_group_id, &mut self.scratch_pdus[..frames_to_generate]);

        if allocated == 0 {
            return Directive::Stop;
        }

        let mut generated = 0;
        // Process each allocated PDU
        for i in 0..allocated {
            // Read the PDU out of the uninit slot (leaves the raw bytes behind, but Rust won't drop them)
            let mut pdu = unsafe { self.scratch_pdus[i].assume_init_read() };
            let seq_num = self.total_generated as u32 + generated;
            let dst_port = self.worker_id as u16;
            if let Err(e) = self.initialize_pdu(&mut pdu, seq_num, dst_port) {
                warn!("Failed to create headers: {:?}", e);
                // If it fails, the PDU (and its frame) will drop here and be released.
                continue;
            }

            // debug!("PacketGenNode {}: Generated packet { }", self.id, pdu);
            // Add to batch
            if batch.push(pdu).is_ok() {
                generated += 1;
            } else {
                // Batch is full, drop remaining PDU (Drop impl will release frame)
                break;
            }
        }

        for pdu in batch.iter() {
            log::trace!("PacketGenNode at worker {}: Generated packet: {}", self.worker_id, pdu);
        }

        self.lastbatch_timestamp = now;

        if generated > 0 {
            // Record first and last burst timestamps for accurate PPS calculation
            //let instant_now = std::time::Instant::now();
            if self.first_burst_timestamp == 0 {
                self.first_burst_timestamp = now;
                self.first_burst_instant = Some(std::time::Instant::now());
            }
            self.last_burst_timestamp = now;

            self.total_generated += generated as usize;

            // Check if we crossed a LOG_INTERVAL boundary
            const LOG_INTERVAL: usize = 1_000_000;

            if self.total_generated >= self.next_log_threshold {
                log::trace!("PacketGenNode {}: Generated {} packets", self.id, self.total_generated);
                self.next_log_threshold += LOG_INTERVAL;
            }

            #[cfg(feature = "profiling")]
            {
                let end_cycles = unsafe { _rdtsc() };
                self.total_cycles += end_cycles - start_cycles;
            }

            Directive::Next(self.next_node.clone())
        } else {
            Directive::Stop
        }
    }
    #[inline]
    fn processed_packets(&self) -> usize {
        self.total_generated
    }

    fn full_batch_count(&self) -> Option<usize> {
        Some(self.batch_size)
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}
