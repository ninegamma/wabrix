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
                debug!("Node {}: Sent {} packets", self.id, sent_count);

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


/// A sink node that intentionally discards all packets it receives.
/// Useful for:
/// 1. Terminating "unmatched" traffic from a Classifier (Default Route).
/// 2. Implementing firewall "Deny" rules.
/// 3. Debugging/Testing throughput (Blackhole).
pub struct DropNode {
    id: NodeId,
    dropped_count: u64,
    /// Shared atomic counter published to the telemetry registry so the GUI
    /// can display the running dropped-packet total in real time.
    telemetry_counter: Option<Arc<AtomicU64>>,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl DropNode {
    pub fn new(id: NodeId, telemetry_counter: Option<Arc<AtomicU64>>) -> Self {
        Self {
            id,
            dropped_count: 0,
            telemetry_counter,
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
            if let Some(counter) = &self.telemetry_counter {
                counter.fetch_add(count as u64, Ordering::Relaxed);
            }
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

use rand::{TryRng};
use rand::rngs::SmallRng;
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use std::mem::MaybeUninit;
use xbricks_base::frame_registry::get_worker_frame_registry;
use xbricks_base::{Directive, Node, NodeId};


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
