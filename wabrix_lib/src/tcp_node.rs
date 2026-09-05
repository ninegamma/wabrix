use crossbeam_utils::CachePadded;
use std::mem::{replace, size_of, MaybeUninit};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::vec::Vec;

use wabrix_base::frame_registry::get_worker_frame_registry;
use wabrix_base::interface::batch::Batch;
use wabrix_base::interface::Pdu;
use wabrix_base::{ControlEvent, Directive, Node, NodeId};

use wabrix_api::connections::{ConnectionPool, FlowKey, TcpState};
use wabrix_api::exports::Pdu0;
use wabrix_api::headers::{IpHeader, MacHeader, TcpHeader};
use wabrix_api::helpers::{parse_ip, parse_mac, parse_suffixed_number};
use wabrix_api::packet::Packet;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_api::{PluginOpcode, StandardOpcode};

use wabrix_api::tcp_common::{
    run_tcp_state_machine, write_tcp_packet, PacketGenerator, TcpEngineConfig, TimeoutStack,
    FLAG_ACK, FLAG_FIN, FLAG_RST, FLAG_SYN, PORT_END_RANGE, PORT_START_RANGE, STAT_COUNT,
    STAT_DROP, STAT_RECV_RST, STAT_RECV_SYN, STAT_SENT_FIN, STAT_SENT_RST, STAT_SENT_SYN,
    STAT_SENT_SYNACK, STAT_TO_LINUX, STAT_UNEXPECTED,
};

const MAX_CONNECTIONS: usize = 524_288;
const ROUTE_TX: u32 = 0;
const ROUTE_LINUX: u32 = 1;
const ROUTE_DROP: u32 = 2;

/// Creates a stack-only high-level packet view over a native PDU.
///
/// `Pdu` and `Pdu0` intentionally have the same `repr(C)` layout. Native
/// UMEM addresses are already absolute, so the packet view uses a zero base.
#[inline(always)]
unsafe fn packet_view(pdu: &mut Pdu) -> Packet<'_> {
    Packet::new(&mut *(pdu as *mut Pdu as *mut Pdu0), 0)
}

pub struct TcpNode {
    id: NodeId,
    routing_table: Vec<NodeId>,
    umem_group_id: usize,
    config: TcpEngineConfig,
    connections: ConnectionPool,
    timeout_queue: TimeoutStack,
    syn_gen: PacketGenerator,
    current_tsc: u64,
    last_timeout_scan: u64,
    stats: [u64; STAT_COUNT],
    scratch_pdus: [MaybeUninit<Pdu>; MAX_BATCH_SIZE],
    /// Persistent per-route output batches, reused across every `process()` call
    /// to keep the fast path allocation-free. Non-empty batches are swapped out
    /// (with a cheap uninit `Batch::new()`) into the `Directive::Split` result.
    output_batches: Vec<Batch>,
    processed: u64,
    counters_pkts: Vec<Arc<CachePadded<AtomicU64>>>,
    tm_force_sync: Arc<AtomicBool>,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl TcpNode {
    pub fn new(
        id: NodeId,
        routing_table: Vec<NodeId>,
        umem_group_id: usize,
        config_data: Vec<(String, String)>,
        tsc_frequency: u64,
        counters_pkts: Vec<Arc<CachePadded<AtomicU64>>>,
        tm_force_sync: Arc<AtomicBool>,
    ) -> Self {
        let mut config = TcpEngineConfig {
            common: wabrix_api::tcp_common::CommonEngineConfig {
                tsc_frequency,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut raw_src_ip: Option<alloc::string::String> = None;

        for (key, value) in config_data {
            match key.as_str() {
                "name" => config.name = Some(value),
                "src_mac" => config.src_mac = parse_mac(&value),
                "dst_mac" => config.dst_mac = parse_mac(&value),
                "src_ip" => {
                    raw_src_ip = Some(value.clone());
                    config.src_ip = parse_ip(&value);
                }
                "dst_ip" => config.dst_ip = parse_ip(&value),
                "rng_seed" => config.rng_seed = value.parse::<u32>().ok(),
                "syn_rate" => config.syn_rate = parse_suffixed_number(&value).ok(),
                "syn_limit" => config.syn_limit = parse_suffixed_number(&value).ok(),
                "close_timeout" => config.close_timeout_millis = value.parse::<u64>().ok(),
                _ => {}
            }
        }

        // Resolve node name using fallback chain
        let node_name = config
            .name
            .clone()
            .or(raw_src_ip)
            .unwrap_or_else(|| alloc::string::String::from("unknown"));

        config.refresh_timeout_cycles();

        log::debug!("TcpNode '{}': config — syn_rate={:?}, syn_limit={:?}, src_ip={:?}, dst_ip={:?}, close_timeout_ms={:?}, tsc_freq={}",
            id.id, config.syn_rate, config.syn_limit, config.src_ip, config.dst_ip,
            config.close_timeout_millis, tsc_frequency);
        let num_routes = routing_table.len();
        Self {
            id,
            routing_table,
            umem_group_id,
            config,
            connections: ConnectionPool::new(MAX_CONNECTIONS, node_name.clone()),
            timeout_queue: TimeoutStack::new(),
            syn_gen: PacketGenerator {
                last_tsc: 0,
                cycle_accumulator: 0,
                port_counter: PORT_START_RANGE,
                ip_offset: 0,
                prng_state: 1,
                packets_sent: 0,
                activated: false,
            },
            current_tsc: 0,
            last_timeout_scan: 0,
            stats: [0; STAT_COUNT],
            scratch_pdus: unsafe { MaybeUninit::uninit().assume_init() },
            output_batches: (0..num_routes).map(|_| Batch::new()).collect(),
            processed: 0,
            counters_pkts,
            tm_force_sync,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }

    pub fn set_state(&mut self, state: u8) {
        match state {
            0 => {
                self.syn_gen.activated = false;
                self.config.bypass = false;
            }
            1 => {
                self.syn_gen.activated = true;
                self.config.bypass = false;
            }
            2 => {
                self.syn_gen.activated = false;
                self.config.bypass = true;
            }
            _ => {}
        }
    }

    pub fn flush_state(&mut self, mode: u8) {
        self.set_state(0);
        self.connections.clear();
        self.timeout_queue.clear();
        self.syn_gen.packets_sent = 0;
        self.syn_gen.cycle_accumulator = 0;
        self.syn_gen.last_tsc = 0;
        if mode == 1 {
            self.syn_gen.port_counter = PORT_START_RANGE;
            self.syn_gen.prng_state = 1;
        }
        self.stats = [0; STAT_COUNT];
        log::trace!("TcpNode state flushed (mode: {})", mode);
    }

    pub fn get_telemetry(&self) -> Vec<u64> {
        let mut stats = self.stats.to_vec();
        stats.push(self.connections.watermark as u64);
        stats
    }

    fn sync_telemetry(&self) {
        for (i, counter) in self.counters_pkts.iter().enumerate() {
            let val = if i < STAT_COUNT {
                self.stats[i]
            } else if i == STAT_COUNT {
                self.connections.watermark as u64
            } else {
                break;
            };
            counter.store(val, Ordering::Relaxed);
        }
    }

    pub fn on_control_event(&mut self, raw_opcode: u32, payload: Vec<u8>) -> bool {
        let opcode = PluginOpcode::from(raw_opcode);
        match opcode {
            PluginOpcode::Standard(StandardOpcode::SetOperationalState) => {
                if payload.len() == 1 {
                    let new_state = payload[0];
                    self.set_state(new_state);
                    log::info!(
                        "TcpNode '{}': state → {} (activated={}, bypass={})",
                        self.id.id,
                        new_state,
                        self.syn_gen.activated,
                        self.config.bypass
                    );
                    true
                } else {
                    false
                }
            }
            PluginOpcode::Standard(StandardOpcode::FlushState) => {
                if !payload.is_empty() {
                    self.flush_state(payload[0]);
                }
                true
            }
            PluginOpcode::Standard(StandardOpcode::Ping) => {
                log::info!("TcpNode: PING received");
                true
            }
            _ => {
                log::warn!("TcpNode: ignored opcode: {:?}", opcode);
                false
            }
        }
    }
}

impl Node for TcpNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        if self.tm_force_sync.load(Ordering::Relaxed) {
            self.tm_force_sync.store(false, Ordering::Relaxed);
            self.sync_telemetry();
        }

        if self.config.bypass {
            self.stats[STAT_TO_LINUX] += batch.len() as u64;
            batch.bulk_drop();
            return Directive::Stop;
        }

        if batch.is_empty() {
            return Directive::Stop;
        }

        let num_routes = self.routing_table.len();

        // Reuse the persistent per-route output batches instead of allocating a
        // fresh `Vec<Batch>` every call. They were emptied at the end of the
        // previous call (batches swapped into the returned Split are replaced by
        // fresh empty ones), but clear again defensively in case the prior
        // Directive was dropped without consuming a branch.
        for out in self.output_batches.iter_mut() {
            out.clear();
        }

        self.processed += batch.len() as u64;

        // Drain the input batch in place, preserving FIFO arrival order (required
        // for correct same-flow TCP state transitions). `Batch::drain` returns a
        // borrowing iterator — no intermediate Vec/memcpy, unlike `.collect()`.
        unsafe {
            for mut pdu in batch.drain(..) {
                pdu.parse_fast();

                // Align with Wasm plugin: Use fingerprint instead of manual field checks
                if pdu.header_stack.fingerprint() != wabrix_api::packet::Packet::TCP_OVER_IPV4 {
                    self.stats[STAT_TO_LINUX] += 1;

                    // Align with Wasm plugin: Route non-TCP traffic to Linux instead of Drop
                    if (ROUTE_LINUX as usize) < num_routes {
                        self.output_batches[ROUTE_LINUX as usize].push(pdu).ok();
                    } else {
                        self.output_batches[ROUTE_DROP as usize].push(pdu).ok();
                    }
                    continue;
                }

                let current_tsc = self.current_tsc;
                let route = handle_tcp_packet(
                    &mut pdu,
                    &mut self.connections,
                    &self.config,
                    &mut self.stats,
                    current_tsc,
                ) as usize;

                if route < num_routes {
                    self.output_batches[route].push(pdu).ok();
                } else {
                    self.stats[STAT_DROP] += 1;
                    self.output_batches[ROUTE_DROP as usize].push(pdu).ok();
                }
            }
        }

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        let mut split_branches = Vec::with_capacity(num_routes);
        for i in 0..num_routes {
            if !self.output_batches[i].is_empty() {
                // Swap the populated batch out with a fresh (uninit, no-heap)
                // empty one so ownership can move into the Directive.
                let branch = replace(&mut self.output_batches[i], Batch::new());
                split_branches.push((self.routing_table[i].clone(), branch));
            }
        }

        if split_branches.is_empty() {
            Directive::Stop
        } else {
            Directive::Split(split_branches)
        }
    }

    fn requires_timers(&self) -> bool {
        true
    }

    /// Called by the runtime on a timer tick. Fills `batch` with generated SYN packets
    /// and timeout teardown FIN/RST packets, then routes them to TX via `next_node`.
    fn poll_timers(&mut self, batch: &mut Batch, current_tsc: u64) -> Directive {
        self.current_tsc = current_tsc;

        // Service the telemetry force-sync here as well as in `process()`.
        // `poll_timers` keeps running once per timer tick even when no traffic
        // flows, whereas this downstream node's `process()` is only called while
        // batches are routed to it. Without this, the final counters (e.g. the
        // moment the SYN target is reached and generation stops) are never
        // published, so the last GUI update — the one showing the target hit —
        // is missing even though the run completed.
        if self.tm_force_sync.load(Ordering::Relaxed) {
            self.tm_force_sync.store(false, Ordering::Relaxed);
            self.sync_telemetry();
        }

        let tsc_freq = self.config.tsc_frequency;
        if tsc_freq == 0 {
            return Directive::Stop;
        }

        batch.clear();

        // === 1. TIMEOUT SCAVENGER (runs every 10 ms) ===
        if let Some(timeout_cycles) = self.config.timeout_cycles {
            if current_tsc.saturating_sub(self.last_timeout_scan) > (tsc_freq / 100) {
                self.last_timeout_scan = current_tsc;

                let available_capacity = self.timeout_queue.available_capacity();
                if available_capacity > 0 {
                    self.connections.scavenge_timeouts(
                        current_tsc,
                        timeout_cycles,
                        available_capacity,
                        &mut self.timeout_queue,
                    );
                }
            }
        }

        // === 2. Calculate how many frames we need ===
        let timeout_count = self.timeout_queue.len().min(2 * MAX_BATCH_SIZE / 3);

        let syn_count = {
            let syn_rate = self.config.syn_rate.unwrap_or(0);
            if syn_rate > 0 && self.syn_gen.activated {
                let allowed = match self.config.syn_limit {
                    Some(max) => self.syn_gen.packets_sent < max || max == 0,
                    None => true,
                };
                if allowed {
                    if self.syn_gen.last_tsc == 0 {
                        // First tick after activation: log once, then latch the
                        // timestamp so this branch (and the log) never repeats.
                        // Guarding on `syn_rate > 0` keeps a syn_rate=0 responder
                        // (e.g. the TCP server node) from spamming this line every
                        // poll_timers tick, which floods stdout and starves the
                        // async telemetry tasks.
                        log::info!(
                            "TcpNode '{}': SYN generator activated — syn_rate={}, src_ip={:?}, dst_ip={:?}",
                            self.id.id,
                            syn_rate,
                            self.config.src_ip,
                            self.config.dst_ip
                        );
                        self.syn_gen.last_tsc = current_tsc;
                    }
                    let delta = current_tsc.saturating_sub(self.syn_gen.last_tsc);
                    self.syn_gen.last_tsc = current_tsc;
                    self.syn_gen.cycle_accumulator = self
                        .syn_gen
                        .cycle_accumulator
                        .saturating_add(delta.saturating_mul(syn_rate));
                    let mut n = (self.syn_gen.cycle_accumulator / tsc_freq) as u32;
                    if let Some(max) = self.config.syn_limit {
                        let remaining = max.saturating_sub(self.syn_gen.packets_sent);
                        if (n as u64) > remaining {
                            n = remaining as u32;
                        }
                    }
                    self.syn_gen.cycle_accumulator %= tsc_freq;
                    log::trace!("TcpNode poll_timers: {} SYNs scheduled", n);
                    (n as usize).min(MAX_BATCH_SIZE.saturating_sub(timeout_count))
                } else {
                    0
                }
            } else {
                0
            }
        };

        let frames_needed = timeout_count + syn_count;
        if frames_needed == 0 {
            return Directive::Stop;
        }

        // === 3. Allocate frames ===
        let registry_ptr = get_worker_frame_registry();
        if registry_ptr.is_null() {
            return Directive::Stop;
        }
        let frame_registry = unsafe { &mut *registry_ptr };
        let allocated = frame_registry
            .allocate_pdus_into(self.umem_group_id, &mut self.scratch_pdus[..frames_needed]);
        if allocated == 0 {
            return Directive::Stop;
        }

        let reply_src_mac = self.config.src_mac.unwrap_or([0; 6]);
        let reply_dst_mac = self.config.dst_mac.unwrap_or([0xff; 6]);
        let mut frame_idx = 0;
        let mut generated = 0;

        // === Priority 1: Timeout teardowns ===
        while frame_idx < allocated && frame_idx < timeout_count {
            let flow_key = match self.timeout_queue.pop() {
                Some(k) => k,
                None => break,
            };

            let mut pdu = unsafe { self.scratch_pdus[frame_idx].assume_init_read() };
            frame_idx += 1;

            let (snd_nxt, rcv_nxt, state, exists) = {
                if let Some(conn) = self.connections.get_mut_and_touch(&flow_key, current_tsc) {
                    (conn.snd_nxt, conn.rcv_nxt, conn.state, true)
                } else {
                    (0, 0, TcpState::Closed, false)
                }
            };

            if !exists {
                drop(pdu);
                continue;
            }

            unsafe {
                if state == TcpState::Established || state == TcpState::SynReceived {
                    if let Some(conn) = self.connections.get_mut(&flow_key) {
                        conn.state = TcpState::FinWait1;
                        conn.snd_nxt = conn.snd_nxt.wrapping_add(1);
                    }
                    let mut packet = packet_view(&mut pdu);
                    write_tcp_packet(
                        &mut packet,
                        reply_src_mac,
                        reply_dst_mac,
                        flow_key.2,
                        flow_key.3,
                        flow_key.0,
                        flow_key.1,
                        snd_nxt,
                        rcv_nxt,
                        FLAG_FIN | FLAG_ACK,
                    );
                    self.stats[STAT_SENT_FIN] += 1;
                    batch.push(pdu).ok();
                    generated += 1;
                } else {
                    self.connections.remove(&flow_key);
                    let mut packet = packet_view(&mut pdu);
                    write_tcp_packet(
                        &mut packet,
                        reply_src_mac,
                        reply_dst_mac,
                        flow_key.2,
                        flow_key.3,
                        flow_key.0,
                        flow_key.1,
                        snd_nxt,
                        rcv_nxt,
                        FLAG_RST | FLAG_ACK,
                    );
                    self.stats[STAT_SENT_RST] += 1;
                    batch.push(pdu).ok();
                    generated += 1;
                }
            }
        }

        // === Priority 2: SYN generation ===
        unsafe {
            while frame_idx < allocated {
                let allowed = self.syn_gen.activated
                    && match self.config.syn_limit {
                        Some(max) => self.syn_gen.packets_sent < max || max == 0,
                        None => true,
                    };
                if !allowed {
                    break;
                }

                let mut pdu = self.scratch_pdus[frame_idx].assume_init_read();
                frame_idx += 1;

                self.syn_gen.packets_sent += 1;
                self.syn_gen.prng_state = self
                    .syn_gen
                    .prng_state
                    .wrapping_mul(1_664_525)
                    .wrapping_add(1_013_904_223);
                let isn = self.syn_gen.prng_state;

                let src_port = self.syn_gen.port_counter;
                let base_ip = u32::from_be_bytes(self.config.src_ip.unwrap_or([0, 0, 0, 0]));
                let our_ip = base_ip.wrapping_add(self.syn_gen.ip_offset);

                if self.syn_gen.port_counter == PORT_END_RANGE {
                    self.syn_gen.port_counter = PORT_START_RANGE;
                    self.syn_gen.ip_offset = (self.syn_gen.ip_offset + 1) % 250;
                } else {
                    self.syn_gen.port_counter += 1;
                }

                let dst_port = 80u16;
                let their_ip = u32::from_be_bytes(self.config.dst_ip.unwrap_or([0, 0, 0, 0]));
                let flow_key = FlowKey(their_ip, dst_port, our_ip, src_port);
                self.connections.insert(
                    flow_key,
                    TcpState::SynSent,
                    isn.wrapping_add(1),
                    0,
                    current_tsc,
                );

                let mut packet = packet_view(&mut pdu);
                write_tcp_packet(
                    &mut packet,
                    reply_src_mac,
                    reply_dst_mac,
                    our_ip,
                    src_port,
                    their_ip,
                    dst_port,
                    isn,
                    0,
                    FLAG_SYN,
                );
                self.stats[STAT_SENT_SYN] += 1;
                batch.push(pdu).ok();
                generated += 1;
            }
        }

        // Return any allocated but unused frames to the pool
        while frame_idx < allocated {
            let pdu = unsafe { self.scratch_pdus[frame_idx].assume_init_read() };
            drop(pdu);
            frame_idx += 1;
        }

        if generated > 0 && (ROUTE_TX as usize) < self.routing_table.len() {
            Directive::Next(self.routing_table[ROUTE_TX as usize].clone())
        } else {
            Directive::Stop
        }
    }

    #[inline]
    fn processed_packets(&self) -> u64 {
        self.processed
    }

    fn requires_control_events(&self) -> bool {
        true
    }

    fn handle_control_event(&mut self, event: &ControlEvent) {
        self.on_control_event(event.opcode, event.payload.clone());
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// Extracts TCP header fields, runs the state machine, and writes the reply
/// back into the same frame. Returns `ROUTE_TX` if a reply was written,
/// `ROUTE_DROP` otherwise.
unsafe fn handle_tcp_packet(
    pdu: &mut Pdu,
    pool: &mut ConnectionPool,
    config: &TcpEngineConfig,
    stats: &mut [u64; STAT_COUNT],
    current_tsc: u64,
) -> u32 {
    macro_rules! inc_stat {
        ($stat:expr) => {
            stats[$stat] += 1
        };
    }

    let data = pdu.data_address(0);
    let mac_size = size_of::<MacHeader>();
    let ip_size = size_of::<IpHeader>();

    // Extract all header values before any write so there is no aliasing.
    let (
        incoming_src_mac,
        incoming_dst_mac,
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        tcp_flags,
        seq_num,
        ack_num,
        ip_total_len,
        ip_hdr_len,
        tcp_hdr_len,
    ) = unsafe {
        let mac = &*(data as *const MacHeader);
        let ip = &*(data.add(mac_size) as *const IpHeader);
        let tcp = &*(data.add(mac_size + ip_size) as *const TcpHeader);
        (
            mac.src.into_array(),
            mac.dst.into_array(),
            ip.src(),
            ip.dst(),
            tcp.src_port(),
            tcp.dst_port(),
            tcp.flags(),
            tcp.seq_num(),
            tcp.ack_num(),
            ip.length() as u32,
            (ip.ihl() * 4) as u32,
            (tcp.data_offset() * 4) as u32,
        )
    };

    let flow_key = FlowKey(src_ip, src_port, dst_ip, dst_port);
    let payload_len = ip_total_len.saturating_sub(ip_hdr_len + tcp_hdr_len);

    log::trace!("handle_tcp_packet: flow_key {:?}", flow_key);

    let action_opt;
    let mut current_state = None;

    if let Some(conn) = pool.get_mut_and_touch(&flow_key, current_tsc) {
        action_opt = run_tcp_state_machine(conn, tcp_flags, seq_num, ack_num, payload_len, stats);
        current_state = Some(conn.state);
    } else {
        if tcp_flags & FLAG_RST != 0 {
            inc_stat!(STAT_RECV_RST);
            return ROUTE_DROP;
        }
        if tcp_flags & FLAG_ACK != 0 {
            inc_stat!(STAT_UNEXPECTED);
            let reply_seq = ack_num;
            let reply_ack = seq_num
                .wrapping_add(payload_len)
                .wrapping_add(if tcp_flags & FLAG_FIN != 0 { 1 } else { 0 });
            inc_stat!(STAT_SENT_RST);
            action_opt = Some((FLAG_RST | FLAG_ACK, reply_seq, reply_ack));
        } else if tcp_flags & FLAG_SYN != 0 {
            inc_stat!(STAT_RECV_SYN);
            let rcv_nxt = seq_num.wrapping_add(1);
            let initial_snd_nxt = 1000u32;
            pool.insert(
                flow_key,
                TcpState::SynReceived,
                initial_snd_nxt.wrapping_add(1),
                rcv_nxt,
                current_tsc,
            );
            inc_stat!(STAT_SENT_SYNACK);
            action_opt = Some((FLAG_SYN | FLAG_ACK, initial_snd_nxt, rcv_nxt));
        } else {
            inc_stat!(STAT_UNEXPECTED);
            let reply_seq = if tcp_flags & FLAG_ACK != 0 {
                ack_num
            } else {
                0
            };
            let reply_ack = seq_num
                .wrapping_add(payload_len)
                .wrapping_add(if tcp_flags & FLAG_FIN != 0 { 1 } else { 0 });
            inc_stat!(STAT_SENT_RST);
            action_opt = Some((FLAG_RST | FLAG_ACK, reply_seq, reply_ack));
        }
    }

    if current_state == Some(TcpState::Closed) {
        pool.remove(&flow_key);
    }

    if let Some((reply_flags, reply_seq, reply_ack)) = action_opt {
        let reply_dst_mac = incoming_src_mac;
        let reply_src_mac = config.src_mac.unwrap_or(incoming_dst_mac);
        let mut packet = packet_view(pdu);
        write_tcp_packet(
            &mut packet,
            reply_src_mac,
            reply_dst_mac,
            dst_ip,
            dst_port,
            src_ip,
            src_port,
            reply_seq,
            reply_ack,
            reply_flags,
        );
        return ROUTE_TX;
    }

    ROUTE_DROP
}
