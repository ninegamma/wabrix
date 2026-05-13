#![no_std]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::ptr;
use spin::Mutex;

use network_types::eth::EtherType;
use macaddr::MacAddr6;

use xbricks_protocol::headers::{HeaderKind, IpHeader, MacHeader, TcpHeader};
use xbricks_protocol::headers::hstack::HeaderStack;
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::plugins::packet_engine::Guest;

// 1. Tell the macro to read the .wit file and generate the bindings
wit_bindgen::generate!({
    path: "../../wit/xbricks.wit",
    world: "engine-plugin",
});

// The 4-Tuple that uniquely identifies a TCP connection
// (Src IP, Src Port, Dst IP, Dst Port)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FlowKey(u32, u16, u32, u16);

// Standard TCP Flags
const FLAG_FIN: u8 = 0x01;
const FLAG_SYN: u8 = 0x02;
const FLAG_RST: u8 = 0x04;
const FLAG_ACK: u8 = 0x10;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TcpState {
    Closed,
    Listen,
    SynSent,
    SynReceived,
    Established,
    FinWait1,
    FinWait2,
    Closing,
    TimeWait,
    CloseWait,
    LastAck,
}

// 1. Define a structured config to hold parsed data
#[derive(Clone, Default)]
struct EngineConfig {
    src_mac: Option<[u8; 6]>,
    dst_mac: Option<[u8; 6]>,
    src_ip: Option<[u8; 4]>,
    dst_ip: Option<[u8; 4]>,
    rng_seed: Option<u32>,
}

static CONFIG: Mutex<Option<EngineConfig>> = Mutex::new(None);

struct Connection {
    state: TcpState,
    snd_nxt: u32,
    rcv_nxt: u32,
}

static CONNECTIONS: Mutex<Option<BTreeMap<FlowKey, Connection>>> = Mutex::new(None);

const ROUTE_TX: u32 = 0;
const ROUTE_LINUX: u32 = 1;
const ROUTE_DROP: u32 = 2;

fn init_state_machine() {
    *CONNECTIONS.lock() = Some(BTreeMap::new());
}

// =====================================================================
// #![no_std] Safe Parsers
// =====================================================================
fn parse_mac(mac: &str) -> Option<[u8; 6]> {
    let mut bytes = [0u8; 6];
    let mut parts = mac.split(':');
    for i in 0..6 {
        let part = parts.next()?;
        bytes[i] = u8::from_str_radix(part, 16).ok()?;
    }
    if parts.next().is_some() { return None; }
    Some(bytes)
}

fn parse_ip(ip: &str) -> Option<[u8; 4]> {
    let mut bytes = [0u8; 4];
    let mut parts = ip.split('.');
    for i in 0..4 {
        let part = parts.next()?;
        bytes[i] = part.parse::<u8>().ok()?;
    }
    if parts.next().is_some() { return None; }
    Some(bytes)
}

struct TcpEngine;

impl Guest for TcpEngine {
    fn init_data(data: Vec<(alloc::string::String, alloc::string::String)>) {
        init_state_machine();
        let mut config = EngineConfig::default();

        for (key, value) in data {
            match key.as_str() {
                "src_mac" => config.src_mac = parse_mac(&value),
                "dst_mac" => config.dst_mac = parse_mac(&value),
                "src_ip" => config.src_ip = parse_ip(&value),
                "dst_ip" => config.dst_ip = parse_ip(&value),
                "rng_seed" => config.rng_seed = value.parse::<u32>().ok(),
                _ => {}
            }
        }
        *CONFIG.lock() = Some(config);
    }

    fn process_batch(mut batch_bytes: Vec<u8>, partition_offset: u32) -> (Vec<u32>, Vec<u32>) {
        let pdus = Pdu0::slice_from_bytes(&mut batch_bytes);

        let mut actions = Vec::with_capacity(pdus.len());
        let mut lengths = Vec::with_capacity(pdus.len());
        let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;

        // Lock Global States ONCE per batch
        let mut connections_guard = CONNECTIONS.lock();
        let table = connections_guard.as_mut().expect("Table uninitialized");
        let local_config = CONFIG.lock().clone().unwrap_or_default();

        for pdu in pdus.iter_mut() {
            if pdu.data_len() == 0 {
                // TICK FRAME: empty frames sent by PacketGenerator
                // (Useful for triggering Timeouts or active SYNs in the future)
                actions.push(ROUTE_DROP);
                lengths.push(0);
            } else if pdu.headers().fingerprint() == HeaderStack::TCP_OVER_IPV4 {
                // TCP FRAME
                let (action, len) = handle_tcp_packet(pdu, table, &local_config, umem_addr);
                actions.push(action);
                lengths.push(len);
            } else {
                // Not TCP -> Forward to standard Linux networking stack
                actions.push(ROUTE_LINUX);
                lengths.push(pdu.data_len());
            }
        }

        (actions, lengths)
    }
}

/// Extracts protocol data, runs the state machine, and formats replies
fn handle_tcp_packet(
    pdu: &mut Pdu0,
    table: &mut BTreeMap<FlowKey, Connection>,
    config: &EngineConfig,
    umem_addr: usize,
) -> (u32, u32) {
    let base_addr = pdu.base_addr(umem_addr);
    let headers = pdu.headers();

    // Find header indices safely
    let mut ip_idx = None;
    let mut tcp_idx = None;
    for i in 0..headers.count() {
        if headers.kinds[i] == HeaderKind::Ip { ip_idx = Some(i); }
        if headers.kinds[i] == HeaderKind::Tcp { tcp_idx = Some(i); }
    }

    let (ip_i, tcp_i) = match (ip_idx, tcp_idx) {
        (Some(i), Some(t)) => (i, t),
        _ => return (ROUTE_LINUX, pdu.data_len()), // Forward malformed/unexpected packets
    };

    let ip_hdr = headers.ip(base_addr, ip_i);
    let tcp_hdr = headers.tcp(base_addr, tcp_i);

    // Extract 4-tuple Flow ID
    let src_ip = ip_hdr.src();
    let dst_ip = ip_hdr.dst();
    let src_port = tcp_hdr.src_port();
    let dst_port = tcp_hdr.dst_port();
    let flow_key = FlowKey(src_ip, src_port, dst_ip, dst_port);

    // Extract State Machine variables
    let tcp_flags = tcp_hdr.flags();
    let seq_num = tcp_hdr.seq_num();
    let ack_num = tcp_hdr.ack_num();

    // Calculate approximate payload size
    let hdr_len = 14 + (ip_hdr.ihl() * 4) + (tcp_hdr.data_offset() * 4);
    let payload_len = pdu.data_len().saturating_sub(hdr_len as u32);

    let mut action_opt = None;
    let mut current_state = None;

    // 1. IS THIS A KNOWN CONNECTION?
    if let Some(conn) = table.get_mut(&flow_key) {
        // Yes! Run the normal state machine
        action_opt = run_tcp_state_machine(conn, tcp_flags, seq_num, ack_num, payload_len);
        current_state = Some(conn.state);

    } else {
        // NO! The connection does NOT exist in our BTreeMap.

        // Rule 1: Ignore stray RSTs
        if tcp_flags & FLAG_RST != 0 {
            return (ROUTE_DROP, 0);
        }

        // Rule 2: Accept new SYNs and create the connection!
        if tcp_flags & FLAG_SYN != 0 {
            let rcv_nxt = seq_num.wrapping_add(1);
            let initial_snd_nxt = 1000u32; // In production, generate a random ISN here!

            table.insert(flow_key, Connection {
                state: TcpState::SynReceived,
                snd_nxt: initial_snd_nxt.wrapping_add(1),
                rcv_nxt,
            });

            // Reply with SYN-ACK
            action_opt = Some((FLAG_SYN | FLAG_ACK, initial_snd_nxt, rcv_nxt));
        } else {
            // Rule 3: Stray ACK, FIN, or Data. Send an RST, but DO NOT save state!
            // RFC 793: If incoming ACK flag is 0, reply SEQ = 0, ACK = incoming SEQ + len
            // If incoming ACK is 1, reply SEQ = incoming ACK
            let reply_seq = if tcp_flags & FLAG_ACK != 0 { ack_num } else { 0 };
            let reply_ack = seq_num.wrapping_add(payload_len).wrapping_add(
                if tcp_flags & FLAG_FIN != 0 { 1 } else { 0 }
            );

            action_opt = Some((FLAG_RST | FLAG_ACK, reply_seq, reply_ack));
        }
    }

    // 2. MEMORY CLEANUP: Delete dead connections
    if current_state == Some(TcpState::Closed) {
        table.remove(&flow_key);
    }

    // 3. FORMAT REPLY
    if let Some((reply_flags, reply_seq, reply_ack)) = action_opt {
        let data_ptr = pdu.data_ptr(umem_addr) as *mut u8;

        let len = write_tcp_reply(
            data_ptr, config,
            dst_ip, dst_port, // New Src (Us)
            src_ip, src_port, // New Dst (Them)
            reply_seq, reply_ack, reply_flags
        );
        return (ROUTE_TX, len);
    }

    (ROUTE_DROP, 0)
}

/// The core RFC 793 State Machine Logic
fn run_tcp_state_machine(
    conn: &mut Connection,
    tcp_flags: u8,
    seq_num: u32,
    ack_num: u32,
    payload_len: u32,
) -> Option<(u8, u32, u32)> {

    // Fast-path: Reset drops connection instantly
    if tcp_flags & FLAG_RST != 0 {
        conn.state = TcpState::Closed;
        return None;
    }

    macro_rules! reply {
        ($flags:expr) => { Some(($flags, conn.snd_nxt, conn.rcv_nxt)) }
    }

    match conn.state {
        TcpState::Listen => {
            if tcp_flags & FLAG_SYN != 0 {
                conn.state = TcpState::SynReceived;
                conn.rcv_nxt = seq_num.wrapping_add(1);
                let send_seq = conn.snd_nxt;
                conn.snd_nxt = conn.snd_nxt.wrapping_add(1);
                return Some((FLAG_SYN | FLAG_ACK, send_seq, conn.rcv_nxt));
            }
        }
        TcpState::SynReceived => {
            if tcp_flags & FLAG_ACK != 0 && ack_num == conn.snd_nxt {
                conn.state = TcpState::Established;
            }
        }
        TcpState::Established => {
            if tcp_flags & FLAG_FIN != 0 {
                conn.state = TcpState::CloseWait;
                conn.rcv_nxt = seq_num.wrapping_add(1).wrapping_add(payload_len);
                return reply!(FLAG_ACK);
            } else if tcp_flags & FLAG_ACK != 0 && payload_len > 0 {
                conn.rcv_nxt = conn.rcv_nxt.wrapping_add(payload_len);
                return reply!(FLAG_ACK);
            }
        }
        TcpState::FinWait1 => {
            if tcp_flags & FLAG_FIN != 0 && tcp_flags & FLAG_ACK != 0 {
                // conn.state = TcpState::TimeWait;
                conn.state = TcpState::Closed;
                conn.rcv_nxt = seq_num.wrapping_add(1);
                return reply!(FLAG_ACK);
            } else if tcp_flags & FLAG_FIN != 0 {
                conn.state = TcpState::Closing;
                conn.rcv_nxt = seq_num.wrapping_add(1);
                return reply!(FLAG_ACK);
            } else if tcp_flags & FLAG_ACK != 0 {
                conn.state = TcpState::FinWait2;
            }
        }
        TcpState::FinWait2 => {
            if tcp_flags & FLAG_FIN != 0 {
                // conn.state = TcpState::TimeWait;
                conn.state = TcpState::Closed;
                conn.rcv_nxt = seq_num.wrapping_add(1);
                return reply!(FLAG_ACK);
            }
        }
        TcpState::Closing => {
            if tcp_flags & FLAG_ACK != 0 {
                // conn.state = TcpState::TimeWait;
                conn.state = TcpState::Closed;
            }
        }
        TcpState::LastAck => {
            if tcp_flags & FLAG_ACK != 0 && ack_num == conn.snd_nxt {
                conn.state = TcpState::Closed;
            }
        }
        TcpState::Closed => {
            if tcp_flags & FLAG_RST == 0 {
                return Some((FLAG_RST | FLAG_ACK, 0, seq_num.wrapping_add(payload_len)));
            }
        }
        _ => {}
    }

    None
}

/// Formats the outbound headers back into WASM linear memory
fn write_tcp_reply(
    wasm_ptr: *mut u8,
    config: &EngineConfig,
    src_ip: u32,
    src_port: u16,
    dst_ip: u32,
    dst_port: u16,
    seq_num: u32,
    ack_num: u32,
    flags: u8,
) -> u32 {
    let mut mac = MacHeader::new();
    let sm = config.src_mac.unwrap_or([0; 6]);
    mac.src = MacAddr6::new(sm[0], sm[1], sm[2], sm[3], sm[4], sm[5]);
    let dm = config.dst_mac.unwrap_or([0xff; 6]);
    mac.dst = MacAddr6::new(dm[0], dm[1], dm[2], dm[3], dm[4], dm[5]);
    mac.set_ethertype(EtherType::Ipv4);

    let mut ip = IpHeader::new();
    ip.set_src(src_ip);
    ip.set_dst(dst_ip);
    ip.set_ttl(64);
    ip.set_version(4);
    ip.set_protocol(6);
    ip.set_ihl(5);
    ip.set_length(40);
    ip.set_flags(0x2);

    let mut tcp = TcpHeader::new();
    tcp.set_flags(flags);
    tcp.set_src_port(src_port);
    tcp.set_dst_port(dst_port);
    tcp.set_data_offset(5);
    tcp.set_seq_num(seq_num);
    tcp.set_ack_num(ack_num);

    let mac_size = core::mem::size_of::<MacHeader>();
    let ip_size = core::mem::size_of::<IpHeader>();
    let tcp_size = core::mem::size_of::<TcpHeader>();

    unsafe {
        ptr::copy_nonoverlapping(&mac, wasm_ptr as *mut MacHeader, 1);
        ptr::copy_nonoverlapping(&ip, wasm_ptr.add(mac_size) as *mut IpHeader, 1);
        ptr::copy_nonoverlapping(&tcp, wasm_ptr.add(mac_size + ip_size) as *mut TcpHeader, 1);
    }

    (mac_size + ip_size + tcp_size) as u32
}

// Export the implementation to the runtime
export!(TcpEngine);