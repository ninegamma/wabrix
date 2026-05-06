// Inside your WASM Plugin (Guest)
#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::slice;
use core::ptr;
use core::sync::atomic::{AtomicU32, Ordering};
use macaddr::MacAddr6;
use core::net::Ipv4Addr;
use spin::Mutex; // <-- idiomatic no_std lock
use xbricks_protocol::headers::{IpHeader, MacHeader, TcpHeader};
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::plugins::generator::Guest;
use network_types::eth::EtherType;

// Generate bindings from the WIT file
wit_bindgen::generate!({
    path: "../../wit/xbricks.wit",
    world: "generator-plugin",
});

// 1. Define a structured config to hold parsed data
#[derive(Clone, Default)]
struct GeneratorConfig {
    src_mac: Option<[u8; 6]>,
    dst_mac: Option<[u8; 6]>,
    src_ip: Option<[u8; 4]>,
    dst_ip: Option<[u8; 4]>,
    rng_seed: Option<u32>, // random IP generation!
}

static CONFIG: Mutex<Option<GeneratorConfig>> = Mutex::new(None);
/// Use core atomics for safe #![no_std] global state.
// WASM is single-threaded, so Ordering::Relaxed is essentially zero-cost.
static WORKER_ID: AtomicU32 = AtomicU32::new(0);
static SEQ_COUNTER: AtomicU32 = AtomicU32::new(0);

struct MyGenerator;

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

    if parts.next().is_some() {
        return None;
    }

    Some(bytes)
}

fn parse_ip(ip: &str) -> Option<[u8; 4]> {
    let mut bytes = [0u8; 4];
    let mut parts = ip.split('.');

    for i in 0..4 {
        let part = parts.next()?;
        bytes[i] = part.parse::<u8>().ok()?;
    }

    if parts.next().is_some() {
        return None;
    }

    Some(bytes)
}

// A lightning-fast, zero-dependency PRNG for #![no_std]
#[inline(always)]
fn fast_rand(mut state: u32) -> u32 {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    state
}

impl Guest for MyGenerator {

    fn init_data(data: Vec<(alloc::string::String, alloc::string::String)>) {
        let mut config = GeneratorConfig::default();

        // Parse strings into structured bytes ONCE at startup
        for (key, value) in data {
            match key.as_str() {
                "src_mac" => {
                    if let Some(bytes) = parse_mac(&value) {
                        config.src_mac = Some(bytes);
                    }
                }
                "dst_mac" => {
                    if let Some(bytes) = parse_mac(&value) {
                        config.dst_mac = Some(bytes);
                    }
                }
                "src_ip" => {
                    if let Some(bytes) = parse_ip(&value) {
                        config.src_ip = Some(bytes);
                    }
                }
                "dst_ip" => {
                    if let Some(bytes) = parse_ip(&value) {
                        config.dst_ip = Some(bytes);
                    }
                }
                "rng_seed" => {
                    config.rng_seed = value.parse::<u32>().ok();
                }
                _ => {} // Ignore unknown keys
            }
        }

        // Safely store the parsed config in our global state
        *CONFIG.lock() = Some(config);
    }

    fn init(worker_id: u32) {
        WORKER_ID.store(worker_id, Ordering::Relaxed);
        SEQ_COUNTER.store(0, Ordering::Relaxed);
    }

    fn generate_batch(batch_bytes: Vec<u8>, partition_offset: u32) -> Vec<u32> {
        let pdus = unsafe {
            slice::from_raw_parts(
                batch_bytes.as_ptr() as *const Pdu0,
                batch_bytes.len() / core::mem::size_of::<Pdu0>(),
            )
        };

        let mut lengths = Vec::with_capacity(pdus.len());
        let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;

        // Load the state into local variables for the hot loop
        let worker_id = WORKER_ID.load(Ordering::Relaxed) as u16;
        let mut current_seq = SEQ_COUNTER.load(Ordering::Relaxed);

        // LOCK ONCE PER BATCH: Copy the config locally
        let mut local_config = {
            CONFIG.lock().clone().unwrap_or_default()
        };

        for pdu in pdus {
            let data_ptr = pdu.data_ptr(umem_addr);
            let wasm_ptr = data_ptr as *mut u8;

            // RESOLVE SOURCE IP: Random if seed exists, Static if None
            let source_ip_u32 = if let Some(mut seed) = local_config.rng_seed {
                seed = fast_rand(seed);
                local_config.rng_seed = Some(seed); // Save it for the next iteration
                seed
            } else {
                // Fallback to static IP. If missing from config, default to 192.168.177.2
                let sip = local_config.src_ip.unwrap_or([192, 168, 177, 2]);
                u32::from_be_bytes(sip)
            };

            // Call write_headers with ALL the required parameters
            let total_size = write_headers(
                wasm_ptr,
                current_seq,
                worker_id,
                &local_config,
                source_ip_u32
            );

            lengths.push(total_size);

            // Increment the local sequence counter
            current_seq = current_seq.wrapping_add(1);
        }

        // Save the updated sequence counter and RNG seed back to global state
        SEQ_COUNTER.store(current_seq, Ordering::Relaxed);
        if let Some(ref mut c) = *CONFIG.lock() {
            c.rng_seed = local_config.rng_seed;
        }

        lengths
    }
}

fn write_headers(
    wasm_ptr: *mut u8,
    seq: u32,
    dst_port: u16,
    config: &GeneratorConfig,
    src_ip: u32
) -> u32 {
    // ==========================================
    // 1. Initialize MAC Header
    // ==========================================
    let mut mac = MacHeader::new();

    // Unwrap options: Use user config, or fallback to sensible defaults
    let sm = config.src_mac.unwrap_or([1, 2, 3, 4, 5, 6]);
    mac.src = MacAddr6::new(sm[0], sm[1], sm[2], sm[3], sm[4], sm[5]);

    // Default to Broadcast MAC if not specified
    let dm = config.dst_mac.unwrap_or([0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    mac.dst = MacAddr6::new(dm[0], dm[1], dm[2], dm[3], dm[4], dm[5]);

    mac.set_ethertype(EtherType::Ipv4);

    // ==========================================
    // 2. Initialize IP Header
    // ==========================================
    let mut ip = IpHeader::new();
    ip.set_src(src_ip); // Comes from PRNG or static u32 from the loop

    // Unwrap Option array into Ipv4Addr
    let dip = config.dst_ip.unwrap_or([192, 168, 177, 222]);
    let dst_ipv4 = Ipv4Addr::new(dip[0], dip[1], dip[2], dip[3]);
    ip.set_dst_address(dst_ipv4);

    ip.set_ttl(128);
    ip.set_version(4);
    ip.set_protocol(6); // TCP
    ip.set_ihl(5);
    ip.set_length(40);
    ip.set_flags(0x2); // DF=1, MF=0 (don't fragment)

    // ==========================================
    // 3. Initialize TCP Header
    // ==========================================
    let mut tcp = TcpHeader::new();
    tcp.set_syn_flag();
    tcp.set_src_port(111);
    tcp.set_dst_port(dst_port); // Derived from worker ID
    tcp.set_data_offset(5);
    tcp.set_seq_num(seq);

    // ==========================================
    // 4. Write Directly to WASM Memory
    // ==========================================
    let mac_size = core::mem::size_of::<MacHeader>();
    let ip_size = core::mem::size_of::<IpHeader>();
    let tcp_size = core::mem::size_of::<TcpHeader>();
    let total_size = mac_size + ip_size + tcp_size;

    unsafe {
        // Cast raw pointer to struct pointers at specific offsets
        let mac_ptr = wasm_ptr as *mut MacHeader;
        let ip_ptr = wasm_ptr.add(mac_size) as *mut IpHeader;
        let tcp_ptr = wasm_ptr.add(mac_size + ip_size) as *mut TcpHeader;

        // Copy the structs into the WASM linear memory
        ptr::copy_nonoverlapping(&mac, mac_ptr, 1);
        ptr::copy_nonoverlapping(&ip, ip_ptr, 1);
        ptr::copy_nonoverlapping(&tcp, tcp_ptr, 1);
    }

    // Return the payload length
    total_size as u32
}

// Export the implementation to the runtime
export!(MyGenerator);