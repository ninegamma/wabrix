// Inside your WASM Plugin (Guest)
#![no_std]
extern crate alloc;

use alloc::vec::Vec;
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
    fn init_data(data: Vec<(alloc::string::String, alloc::string::String)>, worker_id: u32) {
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
        WORKER_ID.store(worker_id, Ordering::Relaxed);
        SEQ_COUNTER.store(0, Ordering::Relaxed);
    }

    fn generate_batch(mut batch_bytes: Vec<u8>, partition_offset: u32) -> Vec<u32> {
        let pdus = Pdu0::slice_from_bytes(&mut batch_bytes);

        let worker_id = WORKER_ID.load(Ordering::Relaxed) as u16;
        let mut seq = SEQ_COUNTER.load(Ordering::Relaxed);
        let mut config = CONFIG.lock().clone().unwrap_or_default();

        // Pre-calculate static fields outside the loop for maximum performance
        let dst_mac = MacAddr6::from(config.dst_mac.unwrap_or([0xff; 6]));
        let src_mac = MacAddr6::from(config.src_mac.unwrap_or([1, 2, 3, 4, 5, 6]));
        let dst_ip = Ipv4Addr::from(config.dst_ip.unwrap_or([192, 168, 177, 222]));
        let base_src_ip = config.src_ip.unwrap_or([192, 168, 177, 2]);

        let lengths: Vec<u32> = pdus.iter_mut().map(|pdu| {
            // 1. Calculate dynamic fields per-packet
            let src_ip_u32 = if let Some(seed) = config.rng_seed {
                let new_seed = fast_rand(seed);
                config.rng_seed = Some(new_seed);
                new_seed
            } else {
                u32::from_be_bytes(base_src_ip)
            };

            // 2. Build MacHeader
            let mut mac = MacHeader::new();
            mac.src = src_mac;
            mac.dst = dst_mac;
            mac.set_ethertype(EtherType::Ipv4);

            // 3. Build IpHeader
            let mut ip = IpHeader::new();
            ip.set_src(src_ip_u32);
            ip.set_dst_address(dst_ip);
            ip.set_ttl(128);
            ip.set_version(4);
            ip.set_protocol(6); // TCP
            ip.set_ihl(5);
            ip.set_length(40);
            ip.set_flags(0x2); // DF=1

            // 4. Build TcpHeader
            let mut tcp = TcpHeader::new();
            tcp.set_syn_flag();
            tcp.set_src_port(111);
            tcp.set_dst_port(worker_id);
            tcp.set_data_offset(5);
            tcp.set_seq_num(seq);

            // 5. Serialize directly into Wasm memory
            let len = pdu.write_tcp_ipv4_headers(&mac, &ip, &tcp, partition_offset);

            seq = seq.wrapping_add(1);
            len
        }).collect();

        // Save state
        SEQ_COUNTER.store(seq, Ordering::Relaxed);
        if let Some(ref mut c) = *CONFIG.lock() {
            c.rng_seed = config.rng_seed;
        }

        lengths
    }
}

// Export the implementation to the runtime
export!(MyGenerator);