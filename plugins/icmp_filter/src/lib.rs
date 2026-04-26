// In your new ping_filter/src/lib.rs

#![no_std]
extern crate alloc;
use alloc::vec::Vec;
use core::slice;
use core::sync::atomic::{AtomicU64, Ordering};
use xbricks_protocol::headers::{HeaderKind, IcmpHeader};
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::router::inspector::Guest;

// Point this to the EXACT SAME .wit file you used for your first plugin!
wit_bindgen::generate!({
    path: "../../wit/inspector.wit",
    world: "filter-plugin",
});

// Create a new struct specifically for this plugin
struct IcmpFilter;

// =====================================================================
// WASM LOCAL STATE
// Because each Wasm node is instantiated per worker, these statics
// live safely inside the Wasm sandbox memory and persist across batches!
// =====================================================================
static PING_REQUESTS: AtomicU64 = AtomicU64::new(0);
static PING_REPLIES: AtomicU64 = AtomicU64::new(0);

// Routing Constants
const ROUTE_DROP: u32 = 1;
const ROUTE_STANDARD: u32 = 0;

impl Guest for IcmpFilter {
    fn inspect_batch(batch_bytes: Vec<u8>, partition_offset: u32) -> Vec<u32> {
        let pdus = unsafe {
            slice::from_raw_parts(
                batch_bytes.as_ptr() as *const Pdu0,
                batch_bytes.len() / core::mem::size_of::<Pdu0>(),
            )
        };

        let mut actions = Vec::with_capacity(pdus.len());

        // MAGIC: Calculate the virtual UMEM address for this sandbox.
        // E.g., if partition is 10MB, this becomes -10MB (in 2's complement).
        let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;

        for pdu in pdus {
            let mut action = 0;
            let base_addr = pdu.base_addr(umem_addr);
            let header_count = pdu.headers().count();

            // Safe fallback for empty/corrupted packets
            if header_count == 0 {
                actions.push(ROUTE_DROP);
                continue;
            }

            let mut is_icmp = false;

            // Scan the Header Stack for ICMP
            for i in (0..header_count).rev() {
                let kind = pdu.headers().kinds[i];

                if kind == HeaderKind::Icmp {
                    is_icmp = true;
                    let icmp_hdr = pdu.headers().icmp(base_addr, i);

                    // 🛡️ PROTOCOL GUARD: Check the parent header to see if it is IPv6
                    let is_ipv6 = if i > 0 {
                        pdu.headers().kinds[i - 1] == HeaderKind::Ipv6
                    } else {
                        false
                    };

                    match icmp_hdr.icmp_type() {
                        // ECHO REQUEST: IPv4 (8) or IPv6 (128)
                        8 if !is_ipv6 => {
                            PING_REQUESTS.fetch_add(1, Ordering::Relaxed);
                            action |= 0 << 16; // Bucket 0
                        }
                        128 if is_ipv6 => {
                            PING_REQUESTS.fetch_add(1, Ordering::Relaxed);
                            action |= 0 << 16; // Bucket 0
                        }

                        // ECHO REPLY: IPv4 (0) or IPv6 (129)
                        0 if !is_ipv6 => {
                            PING_REPLIES.fetch_add(1, Ordering::Relaxed);
                            action |= 1 << 16; // Bucket 1
                        }
                        129 if is_ipv6 => {
                            PING_REPLIES.fetch_add(1, Ordering::Relaxed);
                            action |= 1 << 16; // Bucket 1
                        }

                        // OTHER ICMP (Router Solicitations, Time Exceeded, etc.)
                        _ => {
                            action |= 2 << 16; // Bucket 2
                        }
                    }

                    break; // Stop looking once we process the L4 header
                }
            }

            // If we looped through all headers and didn't find ICMP, it's something else
            if !is_icmp {
                action |= 3 << 16; // Bucket 3 for non-ICMP traffic (TCP/UDP/ARP)
            }

            // Route everything successfully parsed to the standard output
            action |= ROUTE_STANDARD;

            actions.push(action);
        }
        actions
    }
}

// Export your new struct
export!(IcmpFilter);