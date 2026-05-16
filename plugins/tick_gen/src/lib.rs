// Inside your WASM Plugin (Guest)
#![no_std]
extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::plugins::generator::Guest;

// Generate bindings from the WIT file
wit_bindgen::generate!({
    path: "../../wit/xbricks.wit",
    world: "generator-plugin",
});

/// Use core atomics for safe #![no_std] global state.
// WASM is single-threaded, so Ordering::Relaxed is essentially zero-cost.
static WORKER_ID: AtomicU32 = AtomicU32::new(0);
static TICK_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TickGenerator;


impl Guest for TickGenerator {
    fn init_data(_data: Vec<(alloc::string::String, alloc::string::String)>, worker_id: u32) {
        // Safely store the parsed config in our global state
        WORKER_ID.store(worker_id, Ordering::Relaxed);
        TICK_COUNTER.store(0, Ordering::Relaxed);
    }
    fn set_dst(
        mac: (u8, u8, u8, u8, u8, u8),
        ip: (u8, u8, u8, u8),
    ) {
        // Reconstruct into native arrays
        let ip_bytes: [u8; 4] = [ip.0, ip.1, ip.2, ip.3];
        let mac_bytes: [u8; 6] = [mac.0, mac.1, mac.2, mac.3, mac.4, mac.5];

        // Store them safely in your atomic or Mutex state
        // Example:
        // DST_IP.store(u32::from_be_bytes(ip_bytes), Ordering::Relaxed);
    }

    fn generate_batch(mut batch_bytes: Vec<u8>, partition_offset: u32) -> Vec<u32> {
        let pdus = Pdu0::slice_from_bytes(&mut batch_bytes);
        let seq = TICK_COUNTER.load(Ordering::Relaxed);

        let mut lengths: Vec<u32> = Vec::with_capacity(pdus.len());

        // Calculate the virtual UMEM address for this sandbox
        let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;

        for pdu in pdus.iter_mut() {
            // Get the raw pointer to the start of the packet data
            let data_ptr = pdu.data_ptr(umem_addr) as *mut u8;

            unsafe {
                // Optional but recommended: Zero out the MAC addresses (first 12 bytes)
                // so we don't leak uninitialized memory or old packet data.
                core::ptr::write_bytes(data_ptr, 0, 12);

                // Write the EtherType (0x88B5 - Local Experimental) at offset 12 and 13.
                // This is standard Big-Endian network byte order.
                *data_ptr.add(12) = 0x88;
                *data_ptr.add(13) = 0xB5;
            }

            // Return a valid minimum Ethernet frame size (64 bytes)
            lengths.push(64);
        }

        TICK_COUNTER.store(seq.wrapping_add(lengths.len() as u64), Ordering::Relaxed);

        lengths
    }
}

// Export the implementation to the runtime
export!(TickGenerator);