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

    /// just creating a batch of 0 length empty frames/pdus
    fn generate_batch(mut batch_bytes: Vec<u8>, _partition_offset: u32) -> Vec<u32> {
        let pdus = Pdu0::slice_from_bytes(&mut batch_bytes);

        let seq = TICK_COUNTER.load(Ordering::Relaxed);

        let lengths: Vec<u32> = vec![0; pdus.len()];
        TICK_COUNTER.store(seq.wrapping_add(lengths.len() as u64), Ordering::Relaxed);

        lengths
    }
}

// Export the implementation to the runtime
export!(TickGenerator);