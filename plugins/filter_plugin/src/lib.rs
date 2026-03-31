#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::slice;
use crate::exports::xbricks::router::inspector::{Guest, Verdict};

// 1. Tell the macro to read the .wit file and generate the bindings
wit_bindgen::generate!({
    path: "../../wit/inspector.wit",
    world: "filter-plugin",
});

struct MyInspector;

// 2. Implement the generated trait
// 2. Implement the generated trait

impl Guest for MyInspector {
    fn inspect_batch(offsets: Vec<u64>, lengths: Vec<u32>) -> Vec<Verdict> {
        let mut actions = Vec::with_capacity(offsets.len());

        for (offset, len) in offsets.iter().zip(lengths.iter()) {
            let ptr = *offset as *const u8;
            let packet_data: &[u8] = unsafe {
                slice::from_raw_parts(ptr, *len as usize)
            };

            if packet_data.len() < 14 {
                actions.push(Verdict::Drop);
                continue;
            }

            // If IPv4, forward it. Otherwise, exception path.
            if packet_data[12] == 0x08 && packet_data[13] == 0x00 {
                actions.push(Verdict::Forward);
            } else {
                actions.push(Verdict::Exception);
            }
        }

        actions
    }
}

// 3. Export the struct to the Wasm engine
export!(MyInspector);
