#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::slice;
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::router::inspector::{Guest};

// 1. Tell the macro to read the .wit file and generate the bindings
wit_bindgen::generate!({
    path: "../../wit/inspector.wit",
    world: "filter-plugin",
});

// Constants mapping to your Verdicts
const VERDICT_FORWARD: u8 = 0;
const VERDICT_EXCEPTION: u8 = 1;
const VERDICT_DROP: u8 = 2;

struct MyInspector;

// Implement the generated trait

impl Guest for MyInspector {
    fn inspect_batch(mut batch_bytes: Vec<u8>) -> Vec<u8> {
        // 1. ZERO-COPY CAST: Overlay our Pdu0 struct onto the raw bytes
        let pdus = unsafe {
            slice::from_raw_parts_mut(
                batch_bytes.as_mut_ptr() as *mut Pdu0,
                batch_bytes.len() / core::mem::size_of::<Pdu0>(),
            )
        };

        // 2. Iterate and inspect!
        for pdu in pdus {
            // Example Logic: IPv4 TCP gets forwarded
            if pdu.is_ipv4() && pdu.is_tcp() {
                pdu.flags = VERDICT_FORWARD;
            } else {
                pdu.flags = VERDICT_EXCEPTION;
            }
        }

        // 3. Return the mutated memory block back to the Host
        batch_bytes
    }
}

export!(MyInspector);