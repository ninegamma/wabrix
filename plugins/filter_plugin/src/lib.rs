#![no_std]
extern crate alloc;

use alloc::vec::Vec;
use core::slice;
use xbricks_protocol::headers::HeaderKind;
use xbricks_protocol::TELEMETRY_FLAG;
use xbricks_protocol::wbatch::Pdu0;
use crate::exports::xbricks::router::inspector::{Guest};

// 1. Tell the macro to read the .wit file and generate the bindings
wit_bindgen::generate!({
    path: "../../wit/inspector.wit",
    world: "filter-plugin",
});

// Assuming your Pdu0 definition is imported
// use crate::pdu::Pdu0;

// Constants for our Action Bitfield
const ROUTE_FAST_PATH: u32 = 0;
const ROUTE_EXCEPTION: u32 = 1;

// DISTRIBUTE AND EXECUTE ACTIONS
// u32 action:
// Bits 0-7 (Route ID): Which output port to send the packet to (0-254). 255 = DROP.
// Bit 8 (Telemetry Flag): 1 = Send a telemetry event for this packet.
// Bits 16-31 (Telemetry Meta): An ID, counter, or metric value for the telemetry subsystem.

struct MyInspector;

impl Guest for MyInspector {
    fn inspect_batch(batch_bytes: Vec<u8>) -> Vec<u32> {
        let pdus = unsafe {
            slice::from_raw_parts(
                batch_bytes.as_ptr() as *const Pdu0,
                batch_bytes.len() / core::mem::size_of::<Pdu0>(),
            )
        };

        let mut actions = Vec::with_capacity(pdus.len());

        for pdu in pdus {
            let mut action = TELEMETRY_FLAG;

            let mut is_v4 = false;
            let mut dst_port = None;

            // SINGLE PASS: Iterate backwards to find L4 ports instantly,
            // and just verify IPv4 exists along the way.
            let header_count = pdu.headers().count();
            let base_ptr = pdu.local_frame_offset(); // Wasm base_ptr IS the offset!

            for i in (0..header_count).rev() {
                let kind = pdu.headers().kinds[i]; // Direct array access!

                if kind == HeaderKind::Ip {
                    is_v4 = true;
                } else if dst_port.is_none() {
                    if kind == HeaderKind::Tcp {
                        dst_port = Some(pdu.headers().tcp(base_ptr, i).dst_port());
                    } else if kind == HeaderKind::Udp {
                        dst_port = Some(pdu.headers().udp(base_ptr, i).dst_port());
                    }
                }
            }

            // Execute Routing Logic
            if is_v4 && dst_port.is_some() {
                action |= ROUTE_FAST_PATH;
                // Safe to unwrap because of the if condition
                let port = dst_port.unwrap();
                let telemetry_bucket = if port <= 9 {
                    port as u32
                } else {
                    10
                };
                action |= telemetry_bucket << 16;
            } else {
                action |= ROUTE_EXCEPTION;
                let telemetry_bucket = 9u32;
                action |= telemetry_bucket << 16;
            }

            actions.push(action);
        }

        actions
    }
}

export!(MyInspector);