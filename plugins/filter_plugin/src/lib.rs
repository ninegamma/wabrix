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
        // 1. ZERO-COPY CAST: View the raw bytes as Pdu0 structs.
        // We use an immutable cast (*const) because Wasm no longer mutates the batch!
        let pdus = unsafe {
            slice::from_raw_parts(
                batch_bytes.as_ptr() as *const Pdu0,
                batch_bytes.len() / core::mem::size_of::<Pdu0>(),
            )
        };

        // 2. Pre-allocate the Action Vector to avoid reallocations
        let mut actions = Vec::with_capacity(pdus.len());

        // 3. Iterate, Inspect, and Pack
        for pdu in pdus {
            let mut action = 0;

            // Trigger Telemetry
            action |= TELEMETRY_FLAG;  // bit 8 = 1

            if pdu.is_ipv4() && pdu.is_tcp() {
                // Route to Fast Path
                action |= ROUTE_FAST_PATH;

                let mut dst_port = None;
                let header_count = pdu.headers().count();

                // FAST PATH: Iterate backwards. L4 headers (TCP/UDP) are usually last!
                for i in (0..header_count).rev() {
                    match pdu.headers().get(i).kind() {
                        HeaderKind::Tcp => {
                            dst_port = Some(pdu.headers().tcp(i).dst_port());
                            break; // Found it, stop scanning!
                        }
                        HeaderKind::Udp => {
                            dst_port = Some(pdu.headers().udp(i).dst_port());
                            break;
                        }
                        _ => {} // Ignore IP, MAC, etc.
                    }
                }

                // Extract Destination Port
                //let dst_port = pdu.w.headers().tcp(2).dst_port();

                let telemetry_bucket: u32;
                // Bucket logic: Ports 0-9 get their own ID, everything else gets ID 10
                if let Some(dport) = dst_port {
                    telemetry_bucket = if dport <= 9 {
                        dport as u32
                    } else {
                        10
                    };
                } else {
                    telemetry_bucket = 10;
                }

                // Pack the bucket ID into the upper 16 bits
                action |= telemetry_bucket << 16;

            } else {
                // Not TCP/IPv4: Send to Exception Route, no telemetry needed
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