#![no_std]
extern crate alloc;

pub mod headers;
pub mod wpdu;
pub mod wbatch;


pub const TELEMETRY_FLAG: u32  = 1 << 8; // Bit 8