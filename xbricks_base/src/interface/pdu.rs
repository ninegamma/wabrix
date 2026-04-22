use core::{cmp, fmt, slice};
use core::ptr;
use std::ops::Add;
use log::{debug, trace};
use xsk_rs::FrameDesc;
use xsk_rs::umem::frame::SegmentLengths;
use xbricks_protocol::headers::{
    TcpHeader,
};

use crate::BusCommand;
use crate::SystemBus;
use xbricks_protocol::headers::check::ipv4_checksum;
use xbricks_protocol::wpdu::WPdu;

#[repr(C, align(64))]
#[derive(Debug)]
pub struct Pdu {
    /// [0..56] 56 Bytes: The pure Wasm-compatible core!
    pub w: WPdu,

    /// [56..64] 8 Bytes: Bus Pointer
    system_bus: *const SystemBus,
}

// This check will now successfully pass in BOTH Linux and Wasm!
const _: () = assert!(core::mem::size_of::<Pdu>() == 64);

// This lets you call `packet.parse_fast()` directly on Pdu0 in Wasm!
impl core::ops::Deref for Pdu {
    type Target = WPdu;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.w
    }
}

impl core::ops::DerefMut for Pdu {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.w
    }
}



impl Default for Pdu {
    fn default() -> Self {
        Pdu {
            w: WPdu::default(),
            system_bus: core::ptr::null(),
        }
    }
}

// SAFETY: Pdu contains a raw pointer to SystemBus, but it's only used
// for frame management and doesn't violate Send requirements. The SystemBus
// itself is thread-safe, and the pointer is never dereferenced across thread
// boundaries in a way that would cause data races.
unsafe impl Send for Pdu {}


// Wasm cannot use the Drop trait because it doesn't have the SystemBus!
// The Host will handle the dropping.
#[cfg(not(target_arch = "wasm32"))]
impl Drop for Pdu {
    fn drop(&mut self) {
        unsafe {
            // 0. Check if the PDU is an uninitialized Pdu::default(), e.g. happens in PacketGenNode
            if self.try_umem_addr().is_none() {
                log::debug!("Ignoring drop of uninitialized PDU slot");
                return;
            }
            // 1. FAST PATH: Thread-Local Return
            // Check if we are currently executing on the worker thread that owns this frame.
            let registry_ptr = crate::frame_registry::get_worker_frame_registry();
            if !registry_ptr.is_null() {
                let registry = &mut *registry_ptr;

                if registry.worker_id == (self.owner_id() as usize) {
                    // We are on the owning thread! Bypass the crossbeam_channel entirely.
                    // Return the memory instantly to the local stash.
                    trace!("Drop for Pdu: Returning frame {} to local stash", self.global_frame_idx());
                    registry.release_local(self.global_frame_idx());
                    return;
                }
            }

            // 2. SLOW PATH: Cross-Thread Return
            // We are on a different thread (e.g., cross-worker exception routing).
            // We must route the release command via the bus.
            if !self.system_bus.is_null() {
                // 2. Access the SystemBus
                // Safety: The SystemBus is allocated in main() inside an Arc
                // and lives for the duration of the program.
                let bus = &*self.system_bus;

                // 3. Route the frame back to its owner
                // We use the owner_id (u8) to select the correct SPSC sender.
                // If this Pdu is on the same core as owner_id, this is a local SPSC send.
                // If on a different core, this is a cross-core SPSC send.
                // We ignore SendError (e.g., if the channel is full or disconnected on shutdown).
                let _ = bus.bus_senders[self.owner_id() as usize].send(BusCommand::Release(self.global_frame_idx()));
            }
        }
    }
}

impl fmt::Display for Pdu {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(Frame: {}, data_len= {}), headers=\n{}",
            self.global_frame_idx(),
            self.data_len(),
            self.headers(),
        )
    }
}

// Isolate methods that require xsk_rs or SystemBus
#[cfg(not(target_arch = "wasm32"))]
impl Pdu {
    #[inline(always)]
    pub fn new(
        desc: FrameDesc,
        global_frame_idx: u32,
        owner_id: u8,
        system_bus: *const SystemBus,
        frame_size_log2: u8,
    ) -> Self {
        let w= WPdu::new(
            desc.addr() as u64,
            desc.lengths().data() as u32,
            desc.options(),
            global_frame_idx,
            frame_size_log2,
            owner_id,
        );
        let pdu = Pdu {
            w,
            system_bus,
        };
        pdu
    }

    /// Reconstruct the FrameDesc for transmission
    pub fn to_frame_desc(&self) -> FrameDesc {
        // 1. Calculate the absolute start of the frame (Physical Address 0 relative to this frame)
        // e.g., if frame_idx=2 and size=2048, base is 4096.
        let local_frame_base = self.w.local_frame_offset();
        // 2. Calculate the length of the headroom segment
        // This is simply the difference between where the data starts now (desc_addr)
        // and where the frame starts (frame_base).
        // Example: If base is 4096 and data starts at 4352 (256 bytes offset), headroom is 256.
        let headroom = self.w.desc_addr as usize- local_frame_base;
        FrameDesc {
            addr: self.w.desc_addr as usize,
            options: self.w.desc_options,
            lengths: SegmentLengths {
                headroom,
                data: self.w.desc_len as usize,
            },
        }
    }

    /// Consumes the PDU for transmission.
    /// Returns the FrameDesc needed for the TX ring.
    /// IMPORTANT: Prevents Drop() from running, so the frame is NOT returned to the pool yet.
    /// The Completion Queue logic in main.rs is responsible for eventually releasing this frame.
    #[inline]
    pub fn consume_for_tx(self) -> FrameDesc {
        let desc = self.to_frame_desc();

        // Prevent Drop from running.
        // We transfer ownership of the frame index to the Kernel/NIC implicitly.
        core::mem::forget(self);

        desc
    }

    /// Using this in parsing logic where we KNOW it exists
    #[inline(always)]
    pub fn umem_addr(&self) -> usize {
        unsafe { self.try_umem_addr().unwrap_unchecked() }
    }

    /// try get Umem pointer from SystemBus
    #[inline(always)]
     pub unsafe fn try_umem_addr(&self) -> Option<usize> {
        // 1. Safe Null Check (Prevents Segfaults)
        if self.system_bus.is_null() {
            return None;
        }

        // SAFETY: We guarantee the pointer is not null.
        // We assume the Host lifecycle guarantees it points to a valid SystemBus.
        let bus = unsafe { &*self.system_bus };

        // 2. Safe Bounds Check (Prevents Panics)
        // .get() returns Option<&Option<usize>>
        // .copied() turns it into Option<Option<usize>>
        // .flatten() turns it into Option<usize>
        bus.umem_addr
            .get(self.umem_id() as usize)
            .copied()
            .flatten()
    }

    /// returns pointer to the first byte of the frame this Pdu represents
    #[inline(always)]
    pub fn base_ptr(&self) -> *const u8 {
        unsafe { (self.umem_addr() as *const u8).add(self.local_frame_offset()) }
    }

    /// returns pointer converted to usize to the first byte of the frame this Pdu represents
    #[inline(always)]
    pub fn base_addr(&self) -> usize {
        self.umem_addr().add(self.local_frame_offset())
    }
    
    
    /// Prepares a freshly allocated PDU for transmission.
    /// Locks the descriptor to the physical frame boundary + headroom.
    #[inline(always)]
    pub fn reset_for_tx(&mut self, headroom: usize) {
        self.desc_addr = (self.local_frame_offset() + headroom) as u64;
        self.desc_len = 0;
        self.headers_mut().clear();
    }
    
    /// Creates a lightweight copy of the Pdu.
    ///
    /// increments the reference count in the SystemBus for this frame_idx.
    /// The actual packet data in UMEM is NOT copied.
    pub fn shallow_copy(&self) -> Self {
        unsafe {
            // 1. Notify the central logic that this frame has a new reference
            if !self.system_bus.is_null() {
                let bus = &*self.system_bus;
                let _ = bus.bus_senders[self.owner_id() as usize].send(BusCommand::Retain(self.global_frame_idx()));
            }
        }

        // 2. Clone the metadata.
        // Note: HeaderStack.clone() is cheap (it copies pointers, not data).
        Pdu {
            w: self.w.clone(),
            system_bus: self.system_bus, // Both PDUs point to the same bus
        }
    }

}

impl Pdu {
    #[inline]
    fn payload_mut(&mut self, which: usize) -> Option<*mut u8> {
        let headers = self.headers().count();
        match which {
            x if x + 1 < headers => self.headers().get(self.base_addr(), x + 1).as_ptr_u8_mut(),
            x if x == headers - 1 => {
                let mut h = self.headers().get(self.base_addr(), x);
                // Assume Header has .offset() returning length
                Some(unsafe { h.as_ptr_u8_mut().unwrap().offset(h.offset().unwrap() as isize) })
            }
            _ => None,
        }
    }

    #[inline]
    fn payload(&self, which: usize) -> Option<*const u8> {
        let headers = self.headers().count();
        match which {
            x if x + 1 < headers => self.headers().get(self.base_addr(), x + 1).as_ptr_u8(),
            x if x == headers - 1 =>
                Some(unsafe {
                    self.headers()
                        .get(self.base_addr(), x)
                        .as_ptr_u8()
                        .unwrap()
                        .offset(self.headers().get(self.base_addr(), x).offset().unwrap() as isize)
                }),
            _ => None,
        }
    }

    #[inline]
    pub fn get_payload(&self, which: usize) -> &[u8] {
        unsafe {
            let len = self.payload_size(which);
            slice::from_raw_parts(self.payload(which).unwrap(), len)
        }
    }

    #[inline]
    pub fn get_payload_mut(&mut self, which: usize) -> &mut [u8] {
        unsafe {
            let len = self.payload_size(which);
            slice::from_raw_parts_mut(self.payload_mut(which).unwrap(), len)
        }
    }

    /// may include padding
    #[inline]
    pub fn payload_size(&self, which: usize) -> usize {
        // Replaced slice iterator with index loop
        let mut sum = 0;
        for i in 0..=which {
            // Assuming header.offset() returns the header length in bytes
            if let Some(len) = self.headers().get(self.base_addr(), i).offset() {
                sum += len;
            }
        }
        if sum > self.w.data_len() as usize { 0 } else { self.w.data_len() as usize - sum }
    }

    // Helper for payload management
    #[inline]
    fn increase_payload_size(&mut self, inc: usize) -> usize {
        // Simple bounds check could go here if we knew max frame size
        self.w.desc_len += inc as u32;
        inc
    }

    #[inline]
    pub fn copy_payload_from_u8_slice(&mut self, payload: &[u8], which: usize) -> usize {
        let copy_len = payload.len();
        if copy_len > 0 {
            let dst = self.payload_mut(which).unwrap();
            let src = payload.as_ptr();
            let payload_size = self.payload_size(which);
            let should_copy = if payload_size < copy_len {
                let increment = copy_len - payload_size;
                payload_size + self.increase_payload_size(increment)
            } else {
                copy_len
            };
            unsafe {
                ptr::copy_nonoverlapping(src, dst, should_copy);
                should_copy
            }
        } else {
            0usize
        }
    }

    /// fills the end of the payload with <len> bytes of value <byte>
    #[inline]
    pub fn write_from_tail_down(&mut self, len: usize, byte: u8) -> usize {
        let payload_size = self.payload_size(self.headers().count() - 1);
        if payload_size > 0 {
            let count = cmp::min(payload_size, len);
            let headers = self.headers().count();
            let dst = unsafe {
                self.payload_mut(headers - 1)
                    .unwrap()
                    .offset(payload_size as isize - count as isize)
            };
            unsafe {
                ptr::write_bytes(dst, byte, count);
            }
            count
        } else {
            0
        }
    }

    #[inline]
    pub fn parse_fast(&mut self) -> usize {
        let umem_addr= self.umem_addr();
        self.w.parse_fast(umem_addr)
    }
    
    
    /// Returns a mutable pointer to the start of the packet data plus the offset argument.
    #[inline(always)]
    pub fn data_address(&self, offset: usize) -> *mut u8 {
        let umem_addr = self.umem_addr();
        // Add offset to the virtual memory pointer of the frame
        (umem_addr + self.desc_addr as usize + offset) as *mut u8
    }
    
    
    pub fn ether_type(&self) -> u16 {
        if let Some(umem_addr) = unsafe { self.try_umem_addr() } {
            self.w.ether_type(umem_addr)
        } else { 0 }
    }

    pub fn ipv4_src(&self) -> Option<u32> {
        if let Some(umem_addr) = unsafe { self.try_umem_addr() } {
            self.w.ipv4_src(umem_addr as usize)
        } else { None }
    }
}

#[inline]
pub fn update_tcp_checksum_(tcp_header: &mut TcpHeader, ip_payload_size: usize, ip_src: u32, ip_dst: u32) {
    let chk;
    {
        chk = ipv4_checksum(
            tcp_header as *mut TcpHeader as *mut u8,
            ip_payload_size,
            8,
            &[],
            ip_src,
            ip_dst,
            6u32,
        );
    }
    tcp_header.set_checksum(chk);
}

#[cfg(test)]

mod tests {
    use alloc::vec;
    use alloc::vec::Vec;
    use super::*;
    use core::ptr;

    /*
        // A dummy header to simulate an Ethernet header or similar
        #[repr(C, packed)]
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct TestHeader {
            src: u32,
            dst: u32,
            etype: u16,
        }

        #[test]
        fn test_push_header_success() {
            // 1. Setup Mock UMEM
            // Allocate a buffer representing a 2KB frame
            let mut umem_buffer: Vec<u8> = vec![0; 2048];
            let buffer_base = umem_buffer.as_mut_ptr();

            // 2. Define Initial State
            // Simulate a packet that starts at offset 256 (leaving 256 bytes of headroom)
            const HEADROOM: usize = 256;
            const INITIAL_PAYLOAD_LEN: usize = 100;

            let initial_start_ptr = unsafe { buffer_base.add(HEADROOM) };

            // Fill the payload area with some known data (0xAA)
            unsafe {
                ptr::write_bytes(initial_start_ptr, 0xAA, INITIAL_PAYLOAD_LEN);
            }

            // 3. Construct the PDU
            // (Assuming Pdu struct structure based on your files)
            let mut pdu = Pdu {
                header_stack: HeaderStack::new(initial_start_ptr),
                desc_addr: HEADROOM,
                desc_len: INITIAL_PAYLOAD_LEN as u32,
                desc_options: 0u32,
                frame_idx: 0u32,
                frame_size_log2: 11,
                owner_id: 0u8,
                flags: 0u8,
                _padding: 0u8,
                system_bus: 0u64 as *const SystemBus,
                timestamp: 0u64,
            };

            // Simulate that we have parsed something (hc > 0) to ensure clear() works
            // (We are manually tweaking internal state for the test)
            pdu.header_stack.hc = 1;

            // 4. Define the Header to Push
            let new_header = TestHeader {
                src: 0xAABBCCDD,
                dst: 0x11223344,
                etype: 0x0800,
            };
            let header_size = std::mem::size_of::<TestHeader>(); // 10 bytes

            // 5. Perform the Push
            let result = pdu.prepend_header(&new_header);

            // 6. Assertions
            assert!(result.is_ok(), "push_header should succeed");

            unsafe {
                // A. Check Pointer Movement
                // The new pointer should be (initial - header_size)
                let expected_ptr = initial_start_ptr.sub(header_size);
                assert_eq!(pdu.header_stack.base_ptr(), expected_ptr, "Base pointer did not move backwards correctly");

                // B. Check Length Update
                assert_eq!(pdu.desc_len as usize, INITIAL_PAYLOAD_LEN + header_size, "PDU length did not increase by header size");

                // C. Check Header Data Integrity
                // Cast the new pointer location back to a TestHeader to verify contents
                let written_header = *(expected_ptr as *const TestHeader);
                assert_eq!(written_header, new_header, "Header data written to memory does not match");

                // D. Check Payload Integrity
                // The byte immediately after the header should still be our payload (0xAA)
                let payload_start = expected_ptr.add(header_size);
                assert_eq!(*payload_start, 0xAA, "Original payload was corrupted");

                // E. Check Stack Invalidated
                assert_eq!(pdu.header_stack.hc, 0, "Header stack count should be reset to 0");
            }
        }
    }

    */

    // A dummy header struct for testing purposes
    #[repr(C, packed)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestHeader {
        id: u32,
        flags: u16,
    }

    #[test]
    fn test_push_header_moves_pointer_and_updates_len() {
        // 1. Setup Mock UMEM (Simulate a 2048-byte frame)
        let mut umem_buffer: Vec<u8> = vec![0; 2048];
        let umem_base = umem_buffer.as_mut_ptr();

        // 2. Define Initial State
        // Frame Index 0 starts at offset 0.
        // We simulate a packet that currently has 256 bytes of headroom.
        let frame_size_log2 = 11; // 2^11 = 2048
        let initial_headroom = 256;
        let initial_payload_len = 100;

        // The current data starts at 256
        let initial_desc_addr = initial_headroom;

        let desc = FrameDesc {
            addr: initial_desc_addr,
            lengths: SegmentLengths {
                headroom: initial_headroom,
                data: initial_payload_len,
            },
            options: 0,
        };

        // 3. Create PDU
        // We pass a null pointer for SystemBus as we won't trigger bus operations
        let mut pdu = Pdu::new(
            desc,
            0, // frame_idx
            0, // owner_id
            core::ptr::null(), // system_bus
            frame_size_log2,
        );

        // 4. Fill Payload with known pattern (0xAA) to verify it stays intact
        unsafe {
            // Use the public API to get the data pointer
            let payload_ptr = pdu.w.data_ptr(umem_base as usize);
            ptr::write_bytes(payload_ptr, 0xAA, initial_payload_len);
        }

        // 5. Create Header to Push
        let header = TestHeader {
            id: 0xDEADBEEF,
            flags: 0xCAFE,
        };
        let header_size = core::mem::size_of::<TestHeader>(); // 6 bytes

        // 6. Perform Push
        // (Assuming the method is named push_header as requested)
        pdu.w.push_header(umem_base as usize, &header).expect("push_header failed");

        // 7. ASSERTIONS

        // A. Verify Length and Headroom updates via Public API
        assert_eq!(
            pdu.w.data_len() as usize,
            initial_payload_len + header_size,
            "Data length should increase"
        );
        assert_eq!(pdu.w.headroom(), initial_headroom - header_size, "Headroom should decrease");

        // B. Verify Address update
        // The new descriptor address should be 256 - 6 = 250
        let new_desc = pdu.to_frame_desc();
        assert_eq!(
            new_desc.addr(),
            initial_desc_addr - header_size,
            "Descriptor address did not move backwards"
        );

        // C. Verify Memory Content
        unsafe {
            let current_data_ptr = pdu.w.data_ptr(umem_base as usize);

            // 1. Check if Header is written at the new start
            let written_header = *(current_data_ptr as *const TestHeader);
            assert_eq!(written_header, header, "Header data was not written correctly to memory");

            // 2. Check if original Payload is preserved immediately after the header
            let payload_check_ptr = current_data_ptr.add(header_size);
            assert_eq!(
                *payload_check_ptr, 0xAA,
                "Original payload data corrupted or offset incorrect"
            );
        }
    }

    #[test]
    fn test_push_header_insufficient_headroom() {
        // Setup UMEM
        let mut umem_buffer: Vec<u8> = vec![0; 2048];
        let umem_base = umem_buffer.as_mut_ptr();

        // Create a PDU with ZERO headroom (addr = 0)
        let desc = FrameDesc {
            addr: 0,
            lengths: SegmentLengths { headroom: 0, data: 100 },
            options: 0,
        };

        let mut pdu = Pdu::new(desc, 0, 0, core::ptr::null(), 11);

        let header = TestHeader { id: 1, flags: 1 };

        // Should fail
        let result = pdu.w.push_header(umem_base as usize, &header);
        assert!(result.is_err(), "Should return error when headroom is insufficient");
    }
}
