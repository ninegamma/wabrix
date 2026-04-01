use core::ptr;
use crate::headers::hstack::HeaderStack;
use crate::headers::{ArpIpv4Header, EndOffset, Header, HeaderKind, IcmpHeader, IpHeader, Ipv6Header, MacHeader, TcpHeader, UdpHeader, MAC_HDR_SIZE};
// Adjust imports as needed

#[repr(C)]
#[derive(Clone, Debug)]
pub struct WPdu {
    /// [0..24] 24 Bytes: HeaderStack
    pub header_stack: HeaderStack,

    /// [24..32] 8 Bytes: The data pointer (absolute UMEM offset)
    /// Corresponds to FrameDesc.addr
    pub desc_addr: u64,

    /// [32..36] 4 Bytes: Data length
    /// Corresponds to FrameDesc.lengths.data
    pub desc_len: u32,

    /// [36..40] 4 Bytes: XDP Options
    /// Corresponds to FrameDesc.options
    pub desc_options: u32,

    /// [40..48] 8 Bytes: Metadata
    pub global_frame_idx: u32, // 4 bytes, identifies the frame over all UMEM instances and partitions
    pub frame_size_log2: u8,     // 1 byte - log2(frame_size) for bit-shift calculations
    pub owner_id: u8, // 1 byte
    pub flags: u8,               // 1 byte
    pub _padding: u8,            // 1 byte

    /// [48..56] 8 Bytes: Telemetry
    pub timestamp: u64,
}

const _: () = assert!(core::mem::size_of::<WPdu>() == 56);
const _: () = assert!(core::mem::size_of::<HeaderStack>() == 24);


impl Default for WPdu {
    fn default() -> Self {
        WPdu {
            header_stack: HeaderStack::new(core::ptr::null_mut()),
            desc_addr: 0,
            desc_len: 0,
            desc_options: 0,
            global_frame_idx: 0,
            frame_size_log2: 11, // 2^11 = 2048 (common default frame size)
            owner_id: 0,
            flags: 0,
            _padding: 0,
            timestamp: 0,
        }
    }
}

impl WPdu {
    /// Helper: Get the frame size in bytes
    #[inline(always)]
    fn frame_size(&self) -> usize {
        1usize << self.frame_size_log2
    }

    /// Calculates the byte offset of the frame's start relative to the UMEM base.
    /// Returns a byte count (e.g., 0, 2048, 4096), NOT an array index.
    #[inline(always)]
    pub fn local_frame_offset(&self) -> usize {
        (self.desc_addr & !((1usize << self.frame_size_log2) - 1) as u64) as usize
    }

    #[inline(always)]
    fn local_frame_idx(&self) -> u32 {
        (self.local_frame_offset() >> self.frame_size_log2) as u32
    }



    /// Accessors for logic compatibility
    #[inline]
    pub fn data_len(&self) -> usize {
        self.desc_len as usize
    }

    #[inline]
    pub fn global_frame_idx(&self) -> u32 {
        self.global_frame_idx
    }

    #[inline]
    pub fn owner_id(&self) -> u8 {
        self.owner_id
    }

    #[inline]
    pub fn headroom(&self) -> usize {
        self.desc_addr as usize - self.local_frame_offset()
    }

    /// Returns a pointer to the start of the packet data in the frame.
    #[inline]
    pub fn data_ptr(&self) -> *mut u8 {
        self.data_address(0)
    }

    /// Sets the packet data length.
    /// Use this when you've written data into the frame and need to update the length.
    #[inline]
    pub fn set_data_length(&mut self, length: u32) {
        self.desc_len = length;
    }

    /// Returns a mutable pointer to the start of the packet data plus the offset argument.
    ///
    /// Calculation:
    /// frame_start_ptr (from HeaderStack) + (desc_addr - absolute_frame_offset)
    #[inline(always)]
    pub fn data_address(&self, offset: usize) -> *mut u8 {
        // 1. Calculate the absolute offset of the frame start in the UMEM
        let local_frame_start = self.local_frame_offset();

        // 2. Calculate where the data is relative to the frame start and add offset argument
        //    (e.g., Data starts at offset 256 in frame, plus additional offset)
        let offset_in_frame = self.desc_addr - local_frame_start as u64 + offset as u64;

        // 3. Add that offset to the virtual memory pointer of the frame
        unsafe { self.header_stack.base_ptr().add(offset_in_frame as usize) }
    }

    /// Assumes an Ethernet frame and parses up to L4
    #[inline]
    pub fn parse(&mut self) -> usize {
        // Clear any existing headers before parsing
        self.header_stack.clear();

        let l = self.data_len();
        if l < MacHeader::size() {
            return 0;
        };

        // Get pointer to the start of the buffer
        let hdr_ptr = self.data_address(0) as *mut MacHeader;

        unsafe {
            self.header_stack.push(Header::Mac(&mut *hdr_ptr));
            let mac = &*hdr_ptr;

            match mac.etype() {
                0x0800 => {
                    // IPv4
                    if l >= mac.offset() + IpHeader::size() {
                        self.parse_ipv4(mac.offset());
                    }
                }
                0x86DD => {
                    // IPv6
                    if l >= mac.offset() + Ipv6Header::size() {
                        self.parse_ipv6(mac.offset());
                    }
                }
                0x0806 => {
                    // ARP
                    if l >= mac.offset() + ArpIpv4Header::size() {
                        self.parse_arp(mac.offset());
                    }
                }
                _ => {}
            }
        }
        self.header_stack.count()
    }

    /// The fixed Fast Parser. It now accounts for the headroom!
    /// Supports: IPv4, IPv6, ARP, TCP, UDP, ICMP, ICMPv6, SCTP
    #[inline]
    pub fn parse_fast(&mut self) -> usize {
        self.header_stack.clear();
        let l = self.data_len();
        if l < MAC_HDR_SIZE {
            return 0;
        }

        // Fetch headroom once to shift all our manual offsets
        let hr = self.headroom() as u16;

        self.header_stack.kinds[0] = HeaderKind::Mac;
        self.header_stack.offsets[0] = hr;
        self.header_stack.hc = 1;

        let mac_ptr = self.data_address(0) as *const MacHeader;
        let etype = unsafe { (*mac_ptr).etype() };

        match etype {
            0x0800 => {
                // IPv4
                if l < 14 + 20 {
                    return 1;
                } // Minimum IPv4 header size

                self.header_stack.kinds[1] = HeaderKind::Ip;
                self.header_stack.offsets[1] = hr + 14;

                let ip_ptr = self.data_address(14) as *const IpHeader;
                let l4_proto = unsafe { (*ip_ptr).protocol() };
                let ip_len = unsafe { (*ip_ptr).offset() as u16 };

                let l4_offset = 14 + ip_len;

                match l4_proto {
                    1 => {
                        // ICMP
                        if l >= (l4_offset + 8) as usize {
                            self.header_stack.kinds[2] = HeaderKind::Icmp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    6 => {
                        // TCP
                        if l >= (l4_offset + 20) as usize {
                            self.header_stack.kinds[2] = HeaderKind::Tcp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    17 => {
                        // UDP
                        if l >= (l4_offset + 8) as usize {
                            self.header_stack.kinds[2] = HeaderKind::Udp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    132 => {
                        // SCTP
                        if l >= (l4_offset + 12) as usize {
                            // Minimum SCTP header: 12 bytes
                            // Note: SCTP not yet in HeaderKind enum, would need to be added
                            // For now, just mark as parsed at IP level
                            self.header_stack.hc = 2;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    _ => {
                        self.header_stack.hc = 2;
                    }
                }
            }
            0x86DD => {
                // IPv6
                if l < 14 + 40 {
                    return 1;
                } // IPv6 header is fixed 40 bytes

                self.header_stack.kinds[1] = HeaderKind::Ipv6;
                self.header_stack.offsets[1] = hr + 14;

                let ipv6_ptr = self.data_address(14) as *const Ipv6Header;
                let next_header = unsafe { (*ipv6_ptr).next_header() };

                let l4_offset = 14 + 40; // IPv6 header is always 40 bytes (no options in base header)

                match next_header {
                    6 => {
                        // TCP
                        if l >= l4_offset + 20 {
                            self.header_stack.kinds[2] = HeaderKind::Tcp;
                            self.header_stack.offsets[2] = hr + l4_offset as u16;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    17 => {
                        // UDP
                        if l >= l4_offset + 8 {
                            self.header_stack.kinds[2] = HeaderKind::Udp;
                            self.header_stack.offsets[2] = hr + l4_offset as u16;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    58 => {
                        // ICMPv6
                        if l >= l4_offset + 8 {
                            self.header_stack.kinds[2] = HeaderKind::Icmp;
                            self.header_stack.offsets[2] = hr + l4_offset as u16;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    132 => {
                        // SCTP
                        if l >= l4_offset + 12 {
                            // SCTP not yet in HeaderKind enum
                            self.header_stack.hc = 2;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    _ => {
                        self.header_stack.hc = 2;
                    }
                }
            }
            0x0806 => {
                // ARP
                if l >= 14 + 28 {
                    // Minimum ARP for IPv4: 28 bytes
                    self.header_stack.kinds[1] = HeaderKind::ArpIpv4;
                    self.header_stack.offsets[1] = hr + 14;
                    self.header_stack.hc = 2;
                }
            }
            _ => {
                // Unknown EtherType - just MAC header
            }
        }

        self.header_stack.count()
    }

    #[inline]
    fn parse_ipv4(&mut self, offset: usize) {
        let hdr_ptr = self.data_address(offset) as *mut IpHeader;
        unsafe {
            let ip = &mut *hdr_ptr;
            self.header_stack.push(Header::Ip(ip));

            let next_offset = offset + ip.offset();
            match ip.protocol() {
                1 => self.parse_icmp(next_offset),
                6 => self.parse_tcp(next_offset),
                17 => self.parse_udp(next_offset),
                _ => {}
            }
        }
    }

    #[inline]
    fn parse_tcp(&mut self, offset: usize) {
        let hdr = self.data_address(offset) as *mut TcpHeader;
        unsafe {
            self.header_stack.push(Header::Tcp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_udp(&mut self, offset: usize) {
        let hdr = self.data_address(offset) as *mut UdpHeader;
        unsafe {
            self.header_stack.push(Header::Udp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_icmp(&mut self, offset: usize) {
        let hdr = self.data_address(offset) as *mut IcmpHeader;
        unsafe {
            self.header_stack.push(Header::Icmp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_arp(&mut self, offset: usize) {
        //TODO generalize for any protocol type, not only Ipv4
        let hdr = self.data_address(offset) as *mut ArpIpv4Header;

        unsafe {
            let arp = &mut *hdr;
            if arp.hw_type() == 1 && arp.proto_etype() == 0x0800 {
                self.header_stack.push(Header::ArpIpv4(&mut *hdr));
            }
        }
    }

    #[inline]
    fn parse_ipv6(&mut self, offset: usize) {
        let hdr = self.data_address(offset) as *mut Ipv6Header;
        unsafe {
            self.header_stack.push(Header::Ipv6(&mut *hdr));
        }
        let next_header;
        unsafe {
            let ipv6 = &mut *hdr;
            next_header = ipv6.next_header();
        }
        match next_header {
            6 => {
                // TCP
                if self.data_len() >= offset + Ipv6Header::size() + TcpHeader::size() {
                    self.parse_tcp(offset + Ipv6Header::size());
                }
            }
            17 => {
                // UDP
                if self.data_len() >= offset + Ipv6Header::size() + UdpHeader::size() {
                    self.parse_udp(offset + Ipv6Header::size());
                }
            }
            58 => {
                // ICMPv6
                if self.data_len() >= offset + Ipv6Header::size() + IcmpHeader::size() {
                    self.parse_icmp(offset + Ipv6Header::size());
                }
            }
            _ => {}
        }
    }

    /// Adds a new header onto the front of the PDU.
    ///
    /// # Safety
    /// This method assumes there is sufficient **Headroom** available in the
    /// underlying memory buffer before `header_stack.base_ptr`.
    ///
    /// Pushes a new header onto the front of the PDU.
    ///
    /// This consumes headroom, updates the descriptor address/length,
    /// and invalidates the header stack.
    pub fn push_header<T: Copy>(&mut self, header: &T) -> anyhow::Result<()> {
        let header_size = core::mem::size_of::<T>();

        // 1. Calculate Frame Boundaries
        // frame_base is the absolute UMEM index where this frame starts
        let local_frame_base = self.local_frame_offset();

        // 2. Check Headroom
        // desc_addr is the absolute UMEM index where data currently starts.
        // We must ensure that moving back by header_size doesn't cross the frame boundary.
        //trace!("Pdu::push_header: desc_addr = {:#x}, frame_base = {:#x}, header_size = {:#x}", self.desc_addr, frame_base, header_size);
        if self.desc_addr < local_frame_base as u64 + header_size as u64 {
            return Err(anyhow::anyhow!("Insufficient headroom to push header"));
        }

        // 3. Calculate New Absolute Address
        let new_desc_addr = self.desc_addr - header_size as u64;

        unsafe {
            // 4. Calculate the write pointer
            // header_stack.base_ptr() points to the Frame Start (Physical Address)
            let frame_start_ptr = self.header_stack.base_ptr();

            // Calculate the offset relative to the Frame Start
            let offset_in_frame = new_desc_addr - local_frame_base as u64;

            // Get the destination address
            let dst_ptr = frame_start_ptr.add(offset_in_frame as usize);

            // 5. Write the header data
            ptr::copy_nonoverlapping(header as *const T as *const u8, dst_ptr, header_size);
        }

        // 6. Update PDU State
        self.desc_addr = new_desc_addr;
        self.desc_len += header_size as u32;

        // 7. Invalidate Parsed Headers
        // The offsets in HeaderStack are no longer valid because we changed
        // the structure of the packet.
        self.header_stack.clear();

        Ok(())
    }

    #[inline]
    pub fn is_ipv4(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::Ip)
    }

    #[inline]
    pub fn is_tcp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::Tcp)
    }

    #[inline]
    pub fn is_udp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::Udp)
    }

    #[inline]
    pub fn is_icmp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::Icmp)
    }

    #[inline]
    pub fn is_arp_ipv4(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::ArpIpv4)
    }

    #[inline]
    pub fn is_ipv6(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.get(i).kind() == HeaderKind::Ipv6)
    }

    /// Gibt den Typ des obersten (letzten) Headers zurück
    #[inline]
    pub fn top_header_kind(&self) -> HeaderKind {
        if self.header_stack.count() == 0 {
            HeaderKind::Null
        } else {
            self.header_stack.get(self.header_stack.count() - 1).kind()
        }
    }

    /// Get the EtherType from the MAC header (if present)
    #[inline]
    pub fn ether_type(&self) -> u16 {
        for i in 0..self.header_stack.count() {
            if self.header_stack.get(i).kind() == HeaderKind::Mac {
                return self.header_stack.mac(i).etype();
            }
        }
        0 // Return 0 if no MAC header found
    }

    /// Get the IPv4 source address (if present)
    #[inline]
    pub fn ipv4_src(&self) -> Option<u32> {
        for i in 0..self.header_stack.count() {
            if self.header_stack.get(i).kind() == HeaderKind::Ip {
                return Some(self.header_stack.ip(i).src());
            }
        }
        None
    }
}

