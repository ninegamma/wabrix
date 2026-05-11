use core::ptr;
use crate::headers::hstack::HeaderStack;
use crate::headers::{
    ArpIpv4Header, EndOffset, Header, HeaderKind, IcmpHeader, IpHeader, Ipv6Header, MacHeader, TcpHeader, UdpHeader,
    MAC_HDR_SIZE,
};
// Adjust imports as needed

#[repr(C)]
#[derive(Clone, Debug)]
pub struct WPdu {
    /// 16 Bytes: HeaderStack
    header_stack: HeaderStack,

    /// 8 Bytes: The data pointer (absolute UMEM offset)
    /// Corresponds to FrameDesc.addr
    pub desc_addr: u64,

    /// 4 Bytes: Data length
    /// Corresponds to FrameDesc.lengths.data
    pub desc_len: u32,

    /// 4 Bytes: XDP Options
    /// Corresponds to FrameDesc.options
    pub desc_options: u32,

    /// 16 Bytes: Metadata
    global_frame_idx: u32, // 4 bytes, identifies the frame over all UMEM instances and partitions
    pub frame_size_log2: u8, // 1 byte - log2(frame_size) for bit-shift calculations
    owner_id: u8,            // 1 byte
    pub flags: u8,           // 1 byte
    pub _padding: [u8; 9],   // 9 byte

    /// 8 Bytes: Telemetry
    pub timestamp: u64,
}

impl WPdu {
    pub fn new(
        desc_addr: u64,
        desc_len: u32,
        desc_options: u32,
        global_frame_idx: u32,
        frame_size_log2: u8,
        owner_id: u8,
    ) -> WPdu {
        WPdu {
            header_stack: HeaderStack::new(),
            desc_addr,
            desc_len,
            desc_options,
            global_frame_idx,
            frame_size_log2,
            owner_id,
            ..Default::default()
        }
    }
}

const _: () = assert!(core::mem::size_of::<WPdu>() == 56);
const _: () = assert!(core::mem::size_of::<HeaderStack>() == 16);

impl Default for WPdu {
    fn default() -> Self {
        WPdu {
            header_stack: HeaderStack::new(),
            desc_addr: 0,
            desc_len: 0,
            desc_options: 0,
            global_frame_idx: 0,
            frame_size_log2: 11, // 2^11 = 2048 (common default frame size)
            owner_id: 0,
            flags: 0,
            _padding: [0; 9],
            timestamp: 0,
        }
    }
}

impl WPdu {

    /// Helper: Get the frame size in bytes
    #[inline(always)]
    pub(crate) fn frame_size(&self) -> usize {
        1usize << self.frame_size_log2
    }

    /// Calculates the byte offset of the frame's start relative to the UMEM base.
    /// Returns a byte count (e.g., 0, 2048, 4096), NOT an array index.
    #[inline(always)]
    pub fn local_frame_offset(&self) -> usize {
        (self.desc_addr & !((1u64 << self.frame_size_log2) - 1)) as usize
    }

    #[inline(always)]
    fn local_frame_idx(&self) -> u32 {
        (self.local_frame_offset() >> self.frame_size_log2) as u32
    }

    /// Accessors for logic compatibility
    #[inline]
    pub fn data_len(&self) -> u32 {
        self.desc_len
    }

    #[inline]
    pub fn global_frame_idx(&self) -> u32 {
        self.global_frame_idx
    }

    #[inline]
    pub fn umem_id(&self) -> u8 {
        (self.global_frame_idx >> 24) as u8
    }
    
    #[inline]
    pub fn owner_id(&self) -> u8 {
        self.owner_id
    }

    #[inline]
    pub fn headroom(&self) -> usize {
        (self.desc_addr - self.local_frame_offset() as u64) as usize
    }

    /// Returns a pointer to the start of the packet data in the frame.
    #[inline]
    pub fn data_ptr(&self, umem_addr: usize,) -> *mut u8 {
        self.data_address(umem_addr, 0)
    }

    /// Sets the packet data length.
    /// Use this when you've written data into the frame and need to update the length.
    #[inline]
    pub fn set_data_length(&mut self, length: u32) {
        self.desc_len = length;
    }

    /// Returns a mutable pointer to the start of the packet data plus the offset argument.
    #[inline(always)]
    pub fn data_address(&self, umem_addr: usize, offset: usize) -> *mut u8 {
        // Add offset to the virtual memory pointer of the frame
        (umem_addr + self.desc_addr as usize + offset) as *mut u8
    }
    #[inline(always)]
    pub fn base_ptr(&self, umem_addr: usize) -> *mut u8 {
        (umem_addr + self.local_frame_offset()) as * mut u8
    }
    #[inline(always)]
    pub fn base_addr(&self, umem_addr: usize) -> usize {
        umem_addr + self.local_frame_offset() 
    }
    #[inline]
    pub fn headers(&self) -> &HeaderStack {
        &self.header_stack
    }

    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderStack {
        &mut self.header_stack
    }

    pub fn get_header_kind(&self, which: usize) -> HeaderKind {
        if which < self.header_stack.count() {
            self.header_stack.kinds[which]
        } else {
            HeaderKind::Null
        }
    }

    #[inline]
    pub fn replace_header(&mut self, umem_addr: usize, which: usize, hdr: &Header) {
        unsafe {
            // HeaderStack calculates headers on the fly. We get a view (pointer wrapper).
            let mut pdu_header = self.header_stack.get(self.base_addr(umem_addr), which);
            assert_eq!(hdr.kind(), pdu_header.kind());

            match pdu_header {
                Header::Null => (),
                Header::Mac(ref mut p) => ptr::copy_nonoverlapping(hdr.as_mac().unwrap() as *const MacHeader, *p, 1),
                Header::Ip(ref mut p) => ptr::copy_nonoverlapping(hdr.as_ip().unwrap() as *const IpHeader, *p, 1),
                Header::Ipv6(ref mut p) => ptr::copy_nonoverlapping(hdr.as_ipv6().unwrap() as *const Ipv6Header, *p, 1),
                Header::Tcp(ref mut p) => ptr::copy_nonoverlapping(hdr.as_tcp().unwrap() as *const TcpHeader, *p, 1),
                Header::Udp(ref mut p) => ptr::copy_nonoverlapping(hdr.as_udp().unwrap() as *const UdpHeader, *p, 1),
                Header::Icmp(ref mut p) => ptr::copy_nonoverlapping(hdr.as_icmp().unwrap() as *const IcmpHeader, *p, 1),
                Header::ArpIpv4(ref mut p) => {
                    ptr::copy_nonoverlapping(hdr.as_arpipv4().unwrap() as *const ArpIpv4Header, *p, 1)
                }
            };
        }
    }

    /// Assumes an Ethernet frame and parses up to L4
    #[inline]
    pub fn parse(&mut self, umem_addr: usize,) -> usize {
        // Clear any existing headers before parsing
        self.header_stack.clear();

        let l = self.data_len() as usize;
        if l < MacHeader::size() {
            return 0;
        };

        // Get pointer to the start of the buffer
        let hdr_ptr = self.data_address(umem_addr, 0) as *mut MacHeader;

        unsafe {
            self.header_stack.push(self.base_addr(umem_addr), Header::Mac(&mut *hdr_ptr));
            let mac = &*hdr_ptr;

            match mac.etype() {
                0x0800 => {
                    // IPv4
                    if l >= mac.offset() + IpHeader::size() {
                        self.parse_ipv4(umem_addr, mac.offset());
                    }
                }
                0x86DD => {
                    // IPv6
                    if l >= mac.offset() + Ipv6Header::size() {
                        self.parse_ipv6(umem_addr, mac.offset());
                    }
                }
                0x0806 => {
                    // ARP
                    if l >= mac.offset() + ArpIpv4Header::size() {
                        self.parse_arp(umem_addr, mac.offset());
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
    pub fn parse_fast(&mut self, umem_addr: usize,) -> usize {
        self.header_stack.clear();
        let l = self.data_len();
        if l < MAC_HDR_SIZE as u32 {
            return 0;
        }

        // Fetch headroom once to shift all our manual offsets
        let hr = self.headroom() as u16;

        self.header_stack.kinds[0] = HeaderKind::Mac;
        self.header_stack.offsets[0] = hr;
        self.header_stack.hc = 1;

        let mac_ptr = self.data_address(umem_addr, 0) as *const MacHeader;
        let etype = unsafe { (*mac_ptr).etype() };

        match etype {
            0x0800 => {
                // IPv4
                if l < 14 + 20 {
                    return 1;
                } // Minimum IPv4 header size

                self.header_stack.kinds[1] = HeaderKind::Ip;
                self.header_stack.offsets[1] = hr + 14;

                let ip_ptr = self.data_address(umem_addr, 14) as *const IpHeader;
                let l4_proto = unsafe { (*ip_ptr).protocol() };
                let ip_len = unsafe { (*ip_ptr).offset() as u16 };

                let l4_offset = 14 + ip_len;

                match l4_proto {
                    1 => {
                        // ICMP
                        if l >= (l4_offset + 8) as u32 {
                            self.header_stack.kinds[2] = HeaderKind::Icmp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    6 => {
                        // TCP
                        if l >= (l4_offset + 20) as u32 {
                            self.header_stack.kinds[2] = HeaderKind::Tcp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    17 => {
                        // UDP
                        if l >= (l4_offset + 8) as u32 {
                            self.header_stack.kinds[2] = HeaderKind::Udp;
                            self.header_stack.offsets[2] = hr + l4_offset;
                            self.header_stack.hc = 3;
                        } else {
                            self.header_stack.hc = 2;
                        }
                    }
                    132 => {
                        // SCTP
                        if l >= (l4_offset + 12) as u32 {
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

                let ipv6_ptr = self.data_address(umem_addr, 14) as *const Ipv6Header;
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
    fn parse_ipv4(&mut self, umem_addr: usize, offset: usize) {
        let hdr_ptr = self.data_address(umem_addr, offset) as *mut IpHeader;
        unsafe {
            let ip = &mut *hdr_ptr;
            self.header_stack.push(self.base_addr(umem_addr), Header::Ip(ip));

            let next_offset = offset + ip.offset();
            match ip.protocol() {
                1 => self.parse_icmp(umem_addr, next_offset),
                6 => self.parse_tcp(umem_addr, next_offset),
                17 => self.parse_udp(umem_addr, next_offset),
                _ => {}
            }
        }
    }

    #[inline]
    fn parse_tcp(&mut self, umem_addr: usize, offset: usize) {
        let hdr = self.data_address(umem_addr, offset) as *mut TcpHeader;
        unsafe {
            self.header_stack.push(self.base_addr(umem_addr), Header::Tcp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_udp(&mut self, umem_addr: usize, offset: usize) {
        let hdr = self.data_address(umem_addr, offset) as *mut UdpHeader;
        unsafe {
            self.header_stack.push(self.base_addr(umem_addr), Header::Udp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_icmp(&mut self, umem_addr: usize, offset: usize) {
        let hdr = self.data_address(umem_addr, offset) as *mut IcmpHeader;
        unsafe {
            self.header_stack.push(self.base_addr(umem_addr), Header::Icmp(&mut *hdr));
        }
    }

    #[inline]
    fn parse_arp(&mut self, umem_addr: usize, offset: usize) {
        //TODO generalize for any protocol type, not only Ipv4
        let hdr = self.data_address(umem_addr, offset) as *mut ArpIpv4Header;

        unsafe {
            let arp = &mut *hdr;
            if arp.hw_type() == 1 && arp.proto_etype() == 0x0800 {
                self.header_stack.push(self.base_addr(umem_addr), Header::ArpIpv4(&mut *hdr));
            }
        }
    }

    #[inline]
    fn parse_ipv6(&mut self, umem_addr: usize, offset: usize) {
        let hdr = self.data_address(umem_addr, offset) as *mut Ipv6Header;
        unsafe {
            self.header_stack.push(self.base_addr(umem_addr), Header::Ipv6(&mut *hdr));
        }
        let next_header;
        unsafe {
            let ipv6 = &mut *hdr;
            next_header = ipv6.next_header();
        }
        match next_header {
            6 => {
                // TCP
                if self.data_len() >= (offset + Ipv6Header::size() + TcpHeader::size()) as u32 {
                    self.parse_tcp(umem_addr,offset + Ipv6Header::size());
                }
            }
            17 => {
                // UDP
                if self.data_len() >= (offset + Ipv6Header::size() + UdpHeader::size()) as u32 {
                    self.parse_udp(umem_addr,offset + Ipv6Header::size());
                }
            }
            58 => {
                // ICMPv6
                if self.data_len() >= (offset + Ipv6Header::size() + IcmpHeader::size()) as u32 {
                    self.parse_icmp(umem_addr,offset + Ipv6Header::size());
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
    pub fn push_header<T: Copy>(&mut self, umem_addr: usize, header: &T) -> anyhow::Result<()> {
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
            // Because new_desc_addr is the absolute UMEM offset, just add it!
            let dst_addr = umem_addr + new_desc_addr as usize;
            ptr::copy_nonoverlapping(header as *const T as *const u8, dst_addr as *mut u8, header_size);
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
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::Ip)
    }

    #[inline]
    pub fn is_tcp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::Tcp)
    }

    #[inline]
    pub fn is_udp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::Udp)
    }

    #[inline]
    pub fn is_icmp(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::Icmp)
    }

    #[inline]
    pub fn is_arp_ipv4(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::ArpIpv4)
    }

    #[inline]
    pub fn is_ipv6(&self) -> bool {
        (0..self.header_stack.count()).any(|i| self.header_stack.kinds[i] == HeaderKind::Ipv6)
    }

    /// Gibt den Typ des obersten (letzten) Headers zurück
    #[inline]
    pub fn top_header_kind(&self) -> HeaderKind {
        let count = self.header_stack.count();
        if count == 0 {
            HeaderKind::Null
        } else {
            self.header_stack.kinds[count - 1]
        }
    }

    /// Get the EtherType from the MAC header (if present)
    #[inline]
    pub fn ether_type(&self, umem_addr: usize) -> u16 {
        let base_addr = self.base_addr(umem_addr);
        for i in 0..self.header_stack.count() {
            if self.header_stack.get(base_addr, i).kind() == HeaderKind::Mac {
                return self.header_stack.mac(base_addr, i).etype();
            }
        }
        0 // Return 0 if no MAC header found
    }

    /// Get the IPv4 source address (if present)
    #[inline]
    pub fn ipv4_src(&self, umem_addr: usize) -> Option<u32> {
        for i in 0..self.header_stack.count() {
            if self.header_stack.get(self.base_addr(umem_addr), i).kind() == HeaderKind::Ip {
                return Some(self.header_stack.ip(self.base_addr(umem_addr), i).src());
            }
        }
        None
    }
}
