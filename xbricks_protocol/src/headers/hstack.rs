use core::fmt;
use crate::headers::{ArpIpv4Header, Header, HeaderKind, IcmpHeader, IpHeader, Ipv6Header, MacHeader, TcpHeader, UdpHeader};

const MAX_HEADERS: usize = 5;

#[derive(Clone, Debug)]
#[repr(C)]
pub struct HeaderStack {
    /// Offsets relative to base_addr
    pub offsets: [u16; MAX_HEADERS],
    /// The type of header (Mac, Ip, etc.)
    pub kinds: [HeaderKind; MAX_HEADERS],
    /// Current count
    pub hc: u8,
}

// Exactly 16 bytes.
const _: () = assert!(core::mem::size_of::<HeaderStack>() == 16);

impl HeaderStack {
    #[inline]
    pub fn new() -> Self {
        HeaderStack {
            offsets: [0; MAX_HEADERS],
            kinds: [HeaderKind::Null; MAX_HEADERS],
            hc: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.hc = 0;
    }

    #[inline]
    pub fn push(&mut self, base_addr: usize, h: Header) {
        let idx = self.hc as usize;
        if idx >= MAX_HEADERS {
            // TODO: return an Err
            panic!("HeaderStack overflow: cannot push more than {} headers", MAX_HEADERS);
        }
        self.kinds[idx] = h.kind();

        let h_addr = h.as_ptr_u8().unwrap() as usize;
        debug_assert!(h_addr >= base_addr, "Header address is before base pointer!");
        debug_assert!(h_addr - base_addr <= u16::MAX as usize, "Header offset exceeds u16!");
        self.offsets[idx] = (h_addr - base_addr) as u16;
        self.hc += 1;
    }

    /// Internal helper to reconstruct the Header enum on the fly
    #[inline]
    fn reconstruct(&self, base_addr: usize, which: usize) -> Header {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let ptr_addr = base_addr + self.offsets[which] as usize;
        Header::from_kind_and_ptr(self.kinds[which], ptr_addr as *mut u8)
    }

    /// a fingerprint of the protocol stack. Returns a u32 where every 4 bits represent a HeaderKind
    #[inline]
    pub fn fingerprint(&self) -> u32 {
        let mut f = 0u32;
        for i in 0..self.hc as usize {
            f |= (self.kinds[i] as u32) << (i * 4);
        }
        f
    }

    /// Then comparison becomes a single integer check:
    /// if pdu.header_stack.fingerprint() == TCP_OVER_IPV4 { ... }
    pub const TCP_OVER_IPV4: u32 = (HeaderKind::Mac as u32) |
        ((HeaderKind::Ip as u32) << 4) |
        ((HeaderKind::Tcp as u32) << 8);


    #[inline]
    pub fn count(&self) -> usize {
        self.hc as usize
    }

    /// Note: To keep the API returning `&Header`, the Header objects
    /// would need to be stored. However, if Header is a "View" type
    /// (pointer wrapper), returning it by value is more efficient.
    /// If you MUST return a reference, see the 'Storage' note below.
    /// The specific typed accessors (tcp_mut, etc.) use raw pointer arithmetic
    /// based on the offsets stored during parsing.
    #[inline]
    pub fn get(&self, base_addr: usize, which: usize) -> Header {
        self.reconstruct(base_addr, which)
    }

    #[inline]
    pub fn tcp_mut(&mut self, base_addr: usize, which: usize) -> &mut TcpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut TcpHeader) }
    }

    #[inline]
    pub fn ip_mut(&mut self, base_addr: usize, which: usize) -> &mut IpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut IpHeader) }
    }

    #[inline]
    pub fn mac_mut(&mut self, base_addr: usize, which: usize) -> &mut MacHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut MacHeader) }
    }

    #[inline]
    pub fn arp_mut(&mut self, base_addr: usize, which: usize) -> &mut ArpIpv4Header {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut ArpIpv4Header) }
    }

    #[inline]
    pub fn ipv6_mut(&mut self, base_addr: usize, which: usize) -> &mut Ipv6Header {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut Ipv6Header) }
    }

    #[inline]
    pub fn udp_mut(&mut self, base_addr: usize, which: usize) -> &mut UdpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut UdpHeader) }
    }

    #[inline]
    pub fn icmp_mut(&mut self, base_addr: usize, which: usize) -> &mut IcmpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &mut *(addr as *mut IcmpHeader) }
    }

    #[inline]
    pub fn tcp(&self, base_addr: usize, which: usize) -> &TcpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const TcpHeader) }
    }

    #[inline]
    pub fn ip(&self, base_addr: usize, which: usize) -> &IpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const IpHeader) }
    }

    #[inline]
    pub fn mac(&self, base_addr: usize, which: usize) -> &MacHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const MacHeader) }
    }

    #[inline]
    pub fn arp(&self, base_addr: usize, which: usize) -> &ArpIpv4Header {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const ArpIpv4Header) }
    }

    #[inline]
    pub fn ipv6(&self, base_addr: usize, which: usize) -> &Ipv6Header {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const Ipv6Header) }
    }

    #[inline]
    pub fn udp(&self, base_addr: usize, which: usize) -> &UdpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const UdpHeader) }
    }

    #[inline]
    pub fn icmp(&self, base_addr: usize, which: usize) -> &IcmpHeader {
        debug_assert!(which < self.hc as usize, "Attempted to access uninitialized header!");
        let addr = base_addr + self.offsets[which] as usize;
        unsafe { &*(addr as *const IcmpHeader) }
    }
}

impl fmt::Display for HeaderStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.hc == 0 {
            write!(f, "<no headers>")
        } else {
            for i in 0..self.hc as usize {
                writeln!(f, "{:1}: {:?} at offset {}", i, self.kinds[i], self.offsets[i])?;
            }
            Ok(())
        }
    }
}
