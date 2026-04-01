use core::fmt;
use crate::headers::{ArpIpv4Header, Header, HeaderKind, IcmpHeader, IpHeader, Ipv6Header, MacHeader, TcpHeader, UdpHeader};

const MAX_HEADERS: usize = 5;

#[derive(Clone, Debug)]
#[repr(C)]
pub struct HeaderStack {
    /// The virtual address of the start of the UMEM frame
    /// we cannot use *mut u8, because we also want to use it in target wasm32-*
    base_ptr: u64,
    /// Offsets relative to base_ptr
    pub offsets: [u16; MAX_HEADERS],
    /// The type of header (Mac, Ip, etc.)
    pub kinds: [HeaderKind; MAX_HEADERS],
    /// Current count
    pub hc: u8,
}

impl HeaderStack {
    #[inline]
    pub fn new(base_ptr: *mut u8) -> Self {
        HeaderStack {
            base_ptr: (base_ptr as u64),
            offsets: [0; MAX_HEADERS],
            kinds: [HeaderKind::Null; MAX_HEADERS],
            hc: 0,
        }
    }

    #[inline(always)]
    pub fn base_ptr(&self) -> *mut u8 {
        self.base_ptr as * mut u8
    }

    #[inline]
    pub fn clear(&mut self) {
        self.hc = 0;
    }

    #[inline]
    pub fn push(&mut self, h: Header) {
        let idx = self.hc as usize;
        if idx >= MAX_HEADERS {
            panic!("HeaderStack overflow: cannot push more than {} headers", MAX_HEADERS);
        }
        self.kinds[idx] = h.kind();
        // Calculate offset: Header pointer minus our base pointer
        let h_ptr = h.as_ptr_u8().unwrap();
        self.offsets[idx] = unsafe { h_ptr.offset_from(self.base_ptr()) as u16 };
        self.hc += 1;
    }

    /// Internal helper to reconstruct the Header enum on the fly
    #[inline]
    fn reconstruct(&self, which: usize) -> Header {
        let ptr = unsafe { self.base_ptr().add(self.offsets[which] as usize) };
        // This recreates the Header enum (which is just a tag + pointer)
        Header::from_kind_and_ptr(self.kinds[which], ptr)
    }

    // --- API Compatibility Layer ---

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
    pub fn get(&self, which: usize) -> Header {
        self.reconstruct(which)
    }

    #[inline]
    pub fn tcp_mut(&mut self, which: usize) -> &mut TcpHeader {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut TcpHeader) }
    }

    #[inline]
    pub fn ip_mut(&mut self, which: usize) -> &mut IpHeader {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut IpHeader) }
    }

    #[inline]
    pub fn mac_mut(&mut self, which: usize) -> &mut MacHeader {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut MacHeader) }
    }

    #[inline]
    pub fn arp_mut(&mut self, which: usize) -> &mut ArpIpv4Header {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut ArpIpv4Header) }
    }

    #[inline]
    pub fn ipv6_mut(&mut self, which: usize) -> &mut Ipv6Header {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut Ipv6Header) }
    }

    #[inline]
    pub fn udp_mut(&mut self, which: usize) -> &mut UdpHeader {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut UdpHeader) }
    }

    #[inline]
    pub fn icmp_mut(&mut self, which: usize) -> &mut IcmpHeader {
        unsafe { &mut *(self.base_ptr().add(self.offsets[which] as usize) as *mut IcmpHeader) }
    }

    #[inline]
    pub fn tcp(&self, which: usize) -> &TcpHeader {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const TcpHeader) }
    }

    #[inline]
    pub fn ip(&self, which: usize) -> &IpHeader {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const IpHeader) }
    }

    #[inline]
    pub fn mac(&self, which: usize) -> &MacHeader {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const MacHeader) }
    }

    #[inline]
    pub fn arp(&self, which: usize) -> &ArpIpv4Header {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const ArpIpv4Header) }
    }

    #[inline]
    pub fn ipv6(&self, which: usize) -> &Ipv6Header {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const Ipv6Header) }
    }

    #[inline]
    pub fn udp(&self, which: usize) -> &UdpHeader {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const UdpHeader) }
    }

    #[inline]
    pub fn icmp(&self, which: usize) -> &IcmpHeader {
        unsafe { &*(self.base_ptr().add(self.offsets[which] as usize) as *const IcmpHeader) }
    }
}

impl fmt::Display for HeaderStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.hc == 0 {
            write!(f, "<no headers>")
        } else {
            for i in 0..self.hc as usize {
                // We reconstruct the HeaderPtr (Header) value here.
                // It's a lightweight view, so this is very efficient.
                let h = self.get(i);
                writeln!(f, "{:1}: {}", i, h)?;
            }
            Ok(())
        }
    }
}
