use log::trace;
use crate::headers::{EndOffset, TcpHeader};
use crate::headers::check::ipv4_checksum;
use crate::wpdu::WPdu;


#[inline]
pub fn update_tcp_checksum(tcp_header: &mut TcpHeader, ip_payload_size: usize, ip_src: u32, ip_dst: u32) {
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



#[inline]
pub fn prepare_checksums_and_ttl(p: &mut WPdu, base_addr: usize) {
    let stack = p.headers_mut();
    let psz;
    let src;
    let dst;
    {
        let ip = stack.ip_mut(base_addr, 1);
        let ttl = ip.ttl();
        if ttl >= 1 {
            ip.set_ttl(ttl - 1);
        }
        ip.update_checksum();
        psz = ip.payload_size(0);
        src = ip.src();
        dst = ip.dst();
    }
    update_tcp_checksum(stack.tcp_mut(base_addr, 2), psz, src, dst);
    trace!("ip-payload_sz= {}, checksum recalc = {:X}", psz, stack.tcp_mut(base_addr, 2).checksum());
}