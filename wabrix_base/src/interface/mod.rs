pub mod batch;
pub mod pciaddress;
pub mod pdu;

pub use self::pdu::*;
use xsk_core::FrameDesc;

/// Generic trait for objects that can receive packets.
pub trait PacketRx {
    fn recv(&mut self, pkts: &mut [FrameDesc]) -> anyhow::Result<(u32, i32)>; // (packets received, queue length (if >=0))
                                                                              //fn rx_stats(&self) -> Arc<CacheAligned<PortStats>>;
}

/// Generic trait for objects that can send packets.
pub trait PacketTx {
    fn send(&mut self, pkts: &mut [FrameDesc]) -> anyhow::Result<u32>;
    //fn tx_stats(&self) -> Arc<CacheAligned<PortStats>>;
}

/*
pub trait FastPacketRx {
    // Returns a batch of slices pointing directly into Umem memory
    fn recv_batch<F>(&self, batch_size: usize, f: F) -> Result<u32, Error>
    where
        F: FnMut(&[u8]);
}
*/

pub trait PacketRxTx: PacketRx + PacketTx {}
