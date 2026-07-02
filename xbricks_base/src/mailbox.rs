use core::mem::size_of;
use xbricks_protocol::MAX_BATCH_SIZE;

#[repr(C)]
pub struct BatchMetadata {
    /// Written by host before calling process-batch: AF_XDP frame descriptor offsets
    pub desc_addrs:  [u32; MAX_BATCH_SIZE],
    /// Written by host before calling process-batch: original packet lengths
    pub lengths:     [u32; MAX_BATCH_SIZE],
    /// Written by plugin during process-batch: routing decisions (route_id / DROP)
    pub actions:     [u32; MAX_BATCH_SIZE],
    /// Written by plugin during process-batch: modified packet lengths (0 = unchanged)
    pub new_lengths: [u32; MAX_BATCH_SIZE],
}

pub const MAILBOX_STRIDE: usize = size_of::<BatchMetadata>();

/// Number of UMEM frames needed to hold the Control Block for `n_instances` plugin instances.
#[inline(always)]
pub fn control_block_frames(n_instances: u32, frame_size: u32) -> u32 {
    let total_bytes = n_instances as u64 * MAILBOX_STRIDE as u64;
    total_bytes.div_ceil(frame_size as u64) as u32
}
