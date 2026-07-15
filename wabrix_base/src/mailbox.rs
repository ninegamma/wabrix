use core::mem::size_of;
use wabrix_api::BatchMetadata;

pub const MAILBOX_STRIDE: usize = size_of::<BatchMetadata>();

/// Number of UMEM frames needed to hold the Control Block located at the end of the
/// reserved region, covering all `n_instances` per-instance mailboxes.
#[inline(always)]
pub fn control_block_frames(n_instances: u32, frame_size: u32) -> u32 {
    let total_bytes = n_instances as u64 * MAILBOX_STRIDE as u64;
    total_bytes.div_ceil(frame_size as u64) as u32
}
