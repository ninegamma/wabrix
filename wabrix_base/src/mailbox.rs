use core::mem::size_of;
use wabrix_api::BatchMetadata;

pub const MAILBOX_STRIDE: usize = size_of::<BatchMetadata>();
