use super::XdpSocketConfig;
use crate::{Batch, BatchTx};
use core::mem::MaybeUninit;
use rustix::event::{poll, PollFd, PollFlags};
use rustix::io::Errno;
use spin::Mutex;
use std::os::fd::{BorrowedFd, RawFd};
use std::sync::Arc;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::frame_registry::{get_worker_frame_registry, FrameToken};
use wabrix_base::interface::Pdu;
use wabrix_base::SystemBus;
use xsk_core::{CompQueue, FillQueue, FrameDesc, TxQueue, Umem};

const LOG_INTERVAL_C_F_QUEUE_PACKETS: usize = 100;

pub struct XSocketTx {
    /// Worker that owns this TX half. This may differ from the RX owner.
    pub(super) worker_id: usize,
    pub(super) umem: Arc<Umem>,
    pub(super) comp_descs_helper: Vec<u64>,
    pub(super) comp_queue: Arc<Mutex<CompQueue>>,
    pub(super) tx_queue: Arc<Mutex<TxQueue>>,
    pub(super) _fill_queue_keepalive: Arc<Mutex<FillQueue>>,
    pub(super) bus: Arc<SystemBus>,
    pub(super) config: Arc<XdpSocketConfig>,
    pub(super) total_consumed: usize,
    pub(super) last_logged_block: usize,  // helper for logging
    pub(super) total_sent_packets: usize, // helper for logging
    pub(super) fd: RawFd,                 // file descriptor of the AF_XDP socket
}

impl BatchTx for XSocketTx {
    fn transmit_batch(&mut self, batch: &mut Batch) -> anyhow::Result<usize> {
        // Reclaim completions even on an empty batch so idle TX frees frames.
        self.drain_completion_queue();

        let original_batch_len = batch.len();

        if original_batch_len == 0 {
            return Ok(0);
        }
        let mut tx_guard = self.tx_queue.lock();
        // Cached capacity is conservative. Avoid reading the kernel-owned
        // consumer cache line while the cached slots can fit this burst.
        let mut free_slots = tx_guard.nb_free(original_batch_len as u32) as usize;

        if free_slots == 0 {
            if self.config.poll_mode == "napi_busy_poll" {
                // Do not hold the TX ring lock while the kernel may busy-poll.
                // NAPI can complete TX work during this call; userspace still
                // has to consume those completions below.
                drop(tx_guard);
                self.poll_for_napi_tx()?;
                self.drain_completion_queue();
                tx_guard = self.tx_queue.lock();
                free_slots = tx_guard.nb_free(original_batch_len as u32) as usize;
            } else {
                // Force progress on a full ring through xsk-core, then recheck:
                // the wakeup can consume descriptors synchronously in SKB mode.
                tx_guard.wakeup()?;
                free_slots = tx_guard.nb_free(original_batch_len as u32) as usize;
            }

            if free_slots == 0 {
                // The ring is still full after the wakeup/poll attempt. Leave
                // the batch intact so the caller can retry it later.
                drop(tx_guard);
                // Saturation is normal backpressure; avoid synchronous warning
                // logging on every burst while the NIC is catching up.
                log::trace!(
                    "TX saturated: worker {} queue {}, batch size {}",
                    self.worker_id,
                    self.config.queue_id,
                    original_batch_len
                );
                return Ok(0);
            }
        }

        let count_to_send = std::cmp::min(original_batch_len, free_slots);
        let mut valid_to_send = 0;
        // Only initialize descriptors that will be submitted, avoiding a
        // MAX_BATCH_SIZE-sized memset for small bursts.
        let mut descs = [MaybeUninit::<FrameDesc>::uninit(); MAX_BATCH_SIZE];

        let my_umem_addr = self.umem.as_ptr() as usize;
        let my_umem_id = self.config.umem_config.group_id;
        let registry_ptr = get_worker_frame_registry();

        // Slice iteration avoids Option handling and per-PDU indexing checks.
        // Group IDs identify the UMEM directly, saving a SystemBus pointer chase
        // on the common path where packets already belong to this TX UMEM.
        for pdu in batch.iter_mut().take(count_to_send) {
            if pdu.umem_group_id() as usize != my_umem_id {
                if registry_ptr.is_null() {
                    break;
                }
                let headroom = pdu.w.headroom();
                let len = pdu.w.data_len();
                // UMEM groups may use different frame sizes. Preserve the
                // unsent suffix if this packet cannot fit the destination.
                if headroom > self.config.umem_config.frame_size as usize
                    || len as usize > self.config.umem_config.frame_size as usize - headroom
                {
                    log::warn!("TX packet does not fit destination UMEM {}", my_umem_id);
                    break;
                }

                // Allocate just a frame token: allocate_pdus_into creates a
                // MAX_BATCH_SIZE temporary even for a single foreign packet.
                // Constructing its PDU here avoids that per-packet stack work.
                let mut tokens = [FrameToken::new(0, 0)];
                // SAFETY: TLS points to this worker's live registry. End the
                // mutable borrow before dropping the old PDU, whose destructor
                // can access the same registry to release its original frame.
                let (allocated, owner) = unsafe {
                    let registry = &mut *registry_ptr;
                    (
                        registry.allocate_batch_into(my_umem_id, &mut tokens),
                        registry.worker_id,
                    )
                };
                if allocated == 0 {
                    break;
                }
                let frame_size_log2 = self.config.umem_config.frame_size_log2;
                let frame_addr =
                    (u64::from(tokens[0].frame_index()) << frame_size_log2) + headroom as u64;
                let new_pdu = Pdu::new(
                    FrameDesc::new(frame_addr).with_length(len),
                    tokens[0].umem_id(),
                    owner as u8,
                    Arc::as_ptr(&self.bus),
                    frame_size_log2,
                );

                // SAFETY: The source PDU owns a live packet in its mapped UMEM;
                // the destination is a newly allocated frame in a different
                // UMEM. The fit check bounds the destination write, and the
                // distinct mappings guarantee non-overlapping byte ranges.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        pdu.w.data_ptr(pdu.umem_addr()),
                        new_pdu.w.data_ptr(my_umem_addr),
                        len as usize,
                    );
                }
                // Normal PDU drop returns the foreign frame to its owner.
                *pdu = new_pdu;
            }

            descs[valid_to_send].write(pdu.to_frame_desc());
            valid_to_send += 1;
        }

        if valid_to_send == 0 {
            return Ok(0);
        }

        // SAFETY: Only the initialized prefix is exposed as FrameDesc values.
        let descs = unsafe {
            std::slice::from_raw_parts(descs.as_ptr().cast::<FrameDesc>(), valid_to_send)
        };
        // SAFETY: Every descriptor belongs to this queue's UMEM, and the batch
        // owns these frames until submission. Only the submitted prefix is
        // forgotten below; the NIC owns those frames until CQ completion.
        let sent = unsafe { tx_guard.produce(descs) };

        // XDP_USE_NEED_WAKEUP is enabled at socket setup. Honor the ring flag
        // once per burst, avoiding an extra sendto syscall for tiny batches.
        let wakeup_result = if tx_guard.needs_wakeup() {
            tx_guard.wakeup()
        } else {
            Ok(())
        };
        drop(tx_guard);
        self.total_sent_packets += sent;

        // Submission and wakeup are separate: a wakeup error must never cause
        // caller cleanup to return frames that have already entered the ring.
        finish_tx_submission(batch, sent, wakeup_result)
    }

    fn sink_id(&self) -> String {
        self.config.interface.clone()
    }
}

/// Commit NIC ownership before propagating a fallible kernel wakeup.
fn finish_tx_submission(
    batch: &mut Batch,
    sent: usize,
    wakeup_result: std::io::Result<()>,
) -> anyhow::Result<usize> {
    if sent == batch.len() {
        // The usual full-burst path just clears the logical length, avoiding
        // moving each 64-byte PDU through a drain iterator to forget it.
        batch.set_count(0);
    } else if sent > 0 {
        // Preserve packet order and ownership of the unsent suffix.
        for pdu in batch.drain(..sent) {
            std::mem::forget(pdu);
        }
    }
    wakeup_result?;
    Ok(sent)
}

impl XSocketTx {
    /// Rebind this half to the worker that takes ownership of it.
    pub(crate) fn assign_worker_id(&mut self, worker_id: usize) {
        self.worker_id = worker_id;
    }

    /// Give the kernel an opportunity to run NAPI and complete TX descriptors.
    ///
    /// The completion queue is consumed by userspace, so `POLLOUT` readiness
    /// alone does not reclaim descriptors. Callers must drain the completion
    /// queue and recheck the TX ring after this returns.
    fn poll_for_napi_tx(&self) -> Result<(), Errno> {
        // SAFETY: self.fd is a valid file descriptor for the lifetime of this call.
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(self.fd) };
        let mut poll_fds = [PollFd::new(&borrowed_fd, PollFlags::OUT)];
        let timeout = rustix::time::Timespec {
            // TOML validation guarantees the timeout is less than one second.
            tv_sec: 0,
            tv_nsec: self.config.napi_poll_timeout_ns as _,
        };

        poll(&mut poll_fds, Some(&timeout))?;
        Ok(())
    }

    /// Consumes frames that the NIC has finished transmitting.
    fn drain_completion_queue(&mut self) -> usize {
        let consumed = {
            let mut cq = self.comp_queue.lock();
            // SAFETY: The guard serializes access to the shared CQ and the
            // helper buffer is writable. Addresses are resolved in this CQ's
            // UMEM below before frames are returned to their owning workers.
            unsafe { cq.consume(&mut self.comp_descs_helper) }
        };

        // Empty completions are common; return before registry access or
        // diagnostics. The CQ lock has already been released.
        if consumed == 0 {
            return 0;
        }
        self.total_consumed += consumed;

        // Skip diagnostic division and bookkeeping when tracing is disabled.
        if log::log_enabled!(log::Level::Trace) {
            let current_block = self.total_consumed / LOG_INTERVAL_C_F_QUEUE_PACKETS;
            if current_block > self.last_logged_block {
                log::trace!(
                    "Worker {}, I/f {}, Queue {}: Drained {} frames from completion queue",
                    self.worker_id,
                    self.config.interface,
                    self.config.queue_id,
                    self.total_consumed
                );
                self.last_logged_block = current_block;
            }
        }

        let frame_size_log2 = self.config.umem_config.frame_size_log2;
        let umem_id = self.config.umem_config.group_id;
        // Fetch TLS once per burst instead of once per completion. No PDU is
        // dropped while this borrow is live, so release cannot reborrow TLS.
        // SAFETY: The TLS pointer is null or points to this worker's live,
        // exclusively accessed registry. as_mut handles the null fallback.
        let mut registry = unsafe { get_worker_frame_registry().as_mut() };

        for &addr in &self.comp_descs_helper[..consumed] {
            // Shift the 64-bit byte offset before narrowing; UMEM mappings can
            // exceed 4 GiB even though their frame indices still fit in u32.
            let local_idx = (addr >> frame_size_log2) as u32;
            let Some(frame) = self.bus.resolve_local_frame_idx(umem_id, local_idx) else {
                log::warn!(
                    "drain_completion_queue: no frame token for UMEM {}, local frame {}",
                    umem_id,
                    local_idx
                );
                continue;
            };
            if let Some(registry) = registry.as_mut() {
                if frame.worker_id == registry.worker_id {
                    registry.release_local(frame.frame_token);
                    continue;
                }
            }
            // Shared completion rings may contain another worker's frames;
            // always route those through the owning worker's SystemBus slot.
            if self.bus.bus_senders[frame.worker_id]
                .push(frame.frame_token)
                .is_err()
            {
                log::error!(
                    "CRITICAL: return queue full, frame token {} leaked!",
                    frame.frame_token
                );
            }
        }
        consumed
    }
}
impl Drop for XSocketTx {
    fn drop(&mut self) {
        log::debug!(
            "RUST DROP: XSocketTx (Queue {}) dropping. Refs remaining -> FQ: {}, CQ: {}, TX: {}",
            self.config.queue_id,
            Arc::strong_count(&self._fill_queue_keepalive),
            Arc::strong_count(&self.comp_queue),
            Arc::strong_count(&self.tx_queue)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_queue::ArrayQueue;
    use wabrix_base::frame_registry::MAX_UMEM;

    fn with_test_batch(test: impl FnOnce(&SystemBus, &mut Batch)) {
        // These PDUs have real backing storage and a live bus, but tests only
        // exercise ownership transfer; they never submit to a hardware ring.
        let memory = [0u8; 4 * 2048];
        let mut umem_addr = [None; MAX_UMEM];
        umem_addr[0] = Some(memory.as_ptr() as usize);
        let bus = SystemBus {
            bus_senders: vec![Arc::new(ArrayQueue::new(8))],
            partition_directory: Vec::new(),
            frame_partition_maps: Vec::new(),
            umem_addr,
        };
        let mut batch = Batch::new();
        for i in 0..4 {
            batch
                .push(Pdu::new(FrameDesc::new(i * 2048), 0, 0, &bus, 11))
                .expect("test batch has capacity");
        }
        test(&bus, &mut batch);
    }

    #[test]
    fn full_submission_keeps_nic_owned_frames_out_of_return_queue() {
        with_test_batch(|bus, batch| {
            assert_eq!(finish_tx_submission(batch, 4, Ok(())).unwrap(), 4);
            assert!(batch.is_empty());
            batch.bulk_drop();
            assert!(bus.bus_senders[0].is_empty());
        });
    }

    #[test]
    fn partial_submission_preserves_unsent_order_and_ownership() {
        with_test_batch(|bus, batch| {
            assert_eq!(finish_tx_submission(batch, 2, Ok(())).unwrap(), 2);
            assert_eq!(batch.len(), 2);
            assert_eq!(batch[0].frame_index(), 2);
            assert_eq!(batch[1].frame_index(), 3);
            assert!(bus.bus_senders[0].is_empty());

            batch.bulk_drop();
            assert_eq!(bus.bus_senders[0].pop(), Some(FrameToken::new(0, 2)));
            assert_eq!(bus.bus_senders[0].pop(), Some(FrameToken::new(0, 3)));
            assert!(bus.bus_senders[0].is_empty());
        });
    }

    #[test]
    fn wakeup_error_after_partial_submission_only_returns_unsent_frames() {
        with_test_batch(|bus, batch| {
            let result =
                finish_tx_submission(batch, 2, Err(std::io::Error::other("wakeup failed")));
            assert!(result.is_err());
            assert_eq!(batch.len(), 2);

            // Match BatchTxNode's error cleanup: submitted frames must remain
            // owned by the NIC even when the wakeup failed after submission.
            batch.clear();
            assert_eq!(bus.bus_senders[0].pop(), Some(FrameToken::new(0, 2)));
            assert_eq!(bus.bus_senders[0].pop(), Some(FrameToken::new(0, 3)));
            assert!(bus.bus_senders[0].is_empty());
        });
    }

    #[test]
    fn wakeup_error_after_full_submission_leaves_no_frames_to_recycle() {
        with_test_batch(|bus, batch| {
            assert!(
                finish_tx_submission(batch, 4, Err(std::io::Error::other("wakeup failed")))
                    .is_err()
            );
            assert!(batch.is_empty());
            batch.clear();
            assert!(bus.bus_senders[0].is_empty());
        });
    }

    #[test]
    fn zero_submission_preserves_all_frames_for_cleanup() {
        with_test_batch(|bus, batch| {
            assert_eq!(finish_tx_submission(batch, 0, Ok(())).unwrap(), 0);
            assert_eq!(batch.len(), 4);
            batch.bulk_drop();
            for i in 0..4 {
                assert_eq!(bus.bus_senders[0].pop(), Some(FrameToken::new(0, i)));
            }
            assert!(bus.bus_senders[0].is_empty());
        });
    }
}
