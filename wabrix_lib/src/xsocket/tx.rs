use super::XdpSocketConfig;
use crate::{Batch, BatchTx};
use core::mem::MaybeUninit;
use log::trace;
use rustix::event::{poll, PollFd, PollFlags};
use rustix::io::Errno;
use spin::Mutex;
use std::os::fd::{BorrowedFd, RawFd};
use std::sync::Arc;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::frame_registry::{get_worker_frame_registry, FrameToken};
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
    #[allow(dead_code)]
    pub(super) last_logged_block: usize, // helper for logging
    pub(super) total_sent_packets: usize, // helper for logging
    pub(super) fd: RawFd,                 // file descriptor of the AF_XDP socket
}

// Implement BatchTx for XSocketTx
impl BatchTx for XSocketTx {
    fn transmit_batch(&mut self, batch: &mut Batch) -> anyhow::Result<usize> {
        self.drain_completion_queue()?;

        let original_batch_len = batch.len();

        if original_batch_len == 0 {
            return Ok(0);
        }
        let mut tx_guard = self.tx_queue.lock();
        //let free_slots = tx_guard.nb_free(original_batch_len as u32) as usize;
        let mut free_slots = tx_guard.nb_free_exact() as usize;

        if free_slots == 0 {
            if self.config.poll_mode == "napi_busy_poll" {
                // Do not hold the TX ring lock while the kernel may busy-poll.
                // NAPI can complete TX work during this call; userspace still
                // has to consume those completions below.
                drop(tx_guard);
                self.poll_for_napi_tx()?;
                self.drain_completion_queue()?;
                tx_guard = self.tx_queue.lock();
                free_slots = tx_guard.nb_free_exact() as usize;
            } else {
                // The ring is completely jammed. Force a kernel wakeup to try
                // and clear it when NAPI busy polling is not enabled.
                unsafe {
                    libc::sendto(
                        self.fd,
                        core::ptr::null(),
                        0,
                        libc::MSG_DONTWAIT,
                        core::ptr::null_mut(),
                        0,
                    );
                }
            }

            if free_slots == 0 {
                // The ring is still full after the wakeup/poll attempt. Leave
                // the batch intact so the caller can retry it later.
                drop(tx_guard);
                log::warn!(
                    "🔥 [TX SATURATED] Worker {} Queue {}: TX Ring is 100% full! (Batch size: {}). Wasm engine is outrunning the hardware.",
                    self.worker_id, self.config.queue_id, original_batch_len
                );
                return Ok(0);
            }
        } else if free_slots < original_batch_len {
            log::info!(
                "⚠️ [TX BACKPRESSURE] Worker {} Queue {}: Batch of {} packets, but only {} TX slots free.",
                self.worker_id,
                self.config.queue_id,
                original_batch_len,
                free_slots
            );
        }

        let count_to_send = std::cmp::min(original_batch_len, free_slots);
        let mut valid_to_send = 0;
        let mut descs = [FrameDesc::default(); MAX_BATCH_SIZE];

        let my_umem_addr = self.umem.as_ptr() as usize;
        let my_umem_id = self.config.umem_config.group_id;
        let registry_ptr = get_worker_frame_registry();

        // ==========================================================
        //  IN-PLACE CROSS-UMEM SWAP
        // ==========================================================
        if !registry_ptr.is_null() {
            let frame_registry = unsafe { &mut *registry_ptr };

            for i in 0..count_to_send {
                let pdu_ref = batch.get_mut(i);
                let foreign_umem_addr = pdu_ref.umem_addr();
                if foreign_umem_addr != my_umem_addr {
                    // It's foreign memory, we must copy it
                    let mut scratch = [MaybeUninit::uninit()];

                    if frame_registry.allocate_pdus_into(my_umem_id, &mut scratch) == 1 {
                        // 2. Read the fully initialized PDU directly out of the memory slot.
                        // No std::mem::replace needed, and absolutely nothing gets dropped!
                        let mut new_pdu = unsafe { scratch[0].assume_init_read() };
                        new_pdu.reset_for_tx(pdu_ref.w.headroom());

                        let len = pdu_ref.w.data_len();
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                pdu_ref.w.data_ptr(foreign_umem_addr),
                                new_pdu.w.data_ptr(my_umem_addr),
                                len as usize,
                            );
                        }
                        new_pdu.w.set_data_length(len);

                        descs[valid_to_send] = new_pdu.to_frame_desc();
                        // MAGIC SWAP: Put the UMEM 1 packet into the batch,
                        // and take ownership of the old UMEM 0 packet!
                        let old_pdu = std::mem::replace(pdu_ref, new_pdu);

                        // Drop the old packet immediately (returns it to UMEM 0)
                        drop(old_pdu);

                        valid_to_send += 1;
                    } else {
                        // UMEM 1 is exhausted. Stop processing this batch.
                        // The remaining unprocessed packets stay in the batch for bulk_drop!
                        // 🚨 PROBE 3: UMEM STARVATION 🚨
                        log::error!(
                            "🩸 [UMEM STARVATION] Worker {}: Failed to allocate frame in UMEM {} for cross-pool swap! Free frames: {}",
                            self.worker_id, my_umem_id, frame_registry.total_free_frames()
                        );
                        break;
                    }
                } else {
                    // Fast path: already local UMEM
                    descs[valid_to_send] = pdu_ref.to_frame_desc();
                    valid_to_send += 1;
                }
            }
        } else {
            for i in 0..count_to_send {
                let pdu_ref = batch.get(i);
                if pdu_ref.umem_addr() == my_umem_addr {
                    descs[valid_to_send] = pdu_ref.to_frame_desc();
                    valid_to_send += 1;
                } else {
                    // We cannot safely transmit this, and we cannot copy it.
                    // Stop building descriptors here.
                    break;
                }
            }
        }

        // ==========================================================
        //  TRANSMIT & CLEANUP
        // ==========================================================
        let mut sent = 0;
        if valid_to_send > 0 {
            // This is where the syscall happens, but now we guarantee it
            // only fires if we have a worthwhile batch of packets!
            // Lock the TX queue just for the burst
            sent = unsafe { tx_guard.produce_and_wakeup(&descs[0..valid_to_send])? };

            // THE LOW-TRAFFIC KICKER:
            // If we are trickling packets in tiny batches (< 16), the driver might
            // silently go to sleep. We force a syscall to guarantee it transmits.
            if valid_to_send < 16 {
                unsafe {
                    libc::sendto(
                        self.fd,
                        core::ptr::null(),
                        0,
                        libc::MSG_DONTWAIT,
                        core::ptr::null_mut(),
                        0,
                    );
                }
            }

            if sent > 0 {
                self.total_sent_packets += sent;
            }
        }

        // Remove ONLY the successfully transmitted packets from the batch.
        for pdu in batch.drain(0..sent) {
            std::mem::forget(pdu); // The NIC owns them now!
        }

        // Any packets left over (because free_slots < batch.len(), or allocation failed)
        // are perfectly preserved in the batch.
        // When this function returns, `BatchTxNode::process` will call `batch.bulk_drop()`
        // and recycle all of them in 5 cycles!

        Ok(sent)
    }

    fn sink_id(&self) -> String {
        self.config.interface.clone()
    }
}

// Add helper methods for XSocketTx
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

    /*
        /// Explicitly wakes up the kernel TX driver if it went to sleep
        #[inline]
        pub fn wakeup_tx_queue(&self) -> anyhow::Result<()> {
            // Note: This requires access to the TX queue status
            // In libxdp/xsk-core, checking needs_wakeup usually happens on the ring struct.
            // If the flag is set:
            unsafe {
                if self.tx_queue.needs_wakeup() {
                    //trace!("Worker {}: TX Queue needs wakeup! Triggering syscall...", self.worker_id);
                    // The sendto call with MSG_DONTWAIT and 0 len is the standard wakeup trigger
                    let ret = libc::sendto(self.fd, std::ptr::null(), 0, libc::MSG_DONTWAIT, std::ptr::null_mut(), 0);
                    if ret < 0 {
                        return Err(std::io::Error::last_os_error().into());
                    }
                }
            }
            Ok(())
        }
    */
    /// Consumes frames that the NIC has finished transmitting.
    fn drain_completion_queue(&mut self) -> anyhow::Result<usize> {
        let threshold = LOG_INTERVAL_C_F_QUEUE_PACKETS;

        let consumed = {
            let mut cq = self.comp_queue.lock();
            let consumed = unsafe { cq.consume(&mut self.comp_descs_helper) };
            drop(cq); // Free the lock immediately!
            consumed
        };

        if consumed > 0 {
            self.total_consumed += consumed;

            let current_block = self.total_consumed / threshold;
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

            let frame_size_log2 = self.config.umem_config.frame_size_log2;

            // FIX: Iterate by index instead of .iter()
            for i in 0..consumed {
                let addr = self.comp_descs_helper[i] as u32;

                // 1. Calculate the local index relative to this specific UMEM
                let local_idx = addr >> frame_size_log2;

                // 2. Recover the frame token from the UMEM-local completion address.
                let Some(resolved_frame) = self
                    .bus
                    .resolve_local_frame_idx(self.config.umem_config.group_id, local_idx)
                else {
                    log::warn!(
                        "drain_completion_queue: no frame token for UMEM {}, local frame {}",
                        self.config.umem_config.group_id,
                        local_idx
                    );
                    continue;
                };
                trace!(
                    "drain_completion_queue: Worker {}: Releasing frame_token={} (local_idx={}, umem_group_id={})",
                    self.worker_id,
                    resolved_frame.frame_token,
                    local_idx,
                    self.config.umem_config.group_id
                );
                self.release_frame(resolved_frame.frame_token, resolved_frame.worker_id);
            }
        } else {
            if self.total_sent_packets > self.total_consumed {
                let pending_in_hardware = self.total_sent_packets - self.total_consumed;

                // Only warn if a massive number of packets are stuck in limbo
                if pending_in_hardware > 10000 {
                    log::warn!(
                        "👻 [CQ GHOST TOWN] Worker {} Queue {}: {} packets sent to TX, but NIC has not completed them! Hardware stall imminent.",
                        self.worker_id, self.config.queue_id, pending_in_hardware
                    );
                }
            }
        }
        Ok(consumed)
    }

    fn release_frame(&mut self, frame_token: FrameToken, owner: usize) {
        /*
        log::trace!(
            "Worker {}: Releasing frame_token={}, determined owner={}, my worker_id={}",
            self.worker_id,
            frame_token,
            owner,
            self.worker_id
        );
        */
        // FAST PATH: If the frame belongs to this worker, bypass the MPSC channel entirely!
        if owner == self.worker_id {
            unsafe {
                let registry_ptr = wabrix_base::frame_registry::get_worker_frame_registry();
                if !registry_ptr.is_null() {
                    trace!(
                        "Worker {}: Released frame token {} to local registry (fast path)",
                        self.worker_id,
                        frame_token
                    );
                    (*registry_ptr).release_local(frame_token);
                    return;
                } else {
                    log::warn!("Worker {}: Frame registry ptr is null!", self.worker_id);
                }
            }
        }

        // SLOW PATH: Only use the cross-thread bus for frames belonging to OTHER workers
        log::trace!(
            "Worker {}: Sending frame token {} to owner {} via bus (slow path)",
            self.worker_id,
            frame_token,
            owner
        );
        if self.bus.bus_senders[owner].push(frame_token).is_err() {
            log::error!(
                "🔥 CRITICAL: return queue full, frame token {} leaked!",
                frame_token
            );
        }
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
