// src/xsocket.rs

use crate::{probe_kernel_fds, Batch, BatchRx, BatchTx};
use core::mem::MaybeUninit;
use log::{debug, info, trace, warn};
use spin::Mutex;
use std::ffi::CString;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::sync::Arc;
use wabrix_base::common::formatting::PreciseSeparator;
use wabrix_base::interface::Pdu;
use xsk_rs::config::SocketConfig;
use xsk_rs::config::{BindFlags, Interface, LibxdpFlags, QueueSize, XdpFlags};
use xsk_rs::socket::XdpStatistics;
use xsk_rs::umem::frame::Data;
use xsk_rs::{CompQueue, FillQueue, FrameDesc, RxQueue, Socket, TxQueue, Umem};

const LOG_INTERVAL_C_F_QUEUE_PACKETS: usize = 100;

/// Configuration specific to a single socket/queue.
#[derive(Debug, Clone)]
pub struct XdpSocketConfig {
    pub id_string: String,
    pub interface: String,
    pub queue_id: u32,
    pub core_id: usize,   // Physical core this socket's worker runs on
    pub worker_id: usize, // Worker thread that owns this socket
    pub rx_queue_size: u32,
    pub tx_queue_size: u32,
    pub zerocopy: bool,
    pub umem_config: Arc<RuntimeUmemConfig>,
    pub xdp_mode: String,  // "native", "skb", or "auto"
    pub poll_mode: String, // "userspace_ring_spin" or "napi_busy_poll"
    pub busy_poll_ns: u64,
    pub busy_poll_budget: i32,
    /// GRO flush timeout in nanoseconds, used to derive the RX poll interval.
    pub napi_flush_timeout_ns: u64,
}

pub struct XSocket {
    umem: Arc<Umem>,
    // Shared Rings are wrapped in Arc<Mutex<>> to allow sharing across threads/managers
    // rings are only shared between sockets bound to the same interface and queue
    // maybe later we implement different XSockets for the case of shared and unshared fq/cq rings
    fill_queue: Arc<Mutex<FillQueue>>,
    comp_queue: Arc<Mutex<CompQueue>>,
    pub(crate) rx_queue: Arc<Mutex<RxQueue>>,
    pub(crate) tx_queue: Arc<Mutex<TxQueue>>,
    fd: RawFd,
    // Note: frame_registry removed - will be managed at worker level
    pub bus: Arc<SystemBus>,
    pub config: Arc<XdpSocketConfig>,
}

const MAX_FILL: usize = 2048;
const RX_DIAGNOSTIC_LOG_INTERVAL: u64 = 10_000;
const NAPI_DIAGNOSTIC_LOG_INTERVAL: u64 = 100_000;
/// Split components for independent RX and TX access
/// These DO NOT own the frame registry - it's accessed via thread-local storage
pub struct XSocketRx {
    fill_queue: Arc<Mutex<FillQueue>>,
    rx_queue: Arc<Mutex<RxQueue>>,
    // THE ANCHOR: Keeps the TX side alive even if the TX node is never built
    _tx_queue_keepalive: Arc<Mutex<TxQueue>>,
    _comp_queue_keepalive: Arc<Mutex<CompQueue>>,
    bus: Arc<SystemBus>,
    config: Arc<XdpSocketConfig>,
    scratch_descriptors: Vec<FrameDesc>,
    scratch_descriptors_recv_batch: Vec<FrameDesc>,
    scratch_frames: Vec<u32>,
    total_produced: usize,
    total_received: usize,
    empty_spins: usize,
    #[allow(dead_code)]
    last_logged_block: usize, // helper for logging
    fill_deficit: usize, // for more targeted replenishing
    // RX Fill Queue diagnostics. These counters are deliberately local to the
    // socket/worker so they can be updated without atomics in the hot path.
    fill_replenish_attempts: u64,
    fill_lock_busy: u64,
    fill_registry_empty: u64,
    fill_ring_full: u64,
    fill_zero_produced: u64,
    fill_partial_submissions: u64,
    max_fill_deficit: usize,
    // NAPI polling diagnostics. These are local to the RX worker and are only
    // read on the cold diagnostic path, so no atomics are needed.
    napi_poll_attempts: u64,
    napi_poll_packets: u64,
    napi_poll_empty: u64,
    max_napi_poll_consume: usize,
    napi_poll_timeout: rustix::time::Timespec,
    napi_busy_poll: bool,
    receive_calls: u64,
    fd: RawFd, // file descriptor of the AF_XDP socket
    // Profiling fields
    pub prof_reclaim_replenish_cycles: u64,
    pub prof_replenish_cycles: u64,
    pub prof_replenish_calls: u64,
    pub prof_consume_cycles: u64,
    pub prof_wakeup_cycles: u64,
    pub prof_pdu_batch_cycles: u64,
    pub prof_empty_spin_cycles: u64,
    pub prof_total_packets: u64,
    pub prof_max_replenish_cycles: u64,
    pub prof_monster_spikes: u64,
    pub prof_burst_start: u64,
    pub prof_started: bool,
}

pub struct XSocketTx {
    umem: Arc<Umem>,
    comp_descs_helper: Vec<FrameDesc>,
    comp_queue: Arc<Mutex<CompQueue>>,
    tx_queue: Arc<Mutex<TxQueue>>,
    // THE ANCHOR: Keeps the RX side alive even if the RX node is never built
    _rx_queue_keepalive: Arc<Mutex<RxQueue>>,
    _fill_queue_keepalive: Arc<Mutex<FillQueue>>,
    bus: Arc<SystemBus>,
    config: Arc<XdpSocketConfig>,
    total_consumed: usize,
    #[allow(dead_code)]
    last_logged_block: usize, // helper for logging
    total_sent_packets: usize, // helper for logging
    fd: RawFd,                 // file descriptor of the AF_XDP socket
}

impl XSocket {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(
        config: XdpSocketConfig,
        bus: Arc<SystemBus>,
        umem: Arc<Umem>,
        // Shared rings are optional; if None, we expect Socket::new to return them
        // these are only needed for sockets bound to the same interface and queue
        shared_rings: Option<(Arc<Mutex<FillQueue>>, Arc<Mutex<CompQueue>>)>,
        initial_descs: Vec<FrameDesc>,
    ) -> Result<(Self, Arc<Mutex<FillQueue>>, Arc<Mutex<CompQueue>>), Box<dyn std::error::Error>>
    {
        let mut bind_flags = BindFlags::XDP_USE_NEED_WAKEUP;
        if config.zerocopy {
            bind_flags |= BindFlags::XDP_ZEROCOPY;
        }

        // Determine XDP flags based on mode
        let xdp_flags = match config.xdp_mode.as_str() {
            "native" => {
                log::debug!("Using native/driver XDP mode for {}", config.interface);
                XdpFlags::XDP_FLAGS_DRV_MODE
            }
            "skb" | "generic" => {
                log::info!("Using SKB/generic XDP mode for {}", config.interface);
                XdpFlags::XDP_FLAGS_SKB_MODE
            }
            "auto" => {
                // Auto-detect: use SKB mode for veth/virtual interfaces, native for physical
                if config.interface.starts_with("veth")
                    || config.interface.starts_with("tap")
                    || config.interface.starts_with("tun")
                {
                    log::info!(
                        "Auto-detected virtual interface {}, using SKB/generic XDP mode",
                        config.interface
                    );
                    XdpFlags::XDP_FLAGS_SKB_MODE
                } else {
                    log::debug!("Using native/driver XDP mode for {}", config.interface);
                    XdpFlags::XDP_FLAGS_DRV_MODE
                }
            }
            _ => {
                log::warn!(
                    "Unknown XDP mode '{}', defaulting to native/driver mode",
                    config.xdp_mode
                );
                XdpFlags::XDP_FLAGS_DRV_MODE
            }
        };

        let socket_config = SocketConfig::builder()
            .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
            .xdp_flags(xdp_flags)
            .bind_flags(bind_flags)
            .rx_queue_size(QueueSize::try_from(config.rx_queue_size)?)
            .tx_queue_size(QueueSize::try_from(config.tx_queue_size)?)
            .build();

        let c_iface = CString::new(config.interface.as_str())?;
        // 2. Bind the Interface to a named variable (NOT a temporary)
        let iface_handle = Interface::new(c_iface);

        probe_kernel_fds("PRE Socket::new");
        let (tx_q, rx_q, queues_opt) =
            unsafe { Socket::new(socket_config, &umem, &iface_handle, config.queue_id)? };
        probe_kernel_fds("POST Socket::new");

        // 3. Resolve Fill/Comp Queues
        // If shared_rings is Some, we use them (this is a secondary socket bound to same i/f and queue in the group).
        // If shared_rings is None, Socket::new MUST have returned them (this is the primary socket).
        let (fq, cq) = if let Some((existing_fq, existing_cq)) = shared_rings {
            // Verify that we didn't accidentally get new queues when we expected shared ones
            if queues_opt.is_some() {
                log::error!(
                    "Worker {}: Socket returned queues despite shared UMEM config. Ignoring new queues.",
                    config.worker_id
                );
            }
            (existing_fq, existing_cq)
        } else {
            let (raw_fq, raw_cq) =
                queues_opt.ok_or("Primary socket failed to return Fill/Comp queues")?;
            (Arc::new(Mutex::new(raw_fq)), Arc::new(Mutex::new(raw_cq)))
        };

        // ==========================================
        //     IMMEDIATELY PRE-FILL THE QUEUE
        // ==========================================
        // We must push the allocated descriptors into the ring right now so the
        // Intel driver can grab them the first time we call poll() in the RX loop!
        if !initial_descs.is_empty() {
            let mut fq_guard = fq.lock();
            unsafe {
                let produced = fq_guard.produce(&initial_descs);
                log::info!(
                    "Worker {}: Pre-filled FQ ring with {} empty frames.",
                    config.worker_id,
                    produced
                );
            }
        }

        let fd = rx_q.fd().as_raw_fd();
        let xdp_fm = Self {
            umem,
            rx_queue: Arc::new(Mutex::new(rx_q)),
            tx_queue: Arc::new(Mutex::new(tx_q)),
            fill_queue: fq.clone(),
            comp_queue: cq.clone(),
            fd,
            bus,
            config: Arc::new(config),
        };

        match xdp_fm.config.poll_mode.as_str() {
            "userspace_ring_spin" => {}
            "napi_busy_poll" => xdp_fm
                .configure_napi_busy_polling(
                    xdp_fm.config.busy_poll_ns,
                    xdp_fm.config.busy_poll_budget,
                )
                .map_err(|error| {
                    format!(
                        "failed to configure NAPI busy polling for {} queue {}: {}",
                        xdp_fm.config.interface, xdp_fm.config.queue_id, error
                    )
                })?,
            mode => {
                return Err(format!(
                    "unsupported XSK poll_mode '{}' for {} queue {}; expected 'userspace_ring_spin' or 'napi_busy_poll'",
                    mode, xdp_fm.config.interface, xdp_fm.config.queue_id
                )
                .into());
            }
        }

        debug!(
            "Worker {}: Created XSocket for {} queue {}",
            xdp_fm.config.worker_id, xdp_fm.config.interface, xdp_fm.config.queue_id
        );
        Ok((xdp_fm, fq, cq))
    }

    // NOTE: Old XSocket methods that directly accessed frame_registry have been removed.
    // Frame management is now done at the worker level through XSocketRx/Tx.

    // --- Accessors ---
    #[inline(always)]
    pub fn umem_ptr(&self) -> *mut u8 {
        self.umem.as_ptr() as *mut u8
    }

    // Standard data access: desc.addr() is relative to UMEM start
    #[inline(always)]
    pub fn data(&self, desc: &FrameDesc) -> Data<'_> {
        unsafe { self.umem.data(desc) }
    }

    /// Returns the raw file descriptor of the AF_XDP socket.
    /// This is required to register the socket in the BPF XSKS-MAP.
    #[inline]
    pub fn socket_fd(&self) -> i32 {
        self.fd
    }

    /// Configure kernel/NAPI busy polling for this AF_XDP socket.
    ///
    /// `timeout_us` is the maximum time, in microseconds, that a blocking
    /// `poll()` may spend polling the device before sleeping. This setting is
    /// effective only when the receive path uses `poll()` with a positive
    /// timeout; it does not change the existing user-space ring-spinning path.
    ///
    /// The socket must be configured after creation and before it is split
    /// into its RX and TX handles. The NIC driver and kernel must support NAPI
    /// busy polling for this to have an effect.
    pub fn configure_napi_busy_polling(&self, timeout_ns: u64, budget: i32) -> std::io::Result<()> {
        if timeout_ns == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "busy-poll timeout must be positive",
            ));
        }

        // SO_BUSY_POLL accepts microseconds. Round up so a positive nanosecond
        // configuration never silently becomes a zero-microsecond timeout.
        let timeout_us: i32 = timeout_ns.div_ceil(1_000).try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "busy-poll timeout is too large for SO_BUSY_POLL",
            )
        })?;

        if budget <= 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "busy-poll budget must be positive",
            ));
        }

        // SAFETY: `self.fd` is the live AF_XDP socket descriptor owned by this
        // XSocket, and both option values remain valid for the duration of
        // their respective setsockopt calls.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &timeout_us as *const libc::c_int as *const libc::c_void,
                std::mem::size_of_val(&timeout_us) as libc::socklen_t,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }

        // SAFETY: `self.fd` is the live AF_XDP socket descriptor owned by this
        // XSocket, and `budget` remains valid for the duration of this call.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL_BUDGET,
                &budget as *const libc::c_int as *const libc::c_void,
                std::mem::size_of_val(&budget) as libc::socklen_t,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }

        info!(
            "Configured NAPI busy polling for {} queue {}: busy_poll_ns={}, socket_busy_poll_us={}, busy_poll_budget={}",
            self.config.interface, self.config.queue_id, timeout_ns, timeout_us, budget,
        );

        // Prefer busy polling over interrupt-driven wakeups where supported.
        // This is a preference rather than the mechanism that enables the
        // per-socket busy-poll timeout, so retain compatibility with kernels
        // that do not expose this option.
        let prefer_busy_poll: libc::c_int = 1;
        // SAFETY: `self.fd` is the live AF_XDP socket descriptor owned by this
        // XSocket, and `prefer_busy_poll` remains valid for this call.
        let result = unsafe {
            libc::setsockopt(
                self.fd,
                libc::SOL_SOCKET,
                libc::SO_PREFER_BUSY_POLL,
                &prefer_busy_poll as *const libc::c_int as *const libc::c_void,
                std::mem::size_of_val(&prefer_busy_poll) as libc::socklen_t,
            )
        };

        if result < 0 {
            let error = std::io::Error::last_os_error();
            log::debug!(
                "SO_PREFER_BUSY_POLL unavailable for {} queue {}: {}",
                self.config.interface,
                self.config.queue_id,
                error
            );
        }

        Ok(())
    }

    // IMPORTANT: Verify compatibility before TX
    #[inline]
    pub fn is_compatible(&self, other_group_id: usize) -> bool {
        self.group_id() == other_group_id
    }

    /// Returns the logical Core ID assigned to this manager.
    #[inline(always)]
    pub fn core_id(&self) -> usize {
        self.config.core_id
    }

    /// Returns the queue ID assigned to this manager.
    #[inline(always)]
    pub fn queue_id(&self) -> u32 {
        self.config.queue_id
    }

    /// Returns the interface name assigned to this manager.
    #[inline(always)]
    pub fn interface(&self) -> &str {
        &self.config.interface
    }

    // Helper to get group ID easily
    #[inline(always)]
    pub fn group_id(&self) -> usize {
        self.config.umem_config.group_id
    }

    #[inline(always)]
    pub fn frames_per_core(&self) -> u32 {
        self.config.umem_config.frame_count
    }

    #[inline(always)]
    pub fn umem_config(&self) -> &RuntimeUmemConfig {
        &self.config.umem_config
    }

    pub fn xdp_rx_statistics(&self) -> std::io::Result<XdpStatistics> {
        let rx_guard = self.rx_queue.lock();
        rx_guard.fd().xdp_statistics()
    }

    /// Split this XSocket into separate RX and TX components
    /// This allows independent ownership of RX and TX rings by different nodes
    ///
    /// Frame registry is accessed via thread-local storage set by the worker
    pub fn split(self) -> (XSocketRx, XSocketTx) {
        let fd = self.socket_fd();
        let target_us = self.config.napi_flush_timeout_ns / 1000 / 2; // Poll at 50% of the timeout
                                                                      // Socket splitting can happen before the worker initializes its
                                                                      // thread-local TSC frequency. Keep the same 2.5 GHz fallback used by
                                                                      // `rdtsc_us()` so the NAPI poll interval never becomes zero.
        let tsc_frequency = try_get_tsc_frequency().unwrap_or_else(|| 2_500_000_000);
        let napi_poll_target_cycles =
            (((target_us as u128) * (tsc_frequency as u128)) / 1_000_000).max(1) as u64;
        let napi_poll_timeout = rustix::time::Timespec {
            tv_sec: (self.config.busy_poll_ns / 1_000_000_000) as _,
            tv_nsec: (self.config.busy_poll_ns % 1_000_000_000) as _,
        };
        let napi_busy_poll = self.config.poll_mode == "napi_busy_poll";
        if self.config.poll_mode == "napi_busy_poll" {
            info!(
                "NAPI poll interval for {} queue {}: flush_timeout_ns={}, poll_target_interval_cycles={}",
                self.config.interface,
                self.config.queue_id,
                self.config.napi_flush_timeout_ns,
                napi_poll_target_cycles,
            );
        }

        let rx = XSocketRx {
            fill_queue: self.fill_queue.clone(),
            rx_queue: self.rx_queue.clone(),
            _tx_queue_keepalive: self.tx_queue.clone(),
            _comp_queue_keepalive: self.comp_queue.clone(),
            bus: self.bus.clone(),
            config: self.config.clone(),
            scratch_descriptors: vec![FrameDesc::default(); MAX_FILL],
            scratch_descriptors_recv_batch: vec![FrameDesc::default(); MAX_FILL],
            scratch_frames: vec![0u32; MAX_FILL],
            total_produced: 0,
            total_received: 0,
            empty_spins: 0,
            last_logged_block: 0,
            fill_deficit: 0,
            fill_replenish_attempts: 0,
            fill_lock_busy: 0,
            fill_registry_empty: 0,
            fill_ring_full: 0,
            fill_zero_produced: 0,
            fill_partial_submissions: 0,
            max_fill_deficit: 0,
            napi_poll_attempts: 0,
            napi_poll_packets: 0,
            napi_poll_empty: 0,
            max_napi_poll_consume: 0,
            napi_poll_timeout,
            napi_busy_poll,
            receive_calls: 0,
            fd,
            prof_reclaim_replenish_cycles: 0,
            prof_replenish_cycles: 0,
            prof_replenish_calls: 0,
            prof_consume_cycles: 0,
            prof_wakeup_cycles: 0,
            prof_pdu_batch_cycles: 0,
            prof_empty_spin_cycles: 0,
            prof_total_packets: 0,
            prof_max_replenish_cycles: 0,
            prof_monster_spikes: 0,
            prof_burst_start: 0,
            prof_started: false,
        };

        let tx = XSocketTx {
            umem: self.umem,
            comp_descs_helper: vec![FrameDesc::default(); 4 * MAX_BATCH_SIZE],
            comp_queue: self.comp_queue,
            tx_queue: self.tx_queue,
            _rx_queue_keepalive: self.rx_queue,
            _fill_queue_keepalive: self.fill_queue,
            bus: self.bus,
            config: self.config,
            total_consumed: 0,
            last_logged_block: 0,
            total_sent_packets: 0,
            fd,
        };

        (rx, tx)
    }
}

use rustix::event::{poll, PollFd, PollFlags};
use rustix::io::Errno;
use separator::Separatable;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::frame_registry::{
    decode_global_frame, encode_global_frame, get_tsc_frequency, get_worker_frame_registry,
};
use wabrix_base::system::{rdtsc_us, try_get_tsc_frequency};
use wabrix_base::{RuntimeUmemConfig, SystemBus};

impl XSocketRx {
    /// Returns the UMEM group ID for this socket
    #[inline]
    pub fn umem_group_id(&self) -> usize {
        self.config.umem_config.group_id
    }

    /// Returns the number of calls made to `receive_into_batch`.
    #[inline]
    pub fn receive_calls(&self) -> u64 {
        self.receive_calls
    }

    #[cfg(feature = "profiling")]
    pub fn get_replenish_avg_cycles(&self) -> u64 {
        self.prof_replenish_cycles
            .checked_div(self.prof_replenish_calls)
            .unwrap_or(0)
    }

    #[cfg(feature = "profiling")]
    pub fn get_replenish_total_cycles(&self) -> u64 {
        self.prof_replenish_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn get_replenish_calls(&self) -> u64 {
        self.prof_replenish_calls
    }

    // Unconditional Wakeup
    pub fn wakeup_fill_queue_force(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut pollfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        unsafe {
            libc::poll(&mut pollfd, 1, 0);
        }
        Ok(())
    }

    fn wakeup_rx_queue(&self) -> Result<(), Errno> {
        // Create a poll structure asking for POLLIN (Read) events
        // SAFETY: self.fd is a valid file descriptor for the lifetime of this call
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(self.fd) };
        let mut poll_fds = [PollFd::new(&borrowed_fd, PollFlags::IN)];

        // Call poll with 0 timeout (non-blocking check).
        // This instantly kicks the kernel driver to check the Fill Queue.
        use rustix::time::Timespec;
        let timeout = Timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        poll(&mut poll_fds, Some(&timeout))?;

        Ok(())
    }

    fn poll_for_napi_packets(&self, timeout: Option<&rustix::time::Timespec>) -> Result<(), Errno> {
        // SAFETY: self.fd is a valid file descriptor for the lifetime of this call.
        let borrowed_fd = unsafe { BorrowedFd::borrow_raw(self.fd) };
        let mut poll_fds = [PollFd::new(&borrowed_fd, PollFlags::IN)];

        poll(&mut poll_fds, timeout)?;
        Ok(())
    }

    /// Replenishes the Fill Queue with available frames from the worker's frame registry.
    fn replenish_fill_queue(&mut self, requested_count: usize) -> usize {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        if requested_count == 0 {
            return 0;
        }

        self.fill_replenish_attempts += 1;

        if requested_count > MAX_FILL {
            if self
                .fill_replenish_attempts
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "Requested {} frames to replenish Fill Queue, but max is {} (attempt {})",
                    requested_count,
                    MAX_FILL,
                    self.fill_replenish_attempts
                );
            }
        }

        let requested_count = requested_count.min(MAX_FILL);

        // Access Thread-Local Registry
        let registry_ptr = get_worker_frame_registry();
        if registry_ptr.is_null() {
            // This happens if called before worker initialization is complete
            self.fill_registry_empty += 1;
            if self
                .fill_registry_empty
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Fill Queue replenishment has no worker registry ({} occurrences)",
                    self.source_id(),
                    self.fill_registry_empty
                );
            }
            return 0;
        }
        let frame_registry = unsafe { &mut *registry_ptr };

        let available = requested_count;

        #[cfg(feature = "profiling")]
        let t0 = unsafe { std::arch::x86_64::_rdtsc() };

        // Acquire Lock on the Fill Queue
        // (Shared because multiple sockets/queues might map to the same UMEM)
        // If another thread/queue is currently replenishing this UMEM, we instantly
        // give up and return to our RX loop. We will just try again on the next loop!
        // This absolutely eliminates the 30,000 cycle OS context-switch penalty.
        let mut fq = match self.fill_queue.try_lock() {
            Some(guard) => guard,
            None => {
                self.fill_lock_busy += 1;
                if self
                    .fill_lock_busy
                    .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
                {
                    log::warn!(
                        "{}: Fill Queue replenishment skipped because queue lock was busy ({} occurrences, deficit={})",
                        self.source_id(),
                        self.fill_lock_busy,
                        self.fill_deficit
                    );
                }
                return 0;
            }
        };

        // Check Capacity
        // nb_free() tells us how many slots are open in the ring
        let nb_free = fq.nb_free_exact();
        //let nb_free = fq.nb_free(available as u32);
        let batch_size = std::cmp::min(nb_free, available as u32);

        if batch_size > 0 {
            trace!(
                "{}: Replenishing {} frames, available = {}, nb_free = {}",
                self.source_id(),
                batch_size,
                available,
                nb_free
            );
        }

        if batch_size == 0 {
            self.fill_ring_full += 1;
            if self
                .fill_ring_full
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Fill Queue has no free slots while replenishing ({} occurrences, requested={}, nb_free={}, deficit={})",
                    self.source_id(),
                    self.fill_ring_full,
                    requested_count,
                    nb_free,
                    self.fill_deficit
                );
            }
            return 0; // Ring is full, nothing to do
        }

        // 1. Allocate Frames directly into our pre-allocated struct buffer (NO MEMSET!)

        #[cfg(feature = "profiling")]
        let t1 = unsafe { std::arch::x86_64::_rdtsc() };

        let umem_id = self.umem_group_id();
        let actual_batch = frame_registry.allocate_batch_into(
            umem_id,
            &mut self.scratch_frames[..batch_size as usize], // Use persistent struct Vec
        );

        #[cfg(feature = "profiling")]
        let t2 = unsafe { std::arch::x86_64::_rdtsc() };

        if actual_batch == 0 {
            // No frames left! This implies the worker is processing too slowly
            // or holding too many frames in buffers.
            self.fill_registry_empty += 1;
            if self
                .fill_registry_empty
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Frame registry returned no frames for Fill Queue ({} occurrences, requested={}, free_frames={}, deficit={})",
                    self.source_id(),
                    self.fill_registry_empty,
                    batch_size,
                    frame_registry.total_free_frames(),
                    self.fill_deficit
                );
            }
            return 0;
        }

        // Translate & Fill
        // Architecture Rule: Kernel expects PHYSICAL ADDRESSES (offsets), not indices.
        // Address = (Local_Index * Frame_Size) + Headroom

        let frame_size_log2 = self.config.umem_config.frame_size_log2 as u64;
        let headroom = self.config.umem_config.head_room as u64;

        for i in 0..actual_batch {
            let (_umem_id, local_idx) = decode_global_frame(self.scratch_frames[i]);
            // Use a 1-cycle bitwise shift instead of multiplication
            let addr = ((local_idx as u64) << frame_size_log2) + headroom;
            // Write directly to our persistent struct buffer
            self.scratch_descriptors[i] = xsk_rs::FrameDesc::new(addr as usize);
        }

        #[cfg(feature = "profiling")]
        let t3 = unsafe { std::arch::x86_64::_rdtsc() };

        // Produce unconditionally first
        let produced = unsafe { fq.produce(&self.scratch_descriptors[..actual_batch]) };

        // Check if kernel went to sleep and needs a nudge
        let needs_wakeup = produced > 0 && fq.needs_wakeup();

        // Drop the mutex guard before calling wakeup to avoid borrow conflict
        drop(fq);

        // THEN wake the kernel if needed
        #[cfg(feature = "profiling")]
        let t4 = unsafe { std::arch::x86_64::_rdtsc() };

        if needs_wakeup {
            // NOTE: This syscall will still cost ~1,500 cycles when it triggers,
            // but it is legally required by the Intel driver to wake it up.
            self.wakeup_fill_queue_force()
                .expect("wakeup_fill_queue_force() failed");
        }

        #[cfg(feature = "profiling")]
        let t5 = unsafe { std::arch::x86_64::_rdtsc() };

        if produced > 0 {
            // Return any frames that weren't produced back to the registry
            // CRITICAL FIX: Only release un-pushed frames up to `actual_batch`
            if produced < actual_batch {
                for &frame_idx in &self.scratch_frames[produced..actual_batch] {
                    trace!(
                        "replenish_fill_queue: Returning frame {} to local stash",
                        frame_idx
                    );
                    frame_registry.release_local(frame_idx);
                }
            }
            log::trace!(
                "Worker {}: Replenished {} frames to {} queue {} Fill Ring, free frames remaining: {}",
                self.config.worker_id,
                produced,
                &self.config.interface,
                self.config.queue_id,
                frame_registry.total_free_frames()
            );

            self.total_produced += produced;
        } else {
            self.fill_zero_produced += 1;
            if self
                .fill_zero_produced
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Fill Queue produced zero frames despite {} prepared ({} occurrences, deficit={})",
                    self.source_id(),
                    actual_batch,
                    self.fill_zero_produced,
                    self.fill_deficit
                );
            }
        }

        if produced < actual_batch && produced > 0 {
            self.fill_partial_submissions += 1;
            if self
                .fill_partial_submissions
                .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Partial Fill Queue submission: produced {} of {} frames ({} occurrences, deficit={})",
                    self.source_id(),
                    produced,
                    actual_batch,
                    self.fill_partial_submissions,
                    self.fill_deficit
                );
            }
        }

        #[cfg(feature = "profiling")]
        let t6 = unsafe { std::arch::x86_64::_rdtsc() };

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            let duration = end_cycles - start_cycles;

            self.prof_replenish_cycles += duration;
            self.prof_replenish_calls += 1;

            // Catch the Sledgehammer
            if duration > 50_000 {
                self.prof_monster_spikes += 1;
                self.prof_max_replenish_cycles = self.prof_max_replenish_cycles.max(duration);

                // Only print if the spike is absurdly high (e.g., > 50,000 cycles)
                log::trace!(
                    "MONSTER SPIKE DETECTED on socket {}: Replenish of {} frames took {} cycles!",
                    self.source_id(),
                    produced,
                    duration
                );
                log::trace!(
                    "\n=== Replenish X-Ray (Cycles) ===\n\
             Lock      : {}\n\
             Allocator : {}\n\
             Math Loop : {}\n\
             FQ Produce: {}\n\
             Syscall   : {}\n\
             Release   : {}",
                    t1 - t0,
                    t2 - t1,
                    t3 - t2,
                    t4 - t3,
                    t5 - t4,
                    t6 - t5
                );
            }
        }

        produced
    }
}

// Implement BatchRx for XSocketRx
impl BatchRx for XSocketRx {
    fn receive_into_batch(&mut self, batch: &mut Batch, max_count: usize) -> anyhow::Result<usize> {
        self.receive_calls += 1;

        // Get the worker's frame registry from thread-local storage
        let frame_registry_ptr = get_worker_frame_registry();

        // Check if pointer is null (happens during shutdown)
        if frame_registry_ptr.is_null() {
            return Ok(0);
        }

        // SAFETY: The pointer is valid for the lifetime of the worker thread
        let frame_registry = unsafe { &mut *frame_registry_ptr };

        // ⏱️ START STAGE 1: Reclaim & Replenish

        #[cfg(feature = "profiling")]
        let t0 = unsafe { core::arch::x86_64::_rdtsc() };

        // 1. Process returns from TX side (via SystemBus)
        // This reclaims frames that TX completed
        // THROTTLED RECLAIM
        // Cross-thread communication is expensive. Only check for returned frames
        // every 32 loops, OR if we are getting dangerously low on free frames.
        if self.empty_spins.is_multiple_of(32) || frame_registry.total_free_frames() < 8192 {
            if frame_registry.total_free_frames() < 8192 {
                log::warn!(
                    "total_free_frames is too low: {}",
                    frame_registry.total_free_frames()
                );
            }
            frame_registry.process_returns();
        }
        const FILL_REPLENISH_THRESHOLD: usize = 1024;

        // 2. Replenish fill queue so NIC has buffers to write into
        // AGGRESSIVE REPLENISHING
        // Always try to keep the fill queue as full as possible.
        // The kernel drops packets if the fill queue is empty when packets arrive.
        //let fq_size = self.config.umem_config.fq_size as usize;
        self.max_fill_deficit = self.max_fill_deficit.max(self.fill_deficit);
        if self.fill_deficit >= FILL_REPLENISH_THRESHOLD {
            let produced = self.replenish_fill_queue(self.fill_deficit);
            self.fill_deficit = self.fill_deficit.saturating_sub(produced);
            if self.fill_deficit > 4096
                && self
                    .fill_replenish_attempts
                    .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!(
                    "{}: Fill deficit is high: {} (max observed={}, replenishments={}, lock_busy={}, no_frames={})",
                    self.source_id(),
                    self.fill_deficit,
                    self.max_fill_deficit,
                    self.fill_replenish_attempts,
                    self.fill_lock_busy,
                    self.fill_registry_empty
                );
            }
        }

        #[cfg(feature = "profiling")]
        {
            let t1 = unsafe { core::arch::x86_64::_rdtsc() };
            if self.prof_started {
                self.prof_reclaim_replenish_cycles += t1 - t0;
            }
            // ⏱️ END STAGE 1
        }

        // Determine how many we can actually pull safely
        let batch_len = batch.len();
        if batch_len != 0 {
            warn!("Batch not empty on receive_into_batch");
        }
        let max_to_pull = std::cmp::min(max_count, MAX_BATCH_SIZE).saturating_sub(batch_len);
        if max_to_pull == 0 {
            return Ok(0);
        }

        // ⏱️ START STAGE 2: Lockless Consume
        #[cfg(feature = "profiling")]
        let t1 = unsafe { core::arch::x86_64::_rdtsc() };

        // USING PRE-ALLOCATED STRUCT BUFFER
        // We slice into the buffer we already own. Zero memory allocation, zero memset overhead!
        // Lock the queue. It is uncontended, so it takes ~15 cycles.
        let mut rx_guard = self.rx_queue.lock();
        let mut rcvd=
            unsafe { rx_guard.consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull]) };

        #[cfg(feature = "profiling")]
        let t2 = unsafe { core::arch::x86_64::_rdtsc() };

        #[cfg(feature = "profiling")]
        {
            if self.prof_started {
                self.prof_consume_cycles += t2 - t1;
            }
            // ⏱️ END STAGE 2
        }

        // ⏱️ START STAGE 3: Wakeup / Empty Path
        // SMART KERNEL WAKEUP LOGIC
        #[cfg(feature = "profiling")]
        let mut t3 = 0;
        // Demand-Driven NAPI Polling
        if rcvd == 0 {
            self.empty_spins += 1;
            // LOCAL SPIN: Allow a brief window to catch fast-arriving packets
            // completely locklessly without wasting cycles on a syscall.
            if self.empty_spins < 64 && self.napi_busy_poll {
                // Yield this tiny micro-window to the CPU pipeline
                core::hint::spin_loop();
                rcvd = unsafe {
                    rx_guard.consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull])
                };
            }

            // Fallback to NAPI poll syscall ONLY if local spinning exhausts
            if rcvd==0 && self.napi_busy_poll {
                self.napi_poll_attempts += 1;

                // Hand control to the kernel to spin safely on the hardware ring.
                self.poll_for_napi_packets(Some(&self.napi_poll_timeout))?;

                let newly_received = unsafe {
                    rx_guard.consume(
                        &mut self.scratch_descriptors_recv_batch[rcvd..max_to_pull],
                    )
                };

                if newly_received == 0 {
                    self.napi_poll_empty += 1;
                }

                self.max_napi_poll_consume =
                    self.max_napi_poll_consume.max(newly_received);
                rcvd += newly_received;
                self.napi_poll_packets += newly_received as u64;

            }

            // Legacy driver wakeup fallback if empty spins persist
            if rcvd == 0 {
                if self.prof_started && self.empty_spins >= 51200 {
                    self.prof_started = false;
                    let now = unsafe { core::arch::x86_64::_rdtsc() };
                    log::debug!(
                        "**** Traffic burst ended after ~ {} cycles ({} us)",
                        (now - self.prof_burst_start).separated_string(),
                        (((now - self.prof_burst_start) as f64
                            * (1_000_000.0 / get_tsc_frequency() as f64)) as u64)
                            .separated_string()
                    );
                }

                if self.empty_spins.is_multiple_of(128) {
                    // Use try_lock so the empty path never blocks on the fill queue.
                    let fq_needs_wakeup = if let Some(fq) = self.fill_queue.try_lock() {
                        fq.needs_wakeup()
                    } else {
                        false
                    };

                    if fq_needs_wakeup {
                        self.wakeup_rx_queue()?;
                        rcvd = unsafe {
                            rx_guard.consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull])
                        };
                    }
                }

                #[cfg(feature = "profiling")]
                {
                    t3 = unsafe { core::arch::x86_64::_rdtsc() };
                }

                #[cfg(feature = "profiling")]
                if self.prof_started {
                    self.prof_wakeup_cycles += t3 - t2;
                }

                if log::log_enabled!(log::Level::Debug) && self.prof_started && rcvd > 0 && self.napi_poll_attempts.is_multiple_of(NAPI_DIAGNOSTIC_LOG_INTERVAL)
                {
                    debug!(
                        "{}: [{} us] After Wakeup: Received {} packets. Empty spins= {}. Total rx: {}, Free frames: {}",
                        self.source_id(),
                        rdtsc_us().separate_with_precision(3),
                        rcvd,
                        self.empty_spins,
                        self.total_received + rcvd,
                        frame_registry.total_free_frames()
                    );
                }
            }
        }

        if rcvd > 0 {
            #[cfg(feature = "profiling")]
            if !self.prof_started {
                self.prof_started = true;
                self.prof_burst_start = unsafe { core::arch::x86_64::_rdtsc() };

                // Start a fresh NAPI diagnostic window for this traffic burst.
                // The poll that discovers the first packet is intentionally not
                // included because it happened before the burst was detected.
                self.napi_poll_attempts = 0;
                self.napi_poll_packets = 0;
                self.napi_poll_empty = 0;
                self.max_napi_poll_consume = 0;
                self.empty_spins = 0;
            }

            self.fill_deficit += rcvd; // We owe the FILL_RING this many frames

            #[cfg(feature = "profiling")]
            let t3 = unsafe { core::arch::x86_64::_rdtsc() };

            #[cfg(feature = "profiling")]
            {
                // Record the time it took just to check the condition when rcvd > 0
                if self.prof_started {
                    self.prof_wakeup_cycles += t3 - t2;
                }
            }

            // 5. ZERO-OVERHEAD LOGGING
            // By wrapping this in `log_enabled!`, Rust absolutely guarantees that `rdtsc_us()`
            // and the string formatting will NEVER evaluate unless you are actively debugging.
            if log::log_enabled!(log::Level::Trace) {
                trace!(
                    "{}: [{} us] Received {} packets. Empty spins= {}. Total rx: {}, Free frames: {}",
                    self.source_id(),
                    rdtsc_us().separate_with_precision(3),
                    rcvd,
                    self.empty_spins,
                    self.total_received + rcvd,
                    frame_registry.total_free_frames()
                );
            }
            self.empty_spins = 0;
        }
        // The RX ring is no longer accessed after the consume/wakeup stage.
        // Release its lock before periodic diagnostics query the socket stats.
        drop(rx_guard);
        // ⏱️ END STAGE 3

        self.total_received += rcvd;

        if log::log_enabled!(log::Level::Debug)
            && self.prof_started
            && self.napi_poll_attempts > 0
            && self
            .napi_poll_attempts
            .is_multiple_of(NAPI_DIAGNOSTIC_LOG_INTERVAL)
        {
            let average_napi_poll_consume =
                self.napi_poll_packets as f64 / self.napi_poll_attempts as f64;
            debug!(
                            "{}: NAPI poll diagnostics: attempts={}, packets={}, average_consume_after_poll={:.2}, empty={}, max_consume_after_poll={}, receive_calls={}, total_rx={}, rcvd= {}",
                            self.source_id(),
                            self.napi_poll_attempts.separated_string(),
                            self.napi_poll_packets.separated_string(),
                            average_napi_poll_consume,
                            self.napi_poll_empty.separated_string(),
                            self.max_napi_poll_consume,
                            self.receive_calls.separated_string(),
                            (self.total_received + rcvd).separated_string(),
                            rcvd
                        );
        }


        /*
        // Emit a periodic summary rather than logging every normal refill.
        // This gives enough information to correlate software drops with FQ
        // starvation while keeping the packet path quiet.
        let received = self.total_received as u64;
        if received >= self.last_diagnostic_received + 10_000_000 {
            self.last_diagnostic_received = received;
            let xdp_stats = {
                let rx_guard = self.rx_queue.lock();
                rx_guard.fd().xdp_statistics()
            };
            match xdp_stats {
                Ok(stats) => log::debug!(
                    "{}: RX diagnostics: received={}, produced={}, deficit={}, max_deficit={}, replenish_attempts={}, lock_busy={}, no_frames={}, ring_full={}, zero_produced={}, partial_submissions={}, xdp_dropped={}, xdp_invalid_descs={}, xdp_rx_ring_full={}, xdp_fill_ring_empty={}",
                    self.source_id(),
                    received,
                    self.total_produced,
                    self.fill_deficit,
                    self.max_fill_deficit,
                    self.fill_replenish_attempts,
                    self.fill_lock_busy,
                    self.fill_registry_empty,
                    self.fill_ring_full,
                    self.fill_zero_produced,
                    self.fill_partial_submissions,
                    stats.rx_dropped(),
                    stats.rx_invalid_descs(),
                    stats.rx_ring_full(),
                    stats.rx_fill_ring_empty_descs()
                ),
                Err(error) => log::warn!(
                    "{}: failed to read AF_XDP RX diagnostics after {} packets: {}",
                    self.source_id(),
                    received,
                    error
                ),
            }
        }
        */


        // If the RX ring is STILL empty after the kick (or if no kick was needed),
        // return Ok(0) instantly and spin in zero-overhead user-space.
        if rcvd == 0 {
            #[cfg(feature = "profiling")]
            {
                if self.prof_started {
                    self.prof_empty_spin_cycles += t3 - t0;
                }
            }
            return Ok(0);
        } else {
            self.empty_spins = 0;
        }

        // ⏱️ START STAGE 4: PDU Batching
        #[cfg(feature = "profiling")]
        let t3_for_stage4 = unsafe { core::arch::x86_64::_rdtsc() };

        // 6. Process the received packets
        let frame_size_log2 = self.config.umem_config.frame_size_log2;
        let bus_ptr = &*self.bus as *const _;
        let worker_id = self.config.worker_id as u8;
        let group_id = self.config.umem_config.group_id;

        // This guarantees zero bounds-checking overhead
        let received_descriptors = &self.scratch_descriptors_recv_batch[..rcvd];
        for desc in received_descriptors {
            // BITWISE SHIFT INSTEAD OF DIVISION
            // Replaces ~20 cycle division with a 1 cycle shift
            let local_idx = (desc.addr() as u32) >> frame_size_log2; // local_idx is wrt. current Umem
            let global_idx = encode_global_frame(group_id, local_idx);

            let mut pdu = Pdu::new(*desc, global_idx, worker_id, bus_ptr, frame_size_log2);
            // Capture the host TSC when the RX descriptor is converted into a
            // pipeline packet. The Wasm-facing timestamp is carried with the
            // PDU for downstream telemetry and processing.
            pdu.w.set_timestamp(unsafe { core::arch::x86_64::_rdtsc() });

            // Since we ALREADY calculated max_to_pull to guarantee capacity,
            // we can confidently unwrap/ignore the error branch.
            batch.push(pdu).expect("Failed to push PDU into batch");
        }

        #[cfg(feature = "profiling")]
        {
            let t4 = unsafe { core::arch::x86_64::_rdtsc() };
            if self.prof_started {
                self.prof_pdu_batch_cycles += t4 - t3_for_stage4;
            }
            // ⏱️ END STAGE 4

            // 📊 PROFILING AGGREGATION & REPORTING
            if self.prof_started {
                self.prof_total_packets += rcvd as u64;
            }

            const PRINT_INTERVAL_PACKETS: u64 = 100_000_000;
            // Print summary every PRINT_INTERVAL_PACKETS and reset counters
            if log::log_enabled!(log::Level::Debug) && self.prof_started && self.prof_total_packets >= PRINT_INTERVAL_PACKETS {
                let total = self.prof_total_packets;

                // Calculate cycles per packet for each stage
                let s1_cpp = self.prof_reclaim_replenish_cycles / total;
                let s2_cpp = self.prof_consume_cycles / total;
                let s3_cpp = self.prof_wakeup_cycles / total;
                let s4_cpp = self.prof_pdu_batch_cycles / total;
                let total_cpp = s1_cpp + s2_cpp + s3_cpp + s4_cpp;

                log::debug!(
                    "\n=== CPU Cycle Breakdown (Per Packet Average over {} pkts) ===\n\
                     1. Reclaim & Replenish : {:>4} cycles\n\
                     2. Lockless Consume    : {:>4} cycles\n\
                     3. Wakeup Logic        : {:>4} cycles\n\
                     4. PDU Gen & Batch Push: {:>4} cycles\n\
                     --------------------------------------------------\n\
                     Total Active CPU Cost  : {:>4} cycles/packet\n\
                     Total Empty Spins Cost : {:>10} cycles (Total for {} packets)\n\
                     ==================================================",
                    total.separated_string(),
                    s1_cpp,
                    s2_cpp,
                    s3_cpp,
                    s4_cpp,
                    total_cpp,
                    self.prof_empty_spin_cycles.separated_string(),
                    total.separated_string()
                );

                log::debug!(
                    "{}: Replenish diagnostics: monster_spikes={}, max_replenish_cycles={}",
                    self.source_id(),
                    self.prof_monster_spikes.separated_string(),
                    self.prof_max_replenish_cycles.separated_string(),
                );

                // Reset counters
                self.prof_reclaim_replenish_cycles = 0;
                self.prof_consume_cycles = 0;
                self.prof_wakeup_cycles = 0;
                self.prof_pdu_batch_cycles = 0;
                self.prof_empty_spin_cycles = 0;
                self.prof_total_packets = 0;
                self.prof_max_replenish_cycles = 0;
                self.prof_monster_spikes = 0;
            }

        }

        Ok(rcvd)
    }

    fn replenish(&mut self, requested_count: usize) -> anyhow::Result<()> {
        self.replenish_fill_queue(requested_count);
        Ok(())
    }

    fn source_id(&self) -> &String {
        &self.config.id_string
    }

    #[cfg(feature = "profiling")]
    fn get_replenish_avg_cycles(&self) -> u64 {
        self.get_replenish_avg_cycles()
    }

    #[cfg(feature = "profiling")]
    fn get_replenish_total_cycles(&self) -> u64 {
        self.get_replenish_total_cycles()
    }

    #[cfg(feature = "profiling")]
    fn get_replenish_calls(&self) -> u64 {
        self.get_replenish_calls()
    }
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
                    self.config.worker_id, self.config.queue_id, original_batch_len
                );
                return Ok(0);
            }
        } else if free_slots < original_batch_len {
            log::info!(
                "⚠️ [TX BACKPRESSURE] Worker {} Queue {}: Batch of {} packets, but only {} TX slots free.",
                self.config.worker_id,
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
        let registry_ptr = crate::xsocket::get_worker_frame_registry();

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
                            self.config.worker_id, my_umem_id, frame_registry.total_free_frames()
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
            tv_sec: (self.config.busy_poll_ns / 1_000_000_000) as _,
            tv_nsec: (self.config.busy_poll_ns % 1_000_000_000) as _,
        };

        poll(&mut poll_fds, Some(&timeout))?;
        Ok(())
    }

    /*
        /// Explicitly wakes up the kernel TX driver if it went to sleep
        #[inline]
        pub fn wakeup_tx_queue(&self) -> anyhow::Result<()> {
            // Note: This requires access to the TX queue status
            // In libxdp/xsk-rs, checking needs_wakeup usually happens on the ring struct.
            // If the flag is set:
            unsafe {
                if self.tx_queue.needs_wakeup() {
                    //trace!("Worker {}: TX Queue needs wakeup! Triggering syscall...", self.config.worker_id);
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
                    self.config.worker_id,
                    self.config.interface,
                    self.config.queue_id,
                    &self.total_consumed
                );
                self.last_logged_block = current_block;
            }

            let frame_size_log2 = self.config.umem_config.frame_size_log2;

            // FIX: Iterate by index instead of .iter()
            for i in 0..consumed {
                let addr = self.comp_descs_helper[i].addr() as u32;

                // 1. Calculate the local index relative to this specific UMEM
                let local_idx = addr >> frame_size_log2;

                // 2. Encode to the global frame index (umem_id << 24 | local_idx)
                let global_idx = encode_global_frame(self.config.umem_config.group_id, local_idx);
                trace!(
                    "drain_completion_queue: Worker {}: Releasing frame_idx={} (local_idx={}, umem_group_id={})",
                    self.config.worker_id,
                    global_idx,
                    local_idx,
                    self.config.umem_config.group_id
                );
                self.release_frame(global_idx);
            }
        } else {
            if self.total_sent_packets > self.total_consumed {
                let pending_in_hardware = self.total_sent_packets - self.total_consumed;

                // Only warn if a massive number of packets are stuck in limbo
                if pending_in_hardware > 10000 {
                    log::warn!(
                        "👻 [CQ GHOST TOWN] Worker {} Queue {}: {} packets sent to TX, but NIC has not completed them! Hardware stall imminent.",
                        self.config.worker_id, self.config.queue_id, pending_in_hardware
                    );
                }
            }
        }
        Ok(consumed)
    }

    fn release_frame(&mut self, frame_idx: u32) {
        let owner = self.determine_owner(frame_idx);
        /*
        log::trace!(
            "Worker {}: Releasing frame_idx={}, determined owner={}, my worker_id={}",
            self.config.worker_id,
            frame_idx,
            owner,
            self.config.worker_id
        );
        */
        // FAST PATH: If the frame belongs to this worker, bypass the MPSC channel entirely!
        if owner == self.config.worker_id {
            unsafe {
                let registry_ptr = wabrix_base::frame_registry::get_worker_frame_registry();
                if !registry_ptr.is_null() {
                    trace!(
                        "Worker {}: Released frame {} to local registry (fast path)",
                        self.config.worker_id,
                        frame_idx
                    );
                    (*registry_ptr).release_local(frame_idx);
                    return;
                } else {
                    log::warn!(
                        "Worker {}: Frame registry ptr is null!",
                        self.config.worker_id
                    );
                }
            }
        }

        // SLOW PATH: Only use the cross-thread bus for frames belonging to OTHER workers
        log::trace!(
            "Worker {}: Sending frame {} to owner {} via bus (slow path)",
            self.config.worker_id,
            frame_idx,
            owner
        );
        if self.bus.bus_senders[owner].push(frame_idx).is_err() {
            log::error!(
                "🔥 CRITICAL: return queue full, frame {} leaked!",
                frame_idx
            );
        }
    }

    fn determine_owner(&self, frame_idx: u32) -> usize {
        for partition in &self.bus.umem_partitions {
            if partition.owns_encoded(frame_idx) {
                return partition.worker_id;
            }
        }
        // Fallback: should never happen with a correctly configured system
        log::warn!(
            "determine_owner: no partition owns frame_idx={}, defaulting to worker 0",
            frame_idx
        );
        0
    }
}
impl Drop for XSocketRx {
    fn drop(&mut self) {
        log::debug!(
            "RUST DROP: XSocketRx (Queue {}) dropping after {} receive calls. Refs remaining -> FQ: {}, CQ: {}, RX: {}, TX: {}",
            self.config.queue_id,
            self.receive_calls,
            Arc::strong_count(&self.fill_queue),
            Arc::strong_count(&self._comp_queue_keepalive),
            Arc::strong_count(&self.rx_queue),
            Arc::strong_count(&self._tx_queue_keepalive)
        );
    }
}

impl Drop for XSocketTx {
    fn drop(&mut self) {
        log::debug!(
            "RUST DROP: XSocketTx (Queue {}) dropping. Refs remaining -> FQ: {}, CQ: {}, RX: {}, TX: {}",
            self.config.queue_id,
            Arc::strong_count(&self._fill_queue_keepalive),
            Arc::strong_count(&self.comp_queue),
            Arc::strong_count(&self._rx_queue_keepalive),
            Arc::strong_count(&self.tx_queue)
        );
    }
}
