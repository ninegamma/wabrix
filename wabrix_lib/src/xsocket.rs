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
use xsk_core::config::SocketConfig;
use xsk_core::config::{BindFlags, Interface, LibxdpFlags, QueueSize, XdpFlags};
use xsk_core::socket::XdpStatistics;
use xsk_core::{CompQueue, FillQueue, FrameDesc, RxQueue, Socket, TxQueue, Umem};

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
    /// Per-socket timeout for userspace poll, in nanoseconds; must be less than one second.
    pub napi_poll_timeout_ns: u64,
    /// Per-socket kernel/NAPI busy-poll duration, in nanoseconds.
    pub busy_poll_ns: u64,
    pub busy_poll_budget: i32,
    /// GRO flush timeout in nanoseconds, used to derive the RX poll interval.
    pub napi_flush_timeout_ns: u64,
    /// Whether this interface requests hardware RX timestamps through XDP metadata.
    pub rx_timestamp: bool,
}

pub struct XSocket {
    umem: Arc<Umem>,
    // Fill/completion rings may be shared across sockets, so they use Arc<Mutex<>>.
    // The RX queue is uniquely owned by the RX half after split.
    fill_queue: Arc<Mutex<FillQueue>>,
    comp_queue: Arc<Mutex<CompQueue>>,
    pub(crate) rx_queue: RxQueue,
    pub(crate) tx_queue: Arc<Mutex<TxQueue>>,
    fd: RawFd,
    // Note: frame_registry removed - will be managed at worker level
    pub bus: Arc<SystemBus>,
    pub config: Arc<XdpSocketConfig>,
}

const MAX_FILL: usize = 2048;
/// Bytes reserved immediately before each RX packet for XDP RX metadata.
const RX_METADATA_HEADROOM: usize = core::mem::size_of::<u64>();
const RX_DIAGNOSTIC_LOG_INTERVAL: u64 = 10_000;
/// Minimum receive batch size to target before returning a short nonempty batch.
const RX_BATCH_TOP_UP_TARGET: usize = 96;
/// Maximum time spent waiting for additional descriptors during top-up.
const RX_BATCH_TOP_UP_WAIT_NS: u64 = 1_000;
const RX_TSC_NS_PER_TICK_SHIFT: u32 = 32;
#[cfg(feature = "profiling")]
const NAPI_DIAGNOSTIC_LOG_INTERVAL: u64 = 100_000;
#[cfg(feature = "profiling")]
const RX_CALL_PATH_COUNT: usize = 8;

#[cfg(feature = "profiling")]
#[derive(Clone, Copy, Default)]
struct RxCallProfile {
    calls: u64,
    packets: u64,
    total_tsc_ticks: u64,
    frames_replenished: u64,
}

#[inline(always)]
fn read_tsc() -> u64 {
    // SAFETY: The dataplane targets x86_64 CPUs with the TSC instruction available.
    unsafe { core::arch::x86_64::_rdtsc() }
}

#[cfg(feature = "profiling")]
fn format_rx_call_paths(profiles: &[RxCallProfile]) -> String {
    use std::fmt::Write as _;

    const PATH_NAMES: [&str; RX_CALL_PATH_COUNT] = [
        "ring/direct/no-refill",
        "ring/direct/refill-attempt",
        "ring/spin/no-refill",
        "ring/spin/refill-attempt",
        "NAPI/no-spin/no-refill",
        "NAPI/no-spin/refill-attempt",
        "NAPI/spin/no-refill",
        "NAPI/spin/refill-attempt",
    ];

    let mut details = String::new();
    for (path, profile) in profiles.iter().enumerate() {
        if profile.calls == 0 {
            continue;
        }
        if !details.is_empty() {
            details.push_str("; ");
        }

        let ticks_per_call = profile.total_tsc_ticks / profile.calls;
        let ticks_per_packet = if profile.packets == 0 {
            0
        } else {
            profile.total_tsc_ticks / profile.packets
        };
        let _ = write!(
            details,
            "{}: {} calls, {} ticks/call, {} ticks/packet, {} frames added",
            PATH_NAMES[path],
            profile.calls,
            ticks_per_call,
            ticks_per_packet,
            profile.frames_replenished
        );
    }
    details
}

/// Split components for independent RX and TX access
/// These DO NOT own the frame registry - it's accessed via thread-local storage
pub struct XSocketRx {
    /// Worker that owns this RX half. This may differ from the TX owner.
    worker_id: usize,
    umem: Arc<Umem>,
    rx_metadata_enabled: bool,
    fill_queue: Arc<Mutex<FillQueue>>,
    rx_queue: RxQueue,
    // THE ANCHOR: Keeps the TX side alive even if the TX node is never built
    _tx_queue_keepalive: Arc<Mutex<TxQueue>>,
    _comp_queue_keepalive: Arc<Mutex<CompQueue>>,
    bus: Arc<SystemBus>,
    frame_owner_lookup: FrameOwnerLookup,
    config: Arc<XdpSocketConfig>,
    scratch_addrs: Vec<u64>,
    scratch_descriptors_recv_batch: Vec<FrameDesc>,
    scratch_frames: Vec<wabrix_base::frame_registry::FrameToken>,
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
    // NAPI polling diagnostics are compiled only for profiling builds. These
    // are local to the RX worker, so no atomics are needed.
    #[cfg(feature = "profiling")]
    napi_poll_attempts: u64,
    #[cfg(feature = "profiling")]
    napi_poll_packets: u64,
    #[cfg(feature = "profiling")]
    napi_poll_available_descriptors: u64,
    #[cfg(feature = "profiling")]
    napi_poll_available_samples: u64,
    #[cfg(feature = "profiling")]
    napi_poll_empty: u64,
    #[cfg(feature = "profiling")]
    max_napi_poll_consume: usize,
    #[cfg(feature = "profiling")]
    max_napi_poll_available: usize,
    /// Number of successful receive calls for each returned packet count.
    #[cfg(feature = "profiling")]
    rx_successful_call_profiles: Vec<RxCallProfile>,
    napi_poll_timeout: rustix::time::Timespec,
    napi_busy_poll: bool,
    /// Q32 fixed-point nanoseconds per TSC tick for the bounded RX top-up wait.
    rx_tsc_ns_per_tick_q32: u64,
    rx_batch_top_up_wait_ticks: u64,
    receive_calls: u64,
    fd: RawFd, // file descriptor of the AF_XDP socket
    // Profiling fields
    pub prof_reclaim_replenish_cycles: u64,
    pub prof_replenish_cycles: u64,
    pub prof_replenish_calls: u64,
    pub prof_consume_cycles: u64,
    pub prof_wakeup_cycles: u64,
    pub prof_rx_batch_top_up_ticks: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_calls: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_target_reached: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_deadline_exits: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_poll_errors: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_packets_added: u64,
    #[cfg(feature = "profiling")]
    rx_batch_top_up_total_ticks: u64,
    pub prof_pdu_batch_cycles: u64,
    pub prof_empty_spin_cycles: u64,
    pub prof_total_packets: u64,
    pub prof_max_replenish_cycles: u64,
    pub prof_monster_spikes: u64,
    pub prof_burst_start: u64,
    pub prof_started: bool,
}

pub struct XSocketTx {
    /// Worker that owns this TX half. This may differ from the RX owner.
    worker_id: usize,
    umem: Arc<Umem>,
    comp_descs_helper: Vec<u64>,
    comp_queue: Arc<Mutex<CompQueue>>,
    tx_queue: Arc<Mutex<TxQueue>>,
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
        initial_addrs: Vec<u64>,
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
        let (tx_q, rx_q, queues_opt) = unsafe {
            Socket::new_with_initial_fill(
                socket_config,
                &umem,
                &iface_handle,
                config.queue_id,
                &initial_addrs,
            )?
        };
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

        let fd = rx_q.fd().as_raw_fd();
        let xdp_fm = Self {
            umem,
            rx_queue: rx_q,
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
        self.rx_queue.fd().xdp_statistics()
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
        let tsc_frequency = try_get_tsc_frequency()
            .unwrap_or(2_500_000_000)
            .max(1);
        let rx_batch_top_up_wait_ticks = (((RX_BATCH_TOP_UP_WAIT_NS as u128)
            * (tsc_frequency as u128))
            / 1_000_000_000)
            .max(1) as u64;
        // Precompute the conversion used before each NAPI poll. This replaces a
        // variable u128 division in the RX loop with a u64 multiply and shift.
        let rx_tsc_ns_per_tick_q32 =
            ((1_000_000_000u64 << RX_TSC_NS_PER_TICK_SHIFT) / tsc_frequency).max(1);
        let napi_poll_target_cycles =
            (((target_us as u128) * (tsc_frequency as u128)) / 1_000_000).max(1) as u64;
        let napi_poll_timeout = rustix::time::Timespec {
            // TOML validation guarantees the timeout is less than one second.
            tv_sec: 0,
            tv_nsec: self.config.napi_poll_timeout_ns as _,
        };
        let napi_busy_poll = self.config.poll_mode == "napi_busy_poll";
        if self.config.poll_mode == "napi_busy_poll" {
            info!(
                "NAPI poll interval for {} queue {}: flush_timeout_ns={}, poll_target_interval_tsc_ticks={}",
                self.config.interface,
                self.config.queue_id,
                self.config.napi_flush_timeout_ns,
                napi_poll_target_cycles,
            );
        }

        let rx = XSocketRx {
            worker_id: self.config.worker_id,
            umem: self.umem.clone(),
            rx_metadata_enabled: self.config.rx_timestamp,
            fill_queue: self.fill_queue.clone(),
            rx_queue: self.rx_queue,
            _tx_queue_keepalive: self.tx_queue.clone(),
            _comp_queue_keepalive: self.comp_queue.clone(),
            bus: self.bus.clone(),
            frame_owner_lookup: self
                .bus
                .frame_owner_lookup(self.config.umem_config.group_id),
            config: self.config.clone(),
            scratch_addrs: vec![0; MAX_FILL],
            scratch_descriptors_recv_batch: vec![FrameDesc::default(); MAX_FILL],
            scratch_frames: vec![
                wabrix_base::frame_registry::FrameToken::new(
                    self.config.umem_config.group_id as u32,
                    0,
                );
                MAX_FILL
            ],
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
            #[cfg(feature = "profiling")]
            napi_poll_attempts: 0,
            #[cfg(feature = "profiling")]
            napi_poll_packets: 0,
            #[cfg(feature = "profiling")]
            napi_poll_available_descriptors: 0,
            #[cfg(feature = "profiling")]
            napi_poll_available_samples: 0,
            #[cfg(feature = "profiling")]
            napi_poll_empty: 0,
            #[cfg(feature = "profiling")]
            max_napi_poll_consume: 0,
            #[cfg(feature = "profiling")]
            max_napi_poll_available: 0,
            #[cfg(feature = "profiling")]
            // Allocate profiling storage at socket setup, never in the RX loop.
            rx_successful_call_profiles: vec![
                RxCallProfile::default();
                (MAX_BATCH_SIZE + 1) * RX_CALL_PATH_COUNT
            ],
            napi_poll_timeout,
            napi_busy_poll,
            rx_tsc_ns_per_tick_q32,
            rx_batch_top_up_wait_ticks,
            receive_calls: 0,
            fd,
            prof_reclaim_replenish_cycles: 0,
            prof_replenish_cycles: 0,
            prof_replenish_calls: 0,
            prof_consume_cycles: 0,
            prof_wakeup_cycles: 0,
            prof_rx_batch_top_up_ticks: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_calls: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_target_reached: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_deadline_exits: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_poll_errors: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_packets_added: 0,
            #[cfg(feature = "profiling")]
            rx_batch_top_up_total_ticks: 0,
            prof_pdu_batch_cycles: 0,
            prof_empty_spin_cycles: 0,
            prof_total_packets: 0,
            prof_max_replenish_cycles: 0,
            prof_monster_spikes: 0,
            prof_burst_start: 0,
            prof_started: false,
        };

        let tx = XSocketTx {
            worker_id: self.config.worker_id,
            umem: self.umem,
            comp_descs_helper: vec![0; 4 * MAX_BATCH_SIZE],
            comp_queue: self.comp_queue,
            tx_queue: self.tx_queue,
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
use wabrix_base::frame_registry::{get_tsc_frequency, get_worker_frame_registry, FrameToken};
use wabrix_base::system::{rdtsc_us, try_get_tsc_frequency};
use wabrix_base::{FrameOwnerLookup, RuntimeUmemConfig, SystemBus};

impl XSocketRx {
    /// Rebind this half to the worker that takes ownership of it.
    pub(crate) fn assign_worker_id(&mut self, worker_id: usize) {
        self.worker_id = worker_id;
    }

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
    #[inline]
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

        if requested_count > MAX_FILL
            && self
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
            let local_idx = self.scratch_frames[i].frame_index();
            // Use a 1-cycle bitwise shift instead of multiplication
            let addr = ((local_idx as u64) << frame_size_log2) + headroom;
            // Write directly to our persistent struct buffer
            self.scratch_addrs[i] = addr;
        }

        #[cfg(feature = "profiling")]
        let t3 = unsafe { std::arch::x86_64::_rdtsc() };

        // Produce unconditionally first
        let produced = unsafe { fq.produce(&self.scratch_addrs[..actual_batch]) };

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

        // Return every frame that was allocated but not submitted. This also
        // restores the whole batch when the Fill Queue produces zero frames.
        if produced < actual_batch {
            for &frame_token in &self.scratch_frames[produced..actual_batch] {
                trace!(
                    "replenish_fill_queue: Returning frame token {} to local stash",
                    frame_token
                );
                frame_registry.release_local(frame_token);
            }
        }

        if produced > 0 {
            log::trace!(
                "Worker {}: Replenished {} frames to {} queue {} Fill Ring, free frames remaining: {}",
                self.worker_id,
                produced,
                self.config.interface,
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

                // Only print if the spike is absurdly high (e.g., > 50,000 TSC ticks).
                log::trace!(
                    "MONSTER SPIKE DETECTED on socket {}: Replenish of {} frames took {} TSC ticks!",
                    self.source_id(),
                    produced,
                    duration
                );
                log::trace!(
                    "\n=== Replenish X-Ray (TSC Ticks) ===\n\
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
        #[cfg(feature = "profiling")]
        let mut fill_replenish_attempted = false;
        #[cfg(feature = "profiling")]
        let mut frames_replenished = 0usize;
        #[cfg(feature = "profiling")]
        let mut used_napi_poll = false;
        #[cfg(feature = "profiling")]
        let mut used_local_spin = false;

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
        // Cross-thread communication is expensive. Drain periodically by receive
        // call count, which continues advancing while packets are arriving, or
        // immediately when the worker is dangerously low on free frames.
        let total_free_frames = frame_registry.total_free_frames();
        let low_free_frames = total_free_frames < 8192;
        if self.receive_calls.is_multiple_of(256) || low_free_frames {
            if low_free_frames
                && self
                    .receive_calls
                    .is_multiple_of(RX_DIAGNOSTIC_LOG_INTERVAL)
            {
                log::warn!("total_free_frames is too low: {}", total_free_frames);
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
            #[cfg(feature = "profiling")]
            {
                fill_replenish_attempted = true;
            }
            let produced = self.replenish_fill_queue(self.fill_deficit);
            #[cfg(feature = "profiling")]
            {
                frames_replenished = produced;
            }
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
        // This RX ring has a single owner, so consume it directly without a mutex.
        let mut rcvd = unsafe {
            self.rx_queue
                .consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull])
        };

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
        // Demand-Driven NAPI Polling
        if rcvd == 0 {
            self.empty_spins += 1;
            // LOCAL SPIN: Allow a brief window to catch fast-arriving packets
            // completely locklessly without wasting cycles on a syscall.
            if self.empty_spins < 64 && self.napi_busy_poll {
                #[cfg(feature = "profiling")]
                {
                    used_local_spin = true;
                }
                // Yield this tiny micro-window to the CPU pipeline
                core::hint::spin_loop();
                rcvd = unsafe {
                    self.rx_queue
                        .consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull])
                };
            }

            // Fallback to NAPI poll syscall ONLY if local spinning exhausts
            if rcvd == 0 && self.napi_busy_poll {
                #[cfg(feature = "profiling")]
                {
                    used_napi_poll = true;
                    self.napi_poll_attempts += 1;
                }

                // Hand control to the kernel to spin safely on the hardware ring.
                self.poll_for_napi_packets(Some(&self.napi_poll_timeout))?;

                // Read the producer index after poll() returns, before consuming
                // descriptors, so this records the RX backlog delivered by NAPI.
                #[cfg(feature = "profiling")]
                {
                    let available_after_poll = self.rx_queue.nb_avail_exact() as usize;
                    self.napi_poll_available_descriptors += available_after_poll as u64;
                    if available_after_poll > 0 {
                        self.napi_poll_available_samples += 1;
                    }
                    self.max_napi_poll_available =
                        self.max_napi_poll_available.max(available_after_poll);
                }

                let newly_received = unsafe {
                    self.rx_queue
                        .consume(&mut self.scratch_descriptors_recv_batch[rcvd..max_to_pull])
                };

                #[cfg(feature = "profiling")]
                {
                    if newly_received == 0 {
                        self.napi_poll_empty += 1;
                    }

                    self.max_napi_poll_consume = self.max_napi_poll_consume.max(newly_received);
                }
                rcvd += newly_received;
                #[cfg(feature = "profiling")]
                {
                    self.napi_poll_packets += newly_received as u64;
                }
            }

            // Legacy driver wakeup fallback if empty spins persist
            if rcvd == 0 {
                if self.prof_started && self.empty_spins >= 51200 {
                    self.prof_started = false;
                    let now = unsafe { core::arch::x86_64::_rdtsc() };
                    log::debug!(
                        "**** Traffic burst ended after ~ {} TSC ticks ({} us)",
                        (now - self.prof_burst_start).separated_string(),
                        (((now - self.prof_burst_start) as f64
                            * (1_000_000.0 / get_tsc_frequency() as f64))
                            as u64)
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
                            self.rx_queue
                                .consume(&mut self.scratch_descriptors_recv_batch[..max_to_pull])
                        };
                    }
                }

                #[cfg(feature = "profiling")]
                {
                    if log::log_enabled!(log::Level::Debug)
                        && self.prof_started
                        && rcvd > 0
                        && self
                            .napi_poll_attempts
                            .is_multiple_of(NAPI_DIAGNOSTIC_LOG_INTERVAL)
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
        }

        // End the wakeup stage before any optional short-batch waiting so stage
        // timings remain additive in the periodic RX cycle breakdown.
        #[cfg(feature = "profiling")]
        let t3 = read_tsc();
        #[cfg(feature = "profiling")]
        if self.prof_started {
            self.prof_wakeup_cycles += t3 - t2;
        }

        // When a short batch is already available, briefly wait for more descriptors
        // before returning it. This amortizes fixed RX work across larger batches.
        if rcvd > 0 {
            // `max_to_pull` is bounded by the caller's configured batch_size
            // (`max_count`), so top-up never targets a larger batch than requested.
            let target = RX_BATCH_TOP_UP_TARGET.min(max_to_pull);
            if rcvd < target {
                #[cfg(feature = "profiling")]
                let packets_before_top_up = rcvd;
                let top_up_start = read_tsc();
                #[cfg(feature = "profiling")]
                let mut top_up_poll_error = false;

                loop {
                    let elapsed_ticks = read_tsc().wrapping_sub(top_up_start);
                    if elapsed_ticks >= self.rx_batch_top_up_wait_ticks {
                        break;
                    }

                    if self.napi_busy_poll {
                        let remaining_ticks =
                            self.rx_batch_top_up_wait_ticks - elapsed_ticks;
                        // remaining_ticks is bounded by the top-up budget, so
                        // this Q32 product fits in u64 and avoids hot-path division.
                        let remaining_ns = (remaining_ticks * self.rx_tsc_ns_per_tick_q32)
                            >> RX_TSC_NS_PER_TICK_SHIFT;
                        let poll_timeout_ns =
                            remaining_ns.min(self.napi_poll_timeout.tv_nsec as u64);
                        if poll_timeout_ns == 0 {
                            break;
                        }

                        #[cfg(feature = "profiling")]
                        {
                            used_napi_poll = true;
                            self.napi_poll_attempts += 1;
                        }

                        let poll_timeout = rustix::time::Timespec {
                            // Both the remaining top-up budget and configured timeout
                            // are below one second, so this value fits in tv_nsec.
                            tv_sec: 0,
                            tv_nsec: poll_timeout_ns as _,
                        };
                        if self
                            .poll_for_napi_packets(Some(&poll_timeout))
                            .is_err()
                        {
                            #[cfg(feature = "profiling")]
                            {
                                top_up_poll_error = true;
                            }
                            break;
                        }

                        #[cfg(feature = "profiling")]
                        {
                            let available_after_poll =
                                self.rx_queue.nb_avail_exact() as usize;
                            self.napi_poll_available_descriptors +=
                                available_after_poll as u64;
                            if available_after_poll > 0 {
                                self.napi_poll_available_samples += 1;
                            }
                            self.max_napi_poll_available =
                                self.max_napi_poll_available.max(available_after_poll);
                        }
                    } else {
                        #[cfg(feature = "profiling")]
                        {
                            used_local_spin = true;
                        }
                        core::hint::spin_loop();
                    }

                    // SAFETY: The RX ring is single-owner, and this slice starts after the
                    // descriptors already consumed and ends at the caller-provided capacity.
                    let newly_received = unsafe {
                        self.rx_queue.consume(
                            &mut self.scratch_descriptors_recv_batch[rcvd..max_to_pull],
                        )
                    };
                    rcvd += newly_received;

                    #[cfg(feature = "profiling")]
                    if self.napi_busy_poll {
                        if newly_received == 0 {
                            self.napi_poll_empty += 1;
                        }
                        self.max_napi_poll_consume =
                            self.max_napi_poll_consume.max(newly_received);
                        self.napi_poll_packets += newly_received as u64;
                    }

                    if rcvd >= target {
                        break;
                    }
                }

                #[cfg(feature = "profiling")]
                let top_up_ticks = read_tsc().wrapping_sub(top_up_start);
                #[cfg(feature = "profiling")]
                if self.prof_started {
                    self.prof_rx_batch_top_up_ticks += top_up_ticks;
                }

                #[cfg(feature = "profiling")]
                {
                    self.rx_batch_top_up_total_ticks += top_up_ticks;
                    self.rx_batch_top_up_calls += 1;
                    self.rx_batch_top_up_packets_added +=
                        (rcvd - packets_before_top_up) as u64;
                    if rcvd >= target {
                        self.rx_batch_top_up_target_reached += 1;
                    } else if top_up_ticks >= self.rx_batch_top_up_wait_ticks {
                        self.rx_batch_top_up_deadline_exits += 1;
                    }
                    if top_up_poll_error {
                        self.rx_batch_top_up_poll_errors += 1;
                    }
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
                self.napi_poll_available_descriptors = 0;
                self.napi_poll_available_samples = 0;
                self.napi_poll_empty = 0;
                self.max_napi_poll_consume = 0;
                self.max_napi_poll_available = 0;
                self.empty_spins = 0;
            }

            self.fill_deficit += rcvd; // We owe the FILL_RING this many frames

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
        // ⏱️ END STAGE 3

        self.total_received += rcvd;

        #[cfg(feature = "profiling")]
        {
            if log::log_enabled!(log::Level::Debug)
                && self.prof_started
                && self.napi_poll_attempts > 0
                && self
                    .napi_poll_attempts
                    .is_multiple_of(NAPI_DIAGNOSTIC_LOG_INTERVAL)
            {
                let average_napi_poll_consume =
                    self.napi_poll_packets as f64 / self.napi_poll_attempts as f64;
                let average_napi_poll_available = if self.napi_poll_available_samples > 0 {
                    self.napi_poll_available_descriptors as f64
                        / self.napi_poll_available_samples as f64
                } else {
                    0.0
                };
                debug!(
                    "{}: NAPI poll diagnostics: attempts={}, available_descriptors={}, average_available_after_poll={:.2}, max_available_after_poll={}, packets={}, average_consume_after_poll={:.2}, empty={}, max_consume_after_poll={}, receive_calls={}, total_rx={}, rcvd= {}",
                    self.source_id(),
                    self.napi_poll_attempts.separated_string(),
                    self.napi_poll_available_descriptors.separated_string(),
                    average_napi_poll_available,
                    self.max_napi_poll_available,
                    self.napi_poll_packets.separated_string(),
                    average_napi_poll_consume,
                    self.napi_poll_empty.separated_string(),
                    self.max_napi_poll_consume,
                    self.receive_calls.separated_string(),
                    (self.total_received + rcvd).separated_string(),
                    rcvd
                    );
            }
        }

        /*
        // Emit a periodic summary rather than logging every normal refill.
        // This gives enough information to correlate software drops with FQ
        // starvation while keeping the packet path quiet.
        let received = self.total_received as u64;
        if received >= self.last_diagnostic_received + 10_000_000 {
            self.last_diagnostic_received = received;
            let xdp_stats = self.rx_queue.fd().xdp_statistics();
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
        let umem_group_id = self.config.umem_config.group_id;
        // A shared receive timestamp per batch avoids one TSC read per descriptor.
        let receive_tsc = unsafe { core::arch::x86_64::_rdtsc() };

        // This guarantees zero bounds-checking overhead
        let received_descriptors = &self.scratch_descriptors_recv_batch[..rcvd];
        for desc in received_descriptors {
            let local_idx = (desc.addr() as u32) >> frame_size_log2; // local_idx is wrt. current Umem
            let Some(frame_owner) = self
                .frame_owner_lookup
                .worker_for_local_frame_idx(local_idx)
            else {
                log::warn!(
                    "receive_into_batch: no frame token for UMEM {}, local frame {}",
                    umem_group_id,
                    local_idx
                );
                continue;
            };
            // The worker owning the RX ring is not necessarily the owner of
            // this frame. The initial Fill Queue may contain frames supplied
            // by another worker, and frames returned through the shared bus
            // retain their original partition ownership.
            let frame_owner = frame_owner as u8;

            let mut pdu = Pdu::new(
                *desc,
                umem_group_id as u32,
                frame_owner,
                bus_ptr,
                frame_size_log2,
            );

            // XDP writes the hardware RX timestamp into the UMEM headroom
            // immediately before packet data. A zero value means the driver
            // did not provide a timestamp for this packet.
            let hardware_timestamp =
                if self.rx_metadata_enabled && desc.addr() >= RX_METADATA_HEADROOM as u64 {
                    let timestamp_ptr = unsafe {
                        self.umem
                            .as_ptr()
                            .add(desc.addr() as usize - RX_METADATA_HEADROOM)
                            as *const u64
                    };
                    unsafe { timestamp_ptr.read_unaligned() }
                } else {
                    0
                };
            pdu.w.set_rx_timestamp(hardware_timestamp);
            // Keep the existing host-TSC timestamp for latency calculations;
            // hardware RX time is exposed separately through rx_timestamp().
            pdu.w.set_timestamp(receive_tsc);

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
            if log::log_enabled!(log::Level::Debug)
                && self.prof_started
                && self.prof_total_packets >= PRINT_INTERVAL_PACKETS
            {
                let total = self.prof_total_packets;

                // Calculate cycles per packet for each stage
                let s1_cpp = self.prof_reclaim_replenish_cycles / total;
                let s2_cpp = self.prof_consume_cycles / total;
                let s3_cpp = self.prof_wakeup_cycles / total;
                let s4_cpp = self.prof_rx_batch_top_up_ticks / total;
                let s5_cpp = self.prof_pdu_batch_cycles / total;
                let total_cpp = s1_cpp + s2_cpp + s3_cpp + s4_cpp + s5_cpp;

                log::debug!(
                    "\n=== TSC Tick Breakdown (Per Packet Average over {} pkts) ===\n\
                     1. Reclaim & Replenish : {:>4} TSC ticks\n\
                     2. Lockless Consume    : {:>4} TSC ticks\n\
                     3. Wakeup Logic        : {:>4} TSC ticks\n\
                     4. RX Batch Top-up     : {:>4} TSC ticks\n\
                     5. PDU Gen & Batch Push: {:>4} TSC ticks\n\
                     --------------------------------------------------\n\
                     Total Active RX Cost   : {:>4} TSC ticks/packet\n\
                     Total Empty Spins Cost : {:>10} TSC ticks (Total for {} packets)\n\
                     ==================================================",
                    total.separated_string(),
                    s1_cpp,
                    s2_cpp,
                    s3_cpp,
                    s4_cpp,
                    s5_cpp,
                    total_cpp,
                    self.prof_empty_spin_cycles.separated_string(),
                    total.separated_string()
                );

                log::debug!(
                    "{}: Replenish diagnostics: monster_spikes={}, max_replenish_tsc_ticks={}",
                    self.source_id(),
                    self.prof_monster_spikes.separated_string(),
                    self.prof_max_replenish_cycles.separated_string(),
                );

                // Reset counters
                self.prof_reclaim_replenish_cycles = 0;
                self.prof_consume_cycles = 0;
                self.prof_wakeup_cycles = 0;
                self.prof_rx_batch_top_up_ticks = 0;
                self.prof_pdu_batch_cycles = 0;
                self.prof_empty_spin_cycles = 0;
                self.prof_total_packets = 0;
                self.prof_max_replenish_cycles = 0;
                self.prof_monster_spikes = 0;
            }
        }

        #[cfg(feature = "profiling")]
        {
            // Time reclaim through packet-to-PDU batching; exclude the accounting update itself.
            let call_cycles = read_tsc() - t0;
            // Path index bits: NAPI poll, local spin, then fill-ring replenish attempt.
            let path = (usize::from(used_napi_poll) << 2)
                | (usize::from(used_local_spin) << 1)
                | usize::from(fill_replenish_attempted);
            // One worker-local update captures batching and path costs without locks,
            // allocation, or logging in the packet loop.
            if let Some(profile) = self
                .rx_successful_call_profiles
                .get_mut(rcvd * RX_CALL_PATH_COUNT + path)
            {
                profile.calls += 1;
                profile.packets += rcvd as u64;
                profile.total_tsc_ticks += call_cycles;
                profile.frames_replenished += frames_replenished as u64;
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
impl Drop for XSocketRx {
    fn drop(&mut self) {
        #[cfg(feature = "profiling")]
        {
            let target_hits_percent = if self.rx_batch_top_up_calls == 0 {
                0.0
            } else {
                self.rx_batch_top_up_target_reached as f64 * 100.0
                    / self.rx_batch_top_up_calls as f64
            };
            let packets_added_per_attempt = if self.rx_batch_top_up_calls == 0 {
                0.0
            } else {
                self.rx_batch_top_up_packets_added as f64
                    / self.rx_batch_top_up_calls as f64
            };
            let ticks_per_attempt = if self.rx_batch_top_up_calls == 0 {
                0
            } else {
                self.rx_batch_top_up_total_ticks / self.rx_batch_top_up_calls
            };
            info!(
                "{}: RX batch top-up: target={} packets, wait_limit={} ns, attempts={}, target_reached={} ({:.1}%), deadline_exits={}, poll_errors={}, packets_added={}, avg_added_per_attempt={:.2}, total={} TSC ticks, avg={} TSC ticks/attempt",
                self.source_id(),
                RX_BATCH_TOP_UP_TARGET,
                RX_BATCH_TOP_UP_WAIT_NS,
                self.rx_batch_top_up_calls.separated_string(),
                self.rx_batch_top_up_target_reached.separated_string(),
                target_hits_percent,
                self.rx_batch_top_up_deadline_exits.separated_string(),
                self.rx_batch_top_up_poll_errors.separated_string(),
                self.rx_batch_top_up_packets_added.separated_string(),
                packets_added_per_attempt,
                self.rx_batch_top_up_total_ticks.separated_string(),
                ticks_per_attempt
            );

            let mut calls_by_batch = [0u64; MAX_BATCH_SIZE + 1];
            let mut totals_by_path = [RxCallProfile::default(); RX_CALL_PATH_COUNT];
            let mut successful_calls = 0u64;
            let mut total_packets = 0u64;
            let mut total_tsc_ticks = 0u64;

            for batch_size in 1..=MAX_BATCH_SIZE {
                let profile_offset = batch_size * RX_CALL_PATH_COUNT;
                for path in 0..RX_CALL_PATH_COUNT {
                    let profile = self.rx_successful_call_profiles[profile_offset + path];
                    calls_by_batch[batch_size] += profile.calls;
                    successful_calls += profile.calls;
                    total_packets += profile.packets;
                    total_tsc_ticks += profile.total_tsc_ticks;
                    totals_by_path[path].calls += profile.calls;
                    totals_by_path[path].packets += profile.packets;
                    totals_by_path[path].total_tsc_ticks += profile.total_tsc_ticks;
                    totals_by_path[path].frames_replenished += profile.frames_replenished;
                }
            }

            info!(
                "{}: RX successful-call packet distribution: {} calls, {} packets; individual sizes below 0.5% are merged into adjacent ranges",
                self.source_id(),
                successful_calls,
                self.total_received
            );

            let mut packet_count = if successful_calls == 0 {
                MAX_BATCH_SIZE + 1
            } else {
                1
            };
            while packet_count <= MAX_BATCH_SIZE {
                let calls = calls_by_batch[packet_count];
                if (calls as f64) * 200.0 < successful_calls as f64 {
                    let range_start = packet_count;
                    let mut range_calls = 0u64;
                    while packet_count <= MAX_BATCH_SIZE
                        && (calls_by_batch[packet_count] as f64) * 200.0
                            < successful_calls as f64
                    {
                        range_calls += calls_by_batch[packet_count];
                        packet_count += 1;
                    }

                    if range_calls > 0 {
                        let range_end = packet_count - 1;
                        let percent = range_calls as f64 * 100.0 / successful_calls as f64;
                        if range_start == range_end {
                            info!(
                                "{}: RX batch size {}: {} calls ({:.1}%)",
                                self.source_id(),
                                range_start,
                                range_calls.separated_string(),
                                percent
                            );
                        } else {
                            info!(
                                "{}: RX batch sizes {}–{}: {} calls ({:.1}%)",
                                self.source_id(),
                                range_start,
                                range_end,
                                range_calls.separated_string(),
                                percent
                            );
                        }
                    }
                } else {
                    let percent = calls as f64 * 100.0 / successful_calls as f64;
                    info!(
                        "{}: RX batch size {}: {} calls ({:.1}%)",
                        self.source_id(),
                        packet_count,
                        calls.separated_string(),
                        percent
                    );
                    packet_count += 1;
                }
            }

            let ticks_per_call = if successful_calls == 0 {
                0
            } else {
                total_tsc_ticks / successful_calls
            };
            let ticks_per_packet = if total_packets == 0 {
                0
            } else {
                total_tsc_ticks / total_packets
            };
            debug!(
                "{}: RX successful-call timing: {} calls, {} packets, {} TSC ticks/call, {} TSC ticks/packet; paths=[{}]",
                self.source_id(),
                successful_calls,
                total_packets,
                ticks_per_call,
                ticks_per_packet,
                format_rx_call_paths(&totals_by_path)
            );

            let mut top_batches = calls_by_batch
                .iter()
                .enumerate()
                .skip(1)
                .filter_map(|(batch_size, &calls)| (calls > 0).then_some((batch_size, calls)))
                .collect::<Vec<_>>();
            top_batches.sort_unstable_by(|left, right| right.1.cmp(&left.1));
            top_batches.truncate(12);

            let mut is_top_batch = [false; MAX_BATCH_SIZE + 1];
            for &(batch_size, _) in &top_batches {
                is_top_batch[batch_size] = true;
            }

            let mut other_sizes_by_path = [RxCallProfile::default(); RX_CALL_PATH_COUNT];
            for batch_size in 1..=MAX_BATCH_SIZE {
                if is_top_batch[batch_size] {
                    continue;
                }
                let profile_offset = batch_size * RX_CALL_PATH_COUNT;
                for path in 0..RX_CALL_PATH_COUNT {
                    let profile = self.rx_successful_call_profiles[profile_offset + path];
                    other_sizes_by_path[path].calls += profile.calls;
                    other_sizes_by_path[path].packets += profile.packets;
                    other_sizes_by_path[path].total_tsc_ticks += profile.total_tsc_ticks;
                    other_sizes_by_path[path].frames_replenished += profile.frames_replenished;
                }
            }

            for (batch_size, calls) in top_batches {
                let profile_offset = batch_size * RX_CALL_PATH_COUNT;
                let paths = &self.rx_successful_call_profiles
                    [profile_offset..profile_offset + RX_CALL_PATH_COUNT];
                let total_tsc_ticks = paths.iter().map(|profile| profile.total_tsc_ticks).sum::<u64>();
                let packets = paths.iter().map(|profile| profile.packets).sum::<u64>();
                let calls_percent = if successful_calls == 0 {
                    0.0
                } else {
                    calls as f64 * 100.0 / successful_calls as f64
                };
                let ticks_per_call = total_tsc_ticks / calls;
                let ticks_per_packet = if packets == 0 {
                    0
                } else {
                    total_tsc_ticks / packets
                };

                debug!(
                    "{}: RX batch={} calls={} ({:.1}%), {} TSC ticks/call, {} TSC ticks/packet; paths=[{}]",
                    self.source_id(),
                    batch_size,
                    calls,
                    calls_percent,
                    ticks_per_call,
                    ticks_per_packet,
                    format_rx_call_paths(paths)
                );
            }

            debug!(
                "{}: RX other batch sizes timing: paths=[{}]",
                self.source_id(),
                format_rx_call_paths(&other_sizes_by_path)
            );
        }

        log::debug!(
            "RUST DROP: XSocketRx (Queue {}) dropping after {} receive calls. Refs remaining -> FQ: {}, CQ: {}, TX: {}",
            self.config.queue_id,
            self.receive_calls,
            Arc::strong_count(&self.fill_queue),
            Arc::strong_count(&self._comp_queue_keepalive),
            Arc::strong_count(&self._tx_queue_keepalive)
        );
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
