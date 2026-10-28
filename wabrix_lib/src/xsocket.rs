// src/xsocket.rs

use crate::probe_kernel_fds;
use log::{debug, info};
use spin::Mutex;
use std::ffi::CString;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use wabrix_api::MAX_BATCH_SIZE;
use wabrix_base::system::try_get_tsc_frequency;
use wabrix_base::{RuntimeUmemConfig, SystemBus};
use xsk_core::config::SocketConfig;
use xsk_core::config::{BindFlags, Interface, LibxdpFlags, QueueSize, XdpFlags};
use xsk_core::socket::XdpStatistics;
use xsk_core::{CompQueue, FillQueue, FrameDesc, RxQueue, Socket, TxQueue, Umem};

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
mod rx;
mod tx;

pub use rx::XSocketRx;
#[cfg(feature = "profiling")]
use rx::{RxCallProfile, RX_CALL_PATH_COUNT};
use rx::{RX_BATCH_TOP_UP_WAIT_NS, RX_TSC_NS_PER_TICK_SHIFT};
pub use tx::XSocketTx;

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
        let tsc_frequency = try_get_tsc_frequency().unwrap_or(2_500_000_000).max(1);
        let rx_batch_top_up_wait_ticks =
            (((RX_BATCH_TOP_UP_WAIT_NS as u128) * (tsc_frequency as u128)) / 1_000_000_000).max(1)
                as u64;
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
