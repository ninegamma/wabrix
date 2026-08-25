use crate::wasm_manager::{MyHostState, WasmManager};
use core::mem::size_of;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use wabrix_api::helpers::parse_suffixed_number;
use wabrix_api::{
    BatchMetadata, MAX_BATCH_SIZE, TIMER_KIND_NONE, TIMER_KIND_SYN, TIMER_KIND_TIMEOUT,
};
use wasmtime::component::{bindgen, ResourceTable};
use wasmtime::Store;
use wasmtime_wasi::WasiCtxBuilder;

/// Host-side view of `BatchMetadata` that uses `Pdu` instead of `Pdu0`.
/// `Pdu` and `Pdu0` are layout-identical (64 bytes: 56-byte `WPdu` core + 8-byte tail);
/// plugins never read/write the last 8 bytes, so the cast is safe.
#[repr(C)]
struct HostBatch {
    pdus: [Pdu; MAX_BATCH_SIZE],
    actions: [u32; MAX_BATCH_SIZE],
}
const _: () = assert!(size_of::<HostBatch>() == size_of::<BatchMetadata>());
use wabrix_base::frame_registry::get_worker_frame_registry;
use wabrix_base::interface::batch::Batch;
use wabrix_base::interface::Pdu;
use wabrix_base::system::PlatformData;
use wabrix_base::telemetry::TelemetryMetrics;
use wabrix_base::{ControlEvent, Directive, Node, NodeId};

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../wabrix_api/wit/wabrix.wit",
    world: "engine-plugin",
    // async: true,
});

pub struct WasmEngine {
    id: NodeId,
    #[cfg(feature = "runtime-diagnostics")]
    pipeline_name: String,
    #[cfg(feature = "runtime-diagnostics")]
    worker_id: usize,
    #[cfg(feature = "runtime-diagnostics")]
    worker_index: usize,
    /// Wasmtime state: Store holds the UMEM handle via MyHostState
    store: Store<MyHostState>,
    /// The generated bindings struct (named after the 'world' in CamelCase)
    bindings: EnginePlugin,
    /// Index corresponds to the Route ID returned by Wasm
    routing_table: Vec<NodeId>,
    /// Control event receiver channel
    processed_packets: u64,
    /// helper for reducing telemetry fetching frequency
    batch_count: u32,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    telemetry_metrics: TelemetryMetrics,
    /// Byte offset from Umem base where WASM address 0 begins for this instance.
    partition_start_bytes: u32,
    /// UMEM-relative byte offset of this instance's BatchMetadata mailbox.
    mailbox_offset: u32,
    /// The Umem this node is bound to. Every incoming PDU must carry this id.
    umem_id: u8,
    /// Set permanently true once call_get_telemetry() returns non-empty stats.
    /// Disables in-band telemetry extraction which lacks TELEMETRY_FLAG guarding.
    supports_oob_telemetry: bool,
    /// Set permanently true after WASM returns a malformed response.
    /// All further WASM calls are skipped to prevent SIGSEGV from bad UMEM writes.
    poisoned: bool,
    /// Timer-path diagnostics. These are cumulative and emitted periodically.
    timer_requested_frames: u64,
    timer_allocated_frames: u64,
    timer_routed_frames: u64,
    timer_retained_frames: u64,
    timer_allocation_shortfalls: u64,
    timer_batch_mismatches: u64,
    mailbox_conservation_failures: u64,
    timer_route_counts: Vec<u64>,
    /// Route carrying timer-generated TCP transmissions (shared_tx_sink in tcp2.toml).
    timer_syn_route_index: usize,
    tsc_frequency: u64,
    last_timer_diagnostics_tsc: Option<u64>,
    last_timer_diagnostics_syn_count: u64,
    timer_diagnostics_enabled: bool,
    timer_diagnostics_active: bool,
}

impl WasmEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        pipeline_name: String,
        worker_id: usize,
        worker_index: usize,
        manager: &WasmManager,
        wasm_file_path: &str,
        routing_table: Vec<NodeId>, // Accept any number of routes
        telemetry_metrics: TelemetryMetrics,
        partition_start_bytes: u32,
        umem_id: u8,
        wasm_data: HashMap<String, String>,
        platform_data: &PlatformData,
        mailbox_offset: u32,
    ) -> Result<Self, String> {
        #[cfg(not(feature = "runtime-diagnostics"))]
        let _ = (&pipeline_name, worker_id, worker_index);

        let timer_diagnostics_enabled = wasm_data
            .get("syn_rate")
            .and_then(|rate| parse_suffixed_number(rate).ok())
            .is_some_and(|rate| rate > 0);

        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component = manager
            .get_component(wasm_file_path)
            .expect("Failed to load Wasm component");

        // 2. Build the partition-specific UmemHandle for this instance.
        //    WASM address 0 will map to `umem_base + partition_start_bytes`.
        //    The window extends to the end of the full Umem.
        let partition_handle = manager.make_partition_handle(partition_start_bytes);
        log::info!(
            "NODE {}: Partition start bytes: {}",
            id,
            partition_start_bytes
        );

        // 3. Pre-load the partition handle into the shared XdpMemoryCreator so
        //    that Wasmtime picks it up when it calls new_memory() during instantiate().
        manager.set_instantiation_handle(partition_handle.clone());

        // 4. Initialize the WASI Context for THIS specific node
        let wasi_ctx = WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .build();

        // 5. Create the Store
        // This is where the magic happens: Wasmtime calls our `XdpMemoryCreator`
        // in the background to hand out the zero-copy UMEM handle.
        let mut store = Store::new(
            manager.engine(),
            MyHostState {
                ctx: wasi_ctx,
                table: ResourceTable::new(),
                umem: partition_handle, // Host-side state also sees the partition window
            },
        );

        // 6. Instantiate the plugin.
        //    Wasmtime calls XdpMemoryCreator::new_memory() here, which returns
        //    the partition handle we pre-loaded in step 3.
        //    We use the Manager's shared Linker, which already has WASI registered.
        let bindings = EnginePlugin::instantiate(&mut store, &component, manager.linker())
            .expect("Failed to instantiate Wasm component");

        if !wasm_data.is_empty() {
            let data: Vec<(String, String)> = wasm_data.into_iter().collect();
            let init_result = bindings.wabrix_plugins_packet_engine().call_init_data(
                &mut store,
                &data,
                platform_data.tsc_frequency,
            );

            match init_result {
                Ok(true) => {
                    // Plugin accepted the configuration and initialized successfully
                }
                Ok(false) => {
                    // Plugin rejected the configuration due to missing or invalid data
                    return Err(format!(
                        "WasmEngine node {} rejected initialization data. Verify TOML parameters.",
                        id
                    ));
                }
                Err(e) => {
                    // Wasm runtime encountered a trap or fatal execution error during init
                    return Err(format!(
                        "WasmEngine node {} crashed during init-data execution: {}",
                        id, e
                    ));
                }
            }
        }

        let route_count = routing_table.len();
        let timer_syn_route_index = routing_table
            .iter()
            .position(|route| route.id == "shared_tx_sink")
            .unwrap_or(0);

        Ok(Self {
            id,
            #[cfg(feature = "runtime-diagnostics")]
            pipeline_name,
            #[cfg(feature = "runtime-diagnostics")]
            worker_id,
            #[cfg(feature = "runtime-diagnostics")]
            worker_index,
            store,
            bindings,
            routing_table,
            processed_packets: 0,
            batch_count: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            telemetry_metrics,
            partition_start_bytes,
            mailbox_offset,
            umem_id,
            supports_oob_telemetry: false,
            poisoned: false,
            timer_requested_frames: 0,
            timer_allocated_frames: 0,
            timer_routed_frames: 0,
            timer_retained_frames: 0,
            timer_allocation_shortfalls: 0,
            timer_batch_mismatches: 0,
            mailbox_conservation_failures: 0,
            timer_route_counts: vec![0; route_count],
            timer_syn_route_index,
            tsc_frequency: platform_data.tsc_frequency,
            last_timer_diagnostics_tsc: None,
            last_timer_diagnostics_syn_count: 0,
            timer_diagnostics_enabled,
            timer_diagnostics_active: false,
        })
    }

    /// PRIVATE HELPER: Handles WASM execution, Length mutation, Telemetry, and Routing
    #[inline(always)]
    fn execute_wasm_and_route(&mut self, batch: &mut Batch) -> Directive {
        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        // Capture the raw mailbox pointer before mutably borrowing self.store.
        // SAFETY: Control Block frames are excluded from the AF_XDP Fill Queue;
        // this instance's mailbox slice is exclusively owned by this thread
        // during the entire call window (write before call, read after return).
        let mailbox_ptr: *mut HostBatch = {
            let base = self.store.data().umem.ptr;
            // SAFETY: mailbox_offset is within the Control Block, which the host
            // guarantees is backed by allocated UMEM memory.
            unsafe { base.add(self.mailbox_offset as usize) as *mut HostBatch }
        };

        // ==========================================================
        //    THE FAST METADATA COPY (Host Batch -> UMEM Mailbox)
        // ==========================================================
        // Only the WPdu core (56 bytes) is written into each mailbox slot.
        // The last 8 bytes of each Pdu (system_bus: *const SystemBus) must NOT reach
        // WASM-accessible memory: the low 32 bits of a native Linux pointer (~1.5 GB)
        // look like a packet address to the plugin and cause a linear-memory bounds trap.
        //
        // SAFETY: `batch.as_ptr()` points to a `[MaybeUninit<Pdu>; MAX_BATCH_SIZE]` array
        // whose first `len` slots are fully initialized (guaranteed by `Batch::push`).
        // Casting to `*const Pdu` is sound because `MaybeUninit<Pdu>` and `Pdu` share layout.
        // `(*mailbox_ptr).pdus` is inside the Control Block, exclusively owned by this thread
        // for the entire call window (write → WASM call → read).
        unsafe {
            let m = &mut *mailbox_ptr;
            let src = batch.as_ptr() as *const Pdu;
            for i in 0..len {
                m.pdus[i].w = (*src.add(i)).w.clone();
            }
        }

        let status = self
            .bindings
            .wabrix_plugins_packet_engine()
            .call_process_batch(
                &mut self.store,
                len as u32,
                self.partition_start_bytes,
                self.mailbox_offset,
            )
            .unwrap_or_else(|e| {
                log::error!("NODE {}: Wasm execution failed: {}", self.id, e);
                u32::MAX
            });

        if status == u32::MAX {
            batch.clear();
            self.poisoned = true;
            return Directive::Stop;
        }

        if status != len as u32 {
            log::error!(
                "NODE {}: WASM process_batch processed {} frames, expected {}; rejecting batch",
                self.id,
                status,
                len
            );
            batch.clear();
            self.poisoned = true;
            return Directive::Stop;
        }

        // ==========================================================
        // 2. HYBRID TELEMETRY EXTRACTION
        // ==========================================================
        self.batch_count = self.batch_count.wrapping_add(1);

        let control_plane_wants_sync = match &self.telemetry_metrics.force_sync_flag {
            Some(flag) => flag.load(Ordering::Relaxed),
            None => false,
        };

        let is_telemetry_tick = control_plane_wants_sync;
        if control_plane_wants_sync {
            log::debug!("NODE {}: TELEMETRY SYNC REQUESTED", self.id);
        }
        if is_telemetry_tick {
            if let Some(flag) = &self.telemetry_metrics.force_sync_flag {
                flag.store(false, Ordering::Relaxed);
            }
            let out_of_band_stats = self
                .bindings
                .wabrix_plugins_packet_engine()
                .call_get_telemetry(&mut self.store)
                .unwrap_or_else(|e| {
                    log::error!("Failed to call get-telemetry on packet-engine: {}", e);
                    vec![]
                });

            if !out_of_band_stats.is_empty() {
                self.supports_oob_telemetry = true;
                log::debug!(
                    "NODE {}: Extracted {} OOB telemetry packets",
                    self.id,
                    out_of_band_stats.len()
                );
                for (idx, &count) in out_of_band_stats.iter().enumerate() {
                    if idx < self.telemetry_metrics.counters_pkts.len() {
                        self.telemetry_metrics.counters_pkts[idx].store(count, Ordering::Relaxed);
                    }
                }
            }
        }

        // ==========================================
        // 3. MUTATION PHASE — copy plugin-modified WPdu fields back into the host Batch
        // ==========================================
        // Only the WPdu core (56 bytes) is read back from each mailbox slot.
        // This preserves the original system_bus pointer in each batch Pdu — copying the
        // full slot would overwrite it with whatever the plugin left in _do_not_touch_padding.
        //
        // SAFETY: WASM call returned; we have exclusive access to the mailbox again.
        unsafe {
            let m = &*mailbox_ptr;
            let dst = batch.as_mut_ptr() as *mut Pdu;
            for i in 0..len {
                (*dst.add(i)).w = m.pdus[i].w.clone();
            }
        }

        // ==========================================
        // 4. ROUTING PHASE — read actions from mailbox
        // ==========================================
        let num_routes = self.routing_table.len();
        let mut outputs: Vec<Batch> = std::iter::repeat_with(Batch::new)
            .take(num_routes)
            .collect();
        let mut pdu_index = 0;

        batch.distribute(&mut outputs, |pdu| {
            // SAFETY: exclusive access restored after WASM returned.
            let action = unsafe { (*mailbox_ptr).actions[pdu_index] };
            pdu_index += 1;

            if !self.supports_oob_telemetry {
                let telemetry_bucket = ((action >> 16) & 0xFF) as usize;
                if telemetry_bucket < self.telemetry_metrics.counters_pkts.len() {
                    self.telemetry_metrics.counters_pkts[telemetry_bucket]
                        .fetch_add(1, Ordering::Relaxed);
                    self.telemetry_metrics.counters_bytes[telemetry_bucket]
                        .fetch_add(pdu.w.data_len() as u64, Ordering::Relaxed);
                }
            }

            let route_id = (action & 0xFF) as usize;
            if route_id >= num_routes {
                log::warn!("NODE {}: Invalid Route ID {} for Pkt.", self.id, route_id);
                usize::MAX
            } else {
                route_id
            }
        });

        // A mailbox entry must have exactly one owner after routing: either it
        // was moved to one output batch, or it remains in `batch` because the
        // route was invalid / the destination batch was full.  This is the
        // conservation check that catches mailbox/action-length mismatches
        // without confusing an intentionally retained packet with a loss.
        let routed_frames: usize = outputs.iter().map(Batch::len).sum();
        let retained_frames = batch.len();
        if routed_frames.saturating_add(retained_frames) != len || pdu_index != len {
            self.mailbox_conservation_failures =
                self.mailbox_conservation_failures.saturating_add(1);
            log::error!(
                "NODE {}: mailbox conservation failure: input={}, actions_read={}, routed={}, retained={}; rejecting batch",
                self.id,
                len,
                pdu_index,
                routed_frames,
                retained_frames
            );
            batch.clear();
            return Directive::Stop;
        }

        if self.timer_diagnostics_active {
            self.timer_routed_frames += routed_frames as u64;
            self.timer_retained_frames += retained_frames as u64;
            for (route_idx, output_batch) in outputs.iter().enumerate() {
                self.timer_route_counts[route_idx] += output_batch.len() as u64;
            }
        }

        // ==========================================
        // 5. DISPATCH DIRECTIVES
        // ==========================================
        let mut split_branches = Vec::with_capacity(num_routes);
        for (i, output_batch) in outputs.into_iter().enumerate() {
            if !output_batch.is_empty() {
                split_branches.push((self.routing_table[i].clone(), output_batch));
            }
        }

        if split_branches.is_empty() {
            Directive::Stop
        } else {
            Directive::Split(split_branches)
        }
    }
}

impl Node for WasmEngine {
    fn id(&self) -> &NodeId {
        &self.id
    }

    // ----------------------------------------------------
    // STANDARD TRAFFIC PROCESSING
    // ----------------------------------------------------
    fn process(&mut self, batch: &mut Batch) -> Directive {
        if self.poisoned {
            batch.clear();
            return Directive::Stop;
        }

        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        debug_assert!(
            batch.iter().all(|pdu| pdu.w.umem_id() == self.umem_id),
            "NODE {}: PDU from wrong Umem arrived",
            self.id
        );

        // Normal ingress must never inherit a timer classification from a
        // recycled UMEM frame. Timer batches are explicitly marked in
        // `poll_timers` below.
        for pdu in batch.iter_mut() {
            pdu.w.set_timer_kind(TIMER_KIND_NONE);
        }

        self.processed_packets += batch.len() as u64;

        // Hand off to the shared logic
        let directive = self.execute_wasm_and_route(batch);

        #[cfg(feature = "profiling")]
        {
            self.total_cycles += unsafe { std::arch::x86_64::_rdtsc() } - start_cycles;
        }

        directive
    }

    fn requires_timers(&self) -> bool {
        true
    }

    // ----------------------------------------------------
    // TIMER / RETRANSMISSION PROCESSING
    // ----------------------------------------------------
    fn poll_timers(&mut self, batch: &mut Batch, current_tsc: u64) -> Directive {
        if self.poisoned {
            return Directive::Stop;
        }

        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { core::arch::x86_64::_rdtsc() };

        // Evaluate the core logic into a single variable using a block expression
        let directive = {
            let timer_plan = match self
                .bindings
                .wabrix_plugins_packet_engine()
                .call_process_timers(&mut self.store, current_tsc)
            {
                Ok(plan) => plan,
                Err(err) => {
                    log::error!("NODE {}: Error processing poll_timers: {}", self.id, err);
                    return Directive::Stop;
                }
            };

            let timeout_frames_requested = timer_plan.timeout_frames;
            let packet_frames_requested = timer_plan.packet_frames;
            let n_requested_frames =
                timeout_frames_requested.saturating_add(packet_frames_requested);

            if n_requested_frames == 0 {
                // Service force_sync_flag even when idle so final WASM state is
                // captured in host AtomicU64 counters at run end.
                let control_plane_wants_sync = self
                    .telemetry_metrics
                    .force_sync_flag
                    .as_ref()
                    .map(|f| f.load(Ordering::Relaxed))
                    .unwrap_or(false);

                if control_plane_wants_sync {
                    if let Some(flag) = &self.telemetry_metrics.force_sync_flag {
                        flag.store(false, Ordering::Relaxed);
                    }
                    let oob = self
                        .bindings
                        .wabrix_plugins_packet_engine()
                        .call_get_telemetry(&mut self.store)
                        .unwrap_or_default();
                    if !oob.is_empty() {
                        self.supports_oob_telemetry = true;
                        for (idx, &count) in oob.iter().enumerate() {
                            if idx < self.telemetry_metrics.counters_pkts.len() {
                                self.telemetry_metrics.counters_pkts[idx]
                                    .store(count, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Directive::Stop
            } else {
                // 1. Allocate exact memory from the shared FrameRegistry
                let registry = unsafe { &mut *get_worker_frame_registry() };
                let mut scratch: [MaybeUninit<Pdu>; MAX_BATCH_SIZE] =
                    core::array::from_fn(|_| MaybeUninit::uninit());
                if n_requested_frames > MAX_BATCH_SIZE as u32 {
                    self.timer_batch_mismatches = self.timer_batch_mismatches.saturating_add(1);
                    log::error!(
                        "NODE {}: timer requested {} frames, exceeding MAX_BATCH_SIZE {}; rejecting request",
                        self.id,
                        n_requested_frames,
                        MAX_BATCH_SIZE
                    );
                    return Directive::Stop;
                }
                let frames_to_allocate = n_requested_frames as usize;
                self.timer_requested_frames += n_requested_frames as u64;

                let allocated = registry
                    .allocate_pdus_into(self.umem_id as usize, &mut scratch[..frames_to_allocate]);
                self.timer_allocated_frames += allocated as u64;
                if allocated < frames_to_allocate {
                    self.timer_allocation_shortfalls += 1;
                }

                if allocated == 0 {
                    log::warn!("NODE {}: Timer starved of UMEM frames!", self.id);
                    Directive::Stop
                } else {
                    // 2. Load and initialize the freshly allocated PDUs into the Batch
                    batch.clear();
                    // SYN frames are placed first so an UMEM shortfall cannot
                    // consume the SYN reservation with timeout work.
                    let allocated_syn_frames = allocated.min(packet_frames_requested as usize);
                    let allocated_timeout_frames = allocated
                        .saturating_sub(allocated_syn_frames)
                        .min(timeout_frames_requested as usize);

                    for (index, slot) in scratch[..allocated].iter().enumerate() {
                        let mut pdu = unsafe { slot.assume_init_read() };
                        let umem_addr = pdu.umem_addr();
                        let data_ptr = pdu.data_ptr(umem_addr);

                        unsafe {
                            core::ptr::write_bytes(data_ptr, 0, 12);
                            *data_ptr.add(12) = 0x88;
                            *data_ptr.add(13) = 0xB5;
                        }

                        pdu.w.set_data_length(64);
                        let timer_kind = if index < allocated_syn_frames {
                            TIMER_KIND_SYN
                        } else if index < allocated_syn_frames + allocated_timeout_frames {
                            TIMER_KIND_TIMEOUT
                        } else {
                            TIMER_KIND_NONE
                        };
                        pdu.w.set_timer_kind(timer_kind);
                        if batch.push(pdu).is_err() {
                            self.timer_batch_mismatches =
                                self.timer_batch_mismatches.saturating_add(1);
                            log::error!(
                                "NODE {}: timer batch overflow while inserting {} allocated frames",
                                self.id,
                                allocated
                            );
                            batch.clear();
                            return Directive::Stop;
                        }
                    }

                    if batch.len() != allocated {
                        self.timer_batch_mismatches = self.timer_batch_mismatches.saturating_add(1);
                        log::error!(
                            "NODE {}: timer batch length mismatch: allocated={}, batch_len={}",
                            self.id,
                            allocated,
                            batch.len()
                        );
                        batch.clear();
                        return Directive::Stop;
                    }

                    self.processed_packets += allocated as u64;

                    // 3. Hand off to WASM execution (evaluates to Directive)
                    self.timer_diagnostics_active = true;
                    let directive = self.execute_wasm_and_route(batch);
                    self.timer_diagnostics_active = false;
                    directive
                }
            }
        }; // <-- The block ends here, yielding the final Directive

        let should_log_timer_diagnostics = self.timer_diagnostics_enabled
            && (self.last_timer_diagnostics_tsc.is_none()
                || current_tsc
                    .saturating_sub(self.last_timer_diagnostics_tsc.unwrap_or(current_tsc))
                    >= 30 * self.tsc_frequency);

        if should_log_timer_diagnostics {
            // Report elapsed time from the worker's run start, rather than
            // wall-clock epoch. Three decimal places after milliseconds
            // provide microsecond resolution: 1234.567 ms.
            let run_start_tsc = wabrix_base::frame_registry::get_tsc_start();
            let elapsed_tsc_from_start = current_tsc.saturating_sub(run_start_tsc);
            let elapsed_micros = if self.tsc_frequency > 0 {
                (elapsed_tsc_from_start as u128 * 1_000_000) / self.tsc_frequency as u128
            } else {
                0
            };
            let timestamp_ms = elapsed_micros / 1_000;
            let timestamp_ms_fraction = elapsed_micros % 1_000;

            let routed_syns = self
                .timer_route_counts
                .get(self.timer_syn_route_index)
                .copied()
                .unwrap_or(0);
            let (syn_rate_per_second, elapsed_tsc) =
                if let Some(previous_tsc) = self.last_timer_diagnostics_tsc {
                    let elapsed_tsc = current_tsc.saturating_sub(previous_tsc);
                    let elapsed_syns =
                        routed_syns.saturating_sub(self.last_timer_diagnostics_syn_count);
                    if elapsed_tsc > 0 && self.tsc_frequency > 0 {
                        (
                            elapsed_syns as f64 * self.tsc_frequency as f64 / elapsed_tsc as f64,
                            elapsed_tsc,
                        )
                    } else {
                        (0.0, elapsed_tsc)
                    }
                } else {
                    (0.0, 0)
                };
            let elapsed_ms = if self.tsc_frequency > 0 {
                elapsed_tsc as f64 * 1_000.0 / self.tsc_frequency as f64
            } else {
                0.0
            };
            self.last_timer_diagnostics_tsc = Some(current_tsc);
            self.last_timer_diagnostics_syn_count = routed_syns;

            log::debug!(
                "NODE {}: timer diagnostics: timestamp_ms={}.{:03}, routed_syns={}, routed_syn_rate_per_sec={:.3}, elapsed_ms={:.3}",
                self.id,
                timestamp_ms,
                timestamp_ms_fraction,
                routed_syns,
                syn_rate_per_second,
                elapsed_ms,
            );
        }

        // Exactly one exit point for profiling
        #[cfg(feature = "profiling")]
        {
            let elapsed = unsafe { core::arch::x86_64::_rdtsc() } - start_cycles;
            self.total_cycles += elapsed;
        }

        directive
    }

    fn requires_control_events(&self) -> bool {
        true
    }

    fn handle_control_event(&mut self, event: &ControlEvent) {
        if self.poisoned {
            return;
        }
        // We only get here if the PipelineRunner already verified the wildcard matched us!
        tokio::task::block_in_place(|| {
            let _ = self
                .bindings
                .wabrix_plugins_packet_engine()
                .call_on_control_event(&mut self.store, event.opcode, &event.payload);
        });
    }

    fn processed_packets(&self) -> u64 {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

impl Drop for WasmEngine {
    fn drop(&mut self) {
        if !self.timer_diagnostics_enabled {
            return;
        }

        #[cfg(feature = "runtime-diagnostics")]
        log::info!(
            "NODE {}: final timer runtime counters: pipeline='{}', worker_id={}, worker_index={}, requested={}, allocated={}, unallocated={}, routed={}, retained={}, allocation_shortfalls={}, batch_mismatches={}, mailbox_conservation_failures={}, route_counts={:?}",
            self.id,
            self.pipeline_name,
            self.worker_id,
            self.worker_index,
            self.timer_requested_frames,
            self.timer_allocated_frames,
            self.timer_requested_frames
                .saturating_sub(self.timer_allocated_frames),
            self.timer_routed_frames,
            self.timer_retained_frames,
            self.timer_allocation_shortfalls,
            self.timer_batch_mismatches,
            self.mailbox_conservation_failures,
            self.timer_route_counts,
        );
    }
}
