use crate::wasm_manager::{MyHostState, WasmManager};
use core::mem::size_of;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use wabrix_api::tcp_common::EngineTimerPlan;
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
use crate::timer_diagnostics::TimerDiagnostics;
use wabrix_base::frame_registry::get_worker_frame_registry;
use wabrix_base::interface::batch::Batch;
use wabrix_base::interface::Pdu;
use wabrix_base::system::PlatformData;
use wabrix_base::telemetry::TelemetryMetrics;
use wabrix_base::{ControlEvent, ControlEventResult, Directive, Node, NodeId};

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../wabrix_api/wit/wabrix.wit",
    world: "engine-plugin",
    // async: true,
});

use self::exports::wabrix::plugins::packet_engine::TelemetryMetric;

pub struct WasmEngine {
    id: NodeId,
    /// Wasmtime state: Store holds the UMEM handle via MyHostState
    store: Store<MyHostState>,
    /// The generated bindings struct (named after the 'world' in CamelCase)
    bindings: EnginePlugin,
    /// Index corresponds to the Route ID returned by Wasm
    routing_table: Vec<NodeId>,
    /// Control event receiver channel
    processed_packets: u64,
    /// Aggregate execution cycles retained for the shutdown summary.
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    telemetry_metrics: TelemetryMetrics,
    /// ABI offset used to translate UMEM-relative packet descriptors into the
    /// stitched linear memory (heap below, UMEM above).
    umem_offset: u32,
    /// Wasm-linear-memory offset of this instance's private mailbox.
    mailbox_offset: u32,
    /// The UMEM group this node uses for timer-frame allocation.
    umem_id: u8,
    /// Set permanently true after WASM returns a malformed response.
    /// All further WASM calls are skipped to prevent SIGSEGV from bad UMEM writes.
    poisoned: bool,
    timer_diagnostics: TimerDiagnostics,
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
        umem_id: u8,
        node_data: HashMap<String, String>,
        platform_data: &PlatformData,
        mailbox_offset: u32,
    ) -> Result<Self, String> {
        let timer_diagnostics = TimerDiagnostics::new(
            &node_data,
            &routing_table,
            platform_data.tsc_frequency,
            pipeline_name,
            worker_id,
            worker_index,
        );

        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component = manager
            .get_component(wasm_file_path)
            .map_err(|e| format!("Failed to load Wasm component '{}': {}", wasm_file_path, e))?;

        // 2. Build an instance-specific handle with a private linear mapping
        //    and an alias of the complete shared UMEM.
        let partition_handle = manager.make_partition_handle();

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
                umem: partition_handle.clone(), // Host-side state also sees the partition window
            },
        );

        // 6. Instantiate the plugin.
        //    Wasmtime calls XdpMemoryCreator::new_memory() here, which returns
        //    the partition handle we pre-loaded in step 3.
        //    We use the Manager's shared Linker, which already has WASI registered.
        let bindings = EnginePlugin::instantiate(&mut store, &component, manager.linker())
            .map_err(|e| {
                format!(
                    "Failed to instantiate Wasm component '{}': {}",
                    wasm_file_path, e
                )
            })?;

        if !node_data.is_empty() {
            let data: Vec<(String, String)> = node_data.into_iter().collect();
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

        Ok(Self {
            id,
            store,
            bindings,
            routing_table,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            telemetry_metrics,
            umem_offset: partition_handle.wasm_umem_offset(),
            mailbox_offset,
            umem_id,
            poisoned: false,
            timer_diagnostics,
        })
    }

    fn store_oob_telemetry(&self, metrics: Vec<TelemetryMetric>) {
        let mut counter_index = 0;
        for metric in metrics {
            match metric {
                TelemetryMetric::Counter(counter) => {
                    if let Ok(mut names) = self.telemetry_metrics.counter_names.write() {
                        if counter_index >= names.len() {
                            names.resize(counter_index + 1, String::new());
                        }
                        names[counter_index] = counter.name;
                    }
                    if let Some(counter_slot) =
                        self.telemetry_metrics.counters_pkts.get(counter_index)
                    {
                        counter_slot.store(counter.value, Ordering::Relaxed);
                    }
                    counter_index += 1;
                }
                TelemetryMetric::Hist(histogram) => {
                    if let Some(named_histogram) = self
                        .telemetry_metrics
                        .histograms
                        .iter()
                        .find(|candidate| candidate.name == histogram.name)
                    {
                        named_histogram.metrics.store_snapshot(
                            histogram.sum,
                            histogram.count,
                            &histogram.buckets,
                        );
                    }
                }
            }
        }
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
            let base = self.store.data().umem.linear_base();
            // SAFETY: mailbox_offset is within this instance's private linear
            // memory mapping, which the host created before instantiation.
            unsafe { (base as *mut u8).add(self.mailbox_offset as usize) as *mut HostBatch }
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
                self.umem_offset,
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
        //  OUT-OF-BAND TELEMETRY EXTRACTION
        // ==========================================================
        let control_plane_wants_sync = match &self.telemetry_metrics.force_sync_flag {
            Some(flag) => flag.load(Ordering::Relaxed),
            None => false,
        };

        let is_telemetry_tick = control_plane_wants_sync;
        if control_plane_wants_sync {
            log::trace!("NODE {}: TELEMETRY SYNC REQUESTED", self.id);
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
                log::trace!(
                    "NODE {}: Extracted {} OOB telemetry packets",
                    self.id,
                    out_of_band_stats.len()
                );
                self.store_oob_telemetry(out_of_band_stats);
            }
        }

        // ==========================================
        //  MUTATION PHASE — copy plugin-modified WPdu fields back into the host Batch
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

        batch.distribute(&mut outputs, |_pdu| {
            // SAFETY: exclusive access restored after WASM returned.
            let action = unsafe { (*mailbox_ptr).actions[pdu_index] };
            pdu_index += 1;

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
            self.timer_diagnostics.record_mailbox_conservation_failure();
            log::error!(
                "NODE {}: mailbox conservation failure: input={}, actions_read={}, routed={}, retained={}; rejecting batch",
                self.id,
                len,
                pdu_index,
                routed_frames,
                retained_frames
            );
            batch.clear();
            self.poisoned = true;
            return Directive::Stop;
        }

        self.timer_diagnostics
            .record_routing(routed_frames, retained_frames, &outputs);

        // ==========================================
        // 5. DISPATCH DIRECTIVES
        // ==========================================
        let mut split_branches = Vec::with_capacity(num_routes);
        for (i, output_batch) in outputs.into_iter().enumerate() {
            if !output_batch.is_empty() {
                split_branches.push(((&self.routing_table[i]).into(), output_batch));
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
            batch.iter().all(|pdu| pdu.umem_id() == Some(self.umem_id)),
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
            self.total_cycles = self
                .total_cycles
                .saturating_add(unsafe { std::arch::x86_64::_rdtsc() } - start_cycles);
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
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        // Timer polling is deliberately separated from `process`: the plugin may need to
        // retransmit packets or emit new SYN traffic even when no ingress packet arrived.
        // The returned plan contains counts, not packet descriptors; the host remains
        // responsible for obtaining valid UMEM frames and returning them through routing.
        // Evaluate the complete timer transaction into one directive so diagnostics and
        // profiling are applied consistently to both the idle and packet-producing paths.
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
            let requested_plan =
                EngineTimerPlan::new(timer_plan.timeout_frames, timer_plan.packet_frames, 0);
            let timeout_frames_requested = requested_plan.timeout_frames;
            let packet_frames_requested = requested_plan.packet_frames;
            let n_requested_frames = requested_plan.requested_frames();

            if n_requested_frames == 0 {
                // An empty plan still represents a useful poll: the plugin's counters may
                // have changed since the previous call. Drain the explicit telemetry request
                // here because there is no packet batch below whose normal path can do it.
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
                        self.store_oob_telemetry(oob);
                    }
                }
                Directive::Stop
            } else {
                // Allocate the exact number of frames requested by the plugin from the
                // shared registry. A short allocation is allowed: the resulting batch is
                // reduced to the frames that actually exist rather than using invalid slots.
                let registry = unsafe { &mut *get_worker_frame_registry() };
                let mut scratch: [MaybeUninit<Pdu>; MAX_BATCH_SIZE] =
                    core::array::from_fn(|_| MaybeUninit::uninit());
                if n_requested_frames > MAX_BATCH_SIZE as u32 {
                    self.timer_diagnostics.record_batch_mismatch();
                    log::error!(
                        "NODE {}: timer requested {} frames, exceeding MAX_BATCH_SIZE {}; rejecting request",
                        self.id,
                        n_requested_frames,
                        MAX_BATCH_SIZE
                    );
                    return Directive::Stop;
                }
                let frames_to_allocate = n_requested_frames as usize;
                self.timer_diagnostics
                    .record_requested(requested_plan.requested_frames());

                let allocated = registry
                    .allocate_pdus_into(self.umem_id as usize, &mut scratch[..frames_to_allocate]);
                let allocated_plan = requested_plan.with_allocated(allocated as u32);
                self.timer_diagnostics.record_allocated(allocated);
                if allocated < frames_to_allocate {
                    self.timer_diagnostics.record_allocation_shortfall();
                }

                if allocated == 0 {
                    log::warn!("NODE {}: Timer starved of UMEM frames!", self.id);
                    Directive::Stop
                } else {
                    // Move the allocated descriptors into the batch and initialize a
                    // minimal Ethernet/IPv4-looking header. The timer kind is metadata for
                    // the plugin: SYN frames are placed first so timeout work cannot consume
                    // frames reserved for SYN generation when allocation is short.
                    batch.clear();
                    let allocated_syn_frames = (allocated_plan.allocated_frames as usize)
                        .min(packet_frames_requested as usize);
                    let allocated_timeout_frames = (allocated_plan.allocated_frames as usize)
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
                            self.timer_diagnostics.record_batch_mismatch();
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
                        self.timer_diagnostics.record_batch_mismatch();
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

                    // Reuse the ordinary mailbox execution path. This copies the timer
                    // descriptors into the plugin mailbox, lets Wasm choose actions/routes,
                    // copies mutations back, and transfers ownership to downstream batches.
                    self.timer_diagnostics.begin_timer_batch();
                    let directive = self.execute_wasm_and_route(batch);
                    self.timer_diagnostics.end_timer_batch();
                    directive
                }
            }
        }; // <-- The block ends here, yielding the final Directive

        self.timer_diagnostics.report_if_due(&self.id, current_tsc);

        #[cfg(feature = "profiling")]
        {
            self.total_cycles = self
                .total_cycles
                .saturating_add(unsafe { std::arch::x86_64::_rdtsc() } - start_cycles);
        }

        directive
    }

    fn requires_control_events(&self) -> bool {
        true
    }

    fn handle_control_event(&mut self, event: &ControlEvent) -> ControlEventResult {
        if self.poisoned {
            return ControlEventResult::Unsupported;
        }
        // We only get here if the PipelineRunner already verified the wildcard matched us!
        let handled = tokio::task::block_in_place(|| {
            self.bindings
                .wabrix_plugins_packet_engine()
                .call_on_control_event(&mut self.store, event.opcode, &event.payload)
        });
        match handled {
            Ok(true) => ControlEventResult::Handled,
            Ok(false) | Err(_) if matches!(event.opcode, 1 | 2 | 3 | 255) => {
                ControlEventResult::InvalidPayload
            }
            Ok(false) | Err(_) => ControlEventResult::Unsupported,
        }
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
        self.timer_diagnostics.report_final(&self.id);
    }
}
