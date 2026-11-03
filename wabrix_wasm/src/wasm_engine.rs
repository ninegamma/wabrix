use crate::wasm_manager::{MyHostState, SharedStateRegion, WasmManager};
use crate::wasm_reloader::WasmReloadSpec;
use core::mem::size_of;
use crossbeam_utils::CachePadded;
use std::collections::{HashMap, HashSet};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use wabrix_api::tcp_common::EngineTimerPlan;
use wabrix_api::{
    BatchMetadata, MAX_BATCH_SIZE, MAX_HISTOGRAM_BUCKETS, TIMER_KIND_NONE, TIMER_KIND_SYN,
    TIMER_KIND_TIMEOUT,
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
use wabrix_base::telemetry::{HistogramMetrics, NamedHistogramMetrics, TelemetryMetrics};
use wabrix_base::{ControlEvent, ControlEventResult, Directive, Node, NodeId};

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../wabrix_api/wit/wabrix.wit",
    world: "engine-plugin",
    // async: true,
});

use self::exports::wabrix::plugins::packet_engine::{TelemetryMetric, TelemetrySchema};

pub struct WasmEngine {
    id: NodeId,
    pipeline_name: String,
    worker_id: usize,
    worker_index: usize,
    tsc_frequency: u64,
    manager: Arc<WasmManager>,
    /// Backing region retained across active and staged runtimes for this node.
    shared_state: Arc<SharedStateRegion>,
    wasm_file_path: String,
    base_node_data: HashMap<String, String>,
    hot_node_data: HashMap<String, String>,
    /// Wasmtime state: Store holds the UMEM handle via MyHostState
    runtime: WasmRuntime,
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
    timer_diagnostics: FinalTimerDiagnostics,
}

pub(crate) struct WasmRuntime {
    store: Store<MyHostState>,
    bindings: EnginePlugin,
}

struct FinalTimerDiagnostics {
    node_id: NodeId,
    diagnostics: TimerDiagnostics,
    report_on_drop: bool,
}

impl Deref for FinalTimerDiagnostics {
    type Target = TimerDiagnostics;

    fn deref(&self) -> &Self::Target {
        &self.diagnostics
    }
}

impl DerefMut for FinalTimerDiagnostics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.diagnostics
    }
}

impl Drop for FinalTimerDiagnostics {
    fn drop(&mut self) {
        if self.report_on_drop {
            self.diagnostics.report_final(&self.node_id);
        }
    }
}

impl WasmEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        pipeline_name: String,
        worker_id: usize,
        worker_index: usize,
        manager: Arc<WasmManager>,
        wasm_file_path: &str,
        routing_table: Vec<NodeId>, // Accept any number of routes
        umem_id: u8,
        node_data: HashMap<String, String>,
        base_node_data: HashMap<String, String>,
        hot_node_data: HashMap<String, String>,
        platform_data: &PlatformData,
        mailbox_offset: u32,
    ) -> Result<Self, String> {
        let shared_state = manager
            .create_shared_state_region()
            .map_err(|error| format!("failed to create shared-state region: {error}"))?;
        Self::new_with_shared_state(
            id,
            pipeline_name,
            worker_id,
            worker_index,
            manager,
            wasm_file_path,
            routing_table,
            None,
            umem_id,
            node_data,
            base_node_data,
            hot_node_data,
            platform_data,
            mailbox_offset,
            shared_state,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_shared_state(
        id: NodeId,
        pipeline_name: String,
        worker_id: usize,
        worker_index: usize,
        manager: Arc<WasmManager>,
        wasm_file_path: &str,
        routing_table: Vec<NodeId>,
        telemetry_metrics: Option<TelemetryMetrics>,
        umem_id: u8,
        node_data: HashMap<String, String>,
        base_node_data: HashMap<String, String>,
        hot_node_data: HashMap<String, String>,
        platform_data: &PlatformData,
        mailbox_offset: u32,
        shared_state: Arc<SharedStateRegion>,
    ) -> Result<Self, String> {
        let timer_diagnostics = TimerDiagnostics::new(
            &node_data,
            &routing_table,
            platform_data.tsc_frequency,
            pipeline_name.clone(),
            worker_id,
            worker_index,
        );
        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component_load_started = Instant::now();
        let component = manager
            .get_component(wasm_file_path)
            .map_err(|e| format!("Failed to load Wasm component '{}': {}", wasm_file_path, e))?;
        let component_load_compile_validate_ns = component_load_started.elapsed().as_nanos();
        log::debug!(
            "NODE {}: Wasm component is ready; instantiating {}",
            id,
            wasm_file_path
        );

        // 2. Build an instance-specific handle with a private linear mapping
        //    and an alias of the complete shared UMEM.
        let partition_handle =
            manager.make_partition_handle_with_shared_state(shared_state.clone());

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
            MyHostState::new(wasi_ctx, ResourceTable::new(), partition_handle.clone()),
        );

        // 6. Instantiate the plugin.
        //    Wasmtime calls XdpMemoryCreator::new_memory() here, which returns
        //    the partition handle we pre-loaded in step 3.
        //    We use the Manager's shared Linker, which already has WASI registered.
        let component_instantiate_started = Instant::now();
        let bindings_result = EnginePlugin::instantiate(&mut store, &component, manager.linker());
        let component_instantiate_ns = component_instantiate_started.elapsed().as_nanos();
        let bindings = bindings_result.map_err(|e| {
            format!(
                "Failed to instantiate Wasm component '{}': {}",
                wasm_file_path, e
            )
        })?;
        log::debug!(
            "NODE {}: Wasm instance created; calling init-data for {}",
            id,
            wasm_file_path
        );

        let data: Vec<(String, String)> = node_data.into_iter().collect();
        let init_data_started = Instant::now();
        let init_result = bindings.wabrix_plugins_packet_engine().call_init_data(
            &mut store,
            &data,
            platform_data.tsc_frequency,
        );
        let init_data_ns = init_data_started.elapsed().as_nanos();
        log::info!(
            "NODE {}: component preparation {}: artifact_load_compile_validate={} ns, instantiate={} ns, init_data={} ns",
            id,
            wasm_file_path,
            component_load_compile_validate_ns,
            component_instantiate_ns,
            init_data_ns
        );
        log::debug!("NODE {}: init-data returned for {}", id, wasm_file_path);

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

        let telemetry_schema = bindings
            .wabrix_plugins_packet_engine()
            .call_get_telemetry_schema(&mut store)
            .map_err(|error| {
                format!(
                    "WasmEngine node {} failed to read telemetry schema: {}",
                    id, error
                )
            })?;
        validate_plugin_telemetry_schema(&telemetry_schema)?;
        let telemetry_metrics = match telemetry_metrics {
            Some(existing) => {
                validate_reload_telemetry_schema(&telemetry_schema, &existing, &id)?;
                existing
            }
            None => create_plugin_telemetry_metrics(&id, &telemetry_schema),
        };

        let final_timer_diagnostics = FinalTimerDiagnostics {
            node_id: id.clone(),
            diagnostics: timer_diagnostics,
            report_on_drop: true,
        };

        Ok(Self {
            id,
            pipeline_name,
            worker_id,
            worker_index,
            tsc_frequency: platform_data.tsc_frequency,
            manager,
            shared_state,
            wasm_file_path: wasm_file_path.to_string(),
            base_node_data,
            hot_node_data,
            runtime: WasmRuntime { store, bindings },
            routing_table,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            telemetry_metrics,
            umem_offset: partition_handle.wasm_umem_offset(),
            mailbox_offset,
            umem_id,
            poisoned: false,
            timer_diagnostics: final_timer_diagnostics,
        })
    }

    pub(crate) fn reload_spec(&self) -> WasmReloadSpec {
        WasmReloadSpec {
            id: self.id.clone(),
            pipeline_name: self.pipeline_name.clone(),
            worker_id: self.worker_id,
            worker_index: self.worker_index,
            tsc_frequency: self.tsc_frequency,
            manager: self.manager.clone(),
            base_node_data: self.base_node_data.clone(),
            routing_table: self.routing_table.clone(),
            telemetry_metrics: self.telemetry_metrics.clone(),
            umem_id: self.umem_id,
            mailbox_offset: self.mailbox_offset,
            shared_state: self.shared_state.clone(),
        }
    }

    pub fn telemetry_metrics(&self) -> &TelemetryMetrics {
        &self.telemetry_metrics
    }

    /// Installs only runtime-owned state; graph and node accounting remain with this node.
    pub(crate) fn install_candidate(&mut self, mut candidate: WasmEngine) -> WasmRuntime {
        debug_assert!(Arc::ptr_eq(&self.shared_state, &candidate.shared_state));
        std::mem::swap(&mut self.runtime, &mut candidate.runtime);
        self.poisoned = false;
        self.wasm_file_path = candidate.wasm_file_path;
        self.hot_node_data = candidate.hot_node_data;
        candidate.runtime
    }

    /// Upgrade shared payload metadata at the runner's quiescent switchover boundary.
    /// State bytes are already visible through the shared mapping; only declared
    /// append-only migrations call into the staged guest.
    pub(crate) fn upgrade_shared_state_for_candidate(
        candidate: &mut WasmEngine,
    ) -> Result<(), String> {
        let Some(attachment) = candidate.runtime.store.data().shared_state_attachment else {
            return Ok(());
        };
        if !attachment.upgrade_required {
            return Ok(());
        }

        tokio::task::block_in_place(|| {
            candidate
                .runtime
                .bindings
                .wabrix_plugins_packet_engine()
                .call_upgrade_shared_state(
                    &mut candidate.runtime.store,
                    attachment.current_schema_version,
                    attachment.current_payload_size,
                )
        })
        .map_err(|error| {
            format!("replacement plugin trapped during shared-state upgrade: {error}")
        })?
        .map_err(|error| format!("replacement plugin rejected shared-state upgrade: {error}"))?;

        candidate
            .runtime
            .store
            .data()
            .umem
            .commit_shared_state_upgrade(attachment)
    }

    /// Extracts a staged runtime so its destruction can happen on the reaper thread.
    pub(crate) fn into_runtime(self) -> WasmRuntime {
        self.runtime
    }

    pub(crate) fn send_plugin_control_event(&mut self, event: &ControlEvent) -> ControlEventResult {
        let handled = tokio::task::block_in_place(|| {
            self.runtime
                .bindings
                .wabrix_plugins_packet_engine()
                .call_on_control_event(&mut self.runtime.store, event.opcode, &event.payload)
        });
        match handled {
            Ok(true) => ControlEventResult::Handled,
            Ok(false) | Err(_) if matches!(event.opcode, 1 | 2 | 3 | 255) => {
                ControlEventResult::InvalidPayload
            }
            Ok(false) | Err(_) => ControlEventResult::Unsupported,
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(crate) fn node_id(&self) -> &NodeId {
        &self.id
    }

    pub(crate) fn wasm_file_path(&self) -> &str {
        &self.wasm_file_path
    }

    pub(crate) fn hot_node_data(&self) -> &HashMap<String, String> {
        &self.hot_node_data
    }

    pub(crate) fn set_hot_node_data(&mut self, hot_node_data: HashMap<String, String>) {
        self.hot_node_data = hot_node_data;
    }

    pub(crate) fn suppress_final_diagnostics(&mut self) {
        self.timer_diagnostics.report_on_drop = false;
    }

    fn store_oob_telemetry(&self, metrics: Vec<TelemetryMetric>) {
        let counter_names = match self.telemetry_metrics.counter_names.read() {
            Ok(names) => names,
            Err(_) => {
                log::error!(
                    "NODE {}: telemetry counter schema lock is poisoned",
                    self.id
                );
                return;
            }
        };
        for metric in metrics {
            match metric {
                TelemetryMetric::Counter(counter) => {
                    if let Some(counter_index) =
                        counter_names.iter().position(|name| name == &counter.name)
                    {
                        if let Some(counter_slot) =
                            self.telemetry_metrics.counters_pkts.get(counter_index)
                        {
                            counter_slot.store(counter.value, Ordering::Relaxed);
                        }
                    } else {
                        log::warn!(
                            "NODE {}: plugin emitted undeclared telemetry counter '{}'",
                            self.id,
                            counter.name
                        );
                    }
                }
                TelemetryMetric::Hist(histogram) => {
                    if let Some(named_histogram) = self
                        .telemetry_metrics
                        .histograms
                        .iter()
                        .find(|candidate| candidate.name == histogram.name)
                    {
                        if named_histogram.metrics.buckets.len() == histogram.buckets.len() {
                            named_histogram.metrics.store_snapshot(
                                histogram.sum,
                                histogram.count,
                                &histogram.buckets,
                            );
                        } else {
                            log::warn!(
                                "NODE {}: plugin emitted histogram '{}' with {} buckets; schema declares {}",
                                self.id,
                                histogram.name,
                                histogram.buckets.len(),
                                named_histogram.metrics.buckets.len()
                            );
                        }
                    } else {
                        log::warn!(
                            "NODE {}: plugin emitted undeclared telemetry histogram '{}'",
                            self.id,
                            histogram.name
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
            let base = self.runtime.store.data().umem.linear_base();
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
            .runtime
            .bindings
            .wabrix_plugins_packet_engine()
            .call_process_batch(
                &mut self.runtime.store,
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
                .runtime
                .bindings
                .wabrix_plugins_packet_engine()
                .call_get_telemetry(&mut self.runtime.store)
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

/// Checks a plugin's declared schema before the host allocates telemetry storage.
/// Names must be nonempty and unique within each metric type, and histogram bucket
/// counts must fit the host's supported range. The caller propagates errors with
/// `?`, so a bad schema aborts initial node startup or rejects a staged reload.
fn validate_plugin_telemetry_schema(schema: &TelemetrySchema) -> Result<(), String> {
    let mut counter_names = HashSet::with_capacity(schema.counters.len());
    for name in &schema.counters {
        if name.is_empty() {
            return Err("WASM telemetry schema contains an empty counter name".to_string());
        }
        if !counter_names.insert(name) {
            return Err(format!(
                "WASM telemetry schema contains duplicate counter '{}'",
                name
            ));
        }
    }

    let mut histogram_names = HashSet::with_capacity(schema.histograms.len());
    for histogram in &schema.histograms {
        if histogram.name.is_empty() {
            return Err("WASM telemetry schema contains an empty histogram name".to_string());
        }
        if !histogram_names.insert(&histogram.name) {
            return Err(format!(
                "WASM telemetry schema contains duplicate histogram '{}'",
                histogram.name
            ));
        }
        let bucket_count = histogram.bucket_count as usize;
        if bucket_count == 0 || bucket_count > MAX_HISTOGRAM_BUCKETS {
            return Err(format!(
                "WASM telemetry histogram '{}' declares {} buckets; supported range is 1..={}",
                histogram.name, bucket_count, MAX_HISTOGRAM_BUCKETS
            ));
        }
    }

    Ok(())
}

/// Ensures a replacement plugin matches the telemetry layout already registered
/// for this node: counter names and order, backing-array lengths, histogram names,
/// and bucket counts must remain stable. An error aborts candidate construction,
/// leaving the active runtime and its registered telemetry storage in place.
fn validate_reload_telemetry_schema(
    schema: &TelemetrySchema,
    existing: &TelemetryMetrics,
    node_id: &NodeId,
) -> Result<(), String> {
    let existing_names = existing
        .counter_names
        .read()
        .map_err(|_| format!("WASM node {} telemetry schema lock is poisoned", node_id))?;
    if existing_names.as_slice() != schema.counters.as_slice()
        || existing.counters_pkts.len() != schema.counters.len()
        || existing.counters_bytes.len() != schema.counters.len()
    {
        return Err(format!(
            "WASM node {} reload changes its telemetry counter schema",
            node_id
        ));
    }

    if existing.histograms.len() != schema.histograms.len()
        || existing
            .histograms
            .iter()
            .zip(&schema.histograms)
            .any(|(current, replacement)| {
                current.name != replacement.name
                    || current.metrics.buckets.len() != replacement.bucket_count as usize
            })
    {
        return Err(format!(
            "WASM node {} reload changes its telemetry histogram schema",
            node_id
        ));
    }

    Ok(())
}

fn create_plugin_telemetry_metrics(node_id: &NodeId, schema: &TelemetrySchema) -> TelemetryMetrics {
    let counter_names = Arc::new(RwLock::new(schema.counters.clone()));
    let counters_pkts = (0..schema.counters.len())
        .map(|_| Arc::new(CachePadded::new(AtomicU64::new(0))))
        .collect();
    let counters_bytes = (0..schema.counters.len())
        .map(|_| Arc::new(CachePadded::new(AtomicU64::new(0))))
        .collect();
    let histograms = schema
        .histograms
        .iter()
        .map(|histogram| NamedHistogramMetrics {
            name: histogram.name.clone(),
            metrics: Arc::new(HistogramMetrics::new(histogram.bucket_count as usize)),
        })
        .collect();

    TelemetryMetrics {
        node_id: node_id.to_string(),
        counter_names,
        counters_pkts,
        counters_bytes,
        active_flag: None,
        force_sync_flag: Some(Arc::new(AtomicBool::new(false))),
        histograms,
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
                .runtime
                .bindings
                .wabrix_plugins_packet_engine()
                .call_process_timers(&mut self.runtime.store, current_tsc)
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
                        .runtime
                        .bindings
                        .wabrix_plugins_packet_engine()
                        .call_get_telemetry(&mut self.runtime.store)
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
        // Host-side Wasm updates are intercepted by PipelineRunner's reload controller.
        self.send_plugin_control_event(event)
    }

    fn processed_packets(&self) -> u64 {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}
