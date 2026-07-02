use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use crate::wasm_manager::{MyHostState, WasmManager};
use wasmtime::Store;
use wasmtime::component::{bindgen, ResourceTable};
use wasmtime_wasi::WasiCtxBuilder;
use xbricks_protocol::MAX_BATCH_SIZE;
use xbricks_base::{ControlEvent, Directive, Node, NodeId};
use xbricks_base::interface::batch::{Batch};
use xbricks_base::interface::Pdu;
use xbricks_base::mailbox::BatchMetadata;
use xbricks_base::telemetry::TelemetryMetrics;
use xbricks_base::frame_registry::get_worker_frame_registry;
use xbricks_base::system::PlatformData;

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../xbricks_public/wit/xbricks.wit",
    world: "engine-plugin",
    // async: true,
});

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
}

impl WasmEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        manager: &WasmManager,
        wasm_file_path: &str,
        routing_table: Vec<NodeId>, // Accept any number of routes
        telemetry_metrics: TelemetryMetrics,
        partition_start_bytes: u32,
        umem_id: u8,
        wasm_data: HashMap<String, String>,
        platform_data: &PlatformData,
        mailbox_offset: u32,
    ) -> Self {
        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component = manager.get_component(wasm_file_path).expect("Failed to load Wasm component");

        // 2. Build the partition-specific UmemHandle for this instance.
        //    WASM address 0 will map to `umem_base + partition_start_bytes`.
        //    The window extends to the end of the full Umem.
        let partition_handle = manager.make_partition_handle(partition_start_bytes);
        log::info!("NODE {}: Partition start bytes: {}", id, partition_start_bytes);

        // 3. Pre-load the partition handle into the shared XdpMemoryCreator so
        //    that Wasmtime picks it up when it calls new_memory() during instantiate().
        manager.set_instantiation_handle(partition_handle.clone());

        // 4. Initialize the WASI Context for THIS specific node
        let wasi_ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build();

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
            bindings.xbricks_plugins_packet_engine()
                .call_init_data(&mut store, &data, platform_data.tsc_frequency)
                .unwrap_or_else(|e| {
                    log::error!("Failed to call init-data on packet-engine: {}", e);
                });
        }

        Self {
            id,
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
        }
    }

    /// PRIVATE HELPER: Handles WASM execution, Length mutation, Telemetry, and Routing
    #[inline(always)]
    fn execute_wasm_and_route(&mut self, batch: &mut Batch) -> Directive {
        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        // ==========================================================
        // 1. POPULATE MAILBOX & CALL PLUGIN
        // ==========================================================

        // Capture the raw mailbox pointer before mutably borrowing self.store.
        // SAFETY: Control Block frames are excluded from the AF_XDP Fill Queue;
        // this instance's mailbox slice is exclusively owned by this thread
        // during the entire call window (write before call, read after return).
        let mailbox_ptr: *mut BatchMetadata = {
            let base = self.store.data().umem.ptr;
            // SAFETY: mailbox_offset is within the Control Block, which the host
            // guarantees is backed by allocated UMEM memory.
            unsafe { base.add(self.mailbox_offset as usize) as *mut BatchMetadata }
        };

        unsafe {
            let m = &mut *mailbox_ptr;
            for (i, pdu) in batch.iter().enumerate() {
                m.desc_addrs[i] = pdu.w.desc_addr as u32;
                m.lengths[i]    = pdu.w.data_len();
            }
        }

        let status = self.bindings
            .xbricks_plugins_packet_engine()
            .call_process_batch(&mut self.store, len as u32, self.partition_start_bytes, self.mailbox_offset)
            .unwrap_or_else(|e| {
                log::error!("NODE {}: Wasm execution failed: {}", self.id, e);
                u32::MAX
            });

        if status == u32::MAX {
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
        if control_plane_wants_sync { log::debug!("NODE {}: TELEMETRY SYNC REQUESTED", self.id); }
        if is_telemetry_tick {
            if let Some(flag) = &self.telemetry_metrics.force_sync_flag {
                flag.store(false, Ordering::Relaxed);
            }
            let out_of_band_stats = self.bindings.xbricks_plugins_packet_engine()
                .call_get_telemetry(&mut self.store)
                .unwrap_or_else(|e| {
                    log::error!("Failed to call get-telemetry on packet-engine: {}", e);
                    vec![]
                });

            if !out_of_band_stats.is_empty() {
                self.supports_oob_telemetry = true;
                log::debug!("NODE {}: Extracted {} OOB telemetry packets", self.id, out_of_band_stats.len());
                for (idx, &count) in out_of_band_stats.iter().enumerate() {
                    if idx < self.telemetry_metrics.counters_pkts.len() {
                        self.telemetry_metrics.counters_pkts[idx].store(count, Ordering::Relaxed);
                    }
                }
            }
        }

        // ==========================================
        // 3. MUTATION PHASE — read new_lengths from mailbox
        // ==========================================
        // SAFETY: WASM call returned; we have exclusive access again.
        unsafe {
            let m = &*mailbox_ptr;
            for (i, pdu) in batch.iter_mut().enumerate() {
                let new_len = m.new_lengths[i];
                if new_len > 0 {
                    pdu.w.set_data_length(new_len);
                }
            }
        }

        // ==========================================
        // 4. ROUTING PHASE — read actions from mailbox
        // ==========================================
        let num_routes = self.routing_table.len();
        let mut outputs: Vec<Batch> = std::iter::repeat_with(Batch::new).take(num_routes).collect();
        let mut pdu_index = 0;

        batch.distribute(&mut outputs, |pdu| {
            // SAFETY: exclusive access restored after WASM returned.
            let action = unsafe { (*mailbox_ptr).actions[pdu_index] };
            pdu_index += 1;

            if !self.supports_oob_telemetry {
                let telemetry_bucket = ((action >> 16) & 0xFF) as usize;
                if telemetry_bucket < self.telemetry_metrics.counters_pkts.len() {
                    self.telemetry_metrics.counters_pkts[telemetry_bucket].fetch_add(1, Ordering::Relaxed);
                    self.telemetry_metrics.counters_bytes[telemetry_bucket].fetch_add(pdu.w.data_len() as u64, Ordering::Relaxed);
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
            "NODE {}: PDU from wrong Umem arrived", self.id
        );

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
            let n_requested_frames = self.bindings
                .xbricks_plugins_packet_engine()
                .call_process_timers(&mut self.store, current_tsc)
                .unwrap_or_else(|err| {
                    log::error!("NODE {}: Error processing poll_timers: {}", self.id, err);
                    0
                });

            if n_requested_frames == 0 {
                // Service force_sync_flag even when idle so final WASM state is
                // captured in host AtomicU64 counters at run end.
                let control_plane_wants_sync = self.telemetry_metrics.force_sync_flag
                    .as_ref()
                    .map(|f| f.load(Ordering::Relaxed))
                    .unwrap_or(false);

                if control_plane_wants_sync {
                    if let Some(flag) = &self.telemetry_metrics.force_sync_flag {
                        flag.store(false, Ordering::Relaxed);
                    }
                    let oob = self.bindings.xbricks_plugins_packet_engine()
                        .call_get_telemetry(&mut self.store)
                        .unwrap_or_default();
                    if !oob.is_empty() {
                        self.supports_oob_telemetry = true;
                        for (idx, &count) in oob.iter().enumerate() {
                            if idx < self.telemetry_metrics.counters_pkts.len() {
                                self.telemetry_metrics.counters_pkts[idx].store(count, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Directive::Stop
            } else {
                // 1. Allocate exact memory from the shared FrameRegistry
                let registry = unsafe { &mut *get_worker_frame_registry() };
                let mut scratch: [MaybeUninit<Pdu>; MAX_BATCH_SIZE] = core::array::from_fn(|_| MaybeUninit::uninit());
                let frames_to_allocate = std::cmp::min(n_requested_frames as usize, MAX_BATCH_SIZE);

                let allocated = registry.allocate_pdus_into(
                    self.umem_id as usize,
                    &mut scratch[..frames_to_allocate]
                );

                if allocated == 0 {
                    log::warn!("NODE {}: Timer starved of UMEM frames!", self.id);
                    Directive::Stop
                } else {
                    // 2. Load and initialize the freshly allocated PDUs into the Batch
                    batch.clear();
                    for slot in scratch[..allocated].iter() {
                        let mut pdu = unsafe { slot.assume_init_read() };
                        let umem_addr = pdu.umem_addr();
                        let data_ptr = pdu.data_ptr(umem_addr);

                        unsafe {
                            core::ptr::write_bytes(data_ptr, 0, 12);
                            *data_ptr.add(12) = 0x88;
                            *data_ptr.add(13) = 0xB5;
                        }

                        pdu.w.set_data_length(64);
                        let _ = batch.push(pdu);
                    }

                    self.processed_packets += allocated as u64;

                    // 3. Hand off to WASM execution (evaluates to Directive)
                    self.execute_wasm_and_route(batch)
                }
            }
        }; // <-- The block ends here, yielding the final Directive

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
            let _ = self.bindings
                .xbricks_plugins_packet_engine()
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
