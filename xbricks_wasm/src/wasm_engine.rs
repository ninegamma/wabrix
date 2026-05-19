use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;
use crate::wasm_manager::{MyHostState, WasmManager}; // Assuming WasmManager is defined here
use wasmtime::Store;
use wasmtime::component::{bindgen, ResourceTable};
use wasmtime_wasi::WasiCtxBuilder;
use xbricks_base::{Directive, Node, NodeId};
use xbricks_base::interface::batch::Batch;
use xbricks_base::interface::Pdu;
use xbricks_base::telemetry::TelemetryMetrics;
use crossbeam_channel::Sender;
use xbricks_base::frame_registry::get_worker_frame_registry;
use xbricks_protocol::TELEMETRY_FLAG;
use xbricks_protocol::wbatch::MAX_BATCH_SIZE;

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../xbricks_public/wit/xbricks.wit",
    world: "engine-plugin",
    // async: true,
});

pub struct WasmEngine {
    id: NodeId,
    // Wasmtime state: Store holds the UMEM handle via MyHostState
    store: Store<MyHostState>,
    // The generated bindings struct (named after the 'world' in CamelCase)
    bindings: EnginePlugin,
    // Index corresponds to the Route ID returned by Wasm
    routing_table: Vec<NodeId>,
    // Telemetry dispatch channel
    telemetry_tx: Option<Sender<u32>>,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    telemetry_metrics: TelemetryMetrics,
    /// Byte offset from Umem base where WASM address 0 begins for this instance.
    /// Used to translate `desc_addr` values to partition-relative WASM addresses
    /// before calling the plugin, and back to absolute offsets after.
    /// Zero for the first (or only) instance.
    partition_start_bytes: u32,
    /// The Umem this node is bound to. Every incoming PDU must carry this id.
    umem_id: u8,
}

impl WasmEngine {
    pub fn new(
        id: NodeId,
        manager: &WasmManager,
        wasm_file_path: &str,
        routing_table: Vec<NodeId>, // Accept any number of routes
        telemetry_tx: Option<Sender<u32>>,
        telemetry_metrics: TelemetryMetrics,
        partition_start_bytes: u32,
        umem_id: u8,
        wasm_data: HashMap<String, String>,
    ) -> Self {
        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component = manager.get_component(wasm_file_path).expect("Failed to load Wasm component");

        // 2. Build the partition-specific UmemHandle for this instance.
        //    WASM address 0 will map to `umem_base + partition_start_bytes`.
        //    The window extends to the end of the full Umem.
        let partition_handle = manager.make_partition_handle(partition_start_bytes);

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
                .call_init_data(&mut store, &data)
                .expect("Failed to call init-data on Wasm inspector");
        }

        Self {
            id,
            store,
            bindings,
            routing_table,
            telemetry_tx,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            telemetry_metrics,
            partition_start_bytes,
            umem_id,
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
        // 1. WASM EXECUTION
        // ==========================================================
        let active_bytes = len * size_of::<Pdu>();
        let (action_vector, lengths_vector): (Vec<u32>, Vec<u32>) = {
            let batch_bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(batch.as_ptr(), active_bytes)
            };

            self.bindings
                .xbricks_plugins_packet_engine()
                .call_process_batch(&mut self.store, batch_bytes, self.partition_start_bytes)
                .unwrap_or_else(|e| {
                    log::error!("Wasm execution failed: {}", e);
                    (vec![255; len], vec![0; len]) // 255 = DROP fallback
                })
        };

        if action_vector.len() != len || lengths_vector.len() != len {
            log::error!("Wasm returned mismatched action or lengths array!");
            batch.clear(); // Drop all memory safely
            return Directive::Stop;
        }

        // ==========================================
        // 2. MUTATION PHASE
        // ==========================================
        for (i, pdu) in batch.iter_mut().enumerate() {
            let new_len = lengths_vector[i];
            if new_len > 0 {
                pdu.w.set_data_length(new_len);
            }
        }

        // ==========================================
        // 3. ROUTING & TELEMETRY PHASE
        // ==========================================
        let num_routes = self.routing_table.len();
        let mut outputs: Vec<Batch> = std::iter::repeat_with(Batch::new).take(num_routes).collect();
        let mut pdu_index = 0;

        batch.distribute(&mut outputs, |pdu| {
            let action = action_vector[pdu_index];
            pdu_index += 1;

            let route_id = (action & 0xFF) as usize;
            let telemetry_bucket = ((action >> 16) & 0xFF) as usize;

            if telemetry_bucket < self.telemetry_metrics.counters_pkts.len() {
                self.telemetry_metrics.counters_pkts[telemetry_bucket].fetch_add(1, Ordering::Relaxed);
                self.telemetry_metrics.counters_bytes[telemetry_bucket].fetch_add(pdu.w.data_len() as u64, Ordering::Relaxed);
            }
            if route_id >= num_routes {
                log::warn!("NODE {}: Invalid Route ID {} for Pkt.", self.id, route_id);
                usize::MAX
            } else {
                route_id
            }
        });

        // ==========================================
        // 4. DISPATCH DIRECTIVES
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
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        debug_assert!(
            batch.iter().all(|pdu| pdu.w.umem_id() == self.umem_id),
            "NODE {}: PDU from wrong Umem arrived", self.id
        );

        self.processed_packets += batch.len();

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
        let n_requested_frames = self.bindings
            .xbricks_plugins_packet_engine()
            .call_process_timers(&mut self.store, current_tsc)
            .unwrap_or_else(|err| {
                log::error!("NODE {}: Error processing poll_timers: {}", self.id, err);
                0
            });

        if n_requested_frames == 0 {
            return Directive::Stop;
        }

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
            return Directive::Stop;
        }

        // 2. Load and initialize the freshly allocated PDUs into the Batch
        batch.clear();
        for i in 0..allocated {
            let mut pdu = unsafe { scratch[i].assume_init_read() };

            // Resolve the physical memory address for this specific frame
            let umem_addr = pdu.umem_addr();
            let data_ptr = pdu.data_ptr(umem_addr) as *mut u8;

            unsafe {
                // Zero out the MAC addresses (first 12 bytes) to prevent leaking old data
                core::ptr::write_bytes(data_ptr, 0, 12);

                // Write the EtherType (0x88B5 - Local Experimental) at offset 12 and 13.
                *data_ptr.add(12) = 0x88;
                *data_ptr.add(13) = 0xB5;
            }

            // CRITICAL: Set to a valid minimum Ethernet frame size so WASM parses it!
            pdu.w.set_data_length(64);

            // Ignore the Result; we bounded `allocated` to MAX_BATCH_SIZE
            let _ = batch.push(pdu);
        }

        self.processed_packets += allocated;
        // 3. Hand off to the exact same shared WASM execution and routing logic!
        self.execute_wasm_and_route(batch)
    }

    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}
