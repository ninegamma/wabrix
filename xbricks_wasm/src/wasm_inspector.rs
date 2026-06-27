use std::collections::HashMap;
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
use xbricks_protocol::TELEMETRY_FLAG;

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../../xbricks_public/wit/xbricks.wit",
    world: "filter-plugin",
    // async: true,
});

pub struct WasmInspectorNode {
    id: NodeId,
    // Wasmtime state: Store holds the UMEM handle via MyHostState
    store: Store<MyHostState>,
    // The generated bindings struct (named after the 'world' in CamelCase)
    bindings: FilterPlugin,
    // Index corresponds to the Route ID returned by Wasm
    routing_table: Vec<NodeId>,
    // Telemetry dispatch channel (reserved for future use)
    #[allow(dead_code)]
    telemetry_tx: Option<Sender<u32>>,
    processed_packets: u64,
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

impl WasmInspectorNode {
    #[allow(clippy::too_many_arguments)]
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
        let bindings = FilterPlugin::instantiate(&mut store, &component, manager.linker())
            .expect("Failed to instantiate Wasm component");

        if !wasm_data.is_empty() {
            let data: Vec<(String, String)> = wasm_data.into_iter().collect();
            bindings.xbricks_plugins_inspector()
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
}

impl Node for WasmInspectorNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        debug_assert!(
            batch.iter().all(|pdu| pdu.w.umem_id() == self.umem_id),
            "NODE {}: PDU from wrong Umem arrived (expected umem_id={})",
            self.id, self.umem_id
        );

        self.processed_packets += len as u64;

        // ==========================================================
        // 1. CROSS THE WASM BOUNDARY (The 4KB L1-Cache Transfer)
        // ==========================================================

        // Calculate the exact byte size of the populated packets
        let active_bytes = len * size_of::<Pdu>();
        log::trace!("NODE {}: Processing batch of {} packets. Partition offset: {}", self.id, len, self.partition_start_bytes);
        // Call the Component Model plugin inside a block so that `batch_bytes`
        // (a shared reference into `batch`) is dropped before we restore desc_addr.
        let action_vector = {
            // View the Host Batch as a raw byte slice
            let batch_bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(batch.as_ptr(), active_bytes)
            };

            self.bindings
                .xbricks_plugins_inspector()
                .call_inspect_batch(&mut self.store, batch_bytes, self.partition_start_bytes)
                .unwrap_or_else(|e| {
                    log::error!("Wasm execution failed: {}", e);
                    // If Wasm crashes, we skip step 2 and the router defaults to Forward/Drop
                    vec![255; len] // 255 = DROP fallback
                })
        };


        // Safety check: Ensure Wasm returned an action for every packet
        if action_vector.len() != len {
            log::error!("Wasm returned mismatched action array!");
            batch.clear(); // Drop all
            return Directive::Stop;
        }

        // PREPARE DYNAMIC OUTPUTS
        let num_routes = self.routing_table.len();
        log::trace!("NODE {}: Routing table has {} available routes.", self.id, num_routes);
        let mut outputs: Vec<Batch> = std::iter::repeat_with(Batch::new)
            .take(num_routes)
            .collect();

        // We need an iterator index to match PDUs to the Wasm Action Vector
        let mut pdu_index = 0;


        // DISTRIBUTE AND EXECUTE ACTIONS
        // Bits 0-7 (Route ID): Which output port to send the packet to (0-254). 255 = DROP.
        // Bit 8 (Telemetry Flag): 1 = Send a telemetry event for this packet.
        // Bits 16-31 (Telemetry Meta): An ID, counter, or metric value for the telemetry subsystem.

        batch.distribute(&mut outputs, |pdu| {
            let action = action_vector[pdu_index];
            pdu_index += 1;

            let route_id = (action & 0xFF) as usize;
            let telemetry_bucket = ((action >> 16) & 0xFF) as usize; // Get the port bucket (0-10)
            let _send_sample = (action & TELEMETRY_FLAG) != 0;
            // TRACE LOG: Inspect the exact bitfield returned by WASM for this packet
            log::trace!("  -> Pkt [{}]: Action Bits = {:#010X} | Extracted Route ID = {} | Tel Bucket = {}", pdu_index, action, route_id, telemetry_bucket);
            // 1. HIGH VOLUME (Atomics) -> Cost: ~10 cycles
            // (Assuming telemetry_metrics is thread-local to this worker!)
            if telemetry_bucket < self.telemetry_metrics.counters_pkts.len() {
                self.telemetry_metrics.counters_pkts[telemetry_bucket].fetch_add(1, Ordering::Relaxed);
                self.telemetry_metrics.counters_bytes[telemetry_bucket].fetch_add(pdu.w.data_len() as u64, Ordering::Relaxed);
            }
/*
            // TODO
            // 2. LOW VOLUME (Channels) -> Cost: ~80 cycles (but rarely happens)
            if send_sample {
                // Only send a sample for anomalous packets
                let _ = self.telemetry_channel.try_send(AnomalyEvent {
                    bucket: telemetry_bucket,
                    // ... packet metadata
                });
            }
*/
            // invalid route ID: pdu remains in batch, and maybe dropped later
            if route_id >= num_routes {
                log::warn!("NODE {}: Invalid Route ID {} for Pkt [{}].", self.id, route_id, pdu_index);
                usize::MAX
            } else { route_id }
        });

        // 4. DISPATCH DIRECTIVES
        let mut split_branches = Vec::with_capacity(num_routes);
        for (i, output_batch) in outputs.into_iter().enumerate() {
            if !output_batch.is_empty() {
                log::trace!("NODE {}: Branching {} packets to Target Node ID: {}", self.id, output_batch.len(), self.routing_table[i]);
                // Route ID matches the index in our dynamic routing table
                split_branches.push((self.routing_table[i].clone(), output_batch));
            }
        }
        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        if split_branches.is_empty() {
            Directive::Stop
        } else {
            Directive::Split(split_branches)
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
