use crate::wasm_manager::{MyHostState, WasmManager}; // Assuming WasmManager is defined here

use wasmtime::Store;
use wasmtime::component::{bindgen, ResourceTable};
use wasmtime_wasi::WasiCtxBuilder;
use xbricks_base::{Directive, Node, NodeId};
use xbricks_base::interface::batch::Batch;
use xbricks_base::interface::Pdu;

// 1. Generate bindings.
// Note: `async: true` is required to use `instantiate_async` and `call_async`
bindgen!({
    path: "../wit/inspector.wit",
    world: "filter-plugin",
    // async: true,
});

pub struct WasmInspectorNode {
    id: NodeId,
    // Wasmtime state: Store holds the UMEM handle via MyHostState
    store: Store<MyHostState>,
    // The generated bindings struct (named after the 'world' in CamelCase)
    bindings: FilterPlugin,
    // Add reusable buffers for the fast path
    fast_path_route: NodeId,
    exception_route: NodeId,
    processed_packets: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl WasmInspectorNode {
    pub fn new(
        id: NodeId,
        manager: &WasmManager, // Pass the shared WasmManager instead of Arc<Engine>
        wasm_file_path: &str,
        fast_path_route: NodeId,
        exception_route: NodeId,
    ) -> Self {
        // 1. Fetch the Component from the Manager's DashMap cache
        // This takes nanoseconds if already loaded, avoiding disk I/O.
        let component = manager.get_component(wasm_file_path).expect("Failed to load Wasm component");

        // 2. Initialize the WASI Context for THIS specific node
        let wasi_ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build();

        // 3. Create the Store
        // This is where the magic happens: Wasmtime calls our `XdpMemoryCreator`
        // in the background to hand out the zero-copy UMEM handle.
        let mut store = Store::new(
            manager.engine(),
            MyHostState {
                ctx: wasi_ctx,
                table: ResourceTable::new(),
                umem: manager.umem(), // Pass the UMEM pointer so the HostState has access
            },
        );

        // 4. Instantiate the plugin
        // We use the Manager's shared Linker, which already has WASI registered.
        let bindings = FilterPlugin::instantiate(&mut store, &component, manager.linker())
            .expect("Failed to instantiate Wasm component");

        Self {
            id,
            store,
            bindings,
            fast_path_route,
            exception_route,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
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

        self.processed_packets += len;

        // ==========================================================
        // 1. CROSS THE WASM BOUNDARY (The 4KB L1-Cache Transfer)
        // ==========================================================

        // Calculate the exact byte size of the populated packets
        let active_bytes = len * std::mem::size_of::<Pdu>();

        // View the Host Batch as a raw byte slice
        let batch_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                batch.as_ptr(),
                active_bytes
            )
        };

        // Call the Component Model plugin
        let returned_bytes = self
            .bindings
            .xbricks_router_inspector()
            .call_inspect_batch(&mut self.store, batch_bytes).unwrap_or_else(|e| {
            log::error!("Wasm execution failed: {}", e);
            // If Wasm crashes, we skip step 2 and the router defaults to Forward/Drop
            Vec::new()
        });

        // ==========================================================
        // 2. APPLY THE MUTATIONS BACK TO THE HOST BATCH
        // ==========================================================
        if returned_bytes.len() == active_bytes {
            unsafe {
                // Instantly overwrite our Host Batch with the Wasm-mutated Batch.
                // Because Wasm treats the `SystemBus` pointer as locked `_padding`,
                // the pointers are perfectly preserved during this copy!
                std::ptr::copy_nonoverlapping(
                    returned_bytes.as_ptr(),
                    batch.as_mut_ptr() as *mut u8,
                    active_bytes,
                );
            }
        }
        // ==========================================================
        // 3. ROUTE THE PACKETS (Based on the new Wasm flags!)
        // ==========================================================
        let mut split_branches = Vec::with_capacity(2);
        let mut outputs = [Batch::new(), Batch::new()];

        batch.distribute(&mut outputs, |pdu| {
            // Wasm wrote the verdict directly into the `flags` field!
            match pdu.w.flags {
                0 => 0, // VERDICT_FORWARD -> mapped to outputs[0] (fast_path)
                1 => 1, // VERDICT_EXCEPTION -> mapped to outputs[1]
                2 => 1, // VERDICT_DROP -> mapped to outputs[1] (exception/drop route)
                _ => 1, // Fallback safety
            }
        });
        
        let fast_batch = std::mem::replace(&mut outputs[0], Batch::new());
        if !fast_batch.is_empty() {
            split_branches.push((self.fast_path_route.clone(), fast_batch));
        }

        let exception_batch = std::mem::replace(&mut outputs[1], Batch::new());
        if !exception_batch.is_empty() {
            split_branches.push((self.exception_route.clone(), exception_batch));
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

    fn processed_packets(&self) -> usize {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}
