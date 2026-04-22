use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, LinearMemory, MemoryCreator, MemoryType, OptLevel, Result};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use xsk_rs::Umem;
// --- 1. Memory Layer (The Zero-Copy Bridge) ---

/// Thread-safe handle to the mmap'd AF_XDP UMEM region.
///
/// Each `WasmInspectorNode` instance gets its own handle with a distinct
/// `partition_start_bytes` so that WASM address 0 maps to the start of
/// that instance's reserved scratchpad. The window then extends to the
/// end of the full Umem, giving the plugin access to all packet frames
/// above the reserved region.
///
/// Memory layout (host-side byte offsets from Umem base):
/// ```
/// [0 .. reserved_bytes)         reserved region, split into equal partitions
/// [reserved_bytes .. total_size) packet-frame region, accessible to all instances
/// ```
/// For instance N with `partition_start_bytes = N * partition_size`:
/// ```
/// WASM addr 0                   == host offset partition_start_bytes
/// WASM addr partition_size      == host offset reserved_bytes (packet frames begin)
/// WASM addr (total - start - 1) == last byte of Umem
/// ```
#[derive(Clone)]
pub struct UmemHandle {
    pub ptr: *mut u8,
    /// Total Umem size in bytes (`frame_count * frame_size`).
    pub size: usize,
    /// Byte offset from `ptr` where this instance's WASM address 0 begins.
    /// Zero for non-partitioned or first-instance use.
    pub partition_start_bytes: u32,
    /// Number of frames at the start of the UMEM exclusively reserved for WASM plugins.
    /// Byte range: [0, reserved_frames * frame_size).
    pub reserved_frames: u32,
    pub frame_size: u32,
}

impl UmemHandle {
    /// Byte size of the region reserved exclusively for WASM plugins: [0, reserved_bytes()).
    #[inline]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_frames as usize * self.frame_size as usize
    }

    /// Size of the WASM linear memory window: from `partition_start_bytes` to end of Umem.
    /// This covers both the instance's own scratchpad partition and all packet frames above.
    #[inline]
    pub fn window_size_bytes(&self) -> usize {
        self.size - (self.partition_start_bytes as usize)
    }
}

// SAFETY: UMEM is a fixed hardware-backed buffer managed by the router lifecycle.
unsafe impl Send for UmemHandle {}
unsafe impl Sync for UmemHandle {}

unsafe impl LinearMemory for UmemHandle {
    /// The visible window starts at `partition_start_bytes` and reaches the end of the Umem.
    fn byte_size(&self) -> usize { self.window_size_bytes() }
    fn byte_capacity(&self) -> usize { self.window_size_bytes() }
    fn grow_to(&mut self, _new_size: usize) -> Result<()> {
        Err(wasmtime::Error::msg("XBricks: Memory growth disabled for zero-copy UMEM"))
    }

    /// WASM address 0 maps to `ptr + partition_start_bytes`.
    fn as_ptr(&self) -> *mut u8 {
        // SAFETY: partition_start_bytes is always <= size, validated at construction.
        unsafe { self.ptr.add(self.partition_start_bytes as usize) }
    }
}

/// Per-engine custom memory allocator.
///
/// `current_handle` is pre-loaded with the partition-specific `UmemHandle`
/// just before each `FilterPlugin::instantiate()` call. Wasmtime calls
/// `new_memory()` exactly once per instantiation (for single-memory modules),
/// so the mutex hand-off is safe as long as instance creation is sequential.
pub struct XdpMemoryCreator {
    pub current_handle: Mutex<Arc<UmemHandle>>,
}

unsafe impl MemoryCreator for XdpMemoryCreator {
    fn new_memory(
        &self,
        _ty: MemoryType,
        _minimum: usize,
        _maximum: Option<usize>,
        _reserved: Option<usize>,
        _guard_size: usize,
    ) -> std::result::Result<Box<dyn LinearMemory>, String> {
        // Clone the handle currently loaded for this instantiation.
        let handle = self.current_handle.lock().unwrap().clone();
        Ok(Box::new((*handle).clone()))
    }
}

// --- 2. Host State Layer (WASI & Custom) ---

pub struct MyHostState {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
    pub umem: Arc<UmemHandle>, // Allows plugin host-calls to see UMEM info
}

impl WasiView for MyHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.ctx, table: &mut self.table }
    }
}

// --- 3. Management Layer (Orchestration) ---

pub struct WasmManager {
    engine: Engine,
    linker: Linker<MyHostState>,
    cache: DashMap<String, Component>,
    umem: Arc<UmemHandle>,
    /// Kept so `set_instantiation_handle` can pre-load the correct partition
    /// handle before each `FilterPlugin::instantiate()` call.
    memory_creator: Arc<XdpMemoryCreator>,
}

impl WasmManager {
    pub fn new(frame_size: u32, reserved_frames: u32, frame_count: u32, umem: Arc<Umem>) -> Result<Self> {
        let umem_ptr = umem.as_ptr();
        let size = frame_count as usize * frame_size as usize;
        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            size,
            partition_start_bytes: 0,
            reserved_frames,
            frame_size,
        });

        // The creator is pre-loaded with the base handle; each WasmInspectorNode
        // will call set_instantiation_handle() before instantiation to replace it.
        let memory_creator = Arc::new(XdpMemoryCreator {
            current_handle: Mutex::new(umem_handle.clone()),
        });

        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_memory64(true)
            .memory_reservation(size as u64)
            .memory_may_move(false) // Critical for AF_XDP stability
            .cranelift_opt_level(OptLevel::Speed)
            .with_host_memory(memory_creator.clone())
            // Disable mmap-based image initialization because we are using a custom hardware UMEM creator:
            .memory_init_cow(false);

        let engine = Engine::new(&config)?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;

        Ok(Self {
            engine,
            linker,
            cache: DashMap::new(),
            umem: umem_handle,
            memory_creator,
        })
    }

    /// Retrieves a compiled component from cache or loads it from disk.
    pub fn get_component(&self, path: &str) -> Result<Component> {
        if let Some(c) = self.cache.get(path) {
            return Ok(c.clone());
        }

        let component = Component::from_file(&self.engine, path)?;
        self.cache.insert(path.to_string(), component.clone());
        Ok(component)
    }

    pub fn engine(&self) -> &Engine { &self.engine }
    pub fn linker(&self) -> &Linker<MyHostState> { &self.linker }
    pub fn umem(&self) -> Arc<UmemHandle> { self.umem.clone() }

    /// Pre-load the partition handle that `XdpMemoryCreator::new_memory()` will
    /// hand to Wasmtime during the next `FilterPlugin::instantiate()` call.
    ///
    /// Must be called on the same thread as `instantiate()`, and before it.
    /// Instance creation is sequential during startup so this is safe.
    pub fn set_instantiation_handle(&self, handle: Arc<UmemHandle>) {
        *self.memory_creator.current_handle.lock().unwrap() = handle;
    }

    /// Create a windowed `UmemHandle` for a specific WASM instance partition.
    ///
    /// `partition_start_bytes` is the byte offset from the Umem base where this
    /// instance's WASM address 0 begins. The window extends from that offset to
    /// the end of the full Umem, so the plugin can access both its own scratchpad
    /// and all packet frames above the reserved region.
    ///
    /// # Panics
    /// Panics if `partition_start_bytes >= self.umem.size`.
    pub fn make_partition_handle(&self, partition_start_bytes: u32) -> Arc<UmemHandle> {
        assert!(
            (partition_start_bytes as usize) < self.umem.size,
            "partition_start_bytes ({}) must be less than total Umem size ({})",
            partition_start_bytes,
            self.umem.size
        );
        Arc::new(UmemHandle {
            ptr: self.umem.ptr,
            size: self.umem.size,
            partition_start_bytes,
            reserved_frames: self.umem.reserved_frames,
            frame_size: self.umem.frame_size,
        })
    }
}