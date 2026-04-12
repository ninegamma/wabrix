use dashmap::DashMap;
use std::sync::Arc;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, LinearMemory, MemoryCreator, MemoryType, OptLevel, Result};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use xsk_rs::Umem;
// --- 1. Memory Layer (The Zero-Copy Bridge) ---

/// Thread-safe handle to the mmap'd AF_XDP UMEM region.
#[derive(Clone)]
pub struct UmemHandle {
    pub ptr: *mut u8,
    pub size: usize,
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
}

// SAFETY: UMEM is a fixed hardware-backed buffer managed by the router lifecycle.
unsafe impl Send for UmemHandle {}
unsafe impl Sync for UmemHandle {}

unsafe impl LinearMemory for UmemHandle {
    fn byte_size(&self) -> usize { self.size }
    fn byte_capacity(&self) -> usize { self.size }
    fn grow_to(&mut self, _new_size: usize) -> Result<()> {
        Err(wasmtime::Error::msg("XBricks: Memory growth disabled for zero-copy UMEM"))
    }

    fn as_ptr(&self) -> *mut u8 { self.ptr }
}

pub struct XdpMemoryCreator {
    pub handle: Arc<UmemHandle>,
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
        // Hand out a clone of the handle which implements LinearMemory
        Ok(Box::new((*self.handle).clone()))
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
}

impl WasmManager {
    pub fn new(frame_size: u32, reserved_frames: u32, frame_count: u32, umem: Arc<Umem>) -> Result<Self> {
        let umem_ptr = umem.as_ptr();
        let size = frame_count as usize * frame_size as usize;
        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            size,
            reserved_frames,
            frame_size,
        });
        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_memory64(true)
            .memory_reservation(size as u64)
            .memory_may_move(false) // Critical for AF_XDP stability
            .cranelift_opt_level(OptLevel::Speed)
            .with_host_memory(Arc::new(XdpMemoryCreator { handle: umem_handle.clone() }))
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
}