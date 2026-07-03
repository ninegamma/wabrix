use std::cell::RefCell;
use dashmap::DashMap;
use std::sync::Arc;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, LinearMemory, MemoryCreator, MemoryType, OptLevel, Result};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use xbricks_protocol::BatchMetadata;
use xsk_rs::Umem;
use xbricks_base::mailbox::{control_block_frames, MAILBOX_STRIDE};
use crate::wasm_engine;

// A thread-local slot to securely hold the partition handle during instantiation
thread_local! {
    static CURRENT_INSTANTIATION_HANDLE: RefCell<Option<Arc<UmemHandle>>> = const { RefCell::new(None) };
}

// --- 1. Memory Layer (The Zero-Copy Bridge) ---

/// Thread-safe handle to the mmap'd AF_XDP UMEM region.
///
/// Memory layout (host-side byte offsets from Umem base):
/// ```
/// [0 .. control_block_bytes)              Control Block: per-instance BatchMetadata mailboxes
/// [control_block_bytes .. reserved_bytes) Per-instance WASM heap/stack partitions
/// [reserved_bytes .. total_size)          AF_XDP payload frames (accessible to all instances)
/// ```
/// For instance N, WASM address 0 maps to `control_block_bytes + N * scratchpad_size`.
#[derive(Clone)]
pub struct UmemHandle {
    pub ptr: *mut u8,
    /// Total Umem size in bytes (`frame_count * frame_size`).
    pub size: usize,
    /// Byte offset from `ptr` where this instance's WASM address 0 begins.
    pub partition_start_bytes: u32,
    /// Number of frames at the start of the UMEM exclusively reserved for WASM plugins.
    /// Byte range: [0, reserved_frames * frame_size).
    pub reserved_frames: u32,
    pub frame_size: u32,
    /// Number of WASM plugin instances sharing this UMEM. Determines Control Block size.
    pub instance_count: u32,
}

impl UmemHandle {
    /// Number of UMEM frames occupied by the Control Block (all per-instance mailboxes).
    #[inline]
    pub fn control_block_frame_count(&self) -> u32 {
        control_block_frames(self.instance_count, self.frame_size)
    }

    /// Byte size of the Control Block at UMEM offset 0.
    #[inline]
    pub fn control_block_bytes(&self) -> usize {
        self.control_block_frame_count() as usize * self.frame_size as usize
    }

    /// UMEM-relative byte offset to instance N's `BatchMetadata` mailbox.
    #[inline]
    pub fn mailbox_offset(&self, instance_idx: u32) -> u32 {
        instance_idx * MAILBOX_STRIDE as u32
    }

    /// Raw pointer to instance N's `BatchMetadata` within the shared UMEM.
    ///
    /// # Safety
    /// Caller must ensure exclusive access to this instance's mailbox slice and that
    /// `instance_idx < instance_count`. Control Block frames are excluded from the
    /// AF_XDP Fill Queue so the NIC never DMAs here.
    #[inline]
    pub unsafe fn mailbox_ptr(&self, instance_idx: u32) -> *mut BatchMetadata {
        // SAFETY: caller guarantees the slice is exclusively owned.
        unsafe { self.ptr.add(self.mailbox_offset(instance_idx) as usize) as *mut BatchMetadata }
    }

    /// Byte size of the region reserved exclusively for WASM plugins: [0, reserved_bytes()).
    #[inline]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_frames as usize * self.frame_size as usize
    }

    /// Size of the WASM linear memory window: from `partition_start_bytes` to end of Umem.
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
pub struct XdpMemoryCreator;

unsafe impl MemoryCreator for XdpMemoryCreator {
    fn new_memory(
        &self,
        _ty: MemoryType,
        _minimum: usize,
        _maximum: Option<usize>,
        _reserved: Option<usize>,
        _guard_size: usize,
    ) -> std::result::Result<Box<dyn LinearMemory>, String> {
        // Read and instantly consume the handle from THIS specific thread
        let handle = CURRENT_INSTANTIATION_HANDLE.with(|slot| {
            slot.borrow_mut().take()
        }).ok_or_else(|| "XdpMemoryCreator: No UmemHandle was set for this thread prior to instantiation!".to_string())?;

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

// Wasmtime generates the trait path based on your package name
impl crate::wasm_engine::xbricks::plugins::logging::Host for MyHostState {
    fn log(&mut self, level: u32, message: String) {
        // Map the u32 back to Rust log levels
        match level {
            1 => log::error!("WASM Node: {}", message),
            2 => log::warn!("WASM Node: {}", message),
            3 => log::info!("WASM Node: {}", message),
            4 => log::info!("WASM Node: {}", message),
            _ => log::trace!("WASM Node: {}", message),
        }
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
    pub fn new(frame_size: u32, reserved_frames: u32, frame_count: u32, umem: Arc<Umem>, instance_count: u32) -> Result<Self> {
        let min_ctrl = control_block_frames(instance_count, frame_size);
        if reserved_frames < min_ctrl {
            return Err(wasmtime::Error::msg(format!(
                "WasmManager: reserved_frames ({}) is too small to hold the Control Block \
                 ({} frames needed for {} instances × {} B mailbox)",
                reserved_frames, min_ctrl, instance_count, MAILBOX_STRIDE
            )));
        }

        let umem_ptr = umem.as_ptr();
        let total_umem_size = frame_count as usize * frame_size as usize;
        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            size: total_umem_size,
            partition_start_bytes: 0,
            reserved_frames,
            frame_size,
            instance_count,
        });

        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_memory64(true)
            .memory_reservation(total_umem_size as u64)
            .memory_may_move(false) // Critical for AF_XDP stability
            .cranelift_opt_level(OptLevel::Speed)
            .with_host_memory(Arc::new(XdpMemoryCreator))
            // Disable mmap-based image initialization because we are using a custom hardware UMEM creator:
            .memory_init_cow(false);

        let engine = Engine::new(&config)?;
        let mut linker: Linker<MyHostState>  = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;

        wasm_engine::EnginePlugin::add_to_linker::<MyHostState, HasSelf<MyHostState>>(
            &mut linker,
            |state| state
        ).expect("Failed to link EnginePlugin world");

        Ok(Self {
            engine,
            linker,
            cache: DashMap::new(),
            umem: umem_handle,
        })
    }

    /// Retrieves a compiled component from in-memory cache, disk cache, or compiles from source.
    /// Compiled artifacts are stored in /tmp/xbricks-wasm-cache/ keyed by filename + mtime,
    /// so repeated dataplane restarts skip the Cranelift compilation step entirely.
    pub fn get_component(&self, path: &str) -> Result<Component> {
        use dashmap::mapref::entry::Entry;

        match self.cache.entry(path.to_string()) {
            Entry::Occupied(entry) => {
                // Another thread already compiled it. Return the cached Arc.
                Ok(entry.get().clone())
            }
            Entry::Vacant(entry) => {
                // We are the first thread! Compile or load from disk.
                let component = Self::load_or_compile(&self.engine, path)?;
                entry.insert(component.clone());
                Ok(component)
            }
        }
    }

    fn load_or_compile(engine: &Engine, wasm_path: &str) -> Result<Component> {
        if let Some(cache_path) = Self::disk_cache_path(wasm_path) {
            if cache_path.exists() {
                // SAFETY: These files are written exclusively by Component::serialize() below,
                // from WASM files under our own control on the same machine.
                match unsafe { Component::deserialize_file(engine, &cache_path) } {
                    Ok(component) => {
                        log::info!("Loaded WASM from disk cache: {}", wasm_path);
                        return Ok(component);
                    }
                    Err(e) => {
                        log::warn!("WASM disk cache stale ({}), recompiling: {}", e, wasm_path);
                        let _ = std::fs::remove_file(&cache_path);
                    }
                }
            }

            log::info!("Compiling WASM component (this may take a few seconds): {}", wasm_path);
            let component = Component::from_file(engine, wasm_path)?;

            let dir = cache_path.parent().unwrap_or_else(|| std::path::Path::new("/tmp"));
            let _ = std::fs::create_dir_all(dir);
            match component.serialize() {
                Ok(bytes) => {
                    if let Err(e) = std::fs::write(&cache_path, &bytes) {
                        log::warn!("Failed to write WASM disk cache: {}", e);
                    } else {
                        log::info!("WASM compiled artifact cached at {:?}", cache_path);
                    }
                }
                Err(e) => log::warn!("Failed to serialize WASM component for caching: {}", e),
            }

            Ok(component)
        } else {
            // No cache path derivable (unusual path) — compile directly
            Component::from_file(engine, wasm_path)
        }
    }

    /// Derives a disk cache path for a given .wasm file.
    /// The modification time of the source file is embedded in the name so that
    /// rebuilding the WASM automatically invalidates the cache.
    fn disk_cache_path(wasm_path: &str) -> Option<std::path::PathBuf> {
        let mtime = std::fs::metadata(wasm_path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let stem = std::path::Path::new(wasm_path).file_stem()?.to_str()?;
        let cache_dir = std::path::Path::new("/tmp/xbricks-wasm-cache");
        Some(cache_dir.join(format!("{}-{}.cwasm", stem, mtime)))
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
        CURRENT_INSTANTIATION_HANDLE.with(|slot| {
            *slot.borrow_mut() = Some(handle);
        });
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
            instance_count: self.umem.instance_count,
        })
    }
}