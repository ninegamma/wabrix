use crate::wasm_engine;
use dashmap::DashMap;
use std::cell::RefCell;
use std::sync::Arc;
use wabrix_api::BatchMetadata;
use wabrix_base::mailbox::{control_block_frames, MAILBOX_STRIDE};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, LinearMemory, MemoryCreator, MemoryType, OptLevel, Result};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use xsk_rs::Umem;

// A thread-local slot to securely hold the memory handle during instantiation
thread_local! {
    static CURRENT_INSTANTIATION_HANDLE: RefCell<Option<Arc<UmemHandle>>> = const { RefCell::new(None) };
}

// --- Memory Layer (The Zero-Copy Bridge) ---

/// Thread-safe handle to the mmap'd AF_XDP UMEM region.
///
/// In the multi-memory architecture, this handle backs the secondary WASM memory instance.
/// Address zero in this memory maps exactly to the physical start of the UMEM region,
/// eliminating all partition offset calculations.
///
/// Memory layout (host-side byte offsets from Umem base):
/// * [0 .. reserved_bytes - control_block_bytes)            Unused / Reserved frames
/// * [reserved_bytes - control_block_bytes .. reserved_bytes) Control Block (per-instance mailboxes)
/// * [reserved_bytes .. total_size)                           AF_XDP payload frames
#[derive(Clone)]
pub struct UmemHandle {
    pub ptr: *mut u8,
    /// Total Umem size in bytes (`frame_count * frame_size`).
    pub size: usize,
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

    /// Byte size of the Control Block located at the end of the reserved region.
    #[inline]
    pub fn control_block_bytes(&self) -> usize {
        self.control_block_frame_count() as usize * self.frame_size as usize
    }

    /// UMEM-relative byte offset to the instance's `BatchMetadata` mailbox.
    #[inline]
    pub fn mailbox_offset(&self, instance_idx: u32) -> u32 {
        self.reserved_bytes() as u32 - (instance_idx + 1) * MAILBOX_STRIDE as u32
    }

    /// Raw pointer to the instance's `BatchMetadata` within the shared UMEM.
    ///
    /// # Safety
    /// Caller must ensure exclusive access to this instance's mailbox slice and that
    /// instance_idx < instance_count. Control Block frames are excluded from the
    /// AF_XDP Fill Queue so the NIC never DMAs here.
    #[inline]
    pub unsafe fn mailbox_ptr(&self, instance_idx: u32) -> *mut BatchMetadata {
        // SAFETY: caller guarantees the slice is exclusively owned.
        unsafe { self.ptr.add(self.mailbox_offset(instance_idx) as usize) as *mut BatchMetadata }
    }

    /// Byte size of the region reserved exclusively for WASM plugins and their mailboxes.
    #[inline]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_frames as usize * self.frame_size as usize
    }
}

// SAFETY: UMEM is a fixed hardware-backed buffer managed by the router lifecycle.
unsafe impl Send for UmemHandle {}
unsafe impl Sync for UmemHandle {}

unsafe impl LinearMemory for UmemHandle {
    /// The visible window maps the entire UMEM block directly.
    fn byte_size(&self) -> usize {
        self.size
    }
    fn byte_capacity(&self) -> usize {
        self.size
    }
    fn grow_to(&mut self, _new_size: usize) -> Result<()> {
        Err(wasmtime::Error::msg(
            "XBricks: Memory growth disabled for zero-copy UMEM",
        ))
    }

    /// WASM address zero maps directly to the physical start of the UMEM.
    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

/// Per-engine custom memory allocator.
pub struct XdpMemoryCreator;

struct HeapMemory {
    ptr: *mut u8,
    size: usize,
    capacity: usize,
}

impl HeapMemory {
    fn new(
        minimum: usize,
        maximum: Option<usize>,
        reserved: Option<usize>,
    ) -> std::result::Result<Self, String> {
        let capacity = reserved.or(maximum).unwrap_or(minimum).max(minimum);
        // SAFETY: Anonymous private mmap returns an exclusively owned, page-aligned region.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().to_string());
        }
        Ok(Self {
            ptr: ptr.cast(),
            size: minimum,
            capacity,
        })
    }
}

impl Drop for HeapMemory {
    fn drop(&mut self) {
        // SAFETY: `ptr/capacity` are the exact region returned by `mmap` in `new`.
        unsafe { libc::munmap(self.ptr.cast(), self.capacity) };
    }
}

// SAFETY: Wasmtime owns each memory exclusively and synchronizes access through its Store.
unsafe impl Send for HeapMemory {}
unsafe impl Sync for HeapMemory {}

unsafe impl LinearMemory for HeapMemory {
    fn byte_size(&self) -> usize {
        self.size
    }
    fn byte_capacity(&self) -> usize {
        self.capacity
    }
    fn grow_to(&mut self, new_size: usize) -> Result<()> {
        if new_size > self.capacity {
            return Err(wasmtime::Error::msg("WASM heap exceeds reserved capacity"));
        }
        self.size = new_size;
        Ok(())
    }
    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

unsafe impl MemoryCreator for XdpMemoryCreator {
    fn new_memory(
        &self,
        ty: MemoryType,
        minimum: usize,
        maximum: Option<usize>,
        reserved: Option<usize>,
        _guard_size: usize,
    ) -> std::result::Result<Box<dyn LinearMemory>, String> {
        // The composed UMEM module is deliberately declared as one page with no maximum.
        // All Rust-produced memories have a finite maximum and receive ordinary anonymous
        // storage, keeping heap/stack/static data isolated from packet memory.
        let is_umem_adapter = ty.minimum() == 1 && ty.maximum() == Some(65_536);
        if !is_umem_adapter {
            return HeapMemory::new(minimum, maximum, reserved)
                .map(|memory| Box::new(memory) as Box<dyn LinearMemory>);
        }
        let handle = CURRENT_INSTANTIATION_HANDLE
            .with(|slot| slot.borrow().clone())
            .ok_or_else(|| {
                "XdpMemoryCreator: No UmemHandle was set for this thread prior to instantiation!"
                    .to_string()
            })?;

        if minimum > handle.size {
            return Err("UMEM is smaller than the adapter memory minimum".to_string());
        }
        Ok(Box::new((*handle).clone()))
    }
}

// --- Host State Layer (WASI & Custom) ---

pub struct MyHostState {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
    pub umem: Arc<UmemHandle>,
}

impl WasiView for MyHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

impl crate::wasm_engine::wabrix::plugins::logging::Host for MyHostState {
    fn log(&mut self, level: u32, message: String) {
        match level {
            1 => log::error!("WASM Node: {}", message),
            2 => log::warn!("WASM Node: {}", message),
            3 => log::info!("WASM Node: {}", message),
            4 => log::info!("WASM Node: {}", message),
            _ => log::trace!("WASM Node: {}", message),
        }
    }
}

// --- Management Layer (Orchestration) ---

pub struct WasmManager {
    engine: Engine,
    linker: Linker<MyHostState>,
    cache: DashMap<String, Component>,
    umem: Arc<UmemHandle>,
}

impl WasmManager {
    pub fn new(
        frame_size: u32,
        reserved_frames: u32,
        frame_count: u32,
        umem: Arc<Umem>,
        instance_count: u32,
    ) -> Result<Self> {
        let min_ctrl = control_block_frames(instance_count, frame_size);
        let min_reserved = min_ctrl + instance_count;
        if reserved_frames < min_reserved {
            return Err(wasmtime::Error::msg(format!(
                "WasmManager: reserved_frames ({}) is too small. Need at least {} frames: \
                 {} for the Control Block ({} instances × {} B mailbox) + {} scratchpad frames",
                reserved_frames,
                min_reserved,
                min_ctrl,
                instance_count,
                MAILBOX_STRIDE,
                instance_count
            )));
        }

        let umem_ptr = umem.as_ptr();
        let total_umem_size = frame_count as usize * frame_size as usize;

        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            size: total_umem_size,
            reserved_frames,
            frame_size,
            instance_count,
        });

        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_multi_memory(true)
            .wasm_memory64(true)
            .memory_reservation(total_umem_size as u64)
            // UMEM has no host-owned guard mapping after its physical allocation. Keeping the
            // guard at zero makes Cranelift retain explicit bounds checks for both memories.
            .memory_guard_size(0)
            .memory_may_move(false) // Critical for AF_XDP stability
            .cranelift_opt_level(OptLevel::Speed)
            .with_host_memory(Arc::new(XdpMemoryCreator))
            // Disable mmap-based image initialization because we are using a custom hardware UMEM creator:
            .memory_init_cow(false);

        let engine = Engine::new(&config)?;
        let mut linker: Linker<MyHostState> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;

        wasm_engine::EnginePlugin::add_to_linker::<MyHostState, HasSelf<MyHostState>>(
            &mut linker,
            |state| state,
        )
        .expect("Failed to link EnginePlugin world");

        Ok(Self {
            engine,
            linker,
            cache: DashMap::new(),
            umem: umem_handle,
        })
    }

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

            log::info!(
                "Compiling WASM component (this may take a few seconds): {}",
                wasm_path
            );
            let component = Component::from_file(engine, wasm_path)?;

            let dir = cache_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("/tmp"));
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
            Component::from_file(engine, wasm_path)
        }
    }

    fn disk_cache_path(wasm_path: &str) -> Option<std::path::PathBuf> {
        let mtime = std::fs::metadata(wasm_path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let stem = std::path::Path::new(wasm_path).file_stem()?.to_str()?;
        let cache_dir = std::path::Path::new("/tmp/wabrix-wasm-cache");
        Some(cache_dir.join(format!("{}-{}.cwasm", stem, mtime)))
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn linker(&self) -> &Linker<MyHostState> {
        &self.linker
    }

    pub fn umem(&self) -> Arc<UmemHandle> {
        self.umem.clone()
    }

    pub fn set_instantiation_handle(&self, handle: Arc<UmemHandle>) {
        CURRENT_INSTANTIATION_HANDLE.with(|slot| {
            *slot.borrow_mut() = Some(handle);
        });
    }

    /// Retrieves the UMEM handle for a WASM instance.
    ///
    /// In the multi-memory architecture, all instances map the entire UMEM
    /// starting from address zero in their secondary memory instance.
    pub fn make_instance_handle(&self) -> Arc<UmemHandle> {
        self.umem.clone()
    }
}
