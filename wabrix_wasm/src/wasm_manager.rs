use crate::wasm_engine;
use dashmap::DashMap;
use std::cell::RefCell;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use wabrix_api::BatchMetadata;
use wabrix_base::mailbox::MAILBOX_STRIDE;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, LinearMemory, MemoryCreator, MemoryType, OptLevel, Result};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use xsk_core::Umem;

/// Maximum virtual address space reserved for each instance's lower Wasm region.
/// The final `BatchMetadata` bytes of this region are reserved for the mailbox.
///
/// The AF_XDP UMEM is already mapped and registered with the kernel by
/// `xsk-core`. Each instance maps an alias of those same memfd-backed pages
/// after its own ordinary Wasm region, so no DMA registration is moved.
const MAX_WASM_HEAP_BYTES: usize = 2 * 1024 * 1024 * 1024;
const WASM_PAGE_BYTES: usize = 64 * 1024;

// A thread-local slot to securely hold the partition handle during instantiation
thread_local! {
    static CURRENT_INSTANTIATION_HANDLE: RefCell<Option<Arc<UmemHandle>>> = const { RefCell::new(None) };
}

// --- 1. Memory Layer (The Zero-Copy Bridge) ---

/// Thread-safe handle to the mmap'd AF_XDP UMEM region.
///
/// The UMEM remains at the address allocated and registered by `xsk-core`; moving
/// that original mapping would invalidate the AF_XDP DMA registration. The custom
/// `MemoryCreator` reserves a private contiguous range for each instance, maps
/// the ordinary Wasm region at its start, and aliases the same memfd-backed UMEM
/// pages immediately after it. Wasm memory 0 therefore presents one contiguous
/// range without copying packet data:
///
/// ```text
/// Wasm address 0                                      Wasm UMEM offset
///      |                                                   |
///      v                                                   v
///      [ ordinary Wasm region ][ private mailbox ][ UMEM alias ]
///      ^                                                   ^
///      |                                                   |
///      linear memory base                                  same pages as AF_XDP UMEM
/// ```
///
/// The UMEM's internal layout is unchanged and uses offsets relative to the
/// original UMEM host pointer. Mailboxes are private to each Wasm mapping:
/// ```text
/// [0 .. total_size)                                       AF_XDP payload frames
/// ```
/// Each instance aliases the complete UMEM, so packet descriptors remain
/// UMEM-relative while mailbox offsets are Wasm-linear addresses. The host
/// passes `umem_offset = MAX_WASM_HEAP_BYTES` through the existing ABI. The
/// plugin then computes:
/// ```text
/// linear_memory_base + mailbox_offset
///     == this instance's private BatchMetadata mailbox
/// linear_memory_base + MAX_WASM_HEAP_BYTES + desc_addr
///     == host UMEM base + desc_addr
/// ```
#[derive(Clone)]
pub struct UmemHandle {
    pub ptr: *mut u8,
    /// Duplicated memfd which backs the AF_XDP UMEM pages.
    pub backing_fd: Arc<OwnedFd>,
    /// Base address of this instance's stitched linear memory.
    linear_base: Arc<AtomicUsize>,
    /// Total Umem size in bytes (`frame_count * frame_size`).
    pub size: usize,
}

impl UmemHandle {
    /// Wasm-linear-memory byte offset of this instance's private mailbox.
    #[inline]
    pub fn mailbox_offset(&self, _instance_idx: u32) -> u32 {
        (MAX_WASM_HEAP_BYTES - MAILBOX_STRIDE) as u32
    }

    /// Raw pointer to this instance's mailbox in its private Wasm memory.
    ///
    /// # Safety
    /// Caller must ensure exclusive access to this instance's mailbox slice and that
    /// the linear memory has already been created for this instance.
    #[inline]
    pub unsafe fn mailbox_ptr(&self, instance_idx: u32) -> *mut BatchMetadata {
        let base = self.linear_base.load(Ordering::Acquire);
        debug_assert_ne!(base, 0, "Wasm linear memory has not been created");
        // SAFETY: the MemoryCreator maps the mailbox offset inside this
        // instance's private linear-memory mapping before the Wasm instance is
        // exposed to the host.
        unsafe {
            (base as *mut u8).add(self.mailbox_offset(instance_idx) as usize) as *mut BatchMetadata
        }
    }

    /// Positive Wasm-linear offset where the shared UMEM begins.
    #[inline]
    pub fn wasm_umem_offset(&self) -> u32 {
        MAX_WASM_HEAP_BYTES as u32
    }

    #[inline]
    pub fn linear_base(&self) -> usize {
        self.linear_base.load(Ordering::Acquire)
    }
}

// SAFETY: UMEM is a fixed hardware-backed buffer managed by the router lifecycle.
unsafe impl Send for UmemHandle {}
unsafe impl Sync for UmemHandle {}

/// A Wasm linear memory whose lower virtual-address region is the ordinary Wasm
/// region and whose upper region aliases the shared AF_XDP UMEM.
struct StitchedLinearMemory {
    handle: Arc<UmemHandle>,
    base_ptr: *mut u8,
}

// SAFETY: `base_ptr` identifies a private virtual mapping owned by this
// object. Wasmtime may move the object between threads, but it never permits
// concurrent mutable access to one LinearMemory implementation.
unsafe impl Send for StitchedLinearMemory {}
unsafe impl Sync for StitchedLinearMemory {}

impl StitchedLinearMemory {
    fn new(handle: Arc<UmemHandle>, initial_size: usize) -> std::result::Result<Self, String> {
        let heap_size = align_up(initial_size, WASM_PAGE_BYTES)?;
        if heap_size > MAX_WASM_HEAP_BYTES {
            return Err(format!(
                "Wasm minimum memory exceeds {} bytes",
                MAX_WASM_HEAP_BYTES
            ));
        }

        let mapping_size = MAX_WASM_HEAP_BYTES
            .checked_add(handle.size)
            .ok_or_else(|| "stitched Wasm mapping size overflow".to_string())?;
        // SAFETY: A null hint lets the kernel choose an unused contiguous range;
        // the range is private to this memory object until Drop.
        let reservation = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapping_size,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if reservation == libc::MAP_FAILED {
            return Err(format!(
                "could not reserve {} bytes for stitched Wasm memory: {}",
                mapping_size,
                std::io::Error::last_os_error()
            ));
        }
        let base_addr = reservation as usize;

        // SAFETY: The range was reserved above and is exclusively owned by this
        // object until Drop. Mapping the complete lower portion makes it part
        // of the accessible linear memory; physical pages remain
        // demand-allocated by the kernel.
        let mapped = unsafe {
            libc::mmap(
                base_addr as *mut libc::c_void,
                MAX_WASM_HEAP_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
                -1,
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            // SAFETY: The preceding mmap reserved exactly this private range.
            unsafe { libc::munmap(base_addr as *mut libc::c_void, mapping_size) };
            return Err(format!(
                "could not map initial Wasm heap: {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: The upper range is inside the reservation made above, and
        // `backing_fd` refers to the same pages registered as the AF_XDP UMEM.
        let umem_mapping = unsafe {
            libc::mmap(
                (base_addr + MAX_WASM_HEAP_BYTES) as *mut libc::c_void,
                handle.size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_FIXED,
                handle.backing_fd.as_raw_fd(),
                0,
            )
        };
        if umem_mapping == libc::MAP_FAILED {
            // SAFETY: The reservation is owned by this object and the lower
            // mapping has not escaped this constructor.
            unsafe { libc::munmap(base_addr as *mut libc::c_void, mapping_size) };
            return Err(format!(
                "could not alias AF_XDP UMEM into Wasm memory: {}",
                std::io::Error::last_os_error()
            ));
        }

        handle.linear_base.store(base_addr, Ordering::Release);

        Ok(Self {
            handle,
            base_ptr: base_addr as *mut u8,
        })
    }

    fn total_size(&self) -> usize {
        MAX_WASM_HEAP_BYTES + self.handle.size
    }
}

fn align_up(value: usize, alignment: usize) -> std::result::Result<usize, String> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| "Wasm memory size overflow".to_string())
}

unsafe impl LinearMemory for StitchedLinearMemory {
    /// The exposed memory contains both the ordinary heap and the UMEM.
    fn byte_size(&self) -> usize {
        self.total_size()
    }

    fn byte_capacity(&self) -> usize {
        self.total_size()
    }

    fn grow_to(&mut self, new_size: usize) -> Result<()> {
        if new_size > self.total_size() {
            return Err(wasmtime::Error::msg(
                "Wasm memory growth exceeds the stitched mapping",
            ));
        }
        Ok(())
    }

    fn as_ptr(&self) -> *mut u8 {
        self.base_ptr
    }
}

impl Drop for StitchedLinearMemory {
    fn drop(&mut self) {
        // SAFETY: This is the exact reservation created by `new`; the UMEM
        // starts at its upper boundary. The original AF_XDP mapping is owned
        // by xsk-core and is deliberately left mapped.
        unsafe {
            libc::munmap(
                self.base_ptr as *mut libc::c_void,
                MAX_WASM_HEAP_BYTES + self.handle.size,
            );
            self.handle.linear_base.store(0, Ordering::Release);
        }
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
        let handle = CURRENT_INSTANTIATION_HANDLE
            .with(|slot| slot.borrow_mut().take())
            .ok_or_else(|| {
                "XdpMemoryCreator: No UmemHandle was set for this thread prior to instantiation!"
                    .to_string()
            })?;

        let initial_size = _minimum;
        Ok(Box::new(StitchedLinearMemory::new(handle, initial_size)?))
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
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

// Wasmtime generates the trait path based on your package name
impl crate::wasm_engine::wabrix::plugins::logging::Host for MyHostState {
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
    pub fn new(frame_size: u32, frame_count: u32, umem: Arc<Umem>) -> Result<Self> {
        // The UMEM is already shared with AF_XDP, so `Arc::get_mut` cannot be used.
        // Convert the read-only raw pointer to a writable one; the disjoint-access
        // invariants documented on `UmemHandle` must be upheld by every caller.
        let umem_ptr = umem.as_ptr() as *mut u8;
        let total_umem_size = frame_count as usize * frame_size as usize;
        let backing_fd = umem
            .backing_fd()
            .map_err(|e| wasmtime::Error::msg(format!("failed to duplicate UMEM fd: {e}")))?;
        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            // SAFETY: `backing_fd` is a newly duplicated, owned descriptor.
            backing_fd: Arc::new(unsafe { OwnedFd::from_raw_fd(backing_fd) }),
            linear_base: Arc::new(AtomicUsize::new(0)),
            size: total_umem_size,
        });

        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_memory64(true)
            .memory_reservation((MAX_WASM_HEAP_BYTES + total_umem_size) as u64)
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

    /// Retrieves a compiled component from in-memory cache, disk cache, or compiles from source.
    /// Compiled artifacts are stored in /tmp/wabrix-wasm-cache/ keyed by filename + mtime,
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

    /// Create a handle for a new Wasm instance sharing this UMEM.
    pub fn make_partition_handle(&self) -> Arc<UmemHandle> {
        Arc::new(UmemHandle {
            ptr: self.umem.ptr,
            backing_fd: self.umem.backing_fd.clone(),
            linear_base: Arc::new(AtomicUsize::new(0)),
            size: self.umem.size,
        })
    }
}
