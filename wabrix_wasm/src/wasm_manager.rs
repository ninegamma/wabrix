use crate::wasm_engine;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
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
const SHARED_STATE_HEADER_BYTES: usize = 64;
const SHARED_STATE_HEADER_VERSION: u32 = 1;
const SHARED_STATE_MAGIC: [u8; 8] = *b"WABSHR01";
const SHARED_STATE_MAX_ALIGNMENT: u32 = 64;
/// Temporary capacity ceiling until plugin state descriptors choose this per component.
pub(crate) const MAX_SHARED_STATE_PAYLOAD_BYTES: usize = 127 * 1024;
const WASM32_ADDRESS_SPACE_BYTES: u64 = u32::MAX as u64 + 1;

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
/// the ordinary Wasm region at its start, aliases the UMEM pages at 2 GiB, and
/// maps a per-node state memfd after the page-aligned UMEM. Wasm memory 0
/// therefore presents one contiguous range without copying packet data:
///
/// ```text
/// Wasm address 0                 2 GiB                       tail
///      |                           |                            |
///      v                           v                            v
///      [ private Wasm region ][ UMEM alias ][ shared state alias ]
///      ^                           ^                            ^
///      |                           |                            |
///      linear memory base          xsk-core pages               per-node memfd
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
/// linear_memory_base + shared_state_offset
///     == this node's shared-state memfd mapping
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
    /// Dedicated backing shared by the active and staged runtime for this node.
    shared_state: Option<Arc<SharedStateRegion>>,
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
    /// Caller must ensure this instance's linear-memory mapping stays alive and that
    /// it has exclusive access to the mailbox slice for the pointer's use. Returns
    /// an error if no mapping is currently published for this handle.
    #[inline]
    pub unsafe fn mailbox_ptr(
        &self,
        instance_idx: u32,
    ) -> std::result::Result<*mut BatchMetadata, &'static str> {
        let base = self.linear_base.load(Ordering::Acquire);
        if base == 0 {
            return Err("Wasm linear memory has not been created");
        }
        // SAFETY: the MemoryCreator maps the mailbox offset inside this
        // instance's private linear-memory mapping before the Wasm instance is
        // exposed to the host.
        Ok(unsafe {
            (base as *mut u8).add(self.mailbox_offset(instance_idx) as usize) as *mut BatchMetadata
        })
    }

    /// Positive Wasm-linear offset where the shared UMEM begins.
    #[inline]
    pub fn wasm_umem_offset(&self) -> u32 {
        MAX_WASM_HEAP_BYTES as u32
    }

    /// Guest-linear offset of this instance's shared-state mapping, if present.
    pub fn shared_state_offset(&self) -> std::result::Result<Option<u32>, String> {
        let Some(shared_state) = self.shared_state.as_ref() else {
            return Ok(None);
        };
        if shared_state.mapping_size == 0 {
            return Err("shared-state mapping size must be greater than zero".to_string());
        }
        let aligned_umem_size = align_up(self.size, WASM_PAGE_BYTES)?;
        let offset = MAX_WASM_HEAP_BYTES
            .checked_add(aligned_umem_size)
            .ok_or_else(|| "shared-state offset overflow".to_string())?;
        let offset = u32::try_from(offset)
            .map_err(|_| "shared-state offset exceeds the guest address range".to_string())?;
        Ok(Some(offset))
    }

    /// Payload capacity reserved in the dedicated shared-state mapping.
    pub fn shared_state_capacity(&self) -> Option<usize> {
        self.shared_state
            .as_ref()
            .map(|state| state.payload_capacity)
    }

    /// Validate a plugin's state layout and return its typed payload location.
    /// The WIT import invokes this during component initialization, after the
    /// memory creator has installed this instance's shared mapping.
    pub(crate) fn attach_shared_state(
        &self,
        type_id: [u8; 16],
        schema_version: u32,
        payload_size: u32,
        reserved_capacity: u32,
        required_alignment: u32,
        compatible_source_versions: &[u32],
    ) -> std::result::Result<AttachedSharedState, String> {
        let shared_state = self
            .shared_state
            .as_ref()
            .ok_or_else(|| "this Wasm instance has no shared-state region".to_string())?;
        let _metadata_guard = shared_state
            .metadata_lock
            .lock()
            .map_err(|_| "shared-state metadata lock is poisoned".to_string())?;
        if schema_version == 0 {
            return Err("shared-state schema version must be nonzero".to_string());
        }
        if payload_size == 0
            || reserved_capacity < payload_size
            || reserved_capacity as usize > shared_state.payload_capacity
        {
            return Err(format!(
                "shared-state payload size {payload_size} and reserved capacity {reserved_capacity} are invalid (host maximum {})",
                shared_state.payload_capacity,
            ));
        }
        if required_alignment == 0
            || !required_alignment.is_power_of_two()
            || required_alignment > SHARED_STATE_MAX_ALIGNMENT
        {
            return Err(format!(
                "shared-state alignment must be a power of two no greater than {SHARED_STATE_MAX_ALIGNMENT}"
            ));
        }
        if compatible_source_versions
            .iter()
            .any(|version| *version == 0 || *version >= schema_version)
        {
            return Err("compatible source schemas must be older nonzero versions".to_string());
        }

        let linear_base = self.linear_base.load(Ordering::Acquire);
        if linear_base == 0 {
            return Err("shared-state linear memory is not mapped yet".to_string());
        }
        let region_offset = self
            .shared_state_offset()
            .map_err(|error| format!("invalid shared-state offset: {error}"))?
            .ok_or_else(|| "this Wasm instance has no shared-state region".to_string())?;
        let payload_offset = region_offset
            .checked_add(SHARED_STATE_HEADER_BYTES as u32)
            .ok_or_else(|| "shared-state payload offset overflow".to_string())?;
        if payload_offset % required_alignment != 0 {
            return Err(format!(
                "shared-state payload offset {payload_offset} does not meet alignment {required_alignment}"
            ));
        }

        let region_ptr = (linear_base as *mut u8).wrapping_add(region_offset as usize);
        let mut header = [0u8; SHARED_STATE_HEADER_BYTES];
        // SAFETY: The memory creator maps exactly `mapping_size` bytes of this
        // region at `region_offset` before the component can call this import.
        unsafe {
            std::ptr::copy_nonoverlapping(region_ptr, header.as_mut_ptr(), header.len());
        }

        if header.iter().all(|byte| *byte == 0) {
            let mut initialized_header = [0u8; SHARED_STATE_HEADER_BYTES];
            initialized_header[..8].copy_from_slice(&SHARED_STATE_MAGIC);
            write_header_u32(&mut initialized_header, 8, SHARED_STATE_HEADER_VERSION);
            write_header_u32(
                &mut initialized_header,
                12,
                SHARED_STATE_HEADER_BYTES as u32,
            );
            initialized_header[16..32].copy_from_slice(&type_id);
            write_header_u32(&mut initialized_header, 32, schema_version);
            write_header_u32(&mut initialized_header, 36, payload_size);
            write_header_u32(&mut initialized_header, 40, reserved_capacity);
            write_header_u32(&mut initialized_header, 44, SHARED_STATE_MAX_ALIGNMENT);

            // SAFETY: The freshly truncated memfd is mapped for the full region;
            // the payload range is exactly the declared capacity after its header.
            unsafe {
                std::ptr::write_bytes(
                    region_ptr.add(SHARED_STATE_HEADER_BYTES),
                    0,
                    shared_state.payload_capacity,
                );
                // Publish the magic last so a partial initialization is rejected.
                std::ptr::copy_nonoverlapping(
                    initialized_header.as_ptr().add(8),
                    region_ptr.add(8),
                    SHARED_STATE_HEADER_BYTES - 8,
                );
                std::ptr::copy_nonoverlapping(
                    initialized_header.as_ptr(),
                    region_ptr,
                    SHARED_STATE_MAGIC.len(),
                );
            }

            return Ok(AttachedSharedState {
                type_id,
                payload_offset,
                payload_capacity: reserved_capacity,
                current_schema_version: schema_version,
                current_payload_size: payload_size,
                requested_schema_version: schema_version,
                requested_payload_size: payload_size,
                requested_capacity: reserved_capacity,
                is_new: true,
                upgrade_required: false,
            });
        }

        if header[..8] != SHARED_STATE_MAGIC {
            return Err("shared-state header is neither empty nor valid".to_string());
        }
        if read_header_u32(&header, 8) != SHARED_STATE_HEADER_VERSION
            || read_header_u32(&header, 12) != SHARED_STATE_HEADER_BYTES as u32
        {
            return Err("unsupported shared-state header version".to_string());
        }
        if header[48..].iter().any(|byte| *byte != 0) {
            return Err("shared-state header has unsupported flags or reserved bytes".to_string());
        }
        if header[16..32] != type_id {
            return Err("shared-state type ID does not match the plugin".to_string());
        }

        let current_schema_version = read_header_u32(&header, 32);
        let current_payload_size = read_header_u32(&header, 36);
        let current_capacity = read_header_u32(&header, 40);
        let region_alignment = read_header_u32(&header, 44);
        if current_capacity as usize > shared_state.payload_capacity
            || reserved_capacity > current_capacity
            || region_alignment != SHARED_STATE_MAX_ALIGNMENT
            || current_payload_size == 0
            || current_payload_size > current_capacity
            || current_schema_version == 0
        {
            return Err("shared-state header contains invalid bounds".to_string());
        }

        let exact_match =
            current_schema_version == schema_version && current_payload_size == payload_size;
        let upgrade_required = current_schema_version < schema_version
            && compatible_source_versions.contains(&current_schema_version)
            && current_payload_size <= payload_size;
        if !exact_match && !upgrade_required {
            return Err(format!(
                "shared-state schema mismatch: region is version {current_schema_version} with {current_payload_size} bytes, plugin requires version {schema_version} with {payload_size} bytes"
            ));
        }

        Ok(AttachedSharedState {
            type_id,
            payload_offset,
            payload_capacity: current_capacity,
            current_schema_version,
            current_payload_size,
            requested_schema_version: schema_version,
            requested_payload_size: payload_size,
            requested_capacity: reserved_capacity,
            is_new: false,
            upgrade_required,
        })
    }

    /// Publish a completed guest append-only migration in the shared header.
    /// This is called only after the staged guest's upgrade callback succeeds.
    pub(crate) fn commit_shared_state_upgrade(
        &self,
        attachment: AttachedSharedState,
    ) -> std::result::Result<(), String> {
        if !attachment.upgrade_required {
            return Err("shared-state attachment does not require an upgrade".to_string());
        }
        let shared_state = self
            .shared_state
            .as_ref()
            .ok_or_else(|| "this Wasm instance has no shared-state region".to_string())?;
        let _metadata_guard = shared_state
            .metadata_lock
            .lock()
            .map_err(|_| "shared-state metadata lock is poisoned".to_string())?;

        if attachment.requested_schema_version <= attachment.current_schema_version
            || attachment.requested_payload_size < attachment.current_payload_size
            || attachment.requested_capacity > attachment.payload_capacity
        {
            return Err("shared-state upgrade target is not append-compatible".to_string());
        }

        let linear_base = self.linear_base.load(Ordering::Acquire);
        if linear_base == 0 {
            return Err("shared-state linear memory is not mapped".to_string());
        }
        let region_offset = self
            .shared_state_offset()
            .map_err(|error| format!("invalid shared-state offset: {error}"))?
            .ok_or_else(|| "this Wasm instance has no shared-state region".to_string())?;
        let region_ptr = (linear_base as *mut u8).wrapping_add(region_offset as usize);
        let mut header = [0u8; SHARED_STATE_HEADER_BYTES];
        // SAFETY: The memory creator mapped the complete shared-state region at
        // `region_offset` for the lifetime of this instance.
        unsafe {
            std::ptr::copy_nonoverlapping(region_ptr, header.as_mut_ptr(), header.len());
        }
        if header[..8] != SHARED_STATE_MAGIC
            || read_header_u32(&header, 8) != SHARED_STATE_HEADER_VERSION
            || read_header_u32(&header, 12) != SHARED_STATE_HEADER_BYTES as u32
            || header[16..32] != attachment.type_id
            || read_header_u32(&header, 32) != attachment.current_schema_version
            || read_header_u32(&header, 36) != attachment.current_payload_size
            || read_header_u32(&header, 40) != attachment.payload_capacity
            || header[48..].iter().any(|byte| *byte != 0)
        {
            return Err("shared-state header changed before upgrade commit".to_string());
        }
        if attachment.requested_payload_size > attachment.payload_capacity {
            return Err("shared-state upgrade exceeds reserved capacity".to_string());
        }

        write_header_u32(&mut header, 32, attachment.requested_schema_version);
        write_header_u32(&mut header, 36, attachment.requested_payload_size);
        // SAFETY: The header is mapped and exclusively protected from other
        // host metadata operations by `metadata_lock`; guests are quiescent at
        // this switchover point. Only the schema and size fields are changed.
        unsafe {
            std::ptr::copy_nonoverlapping(header.as_ptr().add(32), region_ptr.add(32), 8);
        }
        Ok(())
    }

    #[inline]
    pub fn linear_base(&self) -> usize {
        self.linear_base.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AttachedSharedState {
    pub(crate) type_id: [u8; 16],
    pub(crate) payload_offset: u32,
    pub(crate) payload_capacity: u32,
    pub(crate) current_schema_version: u32,
    pub(crate) current_payload_size: u32,
    pub(crate) requested_schema_version: u32,
    pub(crate) requested_payload_size: u32,
    pub(crate) requested_capacity: u32,
    pub(crate) is_new: bool,
    pub(crate) upgrade_required: bool,
}

fn read_header_u32(header: &[u8; SHARED_STATE_HEADER_BYTES], offset: usize) -> u32 {
    u32::from_le_bytes([
        header[offset],
        header[offset + 1],
        header[offset + 2],
        header[offset + 3],
    ])
}

fn write_header_u32(header: &mut [u8; SHARED_STATE_HEADER_BYTES], offset: usize, value: u32) {
    header[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

// SAFETY: UMEM is a fixed hardware-backed buffer managed by the router lifecycle.
unsafe impl Send for UmemHandle {}
unsafe impl Sync for UmemHandle {}

/// Host-owned, memfd-backed state region shared by the active and staged runtime.
///
/// The mapping is deliberately separate from AF_XDP UMEM. A fresh memfd is
/// zero-filled by the kernel and remains alive through this Arc until every
/// runtime handle and linear-memory mapping has been dropped.
pub(crate) struct SharedStateRegion {
    backing_fd: Arc<OwnedFd>,
    payload_capacity: usize,
    mapping_size: usize,
    metadata_lock: Mutex<()>,
}

impl SharedStateRegion {
    fn create(payload_capacity: usize) -> std::result::Result<Arc<Self>, String> {
        if payload_capacity == 0 || payload_capacity > MAX_SHARED_STATE_PAYLOAD_BYTES {
            return Err(format!(
                "shared-state payload capacity must be between 1 and {} bytes",
                MAX_SHARED_STATE_PAYLOAD_BYTES
            ));
        }

        let logical_size = SHARED_STATE_HEADER_BYTES
            .checked_add(payload_capacity)
            .ok_or_else(|| "shared-state region size overflow".to_string())?;
        let mapping_size = align_up(logical_size, WASM_PAGE_BYTES)?;
        let name = CString::new("wabrix-wasm-state")
            .map_err(|error| format!("invalid shared-state memfd name: {error}"))?;

        // SAFETY: `name` is a valid NUL-terminated string for the duration of
        // the syscall, and MFD_CLOEXEC prevents leaking this backing fd on exec.
        let raw_fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if raw_fd < 0 {
            return Err(format!(
                "could not create shared-state memfd: {}",
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: `memfd_create` returned a new owned descriptor.
        let backing_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let file_size = libc::off_t::try_from(mapping_size)
            .map_err(|_| "shared-state region exceeds off_t capacity".to_string())?;
        // SAFETY: `backing_fd` is a valid memfd and `file_size` is nonnegative.
        if unsafe { libc::ftruncate(backing_fd.as_raw_fd(), file_size) } != 0 {
            return Err(format!(
                "could not size shared-state memfd: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(Arc::new(Self {
            backing_fd: Arc::new(backing_fd),
            payload_capacity,
            mapping_size,
            metadata_lock: Mutex::new(()),
        }))
    }
}

/// A Wasm linear memory whose lower virtual-address region is the ordinary Wasm
/// region and whose upper region aliases the shared AF_XDP UMEM.
struct StitchedLinearMemory {
    handle: Arc<UmemHandle>,
    base_ptr: *mut u8,
    mapping_size: usize,
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

        let aligned_umem_size = align_up(handle.size, WASM_PAGE_BYTES)?;
        let shared_state_offset = MAX_WASM_HEAP_BYTES
            .checked_add(aligned_umem_size)
            .ok_or_else(|| "shared-state offset overflow".to_string())?;
        let shared_state_mapping_size = handle
            .shared_state
            .as_ref()
            .map_or(0, |state| state.mapping_size);
        let mapping_size = shared_state_offset
            .checked_add(shared_state_mapping_size)
            .ok_or_else(|| "stitched Wasm mapping size overflow".to_string())?;
        if mapping_size as u64 > WASM32_ADDRESS_SPACE_BYTES {
            return Err(format!(
                "stitched Wasm mapping of {mapping_size} bytes exceeds the 32-bit guest address space"
            ));
        }
        // AF_XDP UMEM is backed by a HugeTLB memfd. Its alias must begin at a
        // huge-page-aligned address, but an ordinary anonymous mmap only
        // guarantees host-page alignment. Over-reserve, then trim the padding
        // so each instance retains only the stitched Wasm range.
        let hugepage_size = default_hugepage_size_bytes()?;
        if handle.size % hugepage_size != 0 {
            return Err(format!(
                "AF_XDP UMEM size {} is not aligned to HugeTLB page size {}",
                handle.size, hugepage_size
            ));
        }
        let reservation_size = mapping_size
            .checked_add(hugepage_size)
            .ok_or_else(|| "stitched Wasm reservation size overflow".to_string())?;
        // SAFETY: A null hint lets the kernel choose an unused contiguous range;
        // the range is private to this memory object until Drop.
        let reservation = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                reservation_size,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if reservation == libc::MAP_FAILED {
            return Err(format!(
                "could not reserve {} bytes for stitched Wasm memory: {}",
                reservation_size,
                std::io::Error::last_os_error()
            ));
        }
        let reservation_addr = reservation as usize;
        let alias_unaligned_addr = match reservation_addr.checked_add(MAX_WASM_HEAP_BYTES) {
            Some(address) => address,
            None => {
                // SAFETY: The reservation was successfully created above.
                unsafe { libc::munmap(reservation, reservation_size) };
                return Err("AF_XDP UMEM alias address overflow".to_string());
            }
        };
        let alias_addr = match align_up(alias_unaligned_addr, hugepage_size) {
            Ok(address) => address,
            Err(error) => {
                // SAFETY: The reservation was successfully created above.
                unsafe { libc::munmap(reservation, reservation_size) };
                return Err(error);
            }
        };
        let base_addr = alias_addr - MAX_WASM_HEAP_BYTES;
        let Some(reservation_end) = reservation_addr.checked_add(reservation_size) else {
            // SAFETY: The reservation was successfully created above.
            unsafe { libc::munmap(reservation, reservation_size) };
            return Err("stitched Wasm reservation address overflow".to_string());
        };
        let Some(mapping_end) = base_addr.checked_add(mapping_size) else {
            // SAFETY: The reservation was successfully created above.
            unsafe { libc::munmap(reservation, reservation_size) };
            return Err("stitched Wasm mapping address overflow".to_string());
        };
        let prefix_size = base_addr - reservation_addr;
        let suffix_size = reservation_end - mapping_end;
        if prefix_size > 0
            // SAFETY: This is the page-aligned prefix of the owned reservation.
            && unsafe { libc::munmap(reservation, prefix_size) } != 0
        {
            let error = std::io::Error::last_os_error();
            // SAFETY: The complete original reservation is still owned here.
            unsafe { libc::munmap(reservation, reservation_size) };
            return Err(format!("could not trim Wasm reservation prefix: {error}"));
        }
        if suffix_size > 0
            // SAFETY: This is the page-aligned suffix of the owned reservation.
            && unsafe {
                libc::munmap(mapping_end as *mut libc::c_void, suffix_size)
            } != 0
        {
            let error = std::io::Error::last_os_error();
            // SAFETY: The prefix is already released; this covers the remaining
            // stitched range and suffix.
            unsafe { libc::munmap(base_addr as *mut libc::c_void, reservation_end - base_addr) };
            return Err(format!("could not trim Wasm reservation suffix: {error}"));
        }
        // SAFETY: The range was reserved above and is exclusively owned by this
        // object until Drop. Mapping the lower region makes it part of the
        // accessible linear memory; physical pages remain demand-allocated.
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

        let tail_size = mapping_size - MAX_WASM_HEAP_BYTES;
        // SAFETY: The tail range is part of the reservation and is exclusively
        // owned by this object. Anonymous backing keeps alignment padding
        // addressable; the shared UMEM and state mappings replace their slices.
        let mapped_tail = unsafe {
            libc::mmap(
                (base_addr + MAX_WASM_HEAP_BYTES) as *mut libc::c_void,
                tail_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
                -1,
                0,
            )
        };
        if mapped_tail == libc::MAP_FAILED {
            // SAFETY: The reservation is owned by this object and no mapping escaped.
            unsafe { libc::munmap(base_addr as *mut libc::c_void, mapping_size) };
            return Err(format!(
                "could not map stitched Wasm tail: {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: The upper range is inside the reservation made above, starts
        // at a HugeTLB-aligned address, and `backing_fd` refers to the same
        // pages registered as the AF_XDP UMEM.
        let umem_mapping = unsafe {
            libc::mmap(
                (base_addr + MAX_WASM_HEAP_BYTES) as *mut libc::c_void,
                handle.size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_FIXED | libc::MAP_HUGETLB,
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

        if let Some(shared_state) = handle.shared_state.as_ref() {
            // SAFETY: `shared_state_offset` is within the reserved tail, is
            // WASM-page aligned, and the memfd was sized to `mapping_size`.
            let state_mapping = unsafe {
                libc::mmap(
                    (base_addr + shared_state_offset) as *mut libc::c_void,
                    shared_state.mapping_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_FIXED,
                    shared_state.backing_fd.as_raw_fd(),
                    0,
                )
            };
            if state_mapping == libc::MAP_FAILED {
                // SAFETY: The reservation is owned by this object and no mapping escaped.
                unsafe { libc::munmap(base_addr as *mut libc::c_void, mapping_size) };
                return Err(format!(
                    "could not map shared state into Wasm memory: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }

        handle.linear_base.store(base_addr, Ordering::Release);

        Ok(Self {
            handle,
            base_ptr: base_addr as *mut u8,
            mapping_size,
        })
    }

    fn total_size(&self) -> usize {
        self.mapping_size
    }
}

fn align_up(value: usize, alignment: usize) -> std::result::Result<usize, String> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(format!(
            "alignment must be a nonzero power of two, got {alignment}"
        ));
    }
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| "Wasm memory size overflow".to_string())
}

fn default_hugepage_size_bytes() -> std::result::Result<usize, String> {
    let meminfo = std::fs::read_to_string("/proc/meminfo")
        .map_err(|error| format!("could not read /proc/meminfo for HugeTLB alignment: {error}"))?;
    let value = meminfo
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name == "Hugepagesize").then_some(value)
        })
        .ok_or_else(|| "Hugepagesize is missing from /proc/meminfo".to_string())?;
    let mut fields = value.split_whitespace();
    let size_kib = fields
        .next()
        .ok_or_else(|| "Hugepagesize has no numeric value in /proc/meminfo".to_string())?
        .parse::<usize>()
        .map_err(|error| format!("invalid Hugepagesize value: {error}"))?;
    if fields.next() != Some("kB") {
        return Err("Hugepagesize in /proc/meminfo is not expressed in kB".to_string());
    }
    let size = size_kib
        .checked_mul(1024)
        .ok_or_else(|| "HugeTLB page size overflow".to_string())?;
    if size == 0 || !size.is_power_of_two() {
        return Err(format!("invalid HugeTLB page size: {size} bytes"));
    }
    Ok(size)
}

unsafe impl LinearMemory for StitchedLinearMemory {
    /// The exposed memory contains the private region, UMEM alias, and optional state alias.
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
            libc::munmap(self.base_ptr as *mut libc::c_void, self.mapping_size);
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
                "XdpMemoryCreator: No memory handle was set for this thread prior to instantiation!"
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
    pub(crate) shared_state_attachment: Option<AttachedSharedState>,
}

impl MyHostState {
    pub fn new(ctx: WasiCtx, table: ResourceTable, umem: Arc<UmemHandle>) -> Self {
        Self {
            ctx,
            table,
            umem,
            shared_state_attachment: None,
        }
    }
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

impl crate::wasm_engine::wabrix::plugins::shared_state::Host for MyHostState {
    fn attach(
        &mut self,
        layout: crate::wasm_engine::wabrix::plugins::shared_state::StateLayout,
    ) -> std::result::Result<crate::wasm_engine::wabrix::plugins::shared_state::StateView, String>
    {
        if self.shared_state_attachment.is_some() {
            return Err("shared-state.attach may only be called once per instance".to_string());
        }
        let type_id: [u8; 16] = layout
            .type_id
            .as_slice()
            .try_into()
            .map_err(|_| "shared-state type ID must contain exactly 16 bytes".to_string())?;
        let validation_started = Instant::now();
        let state_result = self.umem.attach_shared_state(
            type_id,
            layout.schema_version,
            layout.payload_size,
            layout.reserved_capacity,
            layout.required_alignment,
            &layout.compatible_source_versions,
        );
        log::debug!(
            "WASM shared-state layout validation took {} ns (schema {}, payload {} B)",
            validation_started.elapsed().as_nanos(),
            layout.schema_version,
            layout.payload_size
        );
        let state = state_result?;
        self.shared_state_attachment = Some(state);
        Ok(
            crate::wasm_engine::wabrix::plugins::shared_state::StateView {
                payload_offset: state.payload_offset,
                payload_capacity: state.payload_capacity,
                current_schema_version: state.current_schema_version,
                current_payload_size: state.current_payload_size,
                is_new: state.is_new,
                upgrade_required: state.upgrade_required,
            },
        )
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
        let aligned_umem_size =
            align_up(total_umem_size, WASM_PAGE_BYTES).map_err(wasmtime::Error::msg)?;
        let shared_state_mapping_size = align_up(
            SHARED_STATE_HEADER_BYTES + MAX_SHARED_STATE_PAYLOAD_BYTES,
            WASM_PAGE_BYTES,
        )
        .map_err(wasmtime::Error::msg)?;
        let memory_reservation_bytes = MAX_WASM_HEAP_BYTES
            .checked_add(aligned_umem_size)
            .and_then(|size| size.checked_add(shared_state_mapping_size))
            .ok_or_else(|| wasmtime::Error::msg("stitched Wasm reservation size overflow"))?;
        if memory_reservation_bytes as u64 > WASM32_ADDRESS_SPACE_BYTES {
            return Err(wasmtime::Error::msg(format!(
                "stitched Wasm reservation of {memory_reservation_bytes} bytes exceeds the 32-bit guest address space"
            )));
        }
        let backing_fd = umem
            .backing_fd()
            .map_err(|e| wasmtime::Error::msg(format!("failed to duplicate UMEM fd: {e}")))?;
        let umem_handle = Arc::new(UmemHandle {
            ptr: umem_ptr,
            // SAFETY: `backing_fd` is a newly duplicated, owned descriptor.
            backing_fd: Arc::new(unsafe { OwnedFd::from_raw_fd(backing_fd) }),
            linear_base: Arc::new(AtomicUsize::new(0)),
            size: total_umem_size,
            shared_state: None,
        });

        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .wasm_memory64(true)
            .memory_reservation(memory_reservation_bytes as u64)
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
        )?;

        Ok(Self {
            engine,
            linker,
            cache: DashMap::new(),
            umem: umem_handle,
        })
    }

    /// Retrieves a compiled component from in-memory cache, disk cache, or compiles from source.
    /// Compiled artifacts are cached by source content, so replacing a component at
    /// the same path cannot accidentally reuse the previous version.
    pub fn get_component(&self, path: &str) -> Result<Component> {
        use dashmap::mapref::entry::Entry;

        let bytes = std::fs::read(path).map_err(|e| {
            wasmtime::Error::msg(format!("failed to read Wasm component {path}: {e}"))
        })?;
        let content_hash = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let cache_key = format!("{path}:{content_hash}");

        match self.cache.entry(cache_key) {
            Entry::Occupied(entry) => {
                // Another thread already compiled it. Return the cached Arc.
                Ok(entry.get().clone())
            }
            Entry::Vacant(entry) => {
                // We are the first thread! Compile or load from disk.
                let component = Self::load_or_compile(&self.engine, path, &bytes, &content_hash)?;
                entry.insert(component.clone());
                Ok(component)
            }
        }
    }

    fn load_or_compile(
        engine: &Engine,
        wasm_path: &str,
        bytes: &[u8],
        content_hash: &str,
    ) -> Result<Component> {
        if let Some(cache_path) = Self::disk_cache_path(wasm_path, content_hash) {
            if cache_path.exists() {
                // SAFETY: These files are written exclusively by Component::serialize() below,
                // from WASM files under our own control on the same machine.
                match unsafe { Component::deserialize_file(engine, &cache_path) } {
                    Ok(component) => {
                        log::debug!("Loaded WASM from disk cache: {}", wasm_path);
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
            let component = Component::from_binary(engine, bytes)?;

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
            Component::from_binary(engine, bytes)
        }
    }

    /// Derives a disk cache path from the component filename and its contents.
    fn disk_cache_path(wasm_path: &str, content_hash: &str) -> Option<std::path::PathBuf> {
        let stem = std::path::Path::new(wasm_path).file_stem()?.to_str()?;
        let cache_dir = std::path::Path::new("/tmp/wabrix-wasm-cache");
        Some(cache_dir.join(format!("{}-{}.cwasm", stem, content_hash)))
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
            shared_state: None,
        })
    }

    /// Create the default per-node shared-state backing for a reloadable plugin.
    pub(crate) fn create_shared_state_region(
        &self,
    ) -> std::result::Result<Arc<SharedStateRegion>, String> {
        SharedStateRegion::create(MAX_SHARED_STATE_PAYLOAD_BYTES)
    }

    /// Create an instance handle that aliases both this UMEM and the node's state region.
    pub(crate) fn make_partition_handle_with_shared_state(
        &self,
        shared_state: Arc<SharedStateRegion>,
    ) -> Arc<UmemHandle> {
        Arc::new(UmemHandle {
            ptr: self.umem.ptr,
            backing_fd: self.umem.backing_fd.clone(),
            linear_base: Arc::new(AtomicUsize::new(0)),
            size: self.umem.size,
            shared_state: Some(shared_state),
        })
    }
}

#[cfg(test)]
mod shared_state_tests {
    use super::*;

    struct TestMapping {
        pointer: *mut u8,
        size: usize,
    }

    impl Drop for TestMapping {
        fn drop(&mut self) {
            // SAFETY: `pointer` and `size` are the exact successful mmap range
            // created by `test_handle` and no references escape this test.
            unsafe { libc::munmap(self.pointer as *mut libc::c_void, self.size) };
        }
    }

    fn test_handle() -> (UmemHandle, TestMapping) {
        let region = SharedStateRegion::create(1024).expect("create test state region");
        let mapping_size = region.mapping_size;
        // SAFETY: The region owns a valid memfd sized to `mapping_size`; this
        // test maps it read/write to inspect the same bytes as a guest mapping.
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                region.mapping_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                region.backing_fd.as_raw_fd(),
                0,
            )
        };
        assert_ne!(pointer, libc::MAP_FAILED, "map test state region");
        let pointer = pointer as *mut u8;
        let linear_offset = MAX_WASM_HEAP_BYTES;
        let linear_base = (pointer as usize)
            .checked_sub(linear_offset)
            .expect("test mapping address is above the shared-state offset");
        let handle = UmemHandle {
            ptr: std::ptr::null_mut(),
            backing_fd: region.backing_fd.clone(),
            linear_base: Arc::new(AtomicUsize::new(linear_base)),
            size: 0,
            shared_state: Some(region),
        };

        (
            handle,
            TestMapping {
                pointer,
                size: mapping_size,
            },
        )
    }

    fn read_test_header(mapping: &TestMapping) -> [u8; SHARED_STATE_HEADER_BYTES] {
        let mut header = [0; SHARED_STATE_HEADER_BYTES];
        // SAFETY: The test mapping covers a complete shared-state region whose
        // first bytes are its fixed-size header.
        unsafe {
            std::ptr::copy_nonoverlapping(mapping.pointer, header.as_mut_ptr(), header.len());
        }
        header
    }

    #[test]
    fn append_upgrade_commits_header_and_preserves_old_payload_prefix() {
        let (handle, mapping) = test_handle();
        let type_id = [0x5a; 16];
        let initial = handle
            .attach_shared_state(type_id, 1, 8, 64, 8, &[])
            .expect("initialize source schema");
        assert!(initial.is_new);

        let old_payload = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        // SAFETY: The host initialized a payload with 64 bytes of logical
        // capacity, and this write stays within its first eight bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                old_payload.as_ptr(),
                mapping.pointer.add(SHARED_STATE_HEADER_BYTES),
                old_payload.len(),
            );
        }

        let candidate = handle
            .attach_shared_state(type_id, 2, 16, 64, 8, &[1])
            .expect("accept append-compatible schema");
        assert!(candidate.upgrade_required);
        assert_eq!(candidate.current_schema_version, 1);
        assert_eq!(candidate.current_payload_size, 8);
        assert_eq!(
            read_header_u32(&read_test_header(&mapping), 32),
            1,
            "staging must not commit the target schema"
        );

        handle
            .commit_shared_state_upgrade(candidate)
            .expect("commit completed append migration");
        let header = read_test_header(&mapping);
        assert_eq!(read_header_u32(&header, 32), 2);
        assert_eq!(read_header_u32(&header, 36), 16);

        let mut actual_prefix = [0; 8];
        // SAFETY: Both the expected prefix and this read cover only the
        // initialized eight-byte source payload.
        unsafe {
            std::ptr::copy_nonoverlapping(
                mapping.pointer.add(SHARED_STATE_HEADER_BYTES),
                actual_prefix.as_mut_ptr(),
                actual_prefix.len(),
            );
        }
        assert_eq!(actual_prefix, old_payload);
    }

    #[test]
    fn incompatible_schema_is_rejected_without_changing_the_header() {
        let (handle, mapping) = test_handle();
        let type_id = [0xa5; 16];
        handle
            .attach_shared_state(type_id, 1, 8, 64, 8, &[])
            .expect("initialize source schema");

        let error = handle
            .attach_shared_state(type_id, 2, 16, 64, 8, &[])
            .expect_err("reject a source schema not declared compatible");
        assert!(error.contains("schema mismatch"));
        let header = read_test_header(&mapping);
        assert_eq!(read_header_u32(&header, 32), 1);
        assert_eq!(read_header_u32(&header, 36), 8);
    }
}
