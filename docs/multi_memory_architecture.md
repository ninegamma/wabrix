# Wabrix Memory Architecture & Zero-Copy

Here is a concise breakdown of the memory architecture and address calculation that enables zero-copy packet processing and metadata exchange between the Linux Host and the WebAssembly plugin.

### 1. The Wasm32 Multi-Memory Address Space

In the multi-memory architecture, Wabrix leverages Wasmtime's support for multiple disjoint memory instances. Instead of a single flat array where physical RAM mappings and WebAssembly application state are interleaved, the host provisions distinct memory spaces.

The massive 4GB limit per memory instance remains, but separation of concerns is enforced at the WebAssembly VM layer.
### 2. The Segregated Memory Map

To the #![no_std] Rust plugin compiled with the +multimemory target feature, memory is strictly segregated into two primary instances.

Wasm32 Memory 0 (Primary / Default)
```text
┌──────────────────────────────────────────┐
│                                          │
│  WASM Heap (~2 MB)                       │ -> ConnectionPool & State
│  WASM Stack (~64 KB)                     │ -> Function execution
│  Static Data                             │ -> Globals
│                                          │
└──────────────────────────────────────────┘

Wasm32 Memory 1 (Imported UMEM)
┌──────────────────────────────────────────┐
│                                          │
│  AF_XDP UMEM (128 MB)                    │ -> Raw Ethernet Frames & Mailbox
│  (Directly mapped by Host, starts at 0)  │
│                                          │
└──────────────────────────────────────────┘
```

#### Host-Side Physical UMEM Layout

The host maps the entire AF_XDP UMEM as one contiguous physical region and exposes it entirely as Memory 1. The internal layout of this memory remains divided into functional zones:


```text
Host Physical UMEM (Exposed as WASM Memory 1)
┌──────────────────────────────────────────────────────────────────────┐
│  Control Block  [0 .. control_block_bytes)                           │
│                                                                      │
│  mailbox_offset_0 = 0 × sizeof(BatchMetadata)                        │
│  mailbox_offset_1 = 1 × sizeof(BatchMetadata)                        │
│  mailbox_offset_N = N × sizeof(BatchMetadata)                        │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤  <-- reserved_bytes
│  AF_XDP Payload Frames  [reserved_bytes .. total_size)               │
│                                                                      │
│  Raw Ethernet frames from / to the NIC (shared by all instances)     │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Address Calculation

The transition to a multi-memory architecture completely eliminates the partition_offset and wrapping_sub pointer arithmetic required in a single-memory model.
Because Memory 1 is dedicated exclusively to the UMEM, address 0 in Memory 1 maps exactly to the physical start of the UMEM mmap region.

* Direct Translation: An AF_XDP frame offset of 2048 is accessed directly at address 2048 in Memory 1.

* API Simplification: The plugin no longer needs a partition_offset argument to calculate negative base addresses. Frame offsets provided by the AF_XDP descriptors map 1:1 with Memory 1 addresses.

### Zero-Copy Packet Payloads

Standard Rust pointer dereferencing (*mut u8) implicitly targets Memory 0. To interact with the UMEM, the data plane code must explicitly target the secondary memory.

The plugin utilizes Wasm multi-memory intrinsics (e.g., memory.load, memory.store, or memory.copy instructions targeting index 1, typically abstracted via core::arch::wasm32 or inline assembly). This allows the plugin to write headers directly into the NIC's TX ring buffer located in Memory 1.

No serialization, no Vec allocation, and no boundary crossing occur.

### The Per-Instance Mailbox (Zero-Copy Metadata Exchange)

By default, the WebAssembly Component Model enforces a strict Canonical ABI. Returning data like lists or strings forces the guest to allocate heap memory (in Memory 0), and forces the host to allocate native vectors and physically copy the data across the sandbox boundary.

To bypass the Component Model and achieve 1M+ PPS throughput, Wabrix utilizes a Per-Instance Metadata Mailbox located within the shared Control Block at the start of the UMEM (Memory 1).

#### The AF_XDP Fill Queue Trick

To prevent the physical NIC hardware from overwriting the Control Block with incoming Ethernet frames, the Host Runner utilizes the AF_XDP Fill Queue (FQ). The host **withholds all frames belonging to the Control Block** from the Fill Queue during initialization. Because the NIC never receives these addresses, it will never DMA packets into them.

The number of withheld frames scales with the number of plugin instances:
```
control_block_frames = ceil(N_instances × sizeof(BatchMetadata) / frame_size)
```

All frames in `[0, control_block_frames)` are permanently excluded from the FQ.

#### The Plugin Interface

The WIT interface is reduced to a minimal signaling mechanism — no data crossing the boundary:

```text
   process-batch: func(batch-size: u32, mailbox-offset: u32) -> u32;
```

- **`batch-size`:** Number of valid PDU entries written into the mailbox.
- **`mailbox-offset`:** UMEM-relative byte offset of this instance's `BatchMetadata`. Equals `N × sizeof(BatchMetadata)`.

#### The `BatchMetadata` Layout

`BatchMetadata` (defined in `wabrix_api`) is the shared control structure written by the host and read/written by the plugin:

```rust
#[repr(C)]
pub struct BatchMetadata {
    pub pdus:    [Pdu0; MAX_BATCH_SIZE],  // Full packet descriptors (plugin view)
    pub actions: [u32;  MAX_BATCH_SIZE],  // Routing decisions written by plugin
}
```

Each `Pdu0` slot is 64 bytes: a 56-byte `WPdu` core (header stack, descriptor address, length, options, metadata, timestamp) plus 8 bytes the plugin must never read or write. The host uses an identical-layout `HostBatch` type that places a `*const SystemBus` pointer in those last 8 bytes:

```
Byte layout of each PDU slot (64 bytes, repr(C, align(64))):
┌─────────────────────────────────┬──────────────────┐
│  WPdu core  [0..56]             │  tail  [56..64]  │
│  header_stack (16)              │  host: SystemBus* │
│  desc_addr (8)                  │  plugin: (unused) │
│  desc_len (4)                   │                  │
│  desc_options (4)               │                  │
│  metadata (16)                  │                  │
│  timestamp (8)                  │                  │
└─────────────────────────────────┴──────────────────┘
```

A compile-time assertion enforces layout compatibility:
```rust
const _: () = assert!(size_of::<HostBatch>() == size_of::<BatchMetadata>());
```

#### The Hot-Path Data Flow

Each call to `execute_wasm_and_route` follows four phases:

**Phase 1 — Populate (Host → Mailbox):**  
A single `ptr::copy_nonoverlapping` copies all `len` `Pdu` structs from the host `Batch` into the UMEM mailbox. LLVM lowers this to SIMD instructions, moving ~16 KB in a handful of CPU cycles. The plugin sees the full packet descriptor (address, length, header metadata) for each frame.

```txt
ptr::copy_nonoverlapping(
    batch.as_ptr() as *const Pdu,
    (*mailbox_ptr).pdus.as_mut_ptr(),
    len,
);
```

**Phase 2 — WASM Execution:**  
The host calls `process_batch(batch_size, mailbox_offset)`. The plugin reads `pdus[i]` for packet metadata, processes frames zero-copy, and writes its routing decision to `actions[i]`. It may also mutate `WPdu` fields such as `desc_len` or `desc_addr` to reflect packet modifications.

**Phase 3 — Mutation (Mailbox → Host):**  
A symmetric `ptr::copy_nonoverlapping` copies the plugin-modified PDU slots back into the host `Batch`. The `system_bus` tail bytes are preserved unchanged (plugins never touch them), so the host's `Pdu` state remains consistent.

```txt
ptr::copy_nonoverlapping(
    (*mailbox_ptr).pdus.as_ptr(),
    batch.as_mut_ptr() as *mut Pdu,
    len,
);
```

**Phase 4 — Routing:**  
The host reads `actions[i]` from the mailbox and distributes PDUs across output `Batch` queues according to each plugin's routing decision.

This architecture guarantees that:

1. **Both packet payloads and control metadata are 100% zero-copy and zero-allocation** on the hot path.
2. **Plugin instances execute concurrently without contention**: each instance owns an exclusive, non-overlapping `BatchMetadata` slice within the Control Block.
3. **The control channel is hardware-protected**: withheld FQ frames ensure the NIC never overwrites the Control Block.
