# wabrix Memory Architecture & Zero-Copy

Here is a concise breakdown of the memory architecture and address calculation that enables zero-copy packet processing and metadata exchange between the Linux Host and the WebAssembly plugin.

### 1. The Wasm32 Linear Address Space

In a standard `wasm32` environment, memory is not physical RAM; it is a sandboxed, flat array of bytes managed by the host. Because Wasm32 uses 32-bit pointers, a single module can address a theoretical maximum of **4 Gigabytes** of memory.

Instead of keeping the packet buffer (UMEM) on the host and copying packets into the Wasm sandbox one by one, the host takes advantage of this massive 4GB limit by mapping the entire 128 MB physical UMEM directly into the Wasm linear memory.

### 2. The Unified Memory Map

#### Host-Side Physical UMEM Layout

The host maps the entire AF_XDP UMEM as one contiguous physical region divided into three zones:

```text
Host Physical UMEM (byte offsets from Umem base ptr)
┌──────────────────────────────────────────────────────────────────────┐
│  Scratchpad Partitions  [0 .. reserved_bytes - control_block_bytes)  │
│                                                                      │
│  partition_start_bytes_0 = 0                                         │
│  ┌──────────────────────────────────────┐                            │
│  │  Instance 0: Stack, Heap, Globals    │  (partition_size bytes)    │
│  └──────────────────────────────────────┘                            │
│  partition_start_bytes_1 = 1 × partition_size                        │
│  ┌──────────────────────────────────────┐                            │
│  │  Instance 1: Stack, Heap, Globals    │  (partition_size bytes)    │
│  └──────────────────────────────────────┘                            │
│  partition_start_bytes_N = N × partition_size                        │
│  ┌──────────────────────────────────────┐                            │
│  │  Instance N: Stack, Heap, Globals    │  (partition_size bytes)    │
│  └──────────────────────────────────────┘                            │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  Control Block  [reserved_bytes - control_block_bytes .. reserved_bytes) │
│                                                                      │
│  mailbox_offset_0 = reserved_bytes - 1 × sizeof(BatchMetadata)      │
│  mailbox_offset_1 = reserved_bytes - 2 × sizeof(BatchMetadata)      │
│  mailbox_offset_N = reserved_bytes - (N+1) × sizeof(BatchMetadata)  │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤  <-- reserved_bytes
│  AF_XDP Payload Frames  [reserved_bytes .. total_size)               │
│                                                                      │
│  Raw Ethernet frames from / to the NIC (shared by all instances)     │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

#### Plugin-Side WASM Linear Memory View (Instance N)

Wasmtime maps each instance's WASM linear memory starting at `partition_start_bytes_N`. The window extends to the end of the full UMEM. From the plugin's perspective:

```text
WASM Linear Memory — Instance N  (WASM address 0x0000_0000 onward)
┌──────────────────────────────────────────────────────────────────────┐
│  WASM address 0x0000_0000                                            │
│                                                                      │
│  Static Data / Stack / Heap                                          │
│  (Rust allocator stays within this partition — software convention)  │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  Instance N's BatchMetadata Mailbox                                  │
│  WASM address = mailbox_offset_N - partition_start_bytes_N  (> 0)   │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│  AF_XDP Payload Frames                                               │
│  WASM address = desc_addr - partition_start_bytes_N         (> 0)   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

Because the Control Block sits at the **end** of the reserved region (above every instance's `partition_start_bytes`), and AF_XDP frames start beyond that, both the mailbox and every packet frame are at **positive** WASM addresses within the linear memory window. No address wrapping is required to reach them.

> **`partition_start_bytes_N` vs `partition_offset`**
> These are the same numeric value, but carry different roles:
> - **`partition_start_bytes_N`** — host-side UMEM byte offset where instance N's WASM address 0 is anchored.
> - **`partition_offset`** — the WIT argument `process-batch(…, partition-offset: u32, …)` passed into the plugin so it can translate UMEM-relative addresses to WASM addresses (`umem_offset - partition_offset`).

### 3. Absolute Address Translation

Because the UMEM partition containing the packets is mapped with a specific offset inside the Wasm memory, the Wasm plugin cannot assume a packet starts at address `0`. It must translate the relative packet offsets provided by AF_XDP into absolute Wasm memory pointers.

This is exactly what the address calculation does:

```rust
// The absolute base pointer to the start of the UMEM block
let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;

// The packet base address
let base_addr = pdu.base_addr(umem_addr);

```

**How the math works:**

1. **`partition_offset`:** The host tells the Wasm plugin exactly where the 128 MB UMEM block begins inside the linear memory.
2. **`wrapping_sub`:** This is a clever two's-complement arithmetic trick. Subtracting the `partition_offset` from `0` creates a negative base address (in 32-bit unsigned math) that resolves to the exact absolute memory address inside the Wasm sandbox.

### 4. Zero-Copy Packet Payloads

Once the absolute pointer is calculated:

```rust
let data_ptr = pdu.data_ptr(umem_addr) as *mut u8;
```

The plugin uses standard pointer arithmetic (e.g., `ptr::copy_nonoverlapping`) to write headers directly to `data_ptr`. Because that pointer points to the shared UMEM block, the Wasm engine is writing the bytes directly into the NIC's TX ring buffer. No serialization, no `Vec` allocation, and no boundary crossing.

### 5. The Per-Instance Mailbox (Zero-Copy Metadata Exchange)

By default, the WebAssembly Component Model enforces a strict Canonical ABI. Returning data like `(list<u32>, list<u32>)` from the guest forces the guest to allocate heap memory, and forces the host to allocate native vectors and physically copy the data across the sandbox boundary.

To achieve 1M+ PPS throughput, we bypass the Component Model entirely using a **Per-Instance Metadata Mailbox** located within the shared Control Block at the start of the UMEM.

#### The AF_XDP Fill Queue Trick

To prevent the physical NIC hardware from overwriting the Control Block with incoming Ethernet frames, the Host Runner utilizes the AF_XDP Fill Queue (FQ). The host **withholds all frames belonging to the Control Block** from the Fill Queue during initialization. Because the NIC never receives these addresses, it will never DMA packets into them.

The number of withheld frames scales with the number of plugin instances:
```
control_block_frames = ceil(N_instances × sizeof(BatchMetadata) / frame_size)
```

All frames in `[0, control_block_frames)` are permanently excluded from the FQ.

#### The Plugin Interface

The WIT interface is reduced to a minimal signaling mechanism — no data crossing the boundary:

```wit
process-batch: func(batch-size: u32, partition-offset: u32, mailbox-offset: u32) -> u32;
```

- **`batch-size`:** Number of valid PDU entries written into the mailbox.
- **`partition-offset`:** Equals `partition_start_N`; used to compute `umem_base_wasm_addr`.
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

```rust
ptr::copy_nonoverlapping(
    batch.as_ptr() as *const Pdu,
    (*mailbox_ptr).pdus.as_mut_ptr(),
    len,
);
```

**Phase 2 — WASM Execution:**  
The host calls `process_batch(batch_size, partition_offset, mailbox_offset)`. The plugin reads `pdus[i]` for packet metadata, processes frames zero-copy, and writes its routing decision to `actions[i]`. It may also mutate `WPdu` fields such as `desc_len` or `desc_addr` to reflect packet modifications.

**Phase 3 — Mutation (Mailbox → Host):**  
A symmetric `ptr::copy_nonoverlapping` copies the plugin-modified PDU slots back into the host `Batch`. The `system_bus` tail bytes are preserved unchanged (plugins never touch them), so the host's `Pdu` state remains consistent.

```rust
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
