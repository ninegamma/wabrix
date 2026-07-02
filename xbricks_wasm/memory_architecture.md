# xbricks Memory Architecture & Zero-Copy

Here is a concise breakdown of the memory architecture and address calculation that enables zero-copy packet processing and metadata exchange between the Linux Host and the WebAssembly plugin.

### 1. The Wasm32 Linear Address Space

In a standard `wasm32` environment, memory is not physical RAM; it is a sandboxed, flat array of bytes managed by the host. Because Wasm32 uses 32-bit pointers, a single module can address a theoretical maximum of **4 Gigabytes** of memory.

Instead of keeping the packet buffer (UMEM) on the host and copying packets into the Wasm sandbox one by one, the host takes advantage of this massive 4GB limit by mapping the entire 128 MB physical UMEM directly into the Wasm linear memory.

### 2. The Unified Memory Map

The UMEM is divided into three physical regions, each visible from every plugin's WASM linear memory window at computed offsets. Multiple plugin instances execute concurrently and each owns an exclusive slice of every region.

```text
UMEM Physical Layout (host-side byte offsets from UMEM base)
┌──────────────────────────────────────────┐  0x0000_0000
│                                          │
│  Control Block — Per-Instance Mailboxes  │
│  ┌────────────────────────────────────┐  │
│  │  Instance 0 BatchMetadata          │  │  mailbox_offset_0 = 0
│  ├────────────────────────────────────┤  │
│  │  Instance 1 BatchMetadata          │  │  mailbox_offset_1 = sizeof(BatchMetadata)
│  ├────────────────────────────────────┤  │
│  │  Instance N ...                    │  │  mailbox_offset_N = N * sizeof(BatchMetadata)
│  └────────────────────────────────────┘  │
│                                          │
├──────────────────────────────────────────┤  <-- control_block_end (frame-aligned)
│                                          │
│  Per-Instance WASM Heap/Stack Partitions │
│  ┌────────────────────────────────────┐  │
│  │  Instance 0 WASM private memory    │  │  partition_start_0
│  ├────────────────────────────────────┤  │
│  │  Instance 1 WASM private memory    │  │  partition_start_1
│  ├────────────────────────────────────┤  │
│  │  Instance N ...                    │  │  partition_start_N
│  └────────────────────────────────────┘  │
│                                          │
├──────────────────────────────────────────┤  <-- reserved_bytes (partition_offset boundary)
│                                          │
│  AF_XDP Payload Frames (128 MB)          │  Raw Ethernet frames from/to NIC
│                                          │
└──────────────────────────────────────────┘
```

The strict separation between all reserved regions (Control Block + per-instance partitions) and the AF_XDP packet frames is guaranteed by the Host, which selects `reserved_frames` to cover both the full Control Block and the per-instance WASM private memory.

From Plugin N's WASM linear memory perspective, `partition_start_N` is WASM address `0`. The Control Block and the packet frames are both visible at negative-wrapping addresses computed via `(0u32).wrapping_sub(partition_start_N)`.

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

1. **`partition_offset`:** The host tells the Wasm plugin exactly where the 128 MB UMEM block begins inside the linear memory. Equals `partition_start_N` for instance N.
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

- **`batch-size`:** Number of valid entries written into the mailbox.
- **`partition-offset`:** Equals `partition_start_N`; used to compute `umem_base_wasm_addr`.
- **`mailbox-offset`:** UMEM-relative byte offset of this instance's `BatchMetadata`. Equals `N × sizeof(BatchMetadata)`.

Inside the Wasm Plugin, the Mailbox is a `#[repr(C)]` struct accessed via the UMEM base address:

```rust
#[repr(C)]
pub struct BatchMetadata {
    // Written by host before calling process-batch:
    pub desc_addrs:  [u32; MAX_BATCH_SIZE],   // AF_XDP frame descriptor offsets
    pub lengths:     [u32; MAX_BATCH_SIZE],   // Original packet lengths

    // Written by plugin during process-batch:
    pub actions:     [u32; MAX_BATCH_SIZE],   // Routing decisions (route_id / DROP)
    pub new_lengths: [u32; MAX_BATCH_SIZE],   // Modified packet lengths (0 = unchanged)
}

fn process_batch(batch_size: u32, partition_offset: u32, mailbox_offset: u32) -> u32 {
    // 1. Resolve absolute UMEM base in this plugin's address space
    let umem_base_addr = (0u32).wrapping_sub(partition_offset) as usize;

    // 2. Resolve this instance's exclusive mailbox
    //    SAFETY: mailbox_offset points into the Control Block, which the host guarantees
    //    is excluded from the AF_XDP FQ (NIC never DMAs here).
    let mailbox = unsafe {
        &mut *((umem_base_addr + mailbox_offset as usize) as *mut BatchMetadata)
    };

    // 3. Process each packet
    for i in 0..batch_size as usize {
        let data_ptr = (umem_base_addr + mailbox.desc_addrs[i] as usize) as *mut u8;
        // ... read/write packet payload at data_ptr with length mailbox.lengths[i] ...
        mailbox.actions[i]     = target_route;
        mailbox.new_lengths[i] = new_packet_length;
    }
    0
}
```

#### The Host Interface

When the Wasm execution yields, the host does not copy any vectors. It casts the native UMEM pointer directly into the same `BatchMetadata` struct to read routing decisions and dispatch packets:

```rust
// Before calling the plugin — write inputs
let mailbox_ptr = unsafe {
    (umem_ptr as *mut u8).add(mailbox_offset as usize) as *mut BatchMetadata
};
// SAFETY: Control Block frames are excluded from the AF_XDP FQ.
// This instance's mailbox slice [mailbox_offset, mailbox_offset + sizeof(BatchMetadata))
// is exclusively owned by this host thread during the call window.
unsafe {
    let mailbox = &mut *mailbox_ptr;
    for (i, pdu) in batch.iter().enumerate() {
        mailbox.desc_addrs[i] = pdu.frame_desc_offset() as u32;
        mailbox.lengths[i]    = pdu.w.data_len();
    }
}

// Call plugin — zero bytes cross the sandbox boundary
call_process_batch(batch_size, partition_start_bytes, mailbox_offset)?;

// After the plugin returns — read outputs, no copy needed
unsafe {
    let mailbox = &*mailbox_ptr;
    for i in 0..batch_size {
        let action     = mailbox.actions[i];
        let new_length = mailbox.new_lengths[i];
        // ... apply routing and length mutation ...
    }
}
```

This architecture guarantees that:

1. **Both packet payloads and control metadata are 100% zero-copy and zero-allocation** on the hot path.
2. **Plugin instances execute concurrently without contention**: each instance owns an exclusive, non-overlapping `BatchMetadata` slice within the Control Block.
3. **The control channel is hardware-protected**: withheld FQ frames ensure the NIC never overwrites the Control Block.
