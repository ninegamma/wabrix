Here is the updated `memory_architecture.md` file. I have completely rewritten Section 2 to include the new "Shared Control Block / Mailbox" and added a dedicated Section 5 to document the zero-copy metadata exchange and the AF_XDP Fill Queue withholding technique we just established.

---

# xbricks Memory Architecture & Zero-Copy

Here is a concise breakdown of the memory architecture and address calculation that enables zero-copy packet processing and metadata exchange between the Linux Host and the WebAssembly plugin.

### 1. The Wasm32 Linear Address Space

In a standard `wasm32` environment, memory is not physical RAM; it is a sandboxed, flat array of bytes managed by the host. Because Wasm32 uses 32-bit pointers, a single module can address a theoretical maximum of **4 Gigabytes** of memory.

Instead of keeping the packet buffer (UMEM) on the host and copying packets into the Wasm sandbox one by one, the host takes advantage of this massive 4GB limit by mapping the entire 128 MB physical UMEM directly into the Wasm linear memory.

### 2. The Unified Memory Map

To the `#![no_std]` Rust plugin, the memory looks like one continuous address space containing its application state, the packet payloads, and the shared control metadata.

```text
Wasm32 Linear Memory (0x0000_0000 to max ~4GB)
┌──────────────────────────────────────────┐
│                                          │
│  Static Data                             │ -> Globals (SYN_GEN, etc.)
│  WASM Stack (~64 KB)                     │ -> Function execution
│  WASM Heap (~2 MB)                       │ -> Your ConnectionPool & HashMap
│                                          │
├──────────────────────────────────────────┤  <-- partition_offset boundary
│                                          │
│  Shared Control Block (Mailbox)          │ -> Fixed Metadata (Lengths, Actions)
│  AF_XDP Payload Frames (128 MB)          │ -> Raw Ethernet Frames from/to NIC
│                                          │
└──────────────────────────────────────────┘

```

The strict separation between the Wasm private data (Heap/Stack) and the UMEM is guaranteed by the Host, which purposefully selects a `partition_offset` well beyond the compiled size of the Wasm image.

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

### 5. The "Mailbox" (Zero-Copy Metadata Exchange)

By default, the WebAssembly Component Model enforces a strict Canonical ABI. Returning data like `(list<u32>, list<u32>)` from the guest forces the guest to allocate heap memory, and forces the host to allocate native vectors and physically copy the data across the sandbox boundary.

To achieve 1M+ PPS throughput, we bypass the Component Model entirely using a **Shared Metadata Mailbox** located at the very start of the UMEM partition.

#### The AF_XDP Fill Queue Trick

To prevent the physical NIC hardware from overwriting this Mailbox with incoming Ethernet frames, the Host Runner utilizes the AF_XDP Fill Queue (FQ).
The host simply **withholds the first 2-3 memory frames** (e.g., `0x0000` to `0x17FF`) from the Fill Queue during initialization. Because the NIC never receives these addresses, it will never DMA packets into them, creating an indestructible shared control channel.

#### The Plugin Interface

The WIT interface is reduced to a simple signaling mechanism:

```wit
process-batch: func(batch-size: u32, partition-offset: u32) -> u32;

```

Inside the Wasm Plugin, the Mailbox is treated as a `#[repr(C)]` struct mapped directly to `umem_addr + 0`:

```rust
#[repr(C)]
pub struct BatchMetadata {
    pub lengths: [u32; 256],      // Input from Host
    pub actions: [u32; 256],      // Output to Host
    pub new_lengths: [u32; 256],  // Output to Host
}

// Accessing the Mailbox
let mailbox_ptr = umem_base_addr as *mut BatchMetadata;
let mailbox = unsafe { mailbox_ptr.as_mut().unwrap() };

// The plugin reads incoming packet lengths, runs its logic, 
// and writes routing decisions directly to the same struct.
mailbox.actions[i] = target_route;

```

#### The Host Interface

When the Wasm execution yields, the host does not copy any vectors. It simply casts its native pointer to the start of the UMEM block into the exact same `BatchMetadata` struct, reading the routing decisions and dispatching the packets natively.

This architecture guarantees that **both packet payloads and control metadata are 100% zero-copy and zero-allocation** on the hot path.