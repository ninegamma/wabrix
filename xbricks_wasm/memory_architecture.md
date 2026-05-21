Here is a concise breakdown of the memory architecture and address calculation that enables zero-copy packet processing between the Linux Host and the WebAssembly plugin.

### 1. The Wasm32 Linear Address Space

In a standard `wasm32` environment, memory is not physical RAM; it is a sandboxed, flat array of bytes managed by the host. Because Wasm32 uses 32-bit pointers, a single module can address a theoretical maximum of **4 Gigabytes** of memory.

Instead of keeping the packet buffer (UMEM) on the host and copying packets into the Wasm sandbox one by one, the host takes advantage of this massive 4GB limit by mapping the entire 128 MB UMEM directly into the Wasm linear memory.

### 2. The Unified Memory Map

To the `#![no_std]` Rust plugin, the memory looks like one continuous address space containing both the raw hardware packets and the application state.

```text
Wasm32 Linear Memory (0x0000_0000 to max ~4GB)
┌──────────────────────────────────────────┐
│                                          │
│  WASM Heap (~2 MB)                       │ -> Your ConnectionPool & BTreeMap
│  WASM Stack (~64 KB)                     │ -> Function execution
│  Static Data                             │ -> Globals (SYN_GEN, etc.)
│                                          │
├──────────────────────────────────────────┤  <-- partition_offset boundary
│                                          │
│  AF_XDP UMEM (128 MB)                    │ -> Raw Ethernet Frames from/to NIC
│  (Directly mapped by Host)               │
│                                          │
└──────────────────────────────────────────┘

```

### 3. Address Calculation (The Pointer Math)

Because the UMEM is mapped at a specific offset inside the Wasm memory, the Wasm plugin cannot assume a packet starts at address `0`. It must translate the relative packet offsets provided by AF_XDP into absolute Wasm memory pointers.

This is exactly what your address calculation does:

```rust
let umem_addr = (0u32).wrapping_sub(partition_offset) as usize;
let base_addr = pdu.base_addr(umem_addr);

```

**How the math works:**

1. **`partition_offset`:** The host tells the Wasm plugin exactly where the 128 MB UMEM block begins inside the linear memory.
2. **`wrapping_sub`:** This is a clever two's-complement arithmetic trick. Instead of adding offsets, subtracting the `partition_offset` from `0` creates a negative base address (in 32-bit unsigned math).
3. **Pointer Resolution:** When AF_XDP says *"The packet is at chunk offset 2048,"* your code adds that 2048 to the `umem_addr`. Because of the two's-complement overflow, this perfectly resolves to the exact absolute memory address inside the Wasm linear sandbox.

### 4. The Zero-Copy Result

Once the absolute pointer is calculated:

```rust
let data_ptr = pdu.data_ptr(umem_addr) as *mut u8;

```

Your `write_tcp_reply` function uses `ptr::copy_nonoverlapping` to write headers directly to `data_ptr`. Because that pointer points to the shared UMEM block, your Wasm engine is writing the bytes directly into the NIC's TX ring buffer. No serialization, no syscalls, and absolutely no memory copying between the host and the guest.