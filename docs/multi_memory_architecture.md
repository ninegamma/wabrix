# Wabrix Multi-Memory Architecture

This document describes the currently implemented memory architecture for packet-engine WebAssembly components. It supersedes the earlier design that assumed stable Rust could directly emit loads and stores against WebAssembly Memory 1.

## Overview

Each composed plugin component contains two independently allocated core WebAssembly memories:

```text
Component
├── Rust plugin core module
│   └── default memory: heap, stack, statics, connection state
└── WAT UMEM adapter core module
    └── adapter memory: the host's AF_XDP UMEM mapping
```

The Rust compiler continues to use its module's default memory normally. The WAT adapter owns a second core memory whose address zero maps to the beginning of AF_XDP UMEM. The two memories are separate core-module memories inside one Component Model component; Rust pointers never point into the adapter memory.

This distinction matters: current stable Rust can enable the multi-memory proposal, but it cannot assign a normal `*const T` or `*mut T` to a non-default memory address space. Wabrix therefore accesses UMEM through imported adapter functions rather than Rust pointer dereferences.

## Memory layout

### Rust plugin memory

The Rust core module's default memory contains:

- the allocator heap;
- the shadow stack and function stack data;
- globals and static data;
- connection tables and plugin configuration;
- temporary fixed-size header values returned by the packet API.

Wasmtime backs this memory with a private anonymous mapping created by `HeapMemory`. It is not mapped onto UMEM.

### UMEM adapter memory

The composed component includes a small core module generated from `tools/umem_adapter.wat`. Its memory export is replaced during composition with a dedicated memory declaration. During instantiation, `XdpMemoryCreator` recognizes that declaration by its exact limits and supplies the existing AF_XDP `mmap` through `UmemHandle`.

Address zero in this memory is the physical beginning of UMEM, so descriptor addresses and mailbox offsets are direct UMEM-relative byte offsets. No partition-offset or wrapping-sub address conversion is used.

```text
AF_XDP UMEM
┌─────────────────────────────────────────────────────────────────────┐
│ Reserved frames and per-instance control mailboxes                  │
│                                                                     │
│ mailbox_offset(instance)                                            │
│   = reserved_bytes - (instance + 1) * MAILBOX_STRIDE                │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤ reserved_bytes
│ Packet payload frames                                               │
│ Raw Ethernet frames used by RX and TX rings                         │
└─────────────────────────────────────────────────────────────────────┘ total_size
```

Control-block frames are withheld from the AF_XDP Fill Queue, preventing the NIC from DMA-writing into plugin mailboxes.

## WAT adapter

The adapter exports four core functions:

```text
load8(offset) -> u32
load64(offset) -> u64
store8(offset, value)
store64(offset, value)
```

The functions use ordinary loads and stores against the adapter module's own memory. From the Rust module they appear as imports from `wabrix_umem`. Fixed-size structures are transferred in 64-bit chunks with byte operations for any remainder.

These are direct core-Wasm calls. They do not use the Component Model Canonical ABI, allocate vectors, serialize values, or invoke native host callbacks. Packet payload bytes remain in UMEM. Header reads return small `Copy` values in Rust memory; changes are committed through explicit write methods.

## Plugin packet API

`Packet` is an offset-backed proxy rather than a wrapper around `&mut Pdu0`. It stores the UMEM-relative offset of its mailbox PDU slot and uses the adapter for all descriptor, header, payload, and action access.

Header accessors return values:

```rust
let mut tcp = packet.tcp_header()?;
tcp.set_flags(new_flags);
packet.write_tcp_header(&tcp);
```

They cannot return references into UMEM because a Rust reference belongs to the Rust module's default memory. The explicit read/write API makes the address-space transition visible and prevents invalid cross-memory references.

`Packet::write_bytes` writes stack or heap bytes into the frame. `Packet::contains` scans payload data through a fixed 256-byte stack window and performs no heap allocation. `MailboxBatch` iterates PDU and action offsets without constructing slices into UMEM.

The host-facing layouts remain unchanged:

- `Pdu` remains the native host descriptor;
- `Pdu0` remains 64 bytes and 64-byte aligned;
- `WPdu` remains 56 bytes;
- the final eight bytes of `Pdu0` remain reserved for the host `SystemBus` pointer;
- `BatchMetadata` still contains `[Pdu0; MAX_BATCH_SIZE]` followed by the action array.

## Mailbox call flow

The packet-engine WIT call transfers only scalar signaling values:

```wit
process-batch: func(batch-size: u32, mailbox-offset: u32) -> u32;
```

Processing follows these phases:

1. The host bulk-copies active native PDU descriptors into the instance's UMEM mailbox.
2. The host calls `process-batch(batch-size, mailbox-offset)`.
3. `MailboxBatch` derives each PDU and action offset from the mailbox offset.
4. The plugin reads descriptors and headers through the WAT adapter, accesses payload frames at their direct UMEM offsets, and writes actions or mutations through the adapter.
5. The host bulk-copies mutated PDU descriptors back and routes packets using the mailbox action array.

There are no per-packet heap allocations or Canonical ABI lists in this packet-engine hot path. Descriptor values and headers cross between the two core memories as scalar machine words; packet payloads are not copied into Rust memory except for bounded inspection windows requested by plugin code.

## Build and composition

The `wasm32-wasip2` Rust target normally invokes `wasm-component-ld`, which cannot resolve the custom `wabrix_umem` core imports. The build therefore has two stages:

1. Cargo uses `rust-lld` to emit a core Wasm module containing the WIT metadata and unresolved `wabrix_umem` imports.
2. `tools/build_components.sh` invokes `tools/compose_umem.py`, which:
   - compiles `tools/umem_adapter.wat`;
   - creates an initial component with `wasm-tools component new --adapt`;
   - inserts the dedicated UMEM core memory;
   - rebinds the adapter's memory import from the Rust memory to UMEM;
   - parses and validates the final component.

Build all supplied plugins with:

```bash
cd wabrix_api
bash tools/build_components.sh
```

Loadable components are written to `target/wasm32-wasip2/release/*.wasm`. The intermediate Rust core modules are retained as `*.core.wasm`.

## Wasmtime configuration and safety

`WasmManager` enables the Component Model and multi-memory proposals. Its custom memory creator distinguishes memories by the declaration inserted during composition:

- normal Rust/component memories receive private anonymous mappings;
- only the UMEM adapter signature receives `UmemHandle`.

The Wasmtime guard size is zero because the physical UMEM allocation has no host-owned guard mapping after it. Cranelift therefore retains explicit bounds checks. UMEM growth is rejected and its base pointer cannot move.

Each plugin instance owns a non-overlapping mailbox, but all instances may address the shared packet-frame region. The pipeline and frame-ownership rules remain responsible for ensuring that two instances do not mutate the same frame concurrently.

## Current limitation

The packet-engine and filter paths use the mailbox/proxy architecture described above. The generator WIT interface still accepts a Canonical ABI byte list and has not yet been migrated to a mailbox-based generator call. Although a generator component can be built and composed, it cannot correctly write separated UMEM through its legacy `PacketBatch::new(&mut batch_bytes)` path. Generator deployment must wait for that ABI migration.

## Multi memory currently no longer pursued due to heavy performance degradation

Using the multi-memory architecture the performance has degraded by a factor of 2 vs single memory. We need now roughly 1000 cycles per packet in the tcp_engine.

This is the reason due to Codex 5.6 Sol: 

I’ll break down the hot-path adapter calls made for a received TCP packet and a generated SYN, including whether composition lowered them to direct calls or table-mediated adapter calls. That should account for where the extra cycles are spent.

Yes. The slowdown is caused by the current scalar WAT bridge, not inherently by WebAssembly multi-memory.

A generated SYN currently performs roughly 40 cross-module adapter calls:

- Tick detection: about 12 loads.
- Header generation: about 19 stores.
- Repeated `desc_addr` reads: 3 loads.
- Updating `desc_len`: 4 byte stores.
- Writing the action: 4 byte stores.

Small values are especially expensive because `umem::read::<u32>()` currently becomes four `load8` calls, and a 20-byte header becomes two `store64` plus four `store8` calls.

Received TCP packets are similar: `HeaderStack`, `desc_addr`, and `frame_size_log2` are reread for each MAC/IP/TCP accessor, producing approximately 40–50 adapter calls per packet.

Each adapter call:

- crosses between core Wasm instances;
- cannot be inlined by LLVM;
- switches memory-instance context;
- executes explicit bounds checks because UMEM has no guard region.

Single-memory code used direct loads that LLVM could inline, combine, and cache. Therefore, a roughly 500–1,000 cycle penalty is plausible.

Best optimization sequence:

1. Add `load32` and `store32` to eliminate four-call `u32` operations.
2. Cache `HeaderStack`, `desc_addr`, `desc_len`, and frame size once inside each `Packet`.
3. Avoid rereading descriptor fields for every header.
4. Add specialized wider adapter operations for complete MAC/IP/TCP headers.
5. Ultimately compose a bridge module importing both heap and UMEM, allowing `memory.copy` between them in one call.

These changes should remove most of the regression. The current implementation prioritizes correctness and memory separation but is too fine-grained for the desired dataplane performance.

Given the reported change from roughly 500 to 1,000 cycles/packet:

| Optimization | Estimated result |
|---|---:|
| Current scalar adapter | ~1,000 cycles |
| Add `load32`/`store32` | ~800–900 |
| Cache PDU metadata per `Packet` | ~650–750 |
| Specialized header load/store functions | ~575–675 |
| Dual-memory bulk-copy bridge | ~525–625 |

A realistic short-term target is 650–750 cycles/packet—a 25–35% improvement—using `load32`/`store32` and cached descriptor fields.

With a redesigned bridge using one bulk operation per header or packet, 525–625 cycles appears achievable, within roughly 5–25% of the original single-memory performance.

Returning fully to ~500 cycles may be difficult because cross-core-memory calls, explicit bounds checks, and component dispatch remain. The largest immediate gain should come from reducing approximately 40–50 adapter calls per packet to fewer than 10.