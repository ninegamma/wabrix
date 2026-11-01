# wabrix memory architecture

Each Wasm instance receives a private linear-memory mapping. The mapping has a
private lower region for the module's heap, stack, and globals, followed by a
private mailbox at the end of that region. The complete AF_XDP UMEM is then
mapped above it from the same memfd-backed pages used by `xsk-core`. Reloadable
WASM nodes map their dedicated shared-state region after the page-aligned UMEM:

```text
Wasm address 0                       Wasm address 2 GiB                  tail
       │                                      │                            │
       ▼                                      ▼                            ▼
┌──────────────────────────────┬──────────────┬──────────────────────┬─────────────┐
│ private Wasm heap and stack  │ private      │ shared AF_XDP UMEM   │ per-node    │
│                              │ mailbox      │                      │ shared state│
└──────────────────────────────┴──────────────┴──────────────────────┴─────────────┘
```

The UMEM itself has no reserved prefix or control block. Every frame is
available to AF_XDP queues. Each Wasm instance gets its own mailbox, while all
instances see the same packet frames through their UMEM alias.

Each reloadable `WasmEngine` gets a separate memfd-backed state region. The
active runtime and its staged replacement map the same pages; unrelated nodes
and workers receive separate regions. The current host reserves 127 KiB of
payload capacity. Adding the 64-byte header gives 130,112 bytes; rounding that
up to a 64 KiB boundary gives a 131,072-byte (128 KiB) mapping, leaving 960
bytes unused. The region does not take frames from UMEM.

The host keeps packet descriptor addresses relative to the native UMEM base.
The UMEM starts at `MAX_WASM_HEAP_BYTES` in Wasm memory, so the host passes the
positive offset directly:

```text
umem_offset = MAX_WASM_HEAP_BYTES
```

The guest computes the Wasm address of a UMEM-relative descriptor as:

```text
umem_addr = umem_offset
packet_wasm_addr = umem_addr + desc.addr
```

For the current mapping this is equivalent to adding 2 GiB to the UMEM
offset. The same UMEM-relative descriptor can therefore be used by every
instance, while each instance's private mailbox is addressed by its own
Wasm-linear `mailbox_offset`.

The state region begins at
`MAX_WASM_HEAP_BYTES + align_up(umem_size, 64 KiB)`. Its offset is therefore
the same for instances using the same UMEM, and it does not change packet
descriptor addresses. The mapping reserves the alignment gap after UMEM as
addressable private memory.

The host must keep `umem_offset + desc.addr + packet_length` within the
stitched mapping, including the state region when present. For a `wasm32` guest,
the complete stitched range must also remain within the 4 GiB linear address
space. The current ABI carries the base offset as `u32`, while descriptor
offsets are retained as `u64`.

## Mailboxes

The mailbox is a `BatchMetadata` value stored at the end of the private lower
region. `mailbox_offset` is a Wasm-linear offset and is different for each
instance only because each instance has a different linear-memory base. The
host copies packet metadata into that mailbox, invokes the component, and
copies the resulting actions and descriptor updates back.

Because the mailbox is private and the UMEM is shared, mailbox metadata does
not consume UMEM frames and does not need fill-queue protection. Packet data
continues to be processed in place, so the shared UMEM remains zero-copy.

## Bounds and lifetime

The runtime bounds-checks Wasm accesses against the stitched mapping. The host
must validate descriptor offsets and lengths against the UMEM size before
passing them to Wasm. The UMEM's original mapping remains owned by `xsk-core`
and registered with AF_XDP; the per-instance mapping only aliases its backing
pages.

Hot-reload plugin state uses a separate dedicated shared region and does not
reserve UMEM frames. Its layout and access rules are defined in the
[WASM shared state contract](wasm_shared_state.md).
