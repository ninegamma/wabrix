# wabrix memory architecture

Each Wasm instance receives a private linear-memory mapping. The mapping has a
private lower region for the module's heap, stack, and globals, followed by a
private mailbox at the end of that region. The complete AF_XDP UMEM is then
mapped above it from the same memfd-backed pages used by `xsk-core`:

```text
Wasm address 0                                      Wasm address 2 GiB
       │                                                     │
       ▼                                                     ▼
┌──────────────────────────────┬──────────────┬───────────────────────────┐
│ private Wasm heap and stack  │ private      │ shared AF_XDP UMEM        │
│                              │ mailbox      │ (same pages for all       │
│                              │              │  Wasm instances)          │
└──────────────────────────────┴──────────────┴───────────────────────────┘
```

The UMEM itself has no reserved prefix or control block. Every frame is
available to AF_XDP queues. Mailboxes are outside the UMEM and are therefore
never offered to the NIC. Each Wasm instance gets its own mailbox, while all
instances see the same packet frames through their UMEM alias.

The host keeps packet descriptor addresses relative to the native UMEM base.
The UMEM starts at `MAX_WASM_HEAP_BYTES` in Wasm memory, so the host passes:

```text
wasm_partition_offset = 0 - MAX_WASM_HEAP_BYTES   (u32 wrapping arithmetic)
```

The guest computes the Wasm address of a UMEM-relative descriptor as:

```text
umem_addr = 0u32.wrapping_sub(partition_offset)
packet_wasm_addr = umem_addr + desc.addr
```

For the current mapping this is equivalent to adding 2 GiB to the UMEM
offset. The same UMEM-relative descriptor can therefore be used by every
instance, while each instance's private mailbox is addressed by its own
Wasm-linear `mailbox_offset`.

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
