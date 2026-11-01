# WASM shared state contract

This document defines the contract for preserving plugin state across a WASM
hot reload by mapping the same backing region into the active and staged
instances.

## Ownership and scope

- A region belongs to one logical WASM plugin instance: one pipeline node on
  one worker. The active runtime and its single staged replacement share it.
- Different workers and different plugin nodes receive different regions.
- The host owns the region. It creates and zeroes a region for a fresh plugin
  instance, and keeps its backing alive until every runtime mapping has been
  released.
- A replacement attaches to the existing region. It does not clear or
  initialize the payload.
- Engine hot reload keeps the same backing region for the staged replacement;
  the host does not serialize or restore a state snapshot.
- The shared region is dedicated to plugin state. It is separate from AF_XDP
  UMEM, whose frames remain available to packet queues.

## Region layout, version 1

All offsets below are relative to the start of the shared region. Integer
fields are little-endian. The host page-aligns the allocation; the payload
starts at offset 64, which is also its required minimum alignment.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic bytes `WABSHR01` |
| 8 | 4 | Header version, currently `1` |
| 12 | 4 | Header size, currently `64` |
| 16 | 16 | Plugin state type ID |
| 32 | 4 | Plugin payload schema version |
| 36 | 4 | Current plugin payload size in bytes |
| 40 | 4 | Payload capacity in bytes, reserved for this state type |
| 44 | 4 | Maximum payload alignment, currently `64` |
| 48 | 4 | Flags, zero in version 1 |
| 52 | 12 | Reserved, zero in version 1 |
| 64 | variable | Fixed-layout plugin payload |

The host reserves the declared payload capacity when it creates the region.
The mapped allocation may include page padding after the header and capacity;
that padding is not part of the payload and must not be used by the plugin.
The payload size records the bytes used by the current schema. The capacity
records the maximum space available for append-only extensions.

Payload type ID, schema version, current size, reserved capacity, required
alignment, and supported source schema versions are declared by the plugin.
On first attachment, the host records the declared reserved capacity in the
region header. A replacement must use the same type ID, require no more than
the region's alignment, and declare a reserved capacity no larger than the
capacity already recorded in the header. It must either use the same schema
version and size or declare an append-only upgrade from the region's current
schema version. Unused backing capacity cannot be claimed by a later release.

The state type ID is a stable 16-byte identifier for the payload definition.
It identifies the kind of state, not a particular node, worker, or running
instance. Releases of the same state type keep the same ID; increment the
schema version whenever the payload layout or meaning changes. Independent
payload definitions use different IDs. The ID and the other payload layout
fields are returned in the plugin's read-only shared-state descriptor so the
host can compare the candidate with the region header before enabling payload
access.

The payload is a fixed-size, plugin-defined data structure with a stable
`repr(C)` layout and explicit padding where needed. Its fields use fixed-width
integer types and fixed-size arrays. It contains no pointers, references,
`Vec`, `String`, maps, or other allocation-owning Rust values. Guest pointers
are instance-local, so cross-instance references are represented as offsets
within the payload or as stable identifiers.

## Initialization and access rules

- The first runtime initializes the payload exactly once before it becomes
  active. The plugin defines the initial values; all-zero bytes are not assumed
  to be a valid initialized payload.
- A staged replacement validates the header and payload schema, then attaches
  without mutating the payload during staging.
- An append-only upgrade keeps every existing field at the same offset with
  the same type and meaning, and adds fields only after the old payload. Any
  padding needed to align an appended field is also added after the old
  payload. The replacement defines initial values for the new fields.
- A replacement declares the source schema versions it can upgrade. The host
  checks that declaration, that the new payload size is at least the old size,
  and that the new payload fits within the reserved capacity. The host cannot
  infer field meaning from byte size; the plugin's schema declaration is the
  compatibility contract.
- At switchover, the runner reaches a quiescent boundary: the old instance is
  no longer executing plugin code before the replacement is allowed to mutate
  the region. If the candidate attached an older compatible schema, the host
  calls its `upgrade-shared-state(source_schema_version, source_payload_size)`
  export. The replacement's upgrade routine initializes only its appended
  fields. On success, the host commits the new schema version and payload size
  before installing the replacement. Existing payload bytes are left intact.
  If the upgrade call traps or returns an error, the host leaves the header
  unchanged, discards the candidate, and keeps the old runtime active; the old
  schema ignores any bytes written beyond its payload size. There is one active
  guest writer at a time.
- Version 1 payload access is otherwise ordinary fixed-layout memory. If
  concurrent guest or host access is introduced later, the affected fields
  must use an explicitly specified synchronization scheme.
- A change that reorders, removes, changes the type of, or changes the meaning
  of an existing field is not append-only. It requires a separate migration
  contract; the host rejects it under this contract.

Only state that must survive replacement belongs in the payload. Transient
buffers and caches may be rebuilt by the replacement. Configuration continues
to use the existing configuration path unless a plugin explicitly defines a
configuration field as persistent state.

## Mapping boundary

The contract defines region-relative offsets. The host maps the region at
`MAX_WASM_HEAP_BYTES + align_up(umem_size, 64 KiB)`, after the complete UMEM
alias and its alignment gap. Both instances using the same UMEM receive the
region at the same guest-visible offset. Plugin code must obtain the region
through the shared-state API rather than hard-code a linear-memory address.

The region's payload capacity is fixed for its lifetime. The host currently
reserves a 127 KiB payload capacity for each reloadable `WasmEngine`. The
130,048-byte payload plus the 64-byte header is 130,112 bytes; rounding up to a
multiple of 64 KiB produces a 131,072-byte (128 KiB) mapping, leaving 960 bytes
unused. This is the current host ceiling; a plugin descriptor can request a
smaller capacity. Future compatible plugin versions can use unused capacity
for appended fields, but cannot grow or relocate the region.

The shared state region must not consume UMEM frames. The current memory map
places the UMEM alias at the 2 GiB boundary and uses `u32` guest offsets, so
the mapping implementation must account for the combined range and the
`wasm32` 4 GiB address-space limit.
