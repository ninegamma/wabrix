# wabrix TOML Configuration Reference

The wabrix configuration file (default: `wabrix.toml`) is passed via the `-c/--config` flag.
It has three top-level sections: `[wabrix]`, `[[pipelines]]`, and `[layout]`.

---

## Top-Level Structure

```toml
[wabrix]          # required — hardware and memory infrastructure
[[pipelines]]      # optional (array) — packet processing graphs
[layout]           # optional — UI node positions, managed by the frontend
```

---

## [wabrix] — Infrastructure

All four sub-keys have defaults, but in practice `umem`, `xsk`, and `workers` must be set explicitly for any real workload.

### wabrix.ifaces — Interface Declarations

Array of network interfaces used by Wabrix. Non-veth interfaces must exist on the system.

Default: `[{ name = "lo" }]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Interface name (e.g. `"enp2s0f0np0"`). Must be unique. |
| `rxd` | integer | no | NIC RX descriptor ring size. |
| `txd` | integer | no | NIC TX descriptor ring size. |
| `irq_core` | integer | no | CPU core for NIC interrupt affinity. |
| `type` | string | no | Set to `"veth"` to create a virtual ethernet pair. |
| `peer` | string | no | Peer interface name. Required when `type = "veth"`. |
| `namespace` | string | no | Network namespace for the peer. Required for veth unless `master` is set. |
| `master` | string | no | Bridge master interface. Alternative to `namespace` for veth. |
| `mac_if` | string | no | MAC address source interface (veth inherits its MAC from this). |
| `ipnet` | string | no | IP address and prefix length assigned to the interface, e.g. `"192.168.1.1/24"`. |
| `checksum_offload` | boolean | no | Enable TX checksum offload. Default `false`. For veth, targets the peer. |
| `napi` | table | no | Interface-wide Linux NAPI busy-poll settings. |

Fields within `napi`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `flush_timeout_ns` | integer | `0` | Interface GRO flush timeout in nanoseconds. |
| `defer_irqs` | integer | `0` | Number of consecutive empty NAPI polls allowed before NIC hard IRQs are re-enabled. `0` re-enables interrupts immediately. |

**Validation:** Non-veth interfaces are checked via `ip link show`. Veth interfaces require `peer` and either `namespace` or `master`.

**Example:**
```toml
ifaces = [
    { name = "enp2s0f0np0", rxd = 8160, txd = 4096, irq_core = 8 },
    { name = "enp2s0f1np1", irq_core = 9,
      napi = { flush_timeout_ns = 60000, defer_irqs = 1024 } },
    { name = "veth_wabrix", type = "veth", namespace = "ns-wabrix",
      peer = "tap-veth", mac_if = "enp2s0f0np0", ipnet = "192.168.177.222/24" },
]
```

---

### wabrix.umem — User Memory Regions

Array of AF_XDP UMEM (shared packet buffer) configurations. At least one is required.

Default: one entry with `id=0`, `frame_size=2048`, `frame_count=65536`, `fq_size=8192`, `cq_size=8192`, `head_room=128`, `partitions=1`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | integer | **yes** | Unique UMEM identifier. Must be 0–255. |
| `frame_size` | integer | **yes** | Size in bytes of each frame. Must be a power of 2 and > 0. |
| `frame_count` | integer | **yes** | Total number of frames in the UMEM. Must be > 0 and evenly divisible by `partitions`. The UMEM byte size (`frame_count × frame_size`) must be a multiple of the system's huge-page size; reserve enough huge pages for the UMEM. |
| `fq_size` | integer | **yes** | Fill queue ring size. Must be > 0. |
| `cq_size` | integer | **yes** | Completion queue ring size. Must be > 0. |
| `head_room` | integer | **yes** | Reserved bytes at the start of each frame for metadata headers. Must be < `frame_size`. |
| `partitions` | integer | no | Number of memory partitions (one per worker). Default `1`. Must be ≥ 1. |

**Sizing guidance:** `frame_count` must cover hardware-locked frames (`fq_size + rxq_size + txq_size` per socket) plus the frames needed by the application.

UMEM memory is mapped with huge pages. For example, with 2 MiB huge pages and a `frame_size` of 2048 bytes, `frame_count` must be a multiple of 1024. If `partitions = 3`, it must satisfy both constraints, so use a multiple of 3072 (for example, `301056`). The host must also have enough huge pages reserved for the UMEM.

**Example:**
```toml
umem = [
    { id = 0, frame_size = 2048, frame_count = 524288, fq_size = 32768, cq_size = 16384,
      head_room = 128, partitions = 4 },
]
```

---

### wabrix.xsk — XDP Socket Mappings

Array of AF_XDP socket bindings. At least one is required.

Default: `[{ iface = "lo", queues = [0], umem = 0, rxq_size = 8192, txq_size = 8192, zerocopy = true, xdp_mode = "auto" }]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `iface` | string | **yes** | Interface name. Must match an entry in `ifaces`. |
| `queues` | array of integers | **yes** | Hardware queue IDs to bind (non-empty). |
| `umem` | integer | **yes** | UMEM `id` to attach this socket to. Must exist in `umem`. |
| `rxq_size` | integer | **yes** | RX ring size. Must be a power of 2 and > 0. |
| `txq_size` | integer | **yes** | TX ring size. Must be a power of 2 and > 0. |
| `zerocopy` | boolean | **yes** | Enable AF_XDP zero-copy mode. Requires driver support; use `false` for veth. |
| `xdp_mode` | string | no | XDP attachment mode: `"native"`, `"skb"`, or `"auto"`. Default `"auto"`. |
| `poll_mode` | string | no | RX polling mode: `"userspace_ring_spin"` or `"napi_busy_poll"`. Default `"userspace_ring_spin"`. |
| `napi_poll_timeout_ns` | integer | no | Userspace `poll()` timeout for NAPI RX/TX polling, in nanoseconds. Default `20_000`. |
| `busy_poll_ns` | integer | no | Per-socket NAPI busy-poll timeout in nanoseconds. Default `20_000`. |
| `busy_poll_budget` | integer | no | Maximum packets processed per NAPI busy-poll cycle. Default `64`. |

**Example:**
```toml
xsk = [
    { iface = "enp2s0f0np0", queues = [0, 1], umem = 0, rxq_size = 4096, txq_size = 4096,
      zerocopy = true, xdp_mode = "auto" },
    { iface = "enp2s0f1np1", queues = [0], umem = 1, rxq_size = 2048, txq_size = 2048,
      zerocopy = true, xdp_mode = "auto", poll_mode = "napi_busy_poll",
      napi_poll_timeout_ns = 2050, busy_poll_ns = 3000, busy_poll_budget = 1024 },
]
```

---

### wabrix.workers — Worker Thread Configuration

Array of worker threads. At least one is required.

Default: one worker with `id=0`, `core=0`, `partitions=[{ umem=0, count=1 }]`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | integer | **yes** | Unique worker identifier. |
| `core` | integer | **yes** | Logical CPU core to pin this worker to. Must be < total available cores. |
| `partitions` | array of objects | no | UMEM partition requests (see below). Default `[]`. |

Each element of `partitions`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `umem` | integer | **yes** | UMEM `id` to request partitions from. |
| `count` | integer | **yes** | Number of partitions to claim. Must be > 0 and ≤ the UMEM's `partitions` value. Total across all workers cannot exceed the UMEM's `partitions`. |

**Example:**
```toml
workers = [
    { id = 0, core = 9,  partitions = [{ umem = 0, count = 1 }] },
    { id = 1, core = 10, partitions = [{ umem = 0, count = 1 }, { umem = 1, count = 1 }] },
]
```

---

## [[pipelines]] — Packet Processing Pipelines

Each pipeline is a directed processing graph. Pipelines are optional; there is no default.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Pipeline name. Should be unique. |
| `workers` | array of integers | **yes** | Worker IDs this pipeline runs on. Non-empty. Each worker gets its own instance. |
| `nodes` | array of node tables | **yes** | Processing nodes (non-empty). |

---

### [[pipelines.nodes]] — Processing Nodes

Each pipeline contains an array of node tables declared with `[[pipelines.nodes]]`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | **yes** | Node identifier, unique within the pipeline. |
| `type` | string | **yes** | Node type (see node types below). |
| `routes` | array of strings | no | Node IDs that receive packets from this node. Default `[]`. All referenced IDs must exist in the same pipeline. |
| `queues` | array of integers | no | Hardware queue IDs. Length must equal the pipeline's `workers` array length; `queues[i]` maps to `workers[i]`. Required for `BatchRxNode` and `BatchTxNode` when binding to a hardware queue. |
| `labels` | array of strings | no | Metric label names for WASM output routes. |
| `args` | inline table | no | Node-specific key/value arguments. Values may be strings, integers, floats, or booleans (non-string values are coerced to strings). Default `{}`. |
| `hot_args` | inline table or table section | no | Live-updatable key/value arguments. Same value types as `args`. Take precedence over `args` for the same key. Always serialized as an inline table on save. See [Hot Args](#hot-args) below. |
| `node_data` | inline table | no | Initial state/configuration passed into WASM modules. Same value types as `args`. |

---

### Hot Args

`hot_args` is a TOML sub-table (written as `[pipelines.nodes.hot_args]`) that holds parameters which can be updated at runtime without restarting the dataplane.

**Key properties:**

- **Startup:** Values are parsed once and loaded into the node's typed `NodeDynamicState` alongside the values from `args`. Where the same key appears in both, `hot_args` takes precedence (via `effective_arg()`).
- **Live update:** The GUI "⚡ APPLY LIVE UPDATE" button pushes changed `hot_args` to the running dataplane via `POST /api/pipeline/update`. Workers pick up the new values on the next batch boundary with no downtime.
- **Persistence:** A live update writes the new values to the in-memory base config, so a subsequent "File → Save to Disk" captures them. They are not auto-written to disk on update.
- **Syntax:** `hot_args` accepts both an inline table and a TOML table section. It is always written back as an inline table by File → Save to Disk:

```toml
# inline form (preferred, written by Save to Disk)
[[pipelines.nodes]]
id = "pctgen"
type = "PacketGenerator"
args = { umem = "0" }
hot_args = { pps = "1024", batch_size = "256", packet_count = "10000", dst_ip = "192.168.177.1", dst_mac = "08:bf:b8:0a:0b:0c" }

# table-section form (also accepted on load)
[[pipelines.nodes]]
id = "pctgen"
type = "PacketGenerator"
args = { umem = "0" }

[pipelines.nodes.hot_args]
pps = "1024"
batch_size = "256"
packet_count = "10000"
dst_ip = "192.168.177.1"
dst_mac = "08:bf:b8:0a:0b:0c"
```

**Supported keys per node type:**

| Node type | Key | Description |
|-----------|-----|-------------|
| `PacketGenerator` | `pps` | Packets per second. Same format as `args.pps`. |
| `PacketGenerator` | `batch_size` | Packets generated per batch. Must be 1–512. |
| `PacketGenerator` | `packet_count` | Total packets per run before stopping. `0` = unlimited. |
| `PacketGenerator` | `dst_ip` | Destination IP address for generated packets. |
| `PacketGenerator` | `dst_mac` | Destination MAC address for generated packets. |

> **Note:** For `PacketGenerator`, the inter-batch gap is derived from both `pps` and `batch_size` together (`gap = tsc_freq / pps * batch_size`). Both must be updated consistently: setting `pps=16` with `batch_size=256` (default) produces a 16-second gap, not a 1-second gap.

---

### Node Types

#### `BatchRxNode` — Hardware RX source

Receives packets from an AF_XDP socket.

| `args` key | Required | Description |
|------------|----------|-------------|
| `interface` | **yes** | Interface name (must be in `ifaces`). |
| `batch_size` | no | Maximum packets per receive call. |
| `source_type` | no | Source type; `"xsk"` for AF_XDP (default). |

---

#### `BatchTxNode` — Hardware TX sink

Transmits packets through an AF_XDP socket.

| `args` key | Required | Description |
|------------|----------|-------------|
| `interface` | **yes** | Interface name (must be in `ifaces`). |
| `sink_type` | no | Sink type; `"xsk"` for AF_XDP (default). |

---

#### `PacketGenerator` — Synthetic traffic source

Generates packets at a configurable rate. Does not use `queues`.

`args` keys (static — require dataplane restart to change):

| `args` key | Required | Description |
|------------|----------|-------------|
| `umem` | **yes** | UMEM `id` to allocate frames from. |
| `autostart` | no | `"true"` to start generating immediately on startup. |
| `wasm_file_path` | no | Path to a WASM module that fills packet payloads. |
| `no-zerocopy` | no | `"true"` to disable zero-copy for this generator. |

`hot_args` keys (live-updatable without restart — see [Hot Args](#hot-args)):

| `hot_args` key | Required | Description |
|----------------|----------|-------------|
| `pps` | **yes** | Packets per second. Accepts: plain integer (`1000`), decimal with suffix (`2.5M`, `10k`, `1G`), or `"max"`. |
| `batch_size` | no | Packets per generation batch. Default `256`. Must be 1–512. |
| `packet_count` | no | Total packets per run before the generator stops. Same format as `pps`. `0` means unlimited. |
| `dst_ip` | no | Destination IP address for generated packets. |
| `dst_mac` | no | Destination MAC address for generated packets. |

---

#### `WasmEngine` — Full WASM packet processor

Runs a WASM module that processes each incoming packet and routes output.

| `args` key | Required | Description |
|------------|----------|-------------|
| `wasm_file_path` | **yes** | Absolute path to the `.wasm` module. |
| `umem` | **yes** | UMEM `id` frames belong to. For multi-worker pipelines, this may be an array with one value per worker. |

`node_data` key/value pairs are passed as initial state to the WASM module.

---

#### `FastParserNode` — Protocol header parser

Parses Ethernet/IP/TCP/UDP headers and annotates frames for downstream nodes.

No required `args`. Takes no `queues`.

---

#### `TelemetryNode` — Packet statistics collector

Classifies and counts packets by a chosen algorithm.

| `args` key | Required | Description |
|------------|----------|-------------|
| `algo` | no | Classification algorithm. `"ethertype"` counts by EtherType (IPv4/IPv6/ARP/Other). |
| `buckets` | no | Number of statistic buckets. |
| `labels` | no | Comma-separated label names for the buckets. |

---

#### `DropNode` — Packet discard sink

Silently drops all received packets. No `args` or `queues`.

---

## [layout] — UI Layout State

Stores frontend node positions. Written and read by the UI; ignored by the dataplane.

Keys are strings of the form `"pipeline_name/node_id"` or infrastructure identifiers like `"worker_0"`, `"umem_0"`, `"xsk_0"`, `"iface_enp2s0f0np0"`.

| Field | Type | Description |
|-------|------|-------------|
| `x` | float | X coordinate in the canvas. |
| `y` | float | Y coordinate in the canvas. |

**Example:**
```toml
[layout."my_pipeline/rx_source"]
x = 207.23
y = 335.46
```

---

## Full Configuration Example

```toml
[wabrix]
ifaces = [
    { name = "enp2s0f0np0", rxd = 8160, txd = 4096, irq_core = 8 },
    { name = "enp2s0f1np1", rxd = 8160, txd = 4096, irq_core = 8 },
]

umem = [
    { id = 0, frame_size = 2048, frame_count = 262144, fq_size = 65536,
      cq_size = 65536, head_room = 128, partitions = 2 },
]

xsk = [
    { iface = "enp2s0f0np0", queues = [0], umem = 0, rxq_size = 32768, txq_size = 8192, zerocopy = true },
    { iface = "enp2s0f1np1", queues = [0], umem = 0, rxq_size = 32768, txq_size = 8192, zerocopy = true },
]

workers = [
    { id = 0, core = 9,  partitions = [{ umem = 0, count = 1 }] },
    { id = 1, core = 10, partitions = [{ umem = 0, count = 1 }] },
]

[[pipelines]]
name = "wire_f0_to_f1"
workers = [0]

    [[pipelines.nodes]]
    id = "rx"
    type = "BatchRxNode"
    routes = ["tx"]
    queues = [0]
    args = { interface = "enp2s0f0np0", batch_size = "256" }

    [[pipelines.nodes]]
    id = "tx"
    type = "BatchTxNode"
    queues = [0]
    args = { interface = "enp2s0f1np1" }

[[pipelines]]
name = "wire_f1_to_f0"
workers = [1]

    [[pipelines.nodes]]
    id = "rx"
    type = "BatchRxNode"
    routes = ["tx"]
    queues = [0]
    args = { interface = "enp2s0f1np1", batch_size = "256" }

    [[pipelines.nodes]]
    id = "tx"
    type = "BatchTxNode"
    queues = [0]
    args = { interface = "enp2s0f0np0" }
```

---

## Key Constraints Summary

| Rule | Detail |
|------|--------|
| UMEM `id` | 0–255 |
| `frame_size` | Power of 2, > 0 |
| `partitions` | ≥ 1 |
| `rxq_size` / `txq_size` | Power of 2, > 0 |
| `head_room` | < `frame_size` |
| Frame count | `frame_count` % `partitions` == 0, and `frame_count × frame_size` must be a multiple of the system huge-page size; reserve enough huge pages |
| Worker `core` | < total logical CPUs on the system |
| Node `queues` length | Must equal pipeline `workers` length |
| `pps` format | Integer, decimal, `k`/`m`/`g` suffix, or `"max"` |
| WasmEngine UMEM | All upstream sources must use a UMEM listed in the node binding; arrays map entries to pipeline workers |
| Partition allocation | Total requested across workers ≤ UMEM `partitions` |
