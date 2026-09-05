# AGENTS.md — AI Assistant Project Constitution

## 1. Project Overview & Philosophy

The system is split across two repositories:

* **wabrix:** This repository. The dataplane engine — the AF_XDP socket layer, pipeline engine, config parser, WASM host, and management binary.
* **wabrix_api:** The public SDK for building WASM plugins for the Wabrix platform. Contains the `wabrix_api` crate, the `wit/wabrix.wit` interface definition, and example `plugins/`.

Sibling checkouts live next to this repo and are referenced by relative Cargo paths: `../xsk-rs` (AF_XDP bindings), `../wabrix_api/wabrix_api` (host-side SDK). The prebuilt `ipblower` load generator and example endpoints live in the separate `wabrix_ep` repo.

Wabrix is a high-performance, software-based networking engine/router. The system processes packets at high throughput (target: 25 Gbps – 100 Gbps) using Linux kernel-bypass via AF_XDP, and features a sandboxed WebAssembly plugin architecture for data-plane programmability.

**Core Engineering Principles:**
1. **Zero-Copy First:** Memory allocations inside the packet processing hot-path are strictly prohibited.
2. **Predictable Latency:** Favor deterministic execution paths over peak theoretical throughput.
3. **Strict Memory Safety:** Leverage Rust's type system to guarantee safety across the Native ↔ Wasm boundary.

***This file is for the `wabrix` repository, only.***

---

## 2. Workspace Structure & Module Boundaries

The `wabrix` workspace (`Cargo.toml` at the root) contains four crates:

```
wabrix/
├── wabrix_base/     # Core types shared across all crates (no I/O, no async runtime)
├── wabrix_lib/      # AF_XDP socket layer, pipeline engine, config parser, BPF loading
├── wabrix_wasm/     # Wasmtime runtime, WIT host-side bindings, plugin manager
└── wabrix/          # Binary: CLI entry point + Prometheus metrics server
```

There is **no `ipblower` crate in this workspace.** In-tree traffic generation is a pipeline node (`GeneratorNode` in `wabrix_lib/src/generator_node.rs`); the standalone `ipblower` load tool is an external prebuilt binary (`bin/ipblower`, Docker image `ghcr.io/rstade/ipblower:latest`, driven by `run_ipblower.sh`) sourced from the `wabrix_ep` repo.

### Crate responsibilities & allowed dependencies

| Crate | Role | May depend on |
|---|---|---|
| `wabrix_base` | Core types: `RuntimeUmemConfig`, `UmemPartition`, `SystemBus`, `Directive`, `Node` trait, `NodeId`, `FrameRegistry`, `Pdu`, `Batch`, telemetry counters | `xsk-rs`, `crossbeam-queue`, `log`, `serde`, `macaddr`, `network-types` |
| `wabrix_lib` | AF_XDP sockets (`XSocket`/`XSocketRx`/`XSocketTx`), pipeline DAG, TOML config, BPF program loading, system builder/runner | `wabrix_base`, `xsk-rs`, `libbpf-rs`, `wabrix_wasm` (behind `wasm` feature), `tokio` |
| `wabrix_wasm` | WASM runtime host: loads `.wasm` components, exposes host functions via WIT bindings, manages plugin lifecycle | `wabrix_base`, `wabrix_api`, `wasmtime`, `wasmtime-wasi` |
| `wabrix` | Binary: CLI (`clap`), Prometheus metrics exporter (`axum` + `prometheus`), stdin command loop, config hot-reload orchestration, logging sink | `wabrix_lib`, `wabrix_base` |

**Dependency direction is strictly one-way:** `wabrix` → `wabrix_lib` → `wabrix_base`. `wabrix_lib` pulls in `wabrix_wasm` only through the optional `wasm` feature (`dep:wabrix_wasm`). Never introduce reverse or circular dependencies.

### Key source files

| File | Contains |
|---|---|
| `wabrix_base/src/lib.rs` | `RuntimeUmemConfig`, `UmemPartition`, `SystemBus` (bit-packed return queues), `Directive`, `Node` trait, `NodeId`, `ControlEvent` |
| `wabrix_base/src/frame_registry.rs` | `FrameRegistry`, `PartitionData`, thread-local `WORKER_FRAME_REGISTRY`, encode/decode helpers (`CMD_BIT`, `is_retain`, `decode_global_frame`) |
| `wabrix_base/src/mailbox.rs` | UMEM control-block / mailbox frame math for WASM plugin instances |
| `wabrix_base/src/node_instance_id.rs` | Node instance identity types |
| `wabrix_base/src/system.rs` | Platform detection, TSC/`rdtsc` helpers, NIC stat probing, IRQ/CPU-affinity and system-readiness checks |
| `wabrix_base/src/interface/pdu.rs` | `Pdu` (packet descriptor carrying global frame index + UMEM pointer) |
| `wabrix_base/src/interface/batch.rs` | `Batch` (fixed-capacity burst container, zero-alloc on hot path) |
| `wabrix_base/src/telemetry.rs` | Lock-free metric counters, `TMLiveStats`, telemetry spawners |
| `wabrix_lib/src/xsocket.rs` | `XSocket`, `XSocketRx`, `XSocketTx`, `XdpSocketConfig` |
| `wabrix_lib/src/system_builder.rs` | System wiring: UMEM allocation, partition assignment, socket registry |
| `wabrix_lib/src/system_runner.rs` | Worker thread spawning / lifecycle, pipeline launch |
| `wabrix_lib/src/pipeline.rs` | Pipeline DAG runner / scheduler, node wiring |
| `wabrix_lib/src/nodes.rs` | Built-in pipeline nodes (RX/TX/etc.) |
| `wabrix_lib/src/generator_node.rs` | `GeneratorNode` — in-tree synthetic packet generator |
| `wabrix_lib/src/tcp_node.rs` | Stateful TCP tracking node |
| `wabrix_lib/src/toml_parser.rs` | `WabricksConfig` — top-level TOML config deserializer |
| `wabrix_lib/src/config/` | `configure_bpf`, `interface_setup`, `hot_config` (`SharedSystemState`), `reconciliation` (hot-reload diffing) |
| `wabrix_lib/src/allocators/` | Cache-aligned allocation helpers |
| `wabrix_lib/src/bpf/` | eBPF source (`xscrubber.bpf.c`, `vmlinux.h`) compiled at build time |
| `wabrix_wasm/src/wasm_manager.rs` | Plugin lifecycle: UMEM handles, host state, load/instantiate/call/unload |
| `wabrix_wasm/src/wasm_engine.rs` | Wasmtime `Engine` + `Linker` setup, host-function registration |
| `wabrix_wasm/src/wasm_engine.rs` | `WasmEngine` — pipeline node driving a WASM component |
| `wabrix/src/main.rs` | Entry point: config load, platform detection, `SystemCommand` loop, pipeline launch |
| `wabrix/src/metrics_server.rs` | `spawn_metrics_server` — Prometheus exporter over `axum` |
| `wabrix/src/logger.rs` | Logging sink setup |

---

## 3. Technical Stack & Hard Boundaries

### Language & Compiler
* **Language:** Rust (Edition 2021).
* **Linting:** Code must pass `cargo clippy --all-targets --all-features -- -D warnings`. Fix every warning; do not `#[allow(...)]` without a comment explaining why it is unavoidable.

### Networking & Kernel Bypass (AF_XDP)
* **MANDATORY LIBRARY:** Use the fork of the `xsk-rs` crate (either workspace path `../xsk-rs` or { git = "https://github.com/ninegamma/xsk-rs.git", branch = "master" } ) for all AF_XDP socket manipulations, UMEM setup, and ring-buffer handling.
* **FORBIDDEN:** Do NOT use `aya`, `std::net` sockets, or `libpcap` for data-plane packet I/O.
* BPF programs are loaded via `libbpf-rs`/`libbpf-sys`. Skeleton code is generated at build time by `libbpf-cargo` in `wabrix_lib/build.rs`. The eBPF source lives in `wabrix_lib/src/bpf/` (`xscrubber.bpf.c`).

### Plugin Engine (WebAssembly)
* **Wasm Runtime:** `wasmtime` + `wasmtime-wasi` (Component Model enabled).
* **Interface Standard:** The `.wit` interface lives in the sibling `wabrix_api` repo (`wabrix_api/wit/wabrix.wit`). Host bindings are generated via `wit-bindgen`; the host-side crate is pulled in as `wabrix_api = { path = "../wabrix_api/wabrix_api", features = ["host"] }`.
* The `wasm` feature gate controls whether the WASM runtime is compiled in. It is **on by default for `wabrix_lib`** (`default = ["profiling", "wasm"]`) but **opt-in for the `wabrix` binary** — build the binary with `--features wasm` to include it.

### Async Runtime
* **Tokio** is the async executor — used on the control plane only (metrics server, config hot-reload, telemetry broadcast, stdin command loop).
* Worker threads running the packet pipeline are **not** async; they spin in a tight polling loop pinned to a CPU core via core affinity.

### Feature Flags
* `wasm` — compiles in the Wasmtime runtime and plugin host (see above).
* `profiling` — per-node cycle accounting on the hot path (on by default).
* `performance` — performance-tuned build switches.

### Key External Crates
* `crossbeam-queue` — lock-free `ArrayQueue` used for `SystemBus` return queues.
* `crossbeam-channel` — MPSC channels for control-plane signalling.
* `arc-swap` — atomic `Arc` swapping for zero-downtime config hot-reload.
* `network-types` — zero-copy Ethernet/IP/UDP header structs.
* `macaddr` — MAC address parsing/formatting.
* `axum` + `prometheus` — control-plane HTTP metrics exporter.
* `ts-rs` — TypeScript type export for the management UI boundary.
* `anyhow` / `thiserror` — error handling on the cold path only.

---

## 4. Coding Commandments

### A. The Hot Path (Packet Processing Loop)

When writing or refactoring code inside the data-plane hot path (any code called from a worker thread per-burst, i.e. `Node::process`):

* **No Heap Allocations:** Never use `Box`, `Vec::push` (growing), `String`, `Arc::new`, or `Rc` inside the packet loop. Use pre-allocated buffers or stack arrays.
* **No Panics:** Never `.unwrap()` or `.expect()` on the hot path. Handle broken packets by incrementing a lock-free telemetry counter and dropping the frame immediately.
* **No Dynamic Dispatch:** Avoid `dyn Trait` on the hot path; prefer generics with trait bounds or enum dispatch so the compiler can inline and devirtualize.
* **No Locking:** Never acquire a `Mutex` or `RwLock` from a worker thread. All cross-thread communication goes via lock-free channels (`ArrayQueue`, `crossbeam-channel`).
* **Frame Ownership:** When a worker is done with a `Pdu` it does not own, it MUST push the encoded frame command to the owning worker's `SystemBus` slot. Never silently drop a foreign frame.

### B. The Cold Path (Control Plane, CLI, Setup)

Standard Rust ergonomics apply. Heap allocations, `anyhow`/`thiserror` for error handling, and `tokio`/async are permitted.

### C. Unsafe Code Guidelines

* `unsafe` blocks are restricted to: hardware ring-buffer access, raw pointer dereferencing for UMEM frames, Wasm linear-memory mapping, and platform-specific CPU intrinsics (e.g., TSC reads).
* **MANDATORY:** Every `unsafe` block must be preceded by a `// SAFETY:` comment explicitly proving why the operation cannot violate memory boundaries or cause data races.

### D. Unrequested Code Changes

* **NEVER** change fundamental structs and interfaces except if explicitly requested. In case you need to do the change, ask for permission with the exact reasoning.

---

## 5. Architecture Quick Reference

> Full details: `docs/` and `wabrix/src/architecture.md` (if present).

* **UMEM:** A contiguous `mmap`'d region shared by multiple AF_XDP sockets. Divided into fixed-size frames (e.g., 2048 B). Multiple UMEM groups can coexist (one per NUMA domain or per NIC).
* **FrameRegistry (thread-local):** Each worker owns a `FrameRegistry` stored in thread-local storage (`WORKER_FRAME_REGISTRY`). It holds a `registry_map: Vec<Option<PartitionData>>` (one entry per UMEM group) and a `return_queue` (`Arc<ArrayQueue<u32>>`) for receiving frames returned by other workers.
* **SystemBus:** Connects all workers. One `Arc<ArrayQueue<u32>>` slot per worker (`bus_senders`), plus the full set of `umem_partitions` for owner lookup. Frame commands are bit-packed: `bit 31 = command (0=Release, 1=Retain)`, `bits 30-24 = umem_id`, `bits 23-0 = local_idx`. Helpers live in `frame_registry` (`CMD_BIT`, `is_retain`, `decode_global_frame`).
* **Global vs Local frame index:** Encoded global index = `(umem_id << 24) | local_idx`. The application works in encoded/global indices; the kernel rings use local indices. `XSocketRx`/`XSocketTx` translate at the boundary.
* **XSocket split:** Each `XSocket` is split into `XSocketRx` (owns RX + Fill rings, accesses FrameRegistry via TLS) and `XSocketTx` (owns TX + Completion rings, returns frames via SystemBus only — never touches the registry directly).
* **Pipeline DAG:** Workers run a sequence of nodes implementing the `Node` trait. Each node's `process(&mut self, batch: &mut Batch) -> Directive` mutates the batch in place (run-to-completion) and returns a `Directive` (`Next(NodeId)`, `Split(Vec<(NodeId, Batch)>)`, or `Stop`) telling the scheduler where the batch goes next. Nodes may also opt into `poll_timers` and `handle_control_event`. A `Batch` is a fixed-capacity burst container passed between nodes without allocation.

---

## 6. Verification & Tooling Commands

```bash
# Build the engine binary WITH the WASM runtime (release)
cargo build -p wabrix --release --features wasm

# Build the engine binary WITHOUT WASM (default features only)
cargo build -p wabrix --release

# Lint (all targets, all features — must be clean)
cargo clippy --all-targets --all-features -- -D warnings

# Unit tests
cargo test

# Run the engine (requires root for AF_XDP)
sudo ./target/release/wabrix --config wabrix.toml

# Run the external ipblower load generator (prebuilt binary / Docker image)
sudo ./bin/ipblower --help
./run_ipblower.sh            # convenience wrapper (interface / PPS / core pinning)
```

---

## 7. AI Persona & Output Formatting

1. **Expert Systems Tone:** Do not explain basic Rust concepts (ownership, borrow checker, lifetimes). Assume the reader is a senior systems engineer fluent in Rust and Linux networking internals.
2. **Hot-Path Change Rationale:** When suggesting modifications on the hot path, explicitly comment *why* the change saves CPU cycles, reduces L1/L2 cache misses, or avoids branch mispredictions.
3. **Minimal Diffs:** When asked to fix a function, output only the relevant function or module diff — not the entire file.
4. **Feature Gates:** Always check whether code belongs behind the `wasm`, `profiling`, or `performance` feature flags before adding new conditional compilation.
5. **No Silent Frame Drops:** Never suggest code that discards a `Pdu` or raw frame index without either returning it via `SystemBus` or explicitly accounting for it in a telemetry counter.
