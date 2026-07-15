# AGENTS.md — AI Assistant Project Constitution

## 1. Project Overview & Philosophy

The project is split into two repositories:

* **xbrix:** Contains the code for the platform of the dataplane engine.
* **xbricks_public:** Contains the public SDK for creating WASM plugins for the XBRIX platform (includes `wabrix_api` crate and `.wit` interface definitions).

These repositories implement a high-performance, software-based networking engine/router. The system processes packets at high throughput (target: 25 Gbps – 100 Gbps) using Linux kernel-bypass via AF_XDP, and features a sandboxed WebAssembly plugin architecture for data-plane programmability.

**Core Engineering Principles:**
1. **Zero-Copy First:** Memory allocations inside the packet processing hot-path are strictly prohibited.
2. **Predictable Latency:** Favor deterministic execution paths over peak theoretical throughput.
3. **Strict Memory Safety:** Leverage Rust's type system to guarantee safety across the Native ↔ Wasm boundary.

***This file is for the xbrix repository, only.***

---

## 2. Workspace Structure & Module Boundaries

The `xbrix` workspace (`Cargo.toml` at the root) contains five crates:

```
xbrix/
├── xbricks_base/     # Core types shared across all crates (no I/O, no async runtime)
├── xbricks_lib/      # AF_XDP socket layer, pipeline engine, config parser
├── xbricks_wasm/     # Wasmtime runtime, WIT host-side bindings, plugin manager
├── xbricks/          # Binary: CLI entry point, REST/WebSocket management API
└── ipblower/         # Binary: standalone packet-generator / load tool (no WASM)
```

### Crate responsibilities & allowed dependencies

| Crate | Role | May depend on |
|---|---|---|
| `xbricks_base` | Core types: `FrameRegistry`, `SystemBus`, `UmemPartition`, `Pdu`, `Batch`, `RuntimeUmemConfig`, telemetry counters | `xsk-rs`, `crossbeam-queue`, `log`, `serde`, external protocol types |
| `xbricks_lib` | AF_XDP sockets (`XSocket`/`XSocketRx`/`XSocketTx`), pipeline DAG, TOML config, BPF program loading, system builder | `xbricks_base`, `xsk-rs`, `libbpf-rs`, `wasmtime` (behind `wasm` feature), `tokio` |
| `xbricks_wasm` | WASM runtime host: loads `.wasm` components, exposes host functions via WIT bindings, manages plugin lifecycle | `xbricks_base`, `wabrix_api`, `wasmtime`, `wasmtime-wasi` |
| `xbricks` | Binary: CLI (`clap`), management REST+WebSocket API (`axum`), logging sink, startup/teardown orchestration | `xbricks_lib`, `xbricks_base` |
| `ipblower` | Standalone packet blower / traffic generator binary | `xbricks_lib` (no-wasm), `xbricks_base` |

**Dependency direction is strictly one-way:** `xbricks` → `xbricks_lib` → `xbricks_base`. Never introduce reverse or circular dependencies.

### Key source files

| File | Contains |
|---|---|
| `xbricks_base/src/lib.rs` | `RuntimeUmemConfig`, `UmemPartition`, `SystemBus` (with bit-packing helpers) |
| `xbricks_base/src/frame_registry.rs` | `FrameRegistry`, `PartitionData`, thread-local `WORKER_FRAME_REGISTRY` |
| `xbricks_base/src/interface/pdu.rs` | `Pdu` (packet descriptor carrying global frame index + UMEM pointer) |
| `xbricks_base/src/interface/batch.rs` | `Batch` (fixed-capacity burst container, zero-alloc on hot path) |
| `xbricks_base/src/telemetry.rs` | Lock-free metric counters, `TMLiveStats`, telemetry spawners |
| `xbricks_lib/src/xsocket.rs` | `XSocket`, `XSocketRx`, `XSocketTx`, `XdpSocketConfig` |
| `xbricks_lib/src/system_builder.rs` | `XSocketRegistry`, system wiring (UMEM allocation, partition assignment) |
| `xbricks_lib/src/pipeline.rs` | Pipeline DAG runner, `PipelineFactory`, `PipelineRunner`, worker thread spawner |
| `xbricks_lib/src/nodes.rs` | Built-in pipeline nodes (`BatchRxNode`, `BatchTxNode`, etc.) |
| `xbricks_lib/src/toml_parser.rs` | `XBricksConfig` — top-level TOML config deserializer |
| `xbricks_lib/src/config/` | BPF program loading, interface setup, hot-reload config state |
| `xbricks_wasm/src/wasm_manager.rs` | Plugin lifecycle: load, instantiate, call, unload |
| `xbricks_wasm/src/wasm_engine.rs` | Wasmtime `Engine` + `Linker` setup, host-function registration |
| `xbricks/src/api.rs` | Axum REST/WebSocket management API, `AppState`, `SystemCommand` |
| `xbricks/src/main.rs` | Entry point: config load, platform detection, pipeline launch |

---

## 3. Technical Stack & Hard Boundaries

### Language & Compiler
* **Language:** Rust (Edition 2021).
* **Linting:** Code must pass `cargo clippy --all-targets --all-features -- -D warnings`. Fix every warning; do not `#[allow(...)]` without a comment explaining why it is unavoidable.

### Networking & Kernel Bypass (AF_XDP)
* **MANDATORY LIBRARY:** Use the `xsk-rs` crate (workspace path `../xsk-rs`) for all AF_XDP socket manipulations, UMEM setup, and ring-buffer handling.
* **FORBIDDEN:** Do NOT use `aya`, `std::net` sockets, or `libpcap` for data-plane packet I/O.
* BPF programs are loaded via `libbpf-rs`/`libbpf-sys`. Skeleton code is generated at build time by `libbpf-cargo` in the build scripts of `xbricks_lib` and `ipblower`.

### Plugin Engine (WebAssembly)
* **Wasm Runtime:** `wasmtime` + `wasmtime-wasi` (Component Model enabled).
* **Interface Standard:** `.wit` files live in `xbricks_public/wit`. Host bindings are generated via `wit-bindgen`.
* The `wasm` feature gate on `xbricks_lib` and `xbricks` controls whether the WASM runtime is compiled in.

### Async Runtime
* **Tokio** is the async executor — used on the control plane only (API server, config hot-reload, telemetry broadcast).
* Worker threads running the packet pipeline are **not** async; they spin in a tight polling loop pinned to a CPU core via `core_affinity`.

### Key External Crates
* `crossbeam-queue` — lock-free `ArrayQueue` used for `SystemBus` return queues.
* `crossbeam-channel` — MPSC channels for control-plane signalling.
* `arc-swap` — atomic `Arc` swapping for zero-downtime config hot-reload.
* `network-types` — zero-copy Ethernet/IP/UDP header structs.
* `anyhow` / `thiserror` — error handling on the cold path only.

---

## 4. Coding Commandments

### A. The Hot Path (Packet Processing Loop)

When writing or refactoring code inside the data-plane hot path (any code called from a `PipelineRunner` worker thread per-burst):

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

> Full details: `xbricks/src/architecture.md`

* **UMEM:** A contiguous `mmap`'d region shared by multiple AF_XDP sockets. Divided into fixed-size frames (e.g., 2048 B). Multiple UMEM groups can coexist (one per NUMA domain or per NIC).
* **FrameRegistry (thread-local):** Each worker owns a `FrameRegistry` stored in thread-local storage. It holds a `registry_map: Vec<Option<PartitionData>>` (one entry per UMEM group) and a `return_queue` (`Arc<ArrayQueue<u32>>`) for receiving frames returned by other workers.
* **SystemBus:** Connects all workers. One `Arc<ArrayQueue<u32>>` slot per worker. Frame commands are bit-packed: `bit 31 = command (0=Release, 1=Retain)`, `bits 30-24 = umem_id`, `bits 23-0 = local_idx`.
* **Global vs Local frame index:** `global_idx = local_idx + group_start_frame_idx`. The application always works in global indices; the kernel rings use local indices. `XSocketRx`/`XSocketTx` translate at the boundary.
* **XSocket split:** Each `XSocket` is split into `XSocketRx` (owns RX + Fill rings, accesses FrameRegistry via TLS) and `XSocketTx` (owns TX + Completion rings, returns frames via SystemBus only — never touches the registry directly).
* **Pipeline DAG:** Workers run a sequence of nodes implementing `BatchRx` or `BatchTx`. A `Batch` is a fixed-capacity burst container passed between nodes without allocation.

---

## 6. Verification & Tooling Commands

```bash
# Build (release, WASM enabled)
cargo build --release

# Build without WASM
cargo build --release --no-default-features

# Lint (all targets, all features — must be clean)
cargo clippy --all-targets --all-features -- -D warnings

# Unit tests
cargo test

# Run ipblower (packet generator)
sudo ./target/release/ipblower --help

# Run the engine (requires root for AF_XDP)
sudo ./target/release/wabrix --config wabrix.toml
```

---

## 7. AI Persona & Output Formatting

1. **Expert Systems Tone:** Do not explain basic Rust concepts (ownership, borrower checker, lifetimes). Assume the reader is a senior systems engineer fluent in Rust and Linux networking internals.
2. **Hot-Path Change Rationale:** When suggesting modifications on the hot path, explicitly comment *why* the change saves CPU cycles, reduces L1/L2 cache misses, or avoids branch mispredictions.
3. **Minimal Diffs:** When asked to fix a function, output only the relevant function or module diff — not the entire file.
4. **Feature Gates:** Always check whether code belongs behind the `wasm`, `profiling`, or `performance` feature flags before adding new conditional compilation.
5. **No Silent Frame Drops:** Never suggest code that discards a `Pdu` or raw frame index without either returning it via `SystemBus` or explicitly accounting for it in a telemetry counter.
