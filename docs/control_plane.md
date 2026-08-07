# WABRIX Control Plane Architecture

The WABRIX control plane manages the lifecycle, configuration, and observability of the high-performance AF_XDP data plane. It operates strictly outside the packet-processing hot path to ensure zero interference with the deterministic, lock-free worker threads.

## Architecture and Runtime

* **Binary Entry Point:** The control plane is hosted within the main `wabrix` binary. It exposes a CLI and a management API over REST and WebSockets using the `axum` framework.
* **Asynchronous Execution:** Unlike the data plane, which relies on thread-pinned synchronous polling loops, the control plane is driven by the `tokio` asynchronous runtime.
* **Error Handling:** The control plane utilizes standard Rust ergonomics, including heap allocations and `anyhow` / `thiserror` for cold-path error handling.

## Configuration and State Reconciliation

* **Declarative Configuration:** System state is defined via TOML configuration files, parsed by the `wabrix_lib::toml_parser::WabricksConfig`.
* **Hot-Reloading:** The control plane utilizes `arc-swap` to perform atomic pointer swaps for configuration updates. This allows the system to transition to new states with zero downtime.
* **Structural vs. Dynamic Updates:**
* Changes to physical constraints (e.g., UMEM sizing, worker core affinity, interface bindings) trigger a graceful teardown and full system restart.
* Changes to dynamic plugin arguments (`hot_args`, `hot_wasm_data`) or operational states (Online, Offline, Bypass) trigger live updates dispatched directly to the running nodes.



## Inter-Thread Communication

* **MPSC Channels:** The control plane communicates with the data plane workers using `crossbeam-channel`.
* **Lock-Free Injection:** The control plane pushes `ControlEvent` structures (containing an address pattern, opcode, and byte payload) into the worker's message queue. The worker thread polls this queue during its natural execution cycle, ensuring no mutexes or locks are acquired on the hot path.

## WebAssembly Plugin Interface

The control plane interacts with the Wasm-based processing nodes via specific functions defined in the `wabrix.wit` host-guest ABI:

* **Control Events:** The `on-control-event(opcode, payload) -> bool` function is invoked by the worker thread when a `ControlEvent` arrives from the control plane.
* Opcodes `0x0000` to `0x0FFF` are reserved for `StandardOpcode` values (e.g., SetOperationalState, FlushState).
* Opcodes from `0x1000` upwards are reserved for plugin-specific custom commands.


* **Initialization:** The `init-data(data)` function is called exactly once when the plugin is instantiated, passing key-value configuration data originating from the TOML file's `wasm_data` section.

## Telemetry and Observability

* **Counter Extraction:** The control plane periodically queries telemetry from the data plane. It utilizes `get-telemetry() -> list<u64>` to extract out-of-band counters from the Wasm plugins.
* **In-Band Telemetry:** For high-frequency routing actions, the control plane can track metrics via the upper 16 bits of the routing actions returned by the Wasm hot-path callbacks.
* **Broadcast:** Metrics are aggregated into `TMLiveStats` and broadcasted via Tokio channels to the API and standard output, facilitating integration with external time-series databases like Prometheus.

# Read-Copy-Update (RCU) vs. Control Events

Both RCU (`ArcSwap`) and Control Events serve distinct, complementary roles in a high-performance data plane. While both facilitate runtime reconfiguration, their memory semantics, payload capabilities, and execution paths differ fundamentally. Retaining both is necessary for a flexible and predictable architecture like Wabrix.

## RCU via `ArcSwap` (Declarative State)

RCU mechanisms define continuous state. They represent a "snapshot" of the world that the pipeline should adhere to continuously.

* **Reasoning:** `ArcSwap` is designed for structural pipeline configurations and simple scalar thresholds that must be evaluated constantly. Variables like `pps_limit`, `drop_probability`, and the overarching `GenState` are read on every batch iteration. The worker thread does not need to know *when* or *why* the state changed; it only needs to execute based on the current truth.
* **Performance Impact:** The overhead is extremely low. Loading the configuration via `ArcSwap` requires a single atomic read with `Acquire` memory ordering per batch, rather than per packet. Once the pointer is loaded, accessing the inner `HotPipelineConfig` fields translates to standard memory reads. This pattern avoids cache invalidation unless the control plane explicitly writes a new configuration, ensuring deterministic latency on the hot path.

## Control Events (Imperative Actions)

Control Events define discrete actions. They represent specific commands that a node or WebAssembly plugin must execute at a specific moment.

* **Reasoning:** Event queues are required for complex, stateful mutations that target a specific component. For example, injecting a new route into an active BTree, flushing a TCP connection pool, or updating cryptographic keys via `on-control-event(opcode, payload)`. These operations require structured payloads and execution guarantees that simple atomic swaps cannot provide without reallocating and swapping massive data structures.
* **Performance Impact:** Polling a lock-free queue (such as a channel) carries a higher baseline cost than an atomic pointer read. It involves checking queue indices, executing memory barriers, and potential cache-line bouncing if the control plane is actively writing to the queue. If polled on every packet, this introduces jitter. Polling the queue once per batch limits the overhead, but processing complex payloads still consumes CPU cycles that would otherwise be spent on packet forwarding.

---

### Architectural Comparison

| Feature | RCU / `ArcSwap` | Control Events |
| --- | --- | --- |
| **Paradigm** | Declarative State ("What is") | Imperative Actions ("What to do") |
| **Payload Capacity** | Fixed structs, simple scalars | Variable payloads, standard opcodes |
| **Delivery Target** | Broadcast (Pipeline-wide) | Targeted (Specific Node / Plugin) |
| **Worker Execution** | Passive reads during execution | Active processing of opcodes |
| **Fast-Path Cost** | Negligible (Single atomic load per batch) | Moderate (Queue polling and payload parsing) |

Relying solely on Control Events forces worker threads to manually cache and maintain global state, duplicating memory and increasing branch complexity. Conversely, relying solely on RCU makes complex state transitions impossible without violating zero-copy principles, as the control plane would need to clone and swap the entire routing table or state machine for a single entry update. Using both constructs ensures the hot path remains optimized for predictable execution while retaining dynamic extensibility.