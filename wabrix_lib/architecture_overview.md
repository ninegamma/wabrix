Here is a concise architectural overview of the XBricks system, reflecting the highly-optimized, lock-free design we have established.

---

# XBricks Architecture Overview

XBricks is a high-performance, run-to-completion (RTC) userspace networking engine built on `AF_XDP` and WebAssembly. It enforces a strict separation between **Build-Time Blueprints** (Configuration), the **Control Plane** (API/Routing), and the **Data Plane** (Lock-free packet processing).

---

## 1. The Configuration Layer (The Blueprint)

The configuration layer acts as the static definition of the system state, bridging the TOML files, the React GUI, and the initialization engine.

* **`WabricksConfig`:** The root struct representing the user's intended topology (`[umem]`, `[xsk]`, `[workers]`, `[[pipelines]]`). It holds parameters as loosely-typed strings (`HashMap<String, String>`).
* **Validation Phase:** Ensures mathematical consistency before touching hardware (e.g., verifying UMEM sizes against FQ/CQ/RX/TX rings, validating queue-to-worker assignments, and resolving UMEM partition requests).
* **Translation:** Transforms the `WabricksConfig` into a `SystemBuilder` configuration, mapping logical interfaces and queues to specific CPU cores and worker IDs.

## 2. Initialization Phase (`SystemBuilder`)

The `SystemBuilder` interacts with the Linux kernel and `xsk-core` to allocate physical memory and bind hardware queues.

* **UMEM Management:** Allocates memory regions and logically slices them into `UmemPartition`s. If Wasm plugins are used, it reserves a chunk of frames at the start of the UMEM specifically for Wasm memory sharing.
* **`XSocketRegistry`:** Creates and registers AF_XDP sockets. It immediately splits them into independent `XSocketRx` and `XSocketTx` halves so they can be securely transferred to different nodes in a pipeline.
* **`FrameRegistry`:** Gives each worker thread a localized, lock-free allocator for its assigned UMEM partitions.
* **`SystemBus`:** A set of crossbeam channels allowing worker threads to safely return `AF_XDP` frames to their original UMEM partitions when packets cross thread boundaries.

## 3. Pipeline Construction (`PipelineFactory`)

Translates the static blueprints into the executable runtime structures.

* **Node Instantiation:** Iterates over the `NodeConfig` definitions and creates the actual executable logic (`Box<dyn Node>`), such as `BatchRxNode`, `WasmEngine`, or `PacketGenerator`.
* **Rich Identity Generation (`NodeInstanceId`):** Assigns a globally unique, structural identity to every node: `{worker_id}/{pipeline_name}/{node_name}/{node_type}`.
* **DAG Resolution:** Translates string-based routes into fast, integer-based indices for zero-cost routing during the packet loop.

## 4. The Data Plane (`PipelineRunner`)

This is the ultra-fast, lock-free engine pinned to a specific CPU core.

* **Run-To-Completion Loop:** The runner pulls a `Batch` of packets from source nodes and pushes the batch through the DAG. Nodes mutate the batch in place (e.g., advancing offsets or dropping packets).
* **"Runner as Router" Pattern:** The Runner owns the Tokio `broadcast::Receiver` for control events. Individual nodes do *not* poll channels.
* **Execution Isolation:** The `Box<dyn Node>` array contains only the fast-path logic. All strings, names, and blueprints are stripped out of the core packet-processing path.

## 5. The Control Plane (Hot Updates & Routing)

The nervous system that allows the Axum GUI to modify the data plane at 40M+ pps without acquiring locks.

* **`SharedSystemState` (RCU):** The global state held by the Axum API. It contains `Arc<ArcSwap<HotPipelineConfig>>` for every pipeline.
* **Hot Updates:** When a parameter changes (e.g., PPS limit), the API parses the weakly-typed string into a strongly-typed `NodeDynamicState`. It atomically swaps the RCU pointer. The `PipelineRunner` reads this pointer on the next batch boundary, applying the change with zero lock contention.
* **Event Broadcasting:** The API wraps `ControlEvent`s (like Wasm GUI payloads) in an `Arc` and pushes them to a global Tokio broadcast channel.
* **Wildcard Addressing:** The `PipelineRunner` evaluates the event's `address_pattern` against its array of `NodeInstanceId`s. It supports powerful wildcards (e.g., `w0/*/rx_node`) and explicitly invokes `node.handle_control_event()` only on matched nodes.

## 6. Telemetry & Observability

* **`TelemetryRegistry`:** A centralized, thread-safe registry created during the `SystemBuilder` phase.
* **Metrics Gathering:** WASM engines register atomic counters or localized metrics maps here.
* **Background Polling:** A dedicated Tokio background task periodically scrapes this registry and pushes formatted JSON statistics to the frontend via WebSockets, ensuring the fast path never wastes cycles formatting strings.

---

### Hardware Load-Sharing Flow (Summary)

1. **Inbound Traffic:** Filtered/Distributed by NIC hardware via **Symmetric RSS** or **Flow Director (FDIR)** to specific hardware queues.
2. **AF_XDP BPF Map:** Maps the hardware queue to an XDP socket file descriptor.
3. **`BatchRxNode`:** Wakes up on its pinned core, pulls from the socket, and pushes the batch into the `PipelineRunner` DAG.
