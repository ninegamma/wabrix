# Wabrix Architecture Overview

Wabrix is a run-to-completion (RTC) userspace networking engine built on AF_XDP, with optional WebAssembly plugins. Its configuration, control plane, and data plane have distinct responsibilities. For frame ownership, socket rings, and addressing details, see [Data Model and Architecture](architecture.md).

## 1. Configuration

* **`WabricksConfig`:** The root configuration contains a typed `wabrix` section, `pipelines`, and optional GUI `layout` data. The `[wabrix]` section defines `ifaces`, `umem`, `xsk`, and `workers`; pipelines use `[[pipelines]]`. Node argument and data maps use string values.
* **Validation:** Checks UMEM/ring resource requirements, partition divisibility, and queue/worker assignments before system construction.
* **Translation:** Expands the configuration into runtime worker, socket, and pipeline definitions. It creates each `NodeInstanceId` from its worker ID, pipeline name, node name, and node type.

## 2. Initialization (`SystemBuilder`)

* **UMEM groups and partitions:** Allocates shared packet-buffer memory as UMEM groups. A UMEM partition is a contiguous range of frames assigned to one worker. A worker can own partitions in multiple UMEM groups.
* **Wasm memory:** When enabled, Wasm linear memory includes an alias of UMEM and a private mailbox region. The mailbox does not reserve frames at the start of UMEM; all UMEM frames remain available to worker partitions.
* **`XSocketRegistry`:** Registers AF_XDP sockets and splits each into `XSocketRx` and `XSocketTx` halves for independent node binding.
* **`FrameRegistry`:** Provides a worker-local allocator for its owned partitions, accessed through thread-local storage without a registry mutex.
* **`SystemBus`:** Provides one bounded, lock-free `crossbeam_queue::ArrayQueue<FrameToken>` per worker for frame retain/release commands, plus a partition directory for ownership and address resolution. Each token contains a UMEM group ID and UMEM-local frame index; the queue-only retain command uses the high bit of the UMEM ID. It does not transport packet batches.

## 3. Pipeline Construction (`PipelineFactory`)

* **Node instantiation:** Builds executable nodes such as `BatchRxNode`, `WasmEngine`, and `PacketGenerator` from node configurations.
* **Identity:** Preserves the `NodeInstanceId` created during configuration expansion. Control addressing uses `worker/pipeline/node/type` patterns.
* **DAG resolution:** Resolves configured routes into integer indices for packet routing.

## 4. Data Plane (`PipelineRunner`)

* **Run-to-completion:** A worker runs its pipelines with configured CPU affinity. Source nodes produce packet batches, which the runner routes through the DAG. Nodes process or drop packets in those batches.
* **Frame lifecycle:** Free stashes and SystemBus carry `FrameToken` values (UMEM group ID plus UMEM-local frame index). PDUs carry the UMEM group ID and derive the frame index from the descriptor address. AF_XDP rings carry UMEM-relative byte addresses. Same-owner drops and TX completions normally return directly through the local registry; foreign-owner returns use SystemBus.
* **Synchronization:** The registry is worker-local and frame-return queues are lock-free, but socket ring handles use mutex wrappers. The entire packet path is not lock-free.
* **Control dispatch:** The runner polls its control queue at control/timer polling boundaries and dispatches events to matching nodes. Node identities and control metadata remain available at runtime.

## 5. Control Plane

* **`SharedSystemState`:** Holds per-pipeline `Arc<ArcSwap<HotPipelineConfig>>` state for atomic publication of hot configuration.
* **Hot updates:** Reconciliation publishes updated `hot_args` and pipeline lifecycle state through `ArcSwap`. Runners check configuration at their polling boundary. Structural/static configuration changes require a restart.
* **Control events:** Events are fanned out to separate bounded `crossbeam_channel` queues for each runner. The runner matches `address_pattern` against node identities and invokes `handle_control_event()` on matching nodes. Patterns such as `w0/*/rx_node` allow omitted trailing segments as wildcards.
* **Node data:** Hot node-data updates for `TcpNode` and `WasmEngine` are serialized into control events; they are distinct from the atomic hot-configuration publication.

## 6. Telemetry

* **`TelemetryRegistry`:** Populated during construction and shared through `Arc`. Metrics use atomic counters and flags.
* **Background polling:** A Tokio task reads metrics, requests node synchronization when needed, aggregates statistics, and publishes telemetry through a Tokio broadcast channel for frontend delivery. Telemetry broadcasting is separate from control-event queues.

## 7. Hardware Receive Flow

1. NIC queue selection depends on the hardware and configuration, including RSS or Flow Director where available.
2. The XDP program redirects selected packets through an XSK map to an AF_XDP socket.
3. A `BatchRxNode` polls its socket (with optional NAPI busy polling) on its worker and supplies batches to the pipeline DAG.
