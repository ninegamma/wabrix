# Data Model and Architecture Summary

This document summarizes the data model, architectural roles, and frame lifecycle implemented in `frame_manager.rs` for the high-performance AF_XDP application.

## 1. High-Level Data Model: Multi Umem Static Partitioning Per Worker

The system implements a **Multi Umem Static Partitioning** model over multiple **Shared Global UMEM**. This decouples the physical division of memory from the logical assignment to workers, enabling complex pipelines where workers bridge multiple memory domains (NUMA: Non-uniform memory architecture).

* **Global UMEM:** A single, contiguous region of memory shared by multiple sockets (interfaces/queues) and worker threads.
    * It is logically divided into thousands of fixed-size frames (e.g., 2048 bytes each).
    * The OS sees one large memory map; the application sees discrete `FrameDesc` items.

* **Physical Slicing (The UMEM Layer):** 
    * Each Global UMEM is strictly divided into N fixed-size partitions at initialization.
    * Example: UMEM 0 (16k frames) is split into 4 partitions of 4k frames each.
    * This creates a pool of available frame "slices" .

* **Static Ownership:** Although the memory is technically shared, each **worker thread** can be assigned multiple immutable "slices" (Partitions) of frames that it owns.
    * **Multi-Domain Ownership:** A single worker can own partitions in UMEM 0 and in UMEM 1.
    * Example: Worker 0 claims "1 slice of UMEM 0" and "1 slice of UMEM 1".
    * This prevents lock contention during allocation, as Worker 0 never competes with Worker 1 for a free frame.
    * **Key Design Principle:** Partitioning is per **worker**, not per core. Multiple workers can run on the same physical core but each owns distinct frame ranges.
    * Note: Although workers own frames of their assigned partitions, frames can still sent between different workers.

* **Borrowing & Returning (Work Stealing):**
    * A worker can process *any* frame it receives from the NIC, even if it doesn't own it.
    * **Rule:** If a worker finishes with a frame it *does not own* (e.g., after a TX completion or a drop), it must "return" it to the original owner via the `SystemBus`.

## 2. Configuration Data Model

To enforce consistency across the shared memory region while allowing hardware flexibility, the configuration is split into two distinct structures.

### A. `UmemGroupConfig` (The Physical Layer)
Defines the physical properties of the memory region.
* **Fields:** `frame_size`, `headroom`, `frame_count`, `group_id`.
* **Constraint:** Every socket/manager in the same UMEM group **must** share an identical instance of this configuration. This prevents memory corruption (e.g., one socket thinking frames are 2KB while another thinks they are 4KB).

### B. `XdpSocketConfig` (The Hardware Layer)
Defines the specific connection to the network hardware.
* **Fields:** `interface` (e.g., "eth0"), `queue_id` (e.g., 0), `core_id`, `worker_id`.
* **Relationship:** Contains the `UmemGroupConfig` inside it, linking the hardware queue to the memory region it serves.
* **Note:** `core_id` specifies CPU affinity, while `worker_id` identifies the logical worker thread that owns the frame partition.

## 3. Role: `XSocket` (The Operator)

The `XSocket` acts as the **Hardware Interface**. It sits between the Linux Kernel's ring buffers and the application's logic.

* **Primary Responsibility:** Orchestrating the movement of Frame Indices between userspace memory and the kernel rings.
* **Key Components:**
    * **The Socket:** Wrapper around the file descriptor (`Fd`).
    * **The Rings:** Handles to the `RxQueue` & `TxQueue` (Socket-bound) and `FillQueue` & `CompQueue` (UMEM-bound).
    * **The Translator:** Converts abstract "Frame Indices" (integers like `500`) into physical memory addresses required by the UMEM rings (e.g., `500 * 2048 + 128`).
* **Critical Actions:**
    * **`replenish_fill_queue`:** Pushes available frames from the Registry into the Fill Ring so the NIC has a place to write incoming packets.
    * **`drain_comp_queue`:** Pulls completed TX frames from the Completion Ring and returns them to the Registry (or sends them back to their owner).
    * **`stats`:** Queries the kernel for packet drops and error counters.

## 4. Role: `FrameRegistry` (The Accountant)

The `FrameRegistry` acts as the **Inventory Manager**. It is a container that manages the owned frames across multiple Umem groups. It tracks which frames are currently free and available.

* **Primary Responsibility:** High-speed, lock-free management of free frames within a worker's partitions.
* **Ownership:** Each worker thread has exactly one `FrameRegistry`. Multiple sockets in the same worker access this shared registry via thread-local storage.
* **Key Components per Umem Group:**
    * **`free_stash`:** A `Vec<u32>` acting as a LIFO stack. This is the "hot cache" of frame indices ready to be used immediately.
    * **`partition`:** Defines the range of IDs this registry is allowed to own (e.g., "Worker 0 owns frames 0-4095").
    * **`return_queue`:** A lockless `Arc<ArrayQueue<u32>>` (bounded ring buffer) where *other workers* push bit-packed u32 commands for frames they are returning or sharing. The owning worker pops from this queue in `process_returns()`.
* **Logic:**
    * When a socket needs frames (`alloc`), the Registry pops from `free_stash`.
    * If `free_stash` is empty, it checks the `return_rx` mailbox to reclaim returned frames.
* **Thread Safety:** The registry itself is NOT wrapped in a Mutex. Instead, it's stored in thread-local storage, ensuring only one thread (the worker) ever accesses it.

## 5. The Frame Lifecycle Loop

1.  **Initialization:** Each worker's `LocalFrameRegistry` starts with all its owned frames (e.g., Worker 0 owns 0-4095) in the `free_stash`.
2.  **Fill (Replenish):** `XSocketRx` accesses the worker's registry via thread-local storage, pops frames from `free_stash`, and writes their descriptors to the **Fill Ring**.
3.  **Receive:** The NIC receives a packet, writes data to the frame in UMEM, and the driver moves the descriptor to the **RX Ring**.
4.  **Process:** The worker's application code polls the **RX Ring**, gets the descriptor, and processes the packet data.
5.  **Disposal:**
    * **Path A (TX):** The frame is put onto the **TX Ring**. Once sent, the kernel moves it to the **Completion Ring**. `XSocketTx` drains this ring and sends frames back via `SystemBus`.
    * **Path B (Drop):** The worker checks the frame's ID against its partition.
        * *If Owned by This Worker:* Returns to this worker's registry via `SystemBus` (for consistency).
        * *If Owned by Different Worker:* Sent via `SystemBus` to the owning worker's `return_rx`.

## 6. The Three Layers of Addressing

1. **The Logical Layer (Your Application)**

        Unit: Frame Index (u32)

        Values: 0, 1, 2, ... N

        Purpose: Resource tracking. You use this to know which buffers are free vs. busy. This is what lives in your LocalFrameRegistry.

2. **The Transport Layer (The Rings)**

        Unit: UMEM Offset / Descriptor Address (u64)

        Formula: (Index * Frame_Size) + Headroom

        Purpose: Communication with the Kernel. You write this value into the Fill Ring to tell the NIC "put data here". The Kernel writes this value into the RX Ring to tell you "data arrived here".

        Note: This is strictly an offset relative to the start of the UMEM, not a full pointer.

3. **The Physical Layer (Data Access)**

        Unit: Virtual Pointer (*mut u8)

        Formula: Umem_Base_Address + UMEM_Offset

        Purpose: Reading/Writing packet bytes. You use this to cast EthHdr or Ipv4Hdr over the raw memory.



# AF_XDP Architecture: Entity Relationships

This chapter outlines the architectural relationships between Physical Hardware (Interfaces, Cores), Kernel Structures (Queues, Sockets), and User Space Memory Management (Umem, Partitions).

## 1. Hierarchy Overview

The architecture is best understood as a hierarchy of constraints:

1.  **Umem Group (Top Level):** Defines the shared memory region.
2.  **Network Interfaces:** Physical devices attached to the host.
3.  **Hardware Queues:** Parallel channels within an Interface.
4.  **XDP Sockets (xsk):** The 1:1 link between a Queue and User Space.
5.  **Cores:** The processing units driving the Sockets.
6.  **Partitions:** The logical slicing of Umem to ensure thread safety.

---

## 2. Entity Definitions

### A. The Umem (Unified Memory)
* **Role:** The massive, contiguous block of RAM where all packet data lives.
* **Scope:** Shared across multiple sockets, interfaces, and cores.
* **Physicality:** It is a single `mmap`'d region from the OS perspective.
* **Addressing:** Drivers see physical addresses; Userspace sees offsets.
* **Key Constraint:** All sockets in a "Umem Group" must agree on `Frame Size` and `Headroom`.

### B. The Interface (NIC)
* **Role:** The physical hardware (e.g., `eth0`, `eth1`).
* **Queues:** Modern NICs support **RSS (Receive Side Scaling)**, splitting traffic into multiple Hardware Queues (e.g., Queue 0, Queue 1, Queue 2).
* **Isolation:** In "Program Relocation" mode, each Interface runs its own isolated BPF program instance.

### C. The XDP Socket (xsk)
* **Relation:** **1 Socket ↔ 1 Hardware Queue**.
* **Role:** The "Doorbell" to the kernel.
    * **RX Ring:** Kernel tells User: "Packet arrived at Address X".
    * **TX Ring:** User tells Kernel: "Send packet at Address Y".
    * **Fill Ring:** User gives Kernel: "Here are empty frames (Addresses) you can use".
    * **Comp Ring:** Kernel gives User: "I finished sending these frames".
* **Sharing Rule:** * Sockets on the **same** Queue (rare) share Fill/Comp rings.
    * Sockets on **different** Queues (standard) have **separate** Fill/Comp rings.

### D. The Core (CPU)
* **Relation:** **1 Worker Thread ↔ 1 Core** (Ideal Performance, via CPU affinity).
* **Role:** Physical CPU core where a worker thread executes.
* **Affinity:** Strictly pinning one worker thread to one core prevents context switching overhead.
* **Note:** The relationship is **Worker → Core**, not **Socket → Core**. Multiple sockets can run on the same worker thread.

### E. The Umem Partition (Logical)
* **Relation:** **1 Partition ↔ 1 Worker Thread**.
* **Role:** A "Slice" of the Umem Group assigned to a specific worker thread.
* **Example:** * Umem has 10,000 frames.
    * Worker 0 owns Partition [0 - 4,999].
    * Worker 1 owns Partition [5,000 - 9,999].
* **Purpose:** **Lock-Free Allocation.** Worker 0 can allocate from its partition without locking against Worker 1.
* **Key Point:** Partitioning is per **worker thread**, not per physical core. Two workers pinned to different cores will have separate partitions, even if running the same code.

---

## 3. The Relationship Matrix

| Component A | Component B | Relationship | Description                                                                                               |
| :--- | :--- |:-------------|:----------------------------------------------------------------------------------------------------------|
| **Interface** | **Queues** | 1 : N        | One NIC has multiple queues (RSS).                                                                        |
| **Queue** | **XDP Socket** | 1 : 1        | A socket binds exclusively to one specific queue ID on one interface.                                     |
| **Worker Thread** | **XDP Sockets** | 1 : N        | One worker thread can manage multiple sockets for efficient resource utilization.                         |
| **Worker Thread** | **Core** | 1 : 1        | Each worker is pinned to one physical CPU core via affinity.                                              |
| **Worker Thread** | **Partition** | 1 : M        | Each worker may own partitions from multiple Umem groups for lock-free allocation.                        |
| **Umem Group** | **Interfaces** | 1 : N        | One memory region can serve `eth0` and `eth1` simultaneously, allowing Zero-Copy forwarding between them. |
| **Umem Group** | **Partitions** | 1 : N        | The global memory is sliced into N parts.                                                                 |
| **Partition** | **Frames** | 1 : M        | A partition strictly owns a specific range of Frame Indices.                                              |

---

## 4. Visualizing the Data Flow

### Scenario: Zero-Copy Forwarding (Router)

**Setup:**
* **Umem Group:** Shared Memory (Indices 0–8191).
* **Worker 0:** Owns Partition A (Indices 0–4095). Runs on Core 0. Manages `eth0` / Queue 0.
* **Worker 1:** Owns Partition B (Indices 4096–8191). Runs on Core 1. Manages `eth1` / Queue 0.

**The Flow:**
1.  **Alloc:** Worker 0's registry (accessed via thread-local storage) pops `Frame #100` from its stash (Partition A).
2.  **Fill:** Worker 0's `XSocketRx` puts `Frame #100` into `eth0` Fill Ring.
3.  **RX:** Packet arrives on `eth0`. Driver puts data in `Frame #100`.
4.  **Process:** Worker 0 reads `Frame #100`. Decides to forward to `eth1`.
5.  **Forward:** Worker 0 passes the descriptor `100` to Worker 1 (via channel/bus or directly if same worker).
6.  **TX:** Worker 1's `XSocketTx` puts `Frame #100` onto `eth1` TX Ring. **(Zero Copy: Data never moved)**.
7.  **Completion:** `eth1` finishes sending. `Frame #100` appears in `eth1` Completion Ring.
8.  **Return:** Worker 1's `XSocketTx` drains Completion Ring. Sees `Frame #100`.
    * *Check:* "Does Worker 1 own #100?" -> No (Worker 1's Partition B starts at 4096).
    * *Action:* Worker 1 sends `Frame #100` back to Worker 0 via SystemBus.

---

## 5. Summary of Your Rust Structs

* **`UmemGroupConfig`**: Defines the singular "Warehouse" (Umem).
* **`XSocket`**: Represents a socket bound to a hardware queue. It is split into RX and TX halves for independent pipeline access.
* **`XSocketRx`**: RX-only component of a split socket. Accesses the worker's frame registry via thread-local storage.
* **`XSocketTx`**: TX-only component of a split socket. Returns all frames via SystemBus (no direct registry access).
* **`WorkerFrameRegistry`**: Contains the worker's `LocalFrameRegistry` along with partition info and worker ID.
* **`LocalFrameRegistry`**: Represents the **Worker's Partition**. It holds the inventory (`free_stash`) for that specific worker. Accessed via thread-local storage within the worker thread.
* **`SystemBus`**: The mechanism to satisfy the **Ownership Rule** (returning frames between workers). Holds one `Arc<ArrayQueue<u32>>` per worker. Commands are bit-packed into a single u32: bit 31 = command (0=Release, 1=Retain), bits 30-24 = umem_id (7 bits, max 128 UMEMs), bits 23-0 = local_idx.
* **`UmemPartition`**: Contains `worker_id`, `umem_group_id`, `start_idx`, and `end_idx` to define frame ownership boundaries.

---

## 6. Split Socket Architecture: Independent RX and TX Binding

### Problem Statement
An AF_XDP socket has both RX and TX rings. In a pipeline architecture, we need the flexibility to:
* Bind different nodes to the same socket's RX and TX rings independently
* Support bidirectional packet forwarding without queue conflicts
* Avoid mutex contention on the critical data path

**Example Use Case:** Bidirectional bridge between two interfaces
```
Pipeline 1: Interface A (RX) → Interface B (TX)
Pipeline 2: Interface B (RX) → Interface A (TX)
```
Both pipelines need access to the same physical queues but for different operations (RX vs TX).

### Solution: Socket Splitting

When an `XSocket` is added to the `XSocketRegistry`, it is immediately split into two independent components:

#### A. XSocketRx (The Receiver)
**Owns:**
* RX queue (exclusive access)
* Fill queue (shared via Arc<Mutex>, but only RX uses it)
* UMEM reference (for data access)
* Partition info (to identify owned frames)

**Accesses via Thread-Local Storage:**
* Worker's `LocalFrameRegistry` (for frame allocation and return processing)

**Responsibilities:**
* Poll RX ring for incoming packets
* Create PDUs from received frames
* Process frame returns from TX via SystemBus (by calling registry's `process_returns()`)
* Replenish fill queue with available frames from the worker's registry

**Key Method:** `receive_into_batch()`
```rust
fn receive_into_batch(&mut self, batch: &mut Batch) -> Result<usize> {
    // 1. Access worker's registry via thread-local storage
    let registry_ptr = get_worker_frame_registry();
    let registry = unsafe { &mut *registry_ptr };

    // 2. Process returns from TX (via SystemBus)
    registry.process_returns();

    // 3. Poll RX ring
    let rcvd = self.rx_queue.poll_and_consume(&mut descs, 0)?;

    // 4. Create PDUs from frames
    for desc in &descs[..rcvd] {
        let pdu = Pdu::new(desc, global_idx, ...);
        batch.push(pdu);
    }
    Ok(rcvd)
}
```

#### B. XSocketTx (The Transmitter)
**Owns:**
* TX queue (exclusive access)
* Completion queue (shared via Arc<Mutex>, but only TX uses it)
* UMEM reference (for data access)
* Partition info (to determine frame ownership)
* SystemBus reference (for returning frames)

**Does NOT Own:**
* Frame registry (no direct allocation capability)

**Responsibilities:**
* Transmit PDUs from batch to TX ring
* Drain completion queue for finished frames
* **Always return frames via SystemBus** (even locally-owned frames, for consistency)

**Critical Design Decision:**
```rust
fn release_frame(&mut self, frame_idx: u32) {
    // TX NEVER accesses registry directly
    // Always route through SystemBus to ensure the owning worker receives them.
    // Release command: bit 31 = 0 (Release), lower 31 bits = global frame index.
    let owner_worker_id = self.determine_owner(frame_idx);
    let _ = self.bus.bus_senders[owner_worker_id].push(frame_idx);
}
```

**Note:** This design ensures XSocketTx doesn't need thread-local registry access, simplifying the model.

### Frame Flow in Split Socket Model

```
┌─────────────────────────────────────────────────────────────┐
│                    Worker Thread                             │
│                                                              │
│  ┌──────────────┐              ┌──────────────┐            │
│  │  XSocketRx   │              │  XSocketTx   │            │
│  │              │              │              │            │
│  │ ┌──────────┐ │              │ ┌──────────┐ │            │
│  │ │ RX Ring  │ │              │ │ TX Ring  │ │            │
│  │ └────┬─────┘ │              │ └────▲─────┘ │            │
│  │      │ poll  │              │      │send   │            │
│  │      ▼       │              │      │       │            │
│  │ ┌──────────┐ │   PDU Flow   │      │       │            │
│  │ │   PDU    │─┼──────────────┼──────┘       │            │
│  │ └──────────┘ │              │              │            │
│  │              │              │ ┌──────────┐ │            │
│  │              │              │ │ Comp Q   │ │            │
│  │              │              │ │  drain   │─┼───┐        │
│  │              │              │ └──────────┘ │   │        │
│  └──────────────┘              └──────────────┘   │        │
│         ▲                                          │        │
│         │ get_worker_frame_registry()              │        │
│         │ (thread-local access)                    │        │
│         │                                          │        │
│  ┌──────┴──────────────┐                          │        │
│  │ LocalFrameRegistry  │◄─────────────────────────┘        │
│  │  (via TLS)          │         SystemBus                 │
│  │                     │         (returns)                 │
│  │ • free_stash        │                                   │
│  │ • return_rx         │                                   │
│  │ • partition         │                                   │
│  └─────────────────────┘                                   │
│         ▲                                                   │
│         │ process_returns()                                │
│         │                                                   │
│  ┌──────┴────────────────────────────────────┐            │
│  │           SystemBus Channels               │            │
│  │    (Worker-to-Worker Frame Returns)        │            │
│  └────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

### Key Properties

1. **No Mutex on Hot Path:**
   - RX has exclusive access to RX ring and fill queue
   - TX has exclusive access to TX ring and completion queue
   - Worker's LocalFrameRegistry accessed via thread-local storage (lock-free)
   - RX accesses registry for allocation, TX never touches it directly

2. **Frame Circulation:**
   - Frames flow: RX allocates (from worker registry) → PDU created → TX transmits → TX completes
   - TX always pushes packed u32 commands to the owning worker's `ArrayQueue` (even same-worker)
   - RX processes returns: `while let Some(cmd) = return_queue.pop()` — no enum branching, no mutex
   - No risk of frame accumulation or starvation

3. **Independent Node Binding:**
   - BatchRxNode can take RX component
   - BatchTxNode can take TX component
   - Same socket serves both nodes independently
   - Enables bidirectional pipelines on same queues

4. **SystemBus Coordination:**
   - One `Arc<ArrayQueue<u32>>` per worker; both senders and the registry share the same Arc
   - Works transparently for same-worker and cross-worker scenarios
   - Packed u32 format: bit 31 = command, bits 30-24 = umem_id, bits 23-0 = local_idx
   - Routes based on worker_id, not core_id

### Registry Behavior

```rust
pub struct XSocketRegistry {
    // Each socket is split: (Option<RX>, Option<TX>)
    sockets: HashMap<(String, usize), (Option<XSocketRx>, Option<XSocketTx>)>,
}

impl XSocketManager for XSocketRegistry {
    fn take_rx_socket(&mut self, queue_id, interface)
        -> Option<Box<dyn BatchRx + Send>> {
        // Take RX component only, leave TX for another node
        self.sockets.get_mut(&key)
            .and_then(|(rx_opt, _tx_opt)| rx_opt.take())
    }

    fn take_tx_socket(&mut self, queue_id, interface)
        -> Option<Box<dyn BatchTx + Send>> {
        // Take TX component only, leave RX for another node
        self.sockets.get_mut(&key)
            .and_then(|(_rx_opt, tx_opt)| tx_opt.take())
    }
}
```

### Example: Bidirectional Bridge Configuration

```toml
# Both interfaces use same queue IDs
xsk = [
    { iface = "eth0", queues = [0, 1], umem = 0, ... },
    { iface = "eth1", queues = [0, 1], umem = 0, ... },
]

[[pipelines]]
name = "forward"
workers = [0, 1]
    [[pipelines.nodes]]
    id = "rx_eth0"
    type = "BatchRxNode"
    args = { interface = "eth0", queues = [0, 1] }  # Takes RX component

    [[pipelines.nodes]]
    id = "tx_eth1"
    type = "BatchTxNode"
    args = { interface = "eth1", queues = [0, 1] }  # Takes TX component

[[pipelines]]
name = "reverse"
workers = [0, 1]
    [[pipelines.nodes]]
    id = "rx_eth1"
    type = "BatchRxNode"
    args = { interface = "eth1", queues = [0, 1] }  # Takes RX component

    [[pipelines.nodes]]
    id = "tx_eth0"
    type = "BatchTxNode"
    args = { interface = "eth0", queues = [0, 1] }  # Takes TX component
```

Both pipelines can coexist because they access different rings of the same sockets.

# Understanding `group_start_frame_idx`: Global vs. Local Indexing

This chapter explains the logic behind `group_start_frame_idx` and how it creates a unified "System-Wide" address space for frames, even when the underlying hardware uses separate memory regions.

## 1. The Problem: Duplicate Indices (Address Collision)

In the raw hardware view, every separate UMEM (Unified Memory) region starts counting its frames at **0**.

* **Group 0 (eth0 Memory):** Contains physical frames `0, 1, 2 ... 4095`.
* **Group 1 (eth1 Memory):** Contains physical frames `0, 1, 2 ... 4095`.

This creates a serious ambiguity for the **SystemBus** (the mechanism that allows cores to exchange packets). If Core A sends a message to Core B saying *"Return Frame #50"*, Core B has no way of knowing if that refers to the "Frame #50" residing in `eth0`'s memory or `eth1`'s memory.

## 2. The Solution: The "Global Index" (Application View)

To resolve this, the application layer treats the entire system as one giant, continuous logical array of frames, abstracting away the physical boundaries.

The `group_start_frame_idx` is the **Base Offset** that shifts a specific group's local indices into this unique global range.

### Visual Example
* **Group 0 Base:** `0`
    * Local: `0` $\rightarrow$ Global: `0`
    * Local: `4095` $\rightarrow$ Global: `4095`
* **Group 1 Base:** `4096`
    * Local: `0` $\rightarrow$ Global: `4096`
    * Local: `4095` $\rightarrow$ Global: `8191`

## 3. The Translation Logic

The `XSocket` acts as the translator between these two worlds. It converts indices on-the-fly depending on who it is talking to.

### Direction A: Talking to the Kernel (TX / Fill Rings)
When communicating with the NIC driver or Kernel, you must use the **Local Index** relative to that specific physical UMEM.

**Formula:**
$$\text{Local Index} = \text{Global Index} - \text{group\_start\_frame\_idx}$$

* **Scenario:** You want to transmit using Global Frame #4100 (which lives in Group 1).
* **Math:** `4100 - 4096 = 4`.
* **Action:** You write Descriptor Index **4** into the TX Ring.

### Direction B: Talking to the Application (Registry / Bus)
When receiving completed frames from the Kernel, you convert them back to the **Global Index** so your Registry knows exactly which unique frame was returned.

**Formula:**
$$\text{Global Index} = \text{Local Index} + \text{group\_start\_frame\_idx}$$

* **Scenario:** The Kernel returns completed Descriptor **4** from Group 1.
* **Math:** `4 + 4096 = 4100`.
* **Action:** You tell the Registry *"Frame #4100 is now free"*.

## 4. Detailed Address Calculation: UMEM, XSK, and the Three Address Spaces

Understanding the complete addressing scheme requires distinguishing between **three different address spaces** that coexist in the system:

### 4.1 The Three Address Spaces

1. **Global Frame Index** (Application/System Bus)
   - Used by the application logic and SystemBus for frame lifecycle management
   - Unique across ALL UMEM groups in the entire system
   - Range: `0` to `(total_frames_in_all_groups - 1)`
   - Purpose: Prevents ambiguity when different workers exchange frames

2. **Local Frame Index** (UMEM-relative)
   - Used internally for address calculations within a specific UMEM
   - Resets to `0` for each UMEM group
   - Range: `0` to `(frames_in_this_group - 1)`
   - Purpose: Index into the specific UMEM's frame array

3. **Frame Descriptor Address** (XSK/Kernel)
   - The actual byte offset passed to AF_XDP rings (TX, RX, Fill, Completion)
   - Calculated from the local frame index
   - Formula: `address = (local_idx << frame_size_log2) + headroom`
   - Purpose: Hardware/kernel needs byte addresses, not logical indices

### 4.2 Complete Address Translation Flow

#### Example Setup:
- **UMEM Group 0**: 4096 frames, frame_size = 2048 bytes, headroom = 256, `group_start_frame_idx = 0`
- **UMEM Group 1**: 4096 frames, frame_size = 2048 bytes, headroom = 256, `group_start_frame_idx = 4096`

#### TX Path (Application → Kernel):

```rust
// 1. Application allocates frame using GLOBAL index
let global_idx = 4100;  // This frame is in Group 1

// 2. Convert to LOCAL index for UMEM address calculation
let local_idx = global_idx - group_start_frame_idx;
// local_idx = 4100 - 4096 = 4

// 3. Calculate frame descriptor address for XSK
let frame_size_log2 = 11;  // 2048 = 2^11
let headroom = 256;
let frame_addr = (local_idx << frame_size_log2) + headroom;
// frame_addr = (4 << 11) + 256 = 8192 + 256 = 8448

// 4. Write frame_addr to TX ring
tx_ring.push(FrameDesc::new(frame_addr));
```

#### RX Path (Kernel → Application):

```rust
// 1. Read frame descriptor from RX ring
let frame_desc = rx_ring.pop();
let frame_addr = frame_desc.addr();  // e.g., 8448

// 2. Convert descriptor address to LOCAL index
let local_idx = (frame_addr >> frame_size_log2);
// local_idx = 8448 >> 11 = 4 (headroom is ignored in this calculation)

// 3. Convert LOCAL index to GLOBAL index for application
let global_idx = local_idx + group_start_frame_idx;
// global_idx = 4 + 4096 = 4100

// 4. Create PDU with GLOBAL index for system bus
let pdu = Pdu::new(frame_desc, global_idx, worker_id, ...);
```

### 4.3 Key Implementation Points

#### In `system_builder.rs` (Frame Allocation):
```rust
// When allocating frames for PDUs, always use GLOBAL indices
let global_frame_idx = registry.allocate_frame(umem_id);

// For UMEM address calculation, convert to local:
let local_idx = global_frame_idx - group_start_frame_idx;
let frame_addr = (local_idx << frame_size_log2) + headroom;

// But store GLOBAL index in the PDU!
let pdu = Pdu::new(desc, global_frame_idx, worker_id, ...);
```

#### In `xsocket.rs` (TX/RX):
```rust
// TX: Convert global → local → address
let local_idx = global_idx - self.config.umem_config.group_start_frame_idx;
let frame_addr = (local_idx << frame_size_log2) + headroom;
tx_ring.push(FrameDesc::new(frame_addr));

// RX: Convert address → local → global
let local_idx = (desc.addr() >> frame_size_log2);
let global_idx = local_idx + self.config.umem_config.group_start_frame_idx;
```

### 4.4 Why Partitioning Does NOT Change `group_start_frame_idx`

**Important:** When a UMEM group is partitioned among multiple workers, the `group_start_frame_idx` remains constant for ALL workers sharing that UMEM group.

**Incorrect Approach (Bug):**
```rust
// ❌ WRONG: Setting group_start_frame_idx to partition.start_idx
updated_umem_config.group_start_frame_idx = partition_data.partition.start_idx;
```

This breaks address calculations because:
- Global frame `4100` in partition 1 (frames 4096-8191) would calculate:
  - `local_idx = 4100 - 4100 = 0` ❌ Points to wrong frame!
  - `frame_addr = 0 << 11 = 0` ❌ Points to first frame of UMEM, not frame 4!

**Correct Approach:**
```rust
// ✓ CORRECT: group_start_frame_idx stays at UMEM group level
// Partitioning only affects which frames the allocator provides
umem_config.group_start_frame_idx = 4096;  // Unchanged for all workers in Group 1

// Each worker's FrameRegistry knows its partition range:
registry.registry_map.get(group_id) = PartitionData {
    partition: FrameRange { start_idx: 4096, end_idx: 6144 },  // First half
    ...
};
```

The partition boundaries control frame **ownership** (which worker can allocate which frames), but do not affect **addressing** (how frames map to UMEM memory).

### 4.5 Summary Table

| Address Type | Scope | Used By | Range Example (Group 1) | Purpose |
|-------------|-------|---------|-------------------------|---------|
| **Global Index** | System-wide | Application, SystemBus, FrameRegistry | 4096 - 8191 | Unique frame identification |
| **Local Index** | UMEM-group | Internal calculations | 0 - 4095 | Index into UMEM frame array |
| **Descriptor Address** | XSK/Kernel | AF_XDP rings | 0 - 8,386,560 | Byte offset in UMEM region |

**Conversion Rules:**
- `local_idx = global_idx - group_start_frame_idx`
- `global_idx = local_idx + group_start_frame_idx`
- `descriptor_addr = (local_idx << frame_size_log2) + headroom`
- `local_idx = descriptor_addr >> frame_size_log2` (headroom is within frame bounds)