# Wabrix

**Programmable, line-rate network security for critical infrastructure.**

Wabrix is an ultra-high-performance network data plane that combines **Linux AF_XDP** (kernel bypass) with **WebAssembly (Wasm)** sandboxing. It allows you to build, deploy, and execute network security logic at 25 Gbps+ line rates with zero CPU-copy overhead.

Designed for the convergence of IT and OT (Operational Technology) networks, Wabrix provides the speed of hardware appliances with the flexibility of dynamic software plugins.

---

## 🚀 Performance Metrics
* **Throughput:** Capable of 25 Gbps+ on standard COTS hardware.
* **Stateful Inspection:** High-speed, wire-speed tracking of millions of concurrent connection states (L4/L7) with a minimal, deterministic memory footprint.
* **Efficiency:** Demonstrated setup and release of more than **1 million TCP connections/second per CPU core**.
* **Latency:** Deterministic, microsecond-level processing for time-sensitive industrial control loops.

## 🛠 Architecture
Wabrix works by bypassing the standard Linux networking stack using a modern, kernel-native approach:

1. **AF_XDP (Kernel Bypass):** Packets are moved directly from the Network Interface Card (NIC) memory (UMEM) to userspace.
    * **Wabrix vs. DPDK:** Unlike DPDK, which requires proprietary drivers and consumes significant CPU resources through constant "busy-polling," Wabrix utilizes the Linux kernel's **AF_XDP (eBPF)** framework. This allows us to achieve comparable line-rate performance while using **standard Linux drivers** and significantly reducing power consumption and complexity.

2. **Zero-Copy Wasm:** The UMEM is mapped directly into the linear memory space of a WebAssembly runtime. Wasm plugins process the raw packet data in-place without `memcpy` operations.

3. **Programmable Pipeline:** Network security logic is defined via TOML-based DAGs (Directed Acyclic Graphs), allowing for hot-reloading of security modules without system reboots.

## 📦 Getting Started

### Prerequisites
* **OS:** Linux (Kernel 6.1+ recommended for optimal AF_XDP zero-copy support and performance)
* **Language:** Rust (Latest stable).
* **Dependencies:** `libbpf`, `wasmtime` (recommended).

### Quick Build
```bash
git clone https://github.com/ninegamma/wabrix
cd wabrix
cargo build -p wabrix --release --features wasm
```

*(Detailed installation instructions for XDP drivers and NIC-specific tuning are located in `/docs`.)*

## 🧩 The Ecosystem
Wabrix follows an **Open-Core** model:

* **Wabrix Core (Open Source):** The high-speed execution engine, TOML parser, and XDP socket management.
* **Enterprise Plugins (Commercial License):** Hyper-optimized Modbus/S7 deep-packet inspection, advanced DDoS mitigation, and the Graphical DAG Management UI are available under the Ninegamma Enterprise License.

## 📈 Use Cases
* **Modbus TCP Micro-Segmentation:** Granular control over industrial protocols to prevent unauthorized PLC write commands.
* **Line-Rate DDoS Mitigation:** Dropping malicious traffic at the edge of the network before it reaches sensitive control systems.
* **Custom Protocol Parsing:** Inject proprietary industrial protocol filters instantly via Rust-compiled Wasm modules.

## 🤝 Contributing
Wabrix is currently in **Alpha**. We welcome contributions to the core engine, documentation, and performance tuning efforts. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and pull request process.

## 📄 License
The Wabrix Core is licensed under AGPLv3. 

*Interested in Enterprise features? [Contact our engineering team](mailto:contact@ninegamma.com) for a benchmark demonstration.*

