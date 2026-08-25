# Wabrix

**The programmable, zero-copy data plane for custom network security and line-rate protocol parsing.**

Wabrix is an ultra-high-performance network data plane that combines **Linux AF_XDP** (kernel bypass) with **WebAssembly (Wasm)** sandboxing. It allows you to build, deploy, and execute custom network security logic at 25 Gbps+ line rates with zero CPU-copy overhead.

Designed for environments where standard firewalls fail—such as proprietary IoT networks, ultra-low latency financial gateways, and IT/OT converged infrastructure—Wabrix provides the deterministic speed of hardware appliances with the agility of dynamic software plugins.

---

## 🚀 Performance Metrics
* **Throughput:** Capable of 25 Gbps+ on standard COTS hardware.
* **Stateful Inspection:** High-speed, wire-speed tracking of millions of concurrent connection states (L4/L7) with a minimal, deterministic memory footprint.
* **Efficiency:** Demonstrated setup and release of more than **1 million TCP connections/second per performance CPU core** (Intel© Core™ i7-14700K).
* **Latency:** Deterministic, microsecond-level processing for time-sensitive industrial control loops and high-frequency trading (HFT).

## 🏗️ Architecture
Wabrix works by bypassing the standard Linux networking stack using a modern, kernel-native approach:

1. **AF_XDP (Kernel Bypass):** Packets are moved directly from the Network Interface Card (NIC) memory (UMEM) to userspace.
    * **Wabrix vs. DPDK:** Wabrix vs. DPDK: Both frameworks utilize dedicated core polling for maximum throughput. However, DPDK requires proprietary Poll Mode Drivers (PMDs) that detach the network interface from the operating system, forcing the core into its highest power state permanently. Wabrix utilizes the Linux kernel's AF_XDP (eBPF) framework, achieving comparable line-rate performance using standard Linux drivers. This preserves standard OS networking management, reduces deployment complexity, and allows the kernel to retain control over power state transitions and hardware management.

2. **Zero-Copy Wasm:** The UMEM is mapped directly into the linear memory space of a WebAssembly runtime. Wasm plugins process the raw packet data in-place without `memcpy` operations.

3. **Programmable Pipeline:** Network security logic is defined via TOML-based DAGs (Directed Acyclic Graphs), allowing for hot-reloading of security modules without system reboots.

## 📦 Getting Started

### Prerequisites
* **OS:** Linux (Kernel 6.1+ recommended for optimal AF_XDP zero-copy support and performance).
* **Language:** Rust (Latest stable).
* **Dependencies:** `libbpf`, `wasmtime` (recommended).

### Quick Build
```bash
git clone https://github.com/ninegamma/wabrix
cd wabrix
cargo build -p wabrix --release --features wasm
```

### Runtime Diagnostics

The generator’s optional `runtime-diagnostics` feature records timer scheduling
data for rate-control investigations, including timer calls, credited cycles,
available-frame histograms, cap hits, timestamps, and the final accumulator.
The host-side feature also adds final timer counter summaries with pipeline and
worker identity. These diagnostics are disabled by default because they add
state and work to the timer path.

Enable host diagnostics when building Wabrix:

```bash
cargo build -p wabrix --release --features "wasm,runtime-diagnostics"
```

Build the generator plugin with its extended completion log separately:

```bash
cargo build -p generator_engine --release \
  --target wasm32-wasip2 --features runtime-diagnostics
```

*(Detailed installation instructions for XDP drivers and NIC-specific tuning are located in `/docs`.)*

## 🏢 The Ecosystem & Enterprise

Wabrix follows an **Open-Core** model:

* **Wabrix Core (Open Source):** The high-speed execution engine, TOML parser, and XDP socket management are freely available under the AGPLv3 license.

* **NineGamma Enterprise (In Development):** Wabrix is an evolving solo-project. My mid-term commercial roadmap focuses on advanced plugins for **Deep-Packet Inspection (especially for proprietary/custom protocols), specialized threat mitigation, and enterprise orchestration tools.**

**Design Partners Wanted**
Rather than building these enterprise features in a vacuum, I am currently looking for **1-2 exclusive Design Partners** to sponsor priority development and shape the immediate roadmap. If your organization has a specific high-performance use case (like filtering a proprietary IoT protocol or requiring microsecond latency for financial trading edges), reach out to discuss a collaboration.

## 💡 Primary Use Cases

* **Bring-Your-Own-Protocol (BYOP) Parsing:** Drop-in support for proprietary, in-house, or legacy industrial protocols (IoT/Automotive/Manufacturing). Write a 50-line Rust parser, compile it to Wasm, and filter your custom UDP/TCP traffic at 25 Gbps without waiting for vendor firmware updates.
* **Ultra-Low Latency Financial Gateways:** Traditional firewalls are too slow for high-frequency trading (HFT) environments. Wabrix parses FIX and proprietary trading protocols in microseconds using kernel-bypass, securing trading edges without introducing jitter.
* **Line-Rate Volumetric Mitigation:** Dynamically deploy Wasm-based heuristics to silently drop 20+ Gbps of malicious DDoS traffic on standard COTS servers before it overwhelms sensitive control systems.

## 🤝 Contributing
Wabrix is currently in **Alpha**. I welcome contributions to the core engine, documentation, and performance tuning efforts. Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on our open-core philosophy and pull request process.

## ⚖️ Intellectual Property & Licensing
The Wabrix Core is licensed under **AGPLv3**.

The core architecture of Wabrix, specifically the zero-copy DMA mapping from AF_XDP (UMEM) directly into the WebAssembly linear memory space, is subject to a pending patent application.

In the spirit of open innovation, we grant a free patent license to any user strictly adhering to the terms of the AGPLv3 open-source license. For closed-source, commercial, or cloud-hosted integrations, a commercial Enterprise License is required.

*For Enterprise inquiries, Design Partnerships, or alternative licensing, [contact NineGamma](mailto:contact@ninegamma.com).*

"Wabrix" is a pending trademark of NineGamma. 

© 2026 NineGamma. All rights reserved.
