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
* **OS:** Linux. Kernel 6.1+ is recommended for AF_XDP support; kernel features and
  NIC drivers determine which XDP and zero-copy modes are available at runtime.
* **Rust:** The latest stable Rust toolchain, installed with `rustup`.
* **Native build tools:** A C compiler and linker, `clang`, LLVM, `pkg-config`,
  Git, and `m4`. The build compiles libbpf/libxdp and eBPF programs.
* **Development libraries:** libelf, zlib, and libpcap headers/libraries.

On Debian or Ubuntu, install the native build prerequisites with:

```bash
sudo apt update
sudo apt install build-essential clang llvm libelf-dev zlib1g-dev libpcap-dev pkg-config git m4
```

Install the `wasm32-wasip2` Rust target only when building Wasm guest programs
(for example, the `generator_engine` plugin):

```bash
rustup target add wasm32-wasip2
```

Loading XDP programs and configuring network interfaces at runtime requires
appropriate Linux capabilities; running as root is the simplest option for
development and integration testing.

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

## 📈 Monitoring with Prometheus and Grafana

Wabrix exposes its runtime telemetry as Prometheus metrics at
`http://localhost:9090/metrics`. The repository includes a Docker Compose
stack that collects these metrics with Prometheus and displays them in Grafana.

### Install and start the monitoring stack

Install Docker Engine with the Docker Compose plugin, then start the stack from
the monitoring directory:

```bash
cd monitoring
docker compose up -d
```

Start Wabrix separately so that its metrics endpoint is available on port
`9090`. Prometheus polls that endpoint every five seconds. The Compose
configuration maps the Prometheus web interface to port `9091` to avoid
conflicting with Wabrix's metrics server.

Open the following URLs in a browser:

* **Grafana:** <http://localhost:3000> (default login: `admin` / `admin`)
* **Prometheus:** <http://localhost:9091>
* **Wabrix metrics:** <http://localhost:9090/metrics>

Grafana is automatically configured with Prometheus as its data source and
loads the dashboards from `monitoring/grafana/dashboards/`. If Wabrix is not
running, Prometheus will show the target as down and Grafana will have no new
data to display.

To view the scrape status, open **Status → Targets** in Prometheus and check
the `wabrix_engine` target. Stop the stack with:

```bash
docker compose down
```

The Grafana data is stored in the named `grafana-data` volume, so dashboards
and Grafana settings persist when the containers are stopped.

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
