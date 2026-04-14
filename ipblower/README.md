# ipblower 🌪️

**ipblower** is a blazingly fast, high-performance packet generator written in Rust. It leverages Linux's **AF_XDP** (eXpress Data Path) socket family to bypass the standard kernel network stack, allowing it to saturate at least 10GbE links using a single CPU core with 60 bytes packets (TCP SYN).

This has been tested on 
* Intel Corporation Ethernet 10G 2P X520 Adapter (rev 01), 
  * ixgbe, firmware-version: 0x80000827, 16.5.19
* Intel Corporation Ethernet Controller X710 for 10GbE SFP+ (rev 01)
  * driver: i40e, version: 6.8.0-90-generic, firmware-version: 6.80 0x80003cc1 1.2007.0
* a single performance core of Intel© Core™ i7-14700K × 20,
* Linux Mint 22.2 Cinnamon,
  * Kernel 6.8.0-90-generic

# Performance
* with Intel X520 and single core and queue we achieved 10.5 Mpps with the above system configuration.
* with Intel X710 and single core and queue we achieved 13.3 Mpps with the above system configuration.
* with Intel X520 and 4 workers/cores/queues we achieved up to 3.5 Mpps per queue (up to 14 Mpps total) on an old Intel(R) Xeon(R) CPU E5-2640 0 @ 2.50GHz

## 🧠 Background: Why AF_XDP?

Traditionally, to achieve wire-speed packet generation in userspace, developers relied on **DPDK** (Data Plane Development Kit). While DPDK is incredibly fast, it comes with significant operational trade-offs. `ipblower` uses [**AF_XDP**](https://medium.com/high-performance-network-programming/recapitulating-af-xdp-ef6c1ebead8), the modern Linux-native alternative.



### AF_XDP vs. DPDK

1. **Kernel Coexistence (The Biggest Advantage):**
    * **DPDK:** Completely unbinds the Network Interface Card (NIC) from the Linux kernel using UIO or VFIO. The OS loses total visibility of the interface. You cannot SSH over it, and standard tools like `ethtool`, `tcpdump`, or `iproute2` stop working.
    * **AF_XDP:** Operates *cooperatively* with the kernel. You bind an AF_XDP socket to a specific hardware queue (e.g., Queue 1). Your application gets Zero-Copy, raw DMA access to that queue, while the Linux network stack continues to process normal traffic (SSH, Ping, ARP) seamlessly on Queue 0. All standard Linux networking tools continue to work.

2. **No Custom Drivers:**
    * **DPDK:** Requires maintaining and compiling complex, out-of-tree Poll Mode Drivers (PMDs) tailored to specific NIC hardware.
    * **AF_XDP:** Uses the upstream, built-in Linux NIC drivers (e.g., `i40e`, `ixgbe`, `mlx5_core`). If your Linux kernel supports the NIC, AF_XDP supports it natively.

3. **Security and Stability:**
    * **DPDK:** Runs as a massive userspace process with unmitigated direct memory access (DMA). A bug in DPDK can crash the system or corrupt memory.
    * **AF_XDP:** Uses the kernel's eBPF verifier. The minimal BPF program steering traffic to your application is cryptographically verified for safety before the kernel executes it, preventing memory corruption or infinite loops.

4. **Easier Deployment:**
    * **DPDK:** Requires complex system setup, dedicating CPU cores (isolcpus), and configuring massive amounts of HugePages.
    * **AF_XDP:** Requires no special kernel boot parameters. It just works out of the box on any modern Linux kernel (>= 4.18 for copy-mode, >= 5.x for native zero-copy).

---

## 🚀 Installation 

## 💻 Usage

Because `ipblower` interacts directly with the NIC hardware and loads eBPF programs, it must be run with `sudo` or `CAP_NET_ADMIN` privileges.

### CLI Options

```text
High-performance packet generator using AF_XDP

Usage: ipblower [OPTIONS] --interface <INTERFACE>

Options:
  -i, --interface <INTERFACE>    Interface name (e.g., eth0 or enp114s0f1)
  -p, --pps <PPS>                Packets per second string (e.g., "10M", "100k", or "max") [default: max]
  -n, --no-packets <NO_PACKETS>  Number of packets to generate (e.g., "10M", "100k", "1G", or "0" for unlimited) [default: 0]
  -b, --batch-sz <BATCH_SZ>      Batch size for processing packets [default: 64]
      --no-zerocopy              Disable zerocopy (enabled by default)
  -c, --core <CORE>              CPU core ID for the worker thread [default: 0]
  -w, --workers <WORKERS>        Number of worker threads (1-4), each on separate core and TX queue [default: 1]
  -d, --dst <DST>                Destination IP and MAC address (format: <ip>-<mac>)
                                 Examples: 192.168.1.1-aa:bb:cc:dd:ee:ff
                                          192.168.1.1 (MAC uses default)
                                          -aa:bb:cc:dd:ee:ff (IP uses default)
  -h, --help                     Print help
  -V, --version                  Print version

```

### Examples

**1. The "Firehose" (Maximum Speed)**
Saturate the wire as fast as the CPU and PCIe bus will allow using Zero-Copy hardware DMA.
```bash
sudo ipblower -i enp114s0f1
```

**2. Rate-limited Generation**
Generate 5 Million Packets Per Second (5Mpps) using a batch size of 64 to optimize PCIe transfers.
```bash
sudo ipblower -i enp114s0f1 -p 5M -b 64
```

**3. Custom Destination IP and MAC Address**
Specify both destination IP address and MAC address for generated packets:
```bash
sudo ipblower -i enp114s0f1 -d 192.168.1.100-aa:bb:cc:dd:ee:ff
```

**4. Custom Destination IP (MAC uses default)**
Specify only the destination IP address, MAC will use default `08:bf:b8:0a:0b:0c`:
```bash
sudo ipblower -i enp114s0f1 -d 192.168.1.100
```

**5. Custom Destination MAC (IP uses default)**
Specify only the destination MAC address, IP will use default `192.168.177.1`:
```bash
sudo ipblower -i enp114s0f1 -d -aa:bb:cc:dd:ee:ff
```

**6. Limited Packet Count**
Generate exactly 100 million packets and then stop:
```bash
sudo ipblower -i enp114s0f1 -n 100M
```

**7. Multiple Workers for Higher Throughput**
Use 4 worker threads on separate cores and TX queues to maximize throughput (up to 14 Mpps):
```bash
sudo ipblower -i enp114s0f1 -w 4 -c 0
```
This will spawn 4 workers starting from core 0 (cores 0, 1, 2, 3), each using a separate TX queue.

**8. Software Fallback / Compatibility Mode**
If your NIC driver does not support hardware Zero-Copy, or if you are testing on a virtual interface (veth), use the `--no-zerocopy` flag to fall back to standard kernel SKB copy-mode.
```bash
sudo ipblower -i enp114s0f1 --no-zerocopy
```

---

## ⚠️ Troubleshooting & Performance Tuning

### Sudden Drop in Performance (~2 Mpps limit)
If `ipblower` previously hit 13+ Mpps but is now stuck around 2 Mpps, your NIC has likely fallen out of Zero-Copy mode. This almost always happens if you opened **Wireshark** or `tcpdump` on the interface, which forces the NIC into Promiscuous mode and disables Zero-Copy DMA to allow the kernel to clone packets.

**The Fix:**
Stop any packet captures and hard-reset the interface to restore Zero-Copy capabilities:
```bash
sudo killall dumpcap wireshark tshark tcpdump
sudo ip link set dev enp114s0f1 promisc off
sudo ip link set dev enp114s0f1 down
sudo ip link set dev enp114s0f1 up
```

### Optimizing Batch Size
If you are struggling to approach physical line-rate (e.g., 14.88 Mpps for 10GbE), try increasing the batch size.
* `-b 64`: Good balance of latency and throughput.
* `-b 128`: Maximizes throughput by minimizing the number of `poll()` syscalls and wakeups to the hardware driver.

## ⚠️ Compatibility Note: RHEL / Rocky / Fedora
Currently, **ipblower** may fail to load BPF skeletons when running inside a Docker container on RHEL-based distributions (Rocky Linux, CentOS, AlmaLinux).

**The Error:** `libbpf: Error in bpf_object__probe_loading(): Operation not permitted(1)`

**Cause:** This is due to strict Kernel Lockdown and BPF LSM settings in RHEL-based kernels that restrict BPF system calls from within container namespaces, even when running with `--privileged`.

**Workaround:** For these distributions, it is recommended to run the binary **directly on the host** rather than via Docker.