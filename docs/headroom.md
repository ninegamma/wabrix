# XDP Packet Headroom and UMEM Frame Headroom

**XDP_PACKET_HEADROOM** and **UMEM Frame Headroom** refer to the same functional concept—reserved memory space immediately preceding a packet's payload—but they are governed by different layers of the Linux networking stack and serve slightly different primary actors (the eBPF program vs. the user-space application).

Here is how they break down and interact:

## XDP_PACKET_HEADROOM (Kernel/eBPF Perspective)

* **Definition:** A kernel macro (typically 256 bytes) defining the standard amount of space NIC drivers must reserve before the start of a packet buffer when allocating memory for XDP.
* **Purpose:** It guarantees that an eBPF XDP program running in the kernel has safe, pre-allocated memory to prepend headers (e.g., adding VLAN tags, VXLAN, or IPsec encapsulation) using the `bpf_xdp_adjust_head()` helper.
* **Boundaries:** It represents the space between `xdp->data_hard_start` (the absolute beginning of the memory buffer) and `xdp->data` (the start of the actual packet MAC header).

## UMEM Frame Headroom (AF_XDP User-Space Perspective)

* **Definition:** A user-configurable value passed to the kernel during socket setup via the `xdp_umem_reg` struct (`setsockopt` with `XDP_UMEM_REG`).
* **Purpose:** It reserves space at the start of every chunk within your UMEM. In user-space, this allows a custom data plane to prepend headers before transmitting a packet out of the Tx ring, or to store custom application-level metadata alongside the packet.

## The Intersection in Zero-Copy (ZC) Mode

When utilizing Zero-Copy AF_XDP (such as with the Intel `ice` driver), the kernel bypasses `sk_buff` allocations. The NIC's DMA engine writes the incoming packet *directly* into your user-space UMEM frames. Because the UMEM frame is the only memory buffer that exists for the packet, the two headrooms become physically linked.

| Concept | Who Uses It | How it is Applied in AF_XDP Zero-Copy |
| --- | --- | --- |
| **UMEM Headroom** | User-Space | Defines the physical offset from the start of the UMEM chunk to where the NIC DMA will write the packet. |
| **XDP Headroom** | eBPF Program | Maps directly into the UMEM headroom. The eBPF program executes on the UMEM frame before it reaches user-space. |

### How they interact dynamically

1. **NIC DMA:** The network card receives a packet and places it into a UMEM frame. It offsets the packet payload by the configured **UMEM Frame Headroom**.
2. **eBPF Execution:** The XDP program intercepts the packet. To the XDP program, `xdp->data_hard_start` points to the start of the UMEM frame, and `xdp->data` points to the payload. The space between them is the UMEM headroom, which the eBPF program treats as its available `XDP_PACKET_HEADROOM`.
3. **Kernel Modification:** If your eBPF program calls `bpf_xdp_adjust_head()` to push headers, it consumes a portion of this UMEM headroom.
4. **User-Space Handoff:** When the packet arrives in the Rx ring, the descriptor's `addr` value will reflect the exact offset where the payload now begins. If the eBPF program prepended 20 bytes, the `addr` will be 20 bytes closer to the start of the UMEM frame than the original UMEM headroom boundary.

## Constraint Warning

When configuring high-performance data planes, your UMEM headroom must be large enough to satisfy both the kernel's XDP assumptions and your user-space requirements. If you set the UMEM headroom to 0 to save space, any attempt by an eBPF program to use `bpf_xdp_adjust_head()` to encapsulate a packet will fail and drop the packet, as `xdp->data` is already at `xdp->data_hard_start`.

## Tx Metadata Headroom

**`tx_metadata_len`** is a Tx-specific configuration that carves out a standardized, fixed-size metadata block from the **UMEM Frame Headroom** immediately preceding the outgoing packet payload.

While `XDP_PACKET_HEADROOM` is primarily used by the kernel and eBPF on the **Rx** side to prepend headers, `tx_metadata_len` exists for user-space to communicate hardware offload requests (such as checksum calculation or timestamp recording) to the NIC driver on the **Tx** side.

### Memory Layout in a Zero-Copy UMEM Chunk

```text
+-----------------------------------------------------------------------------------+
|                                 UMEM Chunk (e.g., 4096 bytes)                     |
+-----------------------------------------------------------------------------------+
|               UMEM Frame Headroom                  |                              |
|----------------------------------+-----------------|                              |
| XDP_PACKET_HEADROOM (Unused Tx)  | tx_metadata_len | Packet Payload (MAC/IP/TCP)  |
+----------------------------------+-----------------+------------------------------+
```

### How the Concepts Intersect

* **Location:** The metadata block, represented in C as `struct xsk_tx_metadata`, sits at the end of the UMEM Frame Headroom, directly before the packet payload.
* **Fixed Size:** Unlike Rx metadata, which an eBPF program can size dynamically per packet, `tx_metadata_len` is declared once during socket initialization through the `XDP_UMEM_TX_METADATA_LEN` flag in `xdp_umem_reg`. The length remains identical for every socket sharing that UMEM.
* **The Buffer Sharing Catch:** High-performance data planes often recycle the same UMEM frames from the Rx ring directly into the Tx ring. The total configured UMEM Frame Headroom must therefore be large enough to contain both the `XDP_PACKET_HEADROOM` required by the kernel eBPF program and the `tx_metadata_len` required for Tx offloads.

### Practical Application for TCP Data Planes

For a high-throughput TCP data plane such as wabrix running on an Intel 25G NIC with the `ice` driver, software TCP checksum calculation can be a significant CPU cost.

By configuring `tx_metadata_len = sizeof(struct xsk_tx_metadata)` during UMEM registration, the application reserves space for this metadata. Before transmitting a batch of TCP packets, a Rust application can populate the `xsk_tx_metadata` structure immediately before the packet payload and set the flags required for hardware checksum offload. When the packet reaches the `ice` driver on a kernel supporting this metadata path, the driver can ask the E830 NIC to calculate and insert the TCP checksum in hardware, reducing user-space CPU overhead.
