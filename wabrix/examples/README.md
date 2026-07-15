# XBricks Configuration Examples

This directory contains example TOML configuration files for various deployment scenarios.

## Example Files

### `simple.toml`
Basic configuration with:
- 1 interface (enp114s0f0)
- 2 queues
- 1 UMEM domain
- Cores 0 and 1

**Use case:** Simple testing, minimal setup

### `mixed_umem.toml`
Mixed UMEM architecture with:
- 1 interface (enp114s0f0)
- 4 queues
- 2 UMEM domains (queues 0,1 share UMEM 0; queues 2,3 share UMEM 1)
- Cores 0, 1, 2, 3

**Use case:** Balanced approach for workloads requiring some isolation but also zero-copy within groups

### `shared_umem.toml`
Shared UMEM configuration with:
- 1 interface (enp114s0f0)
- 4 queues
- 1 large shared UMEM
- Cores 0, 1, 2, 3

**Use case:** Maximum zero-copy performance for cross-queue packet forwarding

### `multi_interface.toml`
Multi-interface setup with:
- 2 interfaces (enp114s0f0, enp114s0f1)
- 2 queues per interface
- 2 UMEM domains (one per interface)
- Cores 0, 1, 2, 3

**Use case:** Multi-NIC deployments, firewall/router applications

## Configuration Fields

### `[xbricks.ifaces]`
- `name`: Interface name (e.g., "eth0", "enp114s0f0")
- `rxd`: RX descriptor ring size
- `txd`: TX descriptor ring size
- `queues`: Number of hardware queues available

### `[xbricks.umem]`
- `id`: Unique UMEM identifier
- `frame_size`: Size of each frame in bytes (must be power of 2)
- `frame_count`: **TOTAL number of frames in entire UMEM** (divided equally among cores sharing this UMEM)
  - Example: `frame_count = 16384` with 2 cores = 8192 frames per core
  - Recommended: 8192-16384 frames per core for good performance
- `fq_size`: Fill queue size (must be power of 2)
- `cq_size`: Completion queue size (must be power of 2)
- `head_room`: Reserved space at frame start for headers

### `[xbricks.xsk]`
- `iface`: Interface name (must match an entry in `ifaces`)
- `queues`: Array of queue IDs to bind
- `umem`: UMEM ID to use (must match an entry in `umem`)
- `rxq_size`: RX queue size (must be power of 2)
- `txq_size`: TX queue size (must be power of 2)
- `zerocopy`: Enable zero-copy mode (true/false)
- `core_affinity`: Array of CPU core IDs (must match length of `queues`)



**Note:** Requires `sudo` because:
- Configuring network interfaces requires root privileges
- Loading eBPF programs requires CAP_BPF or root
- AF_XDP sockets require elevated privileges

## Notes

- All queue sizes must be powers of 2
- `core_affinity` length must match `queues` length
- Queue IDs must be < interface's `queues` value
- UMEM IDs must be unique
- Interface names must match system interfaces
- **IMPORTANT**: `frame_count` is the TOTAL frames in the UMEM, automatically divided among cores
  - Formula: `frames_per_core = frame_count / number_of_cores_sharing_umem`
  - Example: 2 cores sharing UMEM with `frame_count = 16384` → 8192 frames per core
