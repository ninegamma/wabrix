# Interface Configuration

XBricks automatically configures network interfaces based on the TOML configuration file.

## What Gets Configured

When you run xbricks, it automatically:

1. **Checks interface exists** - Verifies the interface is present in the system
2. **Brings interface UP** - Ensures the interface is in UP state
3. **Configures queue count** - Sets the number of combined RX/TX queues using `ethtool -L`
4. **Configures ring sizes** - Sets RX and TX ring buffer sizes using `ethtool -G`
5. **Verifies configuration** - Checks that settings were applied correctly
6. **Checks link status** - Reports whether the interface has carrier/link

## Configuration Process

The configuration happens before XDP program attachment and follows this sequence:

```
Load Config → Configure Interfaces → Load BPF → Attach XDP → Start Workers
```

## Example Output

```bash
$ sudo RUST_LOG=info ./target/debug/xbricks --config examples/simple.toml

[INFO  xbricks] Loading configuration from: examples/simple.toml
[INFO  xbricks] Configuration loaded and validated successfully
[INFO  xbricks] Configuring network interfaces...
[INFO  xbricks::interface_setup] Configuring interface enp114s0f0...
[INFO  xbricks::interface_setup] ✓ Interface enp114s0f0 is UP
[INFO  xbricks::interface_setup] ✓ Set 2 combined queues on enp114s0f0
[INFO  xbricks::interface_setup] ✓ Set ring sizes on enp114s0f0: RX=4096, TX=4096
[INFO  xbricks::interface_setup] ✓ Interface enp114s0f0 has link (carrier detected)
[INFO  xbricks::interface_setup] ✓ Interface enp114s0f0 configured successfully
[INFO  xbricks] All interfaces configured successfully
[INFO  xbricks] eBPF program loaded successfully
...
```

## Configuration Fields

From the TOML file:

```toml
[xbricks]
ifaces = [
    { name = "enp114s0f0", rxd = 4096, txd = 4096, queues = 2 }
]
```

- **name**: Interface name (e.g., "eth0", "enp114s0f0")
- **queues**: Number of combined RX/TX queues to configure
- **rxd**: RX ring buffer size (number of receive descriptors)
- **txd**: TX ring buffer size (number of transmit descriptors)

## Automatic Configuration Commands

XBricks executes the equivalent of these commands automatically:

```bash
# Set interface UP
ip link set enp114s0f0 up

# Configure queue count
ethtool -L enp114s0f0 combined 2

# Configure ring buffer sizes
ethtool -G enp114s0f0 rx 4096 tx 4096
```

This configures the NIC with:
- 2 combined RX/TX queue pairs
- 4096 receive descriptors per queue
- 4096 transmit descriptors per queue

## Requirements

### Root Privileges

Interface configuration requires root/sudo:
- Setting interface state (UP/DOWN) requires `CAP_NET_ADMIN`
- Using ethtool requires root or `CAP_NET_ADMIN`

### Supported Operations

- ✅ Bring interface UP
- ✅ Configure combined queue count
- ✅ Configure RX/TX ring buffer sizes
- ✅ Verify interface state
- ✅ Check link/carrier status

## Troubleshooting

### Interface Does Not Exist

```
Error: Interface eth0 does not exist
```

**Solution:** Check interface name with `ip link show` and update TOML config.

### Queue Configuration Failed

```
WARN: Failed to set queue count on eth0: Operation not supported
```

**Cause:** Some drivers don't support changing queue count at runtime.

**Solution:**
- Configure queues at driver load time (via module parameters)
- Or manually configure before running xbricks:
  ```bash
  sudo ethtool -L eth0 combined 4
  ```

### Ring Size Configuration Failed

```
WARN: Failed to set ring sizes on eth0: Operation not supported
```

**Cause:** Some drivers don't support changing ring sizes, or the requested size exceeds hardware limits.

**Solution:**
- Check maximum ring sizes: `ethtool -g eth0`
- Adjust `rxd` and `txd` values in config to be within supported range
- Or manually configure before running xbricks:
  ```bash
  sudo ethtool -G eth0 rx 4096 tx 4096
  ```

### No Link Detected

```
WARN: Interface enp114s0f0 has no link (no carrier)
```

**Cause:** Physical cable unplugged or link not established.

**Solution:** Check physical connection. The program will continue but won't receive packets.

### Permission Denied

```
Error: Failed to bring interface eth0 up: Operation not permitted
```

**Solution:** Run with sudo:
```bash
sudo ./target/debug/xbricks --config examples/simple.toml
```

## Manual Configuration

If you prefer to configure interfaces manually before running xbricks:

```bash
# Bring interface up
sudo ip link set enp114s0f0 up

# Set queue count
sudo ethtool -L enp114s0f0 combined 4

# Set ring buffer sizes
sudo ethtool -G enp114s0f0 rx 4096 tx 4096

# Verify configuration
ethtool -l enp114s0f0  # Check queue count
ethtool -g enp114s0f0  # Check ring sizes
ip link show enp114s0f0 # Check interface state
```

XBricks will detect the existing configuration and skip redundant steps.

## Debug Mode

For detailed interface configuration logs:

```bash
sudo RUST_LOG=debug ./target/debug/xbricks --config examples/simple.toml
```

This shows:
- Interface existence checks
- Current vs. desired queue counts
- Detailed ethtool output
- Verification steps
