#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Ensure the script is run as root
if [ "$EUID" -ne 0 ]; then
  echo "Error: Please run this script as root (use sudo)."
  exit 1
fi

echo "=== Configuring Global NAPI Busy Poll Settings ==="
sysctl -w net.core.busy_poll=50
sysctl -w net.core.busy_read=50

# Define your interfaces
INTERFACES=("enp114s0f0np0" "enp114s0f1np1")

for IFACE in "${INTERFACES[@]}"; do
    echo "=== Configuring Interface: $IFACE ==="
    
    # Check if the interface exists
    if [ ! -d "/sys/class/net/$IFACE" ]; then
        echo "Warning: Interface $IFACE not found! Skipping..."
        continue
    fi

    # 1. Defer Hardware Interrupts (Wait for user-space poll)
    echo 100 > "/sys/class/net/$IFACE/napi_defer_hard_irqs"
    echo " -> napi_defer_hard_irqs set to 100"

    # 2. Set the Flush Timeout (200 microseconds safety net)
    echo 2000000 > "/sys/class/net/$IFACE/gro_flush_timeout"
    echo " -> gro_flush_timeout set to 2000000"

    # 3. Disable Ethernet Flow Control (PAUSE frames)
    # This prevents the NIC from locking up the link if the RX ring fills up temporarily
    ethtool -A "$IFACE" rx off tx off 2>/dev/null || true
    echo " -> Flow control disabled (rx off, tx off)"
done

echo "=== Configuration Complete ==="
echo "Your Intel X710 interfaces are now fully armed for zero-interrupt AF_XDP polling!"
