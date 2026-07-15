#!/bin/bash

# --- Configuration ---
INTERFACE="eno1"
NAMESPACE="nstest"

# Check for root
if [[ $EUID -ne 0 ]]; then
   echo "Please run with sudo: sudo ./cleanup_ns.sh"
   exit 1
fi

echo "--- Cleaning up Namespace: $NAMESPACE ---"

# 1. Flush IP addresses and bring the link down inside the namespace
# This ensures a clean state before moving back
if ip netns list | grep -q "$NAMESPACE"; then
    echo "Clearing $INTERFACE configurations..."
    ip netns exec $NAMESPACE ip addr flush dev $INTERFACE
    ip netns exec $NAMESPACE ip link set $INTERFACE down
    
    # 2. Delete the namespace
    # Note: When a namespace is deleted, any physical interfaces inside it
    # automatically move back to the default (host) namespace.
    echo "Deleting namespace $NAMESPACE..."
    ip netns del $NAMESPACE
else
    echo "Namespace $NAMESPACE not found. Checking if interface is orphaned..."
fi

# 3. Restore the interface on the host
echo "Restoring $INTERFACE to host..."
ip link set $INTERFACE up

# 4. Hand control back to NetworkManager
echo "Re-enabling NetworkManager for $INTERFACE..."
nmcli device set $INTERFACE managed yes

echo "--- Cleanup Complete ---"
ip addr show $INTERFACE
