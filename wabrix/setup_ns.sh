#!/bin/bash

# --- Configuration ---
INTERFACE="eno1"
NAMESPACE="nstest"
IP_ADDR="192.168.177.244/24"
GATEWAY="192.168.177.1"
DNS_SERVER="192.168.177.2"


# Identify the actual user who called sudo
REAL_USER=${SUDO_USER:-$USER}

# Check for root
if [[ $EUID -ne 0 ]]; then
   echo "Please run with sudo"
   exit 1
fi

echo "--- Initializing Namespace: $NAMESPACE ---"

# 1. Create namespace if it doesn't exist
ip netns add $NAMESPACE 2>/dev/null || echo "Namespace already exists."

# 2. Move interface (Ignore NetworkManager)
nmcli device set $INTERFACE managed no 2>/dev/null
ip link set $INTERFACE netns $NAMESPACE

# 3. Configure Interface inside Namespace
ip netns exec $NAMESPACE ip link set lo up
ip netns exec $NAMESPACE ip link set $INTERFACE up
ip netns exec $NAMESPACE ip addr add $IP_ADDR dev $INTERFACE

# 2. Setup Routing and DNS
echo "Setting default gateway to $GATEWAY..."
ip netns exec $NAMESPACE ip route add default via $GATEWAY

# Create custom DNS for this namespace
mkdir -p /etc/netns/$NAMESPACE
echo "nameserver $DNS_SERVER" > /etc/netns/$NAMESPACE/resolv.conf

echo "--- Network Configuration Complete ---"

# 1. Create a tiny helper script to set the prompt inside the namespace
# We put it in /tmp so the namespace bash can read it.
CAT_SCRIPT="/tmp/ns_enter.sh"
echo "export PS1='($NAMESPACE) \u@\h:\w# '" > $CAT_SCRIPT
echo "echo 'Network status for $INTERFACE:'" >> $CAT_SCRIPT
echo "ip addr show $INTERFACE | grep inet" >> $CAT_SCRIPT
echo "ip route show" >> $CAT_SCRIPT
chmod 644 $CAT_SCRIPT

echo "Spawning new terminal for $REAL_USER..."

# 2. Launch the terminal as the REAL USER
# Then inside that terminal, run sudo ip netns exec
# Then load the helper script to force the prompt
sudo -u $REAL_USER DISPLAY=$DISPLAY XAUTHORITY=$XAUTHORITY gnome-terminal -- bash -c \
"sudo ip netns exec $NAMESPACE bash --rcfile $CAT_SCRIPT"

echo "Done. Look for the new terminal window."
