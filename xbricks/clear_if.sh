#!/bin/bash
IFACE="enp114s0f0"
sudo ip link set dev $IFACE xdp off          # Unload XDP
sudo ethtool -L $IFACE combined 2            # Match queues to threads
sudo ethtool -G $IFACE rx 4096 tx 4096       # Maximize buffers
sudo ip link set dev $IFACE up               # Ensure link is up
