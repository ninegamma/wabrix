#!/bin/bash

# Default values
IFACE="enp114s0f1np1"
PPS="max"
BATCH="64"
CORE="0"
DST=""
IMAGE="ghcr.io/rstade/ipblower:latest"
ZEROCOPY="true"

# 1. Check if the interface exists
check_interface() {
    if ! ip link show "$1" > /dev/null 2>&1; then
        echo "❌ Error: Interface '$1' not found."
        exit 1
    fi
}

# 2. Detect NUMA node and suggest CPU pinning
get_numa_cores() {
    local iface=$1
    if [ -d "/sys/class/net/$iface/device" ]; then
        local numa=$(cat "/sys/class/net/$iface/device/numa_node")
        if [ "$numa" != "-1" ]; then
            echo "ℹ️  NIC $iface is on NUMA node $numa."
            # List cores on this NUMA node for user reference
            local cores=$(cat "/sys/class/net/$iface/device/local_cpulist")
            echo "   Recommended cores for this NIC: $cores"
        fi
    fi
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "  -i <iface>   Interface (default: $IFACE)"
    echo "  -p <pps>     PPS (e.g. 10M, 100k, max) (default: $PPS)"
    echo "  -b <size>    Batch size (default: $BATCH)"
    echo "  -c <core>    Core ID (default: $CORE)"
    echo "  -d <dst>     Dest IP-MAC (e.g. 192.168.1.1-AA:BB:CC:DD:EE:FF)"
    echo "  -z           Disable ZeroCopy"
    exit 1
}

# Parse script arguments
while getopts "i:p:b:c:d:zh" opt; do
  case ${opt} in
    i ) IFACE=$OPTARG ;;
    p ) PPS=$OPTARG ;;
    b ) BATCH=$OPTARG ;;
    c ) CORE=$OPTARG ;;
    d ) DST=$OPTARG ;;
    z ) ZEROCOPY="false" ;;
    h ) usage ;;
    * ) usage ;;
  esac
done

# Run Pre-flight checks
check_interface "$IFACE"
get_numa_cores "$IFACE"

# Build the command array
CMD_ARGS=("-i" "$IFACE" "-p" "$PPS" "-b" "$BATCH" "-c" "$CORE")
if [ -n "$DST" ]; then
    CMD_ARGS+=("-d" "$DST")
fi
if [ "$ZEROCOPY" = "false" ]; then
    CMD_ARGS+=("--no-zerocopy")
fi

echo "🚀 Launching IPBlower on Core $CORE..."

docker run -it --rm \
    --network host \
    --privileged \
    --ulimit memlock=-1 \
    --cpuset-cpus "$CORE" \
    -e RUST_LOG=info \
    "$IMAGE" \
    "${CMD_ARGS[@]}"
