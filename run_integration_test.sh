#!/bin/bash

# Exit immediately if any command fails
set -e

# Start the Wabrix data plane engine in the background
sudo RUST_LOG="info" ./target/release/wabrix -c wabrix/examples/filter_veth.toml &

# Capture the process ID of the background engine
WABRIX_PID=$!

# Register a trap to guarantee the engine process is killed upon script exit
trap 'sudo kill $WABRIX_PID 2>/dev/null || true' EXIT

# Pause execution to allow the engine to map UMEM and initialize network namespaces
sleep 2

# Execute the integration test using the configured sudo runner
cargo test --test test_filter_veth