#!/bin/bash

# Exit immediately if any command fails
set -e

# Start the Wabrix data plane engine in the background for the veth test
sudo RUST_LOG="info" ./target/release/wabrix -c wabrix/examples/filter_veth.toml &

# Capture the process ID of the background engine
WABRIX_PID=$!

# Register a fallback trap to guarantee the engine process is killed upon unexpected script exit
trap 'sudo kill $WABRIX_PID 2>/dev/null || true' EXIT

# Pause execution to allow the engine to map UMEM and initialize network namespaces
sleep 2

# Execute the first integration test
cargo test --test test_filter_veth

# Explicitly terminate the background engine
sudo kill $WABRIX_PID

# Wait for the process to fully exit to ensure network ports and AF_XDP queues are released
wait $WABRIX_PID 2>/dev/null || true

# Provide a brief buffer for the kernel to clean up interface states
sleep 2

# Execute the tcp2 test (this test manages its own engine lifecycle internally)
cargo test --test test_tcp2 --release --features wasm