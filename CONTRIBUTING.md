# Contributing to Wabrix (in work)

First off, thank you for considering contributing to Wabrix! 

Our mission is to bridge the gap between IT and OT security by delivering ultra-fast, line-rate packet processing using AF_XDP and WebAssembly. We welcome contributions from the community to help make Wabrix the standard for modern, converged network security.

## ⚖️ Our Open Core Philosophy

Wabrix operates on an **Open Core** model.
* **What belongs here (Open Source):** The base AF_XDP networking engine, the TOML routing graph parser, the Wasmtime host integration, and foundational Wasm plugins (e.g., basic IP/Port filtering) and native processing nodes (e.g. fast_parse).
* **What is Enterprise:** Advanced deep-packet inspection modules (like proprietary S7 or advanced Modbus scrubbers), the graphical management UI, and specialized DDoS mitigation heuristics.

*If you are unsure whether your feature idea belongs in the open-source core, please open a GitHub Issue for discussion before writing code.*

## 🛠️ Setting Up Your Development Environment

Because Wabrix interacts directly with the Linux kernel's networking stack, your development environment requires a few specific components:

1. **OS:** Linux is mandatory. We actively test against **Kernel 6.17+**. At a minimum, Kernel 6.1+ is required for the necessary AF_XDP zero-copy primitives.
2. **Permissions:** You will need `root` or `CAP_NET_ADMIN` privileges to load XDP programs and bind AF_XDP sockets during testing.
3. **Toolchain:** 
    * Latest stable Rust (`rustup default stable`)
    * `clang` and `llvm` (for compiling eBPF programs)
    * `libbpf-dev`
    * `wasmtime`

## 🚀 The Golden Rule: The "Hot Path"

Wabrix is designed to process more than ten million packets per second per core. **Performance is our primary feature.**

When contributing to the core packet-processing loop (the "hot path"):
* **Zero Allocations:** Do not use `Vec`, `Box`, `String`, or any heap allocations in the hot path. Use stack variables, pre-allocated memory pools, or direct UMEM memory mapping.
* **Zero Copy:** Do not use `memcpy` to move packet data unless absolutely explicitly required and justified. We pass pointers to the Wasm sandbox, not data.
* **Unsafe Rust:** We isolate `unsafe` blocks strictly to the FFI boundary (interacting with `libbpf` and UMEM). Any new `unsafe` code must be heavily documented explaining *why* it is safe.

## 🔄 Development Workflow

1. **Fork the Repository:** Create your own fork on GitHub.
2. **Branch:** Create a branch for your feature or bug fix (`git checkout -b feature/amazing-new-filter`).
3. **Write Code & Tests:** Ensure your code is thoroughly tested.
4. **Lint & Format:** 
    * Run `cargo fmt` to format your code.
    * Run `cargo clippy -- -D warnings` to ensure there are no linting errors. PRs with clippy warnings will be rejected by our CI.
5. **Commit:** Use descriptive commit messages.
6. **Push & Pull Request:** Push to your fork and open a PR against our `main` branch.

## 🧪 Testing (in work)

Testing AF_XDP applications can be tricky. We use `veth` (virtual ethernet) pairs for integration testing. 
Before submitting a PR, ensure you run the test suite:

### Note: Requires root for veth creation and XDP attachment
sudo -E cargo test

If you are contributing a new Wasm plugin, please include unit tests that execute the compiled `.wasm` binary against dummy packet byte arrays.

## 🔒 Security Vulnerabilities

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security flaw please email us. 

Note: Address spaces of WASM plugins are currrently not isolated and plugins also have unrestricted access the DMA memory (Umem) of the NIC. With the current memory management of WebAssembly this is unavoidable in a zero-copy architecture.

---
*Thank you for helping us build the future of line-rate network security!*
