# Wabrix Version History

This document is based on the workspace version declared in [`Cargo.toml`](Cargo.toml) and the commit history of the `wabrix` repository.

## 0.3.0

Current workspace version (`Cargo.toml`). 

- Improved AF_XDP receive performance with NAPI-aware polling and busy-poll configuration.
- Added interface-aware NAPI and IRQ affinity checks, including SMT-aware pre-flight diagnostics.
- Added restoration of interface settings after NAPI configuration changes.
- Improved timestamp telemetry validation and updated the workspace to xsk-rs 0.10.0.

Notable commits: `48201f0`, `79f5816`, `93927e6`, `34ce5bc`, `3b5d739`.

## 0.2.0

Tagged on 2026-08-25 at `f3e9d3c9`.

- Unified WASM engines and added multi-worker runtime diagnostics.
- Corrected generator-engine token-bucket rate control and added runtime diagnostics.
- Generalized packet generation for SYN and UDP traffic, with protocol selection and hot-update support.
- Preserved SYN timer reservations and added WASM batch validation.
- Added delayed `Running` lifecycle handling and prevented timer nodes from being delayed by idle backoff.
- Modernized TCP/UDP packet writing and added TSC timestamp support.
- Added Grafana dashboards and expanded monitoring support.
- Added SMT-aware IRQ affinity warnings and improved data-plane latency, telemetry, and configuration handling.
- Adapted the dataplane to xsk-rs 0.9 and resolved dependency issues through local patches.

Release tag: `0.2.0` (`f3e9d3c9`).

## 0.1.0

Tagged baseline released on 2026-08-07 as “Stable data plane state prior to WASM multi-memory support.”

- Established the stable data-plane baseline before the WASM multi-memory experiment.
- Included the initial workspace split into the core Wabrix crates and the public plugin/API components.
- Provided the AF_XDP data-plane, pipeline, telemetry, configuration, and WASM host foundations used by later versions.

Release tag: `v0.1.0` (`eef29e7`).
