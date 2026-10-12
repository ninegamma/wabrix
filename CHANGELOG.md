# Changelog

## [0.5.0] - 2026-10-11

This release replaces positional and in-band plugin telemetry with named
out-of-band metrics and extends telemetry to support multiple histograms per
node. Also, massive performance improvements have been achieved.

### Added

- Named counter and histogram records in the WIT telemetry interface.
- Packet-delay histogram export with cumulative counts, sums, and samples.
- Generic multi-histogram aggregation and Prometheus export.
- Stable counter names for filter, generator, inspector, and TCP plugins.
- Updated telemetry snapshots and TypeScript bindings for named counters and
  histograms.
- Bounded per-worker reuse of `Batch` packet storage after downstream batches
  are dropped.

### Changed

- Migrated plugin telemetry from positional `u64` values and in-band markers
  to structured out-of-band records.
- Removed counter-label arrays from node configuration and example TOML files.
- Updated the host, endpoint UI, metrics exporter, and integration tests to
  consume named counters.
- Updated the workspace to the current `xsk-core`, `libbpf-rs`, and Wabrix API
  dependency versions.
- Initialized AF_XDP fill queues during socket creation with preallocated
  addresses via `xsk-core` 0.2.1.
- Refreshed the one-worker generator example for the current benchmark topology
  and packet rate.

### Fixed

- Preserved histogram snapshots during idle Wasm telemetry synchronization.
- Validated histogram bucket schemas while aggregating metrics across workers.
- Skipped serving-core IRQ-affinity checks when an interface has no RX worker
  assigned.

## [0.4.0] - 2026-09-24

This release extends the Wasm dataplane memory model while retaining shared
AF_XDP UMEM ownership across workers and Wasm instances.

### Added

- Shared zero-copy UMEM mappings for multiple Wasm instances. Each instance
  receives a private Wasm heap and mailbox while aliasing the same AF_XDP
  packet pages.
- Custom Wasmtime linear-memory creation that stitches the private Wasm region
  and the shared UMEM into one contiguous mapping.
- Per-instance mailboxes outside the UMEM, so mailbox metadata does not consume
  AF_XDP frames or require fill-queue protection.
- Positive `umem_offset` address translation for Wasm guests. Packet
  descriptors remain UMEM-relative and are translated with
  `umem_offset + desc.addr`.
- Runtime diagnostics for Wasm timer and mailbox accounting.
- NAPI busy-poll configuration and profiling support for AF_XDP workers.

### Changed

- Removed the reserved UMEM area, scratchpad partitions, and shared UMEM
  control block.
- All UMEM frames are now available to AF_XDP queues.
- Updated the Wasm SDK, WIT interface, built-in plugins, and host pipeline to
  use `umem_offset` consistently.
- Simplified worker pipeline configuration and live node configuration updates.
- Updated packet, fill-queue, completion-queue, and descriptor handling to use
  the current `xsk-core` address APIs.

### Fixed

- Corrected packet and mailbox address calculations for the stitched Wasm
  memory layout.
- Improved shutdown and resource lifetime handling for shared UMEM mappings.
- Removed stale reserved-UMEM documentation and configuration fields.

## [0.3.0]

Release before the shared Wasm memory and positive UMEM offset work described
in 0.4.0.

### Added

- NAPI-aware polling, busy-poll configuration, and interface-aware IRQ
  affinity checks.
- SMT-aware pre-flight diagnostics and restoration of interface settings after
  NAPI configuration changes.

### Changed

- Improved AF_XDP receive performance and timestamp telemetry validation.
- Updated the workspace to the xsk-rs 0.10.0 API baseline.

## [0.2.0] - 2026-08-25

Release tag: `f3e9d3c9`.

### Added

- Unified Wasm engines and multi-worker runtime diagnostics.
- Generator-engine token-bucket rate control, runtime diagnostics, SYN and UDP
  generation, protocol selection, and hot-update support.
- Delayed `Running` lifecycle handling and timer-aware worker scheduling.
- TSC timestamp support, Grafana dashboards, and expanded monitoring.
- SMT-aware IRQ affinity warnings and improved data-plane telemetry.

### Fixed

- Preserved SYN timer reservations and added Wasm batch validation.
- Improved data-plane latency and configuration handling.
- Resolved xsk-rs 0.9 dependency issues through local patches.

## [0.1.0] - 2026-08-07

Stable data-plane baseline before the Wasm multi-memory work.

### Added

- Initial workspace split into the core Wabrix crates and public plugin/API
  components.
- AF_XDP dataplane, pipeline, telemetry, configuration, and Wasm host
  foundations.

Release tag: `v0.1.0` (`eef29e7`).
