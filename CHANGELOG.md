# Changelog

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

Baseline release before the shared Wasm memory and positive UMEM offset work
described in 0.4.0.
