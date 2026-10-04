# Wabrix Telemetry Architecture

To process millions of packets per second, the core AF_XDP worker threads cannot afford to pause, allocate memory, or format data for user interfaces. They need to increment a number in a fraction of a nanosecond and move on.

The telemetry system bridges this ultra-fast "data plane" with the much slower "management plane" (the UI or Prometheus) using three specific structures.

## 1. `TelemetryMetrics` (The Fast-Path Pointers)

`TelemetryMetrics` is the internal link directly to the high-performance worker threads.

## Runtime diagnostics

The optional `runtime-diagnostics` feature provides timer-path diagnostics for
the generator engine. It reports the number of `process_timers()` calls,
rate-weighted timer cycles credited, requested and available frames, cap-hit
counts, maximum available frames, first and last timer TSC values, the final
cycle accumulator, and available/final-frame histograms when a generation run
completes.

The host `wabrix_wasm` crate exposes a matching `runtime-diagnostics` feature
through `wabrix_lib` and the `wabrix` binary. When enabled, it logs final timer
counters for each WASM node, including pipeline name, worker ID, worker index,
allocation shortfalls, routing totals, and mailbox errors.

Diagnostics are disabled by default. Enable host-side logging with:

```bash
cargo build -p wabrix --release --features "wasm,runtime-diagnostics"
```

Enable the extended generator-plugin completion log separately with
`--features runtime-diagnostics` when building `generator_engine` for
`wasm32-wasip2`.

Notice that it doesn't hold standard numbers (`u64`); instead, it holds `Arc<AtomicU64>`.
* **What it is:** A collection of thread-safe memory pointers.
* **How it's used:** When a Wasm plugin or native node is created, it allocates an `AtomicU64` counter. It keeps one pointer for itself (to increment during packet processing) and gives a clone of that pointer to `TelemetryMetrics`.
* **The Goal:** Absolute zero-cost telemetry. The worker thread just updates memory atomically. It never locks, and it never talks to the UI.

## 2. `TelemetryRegistry` (The Central Hub)

Because Wabrix is multi-threaded, there might be 4 different CPU cores all running the same `tcp_engine` plugin.

* **What it is:** A simple repository (a `Vec`) that stores all the `TelemetryMetrics` from every worker thread across the entire system.
* **How it's used:** During system initialization, every worker registers its atomic counters here.
* **The Goal:** It acts as the single source of truth for the background telemetry thread. Instead of the telemetry thread trying to query individual workers (which would cause lock contention), it just iterates over this registry.

## 3. `TMLiveStats` (The Public Snapshot)

While `TelemetryMetrics` is raw, live memory, `TMLiveStats` is a fixed, serialized snapshot of that memory at a specific point in time.

* **What it is:** A standard struct heavily decorated with Serde macros (`#[derive(Serialize)]`) and TypeScript bindings (`#[ts(export_to = "...")]`). It holds standard integers (`u64`).
* **How it's used:** The background slow-path task (`spawn_pipeline_telemetry`) wakes up every ~1 second. It looks at the `TelemetryRegistry`, reads all the atomic counters, aggregates the totals from all the different CPU cores, and calculates the **rates** (Packets Per Second / Bytes Per Second) by comparing the new numbers against the previous second's numbers.
* **The Goal:** This is the finalized, human-readable data packet. It is packaged up and broadcasted over the `telemetry_tx` channel to be converted into JSON for your React UI or formatted for your Prometheus exporter.

## Summary of the Flow

1. **Setup:** Workers register their `AtomicU64` pointers in the `TelemetryRegistry`.
2. **Execution:** Workers process packets at line-rate, incrementing their atomic counters.
3. **Polling:** Every second, a background thread reads the `TelemetryRegistry`.
4. **Formatting:** The background thread calculates the speeds (PPS/BPS), packages the results into a `TMLiveStats` snapshot, and broadcasts it to the outside world.

## Inspector engine plugin packet delay histogram

The inspector keeps fixed 10 µs bins on the packet path, with finite bounds from
10 µs through 1 ms and a `+Inf` bucket for larger delays. The host aggregates
the worker snapshots and exports a Prometheus histogram named
`wabrix_packet_delay_seconds`.

Use this query in a Grafana Heatmap panel to plot the per-second distribution:

```promql
sum by (le) (rate(wabrix_packet_delay_seconds_bucket[1m]))
```

Set the panel unit to seconds; each bucket boundary is 10 µs apart. The
histogram sample sum and count are also available as
`wabrix_packet_delay_seconds_sum` and
`wabrix_packet_delay_seconds_count`.
