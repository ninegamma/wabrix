# Wasm Component Hot Reloading

Wabrix can replace a running `WasmEngine` component without rebuilding its
pipeline or restarting its AF_XDP workers. The replacement is prepared beside
the active Wasmtime runtime, then installed by `PipelineRunner` during its
existing timer/control polling. This keeps packet processing available during
compilation and initialization without checking reload state for every batch.

This is a live replacement of the Wasm runtime only. It does not change the
node's routes, worker assignment, UMEM configuration, or AF_XDP sockets.

## What triggers a reload

The live-update path compares the current and target configurations. A change
to a `WasmEngine` node's effective `wasm_file_path` is sent to the running
workers as an `UpdateConfiguration` control event. `effective_arg` checks
`hot_args` first and falls back to `args`, so the path can be declared in either
place. For live updates, putting it in `hot_args` makes its role clear:

```toml
[[pipelines.nodes]]
id = "filter"
type = "WasmEngine"
routes = ["allow", "drop"]
args = { umem = "0" }
hot_args = { wasm_file_path = "/opt/wabrix/policies/filter-v1.wasm" }
```

To deploy a new version, first place the replacement component at a new path,
then change the configured path and apply the target configuration:

```toml
hot_args = { wasm_file_path = "/opt/wabrix/policies/filter-v2.wasm" }
```

For example, apply the file with the running CLI's `apply <file.toml>` command.
The same configuration update can also change `hot_node_data`; those values
are included when the replacement receives its startup data. Changes to
`node_data` are startup-only and require rebuilding the pipeline.

Use a new path for each deployed artifact. Merely replacing the bytes at the
existing path does not create a configuration difference, so it does not by
itself trigger a reload. Wasmtime's component cache is keyed by both path and
SHA-256 content hash, which prevents changed contents from reusing the old
compiled component when that path is loaded again.

## Reload lifecycle

1. **Reconcile configuration.** The control plane identifies the changed
   `wasm_file_path` and sends an update event to the matching `WasmEngine` node
   on each worker. Structural changes such as node, route, or worker changes
   still require a full pipeline restart.
2. **Stage a candidate.** Each worker starts a background reload thread. It
   reads and compiles or loads the component, creates a new Wasmtime instance,
   and calls the component's `init-data` function. The active runtime continues
   processing packets and timers during this work.
3. **Validate initialization.** The candidate gets the node's base startup data
   merged with the latest `hot_node_data` values. It must instantiate
   successfully and accept `init-data`. A false result, trap, file error, or
   compile/instantiate error rejects the candidate.
4. **Switch at a runner boundary.** The `PipelineRunner` checks for completed
   candidates when it is already polling timers and control events. The active
   and staged engine instances already map the same per-node shared-state
   region, so matching layouts need no copy. If the candidate declares an
   append-only upgrade from the stored schema, the runner calls its
   `upgrade-shared-state` export at this boundary, after the old runtime has
   stopped accessing the payload. The host commits the new schema version and
   payload size only after the callback succeeds. A trap, rejected upgrade, or
   incompatible layout discards the candidate and leaves the old runtime
   installed. The routes, packet counters, pipeline, worker thread, and sockets
   remain in place. Packets already processed in the current iteration use the
   prior component; later work uses the new one.
5. **Retire the old runtime.** The prior Wasmtime runtime is handed to a reaper
   thread for destruction, keeping teardown off the packet-processing path.

The reload and reaper threads use the CPU affinity available to the unpinned
controller thread, captured before packet workers pin themselves. The OS can
schedule this background work on any CPU in that allowed set, and it runs with
the normal `SCHED_OTHER` policy. No housekeeping cores need to be listed in the
TOML configuration. CPUs excluded by the process or container affinity remain
unavailable to these threads.

Each worker stages and activates independently. During rollout, workers that
have switched may run the new component while others still run the old one. A
failed candidate leaves that worker on its existing component and is reported
in the logs; it does not force a pipeline restart.

## Availability and state

The handoff keeps the Wabrix worker and AF_XDP sockets running, avoiding the
service interruption that a dataplane restart would cause. Plugins that need
state continuity opt into the shared-state API and declare a fixed-layout
payload. The host maps that same backing region into both engine instances; it
does not serialize or restore a state snapshot. Plugins that do not attach a
shared region start with instance-local state after reload.

The replacement attaches to the same shared backing as the old instance. For
matching layouts, state is already present when the candidate is installed. For
an append-only schema change, the candidate initializes appended fields at the
switchover boundary; the host updates the header only after the callback
succeeds.

`inspector_engine` keeps its bypass setting and telemetry counters in shared
memory. The replacement still reads `src_mac` and `dst_mac` from its newly
supplied startup data and recalculates its timing scale from the current TSC
frequency. A schema change is accepted only when the new plugin declares the
old schema compatible and its upgrade callback initializes appended fields.
Reordering or changing existing fields requires a separate migration contract.

The reload is per worker rather than a globally atomic rollout. If all workers
must use one policy version at the same instant, this mechanism alone does not
provide that guarantee; use version-aware policy behavior or coordinate the
rollout at a higher level.

## Operational checklist

- Build and validate the replacement component against the Wabrix WIT ABI.
- Deploy it where every worker can read it, preferably at an immutable,
  versioned path.
- Change only the Wasm path and intended live plugin data in the target config.
- Apply the config and watch worker logs for activation or staging failures.
- Confirm stateful plugins attach a compatible shared-state layout, or verify
  that they can tolerate instance-local state after reload.
