The lifecycle of a worker thread and the lifecycle of the pipelines it executes must be decoupled but highly cooperative. Workers are persistent execution engines pinned to physical cores, while pipelines are dynamic workloads that transition through states (Running, Paused, Stopping) via the lock-free control plane.

Because the architecture leverages xsk for direct AF_XDP socket management, the worker must precisely orchestrate the hardware rings and the thread-local FrameRegistry during any pipeline state transition to prevent memory leaks or ring starvation.

Here is the architectural approach for handling this interaction.
The Worker Lifecycle

The worker acts as the persistent host for one or more pipelines. Its lifecycle is tied to the physical allocation of resources.

*    Initialization: The worker thread spawns, sets its CPU core affinity, and initializes its thread-local FrameRegistry. It receives exclusive ownership of the XSocketRx and XSocketTx components required for its assigned pipelines.

*    Execution Loop: The worker enters an infinite polling loop. It iterates over its assigned pipelines, checking their current operational state via the arc-swap RCU configuration.

*    Global Teardown: Upon receiving a global shutdown signal, the worker breaks the loop. It must explicitly clear the thread-local FrameRegistry pointer, drop the pipeline runners (releasing the xsk hardware rings), yield to the kernel for driver cleanup, and finally drop the FrameRegistry.

The Pipeline Lifecycle (Managed by the Worker)

The pipeline lifecycle dictates how the worker interacts with the DAG and the AF_XDP rings on any given iteration. The state transitions are pushed via the control plane and read atomically by the worker.

* Running: The worker executes the standard fast-path. It allocates frames, replenishes the Fill ring, drains the RX ring, traverses the node graph, pushes to the TX ring, and drains the Completion ring. 
* Paused: The pipeline is active but suspended. The worker stops draining the RX ring and skips graph traversal. Crucially, the worker must continue draining the Completion ring and processing SystemBus returns. If a paused pipeline ignores returns, it will starve the FrameRegistry of other active workers sharing the same UMEM.
* Stopping: The pipeline begins graceful termination. The worker stops replenishing the Fill ring but continues to drain the RX, TX, and Completion rings until no frames are left in flight. Once the rings are empty, the pipeline is safely removed from the worker's rotation.

Code Implementation Strategy

Below is a representation of how the worker loop should evaluate pipeline states. Notice that system-level frame maintenance happens regardless of the pipeline's individual state.

    loop {
        // - Check global shutdown signal
        if shutdown.load(Ordering::Acquire) {
        break;
        }
    
        // - Process cross-worker frame returns from the SystemBus
        // - This must happen continuously so paused pipelines do not starve others
        frame_registry.process_returns();
    
        for runner in &mut runners {
            // - Load the hot configuration atomically
            let config = runner.pipeline_rcu.load();
            
            match config.lifecycle {
                PipelineLifecycle::Running => {
                    // - Execute full data-plane traversal
                    // - Replenish Fill ring
                    // - Drain RX ring
                    // - Process DAG
                    // - Flush TX ring
                    let (packets, _status) = runner.run_iteration();
                    
                    // - Drain Completion ring and return frames
                    runner.drain_completions();
                },
                PipelineLifecycle::Paused => {
                    // - Skip RX drain and DAG traversal
                    // - Keep Completion ring drained to free locked frames
                    runner.drain_completions();
                },
                PipelineLifecycle::Stopping => {
                    // - Execute flush sequence
                    // - Do not replenish Fill ring
                    // - Drain all remaining in-flight frames from RX and TX
                    runner.execute_flush_sequence();
                    
                    // - Mark runner for removal if completely flushed
                }
            }
        }
    }



To address idle CPU power consumption without breaking the lock-free, statically partitioned architecture of the dataplane, the worker threads must implement an adaptive polling strategy. Because AF_XDP relies on busy-polling by default, a naive loop {} will consume 100% of the pinned CPU core even when no packets are arriving.

Here is the architectural approach to reduce power consumption while maintaining the static worker model.
Multi-Stage Adaptive Backoff

The execution loop of the worker must track consecutive iterations where no work was performed (no packets received, no timers fired, no control events processed). As the idle streak grows, the worker progressively yields execution resources.

*    Active Polling: When traffic is flowing, the loop runs unhindered. If a pipeline processes a full batch, the loop immediately repeats to drain the hardware ring as fast as possible.

*    Spin Loop Hinting: After a small threshold of empty iterations, the loop executes std::hint::spin_loop(). This emits a specific instruction (e.g., PAUSE on x86) that reduces the core's power consumption and thermal output without yielding the thread context to the operating system scheduler.

*    OS Yielding: If the idle streak continues, the loop executes std::thread::yield_now(). This explicitly hands control back to the Linux scheduler, allowing the CPU core to transition into a lower C-state (power state) if no other tasks are runnable.

*    Micro-Sleeping: For prolonged idle periods, the thread can execute std::thread::sleep(std::time::Duration::from_micros(X)). This puts the thread to sleep, drastically reducing power consumption. The sleep duration must be carefully bounded to match your maximum acceptable latency tolerance upon the arrival of the next packet.

AF_XDP Wakeup Semantics

If power efficiency is a critical design goal, the AF_XDP socket configuration should leverage the XDP_USE_NEED_WAKEUP flag.

When this flag is enabled during socket creation, the kernel and the driver coordinate to avoid unnecessary wakes. If the RX ring is empty, the userspace thread can block using the standard poll() or epoll() system calls on the socket's file descriptor. The kernel will only wake the thread when new packets are placed in the RX ring.

This sacrifices absolute peak microsecond latency (due to the syscall and context switch overhead to wake the thread) in exchange for near-zero CPU utilization during idle periods.
Interaction with Pipeline Lifecycle

The adaptive backoff must integrate tightly with the PipelineLifecycle and the SystemBus.

*    Paused State Optimization: If all pipelines assigned to a worker transition to PipelineLifecycle::Paused, the worker knows no fast-path RX polling or DAG traversal is required. The worker can immediately transition into a deep micro-sleeping state.

*    SystemBus Maintenance: During any backoff, yield, or sleep state, the worker must guarantee it periodically wakes to execute frame_registry.process_returns() and drain the TX Completion rings. Failing to do this during a low-power state will strand frame ownership and starve the SystemBus, causing active workers to exhaust their UMEM partitions.

*    State Transitions: The worker must check the RCU configuration (pipeline_rcu.load()) immediately upon waking from a sleep or yield to detect if a pipeline has transitioned from Paused back to Running, or if a shutdown event has been triggered.