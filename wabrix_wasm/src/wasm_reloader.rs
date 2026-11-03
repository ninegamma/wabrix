use crate::wasm_engine::{WasmEngine, WasmRuntime};
use crate::wasm_manager::SharedStateRegion;
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use wabrix_api::{HotWasmPayload, StandardOpcode};
use wabrix_base::system::PlatformData;
use wabrix_base::telemetry::TelemetryMetrics;
use wabrix_base::{ControlEvent, ControlEventResult, Node, NodeId};

/// CPU mask captured from an unpinned controller thread before workers start.
#[derive(Clone)]
struct BackgroundCpuAffinity {
    allowed_cpus: Vec<usize>,
    mask_size: usize,
}

/// Apply generic-thread affinity and scheduling before doing Wasm work.
fn configure_background_thread(affinity: Option<&BackgroundCpuAffinity>) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        // Set affinity before scheduling so background setup uses the generic
        // CPU mask even when the caller's policy or affinity changes later.
        if let Some(affinity) = affinity {
            let word_size = std::mem::size_of::<libc::c_ulong>();
            let required_words = affinity
                .allowed_cpus
                .iter()
                .max()
                .map(|cpu| cpu / libc::c_ulong::BITS as usize + 1)
                .unwrap_or(0);
            let word_count = affinity.mask_size.div_ceil(word_size).max(required_words);
            let mask_size = word_count * word_size;
            let mut mask = vec![0 as libc::c_ulong; word_count];
            for &cpu in &affinity.allowed_cpus {
                let word = cpu / libc::c_ulong::BITS as usize;
                let bit = cpu % libc::c_ulong::BITS as usize;
                mask[word] |= (1 as libc::c_ulong) << bit;
            }

            let result = unsafe {
                libc::sched_setaffinity(0, mask_size, mask.as_ptr().cast::<libc::cpu_set_t>())
            };
            if result != 0 {
                return Err(format!(
                    "could not restore generic CPU affinity: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }

        let param = libc::sched_param { sched_priority: 0 };
        let scheduling_result =
            unsafe { libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_OTHER, &param) };
        if scheduling_result != 0 {
            return Err(format!(
                "could not set generic SCHED_OTHER policy: {}",
                std::io::Error::from_raw_os_error(scheduling_result)
            ));
        }
    }

    #[cfg(not(target_os = "linux"))]
    let _ = affinity;

    Ok(())
}

/// One queued reload request for a specific Wasm node. Each node has a
/// one-element mailbox, so a newer queued request replaces its stale predecessor.
struct StagingJob {
    spec: WasmReloadSpec,
    wasm_file_path: String,
    hot_node_data: HashMap<String, String>,
    queued_at: Instant,
    generation: u64,
    latest_generation: Arc<AtomicU64>,
    ready: Sender<Result<WasmEngine, String>>,
}

/// A retired runtime and its label, so the background reaper can report drop time.
struct RetiredRuntime {
    runtime: WasmRuntime,
    node_id: String,
    wasm_file_path: String,
    queued_at: Instant,
}

/// A lock-free, one-job mailbox registered for each Wasm node at worker startup.
struct ReloadMailbox {
    job_tx: Sender<StagingJob>,
    job_rx: Receiver<StagingJob>,
    scheduled: AtomicBool,
}

impl ReloadMailbox {
    fn new() -> Arc<Self> {
        let (job_tx, job_rx) = bounded(1);
        Arc::new(Self {
            job_tx,
            job_rx,
            scheduled: AtomicBool::new(false),
        })
    }

    fn submit(
        self: &Arc<Self>,
        wake_tx: &Sender<Arc<ReloadMailbox>>,
        job: StagingJob,
    ) -> Result<(), String> {
        let mut job = job;
        loop {
            match self.job_tx.try_send(job) {
                Ok(()) => break,
                Err(TrySendError::Full(returned_job)) => {
                    // Replace the queued request. If the stager already took
                    // it, this simply observes an empty mailbox and retries.
                    let _ = self.job_rx.try_recv();
                    job = returned_job;
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err("Wasm staging mailbox is disconnected".to_string());
                }
            }
        }

        if !self.scheduled.swap(true, Ordering::AcqRel)
            && wake_tx.try_send(Arc::clone(self)).is_err()
        {
            self.scheduled.store(false, Ordering::Release);
            return Err("Wasm staging worker is unavailable".to_string());
        }

        Ok(())
    }
}

/// Process-wide Wasm background workers. The single staging thread and the
/// runtime reaper are started before packet workers enter their real-time loop.
#[derive(Clone)]
pub struct WasmReloadService {
    wake_tx: Sender<Arc<ReloadMailbox>>,
    retire_tx: Sender<RetiredRuntime>,
}

impl WasmReloadService {
    pub fn new(affinity: (Vec<usize>, usize)) -> Result<Self, String> {
        let affinity = Arc::new(BackgroundCpuAffinity {
            allowed_cpus: affinity.0,
            mask_size: affinity.1,
        });

        // The wake queue has at most one token per registered Wasm node because
        // each mailbox's scheduled flag suppresses duplicate notifications.
        let (wake_tx, wake_rx) = crossbeam_channel::unbounded::<Arc<ReloadMailbox>>();
        let (stager_ready_tx, stager_ready_rx) = bounded(1);
        let stager_affinity = affinity.clone();
        std::thread::Builder::new()
            .name("wasm-reload-stager".to_string())
            .spawn(move || {
                if let Err(error) = configure_background_thread(Some(&stager_affinity)) {
                    let _ = stager_ready_tx.send(Err(error));
                    return;
                }
                let _ = stager_ready_tx.send(Ok(()));

                while let Ok(mailbox) = wake_rx.recv() {
                    loop {
                        match mailbox.job_rx.try_recv() {
                            Ok(job) => {
                                if job.latest_generation.load(Ordering::Acquire) != job.generation {
                                    continue;
                                }

                                let node_id = job.spec.id.to_string();
                                let wasm_file_path = job.wasm_file_path.clone();
                                let queue_wait_ns = job.queued_at.elapsed().as_nanos();
                                let staging_started = Instant::now();
                                let staged =
                                    job.spec.instantiate(job.wasm_file_path, job.hot_node_data);
                                let staging_ns = staging_started.elapsed().as_nanos();
                                let is_current =
                                    job.latest_generation.load(Ordering::Acquire) == job.generation;
                                let outcome = if !is_current {
                                    "superseded"
                                } else if staged.is_ok() {
                                    "ready"
                                } else {
                                    "failed"
                                };
                                log::info!(
                                    "NODE {}: staged Wasm component {}: queue_wait={} ns, staging={} ns, request_to_ready={} ns, outcome={}",
                                    node_id,
                                    wasm_file_path,
                                    queue_wait_ns,
                                    staging_ns,
                                    queue_wait_ns.saturating_add(staging_ns),
                                    outcome
                                );
                                if is_current {
                                    // If superseded concurrently, send returns
                                    // the candidate here so teardown stays off-runner.
                                    let _ = job.ready.send(staged);
                                }
                                // A superseded candidate is dropped on this
                                // background thread, never by a packet worker.
                            }
                            Err(TryRecvError::Disconnected) => {
                                mailbox.scheduled.store(false, Ordering::Release);
                                break;
                            }
                            Err(TryRecvError::Empty) => {
                                mailbox.scheduled.store(false, Ordering::Release);
                                if mailbox.job_rx.is_empty()
                                    || mailbox.scheduled.swap(true, Ordering::AcqRel)
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to start Wasm staging worker: {error}"))?;
        stager_ready_rx
            .recv()
            .map_err(|_| "Wasm staging worker exited before setup completed".to_string())??;

        let (retire_tx, retire_rx) = bounded::<RetiredRuntime>(2);
        let (reaper_ready_tx, reaper_ready_rx) = bounded(1);
        let reaper_affinity = affinity;
        std::thread::Builder::new()
            .name("wasm-runtime-reaper".to_string())
            .spawn(move || {
                if let Err(error) = configure_background_thread(Some(&reaper_affinity)) {
                    let _ = reaper_ready_tx.send(Err(error));
                    return;
                }
                let _ = reaper_ready_tx.send(Ok(()));
                while let Ok(retired) = retire_rx.recv() {
                    let RetiredRuntime {
                        runtime,
                        node_id,
                        wasm_file_path,
                        queued_at,
                    } = retired;
                    let queue_wait_ns = queued_at.elapsed().as_nanos();
                    let destruction_started = Instant::now();
                    drop(runtime);
                    log::info!(
                        "NODE {}: destroyed retired Wasm component {} in {} ns (reaper queue wait {} ns)",
                        node_id,
                        wasm_file_path,
                        destruction_started.elapsed().as_nanos(),
                        queue_wait_ns
                    );
                }
            })
            .map_err(|error| format!("failed to start Wasm runtime reaper: {error}"))?;
        reaper_ready_rx
            .recv()
            .map_err(|_| "Wasm runtime reaper exited before setup completed".to_string())??;

        Ok(Self { wake_tx, retire_tx })
    }

    fn new_mailbox(&self) -> Arc<ReloadMailbox> {
        ReloadMailbox::new()
    }

    fn submit(&self, mailbox: &Arc<ReloadMailbox>, job: StagingJob) -> Result<(), String> {
        mailbox.submit(&self.wake_tx, job)
    }
}

/// Immutable inputs needed to build a replacement Wasm instance off the runner thread.
/// It captures node identity, memory/runtime handles, and base configuration; each reload
/// supplies its target module path and current hot node data separately.
#[derive(Clone)]
pub(crate) struct WasmReloadSpec {
    pub(crate) id: NodeId,
    pub(crate) pipeline_name: String,
    pub(crate) worker_id: usize,
    pub(crate) worker_index: usize,
    pub(crate) tsc_frequency: u64,
    pub(crate) manager: std::sync::Arc<crate::wasm_manager::WasmManager>,
    pub(crate) base_node_data: HashMap<String, String>,
    pub(crate) routing_table: Vec<NodeId>,
    pub(crate) telemetry_metrics: TelemetryMetrics,
    pub(crate) umem_id: u8,
    pub(crate) mailbox_offset: u32,
    /// Reused by every staged candidate for this logical plugin instance.
    pub(crate) shared_state: std::sync::Arc<SharedStateRegion>,
}

impl WasmReloadSpec {
    pub(crate) fn instantiate(
        &self,
        wasm_file_path: String,
        hot_node_data: HashMap<String, String>,
    ) -> Result<WasmEngine, String> {
        let mut node_data = self.base_node_data.clone();
        node_data.extend(hot_node_data.clone());
        let platform_data = PlatformData {
            tsc_frequency: self.tsc_frequency,
        };

        WasmEngine::new_with_shared_state(
            self.id.clone(),
            self.pipeline_name.clone(),
            self.worker_id,
            self.worker_index,
            self.manager.clone(),
            &wasm_file_path,
            self.routing_table.clone(),
            self.telemetry_metrics.clone(),
            self.umem_id,
            node_data,
            self.base_node_data.clone(),
            hot_node_data,
            &platform_data,
            self.mailbox_offset,
            self.shared_state.clone(),
        )
        .map(|mut candidate| {
            // A staged candidate is temporary until the runner installs its runtime.
            candidate.suppress_final_diagnostics();
            candidate
        })
    }
}

/// One in-flight replacement tracked by the runner-owned reload controller.
struct PendingWasmReload {
    /// Index of the target node in the owning `PipelineRunner`'s node vector.
    node_index: usize,
    /// Receives the staged candidate or its error once background construction finishes.
    ready: Receiver<Result<WasmEngine, String>>,
    generation: u64,
    latest_generation: Arc<AtomicU64>,
}

/// PipelineRunner-owned controller for staging, publishing, and retiring Wasm runtimes.
/// It is only polled at the runner's existing timer/control boundary.
#[derive(Default)]
pub struct WasmReloadController {
    /// Candidate builds still in progress, associated with their runner node slots.
    pending: Vec<PendingWasmReload>,
    /// Shared process-wide background workers.
    service: Option<WasmReloadService>,
    /// A lock-free staging mailbox for each Wasm node in this runner.
    mailboxes: HashMap<usize, Arc<ReloadMailbox>>,
    /// Per-node generation counters used to skip superseded staging jobs.
    latest_generations: HashMap<usize, Arc<AtomicU64>>,
    /// Old runtimes retained temporarily when the bounded reaper queue is full.
    retired_runtimes: Vec<RetiredRuntime>,
}

impl WasmReloadController {
    /// Attaches the prestarted service and registers this runner's Wasm nodes.
    pub fn set_service(&mut self, service: WasmReloadService, nodes: &[Box<dyn Node>]) {
        self.mailboxes.clear();
        for (node_index, node) in nodes.iter().enumerate() {
            let node_any = &**node as &dyn std::any::Any;
            if node_any.is::<WasmEngine>() {
                self.mailboxes.insert(node_index, service.new_mailbox());
            }
        }
        self.service = Some(service);
    }

    #[inline]
    pub fn has_work(&self) -> bool {
        !self.pending.is_empty() || !self.retired_runtimes.is_empty()
    }

    /// Owns the host-side Wasm configuration protocol. The node handles only
    /// plugin ABI calls; compilation and reload lifecycle are controlled here.
    pub fn handle_control_event(
        &mut self,
        node_index: usize,
        engine: &mut WasmEngine,
        event: &ControlEvent,
    ) -> ControlEventResult {
        if engine.is_poisoned() {
            return ControlEventResult::Unsupported;
        }

        if event.opcode != StandardOpcode::UpdateConfiguration as u32 {
            return engine.send_plugin_control_event(event);
        }

        let Ok(payload) = postcard::from_bytes::<HotWasmPayload>(&event.payload) else {
            return engine.send_plugin_control_event(event);
        };
        let wasm_file_path = payload.updates.get("wasm_file_path").cloned();
        let mut plugin_updates = payload.updates;
        plugin_updates.remove("wasm_file_path");

        if let Some(path) = wasm_file_path {
            if path.is_empty() {
                return ControlEventResult::InvalidPayload;
            }

            let Some(service) = self.service.clone() else {
                log::error!(
                    "NODE {}: Wasm reload service was not initialized before packet processing",
                    engine.node_id()
                );
                return ControlEventResult::InvalidPayload;
            };
            let Some(mailbox) = self.mailboxes.get(&node_index).cloned() else {
                log::error!(
                    "NODE {}: Wasm reload mailbox is unavailable",
                    engine.node_id()
                );
                return ControlEventResult::InvalidPayload;
            };

            let mut next_hot_node_data = engine.hot_node_data().clone();
            for (key, value) in &plugin_updates {
                if value.is_empty() {
                    next_hot_node_data.remove(key);
                } else {
                    next_hot_node_data.insert(key.clone(), value.clone());
                }
            }

            log::info!(
                "NODE {}: staging Wasm component {} (currently active: {})",
                engine.node_id(),
                path,
                engine.wasm_file_path()
            );

            let spec = engine.reload_spec();
            let latest_generation = self
                .latest_generations
                .entry(node_index)
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone();
            let generation = latest_generation
                .fetch_add(1, Ordering::AcqRel)
                .wrapping_add(1);
            let (ready_tx, ready_rx) = bounded(1);
            let job = StagingJob {
                spec,
                wasm_file_path: path,
                hot_node_data: next_hot_node_data.clone(),
                queued_at: Instant::now(),
                generation,
                latest_generation: latest_generation.clone(),
                ready: ready_tx,
            };

            if let Err(error) = service.submit(&mailbox, job) {
                log::error!(
                    "NODE {}: could not queue Wasm reload: {}",
                    engine.node_id(),
                    error
                );
                return ControlEventResult::InvalidPayload;
            }

            self.retire_ready_superseded_candidates(node_index);
            self.pending.push(PendingWasmReload {
                node_index,
                ready: ready_rx,
                generation,
                latest_generation,
            });
            engine.set_hot_node_data(next_hot_node_data);

            // Keep the active plugin in sync while the replacement is staging.
            // The candidate receives these values through init-data.
            if !plugin_updates.is_empty() {
                let filtered_payload = HotWasmPayload {
                    updates: plugin_updates,
                };
                if let Ok(bytes) = postcard::to_allocvec(&filtered_payload) {
                    let filtered_event = ControlEvent {
                        address_pattern: event.address_pattern.clone(),
                        opcode: event.opcode,
                        payload: bytes,
                    };
                    let _ = engine.send_plugin_control_event(&filtered_event);
                }
            }

            return ControlEventResult::Handled;
        }

        // A data-only update remains a synchronous plugin control event. Keep the
        // cached reload inputs in sync only when the plugin accepts the update.
        let result = engine.send_plugin_control_event(event);
        if matches!(result, ControlEventResult::Handled) {
            let mut next_hot_node_data = engine.hot_node_data().clone();
            for (key, value) in plugin_updates {
                if value.is_empty() {
                    next_hot_node_data.remove(&key);
                } else {
                    next_hot_node_data.insert(key, value);
                }
            }
            engine.set_hot_node_data(next_hot_node_data);
        }
        result
    }

    /// Removes superseded results that are already available. A still-running
    /// result remains tracked so a candidate can be retired off-runner if it
    /// completes after this reload request.
    fn retire_ready_superseded_candidates(&mut self, node_index: usize) {
        let mut index = 0;
        while index < self.pending.len() {
            if self.pending[index].node_index != node_index {
                index += 1;
                continue;
            }

            match self.pending[index].ready.try_recv() {
                Ok(Ok(candidate)) => {
                    self.pending.swap_remove(index);
                    self.queue_retired_engine(candidate);
                }
                Ok(Err(_)) | Err(TryRecvError::Disconnected) => {
                    self.pending.swap_remove(index);
                }
                Err(TryRecvError::Empty) => index += 1,
            }
        }
    }

    /// Checks completed candidates only when the runner is already polling timers
    /// and control events, never from the per-batch Wasm execution path.
    pub fn poll_ready(&mut self, nodes: &mut [Box<dyn Node>]) {
        self.flush_retired_runtimes();

        let mut index = 0;
        while index < self.pending.len() {
            let result = match self.pending[index].ready.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    let pending = self.pending.swap_remove(index);
                    if pending.latest_generation.load(Ordering::Acquire) == pending.generation {
                        log::error!(
                            "Wasm reload worker for node index {} exited without a result",
                            pending.node_index
                        );
                    }
                    continue;
                }
            };

            let Some(result) = result else {
                index += 1;
                continue;
            };

            let pending = self.pending.swap_remove(index);
            let node_index = pending.node_index;
            if pending.latest_generation.load(Ordering::Acquire) != pending.generation {
                if let Ok(candidate) = result {
                    self.queue_retired_engine(candidate);
                }
                continue;
            }

            match result {
                Ok(mut candidate) => {
                    let Some(engine) = nodes.get_mut(node_index).and_then(|node| {
                        let node_any = &mut **node as &mut dyn std::any::Any;
                        node_any.downcast_mut::<WasmEngine>()
                    }) else {
                        log::error!(
                            "Wasm reload candidate for node index {} has no WasmEngine target",
                            node_index
                        );
                        self.queue_retired_engine(candidate);
                        continue;
                    };

                    let validation_started = Instant::now();
                    let validation_result =
                        WasmEngine::upgrade_shared_state_for_candidate(&mut candidate);
                    let validation_ns = validation_started.elapsed().as_nanos();
                    log::info!(
                        "NODE {}: candidate state validation/upgrade took {} ns",
                        engine.node_id(),
                        validation_ns
                    );
                    if let Err(error) = validation_result {
                        log::error!(
                            "Wasm reload state upgrade failed for node {}; keeping component {} active: {}",
                            engine.node_id(),
                            engine.wasm_file_path(),
                            error
                        );
                        self.queue_retired_engine(candidate);
                        continue;
                    }

                    let version = candidate.wasm_file_path().to_string();
                    let previous_version = engine.wasm_file_path().to_string();
                    let node_id = engine.node_id().to_string();
                    let switchover_started = Instant::now();
                    let old_runtime = engine.install_candidate(candidate);
                    let switchover_ns = switchover_started.elapsed().as_nanos();
                    log::info!(
                        "NODE {}: runtime switchover {} -> {} took {} ns",
                        node_id,
                        previous_version,
                        version,
                        switchover_ns
                    );
                    self.queue_retired_runtime(old_runtime, node_id, previous_version);
                }
                Err(error) => {
                    let active_path = nodes
                        .get_mut(node_index)
                        .and_then(|node| {
                            let node_any = &mut **node as &mut dyn std::any::Any;
                            node_any.downcast_mut::<WasmEngine>()
                        })
                        .map(|engine| engine.wasm_file_path())
                        .unwrap_or("<unknown>");
                    log::error!(
                        "Wasm reload failed for node index {}; keeping component {} active: {}",
                        node_index,
                        active_path,
                        error
                    );
                }
            }
        }

        self.flush_retired_runtimes();
    }

    fn queue_retired_engine(&mut self, engine: WasmEngine) {
        let node_id = engine.node_id().to_string();
        let wasm_file_path = engine.wasm_file_path().to_string();
        self.queue_retired_runtime(engine.into_runtime(), node_id, wasm_file_path);
    }

    fn queue_retired_runtime(
        &mut self,
        runtime: WasmRuntime,
        node_id: String,
        wasm_file_path: String,
    ) {
        let retired = RetiredRuntime {
            runtime,
            node_id,
            wasm_file_path,
            queued_at: Instant::now(),
        };
        if let Some(service) = &self.service {
            let retire_tx = &service.retire_tx;
            match retire_tx.try_send(retired) {
                Ok(()) => return,
                Err(TrySendError::Full(retired) | TrySendError::Disconnected(retired)) => {
                    self.retired_runtimes.push(retired);
                }
            }
        } else {
            self.retired_runtimes.push(retired);
        }
    }

    fn flush_retired_runtimes(&mut self) {
        let Some(service) = &self.service else {
            return;
        };
        let retire_tx = &service.retire_tx;

        let mut pending = Vec::new();
        for retired in self.retired_runtimes.drain(..) {
            match retire_tx.try_send(retired) {
                Ok(()) => {}
                Err(TrySendError::Full(retired) | TrySendError::Disconnected(retired)) => {
                    pending.push(retired);
                }
            }
        }
        self.retired_runtimes = pending;
    }
}
