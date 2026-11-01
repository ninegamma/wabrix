use crate::{PipelineStats, XSocketRegistry};
use core_affinity::CoreId;
use crossbeam_channel::{Receiver, Sender};
use log::info;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use tokio::task::JoinHandle;
use wabrix_base::frame_registry::FrameRegistry;
use wabrix_base::system::PlatformData;
use wabrix_base::telemetry::TelemetryRegistry;
use wabrix_base::ControlEvent;
#[cfg(feature = "wasm")]
use wabrix_wasm::WasmReloadService;
use xsk_core::Umem;

/// Capture the CPUs available to an ordinary, unpinned thread before any
/// packet worker applies its per-thread CPU affinity.
#[cfg(all(feature = "wasm", target_os = "linux"))]
fn generic_thread_cpu_affinity() -> anyhow::Result<(Vec<usize>, usize)> {
    let mut mask_size = std::mem::size_of::<libc::cpu_set_t>();

    // Linux can be built with a CPU mask larger than libc's fixed cpu_set_t.
    // Grow the buffer when the kernel reports that the initial mask is too small.
    loop {
        let word_size = std::mem::size_of::<libc::c_ulong>();
        let word_count = mask_size.div_ceil(word_size);
        let mut mask = vec![0 as libc::c_ulong; word_count];
        let actual_mask_size = mask.len() * word_size;
        let result = unsafe {
            libc::sched_getaffinity(
                0,
                actual_mask_size,
                mask.as_mut_ptr().cast::<libc::cpu_set_t>(),
            )
        };

        if result == 0 {
            let cpus = mask
                .iter()
                .enumerate()
                .flat_map(|(word_index, word)| {
                    (0..libc::c_ulong::BITS as usize).filter_map(move |bit| {
                        (word & (1 << bit) != 0)
                            .then_some(word_index * libc::c_ulong::BITS as usize + bit)
                    })
                })
                .collect::<Vec<_>>();

            if cpus.is_empty() {
                anyhow::bail!("the current thread has no CPUs available in its affinity mask");
            }

            return Ok((cpus, actual_mask_size));
        }

        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINVAL) && mask_size < 1024 * 1024 {
            mask_size *= 2;
            continue;
        }

        return Err(error).map_err(Into::into);
    }
}

#[cfg(all(feature = "wasm", not(target_os = "linux")))]
fn generic_thread_cpu_affinity() -> anyhow::Result<(Vec<usize>, usize)> {
    let cpus = core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .map(|core| core.id)
        .collect::<Vec<_>>();
    if cpus.is_empty() {
        anyhow::bail!("could not determine CPUs available to generic threads");
    }
    Ok((cpus, 0))
}

pub struct WabrixSystem {
    pub(crate) xsocket_registry: XSocketRegistry,
    /// Indexed by worker_id (0..num_workers).
    pub(crate) frame_registries: Vec<Option<FrameRegistry>>,
    pub telemetry_registry: Arc<TelemetryRegistry>,
    /// Indexed by worker_id (0..num_workers).
    pub(crate) worker_runners:
        Vec<Option<(CoreId, Option<bool>, Vec<crate::pipeline::PipelineRunner>)>>,
    pub(crate) platform_data: PlatformData,
    pub shared_state: Arc<crate::config::hot_config::SharedSystemState>,
    /// All live UMEM handles indexed by umem_id (0..MAX_UMEM). None = not used.
    pub umems: Vec<Option<Arc<Umem>>>,
    pub(crate) control_rx: Option<Receiver<Arc<ControlEvent>>>,
    pub(crate) control_senders: Vec<Sender<Arc<ControlEvent>>>,
}

impl WabrixSystem {
    /// returns handles of all spawned worker threads, telemetry logger thread, and dst port stats
    /// consumes itself (WabrixSystem)
    #[allow(clippy::type_complexity)]
    pub fn spawn_workers(
        mut self,
        shutdown: Arc<AtomicBool>,
        workers_completed: Arc<AtomicUsize>,
        is_one_shot: bool, // ipblower's one-shot mode
    ) -> anyhow::Result<Vec<JoinHandle<Result<Vec<PipelineStats>, String>>>> {
        // Capture the controller thread's allowed CPUs before worker threads
        // pin themselves to isolated packet-processing cores. Background Wasm
        // work restores this mask so the OS can schedule it like generic work.
        #[cfg(feature = "wasm")]
        let generic_cpu_affinity = generic_thread_cpu_affinity()?;
        #[cfg(feature = "wasm")]
        info!(
            "CPUs allowed for Wasm housekeeping/background threads: {:?}",
            generic_cpu_affinity.0
        );
        #[cfg(feature = "wasm")]
        let wasm_reload_service =
            WasmReloadService::new(generic_cpu_affinity.clone()).map_err(anyhow::Error::msg)?;
        #[cfg(feature = "wasm")]
        info!("Started one shared Wasm staging worker and runtime reaper");

        // --- SPAWN WORKER THREADS ---

        info!("Spawning pipeline worker threads...");
        let mut handles = vec![];
        let mut control_rx = self.control_rx.take();
        let mut control_senders = std::mem::take(&mut self.control_senders);

        for (worker_id, entry) in self.worker_runners.into_iter().enumerate() {
            let (core_id, no_turbo, runners) = match entry {
                Some(v) => v,
                None => continue,
            };

            let num_pipelines = runners.len();
            let shutdown_clone = Arc::clone(&shutdown);
            let workers_completed_clone = Arc::clone(&workers_completed);
            let worker_control_rx = control_rx.take();
            let worker_control_senders = if worker_control_rx.is_some() {
                std::mem::take(&mut control_senders)
            } else {
                Vec::new()
            };

            // Get the frame registry for this worker
            let frame_registry = self.frame_registries[worker_id]
                .take()
                .unwrap_or_else(|| panic!("Frame registry for worker {} not found", worker_id));

            // Spawn the worker with all its pipelines and frame registry
            let handle = crate::pipeline::spawn_worker_pipelines(
                runners,
                crate::pipeline::WorkerPipelineSpawnConfig {
                    core_id,
                    no_turbo,
                    frame_registry,
                    shutdown: shutdown_clone,
                    workers_completed: workers_completed_clone,
                    tsc_frequency: self.platform_data.tsc_frequency,
                    is_one_shot,
                    control_rx: worker_control_rx,
                    control_senders: worker_control_senders,
                    #[cfg(feature = "wasm")]
                    wasm_reload_service: wasm_reload_service.clone(),
                },
            );

            handles.push(handle);

            info!(
                "Spawned worker {} on core {} with {} pipeline(s)",
                worker_id, core_id.id, num_pipelines
            );
        }

        Ok(handles)
    }
}
