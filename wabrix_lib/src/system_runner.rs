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
use xsk_core::Umem;

pub struct WabrixSystem {
    pub(crate) xsocket_registry: XSocketRegistry,
    /// Indexed by worker_id (0..num_workers).
    pub(crate) frame_registries: Vec<Option<FrameRegistry>>,
    pub telemetry_registry: Arc<TelemetryRegistry>,
    /// Indexed by worker_id (0..num_workers).
    pub(crate) worker_runners: Vec<Option<(CoreId, Vec<crate::pipeline::PipelineRunner>)>>,
    pub(crate) platform_data: PlatformData,
    pub shared_state: Arc<crate::config::hot_config::SharedSystemState>,
    /// All live UMEM handles indexed by umem_id (0..MAX_UMEM). None = not used.
    pub umems: Vec<Option<Arc<Umem>>>,
    pub(crate) control_rx: Option<Receiver<Arc<ControlEvent>>>,
    pub(crate) control_senders: Vec<Sender<Arc<ControlEvent>>>,
}

impl WabrixSystem {
    /// returns handles of all spawned worker threads, telemetry logger thread, and dst port stats
    /// consumes itself (XBricksSystem)
    #[allow(clippy::type_complexity)]
    pub fn spawn_workers(
        mut self,
        shutdown: Arc<AtomicBool>,
        workers_completed: Arc<AtomicUsize>,
        is_one_shot: bool, // ipblower's one-shot mode
    ) -> anyhow::Result<Vec<JoinHandle<Result<Vec<PipelineStats>, String>>>> {
        // --- SPAWN WORKER THREADS ---

        info!("Spawning pipeline worker threads...");
        let mut handles = vec![];
        let mut control_rx = self.control_rx.take();
        let mut control_senders = std::mem::take(&mut self.control_senders);

        for (worker_id, entry) in self.worker_runners.into_iter().enumerate() {
            let (core_id, runners) = match entry {
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
                    frame_registry,
                    shutdown: shutdown_clone,
                    workers_completed: workers_completed_clone,
                    tsc_frequency: self.platform_data.tsc_frequency,
                    is_one_shot,
                    control_rx: worker_control_rx,
                    control_senders: worker_control_senders,
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
