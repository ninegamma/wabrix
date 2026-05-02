use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use core_affinity::CoreId;
use log::{info};
use tokio::task::JoinHandle;
use xsk_rs::Umem;
use xbricks_base::frame_registry::{FrameRegistry, MAX_UMEM};
use crate::{PipelineStats, XSocketRegistry};
use xbricks_base::system::PlatformData;
use xbricks_base::telemetry::TelemetryRegistry;

pub struct XBricksSystem {
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
}

impl XBricksSystem {
    /// returns handles of all spawned worker threads, telemetry logger thread, and dst port stats
    /// consumes itself (XBricksSystem)
    pub fn spawn_workers(
        mut self,
        shutdown: Arc<AtomicBool>,
        workers_completed: Arc<AtomicUsize>,
    ) -> anyhow::Result<Vec<JoinHandle<Result<Vec<PipelineStats>, String>>>> 
    {
        // --- SPAWN WORKER THREADS ---

        info!("Spawning pipeline worker threads...");
        let mut handles = vec![];
        for (worker_id, entry) in self.worker_runners.into_iter().enumerate() {
            let (core_id, runners) = match entry {
                Some(v) => v,
                None => continue,
            };

            let num_pipelines = runners.len();
            let shutdown_clone = Arc::clone(&shutdown);
            let workers_completed_clone = Arc::clone(&workers_completed);

            // Get the frame registry for this worker
            let frame_registry = self
                .frame_registries[worker_id]
                .take()
                .expect(&format!("Frame registry for worker {} not found", worker_id));

            // Spawn the worker with all its pipelines and frame registry
            let handle = crate::pipeline::spawn_worker_pipelines(
                runners,
                core_id,
                frame_registry,
                shutdown_clone,
                workers_completed_clone,
                &self.platform_data,
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
