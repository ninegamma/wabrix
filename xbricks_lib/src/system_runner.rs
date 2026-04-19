use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use core_affinity::CoreId;
use log::{info};
use tokio::task::JoinHandle;
use xbricks_base::frame_registry::FrameRegistry;
use crate::{PipelineStats, XSocketRegistry};
use xbricks_base::system::PlatformData;
use xbricks_base::telemetry::TelemetryRegistry;

#[derive(Default)]
pub struct XBricksSystem {
    pub(crate) xsocket_registry: XSocketRegistry,
    /// Map: worker_id -> FrameRegistry
    pub(crate) frame_registries: HashMap<usize, FrameRegistry>,
    pub telemetry_registry: Arc<TelemetryRegistry>,
    /// Group pipelines by worker
    /// Map: worker_id -> (CoreId, Vec<PipelineRunner>)
    /// worker_idx is used by each node to look up its specific queue_id from its queues array
    pub(crate) worker_runners: HashMap<usize, (CoreId, Vec<crate::pipeline::PipelineRunner>)>,
    pub(crate) platform_data: PlatformData,
    pub(crate) dst_port_stats: Vec<Arc<crate::nodes::DstPortStats>>,
    pub shared_state: Arc<crate::config::hot_config::SharedSystemState>,
}

impl XBricksSystem {
    /// returns handles of all spawned worker threads, telemetry logger thread, and dst port stats
    /// consumes itself (XBricksSystem)
    pub fn spawn_workers(
        mut self,
        b_with_telemetry: bool,
        shutdown: Arc<AtomicBool>,
        workers_completed: Arc<AtomicUsize>,
    ) -> anyhow::Result<(
        Vec<JoinHandle<Result<Vec<PipelineStats>, String>>>,
        Option<std::thread::JoinHandle<()>>,
        Vec<Arc<crate::nodes::DstPortStats>>,
    )> {
        // --- SPAWN TELEMETRY LOGGER ---

        let telemetry_handle = if b_with_telemetry {
            Some(self.spawn_telemetry(shutdown.clone())?)
        } else {
            None
        };

        // --- SPAWN WORKER THREADS ---

        info!("Spawning pipeline worker threads...");
        let mut handles = vec![];
        for (worker_id, (core_id, runners)) in self.worker_runners {
            // Find the worker config to get core affinity

            let num_pipelines = runners.len();
            let shutdown_clone = Arc::clone(&shutdown);
            let workers_completed_clone = Arc::clone(&workers_completed);

            // Get the frame registry for this worker
            let frame_registry = self
                .frame_registries
                .remove(&worker_id)
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

        let dst_port_stats = self.dst_port_stats.clone();
        Ok((handles, telemetry_handle, dst_port_stats))
    }

    fn spawn_telemetry(&mut self, shutdown: Arc<AtomicBool>) -> anyhow::Result<std::thread::JoinHandle<()>> {
        // --- SPAWN TELEMETRY LOGGER ---

        let telemetry_interval_secs = 5; // Print telemetry every 5 seconds
        info!(
            "Spawned telemetry logger thread with {} second interval",
            telemetry_interval_secs
        );

        let telemetry_handle = crate::telemetry_logger::spawn_telemetry_logger(
            Arc::clone(&self.telemetry_registry),
            telemetry_interval_secs,
            Arc::clone(&shutdown),
        );
        Ok(telemetry_handle)
    }
}
