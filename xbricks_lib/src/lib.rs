extern crate alloc;
pub mod allocators;
pub mod config;
pub mod xsocket;
pub mod system_builder;
pub mod nodes;
pub mod pipeline;
//pub(crate) mod telemetry_logger;
pub mod toml_parser;
pub(crate) mod system_runner;
mod telemetry_logger;

// Include the generated BPF skeleton code
pub mod xscrubber_skel {
    include!(concat!(env!("OUT_DIR"), "/xscrubber.skel.rs"));
}

// Re-export commonly used types
pub(crate) use xsocket::XSocket;
pub use xbricks_base::interface::batch::Batch;
pub(crate) use crate::system_builder::XSocketRegistry;
pub use crate::pipeline::{spawn_worker_pipelines, PipelineFactory, PipelineRunner, PipelineStats};
pub const MAX_BATCH_SIZE: usize = 512;

/// Trait for any node that acts as a packet source (ingress) in the DAG.
pub trait BatchRx {
    /// Attempts to pull a burst of packets from the source and populate the provided batch.
    ///
    /// # Arguments
    /// * `batch` - A mutable reference to a Batch to be filled.
    ///             The batch should be cleared before calling this if you want a fresh burst.
    /// * `max_count` - Maximum number of packets to receive in this batch.
    ///
    /// # Returns
    /// * `usize` - The number of packets successfully added to the batch.
    /// * `anyhow::Result<()>` - Error context if the hardware/socket access fails.
    fn receive_into_batch(&mut self, batch: &mut Batch, max_count: usize) -> anyhow::Result<usize>;

    /// replenish the fill queue, needed e.g. for warmup at thread start
    fn replenish(&mut self, requested_count: usize) -> anyhow::Result<()>;

    /// Returns the ID or Name of the source for telemetry/scheduling purposes.
    fn source_id(&self) -> &String;

    #[cfg(feature = "profiling")]
    fn get_replenish_avg_cycles(&self) -> u64 {
        0
    }

    #[cfg(feature = "profiling")]
    fn get_replenish_total_cycles(&self) -> u64 {
        0
    }

    #[cfg(feature = "profiling")]
    fn get_replenish_calls(&self) -> u64 {
        0
    }
}

pub trait BatchTx {
    /// Consumes packets from the batch and transmits them.
    ///
    /// # Arguments
    /// * `batch` - The batch containing packets to send.
    ///             Successfully sent packets are REMOVED from the batch.
    ///             Unsent packets (e.g. if TX ring is full) REMAIN in the batch.
    ///
    /// # Returns
    /// * `usize` - The number of packets successfully transmitted.
    fn transmit_batch(&mut self, batch: &mut Batch) -> anyhow::Result<usize>;

    /// Returns the ID for telemetry.
    fn sink_id(&self) -> String;
}

pub fn probe_kernel_fds(stage: &str) {
    let pid = std::process::id();
    log::debug!("🔍 --- Kernel FD Audit: {} ---", stage);

    let fd_dir = format!("/proc/{}/fd", pid);
    let Ok(entries) = std::fs::read_dir(&fd_dir) else {
        log::debug!("Could not read {}", fd_dir);
        return;
    };

    let mut bpf_count = 0;
    let mut socket_count = 0;
    let mut anon_count = 0;

    for entry in entries.flatten() {
        if let Ok(target) = std::fs::read_link(entry.path()) {
            let target_str = target.to_string_lossy();
            let fd_num = entry.file_name().to_string_lossy().to_string();

            // Filter for the resources that cause EBUSY
            if target_str.contains("socket:") {
                log::debug!("⚠️  Lingering Socket FD: {} -> {}", fd_num, target_str);
                socket_count += 1;
            } else if target_str.contains("bpf-map") || target_str.contains("bpf-prog") {
                log::debug!("⚠️  Lingering BPF FD: {} -> {}", fd_num, target_str);
                bpf_count += 1;
            } else if target_str.contains("anon_inode:umem") || target_str.contains("anon_inode") {
                // Ignore standard epoll/eventfds, look for suspicious networking inodes
                if !target_str.contains("eventfd") && !target_str.contains("epoll") {
                    log::debug!("⚠️  Lingering Anon Inode FD: {} -> {}", fd_num, target_str);
                    anon_count += 1;
                }
            }
        }
    }

    log::debug!(
        "📊 FD Summary | Sockets: {}, BPF Maps/Progs: {}, Suspicious Inodes: {}",
        socket_count, bpf_count, anon_count
    );
    log::debug!("-----------------------------------------");
}
