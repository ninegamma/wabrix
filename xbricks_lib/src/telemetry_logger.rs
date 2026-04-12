use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use log::info;

/// Represents a single telemetry node's metrics
#[derive(Clone)]
pub struct TelemetryMetrics {
    pub node_id: String,
    pub labels: Vec<String>,
    pub counters_pkts: Vec<Arc<AtomicU64>>,
    pub counters_bytes: Vec<Arc<AtomicU64>>,
}

/// Registry to hold references to all telemetry nodes
#[derive(Clone, Default)]
pub struct TelemetryRegistry {
    metrics: Vec<TelemetryMetrics>,
}

impl TelemetryRegistry {
    pub fn new() -> Self {
        Self { metrics: Vec::new() }
    }

    pub fn register(
        &mut self,
        node_id: String,
        labels: Vec<String>,
        counters_pkts: Vec<Arc<AtomicU64>>,
        counters_bytes: Vec<Arc<AtomicU64>>,
    ) {
        self.metrics.push(TelemetryMetrics {
            node_id,
            labels,
            counters_pkts,
            counters_bytes,
        });
    }

    pub fn get_metrics(&self) -> &[TelemetryMetrics] {
        &self.metrics
    }
}

/// TelemetryLogger periodically reads and prints telemetry data
pub struct TelemetryLogger {
    registry: Arc<TelemetryRegistry>,
    interval: Duration,
}

impl TelemetryLogger {
    pub fn new(registry: Arc<TelemetryRegistry>, interval_secs: u64) -> Self {
        Self {
            registry,
            interval: Duration::from_secs(interval_secs),
        }
    }

    /// Run the telemetry logger until shutdown signal
    pub fn run(self, shutdown: Arc<AtomicBool>) {
        info!("TelemetryLogger starting with {}s interval", self.interval.as_secs());

        // Store previous values to calculate deltas
        let mut prev_pkts: Vec<Vec<u64>> = Vec::new();
        let mut prev_bytes: Vec<Vec<u64>> = Vec::new();

        // Initialize previous values
        for metrics in self.registry.get_metrics() {
            let num_buckets = metrics.counters_pkts.len();
            prev_pkts.push(vec![0u64; num_buckets]);
            prev_bytes.push(vec![0u64; num_buckets]);
        }

        loop {
            // Sleep for the interval, checking shutdown signal frequently
            let sleep_chunks = 10; // Check shutdown 10 times per interval
            let chunk_duration = self.interval / sleep_chunks;

            for _ in 0..sleep_chunks {
                if shutdown.load(Ordering::Relaxed) {
                    info!("TelemetryLogger received shutdown signal");
                    info!("TelemetryLogger stopped");
                    return;
                }
                std::thread::sleep(chunk_duration);
            }

            // Print separator
            info!("========================================");
            info!("Telemetry Report");
            info!("========================================");

            // Read and print metrics for each telemetry node
            for (idx, metrics) in self.registry.get_metrics().iter().enumerate() {
                info!("Node: {}", metrics.node_id);
                info!("  Bucket Stats:");

                let num_buckets = metrics.counters_pkts.len();

                for i in 0..num_buckets {
                    let pkts = metrics.counters_pkts[i].load(Ordering::Relaxed);
                    let bytes = metrics.counters_bytes[i].load(Ordering::Relaxed);

                    // Calculate deltas
                    let delta_pkts = pkts - prev_pkts[idx][i];
                    let delta_bytes = bytes - prev_bytes[idx][i];

                    // Update previous values
                    prev_pkts[idx][i] = pkts;
                    prev_bytes[idx][i] = bytes;

                    // Get label if available
                    let label = if i < metrics.labels.len() {
                        &metrics.labels[i]
                    } else {
                        "unknown"
                    };

                    // Calculate rate per second
                    let pkts_per_sec = delta_pkts as f64 / self.interval.as_secs_f64();
                    let mbps = (delta_bytes as f64 * 8.0) / (self.interval.as_secs_f64() * 1_000_000.0);

                    info!(
                        "    [{}] {}: {} pkts ({:.2} pps), {} bytes ({:.2} Mbps) | Total: {} pkts, {} bytes",
                        i, label, delta_pkts, pkts_per_sec, delta_bytes, mbps, pkts, bytes
                    );
                }
            }

            info!("========================================");
        }
    }
}

/// Spawn the telemetry logger on a separate thread
pub fn spawn_telemetry_logger(
    registry: Arc<TelemetryRegistry>,
    interval_secs: u64,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let logger = TelemetryLogger::new(registry, interval_secs);
        logger.run(shutdown);
    })
}
