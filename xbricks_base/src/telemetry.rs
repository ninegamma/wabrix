use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use log::debug;
use serde::{Serialize, Deserialize};
use tokio::sync::broadcast::Sender;
use ts_rs::TS;

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[ts(export_to = "TMLiveStats.ts")]
pub struct TMLiveStats {
    pub tmstats_name: String,
    pub labels: Vec<String>,
    pub counters_pkts: Vec<u64>,
    pub counters_bytes: Vec<u64>,
    pub is_active_run: bool,
}


/// Represents a single node's metrics
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


/// Periodically reads pipeline node counters from the TelemetryRegistry and
/// broadcasts them over the same WebSocket channel as NIC telemetry.
/// Multiple entries with the same node_id (e.g. from different workers running
/// the same pipeline) are aggregated into a single `TMLiveStats` message by
/// summing their counters.
pub fn spawn_pipeline_telemetry(
    telemetry_tx: Sender<TMLiveStats>,
    registry: Arc<TelemetryRegistry>,
    shutdown_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    tokio::spawn(async move {
        while !shutdown_signal.load(Ordering::Relaxed) {
            // Aggregate counters across all entries that share the same node_id.
            // We rely on insertion order: the first entry for a given id provides
            // the labels and the initial counter values; subsequent entries for
            // the same id are summed in.
            let mut seen: Vec<(String, Vec<u64>, Vec<u64>, Vec<String>)> = Vec::new();

            for metrics in registry.get_metrics() {
                let pkts: Vec<u64> = metrics.counters_pkts.iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();
                let bytes: Vec<u64> = metrics.counters_bytes.iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();

                if let Some(entry) = seen.iter_mut().find(|(id, ..)| id == &metrics.node_id) {
                    // Accumulate into existing entry
                    for (acc, v) in entry.1.iter_mut().zip(pkts.iter()) {
                        *acc += v;
                    }
                    for (acc, v) in entry.2.iter_mut().zip(bytes.iter()) {
                        *acc += v;
                    }
                } else {
                    seen.push((metrics.node_id.clone(), pkts, bytes, metrics.labels.clone()));
                }
            }

            for (node_id, counters_pkts, counters_bytes, labels) in seen {
                let is_active_run = counters_pkts.iter().any(|&v| v > 0);
                let stats = TMLiveStats {
                    tmstats_name: node_id,
                    labels,
                    counters_pkts,
                    counters_bytes,
                    is_active_run,
                };
                let _ = telemetry_tx.send(stats);
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    });
    Ok(())
}

pub fn spawn_telemetry(telemetry_tx: Sender<TMLiveStats>, iface_name: String, shutdown_signal: Arc<AtomicBool>) -> anyhow::Result<()> {
    tokio::spawn(async move {
        // Persistent tracking variables (Absolute counters from ethtool)
        let mut prev_rx_uc = 0u64;
        let mut prev_rx_mc = 0u64;
        let mut prev_rx_bc = 0u64;
        let mut prev_sw_drops = 0u64;
        let mut prev_hw_drops = 0u64;
        let mut prev_tx_uc = 0u64;
        let mut prev_tx_mc = 0u64;
        let mut prev_tx_bc = 0u64;
        let mut is_first_run = true;

        // Burst tracking variables (Relative counters for the React UI)
        let mut in_run = false;
        let mut run_rx_uc = 0u64;
        let mut run_rx_mc = 0u64;
        let mut run_rx_bc = 0u64;
        let mut run_sw_drops = 0u64;
        let mut run_hw_drops = 0u64;
        let mut run_tx_uc = 0u64;
        let mut run_tx_mc = 0u64;
        let mut run_tx_bc = 0u64;
        let mut idle_ticks = 0;

        while !shutdown_signal.load(Ordering::Relaxed) {
            if let Ok(output) = tokio::process::Command::new("ethtool")
                .args(["-S", &iface_name])
                .output()
                .await
            {
                let stdout = String::from_utf8_lossy(&output.stdout);

                let mut curr_rx_uc = 0u64;
                let mut curr_rx_mc = 0u64;
                let mut curr_rx_bc = 0u64;
                let mut curr_sw_drops = 0u64;
                let mut curr_hw_drops = 0u64;
                let mut curr_tx_uc = 0u64;
                let mut curr_tx_mc = 0u64;
                let mut curr_tx_bc = 0u64;

                // ==========================================
                // 1. THE ROBUST ETHTOOL PARSER
                // ==========================================
                for line in stdout.lines() {
                    // split_whitespace automatically handles the leading tabs/spaces in ethtool
                    let parts: Vec<&str> = line.split_whitespace().collect();

                    // We expect lines like: "rx_unicast: 123456"
                    if parts.len() == 2 {
                        let key = parts[0];
                        if let Ok(val) = parts[1].parse::<u64>() {
                            match key {
                                "rx_unicast:"   => curr_rx_uc = val,
                                "rx_multicast:" => curr_rx_mc = val,
                                "rx_broadcast:" => curr_rx_bc = val,
                                "rx_dropped:"   => curr_sw_drops = val,
                                "rx_dropped.nic:" => curr_hw_drops = val,
                                "tx_unicast:"   => curr_tx_uc = val,
                                "tx_multicast:" => curr_tx_mc = val,
                                "tx_broadcast:" => curr_tx_bc = val,
                                _ => {} // Ignore the other 50+ stats we don't care about
                            }
                        }
                    }
                }

                // Initialize the baseline on the very first loop so we don't
                // trigger a massive false "burst" of billions of packets
                if is_first_run {
                    prev_rx_uc = curr_rx_uc;
                    prev_rx_mc = curr_rx_mc;
                    prev_rx_bc = curr_rx_bc;
                    prev_sw_drops = curr_sw_drops;
                    prev_hw_drops = curr_hw_drops;
                    prev_tx_uc = curr_tx_uc;
                    prev_tx_mc = curr_tx_mc;
                    prev_tx_bc = curr_tx_bc;
                    is_first_run = false;
                    continue; // Skip the rest of the loop for this first tick
                }

                // ==========================================
                // DELTA MATH
                // ==========================================
                // saturating_sub prevents a crash if the NIC driver resets its counters to 0
                let delta_rx_uc    = curr_rx_uc.saturating_sub(prev_rx_uc);
                let delta_rx_mc    = curr_rx_mc.saturating_sub(prev_rx_mc);
                let delta_rx_bc    = curr_rx_bc.saturating_sub(prev_rx_bc);
                let delta_sw_drops = curr_sw_drops.saturating_sub(prev_sw_drops);
                let delta_hw_drops = curr_hw_drops.saturating_sub(prev_hw_drops);
                let delta_tx_uc    = curr_tx_uc.saturating_sub(prev_tx_uc);
                let delta_tx_mc    = curr_tx_mc.saturating_sub(prev_tx_mc);
                let delta_tx_bc    = curr_tx_bc.saturating_sub(prev_tx_bc);

                // Save for the next loop
                prev_rx_uc    = curr_rx_uc;
                prev_rx_mc    = curr_rx_mc;
                prev_rx_bc    = curr_rx_bc;
                prev_sw_drops = curr_sw_drops;
                prev_hw_drops = curr_hw_drops;
                prev_tx_uc    = curr_tx_uc;
                prev_tx_mc    = curr_tx_mc;
                prev_tx_bc    = curr_tx_bc;

                // ==========================================
                // BURST LOGIC (The State Machine)
                // ==========================================
                let any_activity = delta_rx_uc > 0 || delta_rx_mc > 0 || delta_rx_bc > 0
                    || delta_sw_drops > 0 || delta_hw_drops > 0
                    || delta_tx_uc > 0 || delta_tx_mc > 0 || delta_tx_bc > 0;

                if any_activity {
                    if !in_run {
                        // We just woke up! Reset the burst counters
                        in_run = true;
                        run_rx_uc    = 0;
                        run_rx_mc    = 0;
                        run_rx_bc    = 0;
                        run_sw_drops = 0;
                        run_hw_drops = 0;
                        run_tx_uc    = 0;
                        run_tx_mc    = 0;
                        run_tx_bc    = 0;
                    }

                    run_rx_uc    += delta_rx_uc;
                    run_rx_mc    += delta_rx_mc;
                    run_rx_bc    += delta_rx_bc;
                    run_sw_drops += delta_sw_drops;
                    run_hw_drops += delta_hw_drops;
                    run_tx_uc    += delta_tx_uc;
                    run_tx_mc    += delta_tx_mc;
                    run_tx_bc    += delta_tx_bc;
                    idle_ticks = 0;

                } else if in_run {
                    idle_ticks += 1;
                    // If 50 ticks (500ms) pass with 0 packets, the burst is officially over
                    if idle_ticks > 50 {
                        in_run = false;
                    }
                }

                // ==========================================
                // BROADCAST TO REACT UI
                // ==========================================
                // We send updates if we are actively receiving, OR if we just finished a burst
                // so the UI can lock in the final static number.
                if in_run || idle_ticks <= 50 {
                    let stats = TMLiveStats {
                        tmstats_name: iface_name.clone(),
                        labels: vec![
                            "rx_uc_pkts".to_string(), "rx_mc_pkts".to_string(), "rx_bc_pkts".to_string(),
                            "sw_drops".to_string(), "hw_drops".to_string(),
                            "tx_uc_pkts".to_string(), "tx_mc_pkts".to_string(), "tx_bc_pkts".to_string(),
                        ],
                        counters_pkts: vec![
                            run_rx_uc, run_rx_mc, run_rx_bc,
                            run_sw_drops, run_hw_drops,
                            run_tx_uc, run_tx_mc, run_tx_bc,
                        ],
                        counters_bytes: vec![],
                        is_active_run: in_run,
                    };
                    // Ignore errors (this just means no browsers are currently connected to the WebSocket)
                    let _ = telemetry_tx.send(stats);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    });
    Ok(())
}

mod tests {
    use std::path::PathBuf;
    use ts_rs::Config;
    use super::*;

    #[test]
    fn check_env() {
        println!("Exporting to: {:?}", std::env::var("TS_RS_EXPORT_DIR"));
    }

    #[test]
    fn export_bindings() {
        let config = Config::default().with_out_dir(PathBuf::from("../xbricks-ui/src/types/"));
        // Force the base path to your UI folder
        TMLiveStats::export_all(&config).expect("could not export type");
    }
}