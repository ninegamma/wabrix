use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast::Sender;
use ts_rs::TS;

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[ts(export_to = "TMLiveStats.ts")]
pub struct TMLiveStats {
    pub tmstats_name: String,
    pub labels: Vec<String>,
    pub counters_pkts: Vec<u64>,
    pub counters_bytes: Vec<u64>,
    pub rates_pkts: Vec<u64>,
    pub rates_bytes: Vec<u64>,
    pub is_active_run: bool,
}

/// Represents a single node's metrics
#[derive(Clone)]
pub struct TelemetryMetrics {
    pub node_id: String,
    pub labels: Vec<String>,
    pub counters_pkts: Vec<Arc<AtomicU64>>,
    pub counters_bytes: Vec<Arc<AtomicU64>>,
    /// When Some, overrides the `is_active_run` heuristic with an explicit flag.
    pub active_flag: Option<Arc<AtomicBool>>,
    pub force_sync_flag: Option<Arc<AtomicBool>>,
}

/// Registry to hold references to all telemetry nodes
#[derive(Clone, Default)]
pub struct TelemetryRegistry {
    metrics: Vec<TelemetryMetrics>,
}

impl TelemetryRegistry {
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    pub fn register(
        &mut self,
        node_id: String,
        labels: Vec<String>,
        counters_pkts: Vec<Arc<AtomicU64>>,
        counters_bytes: Vec<Arc<AtomicU64>>,
        active_flag: Option<Arc<AtomicBool>>,
        force_sync_flag: Option<Arc<AtomicBool>>,
    ) {
        self.metrics.push(TelemetryMetrics {
            node_id,
            labels,
            counters_pkts,
            counters_bytes,
            active_flag,
            force_sync_flag,
        });
    }

    pub fn get_metrics(&self) -> &[TelemetryMetrics] {
        &self.metrics
    }
}

/// (node_id, pkt_counters, byte_counters, labels, active_flag) aggregated across workers.
type AggregatedMetric = (String, Vec<u64>, Vec<u64>, Vec<String>, Option<bool>);

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
        // Tracker: node_id -> (Last Timestamp, Last Pkts Array, Last Bytes Array)
        let mut rate_tracker: HashMap<String, (Instant, Vec<u64>, Vec<u64>)> = HashMap::new();

        // Tell all Wasm engines / native nodes to dump their out-of-band stats.
        let force_sync = || {
            for metrics in registry.get_metrics() {
                if let Some(flag) = &metrics.force_sync_flag {
                    flag.store(true, Ordering::Relaxed);
                }
            }
        };

        // Read the current counters and aggregate across all entries that share
        // the same node_id. We rely on insertion order: the first entry for a
        // given id provides the labels and the initial counter values;
        // subsequent entries for the same id are summed in.
        // active_flag is OR-aggregated: true if ANY worker reports active.
        let aggregate = || {
            let mut seen: Vec<AggregatedMetric> = Vec::new();
            for metrics in registry.get_metrics() {
                let pkts: Vec<u64> = metrics
                    .counters_pkts
                    .iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();
                let bytes: Vec<u64> = metrics
                    .counters_bytes
                    .iter()
                    .map(|c| c.load(Ordering::Relaxed))
                    .collect();
                let active = metrics
                    .active_flag
                    .as_ref()
                    .map(|f| f.load(Ordering::Relaxed));

                if let Some(entry) = seen.iter_mut().find(|(id, ..)| id == &metrics.node_id) {
                    for (acc, v) in entry.1.iter_mut().zip(pkts.iter()) {
                        *acc += v;
                    }
                    for (acc, v) in entry.2.iter_mut().zip(bytes.iter()) {
                        *acc += v;
                    }
                    // OR the active flags: Some(true) wins over Some(false) and None
                    entry.4 = match (entry.4, active) {
                        (Some(a), Some(b)) => Some(a || b),
                        (Some(a), None) | (None, Some(a)) => Some(a),
                        (None, None) => None,
                    };
                } else {
                    seen.push((
                        metrics.node_id.clone(),
                        pkts,
                        bytes,
                        metrics.labels.clone(),
                        active,
                    ));
                }
            }
            seen
        };

        // Seed an initial baseline sample immediately at startup. A rate requires
        // two samples, so without this the first broadcast finds an empty
        // rate_tracker, reports zero rates, and the true rate only appears a full
        // cadence (~1s) later — the "live badge lags at startup" symptom.
        force_sync();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        {
            let baseline_now = Instant::now();
            for (node_id, pkts, bytes, _, _) in aggregate() {
                rate_tracker.insert(node_id, (baseline_now, pkts, bytes));
            }
        }

        // The first real update follows a short interval so the badge populates
        // quickly with a genuine rate; subsequent updates settle to ~1 Hz.
        let mut settle = tokio::time::Duration::from_millis(250);

        while !shutdown_signal.load(Ordering::Relaxed) {
            // Wait first: paired with the baseline above (or the previous tick),
            // this is the window over which the next rate is measured.
            tokio::time::sleep(settle).await;
            settle = tokio::time::Duration::from_millis(990);

            // ==========================================
            // BROADCAST PHASE
            // ==========================================
            force_sync();

            // ==========================================
            // WAIT PHASE
            // Yield for 10ms. At 100 Hz, a batch takes 10ms. At 1M+ Hz, a batch
            // takes microseconds. This guarantees all active fast-paths hit the flag.
            // ==========================================
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            let seen = aggregate();

            // RATE CALCULATION & DISPATCH PHASE
            let now = Instant::now();

            for (node_id, counters_pkts, counters_bytes, labels, active_flag) in seen {
                let mut rates_pkts = vec![0; counters_pkts.len()];
                let mut rates_bytes = vec![0; counters_bytes.len()];

                if let Some((last_time, last_pkts, last_bytes)) = rate_tracker.get(&node_id) {
                    let elapsed = now.duration_since(*last_time).as_secs_f64();

                    if elapsed > 0.0 {
                        // Calculate PPS for every label
                        for i in 0..counters_pkts.len() {
                            let prev = last_pkts.get(i).copied().unwrap_or(0);
                            let delta = counters_pkts[i].saturating_sub(prev);
                            rates_pkts[i] = (delta as f64 / elapsed) as u64;
                        }

                        // Calculate BPS for every label
                        for i in 0..counters_bytes.len() {
                            let prev = last_bytes.get(i).copied().unwrap_or(0);
                            let delta = counters_bytes[i].saturating_sub(prev);
                            rates_bytes[i] = (delta as f64 / elapsed) as u64;
                        }
                    }
                }

                // Store current vectors for the next tick's delta calculation
                rate_tracker.insert(
                    node_id.clone(),
                    (now, counters_pkts.clone(), counters_bytes.clone()),
                );
                // Use the explicit active flag when available; fall back to the
                // "any pkt counter > 0" heuristic for nodes that don't register one.
                let is_active_run =
                    active_flag.unwrap_or_else(|| counters_pkts.iter().any(|&v| v > 0));
                let stats = TMLiveStats {
                    tmstats_name: node_id,
                    labels,
                    counters_pkts,
                    counters_bytes,
                    rates_pkts,
                    rates_bytes,
                    is_active_run,
                };
                let _ = telemetry_tx.send(stats);
            }
        }
    });
    Ok(())
}

pub fn spawn_telemetry_ice_for_ifaces(
    telemetry_tx: Sender<TMLiveStats>,
    ifaces: &[String],
    registry: Arc<TelemetryRegistry>,
    shutdown_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    for iface in ifaces {
        let is_ice = std::process::Command::new("ethtool")
            .args(["-i", iface])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| {
                String::from_utf8(o.stdout)
                    .ok()?
                    .lines()
                    .find_map(|l| l.strip_prefix("driver: ").map(|d| d.trim() == "ice"))
            })
            .unwrap_or(false);

        if is_ice {
            spawn_telemetry_ice(
                telemetry_tx.clone(),
                iface.clone(),
                registry.clone(),
                shutdown_signal.clone(),
            )?;
        }
    }
    Ok(())
}

pub fn spawn_telemetry_ice(
    telemetry_tx: Sender<TMLiveStats>,
    iface_name: String,
    registry: Arc<TelemetryRegistry>,
    shutdown_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    tokio::spawn(async move {
        // Real-time data-plane activity baseline (summed pipeline pkt counters).
        // The LIVE/IDLE state is driven from userspace pipeline activity rather
        // than `ethtool -S` deltas: on the `ice` driver those counters only
        // refresh on a seconds-scale watchdog, which delayed IDLE→LIVE by ~8s
        // even though packets were already flowing.
        let mut prev_dp_total = 0u64;

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
                                "rx_unicast:" => curr_rx_uc = val,
                                "rx_multicast:" => curr_rx_mc = val,
                                "rx_broadcast:" => curr_rx_bc = val,
                                "rx_dropped:" => curr_sw_drops = val,
                                "rx_dropped.nic:" => curr_hw_drops = val,
                                "tx_unicast:" => curr_tx_uc = val,
                                "tx_multicast:" => curr_tx_mc = val,
                                "tx_broadcast:" => curr_tx_bc = val,
                                _ => {} // Ignore the other 50+ stats we don't care about
                            }
                        }
                    }
                }

                // Sample real-time data-plane activity from the pipeline
                // telemetry registry. `active_flag` (e.g. the generator's
                // tm_is_running) gives an instant signal; for nodes without one
                // we detect a rise in the summed packet counters instead.
                let mut curr_dp_total = 0u64;
                let mut any_active_flag = false;
                for m in registry.get_metrics() {
                    if let Some(flag) = &m.active_flag {
                        any_active_flag |= flag.load(Ordering::Relaxed);
                    }
                    for c in &m.counters_pkts {
                        curr_dp_total = curr_dp_total.wrapping_add(c.load(Ordering::Relaxed));
                    }
                }

                // Initialize the baseline on the very first loop so we don't
                // trigger a massive false "burst" of billions of packets
                if is_first_run {
                    prev_dp_total = curr_dp_total;
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
                let delta_rx_uc = curr_rx_uc.saturating_sub(prev_rx_uc);
                let delta_rx_mc = curr_rx_mc.saturating_sub(prev_rx_mc);
                let delta_rx_bc = curr_rx_bc.saturating_sub(prev_rx_bc);
                let delta_sw_drops = curr_sw_drops.saturating_sub(prev_sw_drops);
                let delta_hw_drops = curr_hw_drops.saturating_sub(prev_hw_drops);
                let delta_tx_uc = curr_tx_uc.saturating_sub(prev_tx_uc);
                let delta_tx_mc = curr_tx_mc.saturating_sub(prev_tx_mc);
                let delta_tx_bc = curr_tx_bc.saturating_sub(prev_tx_bc);

                // Save for the next loop
                prev_rx_uc = curr_rx_uc;
                prev_rx_mc = curr_rx_mc;
                prev_rx_bc = curr_rx_bc;
                prev_sw_drops = curr_sw_drops;
                prev_hw_drops = curr_hw_drops;
                prev_tx_uc = curr_tx_uc;
                prev_tx_mc = curr_tx_mc;
                prev_tx_bc = curr_tx_bc;

                // Data-plane is live if any node reports active, or its packet
                // counters grew since the last poll. This flips within the poll
                // interval (or the pipeline sync cadence) instead of waiting for
                // the ice driver's slow ethtool stats refresh.
                let dp_active = any_active_flag || curr_dp_total > prev_dp_total;
                prev_dp_total = curr_dp_total;

                // ==========================================
                // BURST LOGIC (The State Machine)
                // ==========================================
                let any_activity = dp_active
                    || delta_rx_uc > 0
                    || delta_rx_mc > 0
                    || delta_rx_bc > 0
                    || delta_sw_drops > 0
                    || delta_hw_drops > 0
                    || delta_tx_uc > 0
                    || delta_tx_mc > 0
                    || delta_tx_bc > 0;

                if any_activity {
                    if !in_run {
                        // We just woke up! Reset the burst counters
                        in_run = true;
                        run_rx_uc = 0;
                        run_rx_mc = 0;
                        run_rx_bc = 0;
                        run_sw_drops = 0;
                        run_hw_drops = 0;
                        run_tx_uc = 0;
                        run_tx_mc = 0;
                        run_tx_bc = 0;
                    }

                    run_rx_uc += delta_rx_uc;
                    run_rx_mc += delta_rx_mc;
                    run_rx_bc += delta_rx_bc;
                    run_sw_drops += delta_sw_drops;
                    run_hw_drops += delta_hw_drops;
                    run_tx_uc += delta_tx_uc;
                    run_tx_mc += delta_tx_mc;
                    run_tx_bc += delta_tx_bc;
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
                            "rx_uc_pkts".to_string(),
                            "rx_mc_pkts".to_string(),
                            "rx_bc_pkts".to_string(),
                            "sw_drops".to_string(),
                            "hw_drops".to_string(),
                            "tx_uc_pkts".to_string(),
                            "tx_mc_pkts".to_string(),
                            "tx_bc_pkts".to_string(),
                        ],
                        counters_pkts: vec![
                            run_rx_uc,
                            run_rx_mc,
                            run_rx_bc,
                            run_sw_drops,
                            run_hw_drops,
                            run_tx_uc,
                            run_tx_mc,
                            run_tx_bc,
                        ],
                        counters_bytes: vec![],
                        rates_pkts: vec![],
                        rates_bytes: vec![],
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use ts_rs::Config;

    #[test]
    fn check_env() {
        println!("Exporting to: {:?}", std::env::var("TS_RS_EXPORT_DIR"));
    }

    #[test]
    fn export_bindings() {
        let config = Config::default().with_out_dir(PathBuf::from("../wabrix-ui/src/types/"));
        // Force the base path to your UI folder
        TMLiveStats::export_all(&config).expect("could not export type");
    }
}
