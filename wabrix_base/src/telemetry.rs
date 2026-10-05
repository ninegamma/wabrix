use crossbeam_utils::CachePadded;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::broadcast::Sender;
use ts_rs::TS;
pub use wabrix_api::{
    INSPECTOR_DELAY_HISTOGRAM_BUCKETS, INSPECTOR_DELAY_HISTOGRAM_BUCKET_WIDTH_US,
    INSPECTOR_DELAY_HISTOGRAM_FINITE_BUCKETS,
};

const SIOCETHTOOL: libc::c_ulong = 0x8946;
const ETHTOOL_GDRVINFO: u32 = 0x0000_0003;
const ETHTOOL_GSTRINGS: u32 = 0x0000_001b;
const ETHTOOL_GSTATS: u32 = 0x0000_001d;
const ETH_SS_STATS: u32 = 1;
const ETH_GSTRING_LEN: usize = 32;
const IFNAMSIZ: usize = 16;

#[repr(C)]
#[derive(Default)]
struct EthtoolDrvInfo {
    cmd: u32,
    driver: [u8; 32],
    version: [u8; 32],
    fw_version: [u8; 32],
    bus_info: [u8; 32],
    erom_version: [u8; 32],
    reserved2: [u8; 12],
    n_priv_flags: u32,
    n_stats: u32,
    testinfo_len: u32,
    eedump_len: u32,
    regdump_len: u32,
}

#[repr(C)]
struct IfReq {
    ifr_name: [libc::c_char; IFNAMSIZ],
    ifr_data: *mut c_void,
}

/// Issue an ethtool request directly through SIOCETHTOOL.
/// This avoids spawning `ethtool`, which would fork and can cause avoidable
/// scheduler/TLB activity on latency-sensitive data-plane cores.
fn ethtool_ioctl(interface: &str, request: *mut c_void) -> bool {
    if interface.len() >= IFNAMSIZ {
        return false;
    }

    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return false;
    }

    let mut ifr = IfReq {
        ifr_name: [0; IFNAMSIZ],
        ifr_data: request,
    };
    for (dst, src) in ifr.ifr_name.iter_mut().zip(interface.bytes()) {
        *dst = src as libc::c_char;
    }

    let result = unsafe { libc::ioctl(fd, SIOCETHTOOL, &mut ifr) } == 0;
    unsafe {
        libc::close(fd);
    }
    result
}

fn ethtool_driver(interface: &str) -> Option<String> {
    let mut info = EthtoolDrvInfo {
        cmd: ETHTOOL_GDRVINFO,
        ..Default::default()
    };
    if !ethtool_ioctl(interface, &mut info as *mut _ as *mut c_void) {
        return None;
    }

    let end = info
        .driver
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(info.driver.len());
    Some(String::from_utf8_lossy(&info.driver[..end]).into_owned())
}

fn ethtool_stats(interface: &str) -> Option<(String, HashMap<String, u64>)> {
    let mut info = EthtoolDrvInfo {
        cmd: ETHTOOL_GDRVINFO,
        ..Default::default()
    };
    if !ethtool_ioctl(interface, &mut info as *mut _ as *mut c_void) || info.n_stats == 0 {
        return None;
    }

    let n_stats = info.n_stats as usize;
    let strings_size = size_of::<u32>() * 3 + n_stats * ETH_GSTRING_LEN;
    let mut strings_words = vec![0u64; strings_size.div_ceil(size_of::<u64>())];
    let strings = strings_words.as_mut_ptr() as *mut u8;
    unsafe {
        *(strings as *mut u32) = ETHTOOL_GSTRINGS;
        *(strings.add(size_of::<u32>()) as *mut u32) = ETH_SS_STATS;
        *(strings.add(size_of::<u32>() * 2) as *mut u32) = info.n_stats;
    }
    if !ethtool_ioctl(interface, strings as *mut c_void) {
        return None;
    }

    let stats_size = size_of::<u32>() * 2 + n_stats * size_of::<u64>();
    let mut stats_words = vec![0u64; stats_size.div_ceil(size_of::<u64>())];
    let stats = stats_words.as_mut_ptr() as *mut u8;
    unsafe {
        *(stats as *mut u32) = ETHTOOL_GSTATS;
        *(stats.add(size_of::<u32>()) as *mut u32) = info.n_stats;
    }
    if !ethtool_ioctl(interface, stats as *mut c_void) {
        return None;
    }

    let strings_bytes = unsafe { std::slice::from_raw_parts(strings, strings_size) };
    let stats_bytes = unsafe { std::slice::from_raw_parts(stats, stats_size) };
    let mut values = HashMap::with_capacity(n_stats);
    for index in 0..n_stats {
        let name_start = size_of::<u32>() * 3 + index * ETH_GSTRING_LEN;
        let name_bytes = &strings_bytes[name_start..name_start + ETH_GSTRING_LEN];
        let name_end = name_bytes
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(name_bytes.len());
        let name = String::from_utf8_lossy(&name_bytes[..name_end])
            .trim()
            .to_owned();
        let value_start = size_of::<u32>() * 2 + index * size_of::<u64>();
        let value = u64::from_ne_bytes(stats_bytes[value_start..value_start + 8].try_into().ok()?);
        values.insert(name, value);
    }

    let driver_end = info
        .driver
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(info.driver.len());
    Some((
        String::from_utf8_lossy(&info.driver[..driver_end]).into_owned(),
        values,
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[ts(export_to = "TMLiveStats.ts")]
pub struct TMLiveStats {
    pub tmstats_name: String,
    pub counter_names: Vec<String>,
    pub counters_pkts: Vec<u64>,
    pub counters_bytes: Vec<u64>,
    pub rates_pkts: Vec<u64>,
    pub rates_bytes: Vec<u64>,
    pub is_active_run: bool,
    pub histograms: Vec<NamedHistogramSnapshot>,
}

/// A named cumulative histogram snapshot. `buckets` stores disjoint intervals;
/// the bucket schema is defined by the producer of the histogram.
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct NamedHistogramSnapshot {
    pub name: String,
    pub buckets: Vec<u64>,
    pub sum: u64,
    pub count: u64,
}

/// Shared counters populated by the WASM host when it reads plugin telemetry.
pub struct HistogramMetrics {
    pub buckets: Vec<AtomicU64>,
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
    pub ready: AtomicBool,
}

impl HistogramMetrics {
    pub fn new(bucket_count: usize) -> Self {
        Self {
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            ready: AtomicBool::new(false),
        }
    }

    pub fn store_snapshot(&self, sum_us: u64, count: u64, buckets: &[u64]) {
        if buckets.len() != self.buckets.len() {
            return;
        }
        self.sum_us.store(sum_us, Ordering::Relaxed);
        self.count.store(count, Ordering::Relaxed);
        for (counter, value) in self.buckets.iter().zip(buckets) {
            counter.store(*value, Ordering::Relaxed);
        }
        self.ready.store(true, Ordering::Release);
    }

    fn snapshot(&self, name: &str) -> Option<NamedHistogramSnapshot> {
        self.ready
            .load(Ordering::Acquire)
            .then(|| NamedHistogramSnapshot {
                name: name.to_string(),
                buckets: self
                    .buckets
                    .iter()
                    .map(|counter| counter.load(Ordering::Relaxed))
                    .collect(),
                sum: self.sum_us.load(Ordering::Relaxed),
                count: self.count.load(Ordering::Relaxed),
            })
    }
}

/// Represents a single node's metrics
#[derive(Clone)]
#[repr(align(64))]
pub struct TelemetryMetrics {
    pub node_id: String,
    pub counter_names: Arc<RwLock<Vec<String>>>,
    pub counters_pkts: Vec<Arc<CachePadded<AtomicU64>>>,
    pub counters_bytes: Vec<Arc<CachePadded<AtomicU64>>>,
    /// When Some, overrides the `is_active_run` heuristic with an explicit flag.
    pub active_flag: Option<Arc<AtomicBool>>,
    pub force_sync_flag: Option<Arc<AtomicBool>>,
    pub histograms: Vec<NamedHistogramMetrics>,
}

#[derive(Clone)]
pub struct NamedHistogramMetrics {
    pub name: String,
    pub metrics: Arc<HistogramMetrics>,
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
        counter_names: Arc<RwLock<Vec<String>>>,
        counters_pkts: Vec<Arc<CachePadded<AtomicU64>>>,
        counters_bytes: Vec<Arc<CachePadded<AtomicU64>>>,
        active_flag: Option<Arc<AtomicBool>>,
        force_sync_flag: Option<Arc<AtomicBool>>,
        histograms: Vec<NamedHistogramMetrics>,
    ) {
        self.metrics.push(TelemetryMetrics {
            node_id,
            counter_names,
            counters_pkts,
            counters_bytes,
            active_flag,
            force_sync_flag,
            histograms,
        });
    }

    pub fn get_metrics(&self) -> &[TelemetryMetrics] {
        &self.metrics
    }
}

/// (node_id, pkt_counters, byte_counters, counter_names, active_flag) aggregated across workers.
type AggregatedMetric = (
    String,
    Vec<u64>,
    Vec<u64>,
    Vec<String>,
    Option<bool>,
    Vec<NamedHistogramSnapshot>,
);

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
        // given id provides the counter names and the initial counter values;
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
                let counter_names = metrics
                    .counter_names
                    .read()
                    .map(|names| names.clone())
                    .unwrap_or_default();
                let active = metrics
                    .active_flag
                    .as_ref()
                    .map(|f| f.load(Ordering::Relaxed));
                let histograms = metrics
                    .histograms
                    .iter()
                    .filter_map(|histogram| histogram.metrics.snapshot(&histogram.name))
                    .collect::<Vec<_>>();

                if let Some(entry) = seen.iter_mut().find(|(id, ..)| id == &metrics.node_id) {
                    for (acc, v) in entry.1.iter_mut().zip(pkts.iter()) {
                        *acc += v;
                    }
                    for (acc, v) in entry.2.iter_mut().zip(bytes.iter()) {
                        *acc += v;
                    }
                    if entry.3.is_empty() && !counter_names.is_empty() {
                        entry.3 = counter_names;
                    }
                    // OR the active flags: Some(true) wins over Some(false) and None
                    entry.4 = match (entry.4, active) {
                        (Some(a), Some(b)) => Some(a || b),
                        (Some(a), None) | (None, Some(a)) => Some(a),
                        (None, None) => None,
                    };
                    for histogram in histograms {
                        if let Some(aggregate) =
                            entry.5.iter_mut().find(|item| item.name == histogram.name)
                        {
                            if aggregate.buckets.len() != histogram.buckets.len() {
                                log::warn!(
                                    "Ignoring histogram '{}' for node '{}': bucket schema differs between workers",
                                    histogram.name, metrics.node_id
                                );
                                continue;
                            }
                            for (total, value) in
                                aggregate.buckets.iter_mut().zip(histogram.buckets)
                            {
                                *total = total.saturating_add(value);
                            }
                            aggregate.sum = aggregate.sum.saturating_add(histogram.sum);
                            aggregate.count = aggregate.count.saturating_add(histogram.count);
                        } else {
                            entry.5.push(histogram);
                        }
                    }
                } else {
                    seen.push((
                        metrics.node_id.clone(),
                        pkts,
                        bytes,
                        counter_names,
                        active,
                        histograms,
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
            for (node_id, pkts, bytes, _, _, _) in aggregate() {
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

            for (node_id, counters_pkts, counters_bytes, counter_names, active_flag, histograms) in
                seen
            {
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
                    counter_names,
                    counters_pkts,
                    counters_bytes,
                    rates_pkts,
                    rates_bytes,
                    is_active_run,
                    histograms,
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
        let is_ice = ethtool_driver(iface).as_deref() == Some("ice");

        if is_ice {
            spawn_telemetry_ice(
                telemetry_tx.clone(),
                iface.clone(),
                registry.clone(),
                shutdown_signal.clone(),
            )?;
        } else {
            log::debug!(
                "Skipping ICE telemetry for interface {} (driver is not ice)",
                iface
            );
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
        log::trace!("Starting ICE telemetry for interface {}", iface_name);

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
            // Read the NIC counters through SIOCETHTOOL. This keeps telemetry
            // completely in-process and avoids fork/exec on the data-plane host.
            if let Some((_driver, stats)) = ethtool_stats(&iface_name) {
                let stat = |name: &str| stats.get(name).copied().unwrap_or(0);
                let curr_rx_uc = stat("rx_unicast");
                let curr_rx_mc = stat("rx_multicast");
                let curr_rx_bc = stat("rx_broadcast");
                let curr_sw_drops = stat("rx_dropped");
                let curr_hw_drops = stat("rx_dropped.nic");
                let curr_tx_uc = stat("tx_unicast");
                let curr_tx_mc = stat("tx_multicast");
                let curr_tx_bc = stat("tx_broadcast");

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
                        counter_names: vec![
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
                        histograms: vec![],
                    };
                    log::trace!(
                        "ICE telemetry {}: sw_drops={}, hw_drops={}, active={}",
                        iface_name,
                        run_sw_drops,
                        run_hw_drops,
                        in_run
                    );
                    // Ignore errors (this just means no browsers are currently connected to the WebSocket)
                    let _ = telemetry_tx.send(stats);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
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
