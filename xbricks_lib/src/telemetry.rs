use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use serde::{Serialize, Deserialize};
use tokio::sync::broadcast::Sender;
use ts_rs::TS;

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[ts(export_to = "LiveStats.ts")]
pub struct LiveStats {
    pub iface: String,
    pub direction: String, // "RX" or "TX"
    pub is_active_run: bool,
    pub live_pkts: u64,
    pub sw_drops: u64,
    pub hw_drops: u64,
}

pub fn spawn_nic_telemetry(telemetry_tx: Sender<LiveStats>, iface_name: String, shutdown_signal: Arc<AtomicBool>)-> anyhow::Result<()> {
    tokio::spawn(async move {
        // Persistent tracking variables (Absolute counters from ethtool)
        let mut prev_pkts = 0u64;
        let mut prev_sw_drops = 0u64;
        let mut prev_hw_drops = 0u64;
        let mut is_first_run = true;

        // Burst tracking variables (Relative counters for the React UI)
        let mut in_run = false;
        let mut run_pkts = 0u64;
        let mut run_sw_drops = 0u64;
        let mut run_hw_drops = 0u64;
        let mut idle_ticks = 0;

        while !shutdown_signal.load(Ordering::Relaxed) {
            if let Ok(output) = tokio::process::Command::new("ethtool")
                .args(["-S", &iface_name])
                .output()
                .await
            {
                let stdout = String::from_utf8_lossy(&output.stdout);

                let mut curr_pkts = 0u64;
                let mut curr_sw_drops = 0u64;
                let mut curr_hw_drops = 0u64;

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
                                "rx_unicast:" => curr_pkts = val,
                                "rx_dropped:" => curr_sw_drops = val,
                                "rx_dropped.nic:" => curr_hw_drops = val,
                                _ => {} // Ignore the other 50+ stats we don't care about
                            }
                        }
                    }
                }

                // Initialize the baseline on the very first loop so we don't
                // trigger a massive false "burst" of billions of packets
                if is_first_run {
                    prev_pkts = curr_pkts;
                    prev_sw_drops = curr_sw_drops;
                    prev_hw_drops = curr_hw_drops;
                    is_first_run = false;
                    continue; // Skip the rest of the loop for this first tick
                }

                // ==========================================
                // DELTA MATH
                // ==========================================
                // saturating_sub prevents a crash if the NIC driver resets its counters to 0
                let delta_pkts = curr_pkts.saturating_sub(prev_pkts);
                let delta_sw_drops = curr_sw_drops.saturating_sub(prev_sw_drops);
                let delta_hw_drops = curr_hw_drops.saturating_sub(prev_hw_drops);

                // Save for the next loop
                prev_pkts = curr_pkts;
                prev_sw_drops = curr_sw_drops;
                prev_hw_drops = curr_hw_drops;

                // ==========================================
                // BURST LOGIC (The State Machine)
                // ==========================================
                if delta_pkts > 0 || delta_sw_drops > 0 || delta_hw_drops > 0 {
                    if !in_run {
                        // We just woke up! Reset the burst counters
                        in_run = true;
                        run_pkts = 0;
                        run_sw_drops = 0;
                        run_hw_drops = 0;
                    }

                    run_pkts += delta_pkts;
                    run_sw_drops += delta_sw_drops;
                    run_hw_drops += delta_hw_drops;
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
                    let stats = LiveStats {
                        iface: iface_name.clone(),
                        direction: "RX".to_string(),
                        is_active_run: in_run,
                        live_pkts: run_pkts,
                        sw_drops: run_sw_drops,
                        hw_drops: run_hw_drops,
                    };

                    // Ignore errors (this just means no browsers are currently connected to the WebSocket)
                    let _ = telemetry_tx.send(stats);
                }
            }

            // Sleep for 10ms (matching your 0.01s Bash sleep)
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    });
    Ok(())
}