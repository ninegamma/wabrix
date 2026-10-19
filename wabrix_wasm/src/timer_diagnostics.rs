use std::collections::HashMap;

use wabrix_base::interface::batch::Batch;
use wabrix_base::NodeId;

#[cfg(feature = "profiling")]
use wabrix_api::helpers::parse_suffixed_number;

#[cfg(feature = "profiling")]
pub struct TimerDiagnostics {
    requested_frames: u64,
    allocated_frames: u64,
    routed_frames: u64,
    retained_frames: u64,
    allocation_shortfalls: u64,
    batch_mismatches: u64,
    mailbox_conservation_failures: u64,
    route_counts: Vec<u64>,
    syn_route_index: usize,
    tsc_frequency: u64,
    last_tsc: Option<u64>,
    last_timer_packet_count: u64,
    enabled: bool,
    active: bool,
    #[cfg(feature = "runtime-diagnostics")]
    pipeline_name: String,
    #[cfg(feature = "runtime-diagnostics")]
    worker_id: usize,
    #[cfg(feature = "runtime-diagnostics")]
    worker_index: usize,
}

#[cfg(not(feature = "profiling"))]
pub struct TimerDiagnostics;

impl TimerDiagnostics {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_data: &HashMap<String, String>,
        routing_table: &[NodeId],
        tsc_frequency: u64,
        pipeline_name: String,
        worker_id: usize,
        worker_index: usize,
    ) -> Self {
        #[cfg(feature = "profiling")]
        {
            #[cfg(not(feature = "runtime-diagnostics"))]
            let _ = (&pipeline_name, worker_id, worker_index);

            let enabled = node_data
                .get("generation_rate")
                .and_then(|rate| parse_suffixed_number(rate).ok())
                .is_some_and(|rate| rate > 0);
            let syn_route_index = routing_table
                .iter()
                .position(|route| route.id == "shared_tx_sink")
                .unwrap_or(0);

            return Self {
                requested_frames: 0,
                allocated_frames: 0,
                routed_frames: 0,
                retained_frames: 0,
                allocation_shortfalls: 0,
                batch_mismatches: 0,
                mailbox_conservation_failures: 0,
                route_counts: vec![0; routing_table.len()],
                syn_route_index,
                tsc_frequency,
                last_tsc: None,
                last_timer_packet_count: 0,
                enabled,
                active: false,
                #[cfg(feature = "runtime-diagnostics")]
                pipeline_name,
                #[cfg(feature = "runtime-diagnostics")]
                worker_id,
                #[cfg(feature = "runtime-diagnostics")]
                worker_index,
            };
        }

        #[cfg(not(feature = "profiling"))]
        {
            let _ = (
                node_data,
                routing_table,
                tsc_frequency,
                pipeline_name,
                worker_id,
                worker_index,
            );
            Self
        }
    }

    pub fn record_requested(&mut self, frames: u32) {
        #[cfg(feature = "profiling")]
        {
            self.requested_frames += frames as u64;
        }
        #[cfg(not(feature = "profiling"))]
        let _ = frames;
    }

    pub fn record_allocated(&mut self, frames: usize) {
        #[cfg(feature = "profiling")]
        {
            self.allocated_frames += frames as u64;
        }
        #[cfg(not(feature = "profiling"))]
        let _ = frames;
    }

    pub fn record_allocation_shortfall(&mut self) {
        #[cfg(feature = "profiling")]
        {
            self.allocation_shortfalls += 1;
        }
    }

    pub fn record_batch_mismatch(&mut self) {
        #[cfg(feature = "profiling")]
        {
            self.batch_mismatches = self.batch_mismatches.saturating_add(1);
        }
    }

    pub fn record_mailbox_conservation_failure(&mut self) {
        #[cfg(feature = "profiling")]
        {
            self.mailbox_conservation_failures =
                self.mailbox_conservation_failures.saturating_add(1);
        }
    }

    pub fn begin_timer_batch(&mut self) {
        #[cfg(feature = "profiling")]
        {
            self.active = true;
        }
    }

    pub fn end_timer_batch(&mut self) {
        #[cfg(feature = "profiling")]
        {
            self.active = false;
        }
    }

    pub fn record_routing(
        &mut self,
        routed_frames: usize,
        retained_frames: usize,
        outputs: &[Batch],
    ) {
        #[cfg(feature = "profiling")]
        if self.active {
            self.routed_frames += routed_frames as u64;
            self.retained_frames += retained_frames as u64;
            for (route_idx, output_batch) in outputs.iter().enumerate() {
                self.route_counts[route_idx] += output_batch.len() as u64;
            }
        }
        #[cfg(not(feature = "profiling"))]
        let _ = (routed_frames, retained_frames, outputs);
    }

    pub fn report_if_due(&mut self, node_id: &NodeId, current_tsc: u64) {
        #[cfg(feature = "profiling")]
        {
            let should_log = self.enabled
                && (self.last_tsc.is_none()
                    || current_tsc.saturating_sub(self.last_tsc.unwrap_or(current_tsc))
                        >= 30 * self.tsc_frequency);
            if !should_log {
                return;
            }

            let run_start_tsc = wabrix_base::frame_registry::get_tsc_start();
            let elapsed_tsc_from_start = current_tsc.saturating_sub(run_start_tsc);
            let elapsed_micros = if self.tsc_frequency > 0 {
                (elapsed_tsc_from_start as u128 * 1_000_000) / self.tsc_frequency as u128
            } else {
                0
            };
            let timestamp_ms = elapsed_micros / 1_000;
            let timestamp_ms_fraction = elapsed_micros % 1_000;
            let routed_timer_packets = self
                .route_counts
                .get(self.syn_route_index)
                .copied()
                .unwrap_or(0);
            let (rate, elapsed_tsc) = if let Some(previous_tsc) = self.last_tsc {
                let elapsed_tsc = current_tsc.saturating_sub(previous_tsc);
                let elapsed_packets =
                    routed_timer_packets.saturating_sub(self.last_timer_packet_count);
                if elapsed_tsc > 0 && self.tsc_frequency > 0 {
                    (
                        elapsed_packets as f64 * self.tsc_frequency as f64 / elapsed_tsc as f64,
                        elapsed_tsc,
                    )
                } else {
                    (0.0, elapsed_tsc)
                }
            } else {
                (0.0, 0)
            };
            let elapsed_ms = if self.tsc_frequency > 0 {
                elapsed_tsc as f64 * 1_000.0 / self.tsc_frequency as f64
            } else {
                0.0
            };
            self.last_tsc = Some(current_tsc);
            self.last_timer_packet_count = routed_timer_packets;

            log::debug!(
                "NODE {}: timer diagnostics: timestamp_ms={}.{:03}, routed_timer_packets={}, timer_route_rate_per_sec={:.3}, elapsed_ms={:.3}",
                node_id,
                timestamp_ms,
                timestamp_ms_fraction,
                routed_timer_packets,
                rate,
                elapsed_ms,
            );
        }
        #[cfg(not(feature = "profiling"))]
        let _ = (node_id, current_tsc);
    }

    pub fn report_final(&self, node_id: &NodeId) {
        #[cfg(feature = "profiling")]
        if self.enabled {
            #[cfg(feature = "runtime-diagnostics")]
            log::info!(
                "NODE {}: final timer runtime counters: pipeline='{}', worker_id={}, worker_index={}, requested={}, allocated={}, unallocated={}, routed={}, retained={}, allocation_shortfalls={}, batch_mismatches={}, mailbox_conservation_failures={}, route_counts={:?}",
                node_id,
                self.pipeline_name,
                self.worker_id,
                self.worker_index,
                self.requested_frames,
                self.allocated_frames,
                self.requested_frames.saturating_sub(self.allocated_frames),
                self.routed_frames,
                self.retained_frames,
                self.allocation_shortfalls,
                self.batch_mismatches,
                self.mailbox_conservation_failures,
                self.route_counts,
            );
        }
        // Keep `node_id` considered used when the final runtime-diagnostics
        // log is compiled out by the active feature combination.
        #[cfg(all(feature = "profiling", not(feature = "runtime-diagnostics")))]
        let _ = node_id;
        #[cfg(not(feature = "profiling"))]
        let _ = node_id;
    }
}

#[cfg(all(test, feature = "profiling"))]
mod tests {
    use super::TimerDiagnostics;
    use std::collections::HashMap;

    #[test]
    fn diagnostics_are_enabled_by_positive_generation_rate() {
        let mut node_data = HashMap::new();
        node_data.insert("generation_rate".to_string(), "16M".to_string());

        let diagnostics = TimerDiagnostics::new(&node_data, &[], 1, "test".into(), 0, 0);

        assert!(diagnostics.enabled);
    }

    #[test]
    fn diagnostics_ignore_non_positive_generation_rates() {
        let mut node_data = HashMap::new();
        node_data.insert("generation_rate".to_string(), "0".to_string());

        let diagnostics = TimerDiagnostics::new(&node_data, &[], 1, "test".into(), 0, 0);

        assert!(!diagnostics.enabled);
    }
}
