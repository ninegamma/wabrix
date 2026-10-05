use axum::{routing::get, Router};
use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast::Receiver;
use wabrix_base::telemetry::{NamedHistogramSnapshot, TMLiveStats};

/// Spawns the Prometheus metrics exporter in the background.
/// It listens to the Wabrix telemetry broadcast and updates standard gauges.
pub fn spawn_metrics_server(mut tm_rx: Receiver<TMLiveStats>) {
    tokio::spawn(async move {
        // Setup Prometheus Registry & Gauges
        let registry = Registry::new();

        // Create Gauge Vectors with labels for pipeline name and counter name.
        let packets_total = IntGaugeVec::new(
            Opts::new("wabrix_packets_total", "Total packets processed"),
            &["tmstats_name", "data_label"],
        )
        .unwrap();

        let bytes_total = IntGaugeVec::new(
            Opts::new("wabrix_bytes_total", "Total bytes processed"),
            &["tmstats_name", "data_label"],
        )
        .unwrap();

        let pps_live = IntGaugeVec::new(
            Opts::new("wabrix_packets_per_second", "Live packets per second (PPS)"),
            &["tmstats_name", "data_label"],
        )
        .unwrap();

        let bps_live = IntGaugeVec::new(
            Opts::new("wabrix_bytes_per_second", "Live bytes per second (BPS)"),
            &["tmstats_name", "data_label"],
        )
        .unwrap();

        registry.register(Box::new(packets_total.clone())).unwrap();
        registry.register(Box::new(bytes_total.clone())).unwrap();
        registry.register(Box::new(pps_live.clone())).unwrap();
        registry.register(Box::new(bps_live.clone())).unwrap();

        let histograms = Arc::new(RwLock::new(HashMap::<
            (String, String),
            NamedHistogramSnapshot,
        >::new()));
        let receiver_histograms = Arc::clone(&histograms);
        let metrics_registry = registry.clone();

        // Background task to map Wabrix telemetry to Prometheus metrics
        tokio::spawn(async move {
            while let Ok(stats) = tm_rx.recv().await {
                let tmstats_name = &stats.tmstats_name;
                if let Ok(mut snapshots) = receiver_histograms.write() {
                    for histogram in stats.histograms {
                        snapshots.insert((tmstats_name.clone(), histogram.name.clone()), histogram);
                    }
                }

                // Iterate over named counters and safely update the corresponding data vectors.
                for (i, data_label) in stats.counter_names.iter().enumerate() {
                    let label_values = &[tmstats_name.as_str(), data_label.as_str()];

                    if let Some(&val) = stats.counters_pkts.get(i) {
                        packets_total
                            .with_label_values(label_values)
                            .set(val as i64);
                    }
                    if let Some(&val) = stats.counters_bytes.get(i) {
                        bytes_total.with_label_values(label_values).set(val as i64);
                    }
                    if let Some(&val) = stats.rates_pkts.get(i) {
                        pps_live.with_label_values(label_values).set(val as i64);
                    }
                    if let Some(&val) = stats.rates_bytes.get(i) {
                        bps_live.with_label_values(label_values).set(val as i64);
                    }
                }
            }
        });

        // Expose the registry over HTTP for Prometheus to scrape
        let histogram_snapshots = Arc::clone(&histograms);
        let app = Router::new().route(
            "/metrics",
            get(move || {
                let metrics_registry = metrics_registry.clone();
                let histogram_snapshots = Arc::clone(&histogram_snapshots);
                async move {
                    let mut buffer = vec![];
                    let encoder = TextEncoder::new();
                    let metric_families = metrics_registry.gather();
                    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                        log::error!("Failed to encode prometheus metrics: {}", e);
                    }
                    let mut output = String::from_utf8(buffer).unwrap_or_default();
                    append_histograms(&mut output, &histogram_snapshots);
                    output
                }
            }),
        );

        let addr = SocketAddr::from(([0, 0, 0, 0], 9090));
        log::info!(
            "Prometheus Metrics Exporter listening on http://{}/metrics",
            addr
        );

        if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
            if let Err(e) = axum::serve(listener, app).await {
                log::error!("Prometheus server error: {}", e);
            }
        } else {
            log::error!("Failed to bind Prometheus server to {}", addr);
        }
    });
}

fn append_histograms(
    output: &mut String,
    snapshots: &RwLock<HashMap<(String, String), NamedHistogramSnapshot>>,
) {
    let Ok(snapshots) = snapshots.read() else {
        return;
    };
    if snapshots.is_empty() {
        return;
    }

    let _ = writeln!(
        output,
        "# HELP wabrix_histogram_bucket_count Number of samples in each histogram bucket."
    );
    let _ = writeln!(output, "# TYPE wabrix_histogram_bucket_count gauge");
    let _ = writeln!(
        output,
        "# HELP wabrix_histogram_sum Sum of histogram samples."
    );
    let _ = writeln!(output, "# TYPE wabrix_histogram_sum gauge");
    let _ = writeln!(
        output,
        "# HELP wabrix_histogram_count Number of histogram samples."
    );
    let _ = writeln!(output, "# TYPE wabrix_histogram_count gauge");
    for ((node, name), histogram) in snapshots.iter() {
        let node = escape_label_value(node);
        let name = escape_label_value(name);
        for (index, count) in histogram.buckets.iter().enumerate() {
            let _ = writeln!(
                output,
                "wabrix_histogram_bucket_count{{tmstats_name=\"{node}\",histogram=\"{name}\",bucket=\"{index}\"}} {count}"
            );
        }
        let _ = writeln!(
            output,
            "wabrix_histogram_sum{{tmstats_name=\"{node}\",histogram=\"{name}\"}} {}",
            histogram.sum
        );
        let _ = writeln!(
            output,
            "wabrix_histogram_count{{tmstats_name=\"{node}\",histogram=\"{name}\"}} {}",
            histogram.count
        );
    }
}

fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            character => escaped.push(character),
        }
    }
    escaped
}
