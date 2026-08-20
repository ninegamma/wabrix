use axum::{routing::get, Router};
use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};
use std::net::SocketAddr;
use tokio::sync::broadcast::Receiver;
use wabrix_base::telemetry::TMLiveStats;

/// Spawns the Prometheus metrics exporter in the background.
/// It listens to the Wabrix telemetry broadcast and updates standard gauges.
pub fn spawn_metrics_server(mut tm_rx: Receiver<TMLiveStats>) {
    tokio::spawn(async move {
        // Setup Prometheus Registry & Gauges
        let registry = Registry::new();

        // Create Gauge Vectors with labels for pipeline name and specific node/interface label
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

        let metrics_registry = registry.clone();

        // Background task to map Wabrix telemetry to Prometheus metrics
        tokio::spawn(async move {
            while let Ok(stats) = tm_rx.recv().await {
                let tmstats_name = &stats.tmstats_name;

                // Iterate over the labels and safely update the corresponding data vectors
                for (i, data_label) in stats.labels.iter().enumerate() {
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
        let app = Router::new().route(
            "/metrics",
            get(move || async move {
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                let metric_families = metrics_registry.gather();
                if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                    log::error!("Failed to encode prometheus metrics: {}", e);
                }
                String::from_utf8(buffer).unwrap_or_default()
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
