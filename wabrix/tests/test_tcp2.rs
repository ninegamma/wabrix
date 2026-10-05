use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

// Process guard guarantees the engine is killed when the test exits or panics
struct EngineProcess(Child);

impl Drop for EngineProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Fetches and extracts a specific counter from the Prometheus metrics page
fn get_metric_value(
    metrics_url: &str,
    metric_type: &str,
    node_path: &str,
    label: &str,
) -> Result<u64, String> {
    let mut response = ureq::get(metrics_url).call().map_err(|e| e.to_string())?;
    let body = response
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;

    let target_node = format!("tmstats_name=\"{}\"", node_path);
    let target_label = format!("data_label=\"{}\"", label);

    for line in body.lines() {
        // Enforce strict matching on metric type, fully qualified node path, and data label
        if line.starts_with(metric_type)
            && line.contains(&target_node)
            && line.contains(&target_label)
        {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(val_str) = parts.last() {
                return val_str.parse::<u64>().map_err(|e| e.to_string());
            }
        }
    }

    Err("Metric not found".to_string())
}

#[test]
fn verify_tcp_connection_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    // Utilize the pre-compiled binary path directly to avoid rustc dependency
    let child = Command::new(env!("CARGO_BIN_EXE_wabrix"))
        .args(["-c", "tests/test_tcp2.toml"])
        .spawn()?;

    let _engine_guard = EngineProcess(child);

    // Provide startup delay for UMEM mapping, BPF loading, and Wasm instantiation
    thread::sleep(Duration::from_secs(3));

    let metrics_url = "http://127.0.0.1:9090/metrics";
    let timeout = Duration::from_secs(20);
    let start_time = Instant::now();
    let expected_connections = 10_000_000;

    let mut connections_established = false;
    let mut connections_closed = false;

    let mut last_client_syn = 0;
    let mut last_server_syn = 0;
    let mut last_client_fin = 0;

    // Poll the metrics endpoint until target conditions are met or timeout is reached
    while start_time.elapsed() < timeout {
        // Target wabrix_packets_total and differentiate between client and server pipelines
        last_client_syn = get_metric_value(
            metrics_url,
            "wabrix_packets_total",
            "tcp_client/tcp_engine",
            "sent_syn",
        )
        .unwrap_or(0);
        last_server_syn = get_metric_value(
            metrics_url,
            "wabrix_packets_total",
            "tcp_server/tcp_engine",
            "recv_syn",
        )
        .unwrap_or(0);
        last_client_fin = get_metric_value(
            metrics_url,
            "wabrix_packets_total",
            "tcp_client/tcp_engine",
            "sent_fin",
        )
        .unwrap_or(0);

        if last_client_syn >= expected_connections && last_server_syn >= expected_connections {
            connections_established = true;
        }

        if connections_established && last_client_fin >= expected_connections {
            connections_closed = true;
            break;
        }

        thread::sleep(Duration::from_millis(500));
    }

    // Output debug information if the target conditions were not met
    if !connections_established || !connections_closed {
        println!("--- DEBUG INFO: TEST FAILURE ---");
        println!("Target expected connections: {}", expected_connections);
        println!(
            "Connections established boolean: {}",
            connections_established
        );
        println!("Connections closed boolean: {}", connections_closed);
        println!("Final metric SentSyn (Client): {}", last_client_syn);
        println!("Final metric RecvSyn (Server): {}", last_server_syn);
        println!("Final metric SentFin (Client): {}", last_client_fin);

        // Attempt to fetch and print the entire metrics body for manual inspection
        if let Ok(mut response) = ureq::get(metrics_url).call() {
            if let Ok(body) = response.body_mut().read_to_string() {
                println!("--- RAW METRICS DUMP ---");
                println!("{}", body);
                println!("------------------------");
            }
        } else {
            println!("Could not connect to metrics URL for raw dump.");
        }
    }

    assert!(
        connections_established,
        "Failed to establish expected TCP connections"
    );
    assert!(
        connections_closed,
        "Failed to teardown expected TCP connections"
    );

    Ok(())
}
