mod configure_bpf;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use clap::{Parser, ArgAction};
use log::info;
use separator::Separatable;
use wabrix_base::system::{get_nic_tx_packets_detailed, PacketStats, PlatformData};

#[derive(Debug, Parser)]
#[command(author, version, about = "High-performance packet generator using AF_XDP")]
struct Opt {
    /// Interface name (e.g., eth0 or enp114s0f1)
    #[arg(short = 'i', long)]
    interface: String,

    /// Packets per second string (e.g., "10M", "100k", or "max")
    #[arg(short = 'p', long, default_value = "max")]
    pps: String,

    /// Number of packets to generate (e.g., "10M", "100k", "1G", or "0" for unlimited)
    #[arg(short = 'n', long, default_value = "0")]
    no_packets: String,

    /// Batch size for processing packets
    #[arg(short = 'b', long, default_value = "64")]
    batch_sz: usize,

    /// Disable zerocopy (enabled by default)
    #[arg(long = "no-zerocopy", action = ArgAction::SetFalse)]
    zerocopy: bool,

    /// CPU core ID for the worker thread
    #[arg(short = 'c', long, default_value = "0")]
    core: Option<usize>,

    /// Number of worker threads (1-4), each on a separate core and TX queue
    #[arg(short = 'w', long, default_value = "1")]
    workers: usize,

    /// Destination IP address and MAC address for generated packets (format: <ip-address>-<mac-address>)
    #[arg(short = 'd', long)]
    dst: Option<String>,
}

/// Print NIC tx_packets statistics and delta from initial values
fn print_nic_stats_with_delta(interface: &str, initial_stats: Option<PacketStats>) {
    use separator::Separatable;

    if let Some(final_stats) = get_nic_tx_packets_detailed(interface) {
        println!("NIC tx_packets: {}", final_stats.total.separated_string());
        if let Some(unicast) = final_stats.unicast {
            println!("  - unicast:   {}", unicast.separated_string());
        }
        if let Some(multicast) = final_stats.multicast {
            println!("  - multicast: {}", multicast.separated_string());
        }
        if let Some(broadcast) = final_stats.broadcast {
            println!("  - broadcast: {}", broadcast.separated_string());
        }

        // Show difference from initial values
        if let Some(initial) = initial_stats {
            let total_diff = final_stats.total.saturating_sub(initial.total);
            println!("\nNIC tx_packets delta (from start): {}", total_diff.separated_string());

            if let (Some(final_u), Some(initial_u)) = (final_stats.unicast, initial.unicast) {
                let diff = final_u.saturating_sub(initial_u);
                println!("  - unicast:   {}", diff.separated_string());
            }
            if let (Some(final_m), Some(initial_m)) = (final_stats.multicast, initial.multicast) {
                let diff = final_m.saturating_sub(initial_m);
                println!("  - multicast: {}", diff.separated_string());
            }
            if let (Some(final_b), Some(initial_b)) = (final_stats.broadcast, initial.broadcast) {
                let diff = final_b.saturating_sub(initial_b);
                println!("  - broadcast: {}", diff.separated_string());
            }
        }
    } else {
        log::warn!("Failed to read NIC tx_packets from interface {}", interface);
    }
}

fn parse_dst_config(dst: Option<&str>) -> (Option<&str>, Option<&str>) {
    match dst {
        Some(s) => {
            if let Some(dash_pos) = s.find('-') {
                let ip_part = &s[..dash_pos];
                let mac_part = &s[dash_pos + 1..];

                let ip = if ip_part.is_empty() { None } else { Some(ip_part) };
                let mac = if mac_part.is_empty() { None } else { Some(mac_part) };

                (ip, mac)
            } else {
                // No dash found, treat entire string as IP
                (Some(s), None)
            }
        }
        None => (None, None),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opt = Opt::parse();

    // Validate worker count
    if opt.workers < 1 || opt.workers > 4 {
        eprintln!("Error: Number of workers must be between 1 and 4");
        std::process::exit(1);
    }

    let platform_data = PlatformData::detect();
    info!("TSC frequency: {} Hz", platform_data.tsc_frequency.separated_string());
    let toml_data = r#"
    [wabrix]
    ifaces = [ { name = "lo"} ]"#;

    let mut config = wabrix_lib::toml_parser::XBricksConfig::from_toml_str(toml_data)?;
    config.set_interface_0(&opt.interface);
    let (dst_ip, dst_mac) = parse_dst_config(opt.dst.as_deref());
    let base_core = opt.core.unwrap_or(0);

    // One unified function call for both single and multi-worker modes
    config.create_generator_pipeline(
        opt.workers,
        base_core,
        opt.batch_sz,
        &opt.pps,
        &opt.no_packets,
        dst_ip,
        dst_mac,
    );

    // Apply zerocopy setting to all generated XSK mappings
    for xsk in &mut config.wabrix.xsk {
        xsk.zerocopy = opt.zerocopy;
    }

    // --- SYSTEM CHECKS ---
    let worker_cores: Vec<usize> = config.wabrix.workers.iter().map(|w| w.core).collect();
    let interfaces: Vec<String> = config.wabrix.ifaces.iter().map(|i| i.name.clone()).collect();
    wabrix_base::system::check_system(&worker_cores, &interfaces);

    // --- CONFIGURE NETWORK INTERFACES ---
    info!("Configuring network interfaces...");
    let _configured_ifaces =
        wabrix_lib::config::interface_setup::check_or_configure_interfaces(&config.wabrix.ifaces, &config.wabrix.xsk, true)?;

    let unique_ifaces: Vec<String> = config.wabrix.ifaces.iter().map(|i| i.name.clone()).collect();

    // we must keep _xdp_links alive for the lifetime of the program
    let (_interface_skels, _xdp_links) = configure_bpf::setup_bpf(unique_ifaces)?;

    // Record initial NIC tx_packets before starting (no logging)
    let initial_stats = get_nic_tx_packets_detailed(&opt.interface);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    info!("All interfaces configured successfully");

    // --- BUILD SYSTEM FROM CONFIGURATION ---
    info!("Building XDP system from configuration...");
    let sys_builder = config.to_system_builder()?;
    let xbricks_system = match sys_builder.build(None, platform_data.clone(), None) {
        Ok(system) => system,
        Err(e) => {
            eprintln!("Failed to build XDP system: {}", e);
            eprintln!("Hint: If you're experiencing issues, try using --no-zerocopy flag");
            std::process::exit(1);
        }
    };

    wabrix_base::system::wait_for_physical_link(&opt.interface).await;

    let shutdown = Arc::new(AtomicBool::new(false));
    let workers_completed = Arc::new(AtomicUsize::new(0));
    let total_workers = config.wabrix.workers.len();

    info!(
        "Configuration specifies {} workers, expecting {} completions",
        total_workers, total_workers
    );

    let handles = xbricks_system.spawn_workers(shutdown.clone(), workers_completed.clone(), true)?;

    info!(
        "Spawned {} worker thread handles. Waiting for completion or Ctrl-C...",
        handles.len()
    );

    let start_time = std::time::Instant::now();

    // Create a future that monitors worker completion
    let workers_completed_clone = workers_completed.clone();
    let shutdown_clone = shutdown.clone();
    let completion_monitor = async move {
        let mut last_reported = 0;
        loop {
            let completed = workers_completed_clone.load(Ordering::Acquire);
            if completed != last_reported {
                info!("Worker completion counter: {}/{}", completed, total_workers);
                last_reported = completed;
            }
            if completed >= total_workers {
                info!("All {} workers have finished generating packets", total_workers);
                shutdown_clone.store(true, Ordering::Release);
                // Give workers a moment to see the shutdown flag
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };

    // Wait for either Ctrl-C or all workers to complete
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, initiating shutdown...");
            shutdown.store(true, Ordering::Release);
        }
        _ = completion_monitor => {
            info!("Packet generation complete, shutting down workers...");
        }
    }

    // Wait for all worker threads to exit cleanly
    info!("Waiting for worker threads to terminate...");
    let mut total_packets = 0u64;
    let mut max_burst_duration_cycles = 0u64;
    let mut max_burst_duration = std::time::Duration::ZERO;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(stats)) => {
                info!("Worker {} stopped:", idx);
                let stats: Vec<wabrix_lib::PipelineStats> = stats;
                for pstats in stats.iter() {
                    info!(
                        "  Pipeline '{}': {} packets, {} runs ({} idle)",
                        pstats.pipeline_name,
                        pstats.packets_processed.separated_string(),
                        pstats.runs_with_work.separated_string(),
                        pstats.runs_idle.separated_string()
                    );

                    // Print per-node statistics and collect burst duration
                    for node_stat in &pstats.node_stats {
                        info!(
                            "    Node {}: {} packets",
                            node_stat.node_id,
                            node_stat.packets_processed.separated_string()
                        );

                        if let Some(burst_cycles) = node_stat.burst_duration_cycles {
                            if burst_cycles > 0 {
                                max_burst_duration_cycles = max_burst_duration_cycles.max(burst_cycles);
                                info!("      Burst duration (TSC): {} cycles", burst_cycles.separated_string());
                            }
                        }
                        if let Some(burst_dur) = node_stat.burst_duration {
                            max_burst_duration = max_burst_duration.max(burst_dur);
                            info!("      Burst duration (time): {:.6} seconds", burst_dur.as_secs_f64());
                        }
                    }

                    total_packets += pstats.packets_processed;
                }
            }
            Ok(Err(e)) => {
                info!("Worker {} stopped with error: {}", idx, e);
            }
            Err(e) => {
                info!("Worker {} join error: {}", idx, e);
            }
        }
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    // Calculate PPS using burst duration if available (prefer std::time over TSC)
    let burst_duration_secs = if max_burst_duration.as_secs_f64() > 0.0 {
        max_burst_duration.as_secs_f64()
    } else if max_burst_duration_cycles > 0 {
        max_burst_duration_cycles as f64 / platform_data.tsc_frequency as f64
    } else {
        0.0
    };

    let pps = if burst_duration_secs > 0.0 {
        total_packets as f64 / burst_duration_secs
    } else if elapsed_secs > 0.0 {
        total_packets as f64 / elapsed_secs
    } else {
        0.0
    };

    info!("All workers stopped. Shutdown complete.");
    println!("\n=== Summary ===");
    println!("Total packets: {}", total_packets.separated_string());
    println!("Runtime: {:.3} seconds", elapsed_secs);
    if max_burst_duration.as_secs_f64() > 0.0 {
        println!("Burst duration (time): {:.6} seconds", max_burst_duration.as_secs_f64());
    }
    if max_burst_duration_cycles > 0 {
        let burst_duration_cycles_secs = max_burst_duration_cycles as f64 / platform_data.tsc_frequency as f64;
        println!(
            "Burst duration (TSC): {:.6} seconds ({} cycles)",
            burst_duration_cycles_secs,
            max_burst_duration_cycles.separated_string()
        );
    }
    if pps < 1000. {
        println!("Average PPS: {:.0}", pps);
    } else {
        println!("Average PPS: {}", (pps as u64).separated_string());
    }

    // Print NIC tx_packets statistics and delta from start
    print_nic_stats_with_delta(&opt.interface, initial_stats);

    Ok(())
}
