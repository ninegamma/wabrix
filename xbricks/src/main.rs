pub mod api;
mod pcap;
pub mod logger;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use anyhow::Context;
use clap::Parser;
use log::{debug, error, info, LevelFilter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use separator::Separatable;
use tokio::sync::{mpsc, RwLock};
use xbricks_lib::config::hot_config::SharedSystemState;
use logger::WsLogger;
use xbricks_lib::probe_kernel_fds;
use xbricks_base::telemetry::{spawn_telemetry_ice_for_ifaces, spawn_pipeline_telemetry, TMLiveStats};
use xbricks_base::system::PlatformData;
use crate::api::{spawn_management_api, AppState, SystemCommand};

#[derive(Debug, Parser)]
struct Opt {
    /// Path to xbricks.toml configuration file
    #[clap(short, long, default_value = "xbricks.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Create the broadcast channel FIRST
    let (console_tx, _rx) = tokio::sync::broadcast::channel::<String>(1000);
    let (telemetry_tx, _) = tokio::sync::broadcast::channel::<TMLiveStats>(100);

    // 2. Hijack the global `log` crate and attach it to our channel
    let logger = WsLogger::new(console_tx.clone());
    log::set_boxed_logger(Box::new(logger))?;

    // Open the floodgates globally; WsLogger will filter them!
    log::set_max_level(log::LevelFilter::Debug);

    //let log_level_str = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    //let log_level = LevelFilter::from_str(&log_level_str).unwrap_or(LevelFilter::Info);
    //log::set_max_level(log_level);

    log::info!("Starting XBricks System...");
    let opt = Opt::parse();
    let platform_data = PlatformData::detect();
    info!("TSC frequency: {} Hz", platform_data.tsc_frequency.separated_string());
    xbricks_base::system::probe_xdp_interfaces();

    // --- LOAD CONFIGURATION ---
    info!("Loading configuration from: {}", opt.config);
    let config = xbricks_lib::toml_parser::XBricksConfig::from_file(&opt.config)
        .with_context(|| format!("Failed to load configuration from {}", opt.config))?;
    info!("Configuration loaded and validated successfully");

    // Wrap config in an Arc<RwLock> so both the Main Loop and the Axum API can read/write it
    let shared_config = Arc::new(RwLock::new(config));

    // --- SETUP CONTROL CHANNEL ---
    // This channel allows the Axum API to send commands (Restart/Shutdown) to the main loop
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SystemCommand>(10);

    // ==========================================================
    // START THE AXUM MANAGEMENT API (Runs OUTSIDE the loop)
    // ==========================================================
    info!("Starting Management API on http://0.0.0.0:3000...");
    let initial_state = SharedSystemState::new(HashMap::new());
    // Create a swap-able pointer to hold the live system state
    let active_shared_state = Arc::new(arc_swap::ArcSwap::new(initial_state));

    let app_state = AppState {
        config: shared_config.clone(),
        shared_state: active_shared_state.clone(),
        console_tx: console_tx.clone(),
        cmd_tx: cmd_tx.clone(),
        config_path: Arc::new(RwLock::new(opt.config.clone())),
        telemetry_tx: telemetry_tx.clone(),
    };
    let (axum_shutdown_tx, axum_join_handle) = spawn_management_api(app_state);

    // ==========================================================
    // DATA PLANE LIFECYCLE LOOP (State Machine)
    // ==========================================================

    // We start in the RUNNING state when launched from the console.
    let mut dataplane_active = true;

    loop {
        let mut next_cmd = SystemCommand::Shutdown; // Default failsafe

        if dataplane_active {
            info!("--- Initializing Data Plane ---");

            // We use a block here to strictly enforce Rust's Drop semantics for hardware
            let action = {
                // 1. Read the latest config
                let current_config = {
                    let guard = shared_config.read().await;
                    guard.clone()
                };

                // 2. System Checks
                let worker_cores: Vec<usize> = current_config.xbricks.workers.iter().map(|w| w.core).collect();
                let interfaces: Vec<String> = current_config.xbricks.ifaces.iter().map(|i| i.name.clone()).collect();
                xbricks_base::system::check_system(&worker_cores, &interfaces);

                // 3. Configure Network Interfaces
                info!("Configuring network interfaces...");
                let _configured_ifaces = xbricks_lib::config::interface_setup::check_or_configure_interfaces(
                    &current_config.xbricks.ifaces,
                    &current_config.xbricks.xsk,
                    false,
                ).context("Failed to configure network interfaces").unwrap();

                // 4. Setup BPF
                let (interface_skels, _xdp_links) = xbricks_lib::config::configure_bpf::setup_bpf(interfaces.clone())
                    .context("Failed to setup BPF programs").unwrap();

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                // 5. Build System
                info!("Building XDP system from configuration...");
                let sys_builder = current_config.to_system_builder()
                    .context("Failed to create system builder").unwrap();

                let xbricks_system = tokio::task::block_in_place(|| {
                    sys_builder.build(Some(&interface_skels), platform_data.clone())
                        .map_err(|e| anyhow::anyhow!("Failed to build XDP system: {}", e))
                })?;

                active_shared_state.store(xbricks_system.shared_state.clone());

                // 6. Wait for links
                for iface in &current_config.xbricks.ifaces {
                    xbricks_base::system::wait_for_physical_link(&iface.name).await;
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                // 7. Spawn Workers
                let shutdown = Arc::new(AtomicBool::new(false));
                let workers_completed = Arc::new(AtomicUsize::new(0));
                let pipeline_registry = Arc::clone(&xbricks_system.telemetry_registry);
                let handles = xbricks_system
                    .spawn_workers(shutdown.clone(), workers_completed.clone(), false)
                    .context("Failed to spawn worker threads")?;

                info!("All {} worker threads spawned. Data Plane is LIVE.", handles.len());

                spawn_telemetry_ice_for_ifaces(telemetry_tx.clone(), &interfaces, shutdown.clone())?;
                spawn_pipeline_telemetry(telemetry_tx.clone(), pipeline_registry, shutdown.clone())?;

                // ==========================================================
                // WAIT FOR EVENTS WHILE RUNNING
                // ==========================================================
                let cmd = tokio::select! {
                    _ = tokio::signal::ctrl_c() => SystemCommand::Shutdown,
                    Some(c) = cmd_rx.recv() => c,
                    else => SystemCommand::Shutdown,
                };

                // ==========================================================
                // TEARDOWN DATA PLANE
                // ==========================================================
                info!("Stopping data plane workers...");

                debug!("Evicting sockets from Kernel BPF maps...");
                xbricks_lib::config::configure_bpf::unregister_sockets_from_bpf(&interface_skels);

                shutdown.store(true, Ordering::Release);

                for (idx, handle) in handles.into_iter().enumerate() {
                    match handle.await {
                        Ok(Ok(worker_stats)) => {
                            for stats in worker_stats { stats.print_stats(idx); }
                        }
                        Ok(Err(e)) => error!("Worker {} stopped with error: {}", idx, e),
                        Err(e) => error!("Worker {} join error: {}", idx, e),
                    }
                }

                active_shared_state.store(
                    xbricks_lib::config::hot_config::SharedSystemState::new(std::collections::HashMap::new())
                );

                debug!("Detaching BPF programs from physical interfaces...");
                drop(_xdp_links);
                drop(interface_skels);

                cmd // Return the command out of the block to dictate the next state
            }; // <--- 🔥 RUST SCOPE DESTROYS ALL SOCKETS HERE 🔥

            // Kernel cleanup wait
            probe_kernel_fds("POST-DROP");
            info!("Userspace destroyed. Yielding to kernel for graceful AF_XDP unbind...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            info!("Data plane teardown complete.");

            next_cmd = action;

        } else {
            // ==========================================================
            // IDLE STATE (Dataplane is stopped, waiting for commands)
            // ==========================================================
            info!("Dataplane is currently OFFLINE. Management API is listening...");

            next_cmd = tokio::select! {
                _ = tokio::signal::ctrl_c() => SystemCommand::Shutdown,
                Some(c) = cmd_rx.recv() => c,
                else => SystemCommand::Shutdown,
            };
        }

        // ==========================================================
        // STATE MACHINE EVALUATION
        // ==========================================================
        match next_cmd {
            SystemCommand::Start => {
                info!("Received START command. Booting hardware...");
                dataplane_active = true;
            }
            SystemCommand::Stop => {
                info!("Received STOP command. Keeping API alive...");
                dataplane_active = false;
            }
            SystemCommand::Check => {
                // ==========================================================
                // CONFIGURATION DRY-RUN
                // ==========================================================
                info!("Received CHECK command. Validating configuration...");

                // 1. Read current memory state
                let current_config = {
                    let guard = shared_config.read().await;
                    guard.clone()
                };

                // 2. Validate OS/Hardware prerequisites
                info!("Running pre-flight system checks...");
                let worker_cores: Vec<usize> = current_config.xbricks.workers.iter().map(|w| w.core).collect();
                let interfaces: Vec<String> = current_config.xbricks.ifaces.iter().map(|i| i.name.clone()).collect();
                xbricks_base::system::check_system(&worker_cores, &interfaces);

                // 3. Validate Pipeline Topology (The "Blueprint")
                info!("Validating pipeline topology...");
                match current_config.to_system_builder() {
                    Ok(_) => info!("✅ Configuration and topology are valid! Ready to start."),
                    Err(e) => error!("❌ Topology validation failed: {}", e),
                }

                // IMPORTANT: We do NOT change `dataplane_active` here.
                // If it was stopped, it stays stopped. If it was running, it stays running.
            }
            SystemCommand::Shutdown => {
                info!("Received SHUTDOWN command. Exiting application...");
                break; // Break the master loop to trigger final shutdown
            }
        }
    } // <-- End of loop

    // ==========================================================
    // SHUTDOWN AXUM SERVER
    // ==========================================================
    info!("Shutting down Management API...");
    let _ = axum_shutdown_tx.send(());
    let _ = axum_join_handle.await;
    info!("Axum server exited safely.");

    Ok(())
}