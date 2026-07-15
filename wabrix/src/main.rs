use std::collections::HashMap;
use anyhow::Context;
use clap::Parser;
use log::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use separator::Separatable;
use tokio::sync::{mpsc, RwLock};
use tokio::io::{AsyncBufReadExt, BufReader};

use wabrix_lib::config::hot_config::SharedSystemState;
use wabrix_base::ControlEvent;
use wabrix_lib::probe_kernel_fds;
use wabrix_base::telemetry::{spawn_telemetry_ice_for_ifaces, spawn_pipeline_telemetry, TMLiveStats};
use wabrix_base::system::PlatformData;

#[derive(Debug, Parser)]
struct Opt {
    /// Path to wabrix.toml configuration file
    #[clap(short, long, default_value = "wabrix.toml")]
    config: String,
}

/// Commands sent from the CLI terminal to the main lifecycle loop
#[derive(Debug, Clone, PartialEq)]
pub enum SystemCommand {
    Start,
    Stop,
    Check,
    Restart,
    Shutdown,
    Run,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize standard terminal logging (e.g., via env_logger)
    // Run with `RUST_LOG=info cargo run` to see logs
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let (telemetry_tx, _) = tokio::sync::broadcast::channel::<TMLiveStats>(100);
    let (ctrl_tx, _) = tokio::sync::broadcast::channel::<Arc<ControlEvent>>(100);

    log::info!("Starting Wabrix Open Source Engine...");
    let opt = Opt::parse();
    let platform_data = PlatformData::detect();
    info!("TSC frequency: {} Hz", platform_data.tsc_frequency.separated_string());
    wabrix_base::system::probe_xdp_interfaces();

    // --- LOAD CONFIGURATION ---
    info!("Loading configuration from: {}", opt.config);
    let config = wabrix_lib::toml_parser::XBricksConfig::from_file(&opt.config)
        .with_context(|| format!("Failed to load configuration from {}", opt.config))?;
    info!("Configuration loaded and validated successfully");

    let shared_config = Arc::new(RwLock::new(config));
    let config_path = Arc::new(RwLock::new(opt.config.clone()));

    let initial_state = SharedSystemState::new(HashMap::new());
    let active_shared_state = Arc::new(arc_swap::ArcSwap::new(initial_state));

    // --- SETUP CONTROL CHANNEL ---
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SystemCommand>(10);

    // ==========================================================
    // SPAWN ASYNC CLI LISTENER
    // ==========================================================
    let cli_cmd_tx = cmd_tx.clone();
    let cli_ctrl_tx = ctrl_tx.clone(); // <-- NEW: Give the CLI access to the plugin control channel

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();

        println!("=========================================");
        println!("Wabrix CLI Ready.");
        println!("Commands: start, stop, restart, check, run, trigger <opcode>, shutdown");
        println!("=========================================");

        while let Ok(Some(line)) = reader.next_line().await {
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.is_empty() { continue; }

            match parts[0].to_lowercase().as_str() {
                "start" => { let _ = cli_cmd_tx.send(SystemCommand::Start).await; }
                "stop" => { let _ = cli_cmd_tx.send(SystemCommand::Stop).await; }
                "restart" => { let _ = cli_cmd_tx.send(SystemCommand::Restart).await; }
                "check" => { let _ = cli_cmd_tx.send(SystemCommand::Check).await; }
                "run" => { let _ = cli_cmd_tx.send(SystemCommand::Run).await; }
                "online" | "trigger" => {
                    // If you type `trigger <opcode> <byte1> <byte2>...`
                    let opcode = parts.get(1).and_then(|s| s.parse::<u32>().ok()).unwrap_or(1);

                    // Parse any remaining parts as u8 bytes for the payload
                    let payload: Vec<u8> = parts.iter().skip(2)
                        .filter_map(|s| s.parse::<u8>().ok())
                        .collect();

                    println!("=> Broadcasting ControlEvent (opcode: {}, payload: {:?}) to plugins...", opcode, payload);

                    let event = Arc::new(ControlEvent {
                        address_pattern: "*".to_string(), // Broadcast to all Wasm plugins
                        opcode,
                        payload,
                    });
                    let _ = cli_ctrl_tx.send(event);
                }
                "shutdown" | "exit" | "quit" => {
                    let _ = cli_cmd_tx.send(SystemCommand::Shutdown).await;
                    break;
                }
                _ => println!("Unknown command. Use: start, stop, restart, check, trigger <opcode> [payload_bytes...], shutdown"),
            }
        }
    });

    // ==========================================================
    // DATA PLANE LIFECYCLE LOOP (State Machine)
    // ==========================================================
    let mut dataplane_active = true;

    loop {
        let next_cmd;

        if dataplane_active {
            info!("--- Initializing Data Plane ---");

            let action = {
                // 1. Reload config from disk
                let current_config = {
                    let path = config_path.read().await;
                    match wabrix_lib::toml_parser::XBricksConfig::from_file(&*path) {
                        Ok(fresh) => {
                            info!("Configuration reloaded from: {}", *path);
                            let mut guard = shared_config.write().await;
                            *guard = fresh.clone();
                            fresh
                        }
                        Err(e) => {
                            error!("Failed to reload config from {}: {}. Using last known good config.", *path, e);
                            shared_config.read().await.clone()
                        }
                    }
                };

                // 2. System Checks
                let worker_cores: Vec<usize> = current_config.wabrix.workers.iter().map(|w| w.core).collect();
                let interfaces: Vec<String> = current_config.wabrix.ifaces.iter().map(|i| i.name.clone()).collect();
                wabrix_base::system::check_system(&worker_cores, &interfaces);

                // 3. Configure Network Interfaces
                info!("Configuring network interfaces...");
                let _configured_ifaces = wabrix_lib::config::interface_setup::check_or_configure_interfaces(
                    &current_config.wabrix.ifaces,
                    &current_config.wabrix.xsk,
                    false,
                ).context("Failed to configure network interfaces").unwrap();

                // 4. Setup BPF
                let (interface_skels, _xdp_links) = wabrix_lib::config::configure_bpf::setup_bpf(interfaces.clone())
                    .context("Failed to setup BPF programs").unwrap();

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                // 5. Build System
                info!("Building XDP system from configuration...");
                let sys_builder = current_config.to_system_builder()
                    .context("Failed to create system builder").unwrap();

                let builder_ctrl_tx = Some(ctrl_tx.clone());

                let xbricks_system = tokio::task::block_in_place(|| {
                    sys_builder.build(Some(&interface_skels), platform_data.clone(), builder_ctrl_tx)
                        .map_err(|e| anyhow::anyhow!("Failed to build XDP system: {}", e))
                })?;

                active_shared_state.store(xbricks_system.shared_state.clone());

                // 6. Wait for links
                for iface in &current_config.wabrix.ifaces {
                    wabrix_base::system::wait_for_physical_link(&iface.name).await;
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

                spawn_telemetry_ice_for_ifaces(telemetry_tx.clone(), &interfaces, pipeline_registry.clone(), shutdown.clone())?;
                spawn_pipeline_telemetry(telemetry_tx.clone(), pipeline_registry, shutdown.clone())?;

                // ==========================================================
                // WAIT FOR CLI EVENTS OR WORKER COMPLETION WHILE RUNNING
                // ==========================================================
                let num_workers = handles.len();
                let cmd = tokio::select! {
                    _ = tokio::signal::ctrl_c() => SystemCommand::Shutdown,
                    Some(c) = cmd_rx.recv() => c,
                    _ = async {
                        loop {
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            if num_workers > 0 && workers_completed.load(Ordering::Acquire) >= num_workers {
                                break;
                            }
                        }
                    } => {
                        info!("All workers have completed their execution. Ending single run.");
                        SystemCommand::Stop
                    },
                    else => SystemCommand::Shutdown,
                };

                // ==========================================================
                // TEARDOWN DATA PLANE
                // ==========================================================
                info!("Stopping data plane workers...");

                debug!("Evicting sockets from Kernel BPF maps...");
                wabrix_lib::config::configure_bpf::unregister_sockets_from_bpf(&interface_skels);

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
                    wabrix_lib::config::hot_config::SharedSystemState::new(std::collections::HashMap::new())
                );

                debug!("Detaching BPF programs from physical interfaces...");
                drop(_xdp_links);
                drop(interface_skels);

                cmd
            }; // <--- 🔥 RUST SCOPE DESTROYS ALL SOCKETS HERE 🔥

            probe_kernel_fds("POST-DROP");
            info!("Userspace destroyed. Yielding to kernel for graceful AF_XDP unbind...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            info!("Data plane teardown complete.");

            next_cmd = action;

        } else {
            // ==========================================================
            // IDLE STATE
            // ==========================================================
            info!("Dataplane is currently OFFLINE. Awaiting CLI commands...");

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
            SystemCommand::Run => {
                info!("Received RUN command. Triggering a single run of the dataplane...");
                dataplane_active = true;
            }
            SystemCommand::Stop => {
                info!("Received STOP command. Entering IDLE state...");
                dataplane_active = false;
            }
            SystemCommand::Restart => {
                info!("Received RESTART command. Rebooting data plane...");
                dataplane_active = true;
            }
            SystemCommand::Check => {
                info!("Received CHECK command. Validating configuration...");
                let current_config = {
                    let guard = shared_config.read().await;
                    guard.clone()
                };

                let worker_cores: Vec<usize> = current_config.wabrix.workers.iter().map(|w| w.core).collect();
                let interfaces: Vec<String> = current_config.wabrix.ifaces.iter().map(|i| i.name.clone()).collect();
                wabrix_base::system::check_system(&worker_cores, &interfaces);

                info!("Validating pipeline topology...");
                match current_config.to_system_builder() {
                    Ok(_) => info!("✅ Configuration and topology are valid!"),
                    Err(e) => error!("❌ Topology validation failed: {}", e),
                }
            }
            SystemCommand::Shutdown => {
                info!("Received SHUTDOWN command. Exiting application...");
                break;
            }
        }
    }

    info!("Wabrix shut down gracefully.");
    Ok(())
}