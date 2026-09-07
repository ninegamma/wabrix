mod metrics_server;

use anyhow::Context;
use clap::Parser;
use log::{debug, error, info};
use separator::Separatable;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{self, BufRead};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, RwLock};

use wabrix_base::system::PlatformData;
use wabrix_base::telemetry::{
    spawn_pipeline_telemetry, spawn_telemetry_ice_for_ifaces, TMLiveStats,
};
use wabrix_base::ControlEvent;
use wabrix_lib::config::hot_config::SharedSystemState;
use wabrix_lib::config::reconciliation::{reconcile_config, ReconciliationResult};
use wabrix_lib::probe_kernel_fds;

#[derive(Debug, Parser)]
struct Opt {
    /// Path to wabrix.toml configuration file
    #[clap(short, long, default_value = "wabrix.toml")]
    config: String,
}

fn print_wabrix_section(path: &str) {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return;
    };

    let mut section = Vec::new();
    let mut in_wabrix_section = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            if in_wabrix_section {
                break;
            }
            in_wabrix_section = trimmed == "[wabrix]";
        }
        if in_wabrix_section {
            section.push(line);
        }
    }

    if !section.is_empty() {
        println!("{}\n", section.join("\n"));
    }
}

/// Commands sent from the CLI terminal to the main lifecycle loop
#[derive(Debug, Clone, PartialEq)]
pub enum SystemCommand {
    Start,
    Stop,
    Apply(String),
    Check,
    Shutdown,
}

struct NamespaceCleanup {
    namespaces: HashSet<String>,
}

impl NamespaceCleanup {
    fn new() -> Self {
        Self {
            namespaces: HashSet::new(),
        }
    }

    fn track(&mut self, namespace: String) {
        self.namespaces.insert(namespace);
    }

    fn cleanup(&mut self) {
        if self.namespaces.is_empty() {
            return;
        }

        let mut namespaces: Vec<String> = self.namespaces.drain().collect();
        namespaces.sort();
        if let Err(err) =
            wabrix_lib::config::interface_setup::remove_created_namespaces(&namespaces)
        {
            error!("Namespace cleanup failed: {}", err);
        }
    }
}

impl Drop for NamespaceCleanup {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let mut terminate_signal =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("failed to install SIGTERM handler")?;

    // Telemetry stays Tokio broadcast (control plane only)
    let (telemetry_tx, _) = tokio::sync::broadcast::channel::<TMLiveStats>(100);

    // Control events use crossbeam for lock-free synchronous polling on the hot path
    let (ctrl_tx, ctrl_rx) = crossbeam_channel::bounded::<Arc<ControlEvent>>(100);

    // Spawn Prometheus metrics server
    metrics_server::spawn_metrics_server(telemetry_tx.subscribe());

    log::info!("Starting Wabrix Engine...");
    let opt = Opt::parse();
    let platform_data = PlatformData::detect();
    info!(
        "TSC frequency: {} Hz",
        platform_data.tsc_frequency.separated_string()
    );

    wabrix_base::system::lock_process_memory().expect("Failed to lock process memory");

    // - PROBE INTERFACES
    wabrix_base::system::probe_xdp_interfaces();

    // - LOAD CONFIGURATION AND DETERMINE INITIAL STATE
    info!("Evaluating configuration from: {}", opt.config);

    let (initial_config, mut dataplane_active) =
        match wabrix_lib::toml_parser::WabricksConfig::from_file(&opt.config) {
            Ok(config) => {
                print_wabrix_section(&opt.config);
                info!(
                    "Configuration loaded and validated successfully. Initiating START sequence."
                );
                (config, true)
            }
            Err(e) => {
                error!(
                    "Invalid or missing configuration file at {}: {}",
                    opt.config, e
                );
                info!("System initialized in STOP state. Awaiting valid configuration via CLI.");
                (wabrix_lib::toml_parser::WabricksConfig::default(), false)
            }
        };

    // Use ArcSwap for configuration to ensure RCU compliance across threads
    let shared_config = Arc::new(arc_swap::ArcSwap::from_pointee(initial_config));

    // RwLock is acceptable here ONLY if path updates are strictly confined to Tokio tasks
    let config_path = Arc::new(RwLock::new(opt.config.clone()));
    let initial_state = SharedSystemState::new(HashMap::new());
    let active_shared_state = Arc::new(arc_swap::ArcSwap::new(initial_state));
    let mut namespace_cleanup = NamespaceCleanup::new();

    // - SETUP CONTROL CHANNEL
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SystemCommand>(10);

    // Spawn CLI listener on an OS thread so Tokio shutdown is not blocked by stdin.
    let cli_cmd_tx = cmd_tx.clone();
    let cli_ctrl_tx = ctrl_tx.clone();

    thread::spawn(move || {
        println!("=========================================");
        println!("Wabrix Declarative CLI Ready.");
        println!("Prometheus Exporter: http://localhost:9090/metrics");
        match env::current_dir() {
            Ok(dir) => println!("Working directory is: {}", dir.display()),
            Err(err) => println!("Working directory is unavailable ({})", err),
        }
        println!(
            "Commands: start, stop, apply <file.toml>, check, ctrl <address> <opcode>, shutdown"
        );
        println!("=========================================");

        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = match line {
                Ok(line) => line,
                Err(err) => {
                    eprintln!("CLI input error: {}", err);
                    break;
                }
            };

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0].to_lowercase().as_str() {
                "start" => {
                    if cli_cmd_tx.blocking_send(SystemCommand::Start).is_err() {
                        break;
                    }
                }
                "stop" => {
                    if cli_cmd_tx.blocking_send(SystemCommand::Stop).is_err() {
                        break;
                    }
                }
                "check" => {
                    if cli_cmd_tx.blocking_send(SystemCommand::Check).is_err() {
                        break;
                    }
                }
                "apply" => {
                    let path = parts.get(1).map(|s| s.to_string()).unwrap_or_else(|| "wabrix.toml".to_string());
                    if cli_cmd_tx.blocking_send(SystemCommand::Apply(path)).is_err() {
                        break;
                    }
                }
                "ctrl" => {
                    let address_pattern = parts.get(1).unwrap_or(&"*").to_string();
                    let opcode = parts.get(2).and_then(|s| s.parse::<u32>().ok()).unwrap_or(1);
                    let payload: Vec<u8> = parts.iter().skip(3)
                        .filter_map(|s| s.parse::<u8>().ok())
                        .collect();

                    println!("=> Broadcasting ControlEvent (address: {}, opcode: {}, payload: {:?})", address_pattern, opcode, payload);
                    let event = Arc::new(ControlEvent { address_pattern, opcode, payload });
                    let _ = cli_ctrl_tx.send(event);
                }
                "shutdown" | "exit" | "quit" => {
                    let _ = cli_cmd_tx.blocking_send(SystemCommand::Shutdown);
                    break;
                }
                _ => println!("Unknown command. Use: start, stop, apply <file>, check, ctrl <address> <opcode> [payload_bytes], shutdown"),
            }
        }
    });

    loop {
        let next_cmd;

        if dataplane_active {
            info!("--- Initializing Data Plane ---");

            let action = {
                // Reload config from disk
                let mut current_config = {
                    let path = config_path.read().await;
                    match wabrix_lib::toml_parser::WabricksConfig::from_file(&*path) {
                        Ok(fresh) => {
                            info!("Configuration reloaded from: {}", *path);
                            // Atomically swap the configuration pointer for RCU compliance
                            // The data plane will immediately observe the new pointer on its next batch iteration
                            shared_config.store(std::sync::Arc::new(fresh.clone()));
                            fresh
                        }
                        Err(e) => {
                            error!("Failed to reload config from {}: {}. Using last known good config.", *path, e);
                            // Dereference the Guard returned by load to access the Arc, then clone the underlying config
                            (**shared_config.load()).clone()
                        }
                    }
                };

                // System Checks
                let worker_cores: Vec<usize> = current_config
                    .wabrix
                    .workers
                    .iter()
                    .map(|w| w.core)
                    .collect();
                let interfaces: Vec<String> = current_config
                    .wabrix
                    .ifaces
                    .iter()
                    .map(|i| i.name.clone())
                    .collect();
                let irq_assignments: Vec<(String, usize)> = current_config
                    .wabrix
                    .ifaces
                    .iter()
                    .filter_map(|iface| {
                        iface
                            .irq_core
                            .map(|irq_core| (iface.name.clone(), irq_core))
                    })
                    .collect();
                let interface_checks = current_config.interface_check_configs();
                wabrix_base::system::check_system(
                    &worker_cores,
                    &interface_checks,
                    &irq_assignments,
                );

                // Configure Network Interfaces
                info!("Configuring network interfaces...");
                let configured_ifaces =
                    wabrix_lib::config::interface_setup::check_or_configure_interfaces(
                        &current_config.wabrix.ifaces,
                        &current_config.wabrix.xsk,
                        false,
                    )
                    .context("Failed to configure network interfaces")?;
                for namespace in configured_ifaces
                    .iter()
                    .filter_map(|iface| iface.created_namespace.as_ref())
                {
                    namespace_cleanup.track(namespace.clone());
                }

                // NAPI sysctls are process-scoped while this dataplane run is live.
                // The guard snapshots the original values and restores them on teardown
                // or any subsequent startup error.
                let _napi_sysctl_guard = wabrix_lib::config::napi_sysctl::NapiSysctlGuard::apply(
                    &current_config.wabrix.ifaces,
                    &current_config.wabrix.xsk,
                )
                .context("Failed to configure NAPI system parameters")?;

                // Setup BPF
                let (interface_skels, _xdp_links) =
                    wabrix_lib::config::configure_bpf::setup_bpf(interfaces.clone())
                        .context("Failed to setup BPF programs")?;

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                // Build System
                info!("Building XDP system from configuration...");
                let sys_builder = current_config
                    .to_system_builder()
                    .context("Failed to create system builder")?;

                let builder_ctrl_rx = Some(ctrl_rx.clone());

                let wabrix_system = tokio::task::block_in_place(|| {
                    sys_builder
                        .build(
                            Some(&interface_skels),
                            platform_data.clone(),
                            builder_ctrl_rx,
                        )
                        .map_err(|e| anyhow::anyhow!("Failed to build XDP system: {}", e))
                })?;

                let system_state = wabrix_system.shared_state.clone();
                active_shared_state.store(system_state.clone());

                // Wait for physical links
                for iface in &current_config.wabrix.ifaces {
                    wabrix_base::system::wait_for_physical_link(&iface.name).await;
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                // Spawn Workers
                let shutdown = Arc::new(AtomicBool::new(false));
                let workers_completed = Arc::new(AtomicUsize::new(0));
                let pipeline_registry = Arc::clone(&wabrix_system.telemetry_registry);
                let handles = wabrix_system
                    .spawn_workers(shutdown.clone(), workers_completed.clone(), false)
                    .context("Failed to spawn worker threads")?;

                info!(
                    "All {} worker threads spawned. Data Plane is LIVE.",
                    handles.len()
                );

                spawn_telemetry_ice_for_ifaces(
                    telemetry_tx.clone(),
                    &interfaces,
                    pipeline_registry.clone(),
                    shutdown.clone(),
                )?;

                spawn_pipeline_telemetry(
                    telemetry_tx.clone(),
                    pipeline_registry,
                    shutdown.clone(),
                )?;

                // Wait for CLI events or worker completion while running
                let num_workers = handles.len();

                let cmd = loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => break SystemCommand::Shutdown,
                        _ = terminate_signal.recv() => break SystemCommand::Shutdown,
                        Some(c) = cmd_rx.recv() => {
                            match c {
                                SystemCommand::Apply(new_path) => {
                                    info!("Applying new configuration from: {}", new_path);
                                    let target_config_result = wabrix_lib::toml_parser::WabricksConfig::from_file(&new_path);

                                    match target_config_result {
                                        Ok(target_config) => {
                                            print_wabrix_section(&new_path);
                                            match reconcile_config(system_state.clone(), &current_config, &target_config) {
                                                ReconciliationResult::FullRestart => {
                                                    info!("Structural changes detected. Initiating full data plane restart.");
                                                    let mut path_guard = config_path.write().await;
                                                    *path_guard = new_path;
                                                    break SystemCommand::Start;
                                                },
                                                ReconciliationResult::LiveUpdated(events) => {
                                                    info!("Live configuration updates applied. Dispatching {} control events.", events.len());

                                                    // Dispatch imperative events generated from Wasm diffs
                                                    for event in events {
                                                        info!(
                                                            "Dispatching control event opcode {} to {}",
                                                            event.opcode,
                                                            event.address_pattern
                                                        );
                                                        let _ = ctrl_tx.send(event);
                                                    }

                                                    // Keep the local baseline synchronized for future comparisons
                                                    current_config = target_config;
                                                },
                                                ReconciliationResult::NoAction => {
                                                    info!("Target configuration matches current state. No action taken.");
                                                },
                                                ReconciliationResult::Error(err) => {
                                                    error!("Reconciliation failed: {}. Configuration remains unchanged.", err);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to load target configuration: {}. Ignoring apply command.", e);
                                        }
                                    }
                                },
                                other_cmd => break other_cmd,
                            }
                        },
                        _ = async {
                            loop {
                                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                                if num_workers > 0 && workers_completed.load(Ordering::Acquire) >= num_workers {
                                    break;
                                }
                            }
                        } => {
                            info!("All workers have completed their execution. Ending single run.");
                            break SystemCommand::Stop;
                        }
                    }
                };

                // Teardown data plane
                info!("Stopping data plane workers...");

                debug!("Evicting sockets from Kernel BPF maps...");
                wabrix_lib::config::configure_bpf::unregister_sockets_from_bpf(&interface_skels);

                shutdown.store(true, Ordering::Release);

                for (idx, handle) in handles.into_iter().enumerate() {
                    match handle.await {
                        Ok(Ok(worker_stats)) => {
                            for stats in worker_stats {
                                stats.print_stats(idx);
                            }
                        }
                        Ok(Err(e)) => error!("Worker {} stopped with error: {}", idx, e),
                        Err(e) => error!("Worker {} join error: {}", idx, e),
                    }
                }

                active_shared_state.store(SharedSystemState::new(HashMap::new()));

                debug!("Detaching BPF programs from physical interfaces...");
                drop(_xdp_links);
                drop(interface_skels);

                cmd
            };

            probe_kernel_fds("POST-DROP");
            info!("Userspace destroyed. Yielding to kernel for graceful AF_XDP unbind...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            info!("Data plane teardown complete.");

            next_cmd = action;
        } else {
            // Idle state handling
            info!("Dataplane is currently OFFLINE. Awaiting CLI commands...");

            next_cmd = tokio::select! {
                _ = tokio::signal::ctrl_c() => SystemCommand::Shutdown,
                _ = terminate_signal.recv() => SystemCommand::Shutdown,
                Some(c) = cmd_rx.recv() => c,
                else => SystemCommand::Shutdown,
            };
        }

        // State machine evaluation
        match next_cmd {
            SystemCommand::Start => {
                info!("Received START command. Booting hardware...");
                dataplane_active = true;
            }
            SystemCommand::Stop => {
                info!("Received STOP command. Entering IDLE state...");
                dataplane_active = false;
            }
            SystemCommand::Apply(path) => {
                info!(
                    "Received APPLY command in offline mode. Setting target config to {}",
                    path
                );
                let mut path_guard = config_path.write().await;
                *path_guard = path;
                dataplane_active = true;
            }
            SystemCommand::Check => {
                info!("Received CHECK command. Validating configuration...");
                let current_config = { shared_config.load_full().as_ref().clone() };

                let worker_cores: Vec<usize> = current_config
                    .wabrix
                    .workers
                    .iter()
                    .map(|w| w.core)
                    .collect();
                let irq_assignments: Vec<(String, usize)> = current_config
                    .wabrix
                    .ifaces
                    .iter()
                    .filter_map(|iface| {
                        iface
                            .irq_core
                            .map(|irq_core| (iface.name.clone(), irq_core))
                    })
                    .collect();
                let interface_checks = current_config.interface_check_configs();
                wabrix_base::system::check_system(
                    &worker_cores,
                    &interface_checks,
                    &irq_assignments,
                );

                info!("Validating pipeline topology...");
                match current_config.to_system_builder() {
                    Ok(_) => info!("Configuration and topology are valid!"),
                    Err(e) => error!("Topology validation failed: {}", e),
                }
            }
            SystemCommand::Shutdown => {
                info!("Received SHUTDOWN command. Exiting application...");
                break;
            }
        }
    }

    namespace_cleanup.cleanup();

    info!("Wabrix shut down gracefully.");
    Ok(())
}
