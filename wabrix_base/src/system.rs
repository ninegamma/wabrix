use crate::frame_registry::get_tsc_start;
use macaddr::{MacAddr6 as MacAddress, ParseError};
use std::arch::x86_64::__cpuid;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

const CPU_CLOCK_PATH: &str = "/sys/devices/system/cpu/cpu0/cpufreq/base_frequency";
const PROC_CPUINFO_PATH: &str = "/proc/cpuinfo";

pub fn try_get_tsc_frequency() -> Option<u64> {
    // Step 1: Ask the CPU for the highest basic leaf it supports
    let max_leaf = __cpuid(0x00).eax;

    // Step 2: If it's too old to know what 0x15 is, abort and use the fallback
    if max_leaf < 0x15 {
        return None;
    }

    // Step 3: It is safe to query Leaf 0x15
    let result = __cpuid(0x15);

    if result.ebx == 0 || result.eax == 0 {
        return None;
    }

    let crystal_freq = result.ecx; // in Hz

    // Some CPUs support 0x15 but return 0 for the crystal frequency (ecx).
    // If this happens, we must also abort.
    if crystal_freq == 0 {
        return None;
    }

    // TSC frequency = (crystal_freq * ebx) / eax
    let tsc_freq = (crystal_freq as u64 * result.ebx as u64) / result.eax as u64;

    Some(tsc_freq)
}

#[derive(Clone, Default)]
pub struct PlatformData {
    pub tsc_frequency: u64, // base clock for rdtsc in Hz
}

impl PlatformData {
    pub fn detect() -> PlatformData {
        let tsc_freq = try_get_tsc_frequency();

        PlatformData {
            tsc_frequency: tsc_freq.unwrap_or_else(|| {
                // Fallback 1: Try reading cpufreq base_frequency (kHz -> Hz)
                if let Ok(mut f) = File::open(CPU_CLOCK_PATH) {
                    let mut khz_str = String::new();
                    if f.read_to_string(&mut khz_str).is_ok() {
                        if let Ok(khz) = khz_str.trim().parse::<u64>() {
                            return khz * 1000;
                        }
                    }
                }

                // Fallback 2: Read /proc/cpuinfo (MHz -> Hz)
                // This is essential for VMs and HP ProLiant servers with OS Power Control disabled
                if let Ok(mut f) = File::open(PROC_CPUINFO_PATH) {
                    let mut cpuinfo = String::new();
                    if f.read_to_string(&mut cpuinfo).is_ok() {
                        for line in cpuinfo.lines() {
                            if line.starts_with("cpu MHz") {
                                if let Some(mhz_str) = line.split(':').nth(1) {
                                    if let Ok(mhz) = mhz_str.trim().parse::<f64>() {
                                        return (mhz * 1_000_000.0) as u64;
                                    }
                                }
                            }
                        }
                    }
                }

                // Fallback 3: Ultimate safe default (2.5 GHz) to prevent crashes
                eprintln!("Warning: Could not detect CPU frequency. Defaulting to 2.5 GHz for TSC calculation.");
                2_500_000_000
            }),
        }
    }
}

use std::io::Error;

/// Locks all current and future memory allocations into physical RAM.
pub fn lock_process_memory() -> Result<(), Error> {
    // SAFETY:
    // - mlockall does not mutate memory directly or cause data races.
    // - It only modifies the kernel's virtual memory management behavior for this process.
    let result = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };

    if result != 0 {
        return Err(Error::last_os_error());
    }

    Ok(())
}

pub fn get_mac_from_ifname(ifname: &str) -> Result<MacAddress, ParseError> {
    let iface = Path::new("/sys/class/net").join(ifname).join("address");
    let mut macaddr = String::new();
    File::open(iface)
        .and_then(|mut f| f.read_to_string(&mut macaddr))
        .unwrap();
    macaddr.lines().next().unwrap_or("").parse::<MacAddress>()
}

pub fn get_mac_string_from_ifname(ifname: &str) -> Result<String, ParseError> {
    let iface = Path::new("/sys/class/net").join(ifname).join("address");
    let mut macaddr = String::new();
    File::open(iface)
        .and_then(|mut f| f.read_to_string(&mut macaddr))
        .unwrap();
    Ok(macaddr.lines().next().unwrap_or("").to_string())
}

/// Get rdtsc timestamp in microseconds with nanosecond resolution (3 decimals)
/// Relative to the program start time (when TSC frequency was queried)
#[inline]
pub fn rdtsc_us() -> f64 {
    // Read the Time Stamp Counter
    let tsc: u64;
    unsafe {
        std::arch::asm!(
        "rdtsc",
        "shl rdx, 32",
        "or rax, rdx",
        out("rax") tsc,
        out("rdx") _,
        options(nostack, nomem)
        );
    }
    // Convert to microseconds using the detected TSC frequency, relative to start
    let tsc_freq_hz = crate::frame_registry::get_tsc_frequency();
    let tsc_start = get_tsc_start();

    if tsc_freq_hz == 0 || tsc_start == 0 {
        // Fallback if not initialized
        return (tsc as f64) / 2500.0; // Assume 2.5 GHz
    }

    let elapsed_cycles = tsc.saturating_sub(tsc_start);
    (elapsed_cycles as f64) / (tsc_freq_hz as f64 / 1_000_000.0)
}
/// Detailed packet statistics including unicast, multicast, and broadcast counts
#[derive(Debug, Clone, Default)]
pub struct PacketStats {
    pub total: u64,
    pub unicast: Option<u64>,
    pub multicast: Option<u64>,
    pub broadcast: Option<u64>,
}

/// Read NIC packet counter from ethtool statistics
/// Returns the packet count if successful, None otherwise
///
/// Handles both standard drivers (tx_packets) and modern Intel 'ice' drivers
/// (tx_unicast + tx_multicast + tx_broadcast).
pub fn get_nic_packets(interface: &str, rx: bool) -> Option<u64> {
    get_nic_packets_detailed(interface, rx).map(|stats| stats.total)
}

/// Read NIC packet statistics from ethtool with detailed breakdowns
/// Returns PacketStats with total and optionally unicast/multicast/broadcast counts
///
/// Handles both standard drivers (tx_packets) and modern Intel 'ice' drivers
/// (tx_unicast + tx_multicast + tx_broadcast).
pub fn get_nic_packets_detailed(interface: &str, rx: bool) -> Option<PacketStats> {
    let output = std::process::Command::new("ethtool")
        .arg("-S")
        .arg(interface)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // 1. Try the standard generic keys first (used by ixgbe, i40e, Mellanox)
    let primary_key = if rx { "rx_packets:" } else { "tx_packets:" };

    for line in stdout.lines() {
        if line.trim().starts_with(primary_key) {
            if let Some(value_str) = line.split(':').nth(1) {
                if let Ok(packets) = value_str.trim().parse::<u64>() {
                    // For standard drivers, also try to find detailed stats
                    let (uni_key, multi_key, broad_key) = if rx {
                        ("rx_unicast:", "rx_multicast:", "rx_broadcast:")
                    } else {
                        ("tx_unicast:", "tx_multicast:", "tx_broadcast:")
                    };

                    let mut stats = PacketStats {
                        total: packets,
                        unicast: None,
                        multicast: None,
                        broadcast: None,
                    };

                    // Try to find detailed stats
                    for detail_line in stdout.lines() {
                        let trimmed = detail_line.trim();
                        if trimmed.starts_with(uni_key) {
                            if let Some(val) = detail_line.split(':').nth(1) {
                                stats.unicast = val.trim().parse::<u64>().ok();
                            }
                        } else if trimmed.starts_with(multi_key) {
                            if let Some(val) = detail_line.split(':').nth(1) {
                                stats.multicast = val.trim().parse::<u64>().ok();
                            }
                        } else if trimmed.starts_with(broad_key) {
                            if let Some(val) = detail_line.split(':').nth(1) {
                                stats.broadcast = val.trim().parse::<u64>().ok();
                            }
                        }
                    }

                    return Some(stats); // Fast path success
                }
            }
        }
    }

    // 2. Fallback for Intel 'ice' drivers (E810 / E830)
    // The ice driver splits packets by type. We must sum them up to get the total.
    let (uni_key, multi_key, broad_key) = if rx {
        ("rx_unicast:", "rx_multicast:", "rx_broadcast:")
    } else {
        ("tx_unicast:", "tx_multicast:", "tx_broadcast:")
    };

    let mut stats = PacketStats::default();
    let mut found_any_ice_stat = false;

    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with(uni_key) {
            if let Some(value_str) = line.split(':').nth(1) {
                if let Ok(packets) = value_str.trim().parse::<u64>() {
                    stats.unicast = Some(packets);
                    stats.total += packets;
                    found_any_ice_stat = true;
                }
            }
        } else if trimmed.starts_with(multi_key) {
            if let Some(value_str) = line.split(':').nth(1) {
                if let Ok(packets) = value_str.trim().parse::<u64>() {
                    stats.multicast = Some(packets);
                    stats.total += packets;
                    found_any_ice_stat = true;
                }
            }
        } else if trimmed.starts_with(broad_key) {
            if let Some(value_str) = line.split(':').nth(1) {
                if let Ok(packets) = value_str.trim().parse::<u64>() {
                    stats.broadcast = Some(packets);
                    stats.total += packets;
                    found_any_ice_stat = true;
                }
            }
        }
    }

    if found_any_ice_stat {
        Some(stats)
    } else {
        None
    }
}

/// Wait for physical link to come up on a network interface
/// Checks the carrier status via sysfs and waits up to 5 seconds
pub async fn wait_for_physical_link(iface: &str) {
    let carrier_path = format!("/sys/class/net/{}/carrier", iface);
    log::info!(
        "Waiting for physical link training to complete on {}...",
        iface
    );

    let mut attempts = 0;
    while attempts < 50 {
        // Wait up to 5 seconds
        if let Ok(status) = std::fs::read_to_string(&carrier_path) {
            if status.trim() == "1" {
                log::info!("Physical link is UP! Hardware is ready.");
                // Add a microscopic buffer just to let the silicon settle
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return;
            }
        }
        // Link is still down or negotiating. Wait 100ms and check again.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        attempts += 1;
    }

    log::warn!(
        "Physical link did not come up within 5 seconds on {}!",
        iface
    );
}

/// Read NIC tx_packets counter from ethtool statistics
/// Returns the tx_packets value if successful, None otherwise
pub fn get_nic_tx_packets(interface: &str) -> Option<u64> {
    get_nic_packets(interface, false)
}

/// Read NIC tx_packets statistics with detailed breakdowns from ethtool
/// Returns PacketStats with total and optionally unicast/multicast/broadcast counts
pub fn get_nic_tx_packets_detailed(interface: &str) -> Option<PacketStats> {
    get_nic_packets_detailed(interface, false)
}

/// Read NIC rx_packets counter from ethtool statistics
/// Returns the rx_packets value if successful, None otherwise
pub fn get_nic_rx_packets(interface: &str) -> Option<u64> {
    get_nic_packets(interface, true)
}

/// Read NIC rx_packets statistics with detailed breakdowns from ethtool
/// Returns PacketStats with total and optionally unicast/multicast/broadcast counts
pub fn get_nic_rx_packets_detailed(interface: &str) -> Option<PacketStats> {
    get_nic_packets_detailed(interface, true)
}

use log::{debug, info, warn};
use std::fs;

pub fn check_irqbalance_status() {
    if Path::new("/var/run/irqbalance.pid").exists() || process_exists("irqbalance") {
        warn!("⚠️  ENVIRONMENT ALERT: 'irqbalance' daemon is running.");
        warn!("   This will likely move interrupts to the worker cores and cause packet drops.");
    } else {
        info!("✓ irqbalance is inactive.");
    }
}

pub fn check_cpu_governor(worker_core: usize) {
    let governor_path = format!(
        "/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor",
        worker_core
    );
    if let Ok(governor) = fs::read_to_string(&governor_path) {
        let governor = governor.trim();
        if governor != "performance" {
            warn!(
                "⚠️  LATENCY WARNING: Core {} is set to '{}' mode.",
                worker_core, governor
            );
            warn!("   Run 'cpupower frequency-set -g performance' for zero-drop stability.");
        } else {
            info!("✓ Core {} is in performance mode.", worker_core);
        }
    }
}

fn process_exists(name: &str) -> bool {
    // Einfache Prüfung via /proc/ (funktioniert auf Linux)
    if let Ok(entries) = fs::read_dir("/proc") {
        for entry in entries.flatten() {
            if let Ok(cmdline) = fs::read_to_string(entry.path().join("comm")) {
                if cmdline.trim() == name {
                    return true;
                }
            }
        }
    }
    false
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterfaceCheckConfig {
    pub name: String,
    pub poll_mode: String,
    pub irq_core: Option<usize>,
    pub serving_worker_cores: Vec<usize>,
}

impl InterfaceCheckConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            poll_mode: "userspace_ring_spin".to_string(),
            irq_core: None,
            serving_worker_cores: Vec::new(),
        }
    }

    pub fn with_mode(
        name: impl Into<String>,
        poll_mode: impl Into<String>,
        irq_core: Option<usize>,
        serving_worker_cores: Vec<usize>,
    ) -> Self {
        Self {
            name: name.into(),
            poll_mode: poll_mode.into(),
            irq_core,
            serving_worker_cores,
        }
    }
}

impl From<&str> for InterfaceCheckConfig {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for InterfaceCheckConfig {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl From<&String> for InterfaceCheckConfig {
    fn from(name: &String) -> Self {
        Self::new(name.as_str())
    }
}

pub fn check_irq_affinity(worker_core: usize, interface: &str) {
    let iface = InterfaceCheckConfig::new(interface);
    check_interface_irq_affinity(&iface, &[worker_core]);
}

pub fn check_interface_irq_affinity(iface: &InterfaceCheckConfig, all_worker_cores: &[usize]) {
    let configured_irq_core_mismatch = iface.poll_mode == "napi_busy_poll"
        && iface.irq_core.is_some_and(|irq_core| {
            !iface.serving_worker_cores.is_empty()
                && !iface.serving_worker_cores.contains(&irq_core)
        });

    // 1. Configured irq_core check
    match iface.poll_mode.as_str() {
        "napi_busy_poll" => {
            if let Some(irq_core) = iface.irq_core {
                if !iface.serving_worker_cores.is_empty()
                    && !iface.serving_worker_cores.contains(&irq_core)
                {
                    warn!(
                        "⚠️ WARNING: Interface {} is in napi_busy_poll mode: configured irq_core ({}) does not match worker core {:?} serving that interface.",
                        iface.name, irq_core, iface.serving_worker_cores
                    );
                } else if iface.serving_worker_cores.contains(&irq_core) {
                    info!(
                        "✓ Interface {} IRQ core {} matches serving worker core.",
                        iface.name, irq_core
                    );
                }
            } else if !iface.serving_worker_cores.is_empty() {
                warn!(
                    "⚠️ WARNING: Interface {} is in napi_busy_poll mode, but irq_core is not configured. It should be set to worker core {:?} serving that interface.",
                    iface.name, iface.serving_worker_cores
                );
            } else {
                warn!(
                    "⚠️ WARNING: Interface {} is in napi_busy_poll mode, but irq_core is not configured.",
                    iface.name
                );
            }
        }
        _ => {
            // userspace_ring_spin (default)
            if let Some(irq_core) = iface.irq_core {
                if all_worker_cores.contains(&irq_core) {
                    warn!(
                        "⚠️ WARNING: Interface {} IRQs are configured on worker CPU {}. In userspace_ring_spin mode, move the IRQ to a different physical core to avoid contention.",
                        iface.name, irq_core
                    );
                }
            }
        }
    }

    // 2. /proc/interrupts and smp_affinity_list check
    if let Ok(interrupts) = fs::read_to_string("/proc/interrupts") {
        let mut found_irq = false;
        let mut b_warn = false;
        for line in interrupts.lines() {
            if line.contains(&iface.name) {
                found_irq = true;
                let irq_num = line.split(':').next().unwrap_or("").trim();
                let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq_num);

                if let Ok(affinity) = fs::read_to_string(&affinity_path) {
                    let allowed_cpus = parse_cpu_list(affinity.trim());

                    if iface.poll_mode == "napi_busy_poll" {
                        for &serving_core in &iface.serving_worker_cores {
                            if !configured_irq_core_mismatch
                                && !allowed_cpus.contains(&serving_core)
                            {
                                warn!(
                                    "⚠️  IRQ MISMATCH: Interface {} (IRQ {}) is in napi_busy_poll mode, but is NOT allowed on serving worker core {} (affinity: {})!",
                                    iface.name, irq_num, serving_core, affinity.trim()
                                );
                                if let Some(target_core) = iface.irq_core {
                                    warn!("   (IRQ affinity will be corrected to core {} during interface configuration).", target_core);
                                } else {
                                    warn!("   Check your 'irqaffinity' or /proc/irq/ settings.");
                                }
                                b_warn = true;
                            }
                        }

                        for &worker_core in all_worker_cores {
                            if !iface.serving_worker_cores.contains(&worker_core)
                                && allowed_cpus.contains(&worker_core)
                            {
                                warn!(
                                    "⚠️  IRQ CONFLICT: Interface {} (IRQ {}) is allowed on worker core {} (which does not serve this interface)!",
                                    iface.name, irq_num, worker_core
                                );
                                if let Some(target_core) = iface.irq_core {
                                    warn!("   (IRQ affinity will be corrected to core {} during interface configuration).", target_core);
                                } else {
                                    warn!("   Check your 'irqaffinity' or /proc/irq/ settings.");
                                }
                                b_warn = true;
                            }
                        }
                    } else {
                        for &worker_core in all_worker_cores {
                            if allowed_cpus.contains(&worker_core) {
                                warn!(
                                    "⚠️  IRQ CONFLICT: Interface {} (IRQ {}) is allowed on worker core {}!",
                                    iface.name, irq_num, worker_core
                                );
                                if let Some(target_core) = iface.irq_core {
                                    warn!("   (IRQ affinity will be corrected to core {} during interface configuration).", target_core);
                                } else {
                                    warn!("   Check your 'irqaffinity' or /proc/irq/ settings.");
                                }
                                b_warn = true;
                            }
                        }
                    }
                }
            }
        }
        if found_irq && !b_warn && !configured_irq_core_mismatch {
            info!("✓ No IRQ conflicts found for {}.", iface.name);
        }
    }
}

fn cpu_thread_siblings(cpu: usize) -> Result<Vec<usize>, std::io::Error> {
    let path = format!(
        "/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list",
        cpu
    );
    let contents = fs::read_to_string(path)?;
    let mut siblings = Vec::new();

    for item in contents.trim().split(',') {
        if let Some((start, end)) = item.split_once('-') {
            let start = start.trim().parse::<usize>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid CPU sibling range")
            })?;
            let end = end.trim().parse::<usize>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid CPU sibling range")
            })?;
            if start > end {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "CPU sibling range is reversed",
                ));
            }
            siblings.extend(start..=end);
        } else {
            siblings.push(item.trim().parse::<usize>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid CPU sibling")
            })?);
        }
    }

    Ok(siblings)
}

/// Warn when a configured NIC IRQ CPU is the SMT sibling of a worker CPU.
///
/// This check is intentionally advisory. The worker and IRQ may still be
/// validly configured on separate physical cores, and some systems expose
/// topology information differently. It only reports the common two-thread
/// SMT case requested by the configuration sanity check.
pub fn check_smt_irq_affinity(worker_cores: &[usize], irq_assignments: &[(String, usize)]) {
    for &worker_core in worker_cores {
        let siblings = match cpu_thread_siblings(worker_core) {
            Ok(siblings) => siblings,
            Err(error) => {
                debug!(
                    "Could not read SMT topology for worker CPU {}: {}",
                    worker_core, error
                );
                continue;
            }
        };

        if siblings.len() != 2 {
            continue;
        }

        for (interface, irq_core) in irq_assignments {
            if *irq_core != worker_core && siblings.contains(irq_core) {
                warn!(
                    "⚠️ WARNING: NIC {} IRQs are configured on CPU {}, the SMT sibling of worker CPU {}. Move the IRQ to a different physical core to avoid contention.",
                    interface, irq_core, worker_core
                );
            }
        }
    }
}

/// Set IRQ affinity for a network interface to a specific core
///
/// # Arguments
/// * `interface` - Network interface name
/// * `core` - CPU core number to pin IRQs to
pub fn set_irq_affinity(interface: &str, core: usize) -> Result<(), std::io::Error> {
    use std::process::Command;

    info!("Setting IRQ affinity for {} to core {}...", interface, core);

    if let Ok(interrupts) = fs::read_to_string("/proc/interrupts") {
        let mut irq_count = 0;

        for line in interrupts.lines() {
            if line.contains(interface) {
                let irq_num = line.split(':').next().unwrap_or("").trim();
                let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq_num);

                // Set the affinity using shell command to ensure proper permissions
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(format!(
                        "echo {} | sudo tee {} > /dev/null",
                        core, affinity_path
                    ))
                    .output()?;

                if output.status.success() {
                    info!(
                        "✓ Set IRQ {} ({}) affinity to core {}",
                        irq_num, interface, core
                    );
                    irq_count += 1;
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    warn!("Failed to set IRQ {} affinity: {}", irq_num, stderr);
                }
            }
        }

        if irq_count == 0 {
            warn!("No IRQs found for interface {}", interface);
        } else {
            info!(
                "✓ Set {} IRQ(s) for {} to core {}",
                irq_count, interface, core
            );
        }
    }

    Ok(())
}

/// Checks if a specific CPU ID is properly isolated via GRUB parameters.
/// It verifies 'isolcpus', 'nohz_full', and 'rcu_nocbs'.
pub fn is_core_isolated(cpu_id: usize) -> Result<bool, std::io::Error> {
    let cmdline = fs::read_to_string("/proc/cmdline")?;

    // A core is only truly isolated if it's in all three major isolation lists.
    let isolated = check_param(&cmdline, "isolcpus", cpu_id);
    let nohz = check_param(&cmdline, "nohz_full", cpu_id);
    let rcu = check_param(&cmdline, "rcu_nocbs", cpu_id);

    Ok(isolated && nohz && rcu)
}

/// Helper to check if a CPU ID exists within a specific kernel parameter string.
fn check_param(cmdline: &str, param_name: &str, cpu_id: usize) -> bool {
    let search_str = format!("{}=", param_name);
    if let Some(start) = cmdline.find(&search_str) {
        let remainder = &cmdline[start + search_str.len()..];
        // Extract the value until the next space or end of string.
        let val_str = remainder.split_whitespace().next().unwrap_or("");

        return parse_cpu_list(val_str).contains(&cpu_id);
    }
    false
}

/// Parses Linux list formats like "2,3,4-8" into a flat vector of IDs.
fn parse_cpu_list(list: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in list.split(',') {
        if part.contains('-') {
            // Handle ranges (e.g., "4-8")
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() == 2 {
                if let (Ok(start), Ok(end)) =
                    (bounds[0].parse::<usize>(), bounds[1].parse::<usize>())
                {
                    cpus.extend(start..=end);
                }
            }
        } else if let Ok(single) = part.parse::<usize>() {
            cpus.push(single);
        }
    }
    cpus
}

/// Checks if the 'irdma' kernel module is currently unloaded.
/// High-performance AF_XDP setups often require unloading RDMA drivers
/// to ensure the 'ice' driver has exclusive/clean access to the hardware.
pub fn check_irdma_mod_unloaded() -> bool {
    let module_name = "irdma";

    match fs::read_to_string("/proc/modules") {
        Ok(modules) => {
            // Each line starts with the module name
            let is_loaded = modules.lines().any(|line| line.starts_with(module_name));

            if is_loaded {
                warn!("⚠️  DRIVER ALERT: '{}' module is loaded.", module_name);
                warn!("   This can interfere with AF_XDP performance on Intel E830 cards.");
                warn!("   Consider running: 'sudo modprobe -r irdma'");
                false
            } else {
                info!("✓ '{}' module is not loaded (Clean).", module_name);
                true
            }
        }
        Err(e) => {
            warn!(
                "Could not read /proc/modules to check for {}: {}",
                module_name, e
            );
            // We return true here to not block execution if /proc is weird,
            // but the warning is logged.
            true
        }
    }
}

/// XDP capability information for a single network interface.
#[derive(Clone, Debug)]
pub struct XdpIfaceCapabilities {
    pub name: String,
    pub driver: String,
    pub version: String,
    /// NETDEV_XDP_ACT_BASIC — native XDP (driver mode)
    pub xdp_native: bool,
    /// NETDEV_XDP_ACT_XSK_ZEROCOPY — AF_XDP zero-copy
    pub xdp_zerocopy: bool,
    /// Maximum RX ring size from ethtool -g Pre-set maximums
    pub rx_ring_max: Option<u32>,
    /// Maximum TX ring size from ethtool -g Pre-set maximums
    pub tx_ring_max: Option<u32>,
    /// XDP RX metadata features reported by xdp-info.
    pub xdp_rx_metadata_features: Option<Vec<String>>,
    /// Maximum number of segments supported by XDP zero-copy RX.
    pub xdp_zc_max_segs: Option<u32>,
    /// Named XSK features decoded from NETDEV_A_DEV_XSK_FEATURES.
    pub xsk_feature_names: Option<Vec<String>>,
    /// NETDEV_A_DEV_XSK_FEATURES bitmask.
    pub xsk_features: Option<u64>,
}

const NETLINK_HEADER_LEN: usize = 16;
const GENL_HEADER_LEN: usize = 4;
const NLA_HEADER_LEN: usize = 4;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const NLM_F_REQUEST: u16 = 1;
const CTRL_CMD_GETFAMILY: u8 = 3;
const CTRL_ATTR_FAMILY_ID: u16 = 1;
const CTRL_ATTR_FAMILY_NAME: u16 = 2;
const NETDEV_CMD_DEV_GET: u8 = 1;
const NETDEV_A_DEV_IFINDEX: u16 = 1;
const NETDEV_A_DEV_XDP_ZC_MAX_SEGS: u16 = 4;
const NETDEV_A_DEV_XDP_RX_METADATA_FEATURES: u16 = 5;
const NETDEV_A_DEV_XSK_FEATURES: u16 = 6;
const NETDEV_XDP_RX_METADATA_TIMESTAMP: u64 = 1 << 0;
const NETDEV_XDP_RX_METADATA_HASH: u64 = 1 << 1;
const NETDEV_XDP_RX_METADATA_VLAN_TAG: u64 = 1 << 2;
const NETDEV_XSK_FLAGS_TX_TIMESTAMP: u64 = 1 << 0;
const NETDEV_XSK_FLAGS_TX_CHECKSUM: u64 = 1 << 1;
const NETDEV_XSK_FLAGS_TX_LAUNCH_TIME_FIFO: u64 = 1 << 2;

fn align_netlink(value: usize) -> usize {
    (value + 3) & !3
}

fn append_netlink_attr(attrs: &mut Vec<u8>, kind: u16, value: &[u8]) {
    let length = NLA_HEADER_LEN + value.len();
    attrs.extend_from_slice(&(length as u16).to_ne_bytes());
    attrs.extend_from_slice(&kind.to_ne_bytes());
    attrs.extend_from_slice(value);
    attrs.resize(align_netlink(attrs.len()), 0);
}

fn build_generic_netlink_request(
    family: u16,
    command: u8,
    version: u8,
    sequence: u32,
    attrs: &[u8],
) -> Vec<u8> {
    let length = NETLINK_HEADER_LEN + GENL_HEADER_LEN + attrs.len();
    let mut message = Vec::with_capacity(align_netlink(length));
    message.extend_from_slice(&(length as u32).to_ne_bytes());
    message.extend_from_slice(&family.to_ne_bytes());
    message.extend_from_slice(&NLM_F_REQUEST.to_ne_bytes());
    message.extend_from_slice(&sequence.to_ne_bytes());
    message.extend_from_slice(&0_u32.to_ne_bytes());
    message.push(command);
    message.push(version);
    message.extend_from_slice(&0_u16.to_ne_bytes());
    message.extend_from_slice(attrs);
    message
}

fn netlink_attrs(payload: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
    let mut offset = 0;
    std::iter::from_fn(move || {
        if offset + NLA_HEADER_LEN <= payload.len() {
            let length = u16::from_ne_bytes([payload[offset], payload[offset + 1]]) as usize;
            let kind = u16::from_ne_bytes([payload[offset + 2], payload[offset + 3]]) & 0x3fff;
            if length < NLA_HEADER_LEN || offset + length > payload.len() {
                offset = payload.len();
                return None;
            }
            let value = &payload[offset + NLA_HEADER_LEN..offset + length];
            offset += align_netlink(length);
            return Some((kind, value));
        }
        None
    })
}

fn receive_netlink<T>(
    socket: &netlink_sys::Socket,
    mut parse: impl FnMut(&[u8]) -> Option<T>,
) -> io::Result<Option<T>> {
    let mut buffer = vec![0_u8; 8192];
    loop {
        let (length, _) = socket.recv_from(&mut &mut buffer[..], 0)?;
        let mut offset = 0;
        while offset + NETLINK_HEADER_LEN <= length {
            let message_length = u32::from_ne_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            if message_length < NETLINK_HEADER_LEN || offset + message_length > length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid netlink message length",
                ));
            }

            let message_type = u16::from_ne_bytes([buffer[offset + 4], buffer[offset + 5]]);
            let payload = &buffer[offset + NETLINK_HEADER_LEN..offset + message_length];
            if message_type == NLMSG_ERROR {
                let error = payload
                    .get(..4)
                    .map(|bytes| i32::from_ne_bytes(bytes.try_into().unwrap()))
                    .unwrap_or(-libc::EINVAL);
                if error != 0 {
                    return Err(io::Error::from_raw_os_error(-error));
                }
            } else if message_type == NLMSG_DONE {
                return Ok(None);
            } else if let Some(value) = parse(payload) {
                return Ok(Some(value));
            }

            offset += align_netlink(message_length);
        }
    }
}

fn resolve_netdev_family(socket: &netlink_sys::Socket, sequence: u32) -> io::Result<Option<u16>> {
    let mut attrs = Vec::new();
    append_netlink_attr(&mut attrs, CTRL_ATTR_FAMILY_NAME, b"netdev\0");
    let request = build_generic_netlink_request(16, CTRL_CMD_GETFAMILY, 1, sequence, &attrs);
    socket.send_to(&request, &netlink_sys::SocketAddr::new(0, 0), 0)?;
    receive_netlink(socket, |payload| {
        if payload.len() < GENL_HEADER_LEN {
            return None;
        }
        netlink_attrs(&payload[GENL_HEADER_LEN..]).find_map(|(kind, value)| {
            (kind == CTRL_ATTR_FAMILY_ID && value.len() >= 2)
                .then(|| u16::from_ne_bytes([value[0], value[1]]))
        })
    })
}

fn query_netdev_capabilities(ifindex: u32) -> Option<(Option<u64>, Option<u32>, Option<u64>)> {
    let mut socket = netlink_sys::Socket::new(netlink_sys::protocols::NETLINK_GENERIC).ok()?;
    socket.bind_auto().ok()?;
    let family = resolve_netdev_family(&socket, ifindex).ok()??;

    let mut attrs = Vec::new();
    append_netlink_attr(&mut attrs, NETDEV_A_DEV_IFINDEX, &ifindex.to_ne_bytes());
    let request = build_generic_netlink_request(family, NETDEV_CMD_DEV_GET, 2, ifindex, &attrs);
    socket
        .send_to(&request, &netlink_sys::SocketAddr::new(0, 0), 0)
        .ok()?;

    receive_netlink(&socket, |payload| {
        if payload.len() < GENL_HEADER_LEN {
            return None;
        }
        let attrs = netlink_attrs(&payload[GENL_HEADER_LEN..]).collect::<Vec<_>>();
        let response_ifindex = attrs.iter().find_map(|(kind, value)| {
            (*kind == NETDEV_A_DEV_IFINDEX && value.len() >= 4)
                .then(|| u32::from_ne_bytes(value[..4].try_into().unwrap()))
        })?;
        if response_ifindex != ifindex {
            return None;
        }
        let xdp_rx_metadata_features = attrs.iter().find_map(|(kind, value)| {
            (*kind == NETDEV_A_DEV_XDP_RX_METADATA_FEATURES && value.len() >= 8)
                .then(|| u64::from_ne_bytes(value[..8].try_into().unwrap()))
        });
        let xdp_zc_max_segs = attrs.iter().find_map(|(kind, value)| {
            (*kind == NETDEV_A_DEV_XDP_ZC_MAX_SEGS && value.len() >= 4)
                .then(|| u32::from_ne_bytes(value[..4].try_into().unwrap()))
        });
        let xsk_features = attrs.iter().find_map(|(kind, value)| {
            (*kind == NETDEV_A_DEV_XSK_FEATURES && value.len() >= 8)
                .then(|| u64::from_ne_bytes(value[..8].try_into().unwrap()))
        });
        Some((xdp_rx_metadata_features, xdp_zc_max_segs, xsk_features))
    })
    .ok()?
}

fn xdp_rx_metadata_feature_names(features: Option<u64>) -> Option<Vec<String>> {
    let features = features?;
    let mut names = Vec::new();
    if features & NETDEV_XDP_RX_METADATA_TIMESTAMP != 0 {
        names.push("timestamp".to_string());
    }
    if features & NETDEV_XDP_RX_METADATA_HASH != 0 {
        names.push("hash".to_string());
    }
    if features & NETDEV_XDP_RX_METADATA_VLAN_TAG != 0 {
        names.push("vlan-tag".to_string());
    }
    Some(names)
}

fn xsk_feature_names(features: Option<u64>) -> Option<Vec<String>> {
    let features = features?;
    let mut names = Vec::new();
    if features & NETDEV_XSK_FLAGS_TX_TIMESTAMP != 0 {
        names.push("tx-timestamp".to_string());
    }
    if features & NETDEV_XSK_FLAGS_TX_CHECKSUM != 0 {
        names.push("tx-checksum".to_string());
    }
    if features & NETDEV_XSK_FLAGS_TX_LAUNCH_TIME_FIFO != 0 {
        names.push("tx-launch-time-fifo".to_string());
    }
    Some(names)
}

/// Query all physical network interfaces and return their XDP capabilities.
/// Uses `ethtool -i` for driver/version, `xdp-loader features` for XDP flags,
/// and `ethtool -g` for maximum ring sizes.
pub fn query_xdp_ifaces() -> Vec<XdpIfaceCapabilities> {
    use std::process::Command;

    let net_dir = Path::new("/sys/class/net");
    let mut ifaces: Vec<String> = Vec::new();

    if let Ok(entries) = fs::read_dir(net_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "lo" {
                continue;
            }
            if net_dir.join(&name).join("device").exists() {
                ifaces.push(name);
            }
        }
    }

    ifaces.sort();

    let mut result = Vec::new();

    for iface in &ifaces {
        let mut driver = "?".to_string();
        let mut version = "?".to_string();
        let mut xdp_native = false;
        let mut xdp_zerocopy = false;
        let mut rx_ring_max: Option<u32> = None;
        let mut tx_ring_max: Option<u32> = None;
        let ifindex = std::ffi::CString::new(iface.as_str())
            .ok()
            .map(|name| unsafe { libc::if_nametoindex(name.as_ptr()) })
            .filter(|&ifindex| ifindex != 0);
        let (xdp_rx_metadata_mask, xdp_zc_max_segs, xsk_features) = ifindex
            .and_then(query_netdev_capabilities)
            .unwrap_or((None, None, None));

        // --- ethtool -i <iface> ---
        if let Ok(out) = Command::new("ethtool").args(["-i", iface]).output() {
            if out.status.success() {
                for line in String::from_utf8_lossy(&out.stdout).lines() {
                    if let Some(v) = line.strip_prefix("driver: ") {
                        driver = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("version: ") {
                        version = v.trim().to_string();
                    }
                }
            }
        }

        // --- xdp-loader features <iface> ---
        // Output format (kernel NETDEV_XDP_ACT flags):
        //   NETDEV_XDP_ACT_BASIC:           yes/no
        //   NETDEV_XDP_ACT_XSK_ZEROCOPY:    yes/no
        if let Ok(out) = Command::new("xdp-loader")
            .args(["features", iface])
            .output()
        {
            if out.status.success() {
                for line in String::from_utf8_lossy(&out.stdout).lines() {
                    let is_yes = line.trim_end().ends_with("yes");
                    if line.contains("NETDEV_XDP_ACT_BASIC") {
                        xdp_native = is_yes;
                    } else if line.contains("NETDEV_XDP_ACT_XSK_ZEROCOPY") {
                        xdp_zerocopy = is_yes;
                    }
                }
            }
        }

        // --- ethtool -g <iface> — Pre-set maximums ---
        if let Ok(out) = Command::new("ethtool").args(["-g", iface]).output() {
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let mut in_preset = false;
                for line in stdout.lines() {
                    if line.starts_with("Pre-set maximums:") {
                        in_preset = true;
                        continue;
                    }
                    if in_preset {
                        if line.starts_with("Current hardware settings:") {
                            break;
                        }
                        if rx_ring_max.is_none() && line.trim_start().starts_with("RX:") {
                            rx_ring_max =
                                line.split(':').nth(1).and_then(|v| v.trim().parse().ok());
                        } else if tx_ring_max.is_none() && line.trim_start().starts_with("TX:") {
                            tx_ring_max =
                                line.split(':').nth(1).and_then(|v| v.trim().parse().ok());
                        }
                    }
                }
            }
        }

        result.push(XdpIfaceCapabilities {
            name: iface.clone(),
            driver,
            version,
            xdp_native,
            xdp_zerocopy,
            rx_ring_max,
            tx_ring_max,
            xdp_rx_metadata_features: xdp_rx_metadata_feature_names(xdp_rx_metadata_mask),
            xdp_zc_max_segs,
            xsk_feature_names: xsk_feature_names(xsk_features),
            xsk_features,
        });
    }

    result
}

/// Probe all physical network interfaces for XDP capabilities.
/// Uses `ethtool -i` for driver/version and `xdp-loader features` for XDP support.
/// Prints a formatted table via log::info.
pub fn probe_xdp_interfaces() {
    let rows = query_xdp_ifaces();

    if rows.is_empty() {
        info!("No physical network interfaces found.");
        return;
    }

    let physical_headers = [
        "Interface",
        "Driver",
        "Version",
        "XDP Native",
        "ZeroCopy",
        "RX Metadata Features",
        "ZC Max Segs",
        "Ring RX",
        "Ring TX",
    ];
    let physical_rows: Vec<[String; 9]> = rows
        .iter()
        .map(|r| {
            [
                r.name.clone(),
                r.driver.clone(),
                r.version.clone(),
                if r.xdp_native { "yes" } else { "no" }.to_string(),
                if r.xdp_zerocopy { "yes" } else { "no" }.to_string(),
                r.xdp_rx_metadata_features
                    .as_ref()
                    .map(|features| features.join(","))
                    .unwrap_or_else(|| "?".to_string()),
                r.xdp_zc_max_segs
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                r.rx_ring_max
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                r.tx_ring_max
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "?".to_string()),
            ]
        })
        .collect();
    let physical_widths: Vec<usize> = (0..physical_headers.len())
        .map(|column| {
            std::iter::once(physical_headers[column].len())
                .chain(physical_rows.iter().map(|row| row[column].len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let physical_header = physical_headers
        .iter()
        .enumerate()
        .map(|(column, value)| format!("{value:<width$}", width = physical_widths[column]))
        .collect::<Vec<_>>()
        .join("  ");
    let physical_separator = physical_widths
        .iter()
        .map(|width| "─".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ");

    info!("=== Physical Interface XDP Capabilities ===");
    info!("{}", physical_header);
    info!("{}", physical_separator);
    for row in &physical_rows {
        info!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(column, value)| format!("{value:<width$}", width = physical_widths[column]))
                .collect::<Vec<_>>()
                .join("  "),
        );
    }
    info!("");
    info!("=== XSK Features ===");
    let xsk_headers = ["Interface", "Features"];
    let xsk_rows: Vec<[String; 2]> = rows
        .iter()
        .map(|r| {
            let feature_names = r
                .xsk_feature_names
                .as_ref()
                .map(|features| {
                    if features.is_empty() {
                        "{}".to_string()
                    } else {
                        format!("{{{}}}", features.join(","))
                    }
                })
                .unwrap_or_else(|| "?".to_string());
            [
                r.name.clone(),
                feature_names,
            ]
        })
        .collect();
    let xsk_widths: Vec<usize> = (0..xsk_headers.len())
        .map(|column| {
            std::iter::once(xsk_headers[column].len())
                .chain(xsk_rows.iter().map(|row| row[column].len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let xsk_header = xsk_headers
        .iter()
        .enumerate()
        .map(|(column, value)| format!("{value:<width$}", width = xsk_widths[column]))
        .collect::<Vec<_>>()
        .join("  ");
    let xsk_separator = xsk_widths
        .iter()
        .map(|width| "─".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ");
    info!("{}", xsk_header);
    info!("{}", xsk_separator);
    for row in &xsk_rows {
        info!(
            "{}",
            row.iter()
                .enumerate()
                .map(|(column, value)| format!("{value:<width$}", width = xsk_widths[column]))
                .collect::<Vec<_>>()
                .join("  "),
        );
    }
    info!("{}", xsk_separator);
}

/// Perform system checks for irqbalance, CPU governor, and IRQ affinity
///
/// # Arguments
/// * `worker_cores` - List of CPU cores used by workers
/// * `interfaces` - List of network interface check configurations
/// * `irq_assignments` - Configured `(interface, CPU)` IRQ assignments
pub fn check_system(
    worker_cores: &[usize],
    interfaces: &[InterfaceCheckConfig],
    irq_assignments: &[(String, usize)],
) {
    info!("=== wabrix Pre-Flight System Check ===");
    // check for irdma module, in case of Intel E800 series cards it blocks advanced Eth fcts.
    check_irdma_mod_unloaded();
    // Check irqbalance daemon
    check_irqbalance_status();

    // Check CPU isolation for all worker cores
    for &core in worker_cores {
        match is_core_isolated(core) {
            Ok(true) => info!("✓ Core {} is properly isolated.", core),
            Ok(false) => warn!("⚠️ WARNING: Core {} is NOT isolated in GRUB!", core),
            Err(e) => warn!("⚠️ Error reading cmdline: {}", e),
        }
    }

    // Check CPU governor for all worker cores
    for &core in worker_cores {
        check_cpu_governor(core);
    }

    // Check IRQ affinity for all interfaces
    for interface in interfaces {
        check_interface_irq_affinity(interface, worker_cores);
    }

    // Check configured IRQ CPUs against SMT siblings of worker CPUs.
    check_smt_irq_affinity(worker_cores, irq_assignments);

    info!("======== System Check Completed =======");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interface_check_config_constructors() {
        let default_cfg = InterfaceCheckConfig::new("eth0");
        assert_eq!(default_cfg.name, "eth0");
        assert_eq!(default_cfg.poll_mode, "userspace_ring_spin");
        assert_eq!(default_cfg.irq_core, None);
        assert!(default_cfg.serving_worker_cores.is_empty());

        let napi_cfg = InterfaceCheckConfig::with_mode("eth1", "napi_busy_poll", Some(3), vec![3]);
        assert_eq!(napi_cfg.name, "eth1");
        assert_eq!(napi_cfg.poll_mode, "napi_busy_poll");
        assert_eq!(napi_cfg.irq_core, Some(3));
        assert_eq!(napi_cfg.serving_worker_cores, vec![3]);

        let from_str: InterfaceCheckConfig = "eth2".into();
        assert_eq!(from_str.name, "eth2");
        assert_eq!(from_str.poll_mode, "userspace_ring_spin");
    }

    #[test]
    fn test_check_interface_irq_affinity_logic() {
        let worker_cores = vec![2, 3];

        // NAPI busy poll: irq_core matching serving worker core 3
        let napi_match =
            InterfaceCheckConfig::with_mode("enp2s0f1np1", "napi_busy_poll", Some(3), vec![3]);
        check_interface_irq_affinity(&napi_match, &worker_cores);

        // NAPI busy poll: irq_core mismatch (e.g. 8 instead of 3)
        let napi_mismatch =
            InterfaceCheckConfig::with_mode("enp2s0f1np1", "napi_busy_poll", Some(8), vec![3]);
        check_interface_irq_affinity(&napi_mismatch, &worker_cores);

        // NAPI busy poll: irq_core unset
        let napi_unset =
            InterfaceCheckConfig::with_mode("enp2s0f1np1", "napi_busy_poll", None, vec![3]);
        check_interface_irq_affinity(&napi_unset, &worker_cores);

        // Userspace ring spin: irq_core on separate core 8
        let spin_ok =
            InterfaceCheckConfig::with_mode("enp2s0f0np0", "userspace_ring_spin", Some(8), vec![2]);
        check_interface_irq_affinity(&spin_ok, &worker_cores);

        // Userspace ring spin: irq_core on worker core 2 (conflict)
        let spin_conflict =
            InterfaceCheckConfig::with_mode("enp2s0f0np0", "userspace_ring_spin", Some(2), vec![2]);
        check_interface_irq_affinity(&spin_conflict, &worker_cores);
    }
}
