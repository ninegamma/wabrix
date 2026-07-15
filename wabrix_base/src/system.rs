use std::arch::x86_64::__cpuid;
use std::fs::File;
use std::io::Read;
use macaddr::{MacAddr6 as MacAddress, ParseError};
use std::path::Path;
use crate::frame_registry::get_tsc_start;

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

pub fn get_mac_from_ifname(ifname: &str) -> Result<MacAddress, ParseError> {
    let iface = Path::new("/sys/class/net").join(ifname).join("address");
    let mut macaddr = String::new();
    File::open(iface).and_then(|mut f| f.read_to_string(&mut macaddr)).unwrap();
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
    let output = std::process::Command::new("ethtool").arg("-S").arg(interface).output().ok()?;

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

    if found_any_ice_stat { Some(stats) } else { None }
}

/// Wait for physical link to come up on a network interface
/// Checks the carrier status via sysfs and waits up to 5 seconds
pub async fn wait_for_physical_link(iface: &str) {
    let carrier_path = format!("/sys/class/net/{}/carrier", iface);
    log::info!("Waiting for physical link training to complete on {}...", iface);

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

    log::warn!("Physical link did not come up within 5 seconds on {}!", iface);
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

use std::fs;
use log::{warn, info};

pub fn check_irqbalance_status() {
    if Path::new("/var/run/irqbalance.pid").exists() || process_exists("irqbalance") {
        warn!("⚠️  ENVIRONMENT ALERT: 'irqbalance' daemon is running.");
        warn!("   This will likely move interrupts to the worker cores and cause packet drops.");
    } else {
        info!("✓ irqbalance is inactive.");
    }
}

pub fn check_cpu_governor(worker_core: usize) {
    let governor_path = format!("/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor", worker_core);
    if let Ok(governor) = fs::read_to_string(&governor_path) {
        let governor = governor.trim();
        if governor != "performance" {
            warn!("⚠️  LATENCY WARNING: Core {} is set to '{}' mode.", worker_core, governor);
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

pub fn check_irq_affinity(worker_core: usize, interface: &str) {
    if let Ok(interrupts) = fs::read_to_string("/proc/interrupts") {
        let mut b_warn = false;
        for line in interrupts.lines() {
            if line.contains(interface) {
                let irq_num = line.split(':').next().unwrap_or("").trim();
                let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq_num);

                if let Ok(affinity) = fs::read_to_string(&affinity_path) {
                    if affinity.contains(&worker_core.to_string()) {
                        warn!(
                            "⚠️  IRQ CONFLICT: Interface {} (IRQ {}) is allowed on worker core {}!",
                            interface, irq_num, worker_core
                        );
                        warn!("   Check your 'irqaffinity' or /proc/irq/ settings.");
                        b_warn = true;
                    }
                }
            }
        }
        if !b_warn {
            info!("✓ No IRQ conflicts found.");
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
                    .arg(format!("echo {} | sudo tee {} > /dev/null", core, affinity_path))
                    .output()?;

                if output.status.success() {
                    info!("✓ Set IRQ {} ({}) affinity to core {}", irq_num, interface, core);
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
            info!("✓ Set {} IRQ(s) for {} to core {}", irq_count, interface, core);
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
                if let (Ok(start), Ok(end)) = (bounds[0].parse::<usize>(), bounds[1].parse::<usize>()) {
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
            warn!("Could not read /proc/modules to check for {}: {}", module_name, e);
            // We return true here to not block execution if /proc is weird,
            // but the warning is logged.
            true
        }
    }
}

/// XDP capability information for a single network interface.
#[derive(Clone, Debug)]
pub struct XdpIfaceCapabilities {
    pub name:         String,
    pub driver:       String,
    pub version:      String,
    /// NETDEV_XDP_ACT_BASIC — native XDP (driver mode)
    pub xdp_native:   bool,
    /// NETDEV_XDP_ACT_XSK_ZEROCOPY — AF_XDP zero-copy
    pub xdp_zerocopy: bool,
    /// Maximum RX ring size from ethtool -g Pre-set maximums
    pub rx_ring_max:  Option<u32>,
    /// Maximum TX ring size from ethtool -g Pre-set maximums
    pub tx_ring_max:  Option<u32>,
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
            if name == "lo" { continue; }
            if net_dir.join(&name).join("device").exists() {
                ifaces.push(name);
            }
        }
    }

    ifaces.sort();

    let mut result = Vec::new();

    for iface in &ifaces {
        let mut driver       = "?".to_string();
        let mut version      = "?".to_string();
        let mut xdp_native   = false;
        let mut xdp_zerocopy = false;
        let mut rx_ring_max: Option<u32> = None;
        let mut tx_ring_max: Option<u32> = None;

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
        if let Ok(out) = Command::new("xdp-loader").args(["features", iface]).output() {
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
                            rx_ring_max = line.split(':').nth(1)
                                .and_then(|v| v.trim().parse().ok());
                        } else if tx_ring_max.is_none() && line.trim_start().starts_with("TX:") {
                            tx_ring_max = line.split(':').nth(1)
                                .and_then(|v| v.trim().parse().ok());
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

    // Column widths
    let w = [20usize, 12, 24, 12, 12, 10, 10];
    let header = format!(
        "{:<w0$} {:<w1$} {:<w2$} {:<w3$} {:<w4$} {:>w5$} {:>w6$}",
        "Interface", "Driver", "Version", "XDP Native", "ZeroCopy", "Ring Max RX", "Ring Max TX",
        w0=w[0], w1=w[1], w2=w[2], w3=w[3], w4=w[4], w5=w[5], w6=w[6],
    );
    let sep = "─".repeat(header.len());

    info!("=== Physical Interface XDP Capabilities ===");
    info!("{}", header);
    info!("{}", sep);
    for r in &rows {
        info!(
            "{:<w0$} {:<w1$} {:<w2$} {:<w3$} {:<w4$} {:>w5$} {:>w6$}",
            r.name,
            r.driver,
            r.version,
            if r.xdp_native   { "yes" } else { "no" },
            if r.xdp_zerocopy { "yes" } else { "no" },
            r.rx_ring_max.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string()),
            r.tx_ring_max.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string()),
            w0=w[0], w1=w[1], w2=w[2], w3=w[3], w4=w[4], w5=w[5], w6=w[6],
        );
    }
    info!("{}", sep);
}

/// Perform system checks for irqbalance, CPU governor, and IRQ affinity
///
/// # Arguments
/// * `worker_cores` - List of CPU cores used by workers
/// * `interfaces` - List of network interface names
pub fn check_system(worker_cores: &[usize], interfaces: &[String]) {
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

    // Check IRQ affinity for all interfaces and worker cores
    for interface in interfaces {
        for &core in worker_cores {
            check_irq_affinity(core, interface);
        }
    }

    info!("======== System Check Completed =======");
}
