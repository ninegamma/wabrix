// src/interface_setup.rs
// Module for configuring Linux network interfaces based on TOML configuration

use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use std::process::Command;

/// Interface configuration from TOML
#[derive(Debug, Clone)]
pub struct InterfaceSetup {
    pub name: String,
    pub queues: u32,
    pub rxd: u32,
    pub txd: u32,
    pub iface_type: Option<String>,
    pub namespace: Option<String>,
    pub peer: Option<String>,
    pub mac_if: Option<String>,
    pub ipnet: Option<String>,
}

impl InterfaceSetup {
    /// Configure the interface: bring it up and set queue count
    /// if b_do_not_change == true, only existence is checked, and a down interface is brought up
    pub fn configure(&self, b_just_check: bool) -> Result<()> {
        info!("Configuring interface {}...", self.name);

        // Check if this is a veth interface that needs to be created
        if let Some(ref iface_type) = self.iface_type {
            if iface_type == "veth" {
                self.setup_veth_pair()?;
            }
        }

        // 1. Check if interface exists
        self.check_exists()?;

        // 2. Bring interface up
        self.bring_up()?;

        if !b_just_check {
            // 3. Enable promiscuous mode
            self.enable_promiscuous_mode()?;

            // 4. Configure queue count
            self.configure_queues()?;

            // 5. Configure ring sizes (RX/TX buffers)
            self.configure_ring_sizes()?;

            // 6. Verify configuration
            self.verify()?;
        }

        // Get current ring sizes and warn if too small (skip for veth interfaces)
        let is_veth = self.iface_type.as_ref().map(|t| t == "veth").unwrap_or(false);
        if !is_veth {
            if let Ok((current_rx, current_tx)) = self.get_current_ring_sizes() {
                if current_rx < 2048 || current_tx < 2048 {
                    println!(
                        "Warning: ring sizes can be too small for XDP, especially when large batch sizes are used: RX={}, TX={}",
                        current_rx, current_tx
                    );
                    println!(
                        "Consider increasing ring sizes to at least 2048: sudo ethtool -G <interface> rx 2048 tx 2048)"
                    );
                }
            }
        }

        // Check link status - wait up to 3 seconds for carrier to appear
        self.check_link_status()?;

        info!("✓ Interface {} configured successfully", self.name);
        Ok(())
    }

    /// Setup veth pair with namespace, MAC address, and IP configuration
    fn setup_veth_pair(&self) -> Result<()> {
        let peer_name = self
            .peer
            .as_ref()
            .ok_or_else(|| anyhow!("Veth interface {} requires 'peer' field", self.name))?;
        let namespace = self
            .namespace
            .as_ref()
            .ok_or_else(|| anyhow!("Veth interface {} requires 'namespace' field", self.name))?;

        info!(
            "Setting up veth pair: {} <-> {} (namespace: {})",
            self.name, peer_name, namespace
        );

        // Check if veth pair already exists
        let check_output = Command::new("ip").args(&["link", "show", &self.name]).output()?;

        if check_output.status.success() {
            // Check if the existing interface has the required number of queues
            let existing_queues = std::fs::read_dir(format!("/sys/class/net/{}/queues", self.name))
                .map(|entries| entries.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().starts_with("rx-")).count() as u32)
                .unwrap_or(1);

            if existing_queues >= self.queues {
                info!("✓ Veth pair {} already exists with {} queues", self.name, existing_queues);
                return Ok(());
            }

            info!(
                "Veth pair {} exists with {} queue(s) but {} required — deleting and recreating",
                self.name, existing_queues, self.queues
            );
            let del_output = Command::new("ip").args(&["link", "del", &self.name]).output()?;
            if !del_output.status.success() {
                let stderr = String::from_utf8_lossy(&del_output.stderr);
                return Err(anyhow!("Failed to delete existing veth {}: {}", self.name, stderr));
            }
            info!("✓ Deleted existing veth pair {}", self.name);
        }

        // Get MAC address from mac_if if specified
        let mac_addr = if let Some(ref mac_if) = self.mac_if {
            let output = Command::new("ip").args(&["link", "show", mac_if]).output()?;

            if !output.status.success() {
                return Err(anyhow!("Failed to get MAC address from interface {}", mac_if));
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mac = stdout
                .lines()
                .find_map(|line| {
                    if line.contains("link/ether") {
                        line.split_whitespace()
                            .skip_while(|&s| s != "link/ether")
                            .nth(1)
                            .map(String::from)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| anyhow!("Could not extract MAC address from {}", mac_if))?;

            info!("Using MAC address {} from interface {}", mac, mac_if);
            Some(mac)
        } else {
            None
        };

        // Create network namespace
        info!("Creating network namespace {}...", namespace);
        let ns_list_output = Command::new("ip").args(&["netns", "list"]).output()?;

        let ns_exists = String::from_utf8_lossy(&ns_list_output.stdout)
            .lines()
            .any(|line| line.trim().starts_with(namespace));

        if !ns_exists {
            let output = Command::new("ip").args(&["netns", "add", namespace]).output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("Failed to create namespace {}: {}", namespace, stderr));
            }
            info!("✓ Created namespace {}", namespace);
        } else {
            info!("✓ Namespace {} already exists", namespace);
        }

        // Create veth pair with the required number of queues
        info!("Creating veth pair: {} <-> {} ({} queues)...", self.name, peer_name, self.queues);
        let queues_str = self.queues.to_string();
        let output = Command::new("ip")
            .args(&[
                "link", "add",
                &self.name,
                "numtxqueues", &queues_str,
                "numrxqueues", &queues_str,
                "type", "veth",
                "peer", "name", peer_name,
                "numtxqueues", &queues_str,
                "numrxqueues", &queues_str,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to create veth pair: {}", stderr));
        }
        info!("✓ Created veth pair");

        // Move peer into namespace
        info!("Moving {} into namespace {}...", peer_name, namespace);
        let output = Command::new("ip")
            .args(&["link", "set", peer_name, "netns", namespace])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to move {} to namespace: {}", peer_name, stderr));
        }
        info!("✓ Moved {} into namespace", peer_name);

        // Set MAC address on peer if provided
        if let Some(mac) = mac_addr {
            info!("Setting MAC address {} on {}...", mac, peer_name);
            let output = Command::new("ip")
                .args(&["netns", "exec", namespace, "ip", "link", "set", peer_name, "address", &mac])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("Failed to set MAC address on {}: {}", peer_name, stderr));
            }
            info!("✓ Set MAC address on {}", peer_name);
        }

        // Set IP address on peer if provided
        if let Some(ref ipnet) = self.ipnet {
            info!("Setting IP address {} on {}...", ipnet, peer_name);
            let output = Command::new("ip")
                .args(&["netns", "exec", namespace, "ip", "addr", "add", ipnet, "dev", peer_name])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!("Failed to set IP address on {}: {}", peer_name, stderr));
            }
            info!("✓ Set IP address {} on {}", ipnet, peer_name);
        }

        // Bring up loopback in namespace
        let output = Command::new("ip")
            .args(&["netns", "exec", namespace, "ip", "link", "set", "lo", "up"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("Failed to bring up loopback in namespace: {}", stderr);
        }

        // Bring up peer interface in namespace
        info!("Bringing up {} in namespace...", peer_name);
        let output = Command::new("ip")
            .args(&["netns", "exec", namespace, "ip", "link", "set", peer_name, "up"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to bring up {} in namespace: {}", peer_name, stderr));
        }
        info!("✓ Brought up {} in namespace", peer_name);

        info!("✓ Veth pair setup complete");
        Ok(())
    }

    /// Check if interface exists
    fn check_exists(&self) -> Result<()> {
        let output = Command::new("ip").args(&["link", "show", &self.name]).output()?;

        if !output.status.success() {
            return Err(anyhow!("Interface {} does not exist", self.name));
        }

        debug!("Interface {} exists", self.name);
        Ok(())
    }

    /// Bring interface up
    fn bring_up(&self) -> Result<()> {
        debug!("Bringing interface {} up...", self.name);

        // Check if interface is already up
        let check_output = Command::new("ip").args(&["link", "show", &self.name]).output()?;

        if check_output.status.success() {
            let stdout = String::from_utf8_lossy(&check_output.stdout);
            if stdout.contains("UP") {
                info!("✓ Interface {} is already UP", self.name);
                return Ok(());
            }
        }

        let output = Command::new("ip").args(&["link", "set", &self.name, "up"]).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to bring interface {} up: {}", self.name, stderr));
        }

        // Wait a moment for interface to come up
        std::thread::sleep(std::time::Duration::from_millis(100));

        info!("✓ Interface {} is UP", self.name);
        Ok(())
    }

    /// Enable promiscuous mode on the interface
    fn enable_promiscuous_mode(&self) -> Result<()> {
        debug!("Enabling promiscuous mode on {}...", self.name);

        let output = Command::new("ip")
            .args(&["link", "set", &self.name, "promisc", "on"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("Failed to enable promiscuous mode on {}: {}", self.name, stderr));
        }

        info!("✓ Promiscuous mode enabled on {}", self.name);
        Ok(())
    }

    /// Configure the number of combined queues using ethtool
    fn configure_queues(&self) -> Result<()> {
        // Skip queue configuration for veth interfaces
        if let Some(ref iface_type) = self.iface_type {
            if iface_type == "veth" {
                debug!("Skipping queue configuration for veth interface {}", self.name);
                return Ok(());
            }
        }

        debug!("Configuring {} queues on {}...", self.queues, self.name);

        // First, check current queue configuration
        let current = self.get_current_queue_count()?;

        if current == self.queues {
            info!("Interface {} already has {} queues configured", self.name, self.queues);
            return Ok(());
        }

        // Set the number of combined channels
        let output = Command::new("ethtool")
            .args(&["-L", &self.name, "combined", &self.queues.to_string()])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "Failed to set queue count on {}: {}. This may not be supported by the driver.",
                self.name, stderr
            );
            // Don't fail - some drivers don't support changing queue count
            return Ok(());
        }

        info!("✓ Set {} combined queues on {}", self.queues, self.name);
        Ok(())
    }

    /// Get current queue count
    fn get_current_queue_count(&self) -> Result<u32> {
        let output = Command::new("ethtool").args(&["-l", &self.name]).output()?;

        if !output.status.success() {
            return Err(anyhow!("Failed to query queue count for {}", self.name));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse output looking for "Combined: N" in the "Current hardware settings:" section
        let mut in_current_section = false;
        for line in stdout.lines() {
            if line.contains("Current hardware settings:") {
                in_current_section = true;
                continue;
            }

            if in_current_section && line.contains("Combined:") {
                if let Some(count_str) = line.split_whitespace().last() {
                    if let Ok(count) = count_str.parse::<u32>() {
                        return Ok(count);
                    }
                }
            }
        }

        Err(anyhow!("Could not parse queue count from ethtool output"))
    }

    /// Configure RX and TX ring buffer sizes
    fn configure_ring_sizes(&self) -> Result<()> {
        // Skip ring size configuration for veth interfaces
        if let Some(ref iface_type) = self.iface_type {
            if iface_type == "veth" {
                debug!("Skipping ring size configuration for veth interface {}", self.name);
                return Ok(());
            }
        }

        debug!("Configuring ring sizes: RX={}, TX={} on {}...", self.rxd, self.txd, self.name);

        // Get current ring sizes
        let (current_rx, current_tx) = self.get_current_ring_sizes()?;

        if current_rx == self.rxd && current_tx == self.txd {
            info!(
                "Interface {} already has ring sizes configured (RX={}, TX={})",
                self.name, self.rxd, self.txd
            );
            return Ok(());
        }

        // Set ring sizes using ethtool -G
        let output = Command::new("ethtool")
            .args(&["-G", &self.name, "rx", &self.rxd.to_string(), "tx", &self.txd.to_string()])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "Failed to set ring sizes on {}: {}. Driver may not support changing ring sizes.",
                self.name, stderr
            );
            // Don't fail - some drivers don't support changing ring sizes
            return Ok(());
        }

        info!("✓ Set ring sizes on {}: RX={}, TX={}", self.name, self.rxd, self.txd);
        Ok(())
    }

    /// Get current RX and TX ring sizes
    fn get_current_ring_sizes(&self) -> Result<(u32, u32)> {
        let output = Command::new("ethtool").args(&["-g", &self.name]).output()?;

        if !output.status.success() {
            return Err(anyhow!("Failed to query ring sizes for {}", self.name));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse output looking for "RX:" and "TX:" in the "Current hardware settings:" section
        let mut in_current_section = false;
        let mut rx_size = None;
        let mut tx_size = None;

        for line in stdout.lines() {
            if line.contains("Current hardware settings:") {
                in_current_section = true;
                continue;
            }

            if in_current_section {
                // Look for lines like "RX:		4096"
                if line.starts_with("RX:") || line.contains("RX:") {
                    if let Some(size_str) = line.split_whitespace().last() {
                        if let Ok(size) = size_str.parse::<u32>() {
                            rx_size = Some(size);
                        }
                    }
                }
                // Look for lines like "TX:		4096"
                if line.starts_with("TX:") || line.contains("TX:") {
                    if let Some(size_str) = line.split_whitespace().last() {
                        if let Ok(size) = size_str.parse::<u32>() {
                            tx_size = Some(size);
                        }
                    }
                }

                // Stop after finding both or hitting the next section
                if rx_size.is_some() && tx_size.is_some() {
                    break;
                }
            }
        }

        match (rx_size, tx_size) {
            (Some(rx), Some(tx)) => Ok((rx, tx)),
            _ => Err(anyhow!("Could not parse ring sizes from ethtool output")),
        }
    }

    /// Get maximum hardware-supported RX and TX ring sizes
    fn get_max_ring_sizes(&self) -> Result<(u32, u32)> {
        let output = Command::new("ethtool").args(&["-g", &self.name]).output()?;

        if !output.status.success() {
            return Err(anyhow!("Failed to query max ring sizes for {}", self.name));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse output looking for "RX:" and "TX:" in the "Pre-set maximums:" section
        let mut in_max_section = false;
        let mut rx_max = None;
        let mut tx_max = None;

        for line in stdout.lines() {
            if line.contains("Pre-set maximums:") {
                in_max_section = true;
                continue;
            }

            if line.contains("Current hardware settings:") {
                // Stop when we reach the current settings section
                break;
            }

            if in_max_section {
                // Look for lines like "RX:		4096"
                if line.starts_with("RX:") || line.contains("RX:") {
                    if let Some(size_str) = line.split_whitespace().last() {
                        if let Ok(size) = size_str.parse::<u32>() {
                            rx_max = Some(size);
                        }
                    }
                }
                // Look for lines like "TX:		4096"
                if line.starts_with("TX:") || line.contains("TX:") {
                    if let Some(size_str) = line.split_whitespace().last() {
                        if let Ok(size) = size_str.parse::<u32>() {
                            tx_max = Some(size);
                        }
                    }
                }

                // Stop after finding both
                if rx_max.is_some() && tx_max.is_some() {
                    break;
                }
            }
        }

        match (rx_max, tx_max) {
            (Some(rx), Some(tx)) => Ok((rx, tx)),
            _ => Err(anyhow!("Could not parse maximum ring sizes from ethtool output")),
        }
    }

    ///
    /// Check link status - wait up to 3 seconds for carrier to appear
    fn check_link_status(&self) -> Result<()> {
        // 4. Check link status - wait up to 3 seconds for carrier to appear
        let carrier_path = format!("/sys/class/net/{}/carrier", self.name);
        let mut has_link = false;
        let max_attempts = 30; // 30 attempts * 100ms = 3 seconds

        for attempt in 0..max_attempts {
            if let Ok(carrier) = std::fs::read_to_string(&carrier_path) {
                if carrier.trim() == "1" {
                    has_link = true;
                    if attempt > 0 {
                        debug!("Carrier detected after {}ms", attempt * 100);
                    }
                    break;
                }
            }

            if attempt < max_attempts - 1 {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }

        if has_link {
            info!("✓ Interface {} has link (carrier detected)", self.name);
        } else {
            warn!("Interface {} has no link (no carrier)", self.name);
        }

        Ok(())
    }

    /// Verify interface configuration
    fn verify(&self) -> Result<()> {
        debug!("Verifying configuration for {}...", self.name);

        let is_veth = self.iface_type.as_ref().map(|t| t == "veth").unwrap_or(false);

        // 1. Check interface is UP and in promiscuous mode
        let output = Command::new("ip").args(&["link", "show", &self.name]).output()?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("UP") {
            return Err(anyhow!("Interface {} is not UP", self.name));
        }

        if !stdout.contains("PROMISC") {
            warn!("Interface {} is not in promiscuous mode", self.name);
        } else {
            debug!("✓ Promiscuous mode verified");
        }

        // Skip queue and ring size verification for veth interfaces
        if is_veth {
            debug!("✓ Skipping queue and ring size verification for veth interface");
            return Ok(());
        }

        // 2. Check queue count
        let current_queues = self.get_current_queue_count()?;
        if current_queues != self.queues {
            warn!(
                "Interface {} has {} queues but config specifies {}",
                self.name, current_queues, self.queues
            );
        } else {
            debug!("✓ Queue count verified: {} queues", current_queues);
        }

        // 3. Check ring sizes
        if let Ok((current_rx, current_tx)) = self.get_current_ring_sizes() {
            if current_rx != self.rxd || current_tx != self.txd {
                warn!(
                    "Interface {} ring sizes: RX={} (config: {}), TX={} (config: {})",
                    self.name, current_rx, self.rxd, current_tx, self.txd
                );
            } else {
                debug!("✓ Ring sizes verified: RX={}, TX={}", current_rx, current_tx);
            }
        }

        Ok(())
    }

    /// Get detailed interface information for debugging
    pub fn get_info(&self) -> Result<String> {
        let mut info = String::new();

        // Get link info
        let output = Command::new("ip").args(&["-d", "link", "show", &self.name]).output()?;

        if output.status.success() {
            info.push_str(&format!("=== Interface {} ===\n", self.name));
            info.push_str(&String::from_utf8_lossy(&output.stdout));
            info.push_str("\n");
        }

        // Get ethtool info
        let output = Command::new("ethtool").args(&["-l", &self.name]).output()?;

        if output.status.success() {
            info.push_str("=== Queue Configuration ===\n");
            info.push_str(&String::from_utf8_lossy(&output.stdout));
            info.push_str("\n");
        }

        Ok(info)
    }
}

/// Validate that all interfaces in the configuration exist on the system
/// Checks both the ifaces array and the xsk mappings
/// Skips veth interfaces that will be created
pub fn validate_interfaces_exist(
    ifaces: &[crate::toml_parser::InterfaceConfig],
    xsk_mappings: &[crate::toml_parser::XskMapping],
) -> Result<()> {
    use std::collections::{HashSet, HashMap};

    info!("Validating interface existence...");

    // Build a map of interface name -> type
    let mut iface_types: HashMap<String, Option<String>> = HashMap::new();
    for iface_cfg in ifaces {
        iface_types.insert(iface_cfg.name.clone(), iface_cfg.r#type.clone());
    }

    // Collect all unique interface names from both ifaces array and xsk mappings
    let mut all_ifaces = HashSet::new();

    // Add interfaces from ifaces array
    for iface_cfg in ifaces {
        all_ifaces.insert(iface_cfg.name.as_str());
    }

    // Add interfaces from xsk mappings (these are the ones actually being used)
    for xsk in xsk_mappings {
        all_ifaces.insert(xsk.iface.as_str());
    }

    // Check each unique interface exists (skip veth interfaces that will be created)
    for iface_name in &all_ifaces {
        let is_veth = iface_types
            .get(*iface_name)
            .and_then(|t| t.as_ref())
            .map(|t| t == "veth")
            .unwrap_or(false);

        if is_veth {
            debug!(
                "Skipping existence check for veth interface '{}' (will be created)",
                iface_name
            );
            continue;
        }

        let output = Command::new("ip").args(&["link", "show", iface_name]).output()?;

        if !output.status.success() {
            return Err(anyhow!(
                "Interface '{}' does not exist on the system. Please check your TOML configuration (ifaces or xsk sections).",
                iface_name
            ));
        }
        debug!("✓ Interface '{}' exists", iface_name);
    }

    info!("✓ All interface(s) validated",);
    Ok(())
}

/// Configure all interfaces from the configuration
/// Queue count is determined by the maximum queue ID used in XSK mappings
pub fn check_or_configure_interfaces(
    ifaces: &[crate::toml_parser::InterfaceConfig],
    xsk_mappings: &[crate::toml_parser::XskMapping],
    b_just_check: bool,
) -> Result<Vec<InterfaceSetup>> {
    use std::collections::HashMap;

    // First, validate that all interfaces exist
    validate_interfaces_exist(ifaces, xsk_mappings)?;

    let mut configured = Vec::new();

    // Build a map of interface name -> max queue count
    let mut iface_max_queues: HashMap<String, u32> = HashMap::new();

    for xsk in xsk_mappings {
        let max_queue = xsk.queues.iter().max().copied().unwrap_or(0);
        let entry = iface_max_queues.entry(xsk.iface.clone()).or_insert(0);
        *entry = (*entry).max(max_queue + 1); // +1 because queues are 0-indexed
    }

    for iface_cfg in ifaces {
        let queues = iface_max_queues.get(&iface_cfg.name).copied().unwrap_or(1);

        // Determine ring sizes: use config values or query hardware maximums
        let (rxd, txd) = match (iface_cfg.rxd, iface_cfg.txd) {
            (Some(rx), Some(tx)) => (rx, tx),
            _ => {
                // If either rxd or txd is not specified, query the hardware maximums
                // Create a temporary setup to query the interface
                let temp_setup = InterfaceSetup {
                    name: iface_cfg.name.clone(),
                    queues: 1,
                    rxd: 0,
                    txd: 0,
                    iface_type: None,
                    namespace: None,
                    peer: None,
                    mac_if: None,
                    ipnet: None,
                };

                match temp_setup.get_max_ring_sizes() {
                    Ok((max_rx, max_tx)) => {
                        if !b_just_check {
                            info!(
                                "Using maximum hardware ring sizes for {}: RX={}, TX={}",
                                iface_cfg.name, max_rx, max_tx
                            );
                        }
                        (iface_cfg.rxd.unwrap_or(max_rx), iface_cfg.txd.unwrap_or(max_tx))
                    }
                    Err(e) => {
                        if !b_just_check {
                            warn!(
                                "Failed to query maximum ring sizes for {}: {}. Using defaults: RX=4096, TX=4096",
                                iface_cfg.name, e
                            );
                        }
                        (iface_cfg.rxd.unwrap_or(4096), iface_cfg.txd.unwrap_or(4096))
                    }
                }
            }
        };

        let setup = InterfaceSetup {
            name: iface_cfg.name.clone(),
            queues,
            rxd,
            txd,
            iface_type: iface_cfg.r#type.clone(),
            namespace: iface_cfg.namespace.clone(),
            peer: iface_cfg.peer.clone(),
            mac_if: iface_cfg.mac_if.clone(),
            ipnet: iface_cfg.ipnet.clone(),
        };

        setup.configure(b_just_check)?;

        // Set IRQ affinity if specified
        if let Some(irq_core) = iface_cfg.irq_core {
            if let Err(e) = xbricks_base::system::set_irq_affinity(&iface_cfg.name, irq_core) {
                warn!("Failed to set IRQ affinity for {}: {}", iface_cfg.name, e);
            }
        }

        configured.push(setup);
    }

    Ok(configured)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interface_setup_struct() {
        let setup = InterfaceSetup {
            name: "lo".to_string(),
            queues: 1,
            rxd: 512,
            txd: 512,
            iface_type: None,
            namespace: None,
            peer: None,
            mac_if: None,
            ipnet: None,
        };

        assert_eq!(setup.name, "lo");
        assert_eq!(setup.queues, 1);
    }
}
