// src/config/mod.rs
use serde::{Deserialize, Deserializer, Serialize};
use serde::de::{MapAccess, Visitor};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use ts_rs::TS;
use xbricks_protocol::helpers::parse_suffixed_number;
use xbricks_base::RuntimeUmemConfig;
use xbricks_base::frame_registry::MAX_UMEM;
use xbricks_base::node_instance_id::NodeInstanceId;
use crate::config::hot_config::NodeDynamicState;
use crate::system_builder::SystemBuilder;
use crate::xsocket::XdpSocketConfig;


/// Persisted position of a single graph node, stored under [layout] in xbricks.toml.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SavedPosition {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
#[ts(export_to = "XBricksConfig.ts")] // This "activates" the environment variable path
pub struct XBricksConfig {
    pub xbricks: XBricksSection,
    #[serde(default)]
    pub pipelines: Vec<PipelineConfig>,
    /// UI layout positions — written by the frontend, ignored by the dataplane.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[ts(skip)]
    pub layout: HashMap<String, SavedPosition>,
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct XBricksSection {
    #[serde(default = "default_ifaces")]
    pub ifaces: Vec<InterfaceConfig>,
    #[serde(default = "default_umem_configs")]
    pub umem: Vec<UmemConfig>,
    #[serde(default = "default_xsk_mappings")]
    pub xsk: Vec<XskMapping>,
    #[serde(default = "default_workers")]
    pub workers: Vec<WorkerConfig>,
}

/// Provides the default worker configuration
fn default_workers() -> Vec<WorkerConfig> {
    vec![WorkerConfig {
        id: 0,
        core: 0,
        partitions: vec![PartitionRequest { umem: 0, count: 1 }],
    }]
}

// Provides the default hardware interface configuration
fn default_ifaces() -> Vec<InterfaceConfig> {
    vec![InterfaceConfig {
        name: "lo".to_string(),
        rxd: None,
        txd: None,
        irq_core: None,
        r#type: None,
        namespace: None,
        peer: None,
        mac_if: None,
        ipnet: None,
        master: None,
        checksum_offload: None,
    }]
}
fn default_umem_configs() -> Vec<UmemConfig> {
    // Return a default vector, perhaps with one standard 4KB-frame Umem
    vec![UmemConfig {
        id: 0,
        frame_size: 2048,
        frame_count: 65536,
        fq_size: 8192,
        cq_size: 8192,
        head_room: 128,
        partitions: 1,
        reserved: 0,
    }]
}

fn default_xsk_mappings() -> Vec<XskMapping> {
    vec![XskMapping {
        iface: "lo".to_string(),
        queues: vec![0],
        umem: 0,
        rxq_size: 8192,
        txq_size: 8192,
        zerocopy: true,
        xdp_mode: default_xdp_mode(),
    }]
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct InterfaceConfig {
    pub name: String,
    #[serde(default)]
    pub rxd: Option<u32>,
    #[serde(default)]
    pub txd: Option<u32>,
    #[serde(default)]
    pub irq_core: Option<usize>,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub peer: Option<String>,
    #[serde(default)]
    pub mac_if: Option<String>,
    #[serde(default)]
    pub ipnet: Option<String>,
    #[serde(default)]
    pub master: Option<String>,
    /// Whether to enable TX checksum offload on this interface.
    /// Defaults to false. For veth interfaces the ethtool command targets the peer interface.
    #[serde(default)]
    pub checksum_offload: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct UmemConfig {
    /// umem_group_id
    pub id: usize,
    pub frame_size: u32,
    ///frame_count is per Umem group.
    pub frame_count: u32,
    pub fq_size: u32,
    pub cq_size: u32,
    pub head_room: u32,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// Number of frames at the start of this UMEM exclusively reserved for WASM plugins.
    /// These frames are never assigned to worker partitions or sockets.
    #[serde(default)]
    pub reserved: u32,
}

fn default_partitions() -> u32 {
    1
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct XskMapping {
    pub iface: String,
    pub queues: Vec<u32>,
    pub umem: usize,
    pub rxq_size: u32,
    pub txq_size: u32,
    pub zerocopy: bool,
    /// XDP mode: "native" (driver), "skb" (generic), or "auto" (default: auto-detect based on interface type)
    #[serde(default = "default_xdp_mode")]
    pub xdp_mode: String,
}

fn default_xdp_mode() -> String {
    "auto".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct PartitionRequest {
    /// umem_group_id
    pub umem: usize,
    pub count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct WorkerConfig {
    pub id: usize,
    pub core: usize,
    #[serde(default)]
    pub partitions: Vec<PartitionRequest>,
}

fn default_poll_frequency() -> u64 {
    1000
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct PipelineConfig {
    pub name: String,
    pub workers: Vec<usize>,
    pub nodes: Vec<NodeConfig>,
    #[serde(default = "default_poll_frequency")]
    pub poll_frequency: u64,
}

fn deserialize_args<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ArgsVisitor;

    impl<'de> Visitor<'de> for ArgsVisitor {
        type Value = HashMap<String, String>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map with string keys and string or primitive values")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut result = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let value = map.next_value::<toml::Value>()?;
                let s = match value {
                    toml::Value::String(s) => s,
                    toml::Value::Integer(i) => i.to_string(),
                    toml::Value::Float(f) => f.to_string(),
                    toml::Value::Boolean(b) => b.to_string(),
                    other => return Err(serde::de::Error::custom(
                        format!("unsupported value type in args: {:?}", other)
                    )),
                };
                result.insert(key, s);
            }
            Ok(result)
        }
    }

    deserializer.deserialize_map(ArgsVisitor)
}

fn deserialize_wasm_data<'de, D>(deserializer: D) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct WasmDataVisitor;

    impl<'de> Visitor<'de> for WasmDataVisitor {
        type Value = Option<HashMap<String, String>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("null or a map with string keys and string or primitive values")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            deserialize_args(d).map(Some)
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut result = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let value = map.next_value::<toml::Value>()?;
                let s = match value {
                    toml::Value::String(s) => s,
                    toml::Value::Integer(i) => i.to_string(),
                    toml::Value::Float(f) => f.to_string(),
                    toml::Value::Boolean(b) => b.to_string(),
                    other => return Err(serde::de::Error::custom(
                        format!("unsupported value type in wasm_data: {:?}", other)
                    )),
                };
                result.insert(key, s);
            }
            Ok(Some(result))
        }
    }

    deserializer.deserialize_option(WasmDataVisitor)
}

#[derive(Debug, Deserialize, Serialize, Clone, TS)]
pub struct NodeConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub routes: Vec<String>,
    #[serde(default)]
    pub queues: Option<Vec<u32>>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_args")]
    pub args: HashMap<String, String>,
    /// Hot-updatable args: parsed once at startup, then mirrored into the typed
    /// NodeDynamicState for the fast path. Values here take precedence over `args`.
    #[serde(default, deserialize_with = "deserialize_args")]
    pub hot_args: HashMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_wasm_data")]
    pub wasm_data: Option<HashMap<String, String>>,
}
#[derive(Debug, Clone)]
pub struct NodeInstance {
    pub node_config: NodeConfig,
    pub state: NodeDynamicState,
    pub node_instance_id: NodeInstanceId,
}


impl NodeConfig {
    /// Returns the value for `key` from `hot_args` if present, otherwise from `args`.
    pub fn effective_arg(&self, key: &str) -> Option<&String> {
        self.hot_args.get(key).or_else(|| self.args.get(key))
    }
}

/// Represents a specific instance of a pipeline assigned to a worker.
/// A worker might run multiple of these (e.g., "wire f0->f1" and "wire f1->f0").
#[derive(Debug, Clone)]
pub struct PipelineInstance {
    pub name: String,
    pub nodes: Vec<NodeInstance>,
    pub poll_frequency: u64,
}

/// Configuration for a single Worker Thread.
#[derive(Clone)]
pub struct WorkerBuilderConfig {
    pub id: u32,                          // The Worker ID (unique)
    pub core_id: u32,                     // The CPU Core to pin to
    pub pipelines: Vec<PipelineInstance>, // List of pipelines this worker runs
}



impl XBricksConfig {
    /// Load configuration from a TOML file and validate it
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_toml_str(&contents)
    }

    pub fn from_toml_str(toml_str: &str) -> Result<Self> {
        let mut config: XBricksConfig = toml::from_str(toml_str)?;
        for pipeline in &mut config.pipelines {
            for node in &mut pipeline.nodes {
                if node.node_type == "PacketGenerator" {
                    node.args.remove("cmd");
                }
            }
        }
        config.validate()?;
        Ok(config)
    }

    /// sets the name of the interface for single i/f applications like ipblower
    pub fn set_interface_0(&mut self, iface_name: &str) {
        self.xbricks.ifaces[0].name = iface_name.to_string();
        self.xbricks.xsk[0].iface = iface_name.to_string();
    }

    pub fn set_zerocopy(&mut self, b_zc: bool) {
        self.xbricks.xsk[0].zerocopy = b_zc;
    }

    pub fn set_worker_core(&mut self, core: usize) {
        self.xbricks.workers[0].core = core;
    }

    pub fn create_generator_pipeline(
        &mut self,
        num_workers: usize,
        base_core: usize,
        batch_sz: usize,
        pps: &str,
        no_packets: &str,
        dst_ip: Option<&str>,
        dst_mac: Option<&str>,
    ) {
        let total_pps = parse_suffixed_number(pps).unwrap_or(u64::MAX);
        let total_packets = parse_suffixed_number(no_packets).unwrap_or(0);

        // Calculate per-worker values for PPS
        let pps_per_worker = if total_pps == u64::MAX {
            u64::MAX
        } else {
            total_pps / num_workers as u64
        };

        // Calculate batch distribution
        let packets_per_worker_vec: Vec<u64> = if total_packets == 0 {
            vec![0; num_workers]
        } else {
            let total_batches = (total_packets + batch_sz as u64 - 1) / batch_sz as u64;
            let base_batches_per_worker = total_batches / num_workers as u64;
            let extra_batches = total_batches % num_workers as u64;

            (0..num_workers)
                .map(|i| {
                    let batches = if i < extra_batches as usize {
                        base_batches_per_worker + 1
                    } else {
                        base_batches_per_worker
                    };
                    batches * batch_sz as u64
                })
                .collect()
        };

        // Safely structure UMEM configuration to use partitions
        self.xbricks.umem[0].partitions = num_workers as u32;
        self.xbricks.umem[0].frame_count = 65536 * num_workers as u32;
        self.xbricks.umem[0].head_room = 256;

        // Clear and build Workers dynamically
        self.xbricks.workers.clear();
        for worker_id in 0..num_workers {
            let worker = WorkerConfig {
                id: worker_id,
                core: base_core + worker_id,
                partitions: vec![PartitionRequest { umem: 0, count: 1 }],
            };
            self.xbricks.workers.push(worker);
        }

        // Clear and build XSK mappings for each queue
        self.xbricks.xsk.clear();
        for queue_id in 0..num_workers {
            let xsk_mapping = XskMapping {
                iface: self.xbricks.ifaces[0].name.clone(),
                queues: vec![queue_id as u32],
                umem: 0,
                rxq_size: 8192,
                txq_size: 8192,
                zerocopy: true,
                xdp_mode: default_xdp_mode(),
            };
            self.xbricks.xsk.push(xsk_mapping);
        }

        // Create a separate pipeline for each worker
        for worker_id in 0..num_workers {
            let packets_for_this_worker = packets_per_worker_vec[worker_id];

            // Naming standard: Even for a single worker, it becomes "generator_0"
            let pipeline = PipelineConfig {
                name: format!("generator_{}", worker_id),
                workers: vec![worker_id],
                nodes: vec![
                    // Node 1: PacketGenerator source
                    NodeConfig {
                        id: format!("wire.pctgen_{}", worker_id),
                        node_type: "PacketGenerator".to_string(),
                        routes: vec![format!("wire.tx_sink_{}", worker_id)],
                        queues: None,
                        labels: None,
                        args: {
                            let mut args = HashMap::new();
                            args.insert("umem".to_string(), "0".to_string());
                            args.insert("batch_size".to_string(), batch_sz.to_string());
                            args.insert("no-packets".to_string(), packets_for_this_worker.to_string());
                            args.insert("pps".to_string(), pps_per_worker.to_string());
                            args.insert("autostart".to_string(), "true".to_string());
                            if let Some(ip) = dst_ip {
                                args.insert("dst_ip".to_string(), ip.to_string());
                            }
                            if let Some(mac) = dst_mac {
                                args.insert("dst_mac".to_string(), mac.to_string());
                            }
                            args
                        },
                        hot_args: HashMap::new(),
                        wasm_data: None,
                    },
                    // Node 2: BatchTxNode sink
                    NodeConfig {
                        id: format!("wire.tx_sink_{}", worker_id),
                        node_type: "BatchTxNode".to_string(),
                        routes: vec![],
                        queues: Some(vec![worker_id as u32]),
                        labels: None,
                        args: {
                            let mut args = HashMap::new();
                            args.insert("sink_type".to_string(), "xsk".to_string());
                            args.insert("interface".to_string(), self.xbricks.ifaces[0].name.clone());
                            args
                        },
                        hot_args: HashMap::new(),
                        wasm_data: None,
                    },
                ],
                poll_frequency: default_poll_frequency(),
            };

            self.pipelines.push(pipeline);
        }
    }

    /// Validates that the defined UMEM resources are mathematically sufficient
    /// to support the requested XSK rings, hardware queues, and reserved frames.
    pub fn validate_umem_resources(&self) -> Result<()> {
        for umem in &self.xbricks.umem {
            let mut hardware_locked_frames = 0;
            let mut socket_count = 0;

            // Find all XSK mappings that use this UMEM
            for xsk in &self.xbricks.xsk {
                if xsk.umem == umem.id {
                    let queues = xsk.queues.len() as u32;
                    socket_count += queues;

                    // The driver aggressively fills the RX ring from the Fill Queue (FQ).
                    // To be mathematically safe, we assume the hardware can lock up:
                    // FQ + RX ring + TX ring frames per socket.
                    hardware_locked_frames += queues * (umem.fq_size + xsk.rxq_size + xsk.txq_size);
                }
            }

            let total_required = umem.reserved + hardware_locked_frames;

            // We need frames left over for the application pipelines to actually generate/buffer packets
            // We assume a minimum safe buffer of ~4096 frames per partition.
            let app_breathing_room = 4096 * umem.partitions;
            let safe_minimum = total_required + app_breathing_room;

            if umem.frame_count <= total_required {
                anyhow::bail!(
                    "🔥 UMEM {} Configuration Trap! Total frames ({}) cannot support the requested hardware rings.\n\
                    Reserved: {}, Hardware Locked (FQ+RX+TX across {} sockets): {}.\n\
                    Shortfall: {} frames.\n\
                    Fix: Increase `frame_count` or decrease ring sizes (fq_size, rxq_size) in your TOML!",
                    umem.id, umem.frame_count,
                    umem.reserved, socket_count, hardware_locked_frames,
                    total_required - umem.frame_count
                );
            } else if umem.frame_count < safe_minimum {
                log::warn!(
                    "⚠️ UMEM {} is dangerously low on free frames.\n\
                    Total: {}, Hardware+Reserved: {}. Leaves only {} frames for {} workers to share.\n\
                    This may cause 'Starvation!' warnings under load. Consider increasing frame_count.",
                    umem.id, umem.frame_count, total_required,
                    umem.frame_count - total_required, umem.partitions
                );
            } else {
                log::info!(
                    "✓ UMEM {} memory math verified (Total: {}, Hardware Locked: {}, Free for app: {})",
                    umem.id, umem.frame_count, total_required, umem.frame_count - total_required
                );
            }
        }
        Ok(())
    }


    /// Validate the configuration for consistency and correctness
    fn validate(&self) -> Result<()> {
        let section = &self.xbricks;

        // 1. Validate that at least one of each component exists

        if section.umem.is_empty() {
            return Err(anyhow!("Configuration must define at least one UMEM"));
        }
        if section.xsk.is_empty() {
            return Err(anyhow!("Configuration must define at least one XSK mapping"));
        }

        // 2. Validate UMEM configurations
        let mut umem_ids = HashSet::new();
        for umem_cfg in &section.umem {
            // Check UMEM ID fits in 8 bits (required by encode_global_frame)
            if umem_cfg.id >= MAX_UMEM {
                return Err(anyhow!(
                    "UMEM id {} exceeds maximum of {} (must fit in 8 bits for frame encoding)",
                    umem_cfg.id, MAX_UMEM - 1
                ));
            }

            // Check for duplicate UMEM IDs
            if !umem_ids.insert(umem_cfg.id) {
                return Err(anyhow!("Duplicate UMEM id: {}", umem_cfg.id));
            }

            // Validate UMEM parameters
            if umem_cfg.frame_size == 0 {
                return Err(anyhow!("UMEM {}: frame_size must be > 0 and power of 2", umem_cfg.id));
            }
            if umem_cfg.frame_count == 0 {
                return Err(anyhow!("UMEM {}: frame_count must be > 0", umem_cfg.id));
            }
            if umem_cfg.fq_size == 0 {
                return Err(anyhow!("UMEM {}: fq_size must be > 0", umem_cfg.id));
            }
            if umem_cfg.cq_size == 0 {
                return Err(anyhow!("UMEM {}: cq_size must be > 0", umem_cfg.id));
            }

            // Validate frame_size is a power of 2 (AF_XDP requirement)
            if !umem_cfg.frame_size.is_power_of_two() {
                return Err(anyhow!(
                    "UMEM {}: frame_size must be a power of 2, got {}",
                    umem_cfg.id,
                    umem_cfg.frame_size
                ));
            }

            // Validate head_room is reasonable
            if umem_cfg.head_room >= umem_cfg.frame_size {
                return Err(anyhow!(
                    "UMEM {}: head_room ({}) must be < frame_size ({})",
                    umem_cfg.id,
                    umem_cfg.head_room,
                    umem_cfg.frame_size
                ));
            }

            // Validate partition count is at least 1
            if umem_cfg.partitions == 0 {
                return Err(anyhow!(
                    "UMEM {}: partitions must be at least 1, got {}",
                    umem_cfg.id,
                    umem_cfg.partitions
                ));
            }

            // Validate partition count is a power of 2
            if !umem_cfg.partitions.is_power_of_two() {
                return Err(anyhow!(
                    "UMEM {}: partitions must be a power of 2, got {}",
                    umem_cfg.id,
                    umem_cfg.partitions
                ));
            }

            // Validate reserved frame count
            if umem_cfg.reserved >= umem_cfg.frame_count {
                return Err(anyhow!(
                    "UMEM {}: reserved ({}) must be < frame_count ({})",
                    umem_cfg.id,
                    umem_cfg.reserved,
                    umem_cfg.frame_count
                ));
            }

            // Validate that non-reserved frames are evenly divisible into partitions
            let usable_frames = umem_cfg.frame_count - umem_cfg.reserved;
            if usable_frames % umem_cfg.partitions != 0 {
                return Err(anyhow!(
                    "UMEM {}: usable frames ({} = frame_count {} - reserved {}) must be evenly divisible by partitions ({})",
                    umem_cfg.id,
                    usable_frames,
                    umem_cfg.frame_count,
                    umem_cfg.reserved,
                    umem_cfg.partitions
                ));
            }
        }

        // 3. Validate interface configurations
        let mut iface_names = HashSet::new();
        for iface_cfg in &section.ifaces {
            // Check for duplicate interface names
            if !iface_names.insert(iface_cfg.name.clone()) {
                return Err(anyhow!("Duplicate interface name: {}", iface_cfg.name));
            }

            // Check that interface exists on the system, unless it's a veth that will be created
            let is_veth = iface_cfg.r#type.as_ref().map(|t| t == "veth").unwrap_or(false);

            if !is_veth {
                let output = std::process::Command::new("ip")
                    .args(&["link", "show", &iface_cfg.name])
                    .output()
                    .map_err(|e| anyhow!("Failed to check interface existence: {}", e))?;

                if !output.status.success() {
                    return Err(anyhow!(
                        "Interface '{}' defined in ifaces array does not exist on the system",
                        iface_cfg.name
                    ));
                }
            } else {
                // Validate veth-specific fields
                if iface_cfg.peer.is_none() {
                    return Err(anyhow!("Veth interface '{}' requires 'peer' field", iface_cfg.name));
                }
                if iface_cfg.namespace.is_none() && iface_cfg.master.is_none() {
                    return Err(anyhow!(
                        "Veth interface '{}' requires either 'namespace' or 'master' field",
                        iface_cfg.name
                    ));
                }
            }
        }

        // 4. Validate XSK mappings
        for (idx, xsk_cfg) in section.xsk.iter().enumerate() {
            // Validate referenced interface is defined in ifaces array
            if !iface_names.contains(&xsk_cfg.iface) {
                return Err(anyhow!(
                    "XSK mapping {}: references interface '{}' which is not defined in the ifaces array. Add it to ifaces like: {{ name = \"{}\" }}",
                    idx,
                    xsk_cfg.iface,
                    xsk_cfg.iface
                ));
            }

            // Validate referenced UMEM exists
            if !umem_ids.contains(&xsk_cfg.umem) {
                return Err(anyhow!(
                    "XSK mapping {}: references non-existent UMEM id {}",
                    idx,
                    xsk_cfg.umem
                ));
            }

            // Validate queues list is not empty
            if xsk_cfg.queues.is_empty() {
                return Err(anyhow!("XSK mapping {}: queues list cannot be empty", idx));
            }

            // Validate queue sizes
            if xsk_cfg.rxq_size == 0 {
                return Err(anyhow!("XSK mapping {}: rxq_size must be > 0", idx));
            }
            if xsk_cfg.txq_size == 0 {
                return Err(anyhow!("XSK mapping {}: txq_size must be > 0", idx));
            }

            // Validate queue sizes are powers of 2 (AF_XDP requirement)
            if !xsk_cfg.rxq_size.is_power_of_two() {
                return Err(anyhow!(
                    "XSK mapping {}: rxq_size must be a power of 2, got {}",
                    idx,
                    xsk_cfg.rxq_size
                ));
            }
            if !xsk_cfg.txq_size.is_power_of_two() {
                return Err(anyhow!(
                    "XSK mapping {}: txq_size must be a power of 2, got {}",
                    idx,
                    xsk_cfg.txq_size
                ));
            }
        }

        // 5. Validate worker configurations
        if section.workers.is_empty() {
            return Err(anyhow!("Configuration must define at least one worker"));
        }

        let mut worker_ids = HashSet::new();
        let core_ids = core_affinity::get_core_ids().ok_or_else(|| anyhow!("Failed to get CPU core IDs"))?;
        let max_core_id = core_ids.len();

        for worker_cfg in &section.workers {
            // Check for duplicate worker IDs
            if !worker_ids.insert(worker_cfg.id) {
                return Err(anyhow!("Duplicate worker id: {}", worker_cfg.id));
            }

            // Validate core ID is within available cores
            if worker_cfg.core >= max_core_id {
                return Err(anyhow!(
                    "Worker {}: core {} exceeds available CPU cores ({})",
                    worker_cfg.id,
                    worker_cfg.core,
                    max_core_id
                ));
            }

            // Validate partition requests
            for partition_req in &worker_cfg.partitions {
                // Check that referenced UMEM exists
                if !umem_ids.contains(&partition_req.umem) {
                    return Err(anyhow!(
                        "Worker {}: partition request references non-existent UMEM id {}",
                        worker_cfg.id,
                        partition_req.umem
                    ));
                }

                // Check that count is > 0
                if partition_req.count == 0 {
                    return Err(anyhow!(
                        "Worker {}: partition request for UMEM {} has count = 0",
                        worker_cfg.id,
                        partition_req.umem
                    ));
                }

                // Find the UMEM config to check partition count
                let umem_cfg = section.umem.iter().find(|u| u.id == partition_req.umem).unwrap(); // Safe because we already checked umem_ids contains it

                // Check that requested count doesn't exceed available partitions
                if partition_req.count > umem_cfg.partitions as usize {
                    return Err(anyhow!(
                        "Worker {}: requests {} partitions from UMEM {} which only has {} partitions",
                        worker_cfg.id,
                        partition_req.count,
                        partition_req.umem,
                        umem_cfg.partitions
                    ));
                }
            }
        }

        // 5a. Validate total partition allocation doesn't exceed available partitions per UMEM
        let mut partition_allocation = vec![0usize; MAX_UMEM];

        for worker_cfg in &section.workers {
            for partition_req in &worker_cfg.partitions {
                partition_allocation[partition_req.umem] += partition_req.count;
            }
        }

        for umem_cfg in &section.umem {
            let total_requested = partition_allocation[umem_cfg.id];
            if total_requested > umem_cfg.partitions as usize {
                return Err(anyhow!(
                    "UMEM {}: total requested partitions ({}) exceeds available partitions ({})",
                    umem_cfg.id,
                    total_requested,
                    umem_cfg.partitions
                ));
            }
        }

        // 6. Validate pipeline configurations
        for pipeline in &self.pipelines {
            // Validate pipeline has at least one worker
            if pipeline.workers.is_empty() {
                return Err(anyhow!("Pipeline '{}': must have at least one worker", pipeline.name));
            }

            // Validate all referenced workers exist
            for &worker_id in &pipeline.workers {
                if !worker_ids.contains(&worker_id) {
                    return Err(anyhow!(
                        "Pipeline '{}': references non-existent worker {}",
                        pipeline.name,
                        worker_id
                    ));
                }
            }

            // Validate pipeline has at least one node
            if pipeline.nodes.is_empty() {
                return Err(anyhow!("Pipeline '{}': must have at least one node", pipeline.name));
            }

            // Collect node IDs and validate uniqueness within pipeline
            let mut node_ids = HashSet::new();
            for node in &pipeline.nodes {
                if !node_ids.insert(node.id.clone()) {
                    return Err(anyhow!("Pipeline '{}': duplicate node id '{}'", pipeline.name, node.id));
                }
            }

            // Validate node references (routes, default)
            for node in &pipeline.nodes {
                for route_id in &node.routes {
                    if !node_ids.contains(route_id) {
                        return Err(anyhow!(
                            "Pipeline '{}': node '{}' routes to non-existent node '{}'",
                            pipeline.name,
                            node.id,
                            route_id
                        ));
                    }
                }

                // Validate PacketGenerator specific requirements
                if node.node_type == "PacketGenerator" {
                    // Accept pps from args (e.g. args = { pps = "1M" })
                    match node.effective_arg("pps") {
                        None => {
                            return Err(anyhow!(
                                "Pipeline '{}': node '{}' (PacketGenerator) requires 'pps' parameter",
                                pipeline.name,
                                node.id
                            ));
                        }
                        Some(pps_str) => match parse_suffixed_number(pps_str) {
                            Err(_) => {
                                return Err(anyhow!(
                                    "Pipeline '{}': node '{}' (PacketGenerator) invalid pps value '{}'",
                                    pipeline.name,
                                    node.id,
                                    pps_str
                                ));
                            }
                            Ok(0) => {
                                return Err(anyhow!(
                                    "Pipeline '{}': node '{}' (PacketGenerator) pps must be greater than 0",
                                    pipeline.name,
                                    node.id
                                ));
                            }
                            Ok(_) => {} // Valid
                        },
                    }
                }

                // Validate Rx/Tx nodes have queues array matching workers array length
                if node.node_type == "BatchRxNode" || node.node_type == "BatchTxNode" {
                    if node.queues.is_some() && node.queues.as_ref().unwrap().len() != pipeline.workers.len() {
                        return Err(anyhow!(
                            "Pipeline '{}': node '{}' queues array length ({}) must match workers array length ({})",
                            pipeline.name,
                            node.id,
                            node.queues.as_ref().unwrap().len(),
                            pipeline.workers.len()
                        ));
                    }

                    // Validate referenced interface is defined in ifaces array
                    if let Some(iface) = node.args.get("interface") {
                        if !iface_names.contains(iface) {
                            return Err(anyhow!(
                                "Pipeline '{}': node '{}' references interface '{}' which is not defined in the ifaces array. Add it to ifaces like: {{ name = \"{}\" }}",
                                pipeline.name,
                                node.id,
                                iface,
                                iface
                            ));
                        }

                        // Validate that each queue referenced has a corresponding XSK socket
                        if node.queues.is_some() { for &queue_id in node.queues.as_ref().unwrap() {
                            let has_xsk_socket = section
                                .xsk
                                .iter()
                                .any(|xsk| xsk.iface == *iface && xsk.queues.contains(&queue_id));

                            if !has_xsk_socket {
                                return Err(anyhow!(
                                    "Pipeline '{}': node '{}' references queue {} on interface '{}', but no XSK socket is configured for this queue.\n\
                                     Add the queue to xsk config like: {{ iface = \"{}\", queues = [{}], umem = 0, ... }}",
                                    pipeline.name,
                                    node.id,
                                    queue_id,
                                    iface,
                                    iface,
                                    queue_id
                                ));
                            }
                        }}
                    }
                }
            }

            // Validate WasmInspectorNode upstream umem consistency via Graph Propagation:
            // Any node that routes into a WasmInspectorNode (no matter how deep the chain)
            // must deliver frames from the same UMEM the WasmInspectorNode is bound to.
            {
                // Step A: Build interface -> umem mapping
                let mut iface_umems: HashMap<&str, Vec<usize>> = HashMap::new();
                for xsk in &section.xsk {
                    iface_umems.entry(xsk.iface.as_str()).or_default().push(xsk.umem);
                }

                // Step B: Build adjacency list (node -> targets)
                let mut targets_of: HashMap<&str, Vec<&str>> = HashMap::new();
                for node in &pipeline.nodes {
                    let targets = targets_of.entry(node.id.as_str()).or_default();
                    for route_id in &node.routes {
                        targets.push(route_id.as_str());
                    }
                }

                // Step C: Worklist for UMEM propagation
                let mut output_umems: HashMap<&str, HashSet<usize>> = HashMap::new();
                let mut worklist: Vec<&str> = Vec::new();

                // Initialize graph sources (Rx Nodes and Generators)
                for node in &pipeline.nodes {
                    let mut initial_umems = HashSet::new();

                    if node.node_type == "BatchRxNode" {
                        if let Some(iface) = node.args.get("interface") {
                            if let Some(umems) = iface_umems.get(iface.as_str()) {
                                for &u in umems {
                                    initial_umems.insert(u);
                                }
                            }
                        }
                    } else if node.node_type == "PacketGenerator" {
                        if let Some(u_str) = node.args.get("umem") {
                            if let Ok(u) = u_str.parse::<usize>() {
                                initial_umems.insert(u);
                            }
                        }
                    }

                    if !initial_umems.is_empty() {
                        output_umems.insert(node.id.as_str(), initial_umems);
                        worklist.push(node.id.as_str());
                    }
                }

                // Step D: Propagate UMEMs through the pipeline DAG
                while let Some(current_id) = worklist.pop() {
                    let current_outputs = output_umems.get(current_id).cloned().unwrap_or_default();
                    if let Some(targets) = targets_of.get(current_id) {
                        for &target_id in targets {
                            let target_outputs = output_umems.entry(target_id).or_default();
                            let mut changed = false;

                            for &u in &current_outputs {
                                if target_outputs.insert(u) {
                                    changed = true;
                                }
                            }
                            // If this node received a new UMEM, we must propagate its outputs
                            if changed {
                                worklist.push(target_id);
                            }
                        }
                    }
                }

                // Step E: Validate WasmInspectorNodes against propagated data
                for node in &pipeline.nodes {
                    if node.node_type == "WasmInspectorNode" {
                        let wasm_umem_str = node.args.get("umem").ok_or_else(|| {
                            anyhow!("Pipeline '{}': WasmInspectorNode '{}' is missing 'umem' argument", pipeline.name, node.id)
                        })?;

                        let wasm_umem = wasm_umem_str.parse::<usize>().map_err(|_| {
                            anyhow!("Pipeline '{}': Invalid 'umem' format for WasmInspectorNode '{}'", pipeline.name, node.id)
                        })?;

                        // Check if any propagated UMEM reaching this node differs from its bound UMEM
                        if let Some(reached_umems) = output_umems.get(node.id.as_str()) {
                            for &u in reached_umems {
                                if u != wasm_umem {
                                    return Err(anyhow!(
                                        "Pipeline '{}': WasmInspectorNode '{}' is bound to umem {}, but can receive PDUs from umem {} via upstream nodes.",
                                        pipeline.name, node.id, wasm_umem, u
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // 7. Validate no duplicate queue assignments across pipelines
        // Build a map of (interface, queue) -> (pipeline_name, node_id, worker_id)
        // to detect conflicts where two different pipeline instances try to use the same queue
        use std::collections::HashMap;
        let mut queue_assignments: HashMap<(String, u32), Vec<(String, String, usize)>> = HashMap::new();

        for pipeline in &self.pipelines {
            for node in &pipeline.nodes {
                // Only check Rx and Tx nodes as they actually bind to hardware queues
                if node.node_type == "BatchRxNode" || node.node_type == "BatchTxNode" {
                    if let Some(interface) = node.args.get("interface") {
                        // Each queue in the node's queues array is assigned to a corresponding worker
                        if node.queues.is_some() { for (idx, &queue_id) in node.queues.as_ref().unwrap().iter().enumerate() {
                            if idx < pipeline.workers.len() {
                                let worker_id = pipeline.workers[idx];
                                let key = (interface.clone(), queue_id);

                                queue_assignments.entry(key).or_insert_with(Vec::new).push((
                                    pipeline.name.clone(),
                                    node.id.clone(),
                                    worker_id,
                                ));
                            }
                        }}
                    }
                }
            }
        }

        // Check for conflicts: same (interface, queue) assigned to multiple workers
        for ((interface, queue_id), assignments) in &queue_assignments {
            if assignments.len() > 1 {
                // Check if all assignments are to the same worker (allowed - same pipeline instance)
                let first_worker = assignments[0].2;
                let all_same_worker = assignments.iter().all(|a| a.2 == first_worker);

                if !all_same_worker {
                    // Different workers trying to use the same queue - conflict!
                    let conflict_details: Vec<String> = assignments
                        .iter()
                        .map(|(pipeline, node, worker)| format!("pipeline '{}' node '{}' worker {}", pipeline, node, worker))
                        .collect();

                    return Err(anyhow!(
                        "Queue conflict: interface '{}' queue {} is assigned to multiple workers:\n  {}",
                        interface,
                        queue_id,
                        conflict_details.join("\n  ")
                    ));
                }
            }
        }

        // 8. Validate UMEM consistency: workers must have partitions from the same UMEM as their assigned queues
        // Build map: (interface, queue) -> umem_id from XSK config
        let mut queue_umem_map: HashMap<(String, u32), usize> = HashMap::new();
        for xsk in &section.xsk {
            for &queue_id in &xsk.queues {
                queue_umem_map.insert((xsk.iface.clone(), queue_id), xsk.umem);
            }
        }

        // Check each pipeline's queue assignments against worker UMEM allocations
        for pipeline in &self.pipelines {
            for node in &pipeline.nodes {
                if node.node_type == "BatchRxNode" || node.node_type == "BatchTxNode" {
                    if let Some(interface) = node.args.get("interface") {
                        // Each queue maps to a worker (queues[i] -> workers[i])
                        if node.queues.is_some() { for (idx, &queue_id) in node.queues.as_ref().unwrap().iter().enumerate() {
                            if idx < pipeline.workers.len() {
                                let worker_id = pipeline.workers[idx];

                                // Get the UMEM assigned to this queue via XSK config
                                if let Some(&xsk_umem_id) = queue_umem_map.get(&(interface.clone(), queue_id)) {
                                    // Get the worker config to check its UMEM partition allocations
                                    if let Some(worker_cfg) = section.workers.iter().find(|w| w.id == worker_id) {
                                        // Check if worker has a partition from the same UMEM as the queue
                                        let has_matching_umem = worker_cfg.partitions.iter().any(|p| p.umem == xsk_umem_id);

                                        if !has_matching_umem {
                                            let worker_umems: Vec<usize> =
                                                worker_cfg.partitions.iter().map(|p| p.umem).collect();

                                            return Err(anyhow!(
                                                "UMEM mismatch: Pipeline '{}', node '{}' assigns queue {} (interface '{}') to worker {}.\n\
                                                 Queue {} is bound to UMEM {} (via xsk config), but worker {} only has partitions from UMEM(s): {:?}.\n\
                                                 Worker must have a partition from UMEM {} to use this queue.\n\
                                                 Fix: Either change worker {} to request a partition from UMEM {} or change the xsk config to use a different UMEM.",
                                                pipeline.name,
                                                node.id,
                                                queue_id,
                                                interface,
                                                worker_id,
                                                queue_id,
                                                xsk_umem_id,
                                                worker_id,
                                                worker_umems,
                                                xsk_umem_id,
                                                worker_id,
                                                xsk_umem_id
                                            ));
                                        }
                                    }
                                }
                            }
                        }}
                    }
                }
            }
        }

        self.validate_umem_resources()?;

        Ok(())
    }

    pub fn to_system_builder(&self) -> anyhow::Result<SystemBuilder> {
        // --- STEP 1: Build Lookups (Who owns which queue?) ---

        // Map: Worker ID -> Core ID (indexed by worker_id, dense 0..N)
        let max_worker_id = self.xbricks.workers.iter().map(|w| w.id).max().unwrap_or(0);
        let mut worker_core_map: Vec<Option<usize>> = vec![None; max_worker_id + 1];
        for w in &self.xbricks.workers {
            worker_core_map[w.id] = Some(w.core);
        }

        // Map: (Interface Name, Queue ID) -> Worker ID
        // We scan all pipelines to discover which worker is assigned to which queue.
        let mut queue_owner_map: HashMap<(String, u32), usize> = HashMap::new();

        for pipeline in &self.pipelines {
            for node in &pipeline.nodes {
                // If a node has explicit queues assigned (e.g. Rx/Tx nodes)
                if node.queues.is_some() {
                    // Validation: Ensure queues match workers 1:1
                    if node.queues.as_ref().unwrap().len() != pipeline.workers.len() {
                        return Err(anyhow!(
                            "Pipeline '{}': Node '{}' has {} queues but {} workers. Must be equal.",
                            pipeline.name,
                            node.id,
                            node.queues.as_ref().unwrap().len(),
                            pipeline.workers.len()
                        ));
                    }

                    // Try to identify the interface. Usually passed in 'args'.
                    // If 'interface' is missing, we skip mapping (assuming it's not a hardware-bound node).
                    if let Some(iface) = node.args.get("interface") {
                        if node.queues.is_some() { for (i, &queue_id) in node.queues.as_ref().unwrap().iter().enumerate() {
                            let worker_id = pipeline.workers[i];

                            // Detect conflicts: Two workers cannot own the same queue
                            if let Some(&existing_owner) = queue_owner_map.get(&(iface.clone(), queue_id)) {
                                if existing_owner != worker_id {
                                    return Err(anyhow!(
                                        "Configuration Conflict: Interface '{}' Queue {} is assigned to both Worker {} and Worker {}",
                                        iface,
                                        queue_id,
                                        existing_owner,
                                        worker_id
                                    ));
                                }
                            }
                            queue_owner_map.insert((iface.clone(), queue_id), worker_id);
                        }}
                    }
                }
            }
        }

        // --- STEP 2: Create Runtime UMEM Configs ---

        let mut runtime_umems: Vec<Option<Arc<RuntimeUmemConfig>>> = vec![None; MAX_UMEM];
        // Assuming global frame indices are sequential across UMEM groups based on definition order
        let mut current_global_frame_idx = 0;

        for umem_cfg in &self.xbricks.umem {
            let frame_size_log2 = umem_cfg.frame_size.trailing_zeros() as u8;
            let rc = Arc::new(RuntimeUmemConfig {
                group_id: umem_cfg.id,
                frame_size: umem_cfg.frame_size,
                frame_size_log2,
                frame_count: umem_cfg.frame_count,
                head_room: umem_cfg.head_room,
                fq_size: umem_cfg.fq_size,
                cq_size: umem_cfg.cq_size,
                group_start_frame_idx: current_global_frame_idx,
                partitions: umem_cfg.partitions,
                reserved_frames: umem_cfg.reserved,
            });

            runtime_umems[umem_cfg.id] = Some(rc);
            current_global_frame_idx += umem_cfg.frame_count;
        }

        // --- STEP 3: Create XdpSocketConfigs ---

        let mut xdp_socket_configs = Vec::new();

        for xsk_map in &self.xbricks.xsk {
            // Get the UMEM config for this mapping
            let umem_config = runtime_umems[xsk_map.umem]
                .as_ref()
                .ok_or_else(|| anyhow!("XSK mapping references unknown UMEM ID {}", xsk_map.umem))?;

            for &queue_id in &xsk_map.queues {
                // Find who owns this queue (Worker & Core)
                let worker_id = *queue_owner_map.get(&(xsk_map.iface.clone(), queue_id))
                    .ok_or_else(|| anyhow!(
                        "XSK Config Error: Interface '{}' Queue {} is defined but not assigned to any Worker in the pipelines.",
                        xsk_map.iface, queue_id
                    ))?;

                let core_id = worker_core_map
                    .get(worker_id)
                    .and_then(|opt| *opt)
                    .ok_or_else(|| anyhow!("Worker ID {} not found in workers list", worker_id))?;

                let id_string = format!("{} queue {} worker {}", xsk_map.iface, queue_id, worker_id);

                xdp_socket_configs.push(XdpSocketConfig {
                    id_string,
                    interface: xsk_map.iface.clone(),
                    queue_id,
                    core_id,
                    worker_id,
                    rx_queue_size: xsk_map.rxq_size,
                    tx_queue_size: xsk_map.txq_size,
                    zerocopy: xsk_map.zerocopy,
                    umem_config: umem_config.clone(),
                    xdp_mode: xsk_map.xdp_mode.clone(),
                });
            }
        }
        // --- STEP 4: Extract worker partition requests ---

        let mut worker_partitions: Vec<Option<Vec<PartitionRequest>>> = vec![None; max_worker_id + 1];
        for worker_cfg in &self.xbricks.workers {
            worker_partitions[worker_cfg.id] = Some(worker_cfg.partitions.clone());
        }

        // --- STEP 5: Build System Workers (Existing Logic) ---

        let mut system_builder = SystemBuilder {
            workers: Vec::new(),
            xdp_socket_configs,
            runtime_umems,
            #[cfg(feature = "wasm")]
            wasm_managers: vec![None; MAX_UMEM],
            worker_partitions,
        };

        // Iterate over every defined pipeline
        for pipeline_conf in &self.pipelines {
            // Validate that we have workers to process this pipeline
            if pipeline_conf.workers.is_empty() {
                return Err(anyhow!("Pipeline '{}' has no workers assigned", pipeline_conf.name));
            }

            // Iterate over the workers assigned to this pipeline
            // Note: We use 'enumerate' to get the index (i) to pick the right queue
            for (worker_idx_in_pipeline, &worker_id) in pipeline_conf.workers.iter().enumerate() {
                // 1. Resolve the Worker Configuration
                // CRITICAL: Find by ID, do not assume index matches ID, do not search by core.
                let worker_conf =
                    self.xbricks.workers.iter().find(|w| w.id == worker_id).ok_or_else(|| {
                        anyhow!("Pipeline '{}' references unknown worker {}", pipeline_conf.name, worker_id)
                    })?;

                // 2. Find OR Create the SystemWorkerBuilder
                // CRITICAL: Check if we already created a builder for this worker_id
                // (e.g. from a previous pipeline loop).
                // We cannot use 'entry' API easily here because we are in a loop iterating over pipelines.
                let worker_builder_index =
                    if let Some(idx) = system_builder.workers.iter().position(|w| w.id == worker_id as u32) {
                        idx
                    } else {
                        system_builder.workers.push(WorkerBuilderConfig {
                            id: worker_id as u32,
                            core_id: worker_conf.core as u32, // Set affinity here
                            pipelines: Vec::new(),
                        });
                        system_builder.workers.len() - 1
                    };

                // 3. Construct the Pipeline Instance for this Worker
                // This is where we handle the specific "slice" of the pipeline this worker runs.
                let mut pipeline_instance_nodes = Vec::new();

                for (node_index, node_conf) in pipeline_conf.nodes.iter().enumerate() {
                    // 4. Handle "Queues" Argument
                    // If the config defines "queues = [0, 1]", and this is the 2nd worker (index 1),
                    // we must pick '1'.
                    // If the config defines "queues = [2, 3]" and workers = [2, 2] (drop_if_0),
                    // 1st iteration (Worker 2) gets '2'.
                    // 2nd iteration (Worker 2) gets '3'.
                    // The architecture dictates a 1:1 map: queues[i] -> workers[i]
                    let mut assigned_queue = Vec::new();
                    if node_conf.queues.is_some() {
                        // Safety check: Ensure the node has enough queues for the assigned workers.
                        // (This should be caught by validate(), but good to check here).
                        if worker_idx_in_pipeline >= node_conf.queues.as_ref().unwrap().len() {
                            return Err(anyhow!(
                                "Configuration Error: Pipeline '{}', Node '{}' has fewer queues ({}) than workers ({})",
                                pipeline_conf.name,
                                node_conf.id,
                                node_conf.queues.as_ref().unwrap().len(),
                                pipeline_conf.workers.len()
                            ));
                        }

                        // Get the specific queue ID for this worker
                        assigned_queue = Vec::from([node_conf.queues.as_ref().unwrap()[worker_idx_in_pipeline]]);
                    }

                    // Create the node instance for the builder
                    pipeline_instance_nodes.push(NodeInstance {
                        node_config: NodeConfig {
                            id: node_conf.id.clone(),
                            node_type: node_conf.node_type.clone(),
                            routes: node_conf.routes.clone(),
                            queues: Some(assigned_queue),
                            labels: node_conf.labels.clone(),
                            args: node_conf.args.clone(),
                            hot_args: node_conf.hot_args.clone(),
                            wasm_data: node_conf.wasm_data.clone(),
                        },
                        state: NodeDynamicState::default(),
                        node_instance_id: NodeInstanceId::new(
                            node_index as u32,
                            worker_id as u32,
                            pipeline_conf.name.clone(),
                            node_conf.id.clone(),
                            node_conf.node_type.clone(),
                        ),
                    });
                }

                // Append this pipeline configuration to the worker's list
                system_builder.workers[worker_builder_index].pipelines.push(PipelineInstance {
                    name: pipeline_conf.name.clone(),
                    nodes: pipeline_instance_nodes,
                    poll_frequency: pipeline_conf.poll_frequency,
                });
            }
        }

        Ok(system_builder)
    }
}


mod tests {
    use std::path::PathBuf;
    use ts_rs::Config;
    use super::*;

    #[test]
    fn check_env() {
        println!("Exporting to: {:?}", std::env::var("TS_RS_EXPORT_DIR"));
    }

    #[test]
    fn export_bindings() {
        let config = Config::default().with_out_dir(PathBuf::from("../xbricks-ui/src/types/"));
        // Force the base path to your UI folder
        XBricksConfig::export_all(&config).expect("could not export type");
    }

    #[test]
    fn test_duplicate_umem_id() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 },
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate UMEM id"));
    }

    #[test]
    fn test_invalid_frame_size_not_power_of_two() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2000, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("power of 2"));
    }

    #[test]
    fn test_nonexistent_interface_reference() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "eth1", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // The error message changed to be more helpful
        assert!(err_msg.contains("not defined in the ifaces array") || err_msg.contains("non-existent interface"));
    }

    #[test]
    fn test_nonexistent_umem_reference() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 1, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-existent UMEM"));
    }

    #[test]
    fn test_invalid_rxq_size_not_power_of_two() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1000, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("rxq_size must be a power of 2"));
    }

    #[test]
    fn test_head_room_exceeds_frame_size() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 2048 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("head_room"));
    }

    #[test]
    fn test_mixed_umem_config() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 4096, txd = 4096 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 8192, fq_size = 4096, cq_size = 4096, head_room = 128 },
                { id = 1, frame_size = 2048, frame_count = 8192, fq_size = 4096, cq_size = 4096, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0, 1], umem = 0, rxq_size = 4096, txq_size = 4096, zerocopy = true },
                { iface = "lo", queues = [2, 3], umem = 1, rxq_size = 4096, txq_size = 4096, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 },
                { id = 1, core = 1 },
                { id = 2, core = 2 },
                { id = 3, core = 3 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        // Verify we have 2 UMEM domains
        assert_eq!(config.xbricks.umem.len(), 2);

        // Verify we have 2 XSK mappings
        assert_eq!(config.xbricks.xsk.len(), 2);
    }

    #[test]
    fn test_empty_queues_list() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        // XSK validation happens before worker validation, so empty queues error comes first
        assert!(result.unwrap_err().to_string().contains("queues list cannot be empty"));
    }

    #[test]
    fn test_empty_workers_list() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = []
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least one worker"));
    }

    #[test]
    fn test_duplicate_queue_assignment_different_workers() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0, 1], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 },
                { id = 1, core = 1 }
            ]

            [[pipelines]]
            name = "pipeline_a"
            workers = [0]
                [[pipelines.nodes]]
                id = "rx_a"
                type = "BatchRxNode"
                next = "drop_a"
                queues = [0]
                args = { interface = "lo" }
                [[pipelines.nodes]]
                id = "drop_a"
                type = "DropNode"

            [[pipelines]]
            name = "pipeline_b"
            workers = [1]
                [[pipelines.nodes]]
                id = "rx_b"
                type = "BatchRxNode"
                next = "drop_b"
                queues = [0]
                args = { interface = "lo" }
                [[pipelines.nodes]]
                id = "drop_b"
                type = "DropNode"
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Queue conflict"));
        assert!(err_msg.contains("lo"));
        assert!(err_msg.contains("queue 0"));
    }

    #[test]
    fn test_same_queue_same_worker_allowed() {
        // This should be allowed: same queue used by same worker in different pipelines
        // (though unusual, it's technically valid if both pipelines are scheduled on same worker)
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 65392, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0, 1], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 1 }]  }
            ]

            [[pipelines]]
            name = "pipeline_a"
            workers = [0]
                [[pipelines.nodes]]
                id = "rx_a"
                type = "BatchRxNode"
                next = "drop_a"
                queues = [0]
                args = { interface = "lo" }
                [[pipelines.nodes]]
                id = "drop_a"
                type = "DropNode"

            [[pipelines]]
            name = "pipeline_b"
            workers = [0]
                [[pipelines.nodes]]
                id = "rx_b"
                type = "BatchRxNode"
                next = "drop_b"
                queues = [0]
                args = { interface = "lo" }
                [[pipelines.nodes]]
                id = "drop_b"
                type = "DropNode"
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_example_pipeline_test_0_toml() {
        let config = XBricksConfig::from_file("../xbricks/examples/pipeline_test_0.toml");
        assert!(config.is_ok(), "pipeline_test_0.toml should be valid: {:?}", config.err());

        let config = config.unwrap();

        // Verify we have 3 pipelines
        assert_eq!(config.pipelines.len(), 3, "Should have 3 pipelines");

        // Verify pipeline names
        assert_eq!(config.pipelines[0].name, "l2_reflector");
        assert_eq!(config.pipelines[1].name, "drop_if_0");
        assert_eq!(config.pipelines[2].name, "drop_if_1");

        // Verify first pipeline has correct structure
        let l2_reflector = &config.pipelines[0];
        assert_eq!(l2_reflector.workers, vec![0, 1], "l2_reflector should have workers [0, 1]");
        assert_eq!(l2_reflector.nodes.len(), 5, "l2_reflector should have 5 nodes");

        // Verify RX node has queues at node level (not in args)
        let rx_node = &l2_reflector.nodes[0];
        assert_eq!(rx_node.id, "l2_reflector.rx_root");
        assert_eq!(rx_node.node_type, "BatchRxNode");
        assert_eq!(rx_node.queues, Some(vec![0, 1]), "RX node should have queues [0, 1]");
        assert_eq!(rx_node.args.get("interface"), Some(&"enp2s0f0np0".to_string()));

        // Verify TX node has queues at node level
        let tx_node = &l2_reflector.nodes[3];
        assert_eq!(tx_node.id, "l2_reflector.tx_sink");
        assert_eq!(tx_node.node_type, "BatchTxNode");
        assert_eq!(tx_node.queues, Some(vec![0, 1]), "TX node should have queues [0, 1]");
        assert_eq!(tx_node.args.get("interface"), Some(&"enp2s0f0np0".to_string()));

        // Verify classifier node has no queues (processing node)
        let classifier_node = &l2_reflector.nodes[1];
        assert_eq!(classifier_node.id, "l2_reflector.classifier");
        assert_eq!(classifier_node.node_type, "ClassifierNode");
        assert!(classifier_node.queues.is_none(), "Classifier should have no queues");

        // Verify drop_if_1 pipeline with 4 workers
        let drop_if_1 = &config.pipelines[2];
        assert_eq!(
            drop_if_1.workers,
            vec![2, 2, 2, 2],
            "drop_if_1 should have 4 instances on worker 2"
        );
        let rx_node = &drop_if_1.nodes[0];
        assert_eq!(rx_node.queues, Some(vec![0, 1, 2, 3]), "drop_if_1 RX should have 4 queues");
    }

    #[test]
    fn test_example_simple_pipeline_toml() {
        let config = XBricksConfig::from_file("../xbricks/examples/simple_pipeline.toml");
        assert!(config.is_ok(), "simple_pipeline.toml should be valid: {:?}", config.err());

        let config = config.unwrap();

        // Verify we have 2 pipelines (wire f0 to f1, wire f1 to f0)
        assert_eq!(config.pipelines.len(), 2, "Should have 2 pipelines");

        // Verify both pipelines have workers [0, 1]
        assert_eq!(config.pipelines[0].workers, vec![0, 1]);
        assert_eq!(config.pipelines[1].workers, vec![0, 1]);

        // Verify first pipeline nodes have queues at node level
        let pipeline1 = &config.pipelines[0];
        assert_eq!(pipeline1.nodes[0].queues, Some(vec![0, 1]));
        assert_eq!(pipeline1.nodes[2].queues, Some(vec![0, 1]));

        // Verify telemetry node configuration with HashMap args
        let telemetry_node = &pipeline1.nodes[1];
        assert_eq!(telemetry_node.node_type, "TelemetryNode");
        assert_eq!(telemetry_node.args.get("algo"), Some(&"ethertype".to_string()));
        assert_eq!(telemetry_node.args.get("buckets"), Some(&"4".to_string()));
        assert_eq!(telemetry_node.args.get("labels"), Some(&"IPv4,IPv6,ARP,Other".to_string()));
    }

    #[test]
    fn test_partition_requests_valid() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 4 },
                { id = 1, frame_size = 4096, frame_count = 8192, fq_size = 2048, cq_size = 2048, head_room = 256, partitions = 2 }
            ]
            xsk = [
                { iface = "lo", queues = [0, 1], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 2 }, { umem = 1, count = 1 } ] },
                { id = 1, core = 1, partitions = [ { umem = 0, count = 2 }, { umem = 1, count = 1 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());

        // Verify partition requests were parsed correctly
        assert_eq!(config.xbricks.workers[0].partitions.len(), 2);
        assert_eq!(config.xbricks.workers[0].partitions[0].umem, 0);
        assert_eq!(config.xbricks.workers[0].partitions[0].count, 2);
        assert_eq!(config.xbricks.workers[0].partitions[1].umem, 1);
        assert_eq!(config.xbricks.workers[0].partitions[1].count, 1);
    }

    #[test]
    fn test_partition_requests_exceed_available() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 4 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 5 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requests 5 partitions"));
    }

    #[test]
    fn test_partition_requests_total_exceeds_available() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 4 }
            ]
            xsk = [
                { iface = "lo", queues = [0, 1], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 3 } ] },
                { id = 1, core = 1, partitions = [ { umem = 0, count = 2 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("total requested partitions (5) exceeds available partitions (4)")
        );
    }

    #[test]
    fn test_partition_requests_nonexistent_umem() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 4 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 1, count = 2 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("references non-existent UMEM"));
    }

    #[test]
    fn test_partition_requests_zero_count() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 4 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 0 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("has count = 0"));
    }

    #[test]
    fn test_example_mixed_umem_workers_toml() {
        let config = XBricksConfig::from_file("../xbricks/examples/mixed_umem_workers.toml");
        assert!(config.is_ok(), "mixed_umem_workers.toml should be valid: {:?}", config.err());

        let config = config.unwrap();

        // Verify UMEM partitions were parsed
        assert_eq!(config.xbricks.umem[0].partitions, 4);
        assert_eq!(config.xbricks.umem[1].partitions, 4);

        // Verify worker partition requests were parsed
        assert_eq!(config.xbricks.workers[0].partitions.len(), 2);
        assert_eq!(config.xbricks.workers[0].partitions[0].umem, 0);
        assert_eq!(config.xbricks.workers[0].partitions[0].count, 1);
        assert_eq!(config.xbricks.workers[0].partitions[1].umem, 1);
        assert_eq!(config.xbricks.workers[0].partitions[1].count, 1);

        assert_eq!(config.xbricks.workers[1].partitions.len(), 2);
        assert_eq!(config.xbricks.workers[1].partitions[0].umem, 0);
        assert_eq!(config.xbricks.workers[1].partitions[0].count, 1);
        assert_eq!(config.xbricks.workers[1].partitions[1].umem, 1);
        assert_eq!(config.xbricks.workers[1].partitions[1].count, 2);
    }

    #[test]
    fn test_umem_partitions_not_power_of_two() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 3 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("partitions must be a power of 2"));
    }

    #[test]
    fn test_umem_partitions_zero() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 0 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0 }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("partitions must be at least 1"));
    }

    #[test]
    fn test_umem_partitions_power_of_two_valid() {
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 16384, fq_size = 4096, cq_size = 4096, head_room = 128, partitions = 8 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [
                { id = 0, core = 0, partitions = [ { umem = 0, count = 4 } ] }
            ]
        "#;

        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok());
    }

    /// Helper that builds a minimal TOML config string containing a pipeline whose
    /// PacketGenerator feeds a WasmInspectorNode.  The `gen_umem` / `wasm_umem`
    /// parameters control the `umem` arg on each node so we can provoke mismatches.
    fn wasm_umem_pipeline_toml(gen_umem: usize, wasm_umem: usize) -> String {
        format!(r#"
            [xbricks]
            ifaces = [
                {{ name = "lo", rxd = 2048, txd = 2048 }}
            ]
            umem = [
                {{ id = 0, frame_size = 2048, frame_count = 65392, fq_size = 1024, cq_size = 1024, head_room = 128 }},
                {{ id = 1, frame_size = 2048, frame_count = 65392, fq_size = 1024, cq_size = 1024, head_room = 128 }}
            ]
            xsk = [
                {{ iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }}
            ]
            workers = [{{ id = 0, core = 0 }}]

            [[pipelines]]
            name = "test_pipeline"
            workers = [0]
                [[pipelines.nodes]]
                id = "gen"
                type = "PacketGenerator"
                next = "wasm"
                args = {{ umem = "{gen_umem}", pps = 100 }}

                [[pipelines.nodes]]
                id = "wasm"
                type = "WasmInspectorNode"
                routes = ["drop1", "drop2"]
                args = {{ umem = "{wasm_umem}", wasm_file_path = "dummy.wasm" }}

                [[pipelines.nodes]]
                id = "drop1"
                type = "DropNode"

                [[pipelines.nodes]]
                id = "drop2"
                type = "DropNode"
        "#)
    }


    #[test]
    fn test_wasm_inspector_upstream_umem_match_is_accepted() {
        let toml_str = wasm_umem_pipeline_toml(0, 0);
        let config: XBricksConfig = toml::from_str(&toml_str).unwrap();
        assert!(config.validate().is_ok(), "identical umem should be valid");
    }

    #[test]
    fn test_wasm_inspector_upstream_batchrx_umem_match_is_accepted() {
        // XSK binds "lo" to umem 0; WasmInspectorNode also uses umem 0 → valid.
        let toml_str = r#"
            [xbricks]
            ifaces = [
                { name = "lo", rxd = 2048, txd = 2048 }
            ]
            umem = [
                { id = 0, frame_size = 2048, frame_count = 4096, fq_size = 1024, cq_size = 1024, head_room = 128 }
            ]
            xsk = [
                { iface = "lo", queues = [0], umem = 0, rxq_size = 1024, txq_size = 1024, zerocopy = true }
            ]
            workers = [{ id = 0, core = 0, partitions = [{ umem = 0, count = 1 }] }]

            [[pipelines]]
            name = "test_pipeline"
            workers = [0]
                [[pipelines.nodes]]
                id = "rx"
                type = "BatchRxNode"
                next = "wasm"
                queues = [0]
                args = { interface = "lo" }

                [[pipelines.nodes]]
                id = "wasm"
                type = "WasmInspectorNode"
                routes = ["drop1", "drop2"]
                args = { umem = "0", wasm_file_path = "dummy.wasm" }

                [[pipelines.nodes]]
                id = "drop1"
                type = "DropNode"

                [[pipelines.nodes]]
                id = "drop2"
                type = "DropNode"
        "#;
        let config: XBricksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.validate().is_ok(), "matching umem via XSK lookup should be valid");
    }

    #[test]
    fn test_wasm_inspector_upstream_umem_mismatch() {
        // We create a pipeline where:
        // BatchRxNode (UMEM 0) -> FastParserNode -> WasmInspectorNode (UMEM 1)
        // The validation must traverse FastParserNode and catch the 0 -> 1 mismatch.
        let toml_str = r#"
        [xbricks]
        ifaces = [ { name = "lo" } ]

        [[xbricks.umem]]
        id = 0
        frame_size = 2048
        frame_count = 65392
        fq_size = 512
        cq_size = 512
        head_room = 128
        partitions = 1

        [[xbricks.umem]]
        id = 1
        frame_size = 2048
        frame_count = 65392
        fq_size = 512
        cq_size = 512
        head_room = 128
        partitions = 1

        [[xbricks.xsk]]
        iface = "lo"
        queues = [0]
        umem = 0
        rxq_size = 512
        txq_size = 512
        zerocopy = true

        [[xbricks.workers]]
        id = 0
        core = 0
        partitions = [{ umem = 0, count = 1 }, { umem = 1, count = 1 }]

        [[pipelines]]
        name = "test_pipe"
        workers = [0]

        [[pipelines.nodes]]
        id = "rx"
        type = "BatchRxNode"
        routes = ["parser"]
        queues = [0]
        [pipelines.nodes.args]
        interface = "lo"

        [[pipelines.nodes]]
        id = "parser"
        type = "FastParserNode"
        routes = ["wasm"]
        [pipelines.nodes.args]

        [[pipelines.nodes]]
        id = "wasm"
        type = "WasmInspectorNode"
        [pipelines.nodes.args]
        umem = "1"
        "#;

        let result = XBricksConfig::from_toml_str(toml_str);

        // Assert that the configuration failed validation
        assert!(result.is_err(), "Configuration should have failed due to UMEM mismatch");

        let err_msg = result.unwrap_err().to_string();

        // Assert it threw the correct specific error
        assert!(
            err_msg.contains("WasmInspectorNode 'wasm' is bound to umem 1, but can receive PDUs from umem 0"),
            "Error message did not match expected output. Got: {}", err_msg
        );
    }
}
