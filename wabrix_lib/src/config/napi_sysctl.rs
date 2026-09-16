//! Temporary system settings used by kernel/NAPI busy polling.

use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::toml_parser::{InterfaceConfig, XskMapping};

const GRO_FLUSH_TIMEOUT: &str = "gro_flush_timeout";
const NAPI_DEFER_HARD_IRQS: &str = "napi_defer_hard_irqs";

struct OriginalValue {
    path: PathBuf,
    value: Vec<u8>,
}

/// Restores interface sysfs values when the guard leaves scope.
pub struct NapiSysctlGuard {
    originals: Vec<OriginalValue>,
}

impl NapiSysctlGuard {
    /// Snapshot and apply the configured values for interfaces using NAPI polling.
    ///
    /// An interface is configured only once, even if it has multiple XSK queues.
    /// Conflicting values on the same interface are rejected before any write.
    pub fn apply(ifaces: &[InterfaceConfig], mappings: &[XskMapping]) -> Result<Option<Self>> {
        let iface_napi = ifaces
            .iter()
            .map(|iface| (iface.name.as_str(), iface.napi.as_ref()))
            .collect::<BTreeMap<_, _>>();
        let mut requested = BTreeMap::<&str, (u64, u32)>::new();
        for mapping in mappings
            .iter()
            .filter(|mapping| mapping.poll_mode == "napi_busy_poll")
        {
            let values = iface_napi
                .get(mapping.iface.as_str())
                .and_then(|napi| *napi)
                .map(|napi| (napi.flush_timeout_ns, napi.defer_irqs))
                .unwrap_or((0, 0));
            if let Some(previous) = requested.insert(mapping.iface.as_str(), values) {
                if previous != values {
                    return Err(anyhow!(
                        "conflicting NAPI sysctl values configured for interface {}",
                        mapping.iface
                    ));
                }
            }
        }

        if requested.is_empty() {
            return Ok(None);
        }

        let mut originals = Vec::with_capacity(requested.len() * 2);
        let mut updates = Vec::with_capacity(requested.len() * 2);
        for (iface, (flush_timeout, defer_irqs)) in requested {
            let settings = [
                (GRO_FLUSH_TIMEOUT, flush_timeout.to_string()),
                (NAPI_DEFER_HARD_IRQS, defer_irqs.to_string()),
            ];

            for (parameter, value) in settings {
                let path = sysfs_path(iface, parameter);
                let original = fs::read(&path).with_context(|| {
                    format!("failed to read original value from {}", path.display())
                })?;
                originals.push(OriginalValue {
                    path: path.clone(),
                    value: original,
                });
                updates.push((path, value, iface.to_string(), parameter));
            }
        }

        let guard = Self { originals };
        for (path, value, iface, parameter) in updates {
            if let Err(error) = fs::write(&path, format!("{value}\n")) {
                return Err(error).with_context(|| format!("failed to set {}", path.display()));
            }
            info!("Set {} to {} for {}", parameter, value, iface);
        }

        Ok(Some(guard))
    }
}

impl Drop for NapiSysctlGuard {
    fn drop(&mut self) {
        for original in self.originals.iter().rev() {
            if let Err(error) = fs::write(&original.path, &original.value) {
                warn!("failed to restore {}: {}", original.path.display(), error);
            } else {
                info!("Restored {}", original.path.display());
            }
        }
    }
}

fn sysfs_path(iface: &str, parameter: &str) -> PathBuf {
    Path::new("/sys/class/net").join(iface).join(parameter)
}
