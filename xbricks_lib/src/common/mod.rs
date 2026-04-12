pub mod errors;
pub mod formatting;

// Re-export commonly used system types and functions
pub use xbricks_base::system::{get_nic_rx_packets_detailed, get_nic_tx_packets_detailed, PacketStats};

/// Null metadata associated with packets initially.
pub struct EmptyMetadata;
