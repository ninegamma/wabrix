// src/config/configure_bpf.rs
use anyhow::Result;
use libbpf_rs::skel::{OpenSkel, SkelBuilder};
use libbpf_rs::Link;
use log::{debug, error, info};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;

// Include the generated BPF skeleton code
pub mod ipblower_skel {
    include!(concat!(env!("OUT_DIR"), "/ipblower.skel.rs"));
}

// Import the BPF skeleton type from the crate root
use ipblower_skel::{IpblowerSkel, IpblowerSkelBuilder};

/// Set up BPF programs for the specified interfaces
/// Returns a map of interface names to their BPF skeletons and the XDP links
#[allow(clippy::type_complexity)]
pub fn setup_bpf(
    unique_ifaces: Vec<String>,
) -> Result<(HashMap<String, Arc<IpblowerSkel<'static>>>, Vec<Link>)> {
    // Instead of one global skeleton, we store one per interface.
    // Key: Interface Name -> Value: Arc<Skeleton>
    let mut interface_skels: HashMap<String, Arc<IpblowerSkel<'static>>> = HashMap::new();

    // We need to keep the XDP links alive so the program doesn't detach
    let mut xdp_links = Vec::new();

    let rlim = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    let ret = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlim) };
    if ret != 0 {
        debug!("remove limit on locked memory failed, ret is: {ret}");
    }

    // Iterate over UNIQUE interfaces from the config
    for iface_name in unique_ifaces {
        info!(
            "Loading dedicated BPF skeleton for interface: {}",
            iface_name
        );

        // Load a FRESH skeleton instance for this interface
        let builder = IpblowerSkelBuilder::default();
        let open_object = Box::leak(Box::new(MaybeUninit::uninit()));
        let open_skel = builder.open(open_object)?;
        let skel = open_skel.load()?;

        // Attach to THIS specific interface
        let ifindex = {
            let c_name = std::ffi::CString::new(iface_name.as_str())?;
            let idx = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
            if idx == 0 {
                return Err(anyhow::anyhow!(
                    "Failed to get interface index for {}",
                    iface_name
                ));
            }
            idx as u32
        };

        let prog = &skel.progs.xdp_dummy_prog;
        let link_xdp = prog.attach_xdp(ifindex as i32).map_err(|e| {
            error!("Failed to attach XDP program to {}: {}", iface_name, e);
            e
        })?;

        info!(
            "✓ XDP program attached to {} (Index {})",
            iface_name, ifindex
        );

        // Store the Link and the Skeleton
        xdp_links.push(link_xdp);
        interface_skels.insert(iface_name, Arc::new(skel));
    }

    Ok((interface_skels, xdp_links))
}
