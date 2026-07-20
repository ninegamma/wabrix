// src/config/configure_bpf.rs
use anyhow::Result;
use libbpf_rs::skel::{OpenSkel, SkelBuilder};
use libbpf_rs::Link;
use libbpf_rs::MapCore;
use log::{debug, error, info, trace};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;

// Import the BPF skeleton type from the crate root
use crate::xscrubber_skel::{XscrubberSkel, XscrubberSkelBuilder};

/// Set up BPF programs for the specified interfaces
/// Returns a map of interface names to their BPF skeletons and the XDP links
#[allow(clippy::type_complexity)]
pub fn setup_bpf(
    unique_ifaces: Vec<String>,
) -> Result<(HashMap<String, Arc<XscrubberSkel<'static>>>, Vec<Link>)> {
    // Instead of one global skeleton, we store one per interface.
    // Key: Interface Name -> Value: Arc<Skeleton>
    let mut interface_skels: HashMap<String, Arc<XscrubberSkel<'static>>> = HashMap::new();

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
        let builder = XscrubberSkelBuilder::default();
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

        let prog = &skel.progs.xdp_dispatch;
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

/// Explicitly remove all AF_XDP socket entries from the BPF XSKS_MAP for every interface.
///
/// **Why this is required for clean restart:**
/// When `xsk_socket__delete()` is called with `XSK_LIBXDP_FLAGS__INHIBIT_PROG_LOAD`, libxdp
/// skips the `xsk_delete_map_entry()` call because `ctx->xdp_prog == NULL`. The XSKS_MAP
/// therefore still holds the last kernel reference to the socket object after the userspace fd
/// is closed. Relying on `bpf_object__close()` to destroy the map is insufficient because the
/// resulting `bpf_map_put()` schedules destruction via a kernel work-queue (async/RCU-deferred).
/// There is no timing guarantee that the hardware queue is unbound before the next socket
/// creation attempt.
///
/// Calling `bpf_map_delete_elem` (via `MapCore::delete`) on each XSKS_MAP entry triggers a
/// **synchronous** `sock_put()` in the kernel. If the socket fd was already closed by the
/// workers, the refcount drops to 0 immediately, `xsk_destruct()` → `xsk_unbind_dev()` runs
/// inline, and the hardware queue is released before this function returns.
pub fn unregister_sockets_from_bpf(interface_skels: &HashMap<String, Arc<XscrubberSkel<'static>>>) {
    debug!("Clearing BPF XSKS_MAP entries to release hardware queue bindings...");
    for (iface_name, skel) in interface_skels {
        let sockets_map = &skel.maps.SOCKETS;
        // The SOCKETS map has max_entries=64; iterate all possible queue IDs.
        // Deletion of a non-existent entry returns an error which we silently ignore.
        for queue_id in 0u32..64u32 {
            let key = queue_id.to_ne_bytes();
            if sockets_map.delete(&key).is_ok() {
                trace!("✓ Released XSKS_MAP[{}] for {}", queue_id, iface_name);
            }
        }
    }
    debug!("XSKS_MAP cleared — hardware queues are now unbound.");
}

/// Register all XDP sockets in their corresponding BPF maps
pub fn register_sockets_in_bpf(
    xsockets: &[crate::XSocket],
    interface_skels: &HashMap<String, Arc<XscrubberSkel<'static>>>,
) -> Result<()> {
    info!("Registering {} sockets in BPF maps...", xsockets.len());

    for xsocket in xsockets {
        let iface_name = xsocket.interface();
        let qid = xsocket.queue_id();
        let sock_fd = xsocket.socket_fd();

        // Get the specific BPF skeleton for this interface
        let skel = interface_skels
            .get(iface_name)
            .expect("Critical Error: No BPF skeleton found for interface defined in config");

        // Access the local XSKS_MAP for this interface
        let sockets_map = &skel.maps.SOCKETS;

        // Update the map: Key = Queue ID, Value = Socket FD
        let key = qid.to_ne_bytes();
        let value = (sock_fd as u32).to_ne_bytes();

        sockets_map
            .update(&key, &value, libbpf_rs::MapFlags::ANY)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to register socket for {}:Queue {} in BPF map: {}",
                    iface_name,
                    qid,
                    e
                )
            })?;

        debug!(
            "✓ Registered {}:Queue {} (FD={}) in BPF map",
            iface_name, qid, sock_fd
        );
    }

    info!("✓ All sockets registered successfully.");
    Ok(())
}
