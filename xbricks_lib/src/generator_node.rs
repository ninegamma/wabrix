use std::arch::x86_64::_rdtsc;
use std::mem::MaybeUninit;
use std::net::Ipv4Addr;
use std::sync::Arc;
use log::{info, warn};
use macaddr::MacAddr6;
use network_types::eth::EtherType;
use rand::prelude::SmallRng;
use rand::TryRng;
use xbricks_base::interface::Pdu;
use xbricks_base::{Directive, Node, NodeId};
use xbricks_base::frame_registry::get_worker_frame_registry;
use xbricks_base::interface::batch::Batch;
use xbricks_protocol::headers::{EndOffset, Header, IpHeader, MacHeader, TcpHeader};
use crate::config::hot_config::SharedSystemState;
// WASM Imports
#[cfg(feature = "wasm")]
use wasmtime::Store;
#[cfg(feature = "wasm")]
use wasmtime::component::{bindgen, ResourceTable};
#[cfg(feature = "wasm")]
use wasmtime_wasi::WasiCtxBuilder;
#[cfg(feature = "wasm")]
use xbricks_wasm::wasm_manager::{MyHostState, WasmManager};

bindgen!({
    path: "../wit/xbricks.wit",
    world: "generator-plugin", // Tells bindgen to generate the generator code
});

/// A packet generator node that creates packets 
/// This is a source node that generates synthetic traffic.
pub struct GeneratorNode {
    id: NodeId,
    next_node: NodeId,
    /// The UMEM group ID to allocate frames from
    umem_group_id: usize,
    /// Worker ID for this generator instance
    worker_id: usize,
    /// Number of packets to generate per batch
    batch_size: usize,
    /// minimum interbatch gap in tsc cycles
    min_interbatch_gap: u64,
    lastbatch_timestamp: u64,
    start_delay: u64,
    start_time: u64,
    packets_to_generate: usize,
    /// Total packets generated per worker
    total_generated: usize,
    /// Flag indicating if generation is complete
    finished: bool,
    /// Timestamp of first packet burst (TSC cycles)
    first_burst_timestamp: u64,
    /// Timestamp of last packet burst (TSC cycles)
    last_burst_timestamp: u64,
    /// Instant of first packet burst (std::time)
    first_burst_instant: Option<std::time::Instant>,
    /// Instant of last packet burst (std::time)
    last_burst_instant: Option<std::time::Instant>,
    rng: SmallRng,
    /// Source MAC address (from interface)
    src_mac: MacAddr6,
    /// Destination IP address
    dst_ip: Ipv4Addr,
    /// Destination MAC address
    dst_mac: MacAddr6,
    next_log_threshold: usize,
    scratch_pdus: [core::mem::MaybeUninit<Pdu>; crate::MAX_BATCH_SIZE],
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    // new fields for GUI controlled generator
    state: GenState,
    packets_generated_this_run: usize,
    shared_state: Arc<SharedSystemState>,
    pipeline_name: String,

    // === WASM FIELDS ===
    #[cfg(feature = "wasm")]
    store: Option<Store<MyHostState>>,
    #[cfg(feature = "wasm")]
    bindings: Option<GeneratorPlugin>,
    #[cfg(feature = "wasm")]
    partition_start_bytes: u32,
}

#[derive(PartialEq, Debug)]
enum GenState {
    Idle,
    Generating,
    Finished,
}


impl GeneratorNode {
    pub fn new(
        id: NodeId,
        next_node: NodeId,
        umem_group_id: usize,
        worker_id: usize,
        batch_size: usize,
        min_interbatch_gap: u64,    // minimum interbatch_gap in tsc cycles
        packets_to_generate: usize, // 0: no limit
        src_mac: MacAddr6,
        dst_ip: Ipv4Addr,
        dst_mac: MacAddr6,
        shared_state: Arc<SharedSystemState>,
        pipeline_name: String,
        autostart: bool,
        #[cfg(feature = "wasm")] wasm_manager: Option<&WasmManager>,
        #[cfg(feature = "wasm")] wasm_file_path: Option<&str>,
        #[cfg(feature = "wasm")] partition_start_bytes: u32,
        #[cfg(feature = "wasm")] wasm_data: std::collections::HashMap<String, String>,
    ) -> Self {

        #[cfg(feature = "wasm")]
        let (store, bindings) = if let (Some(manager), Some(path)) = (wasm_manager, wasm_file_path) {
            let component = manager.get_component(path).expect("Failed to load Gen Wasm");
            let partition_handle = manager.make_partition_handle(partition_start_bytes);
            manager.set_instantiation_handle(partition_handle.clone());

            let wasi_ctx = WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build();
            let mut store = Store::new(
                manager.engine(),
                MyHostState {
                    ctx: wasi_ctx,
                    table: ResourceTable::new(),
                    umem: partition_handle,
                },
            );

            let bindings = GeneratorPlugin::instantiate(&mut store, &component, manager.linker())
                .expect("Failed to instantiate Generator Wasm");

            // Initialize the plugin state with the worker ID!
            bindings.xbricks_plugins_generator()
                .call_init(&mut store, worker_id as u32)
                .expect("Failed to initialize WASM plugin state");

            if !wasm_data.is_empty() {
                let data: Vec<(String, String)> = wasm_data.into_iter().collect();
                bindings.xbricks_plugins_generator()
                    .call_init_data(&mut store, &data)
                    .expect("Failed to call init-data on WASM generator");
            }

            (Some(store), Some(bindings))
        } else {
            (None, None)
        };

        Self {
            id,
            next_node,
            umem_group_id,
            worker_id,
            min_interbatch_gap,
            lastbatch_timestamp: 0,
            start_delay: 0,
            start_time: 0,
            batch_size,
            packets_to_generate,
            total_generated: 0,
            finished: false,
            first_burst_timestamp: 0,
            last_burst_timestamp: 0,
            first_burst_instant: None,
            last_burst_instant: None,
            rng: rand::make_rng(),
            src_mac,
            dst_mac,
            dst_ip,
            next_log_threshold: 0,
            scratch_pdus: unsafe { MaybeUninit::uninit().assume_init() },
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            state: if autostart { GenState::Generating } else { GenState::Idle },
            packets_generated_this_run: 0,
            shared_state,
            pipeline_name,
            #[cfg(feature = "wasm")]
            store,
            #[cfg(feature = "wasm")]
            bindings,
            #[cfg(feature = "wasm")]
            partition_start_bytes,
        }
    }

    /// Initializes PDU by writing headers directly into the frame at the current data pointer,
    /// without consuming headroom. This leaves headroom available for future header additions.
    fn initialize_pdu(&mut self, pdu: &mut Pdu, seqn: u32, dst_port: u16) -> anyhow::Result<()> {
        // Create headers
        let mut mac = MacHeader::new();
        mac.src = self.src_mac;
        mac.dst = self.dst_mac;
        mac.set_ethertype(EtherType::Ipv4);

        let mut ip = IpHeader::new();
        ip.set_src(self.rng.try_next_u32()?);
        ip.set_dst_address(self.dst_ip);
        ip.set_ttl(128);
        ip.set_version(4);
        ip.set_protocol(6); //tcp
        ip.set_ihl(5);
        ip.set_length(40);
        ip.set_flags(0x2); // DF=1, MF=0 flag: don't fragment

        let mut tcp = TcpHeader::new();
        tcp.set_syn_flag();
        tcp.set_src_port(111);
        tcp.set_dst_port(dst_port);
        tcp.set_data_offset(5);
        tcp.set_seq_num(seqn);

        // Calculate total packet size
        let mac_size = MacHeader::size();
        let ip_size = IpHeader::size();
        let tcp_size = TcpHeader::size();
        let total_size = mac_size + ip_size + tcp_size;

        let umem_addr = pdu.umem_addr();
        // Write headers directly into the frame at current data pointer
        let data_ptr = pdu.data_ptr(umem_addr);

        unsafe {
            // Write MAC header
            let mac_ptr = data_ptr as *mut MacHeader;
            std::ptr::copy_nonoverlapping(&mac as *const MacHeader, mac_ptr, 1);

            // Write IP header
            let ip_ptr = data_ptr.add(mac_size) as *mut IpHeader;
            std::ptr::copy_nonoverlapping(&ip as *const IpHeader, ip_ptr, 1);

            // Write TCP header
            let tcp_ptr = data_ptr.add(mac_size + ip_size) as *mut TcpHeader;
            std::ptr::copy_nonoverlapping(&tcp as *const TcpHeader, tcp_ptr, 1);


            // Initialize HeaderStack with the correct offsets
            pdu.headers_mut().push(umem_addr, Header::Mac(&mut *mac_ptr));
            pdu.headers_mut().push(umem_addr, Header::Ip(&mut *ip_ptr));
            pdu.headers_mut().push(umem_addr, Header::Tcp(&mut *tcp_ptr));
        }

        // Update PDU data length
        pdu.w.set_data_length(total_size as u32);
        // log::trace!("Initialized PDU with seqn {}, dst_port {}: {}", seqn, dst_port, pdu);
        Ok(())
    }

    pub fn get_stats(&self) -> usize {
        self.total_generated
    }

    /// Check if the generator has finished generating all required packets
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Get the duration of actual packet burst generation in TSC cycles
    pub fn get_burst_duration_cycles(&self) -> u64 {
        if self.last_burst_timestamp > self.first_burst_timestamp {
            self.last_burst_timestamp - self.first_burst_timestamp
        } else {
            0
        }
    }

    /// Get the duration of actual packet burst generation using std::time
    pub fn get_burst_duration(&self) -> Option<std::time::Duration> {
        if let (Some(first), Some(last)) = (self.first_burst_instant, self.last_burst_instant) {
            Some(last.duration_since(first))
        } else {
            None
        }
    }
}

impl Node for GeneratorNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // =========================================================
        // 1. DYNAMIC HOT-CONFIG (GUI CONTROL)
        // =========================================================
        if let Some(rcu) = self.shared_state.pipelines.get(&self.pipeline_name) {
            let hot_config = rcu.load();
            if let Some(idx) = hot_config.name_to_index.get(&self.id.id) {
                let node_data = &hot_config.fast_nodes[*idx];

                // Process start/stop commands from the frontend
                if let Some(cmd) = node_data.args.get("cmd") {
                    // EDGE-TRIGGER: Only start if we are explicitly Idle!
                    if cmd == "start" && self.state == GenState::Idle {
                        self.state = GenState::Generating;
                        self.packets_generated_this_run = 0;
                        self.finished = false;

                        // Reset burst timers for accurate PPS on subsequent runs
                        self.first_burst_timestamp = 0;
                        self.first_burst_instant = None;
                        self.last_burst_timestamp = 0;
                        self.last_burst_instant = None;
                        self.lastbatch_timestamp = 0;

                        log::info!("PacketGenNode {}: GUI triggered START!", self.id);
                    }
                    // RESET LATCH: Any time we receive 'stop', reset back to Idle
                    else if cmd == "stop" && self.state != GenState::Idle {
                        self.state = GenState::Idle;
                        log::info!("PacketGenNode {}: GUI triggered STOP! Reset to Idle.", self.id);
                    }
                }

                // Allow dynamic adjustment of the packet limit mid-run
                if let Some(new_limit) = node_data.args.get("packets_to_generate") {
                    if let Ok(limit) = new_limit.parse::<usize>() {
                        self.packets_to_generate = limit;
                    }
                }
            }
        }

        // =========================================================
        // 2. STATE MACHINE CHECKS
        // =========================================================

        // If we are Idle OR Finished waiting for a reset, pass the empty batch forward!
        if self.state == GenState::Idle || self.state == GenState::Finished {
            batch.clear();
            return Directive::Next(self.next_node.clone());
        }

        // Failsafe for CLI mode
        if self.finished {
            batch.clear();
            return Directive::Next(self.next_node.clone());
        }

        let now = unsafe { _rdtsc() };
        if self.start_time == 0 {
            self.start_time = now;
        }

        // Check if we've reached the packet limit FOR THIS SPECIFIC RUN
        if self.packets_to_generate > 0 && self.packets_generated_this_run >= self.packets_to_generate {
            // LATCH TRIGGERED: Lock into the Finished state!
            self.state = GenState::Finished;
            self.finished = true;
            self.last_burst_instant = Some(std::time::Instant::now());

            info!(
                "PacketGenerator {}: Finished generating {} packets this run. Waiting for STOP signal.",
                self.id, self.packets_generated_this_run
            );
            batch.clear();
            return Directive::Next(self.next_node.clone());
        }

        // Force the very first batch of a run to fire instantly
        let is_first_batch = self.packets_generated_this_run == 0;

        if !is_first_batch {
            // Only enforce interbatch timing AFTER the first batch has fired
            if !((now - self.lastbatch_timestamp) >= self.min_interbatch_gap && (now - self.start_time) >= self.start_delay) {
                batch.clear();
                return Directive::Next(self.next_node.clone());
            }
        }

        // =========================================================
        // 3. PACKET GENERATION
        // =========================================================

        batch.clear();

        let registry_ptr = get_worker_frame_registry();
        if registry_ptr.is_null() {
            warn!("PacketGenNode {}: Frame registry not available", self.id);
            return Directive::Stop;
        }
        let frame_registry = unsafe { &mut *registry_ptr };

        // Calculate how many packets we can still generate THIS RUN to not exceed the limit
        let remaining = if self.packets_to_generate > 0 {
            self.packets_to_generate.saturating_sub(self.packets_generated_this_run)
        } else {
            usize::MAX
        };
        let frames_to_generate = std::cmp::min(std::cmp::min(self.batch_size, remaining), crate::MAX_BATCH_SIZE);

        if frames_to_generate == 0 {
            self.state = GenState::Idle;
            self.finished = true;
            batch.clear();
            return Directive::Next(self.next_node.clone());
        }

        let allocated = frame_registry.allocate_pdus_into(self.umem_group_id, &mut self.scratch_pdus[..frames_to_generate]);

        if allocated == 0 {
            // We are temporarily out of frames (RX socket is hoarding, or TX ring is full).
            // Pass an empty batch forward to force BatchTxNode to poll the Completion Queue!
            batch.clear();
            return Directive::Next(self.next_node.clone());
        }

        let mut generated = 0;
        let start_seq_num = self.total_generated as u32;
        let dst_port = self.worker_id as u16;

        #[cfg(feature = "wasm")]
        let wasm_lengths = if let (Some(store), Some(bindings)) = (&mut self.store, &self.bindings) {
            let active_bytes = allocated * size_of::<Pdu>();
            let batch_bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(self.scratch_pdus.as_ptr() as *const u8, active_bytes)
            };

            // Just pass the bytes and the offset. The plugin handles the rest!
            let lengths = bindings
                .xbricks_plugins_generator()
                .call_generate_batch(store, batch_bytes, self.partition_start_bytes)
                .unwrap_or_else(|e| {
                    log::error!("WASM Generator failed: {}", e);
                    vec![0; allocated]
                });

            if lengths.len() != allocated {
                log::error!("WASM returned {} lengths for {} pdus!", lengths.len(), allocated);
                vec![0; allocated]
            } else {
                lengths
            }
        } else {
            vec![]
        };

        for i in 0..allocated {
            let mut pdu = unsafe { self.scratch_pdus[i].assume_init_read() };

            #[cfg(feature = "wasm")]
            if self.bindings.is_some() {
                let pkt_len = wasm_lengths[i];
                if pkt_len > 0 {
                    pdu.w.set_data_length(pkt_len);
                    if batch.push(pdu).is_ok() {
                        generated += 1;
                    } else { break; }
                }
                continue;
            }

            // Native Fallback (if the continue above is not hit)
            let seq_num = start_seq_num + generated as u32;
            if let Err(e) = self.initialize_pdu(&mut pdu, seq_num, dst_port) {
                log::warn!("Failed to create headers natively: {:?}", e);
                continue;
            }

            if batch.push(pdu).is_ok() {
                generated += 1;
            } else {
                break;
            }

        }

        self.lastbatch_timestamp = now;

        if generated > 0 {
            if self.first_burst_timestamp == 0 {
                self.first_burst_timestamp = now;
                self.first_burst_instant = Some(std::time::Instant::now());
            }
            self.last_burst_timestamp = now;

            // Increment both counters!
            self.total_generated += generated as usize;
            self.packets_generated_this_run += generated as usize;

            const LOG_INTERVAL: usize = 1_000_000;
            if self.total_generated >= self.next_log_threshold {
                log::trace!("PacketGenNode {}: Generated {} packets", self.id, self.total_generated);
                self.next_log_threshold += LOG_INTERVAL;
            }

            #[cfg(feature = "profiling")]
            {
                let end_cycles = unsafe { _rdtsc() };
                self.total_cycles += end_cycles - start_cycles;
            }

            Directive::Next(self.next_node.clone())
        } else {
            Directive::Stop
        }
    }

    #[inline]
    fn processed_packets(&self) -> usize {
        self.total_generated
    }

    fn full_batch_count(&self) -> Option<usize> {
        Some(self.batch_size)
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}