use std::fs::File;
use std::io::BufReader;
use log::{warn, info, debug};
use pcap_file::pcap::PcapReader;

pub struct PcapRxNode {
    id: NodeId,
    next_node: NodeId,
    reader: PcapReader<BufReader<File>>,
    umem_group_id: usize,
    loop_file: bool,
    file_path: String,
    processed_packets: usize,
}

impl PcapRxNode {
    pub fn new(id: NodeId, next: NodeId, filepath: &str, umem_id: usize, loop_file: bool) -> Self {
        let file = File::open(filepath).expect("Could not open PCAP file");
        let reader = PcapReader::new(BufReader::new(file)).expect("Invalid PCAP format");

        PcapRxNode {
            id,
            next_node: next,
            reader,
            umem_group_id: umem_id,
            loop_file,
            file_path: filepath.to_string(),
            processed_packets: 0,
        }
    }

    fn reopen_file(&mut self) {
        let file = File::open(&self.file_path).unwrap();
        self.reader = PcapReader::new(BufReader::new(file)).unwrap();
    }
}

impl Node for PcapRxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        batch.clear();

        // 1. Get Registry
        let registry_ptr = get_worker_frame_registry();
        if registry_ptr.is_null() {
            return Directive::Stop;
        }
        let registry = unsafe { &mut *registry_ptr };

        // 2. Allocate a batch of frames (e.g., 64)
        // We allocate max batch size, even if file has fewer packets left (we return unused later)
        let mut pdus = registry.allocate_pdus(self.umem_group_id, xbricks_lib::MAX_BATCH_SIZE);

        if pdus.is_empty() {
            // System is backed up, retry next cycle
            return Directive::Stop;
        }

        let mut pdus_used = 0;

        for pdu in &mut pdus {
            // 3. Read next packet from file
            match self.reader.next_packet() {
                Some(Ok(packet)) => {
                    let len = packet.data.len();

                    // Safety check: packet fits in frame
                    // Assuming frame size is at least 2048
                    if len > 2048 {
                        warn!("Skipping jumbo packet len={}", len);
                        continue;
                    }

                    unsafe {
                        // 4. Copy data from File Buffer -> UMEM
                        std::ptr::copy_nonoverlapping(packet.data.as_ptr(), pdu.w.data_ptr(), len);
                    }

                    // 5. Update PDU metadata
                    pdu.w.set_data_length(len as u32);
                    // pdu.timestamp = packet.timestamp; // If you add set_timestamp to Pdu

                    // 6. Parse headers (Eth/IP/TCP) so the next node can use them
                    pdu.w.parse();

                    if batch.push(pdu.shallow_copy()).is_ok() {
                        pdus_used += 1;
                    } else {
                        break; // Batch full
                    }
                }
                Some(Err(e)) => {
                    warn!("PCAP read error: {:?}", e);
                    break;
                }
                None => {
                    // EOF
                    if self.loop_file {
                        debug!("PCAP EOF, looping...");
                        self.reopen_file();
                        // Continue in next process() call
                        break;
                    } else {
                        info!("PCAP EOF. Stopping node.");
                        return Directive::Stop; // Or Directive::Remove to kill just this pipeline
                    }
                }
            }
        }
        self.processed_packets += pdus_used;

        // 7. Return unused frames to registry
        // If we allocated 64 but only read 10 packets, return 54.
        if pdus_used < pdus.len() {
            let _unused = pdus.split_off(pdus_used);
        }

        // 8. Output
        if pdus_used > 0 {
            Directive::Next(self.next_node.clone())
        } else {
            // No packets read this cycle
            Directive::Stop
        }
    }
    fn processed_packets(&self) -> usize {
        self.processed_packets
    }
}

// src/nodes/pcap_tx.rs

use std::io::BufWriter;
use std::time::{SystemTime, UNIX_EPOCH};
use pcap_file::pcap::{PcapWriter, PcapPacket};
use xbricks_base::interface::batch::Batch;
use xbricks_base::{Directive, Node, NodeId};
use xbricks_base::frame_registry::get_worker_frame_registry;

pub struct PcapTxNode {
    id: NodeId,
    writer: PcapWriter<BufWriter<File>>,
    umem_group_id: usize, // Needed to know where to return frames
    written_packets: usize,
}

impl PcapTxNode {
    pub fn new(id: NodeId, filepath: &str, umem_id: usize) -> Self {
        let file = File::create(filepath).expect("Could not create PCAP file");
        let writer = PcapWriter::new(BufWriter::new(file)).expect("Failed to init PCAP writer");

        PcapTxNode {
            id,
            writer,
            umem_group_id: umem_id,
            written_packets: 0,
        }
    }
}

impl Node for PcapTxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        if batch.is_empty() {
            return Directive::Stop;
        }
        // 1. Iterate over frames
        // We drain the batch because we are a Sink (we consume the packets)
        while let Some(pdu) = batch.pop() {
            unsafe {
                // 2. Extract Data
                let data_slice = std::slice::from_raw_parts(pdu.w.data_ptr(), pdu.w.data_len());

                // 3. Create generic timestamp (or use pdu.timestamp if available)
                let start = SystemTime::now();
                let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap_or_default();

                let packet = PcapPacket::new(since_the_epoch, data_slice.len() as u32, data_slice);

                // 4. Write to disk (Buffered)
                if let Err(e) = self.writer.write_packet(&packet) {
                    log::error!("Failed to write packet: {:?}", e);
                } else {
                    self.written_packets += 1;
                }
            }

            // PDU is dropped here and frame is returned
        }

        Directive::Stop // We consumed the batch
    }

    fn processed_packets(&self) -> usize {
        self.written_packets
    }
}
