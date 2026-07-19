use std::arch::x86_64::_rdtsc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use wabrix_base::interface::batch::Batch;

pub struct BatchRxNode {
    id: NodeId,
    next_node: NodeId,
    rx_source: Box<dyn BatchRx + Send>,
    received_packets: u64,
    batch_size: usize,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
    #[cfg(feature = "profiling")]
    loaded_cycles: u64,
    #[cfg(feature = "profiling")]
    empty_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_avg_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_total_cycles: u64,
    #[cfg(feature = "profiling")]
    replenish_calls: u64,
}

impl BatchRxNode {
    pub fn new(id: NodeId, next_node: NodeId, rx_source: Box<dyn BatchRx + Send>, batch_size: usize) -> Self {
        Self {
            id,
            next_node,
            rx_source,
            received_packets: 0,
            batch_size,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
            #[cfg(feature = "profiling")]
            loaded_cycles: 0,
            #[cfg(feature = "profiling")]
            empty_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_avg_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_total_cycles: 0,
            #[cfg(feature = "profiling")]
            replenish_calls: 0,
        }
    }
}

impl Node for BatchRxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // 1. Ensure the batch is empty to maximize burst capacity.
        // The Scheduler provides a reused batch container, so we must reset it.
        // Note: internal Pdu pointers were already dropped/returned by the previous run's clean-up.
        batch.clear();

        // 2. Pull from hardware/socket
        let result = match self.rx_source.receive_into_batch(batch, self.batch_size) {
            Ok(count) => {
                if count > 0 {
                    // trace!("Source {}: received {} packets", self.id, count);
                    self.received_packets += count as u64;

                    #[cfg(feature = "profiling")]
                    {
                        let end_cycles = unsafe { _rdtsc() };
                        let elapsed = end_cycles - start_cycles;
                        self.total_cycles += elapsed;
                        self.loaded_cycles += elapsed;
                    }

                    // 3. We have work to do -> Push to the pipeline
                    Directive::Next(self.next_node.clone())
                } else {
                    #[cfg(feature = "profiling")]
                    {
                        let end_cycles = unsafe { _rdtsc() };
                        let elapsed = end_cycles - start_cycles;
                        self.total_cycles += elapsed;
                        self.empty_cycles += elapsed;
                    }

                    // 4. No packets available -> Stop this run immediately
                    Directive::Stop
                }
            }
            Err(e) => {
                // 5. Hardware/Polling error
                // We log it but don't crash the thread. We just stop processing this batch.
                warn!("Source Node {} [{}] error: {}", self.id, self.rx_source.source_id(), e);

                #[cfg(feature = "profiling")]
                {
                    let end_cycles = unsafe { _rdtsc() };
                    let elapsed = end_cycles - start_cycles;
                    self.total_cycles += elapsed;
                    self.empty_cycles += elapsed;
                }

                Directive::Stop
            }
        };

        result
    }

    fn prepare(&mut self) -> anyhow::Result<()> {
        self.rx_source.replenish(1024)
    }

    #[inline]
    fn processed_packets(&self) -> u64 {
        self.received_packets
    }

    fn full_batch_count(&self) -> Option<u32> {
        Some(self.batch_size as u32)
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

impl BatchRxNode {
    #[cfg(feature = "profiling")]
    pub fn loaded_cycles(&self) -> u64 {
        self.loaded_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn empty_cycles(&self) -> u64 {
        self.empty_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_avg_cycles(&self) -> u64 {
        self.replenish_avg_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_total_cycles(&self) -> u64 {
        self.replenish_total_cycles
    }

    #[cfg(feature = "profiling")]
    pub fn replenish_calls(&self) -> u64 {
        self.replenish_calls
    }

    #[cfg(feature = "profiling")]
    pub fn update_profiling_from_source(&mut self) {
        self.replenish_avg_cycles = self.rx_source.get_replenish_avg_cycles();
        self.replenish_total_cycles = self.rx_source.get_replenish_total_cycles();
        self.replenish_calls = self.rx_source.get_replenish_calls();
    }
}

pub struct BatchTxNode {
    id: NodeId,
    tx_sink: Box<dyn BatchTx + Send>,
    sent_packets: u64,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl BatchTxNode {
    /// Creates a new BatchTxNode.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this node.
    /// * `tx_sink` - The implementation of the transmitter (e.g., an XSocket).
    pub fn new(id: NodeId, tx_sink: Box<dyn BatchTx + Send>) -> Self {
        Self {
            id,
            tx_sink,
            sent_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for BatchTxNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        log::trace!("TX Node: Received batch of len {}", batch.len());
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // WE DO NOT RETURN EARLY ON EMPTY BATCHES.
        // We must call transmit_batch() so the underlying XSocket can read the
        // hardware Completion Queue (CQ) and free the previous frames!

        // Attempt transmission
        // The sink is responsible for:
        // - Checking ring capacity (nb_free)
        // - Creating descriptors
        // - Consuming (removing) successful packets from the batch
        let result = match self.tx_sink.transmit_batch(batch) {
            Ok(sent_count) => {
                self.sent_packets += sent_count as u64;
                // Telemetry (optional)
                log::trace!("Node {}: Sent {} packets", self.id, sent_count);

                // 3. Handle Partial Sends / Congestion
                // If sent_count < original batch.len(), the remaining packets
                // match the "Tail Drop" behavior.
                //
                // For performance reasons we bulk drop here:
                // Instantly recycle all tail-dropped packets in ~5 cycles!
                batch.bulk_drop();
                Directive::Stop
            }
            Err(e) => {
                // 4. Handle Errors (e.g., Driver down, Ring broken)
                warn!("Node {}: TX Error on sink {}: {}", self.id, self.tx_sink.sink_id(), e);

                // Explicitly clear to ensure resources are returned immediately
                // rather than waiting for Scheduler cleanup.
                batch.clear();

                Directive::Stop
            }
        };

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        result
    }

    #[inline]
    fn processed_packets(&self) -> u64 {
        self.sent_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

use crate::{BatchRx, BatchTx};
use crossbeam_channel::Sender;
use log::warn;

pub struct QueueNode {
    id: NodeId,
    /// The sender side of a channel leading to another Scheduler/Core
    tx: Sender<Batch>,
    /// The ID of the node that should process this batch on the destination side
    #[allow(dead_code)]
    next_node_id: NodeId,
    processed_packets: u64,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl QueueNode {
    pub fn new(id: NodeId, tx: Sender<Batch>, next_node_id: NodeId) -> Self {
        Self {
            id,
            tx,
            next_node_id,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for QueueNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        // We take ownership of the batch data to move it into the channel.
        // We replace the local batch with an empty one.
        let batch_to_send = std::mem::replace(batch, Batch::new());
        let packet_count = batch_to_send.len();
        // Attempt to send the batch to the next processing stage
        let result = match self.tx.try_send(batch_to_send) {
            Ok(_) => {
                // Return Stop because this thread's RTC path for this batch ends here.
                self.processed_packets += packet_count as u64;
                Directive::Stop
            }
            Err(e) => {
                // If the queue is full, we must drop the batch.
                // Because Batch contains Pdus, and Pdus have a Drop impl,
                // the frames will automatically return to the original UMEM.
                warn!("QueueNode {}: Drop due to congestion: {}", self.id, e);
                Directive::Stop
            }
        };

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        result
    }

    fn processed_packets(&self) -> u64 {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

/// A sink node that intentionally discards all packets it receives.
/// Useful for:
/// 1. Terminating "unmatched" traffic from a Classifier (Default Route).
/// 2. Implementing firewall "Deny" rules.
/// 3. Debugging/Testing throughput (Blackhole).
pub struct DropNode {
    id: NodeId,
    dropped_count: u64,
    /// Shared atomic counter published to the telemetry registry so the GUI
    /// can display the running dropped-packet total in real time.
    telemetry_counter: Option<Arc<AtomicU64>>,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl DropNode {
    pub fn new(id: NodeId, telemetry_counter: Option<Arc<AtomicU64>>) -> Self {
        Self {
            id,
            dropped_count: 0,
            telemetry_counter,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for DropNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { std::arch::x86_64::_rdtsc() };

        let count = batch.len();
        if count > 0 {
            self.dropped_count += count as u64;
            if let Some(counter) = &self.telemetry_counter {
                counter.fetch_add(count as u64, Ordering::Relaxed);
            }
            log::trace!("DropNode {}: Dropped {} packets", self.id, count);
            // 🚀 BOOM: 1-Call Amortized Bulk Drop!
            batch.bulk_drop();
        }

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { std::arch::x86_64::_rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Stop
    }

    #[inline]
    fn processed_packets(&self) -> u64 {
        self.dropped_count
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}

use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
use wabrix_base::{Directive, Node, NodeId};

/// Fast parser node with prefetching optimization.
/// Parses packet headers with data prefetching to minimize cache misses.
pub struct FastParserNode {
    id: NodeId,
    next_node: NodeId,
    processed_packets: u64,
    #[cfg(feature = "profiling")]
    total_cycles: u64,
}

impl FastParserNode {
    pub fn new(id: NodeId, next_node: NodeId) -> Self {
        Self {
            id,
            next_node,
            processed_packets: 0,
            #[cfg(feature = "profiling")]
            total_cycles: 0,
        }
    }
}

impl Node for FastParserNode {
    fn id(&self) -> &NodeId {
        &self.id
    }

    fn process(&mut self, batch: &mut Batch) -> Directive {
        #[cfg(feature = "profiling")]
        let start_cycles = unsafe { _rdtsc() };

        let len = batch.len();
        if len == 0 {
            return Directive::Stop;
        }

        let prefetch_dist = 4;

        for i in 0..len {
            // 1. Pre-fetch the data for a future PDU in the batch
            if i + prefetch_dist < len {
                let future_pdu = batch.get(i + prefetch_dist);
                unsafe {
                    let future_addr = future_pdu.data_address(0);
                    _mm_prefetch(future_addr as *const i8, _MM_HINT_T0);
                }
            }

            // 2. Parse the current PDU
            let current_pdu = batch.get_mut(i);
            current_pdu.parse_fast();

            log::trace!("Current Pdu: {}", current_pdu);
        }

        self.processed_packets += len as u64;

        #[cfg(feature = "profiling")]
        {
            let end_cycles = unsafe { _rdtsc() };
            self.total_cycles += end_cycles - start_cycles;
        }

        Directive::Next(self.next_node.clone())
    }

    #[inline]
    fn processed_packets(&self) -> u64 {
        self.processed_packets
    }

    #[cfg(feature = "profiling")]
    fn profiling_cycles(&self) -> u64 {
        self.total_cycles
    }
}
