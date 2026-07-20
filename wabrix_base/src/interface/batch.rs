use crate::frame_registry::get_worker_frame_registry;
use crate::interface::Pdu;
use core::mem::MaybeUninit;
use core::ops::{Bound, RangeBounds};
use core::ptr;
use log::trace;
use wabrix_api::MAX_BATCH_SIZE;

pub struct Batch {
    /// A dense array of uninitialized memory.
    /// NO padding between PDUs.
    /// For RTC, we usually fill the first `count` slots.
    packets: [MaybeUninit<Pdu>; MAX_BATCH_SIZE],
    /// Number of packets currently in the batch
    count: usize,
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch {
    /// Create an empty batch
    pub fn new() -> Self {
        // Safe because MaybeUninit doesn't require initialization
        let packets = unsafe { MaybeUninit::uninit().assume_init() };
        Self { packets, count: 0 }
    }

    /// Add a PDU to the batch
    #[inline]
    pub fn push(&mut self, pdu: Pdu) -> Result<(), Pdu> {
        if self.count < MAX_BATCH_SIZE {
            // Write Pdu directly into the uninit slot
            self.packets[self.count].write(pdu);
            self.count += 1;
            Ok(())
        } else {
            Err(pdu)
        }
    }

    /// Remove and return the last PDU from the batch
    #[inline]
    pub fn pop(&mut self) -> Option<Pdu> {
        if self.count > 0 {
            self.count -= 1;
            unsafe { Some(ptr::read(self.packets[self.count].as_ptr())) }
        } else {
            None
        }
    }

    /// Returns the number of packets in the batch
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// returns a raw pointer to the batch's data
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.packets.as_ptr() as *const u8
    }

    /// returns a raw pointer to the batch's data
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.packets.as_mut_ptr() as *mut u8
    }

    /// Clear the batch without dropping the PDUs manually
    #[inline]
    pub fn clear(&mut self) {
        // IMPORTANT: We must Drop the active PDUs before resetting count!
        // Pdu::drop handles the return to UMEM.
        for i in 0..self.count {
            unsafe {
                ptr::drop_in_place(self.packets[i].as_mut_ptr());
            }
        }
        self.count = 0;
    }

    /// Access a Pdu reference
    #[inline]
    pub fn get(&self, index: usize) -> &Pdu {
        debug_assert!(index < self.count);
        unsafe { &*self.packets[index].as_ptr() }
    }

    /// Access a mutable Pdu reference
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> &mut Pdu {
        debug_assert!(index < self.count);
        unsafe { &mut *self.packets[index].as_mut_ptr() }
    }

    /// Takes the Pdu out of the slot and shifts remaining elements left.
    /// Compare also with swap_remove
    #[inline]
    pub fn take(&mut self, idx: usize) -> Option<Pdu> {
        if idx < self.count {
            unsafe {
                // Read the Pdu at idx
                let pdu = ptr::read(self.packets[idx].as_ptr());

                // Shift elements left to fill the gap
                if idx < self.count - 1 {
                    ptr::copy(
                        self.packets[idx + 1].as_ptr(),
                        self.packets[idx].as_mut_ptr(),
                        self.count - idx - 1,
                    );
                }

                self.count -= 1;
                Some(pdu)
            }
        } else {
            None
        }
    }

    /// Puts a Pdu into a specific slot (used for compaction/restoring).
    #[inline]
    pub fn replace(&mut self, idx: usize, pdu: Pdu) {
        if idx < MAX_BATCH_SIZE {
            self.packets[idx].write(pdu);
            if idx >= self.count {
                self.count = idx + 1;
            }
        }
    }

    /// Updates the length (used after compaction).
    #[inline]
    pub fn set_count(&mut self, new_count: usize) {
        self.count = new_count;
    }

    /// A high-performance filter: removes packets that don't match the predicate
    /// and returns them to the UMEM immediately.
    #[inline]
    pub fn filter<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&Pdu) -> bool,
    {
        let mut write_idx = 0;
        for read_idx in 0..self.count {
            unsafe {
                let pdu_ref = &*self.packets[read_idx].as_ptr();
                let keep = predicate(pdu_ref);

                if keep {
                    if read_idx != write_idx {
                        self.packets.swap(read_idx, write_idx);
                    }
                    write_idx += 1;
                } else {
                    // Drop the Pdu, which returns the frame to XSocket
                    ptr::drop_in_place(self.packets[read_idx].as_mut_ptr());
                }
            }
        }
        self.count = write_idx;
    }

    /// Iterator support
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Pdu> {
        // Create slice of initialized range
        let slice = unsafe { std::slice::from_raw_parts(self.packets[0].as_ptr(), self.count) };
        slice.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Pdu> {
        let slice =
            unsafe { std::slice::from_raw_parts_mut(self.packets[0].as_mut_ptr(), self.count) };
        slice.iter_mut()
    }

    // --- The "Distribute" / "Drain" Logic ---

    /// Moves a Pdu out of the batch at specific index.
    /// Fills the gap by swapping with the last element (O(1) removal).
    #[inline]
    pub fn swap_remove(&mut self, index: usize) -> Pdu {
        debug_assert!(index < self.count);
        unsafe {
            // Read the element at index (conceptually "taking" it)
            let result = ptr::read(self.packets[index].as_ptr());

            // Decrement count
            self.count -= 1;

            // If we are not the last element, fill the hole with the last element
            if index != self.count {
                ptr::copy_nonoverlapping(
                    self.packets[self.count].as_ptr(), // Source (old last)
                    self.packets[index].as_mut_ptr(),  // Dest (hole)
                    1,
                );
            }

            result
        }
    }

    /// Removes the specified range from the batch in bulk, returning all removed
    /// elements as an iterator. If the iterator is dropped before being fully
    /// consumed, it drops the remaining removed elements.
    /// The remaining elements in the batch are shifted left in O(1) bulk operations.
    pub fn drain<R>(&mut self, range: R) -> Drain<'_>
    where
        R: RangeBounds<usize>,
    {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e + 1,
            Bound::Excluded(&e) => e,
            Bound::Unbounded => self.count,
        };

        assert!(
            start <= end && end <= self.count,
            "Batch::drain range out of bounds"
        );

        let tail_start = end;
        let tail_len = self.count - end;

        // SAFETY: We immediately set the batch count to `start`.
        // If the `Drain` iterator gets leaked (std::mem::forget), the batch safely
        // forgets the drained items and the tail, preventing a double-free!
        self.count = start;

        Drain {
            batch: self,
            start,
            end,
            current: start,
            tail_start,
            tail_len,
        }
    }

    /// Distributes packets into output batches based on a classifier function.
    ///
    /// - `outputs`: A mutable slice of destination batches.
    /// - `classify`: A closure returning the index of the batch to send to.
    ///
    /// If an output batch is full (push fails), the packet is KEPT in this batch.
    pub fn distribute<F>(&mut self, outputs: &mut [Batch], mut classify: F)
    where
        F: FnMut(&Pdu) -> usize,
    {
        let mut write_idx = 0;
        let count = self.count; // Capture original count

        for read_idx in 0..count {
            unsafe {
                // 1. STEAL: Read the Pdu bits onto the stack.
                // We now conceptually own this Pdu. The slot at read_idx is "empty".
                let pdu = ptr::read(self.packets[read_idx].as_ptr());

                let class_idx = classify(&pdu);

                // 2. TRY MOVE: Attempt to push to the designated output.
                let pdu_to_keep = if class_idx < outputs.len() {
                    outputs[class_idx].push(pdu).err()
                } else {
                    Some(pdu) // Invalid class (or default route logic), keep it.
                };

                // 3. COMPACT: If we still hold the Pdu, write it to the 'write_idx' slot.
                if let Some(p) = pdu_to_keep {
                    // This overwrites the slot at write_idx.
                    // If read_idx == write_idx, it effectively writes the data back to where it was.
                    self.packets[write_idx].as_mut_ptr().write(p);
                    write_idx += 1;
                }
            }
        }

        // 4. FINALIZE: Update the count.
        // Any slots between write_idx and old count are now logically empty
        // (and their data was effectively moved or dropped).
        self.count = write_idx;
    }

    /// Optimized bulk release of PDUs back to the UMEM.
    /// This bypasses the individual Drop overhead for each PDU.
    pub fn bulk_drop(&mut self) {
        if self.count == 0 {
            return;
        }

        unsafe {
            let registry_ptr = get_worker_frame_registry();
            if !registry_ptr.is_null() {
                let registry = &mut *registry_ptr;

                // Group by owner_id for efficient cross-thread returns if needed
                // But for now, let's assume local release as it's the 99% case
                for i in 0..self.count {
                    let pdu = &*self.packets[i].as_ptr();
                    if registry.worker_id == pdu.owner_id() as usize {
                        trace!(
                            "bulk_drop: Returning frame {} to local stash",
                            pdu.w.global_frame_idx()
                        );
                        registry.release_local(pdu.w.global_frame_idx());
                    } else {
                        // Slow path: Individual cross-thread release
                        // This still uses the drop logic for consistency
                        ptr::drop_in_place(self.packets[i].as_mut_ptr());
                    }
                }
            } else {
                // Fallback to standard individual drop if no registry found
                for i in 0..self.count {
                    ptr::drop_in_place(self.packets[i].as_mut_ptr());
                }
            }
        }
        self.count = 0;
    }
}

// Ensure proper cleanup if the Batch itself is dropped
impl Drop for Batch {
    fn drop(&mut self) {
        self.bulk_drop();
    }
}

/// An iterator that removes a range of elements from the batch.
/// When the iterator is dropped, all remaining elements are shifted left
/// to fill the gap in a single, highly optimized memory copy.
pub struct Drain<'a> {
    batch: &'a mut Batch,
    start: usize,
    end: usize,
    current: usize,
    tail_start: usize,
    tail_len: usize,
}

impl<'a> Iterator for Drain<'a> {
    type Item = Pdu;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current < self.end {
            unsafe {
                // Read the PDU out of the slot
                let pdu = ptr::read(self.batch.packets[self.current].as_ptr());
                self.current += 1;
                Some(pdu)
            }
        } else {
            None
        }
    }
}

impl<'a> Drop for Drain<'a> {
    fn drop(&mut self) {
        // 1. Drop any un-consumed elements in the drained range
        for pdu in self.by_ref() {
            drop(pdu);
        }

        // 2. Shift the tail elements left into the gap created by the drain
        if self.tail_len > 0 {
            unsafe {
                ptr::copy(
                    self.batch.packets[self.tail_start].as_ptr(),
                    self.batch.packets[self.start].as_mut_ptr(),
                    self.tail_len,
                );
            }
        }

        // 3. Restore the correct length of the batch
        self.batch.count = self.start + self.tail_len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Create a mock Pdu for testing
    // SAFETY: This creates a zeroed Pdu which is only safe for testing purposes.
    // In production, Pdu must be properly initialized with valid UMEM and SystemBus.
    fn create_mock_pdu(_id: u32) -> Pdu {
        unsafe { std::mem::zeroed() }
    }

    #[test]
    fn test_new_batch() {
        let batch = Batch::new();
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_push_and_len() {
        let mut batch = Batch::new();

        // Push one PDU
        let pdu = create_mock_pdu(1);
        let _ = batch.push(pdu);
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_push_until_full() {
        let mut batch = Batch::new();

        // Fill the batch
        for i in 0..MAX_BATCH_SIZE {
            let pdu = create_mock_pdu(i as u32);
            let _ = batch.push(pdu);
        }

        assert_eq!(batch.len(), MAX_BATCH_SIZE);

        // Try to push one more (should fail)
        let pdu = create_mock_pdu(999);
        assert!(batch.push(pdu).is_err());
    }

    #[test]
    fn test_get() {
        let mut batch = Batch::new();

        for i in 0..5 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Access each PDU
        for i in 0..5 {
            let _pdu = batch.get(i);
            // In a real test with proper PDU, we'd verify content here
        }
    }

    #[test]
    fn test_get_mut() {
        let mut batch = Batch::new();

        for i in 0..3 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Get mutable reference
        let _pdu_mut = batch.get_mut(0);
        // In a real test, we'd modify and verify
    }

    #[test]
    fn test_clear() {
        let mut batch = Batch::new();

        for i in 0..10 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        assert_eq!(batch.len(), 10);

        batch.clear();

        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_take() {
        let mut batch = Batch::new();

        for i in 0..5 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Take from middle
        let taken = batch.take(2);
        assert!(taken.is_some());
        assert_eq!(batch.len(), 4);

        // Take out of bounds
        let taken = batch.take(10);
        assert!(taken.is_none());
    }

    #[test]
    fn test_replace() {
        let mut batch = Batch::new();

        for i in 0..3 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Replace at existing index
        let new_pdu = create_mock_pdu(99);
        batch.replace(1, new_pdu);

        // Count should remain the same
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_set_count() {
        let mut batch = Batch::new();

        for i in 0..10 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        batch.set_count(5);
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn test_swap_remove() {
        let mut batch = Batch::new();

        for i in 0..5 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Remove from middle using swap_remove
        let _removed = batch.swap_remove(2);
        assert_eq!(batch.len(), 4);

        // Remove last element
        let _removed = batch.swap_remove(3);
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_filter() {
        let mut batch = Batch::new();

        for i in 0..10 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Filter: keep all (this is a simple test since we can't inspect mock PDU easily)
        batch.filter(|_pdu| true);
        assert_eq!(batch.len(), 10);

        // Filter: remove all
        batch.filter(|_pdu| false);
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_distribute() {
        let mut batch = Batch::new();

        // Create source batch with 10 PDUs
        for i in 0..10 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Create output batches
        let mut outputs = [Batch::new(), Batch::new(), Batch::new()];

        // Distribute: round-robin classification
        let mut counter = 0;
        batch.distribute(&mut outputs, |_pdu| {
            let idx = counter % 3;
            counter += 1;
            idx
        });

        // Source batch should be empty (all distributed)
        assert_eq!(batch.len(), 0);

        // Check distribution (should be roughly equal)
        assert!(!outputs[0].is_empty());
        assert!(!outputs[1].is_empty());
        assert!(!outputs[2].is_empty());

        // Total should be 10
        let total = outputs[0].len() + outputs[1].len() + outputs[2].len();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_distribute_with_full_output() {
        let mut batch = Batch::new();

        // Create a small batch
        for i in 0..3 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Create one output batch and fill it almost completely
        let mut outputs = [Batch::new()];
        for i in 0..MAX_BATCH_SIZE - 1 {
            let pdu = create_mock_pdu(100 + i as u32);
            let _ = outputs[0].push(pdu);
        }

        // Try to distribute - only 1 should fit
        batch.distribute(&mut outputs, |_pdu| 0);

        // One PDU should have been moved, two should remain
        assert_eq!(batch.len(), 2);
        assert_eq!(outputs[0].len(), MAX_BATCH_SIZE);
    }

    #[test]
    fn test_iter() {
        let mut batch = Batch::new();

        for i in 0..5 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        let mut count = 0;
        for _pdu in batch.iter() {
            count += 1;
        }

        assert_eq!(count, 5);
    }

    #[test]
    fn test_iter_mut() {
        let mut batch = Batch::new();

        for i in 0..5 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        let mut count = 0;
        for _pdu in batch.iter_mut() {
            count += 1;
        }

        assert_eq!(count, 5);
    }

    #[test]
    fn test_drain() {
        let mut batch = Batch::new();

        for i in 0..10 {
            let pdu = create_mock_pdu(i);
            let _ = batch.push(pdu);
        }

        // Drain 4 items from the middle [idx 2, 3, 4, 5]
        let drained: Vec<Pdu> = batch.drain(2..6).collect();

        assert_eq!(drained.len(), 4);
        assert_eq!(batch.len(), 6);
    }
}
