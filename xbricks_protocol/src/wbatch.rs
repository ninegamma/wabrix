use core::mem::MaybeUninit;
use core::ops::{Bound, RangeBounds};
use core::ptr;
use crate::wpdu::WPdu;

/// The maximum number of packets in a single batch.
pub const MAX_BATCH_SIZE: usize = 512;
#[repr(C, align(64))]
pub struct Pdu0 {
    /// [0..56] 56 Bytes: The pure Wasm-compatible core!
    pub w: WPdu,
    /// Do not touch! This is the Host's SystemBus pointer.
    _do_not_touch_padding: u64,
}

// This lets you call `packet.parse_fast()` directly on Pdu0 in Wasm!
impl core::ops::Deref for Pdu0 {
    type Target = WPdu;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.w
    }
}

impl core::ops::DerefMut for Pdu0 {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.w
    }
}

impl Drop for Pdu0 {
    fn drop(&mut self) {
        // If a Wasm developer lets a Pdu0 fall out of scope, the plugin will
        // instantly trap and print this exact error to the Host logs!
        panic!(
            "FATAL: Pdu0 dropped inside Wasm! \
            All packets must be explicitly pushed to a WBatch and returned to the Host."
        );
    }
}


const _: () = assert!(size_of::<Pdu0>() == 64);
pub struct WBatch {
    /// A dense array of uninitialized memory.
    /// NO padding between PDUs.
    /// For RTC, we usually fill the first `count` slots.
    packets: [MaybeUninit<Pdu0>; MAX_BATCH_SIZE],
    /// Number of packets currently in the batch
    count: usize,
}



impl WBatch {

    /// Create an empty batch
    pub fn new() -> Self {
        Self {
            // 100% Safe, zero-cost initialization in stable Rust!
            packets: [const { MaybeUninit::uninit() }; MAX_BATCH_SIZE],
            count: 0
        }
    }

    /// Add a PDU to the batch
    #[inline]
    pub fn push(&mut self, pdu: Pdu0) -> Result<(), Pdu0> {
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
    pub fn pop(&mut self) -> Option<Pdu0> {
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


    /// Access a Pdu reference
    #[inline]
    pub fn get(&self, index: usize) -> &Pdu0 {
        debug_assert!(index < self.count);
        unsafe { &*self.packets[index].as_ptr() }
    }

    /// Access a mutable Pdu reference
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> &mut Pdu0 {
        debug_assert!(index < self.count);
        unsafe { &mut *self.packets[index].as_mut_ptr() }
    }

    /// Takes the Pdu out of the slot and shifts remaining elements left.
    /// Compare also with swap_remove
    #[inline]
    pub fn take(&mut self, idx: usize) -> Option<Pdu0> {
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


    /// Updates the length (used after compaction).
    #[inline]
    pub fn set_count(&mut self, new_count: usize) {
        self.count = new_count;
    }


    /// Iterator support
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Pdu0> {
        // Create slice of initialized range
        let slice = unsafe { core::slice::from_raw_parts(self.packets[0].as_ptr(), self.count) };
        slice.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Pdu0> {
        let slice = unsafe { core::slice::from_raw_parts_mut(self.packets[0].as_mut_ptr(), self.count) };
        slice.iter_mut()
    }

    // --- The "Distribute" / "Drain" Logic ---

    /// Moves a Pdu out of the batch at specific index.
    /// Fills the gap by swapping with the last element (O(1) removal).
    #[inline]
    pub fn swap_remove(&mut self, index: usize) -> Pdu0 {
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

        assert!(start <= end && end <= self.count, "Batch::drain range out of bounds");

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
    pub fn distribute<F>(&mut self, outputs: &mut [WBatch], mut classify: F)
    where
        F: FnMut(&Pdu0) -> usize,
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
                    match outputs[class_idx].push(pdu) {
                        Ok(_) => None, // Success: Ownership moved to output
                        Err(returned_pdu) => {
                            // FAILURE: Output full!
                            Some(returned_pdu) // We must keep this Pdu
                        }
                    }
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
}

/// catch dropping of WBatch
impl Drop for WBatch {
    fn drop(&mut self) {
        if self.count > 0 {
            panic!(
                "FATAL: WBatch leak! A WBatch containing {} packets was dropped in Wasm. \
                You must return this batch to the Host.",
                self.count
            );
        }
    }
}

/// An iterator that removes a range of elements from the batch.
/// When the iterator is dropped, all remaining elements are shifted left
/// to fill the gap in a single, highly optimized memory copy.
pub struct Drain<'a> {
    batch: &'a mut WBatch,
    start: usize,
    end: usize,
    current: usize,
    tail_start: usize,
    tail_len: usize,
}

impl<'a> Iterator for Drain<'a> {
    type Item = Pdu0;

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
        while let Some(pdu) = self.next() {
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
