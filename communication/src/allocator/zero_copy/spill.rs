//! Spill strategies and policies for `MergeQueue` entries under memory pressure.
//!
//! Three traits compose here:
//!
//! - [`SpillPolicy`] decides *whether and how* a queue should be reshaped at
//!   each `extend`. It is handed the raw `VecDeque<QueueEntry>` under the
//!   queue's mutex and may replace entries freely.
//! - [`BytesSpill`] decides *where* bytes go when a policy chooses to spill.
//!   Pluggable: file, object store, mlock pool, in-memory mock for tests.
//! - [`BytesFetch`] is the handle returned by a `BytesSpill`; it reads the
//!   spilled bytes back, consuming itself in the process.
//!
//! The shipped [`threshold::Threshold`] pairs a `BytesSpill` strategy with
//! threshold/reserve/budget knobs and encodes the "spill the middle of the
//! queue when resident bytes get too large" heuristic. Other policies can
//! make entirely different decisions (memory-pressure-driven, periodic,
//! manual trigger, adaptive) using the same strategies.

use std::collections::VecDeque;
use std::sync::Arc;

use timely_bytes::arc::Bytes;

use super::bytes_exchange::QueueEntry;

/// A function that produces pairs of writer and reader [`SpillPolicy`]s.
///
/// This type is the entry point to spilling, and the two returned policies
/// contain the opinions about how to handle excess data for an instance of
/// a `MergeQueue`.
pub type SpillPolicyFn = Arc<dyn Fn() -> (Box<dyn SpillPolicy>, Box<dyn SpillPolicy>) + Send + Sync>;

/// Inspects and optionally rewrites a `MergeQueue`'s entries.
pub trait SpillPolicy: Send {
    /// Optionally transforms some (ranges of) queue entries.
    ///
    /// This trait is used both for spilling and rehydrating, and just acts
    /// on the list of queue entries, rewriting ranges of them as it sees fit.
    /// This is intented for spilling data to secondary storage, but can also
    /// be used for compression, or other mechanisms to reduce resource load.
    fn apply(&mut self, queue: &mut VecDeque<QueueEntry>);
}

/// A type that can convert runs of bytes into runs of boxed bytes retrieval.
pub trait BytesSpill: Send {
    /// Move entries from `chunks` into `handles`, spilling each to backing storage.
    ///
    /// The implementor should drain from `chunks` and push to `handles`as it goes;
    /// on failure it may stop partway, leaving the data in a consistent state that
    /// will be retried in the future. If it cannot leave the lists in a consistent
    /// state it should panic.
    fn spill(&mut self, chunks: &mut Vec<Bytes>, handles: &mut Vec<Box<dyn BytesFetch>>);
}

/// A consume-once handle to bytes previously written via a [`BytesSpill`].
pub trait BytesFetch: Send {
    /// Consume the handle and return the spilled payload as `Bytes`.
    ///
    /// On failure, the handle is returned so the caller can retry later.
    fn fetch(self: Box<Self>) -> Result<Vec<Bytes>, Box<dyn BytesFetch>>;
}

/// Writer-side spill policy: threshold-based, spills the middle of the queue.
pub mod threshold {
    use super::*;

    /// A threshold-based [`SpillPolicy`]: when a queue's resident-byte count
    /// exceeds `head_reserve_bytes + threshold_bytes`, spill all entries past
    /// the head reserve (except the last entry, which stays as the `try_merge`
    /// target).
    pub struct Threshold {
        strategy: Box<dyn BytesSpill>,
        /// Spillable surplus: spill is considered when resident bytes exceed
        /// `head_reserve_bytes + threshold_bytes`.
        pub threshold_bytes: usize,
        /// Bytes near the head of the queue stay resident, protecting the
        /// consumer from an immediate page-in stall.
        pub head_reserve_bytes: usize,
    }

    impl Threshold {
        /// Create a new threshold policy with default knobs, dispatching spills
        /// through `strategy`.
        pub fn new(strategy: Box<dyn BytesSpill>) -> Self {
            Threshold {
                strategy,
                threshold_bytes:    256 << 20,  // 256 MB
                head_reserve_bytes:  64 << 20,  //  64 MB
            }
        }
    }

    impl SpillPolicy for Threshold {
        fn apply(&mut self, queue: &mut VecDeque<QueueEntry>) {
            let resident: usize = queue.iter().map(|e| match e {
                QueueEntry::Bytes(b) => b.len(),
                QueueEntry::Paged(_) => 0,
            }).sum();
            if resident <= self.head_reserve_bytes + self.threshold_bytes {
                return;
            }

            let head_reserve = self.head_reserve_bytes;

            let mut cumulative: usize = 0;
            let last_index = queue.len().saturating_sub(1);
            let mut target_indices: Vec<usize> = Vec::new();
            let mut target_bytes: Vec<Bytes> = Vec::new();
            for (i, entry) in queue.iter().enumerate() {
                if i == last_index { break; }
                match entry {
                    QueueEntry::Bytes(b) => {
                        if cumulative >= head_reserve {
                            target_indices.push(i);
                            target_bytes.push(b.clone());
                        }
                        cumulative += b.len();
                    }
                    QueueEntry::Paged(_) => {}
                }
            }

            if target_bytes.is_empty() {
                return;
            }

            let mut handles: Vec<Box<dyn BytesFetch>> = Vec::new();
            self.strategy.spill(&mut target_bytes, &mut handles);
            // Replace queue entries for however many chunks were spilled.
            for (i, handle) in target_indices.into_iter().zip(handles) {
                queue[i] = QueueEntry::Paged(handle);
            }
            // Remaining entries in target_bytes (if any) stay Resident.
        }
    }
}

/// Reader-side policy: materializes `Paged` entries near the front.
pub mod prefetch {
    use super::*;

    /// A reader-side [`SpillPolicy`] that materializes `Paged` entries near
    /// the front of the queue up to a byte budget. The writer-side dual of
    /// [`super::threshold::Threshold`]: the writer pages data out, the reader
    /// pages data back in — both through `SpillPolicy::apply`.
    pub struct PrefetchPolicy {
        /// Maximum bytes to materialize per `apply` invocation.
        pub budget: usize,
    }

    impl PrefetchPolicy {
        /// Create a prefetch policy with the given byte budget.
        pub fn new(budget: usize) -> Self {
            PrefetchPolicy { budget }
        }
    }

    impl SpillPolicy for PrefetchPolicy {
        fn apply(&mut self, queue: &mut VecDeque<QueueEntry>) {
            let mut resident_head = 0;
            let mut i = 0;
            while i < queue.len() && resident_head < self.budget {
                match &queue[i] {
                    QueueEntry::Bytes(b) => {
                        resident_head += b.len();
                        i += 1;
                    }
                    QueueEntry::Paged(_) => {
                        let entry = queue.remove(i).expect("index valid");
                        if let QueueEntry::Paged(h) = entry {
                            match h.fetch() {
                                Ok(fetched) => {
                                    let n = fetched.len();
                                    for (j, b) in fetched.into_iter().enumerate() {
                                        resident_head += b.len();
                                        queue.insert(i + j, QueueEntry::Bytes(b));
                                    }
                                    i += n;
                                }
                                Err(h) => {
                                    // Fetch failed; put the handle back and stop.
                                    queue.insert(i, QueueEntry::Paged(h));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// Re-export the key types at the spill module level.
pub use threshold::Threshold;
pub use prefetch::PrefetchPolicy;

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_of(data: &[u8]) -> Bytes {
        timely_bytes::arc::BytesMut::from(data.to_vec()).freeze()
    }

    struct MockStrategy;
    struct MockHandle { data: Bytes }
    impl BytesSpill for MockStrategy {
        fn spill(&mut self, chunks: &mut Vec<Bytes>, handles: &mut Vec<Box<dyn BytesFetch>>) {
            handles.extend(chunks.drain(..)
                .map(|b| Box::new(MockHandle { data: b }) as Box<dyn BytesFetch>));
        }
    }
    impl BytesFetch for MockHandle {
        fn fetch(self: Box<Self>) -> Result<Vec<Bytes>, Box<dyn BytesFetch>> { Ok(vec![self.data]) }
    }

    #[test]
    fn eager_policy_moves_middle_entries() {
        struct EagerPolicy { strategy: Box<dyn BytesSpill> }
        impl SpillPolicy for EagerPolicy {
            fn apply(&mut self, queue: &mut VecDeque<QueueEntry>) {
                let last = queue.len().saturating_sub(1);
                let mut indices = Vec::new();
                let mut bytes = Vec::new();
                for (i, entry) in queue.iter().enumerate() {
                    if i == last { break; }
                    if let QueueEntry::Bytes(b) = entry {
                        indices.push(i);
                        bytes.push(b.clone());
                    }
                }
                if bytes.is_empty() { return; }
                let mut handles = Vec::new();
                self.strategy.spill(&mut bytes, &mut handles);
                for (i, h) in indices.into_iter().zip(handles) {
                    queue[i] = QueueEntry::Paged(h);
                }
            }
        }

        let mut p = EagerPolicy { strategy: Box::new(MockStrategy) };
        let mut queue: VecDeque<QueueEntry> = VecDeque::new();
        for i in 0..4 {
            queue.push_back(QueueEntry::Bytes(bytes_of(&[i as u8; 8])));
        }
        p.apply(&mut queue);
        assert!(matches!(queue[0], QueueEntry::Paged(_)));
        assert!(matches!(queue[1], QueueEntry::Paged(_)));
        assert!(matches!(queue[2], QueueEntry::Paged(_)));
        assert!(matches!(queue[3], QueueEntry::Bytes(_)));
    }

    #[test]
    fn merge_queue_spill_roundtrip_mock() {
        use super::super::bytes_exchange::{MergeQueue, BytesPush, BytesPull};

        let head_reserve = 128;
        let mut tp = Threshold::new(Box::new(MockStrategy));
        tp.threshold_bytes = 512;
        tp.head_reserve_bytes = head_reserve;
        let writer_policy: Box<dyn SpillPolicy> = Box::new(tp);
        let reader_policy: Box<dyn SpillPolicy> = Box::new(PrefetchPolicy::new(head_reserve));

        let buzzer = crate::buzzer::Buzzer::default();
        let (mut writer, mut reader) =
            MergeQueue::new_pair(buzzer, Some(writer_policy), Some(reader_policy));

        let mut expected: Vec<Vec<u8>> = Vec::new();
        for i in 0..100 {
            let data = vec![(i % 251) as u8; 64];
            expected.push(data.clone());
            writer.extend(Some(bytes_of(&data)));
        }

        let mut received: Vec<Bytes> = Vec::new();
        loop {
            let before = received.len();
            reader.drain_into(&mut received);
            if received.len() == before { break; }
        }

        let expected_flat: Vec<u8> = expected.into_iter().flatten().collect();
        let received_flat: Vec<u8> = received.iter().flat_map(|b| b.iter().copied()).collect();
        assert_eq!(expected_flat, received_flat);
    }
}
