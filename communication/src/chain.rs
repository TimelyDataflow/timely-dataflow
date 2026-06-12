//! An intra-process single-writer, multi-reader all-reduce structure.
//!
//! This module provides a forward-linked, compacting chain of "atoms", intended to
//! eventually replace the intra-process leg of timely's progress broadcasting: each
//! worker writes progress updates into its own [`Chain`], and all workers (and any
//! network threads) read from all chains. A [`Mesh`] bundles one chain per writer.
//!
//! # Contract
//!
//! - A [`Chain<T>`] has **one writer** ([`Writer`]) and any number of readers
//!   ([`Reader`]), created from the [`Chain`] handle. Readers created later observe
//!   only atoms sent after their creation.
//! - [`Writer::send`] commits an atom. Every reader eventually folds, exactly once,
//!   every atom committed after its registration. Atoms may be merged with adjacent
//!   atoms (never split) via [`Chainable`], which must be a **commutative** monoid:
//!   commutativity is required because multiple chains and multiple readers impose
//!   no cross-atom ordering (a reader of a [`Mesh`] sees atoms from different chains
//!   in no particular relative order, and merging may reorder contributions).
//! - Live state is bounded by `O(#readers)`, independent of the number of sends,
//!   provided readers occasionally call [`Reader::recv`] (each `recv`, and each
//!   reader drop, runs a compaction sweep over the whole chain).
//!
//! # Structure
//!
//! The chain is a forward-linked list of nodes, oldest to newest. Each node holds
//! a payload `(value, next)` behind a single `RwLock`, so that walkers snapshot a
//! consistent pair and compactors mutate both atomically, plus an atomic `holders`
//! count of the readers currently *pinned* at the node. A reader pinned at a node
//! has folded every atom up to and including that node, and resumes from its `next`.
//!
//! The chain object holds a `newest` pointer (where the writer appends or merges)
//! and an `oldest` pointer (where compaction sweeps begin). Nodes strictly before
//! `oldest` are unreachable and reclaimed by `Arc` reference counting: forward links
//! mean old nodes are kept alive only by `oldest`, by reader pins, and by their
//! predecessors' `next` pointers, so abandoning a prefix frees it.
//!
//! # Writer fast path
//!
//! `send` locks the newest pointer and the newest node's payload; if the node's
//! `holders` is zero it merges the value in place (zero allocation — the common
//! case when no reader has just caught up); otherwise it allocates a new node,
//! links it from the old newest, and swaps the newest pointer.
//!
//! # Compaction rule
//!
//! Node `a` may absorb its successor `b` (merging `b`'s value into `a` and setting
//! `a.next = b.next`) iff **both** `a.holders == 0` and `b.holders == 0`, and `b`
//! is not the newest node (the writer merges into the newest node instead). The
//! `holders` checks are made under both payload write locks, which excludes
//! concurrent pinning (pins are taken under a payload read lock, or under the
//! newest-pointer lock for the newest node).
//!
//! Safety argument:
//! - *No lost atoms*: a value only moves backwards, from `b` into its unique live
//!   predecessor `a`. Any reader that has not folded `b` is pinned strictly before
//!   `b` (pinned exactly at `a` is excluded by `a.holders == 0`; mid-walk readers
//!   pin hand-over-hand, so their fold frontier is always a pinned node), and every
//!   path from a pin before `a` to `b` passes through `a`, where the value now lives.
//! - *No double folds*: a reader that has folded `b` is pinned at or after `b` and
//!   never revisits `a`; readers pinned exactly at `b` are excluded by
//!   `b.holders == 0`.
//! - *Pins never dangle*: pinned nodes are never absorbed nor bypassed, so a pin's
//!   `next` always leads into the live chain.
//!
//! Note: this rule is deliberately stronger than "absorb iff `a.holders == 0`"
//! (bypassing pinned successors). Bypassing a pinned node `p` freezes `p.next` as a
//! side entrance into the chain; a later absorption of `*p.next` into a node behind
//! `p` would then move an unfolded value behind the pinned reader's fold frontier,
//! losing it. Restricting absorption to unpinned pairs removes the side entrances.
//!
//! Because pinned nodes are never bypassed, a sweep from a reader's own pin cannot
//! compact the region behind it. Instead, each `recv` (and each reader drop) runs a
//! full sweep from the chain's `oldest` pointer: it first advances `oldest` past
//! unpinned head nodes (all readers are pinned at or after `oldest`, so an unpinned
//! head has been folded by everyone and can be abandoned to the reference counter),
//! and then merges every adjacent unpinned pair up to the newest node. This is the
//! "self-healing" of the design: no leaked adjacency survives any reader's `recv`.
//!
//! # Lock ordering
//!
//! 1. the chain's `oldest` mutex (held for the duration of a sweep, serializing
//!    sweeps against one another),
//! 2. the chain's `newest` mutex,
//! 3. node payload locks, in chain order (older before newer), at most two at once.
//!
//! Every code path acquires locks consistently with this order, so no cycle exists.
//! Walkers hold at most one payload read lock at a time; the writer holds the
//! `newest` mutex and the newest node's payload write lock; sweeps hold the
//! `oldest` mutex, briefly the `newest` mutex (released before node locks are
//! taken), and pairwise payload write locks in chain order.
//!
//! [`Mesh`] operations touch one chain at a time, so the per-chain order suffices.

use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A commutative monoid into which atoms can be merged.
///
/// Adjacent atoms in a chain may be merged (never split) before a reader observes
/// them. Commutativity is required because multiple chains and multiple readers
/// impose no cross-atom ordering: the merged result must not depend on the order
/// in which contributions are folded.
pub trait Chainable {
    /// Merges `other` into `self`.
    fn merge_from(&mut self, other: &Self);
}

macro_rules! implement_chainable_int {
    ($($index_type:ty,)*) => (
        $(
            impl Chainable for $index_type {
                #[inline] fn merge_from(&mut self, other: &Self) { *self = self.wrapping_add(*other); }
            }
        )*
    )
}

implement_chainable_int!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize,);

/// The payload of a node: its (possibly merged) value, and the next node.
///
/// Both live behind one `RwLock` so that walkers snapshot a consistent pair,
/// and so that compaction can mutate both atomically.
struct Payload<T> {
    /// The merged atoms recorded at this node, if any (`None` only for sentinels).
    value: Option<T>,
    /// The next (newer) node; `None` iff this node is the chain's newest.
    next: Option<Arc<Node<T>>>,
}

/// A node in the chain.
struct Node<T> {
    /// The number of readers currently pinned at this node.
    ///
    /// Incremented either under the chain's `newest` mutex (for the newest node)
    /// or under this node's payload read lock (mid-walk, hand-over-hand); the
    /// writer and the compactor re-check this count under the payload write lock,
    /// which excludes concurrent pinning.
    holders: AtomicUsize,
    /// The node's value and successor.
    payload: RwLock<Payload<T>>,
}

impl<T> Node<T> {
    fn new(value: Option<T>) -> Self {
        Self {
            holders: AtomicUsize::new(0),
            payload: RwLock::new(Payload { value, next: None }),
        }
    }
}

/// An RAII pin on a node: while held, the node will be neither absorbed nor
/// bypassed, nor merged into by the writer, and its `next` leads into the live
/// chain.
///
/// Construction does *not* increment `holders`: callers increment it inside the
/// appropriate critical section (see [`Node::holders`]) and then wrap the node.
/// Dropping the pin decrements `holders`.
struct Held<T> {
    node: Arc<Node<T>>,
}

impl<T> Held<T> {
    /// Wraps an already-incremented pin on `node`.
    fn pinned(node: Arc<Node<T>>) -> Self { Self { node } }
}

impl<T> Drop for Held<T> {
    fn drop(&mut self) {
        self.node.holders.fetch_sub(1, Ordering::SeqCst);
    }
}

/// State shared by the writer, the readers, and the chain handle.
struct ChainInner<T> {
    /// The newest node, where the writer appends or merges.
    newest: Mutex<Arc<Node<T>>>,
    /// The oldest retained node, where compaction sweeps begin.
    ///
    /// Invariant: every reader is pinned at or after `oldest`.
    oldest: Mutex<Arc<Node<T>>>,
}

impl<T: Chainable> ChainInner<T> {
    /// Compacts the chain: advances `oldest` past unpinned head nodes, and merges
    /// every adjacent pair of unpinned nodes older than the newest node.
    ///
    /// Holds the `oldest` mutex throughout, serializing sweeps; concurrent sweeps
    /// merging overlapping pairs would otherwise drain values into unlinked nodes.
    fn sweep(&self) {
        let mut oldest = self.oldest.lock().expect("lock poisoned");
        let newest = self.newest.lock().expect("lock poisoned").clone();

        // Advance `oldest` past unpinned head nodes: all reader pins are at or after
        // `*oldest`, so an unpinned head node has been folded by every reader, and no
        // reader can pin it anymore (readers pin only their pin's successors, or the
        // newest node). The abandoned prefix is reclaimed by `Arc` reference counts.
        while !Arc::ptr_eq(&oldest, &newest) && oldest.holders.load(Ordering::SeqCst) == 0 {
            let next = oldest.payload.read().expect("lock poisoned").next.clone();
            match next {
                Some(node) => *oldest = node,
                None => break,
            }
        }

        // Merge adjacent unpinned pairs in `[oldest, newest)`. The pair's `holders`
        // are checked under both payload write locks, excluding concurrent pinning.
        // We never absorb the (snapshot) newest node: the writer may be merging into
        // the true newest node, and the newest pointer must not be left dangling.
        // (If the true newest has moved past our snapshot, we conservatively treat
        // the snapshot as un-absorbable this sweep; the true newest is newer still.)
        let mut cursor = Arc::clone(&oldest);
        while !Arc::ptr_eq(&cursor, &newest) {
            let mut advance = None;
            {
                let mut payload_a = cursor.payload.write().expect("lock poisoned");
                let Some(node_b) = payload_a.next.clone() else { break };
                if !Arc::ptr_eq(&node_b, &newest) && cursor.holders.load(Ordering::SeqCst) == 0 {
                    let mut payload_b = node_b.payload.write().expect("lock poisoned");
                    if node_b.holders.load(Ordering::SeqCst) == 0 {
                        // Absorb `node_b` into `cursor`.
                        if let Some(value_b) = payload_b.value.take() {
                            match payload_a.value.as_mut() {
                                Some(value_a) => value_a.merge_from(&value_b),
                                None => payload_a.value = Some(value_b),
                            }
                        }
                        payload_a.next = payload_b.next.clone();
                        // Leave `cursor` in place: its new successor may also be absorbable.
                    }
                    else {
                        drop(payload_b);
                        advance = Some(node_b);
                    }
                }
                else {
                    advance = Some(node_b);
                }
            }
            if let Some(node) = advance { cursor = node; }
        }
    }

    /// The number of nodes currently retained, from `oldest` through `newest`.
    ///
    /// A diagnostic; `O(length)`, and approximate under concurrent activity.
    fn live_len(&self) -> usize {
        // Hold the `oldest` mutex to serialize against sweeps.
        let oldest = self.oldest.lock().expect("lock poisoned");
        let mut count = 1;
        let mut cursor = Arc::clone(&oldest);
        loop {
            let next = cursor.payload.read().expect("lock poisoned").next.clone();
            match next {
                Some(node) => { count += 1; cursor = node; }
                None => break,
            }
        }
        count
    }
}

/// A handle to a single-writer, multi-reader compacting chain.
///
/// Cloneable; used to create [`Reader`]s (and for diagnostics). The unique
/// [`Writer`] is created together with the chain by [`Chain::new`].
pub struct Chain<T> {
    inner: Arc<ChainInner<T>>,
}

impl<T> Clone for Chain<T> {
    fn clone(&self) -> Self { Self { inner: Arc::clone(&self.inner) } }
}

impl<T: Chainable> Chain<T> {
    /// Creates a new chain, returning its unique writer and a reader-factory handle.
    pub fn new() -> (Writer<T>, Chain<T>) {
        let sentinel = Arc::new(Node::new(None));
        let inner = Arc::new(ChainInner {
            newest: Mutex::new(Arc::clone(&sentinel)),
            oldest: Mutex::new(sentinel),
        });
        (Writer { inner: Arc::clone(&inner) }, Chain { inner })
    }

    /// Creates a new reader, which will observe exactly the atoms sent after this call.
    pub fn reader(&self) -> Reader<T> {
        // Pin the newest node under the `newest` mutex: this excludes the writer,
        // so atoms sent after we return go to nodes after our pin.
        let newest = self.inner.newest.lock().expect("lock poisoned");
        newest.holders.fetch_add(1, Ordering::SeqCst);
        let pin = Held::pinned(Arc::clone(&newest));
        drop(newest);
        Reader { inner: Arc::clone(&self.inner), pin: Some(pin) }
    }

    /// The number of nodes currently retained by the chain.
    ///
    /// A diagnostic; `O(length)`, and approximate under concurrent activity.
    pub fn live_len(&self) -> usize { self.inner.live_len() }
}

/// The unique writing endpoint of a [`Chain`].
pub struct Writer<T> {
    inner: Arc<ChainInner<T>>,
}

impl<T: Chainable> Writer<T> {
    /// Commits an atom: every reader registered before this call will fold `value`
    /// exactly once, possibly merged with adjacent atoms.
    pub fn send(&mut self, value: T) {
        let mut newest = self.inner.newest.lock().expect("lock poisoned");
        let node = Arc::clone(&newest);
        let mut payload = node.payload.write().expect("lock poisoned");
        // The `holders` check happens under the payload write lock: mid-walk pins
        // are taken under the payload read lock, and reader registration under the
        // `newest` mutex, so neither can race this check.
        if node.holders.load(Ordering::SeqCst) == 0 {
            // Fast path: no reader is pinned here, so none has folded this node's
            // value yet; merge in place without allocating.
            match payload.value.as_mut() {
                Some(current) => current.merge_from(&value),
                None => payload.value = Some(value),
            }
        }
        else {
            // Slow path: some reader has folded this node; append a new node.
            let appended = Arc::new(Node::new(Some(value)));
            payload.next = Some(Arc::clone(&appended));
            drop(payload);
            *newest = appended;
        }
    }
}

/// A reading endpoint of a [`Chain`].
///
/// Each reader folds, exactly once, every atom sent after its creation. Dropping a
/// reader releases its pin and compacts the chain, so departed readers leak nothing.
///
/// The `T: Chainable` bound on the type itself allows `Drop` to compact the chain.
pub struct Reader<T: Chainable> {
    inner: Arc<ChainInner<T>>,
    /// The reader's pin; `Some` except transiently during drop.
    pin: Option<Held<T>>,
}

impl<T: Chainable> Reader<T> {
    /// Folds every atom sent since the last call (or since creation) into `out`.
    pub fn recv(&mut self, out: &mut T) {
        self.recv_with(|value| out.merge_from(value));
    }

    /// Hands every atom sent since the last call (or since creation) to `logic`.
    ///
    /// Atoms are presented oldest first, though adjacent atoms may have been merged.
    /// `logic` is invoked while a chain lock is held, and must not call back into
    /// this chain.
    pub fn recv_with(&mut self, mut logic: impl FnMut(&T)) {
        // Compact the whole chain first: this is what keeps live state bounded,
        // even when this reader is the laggard everyone else has moved past.
        self.inner.sweep();
        // Walk forward from our pin, hand-over-hand: pin and fold each successor
        // before unpinning its predecessor, so compaction (which skips pinned nodes
        // and successors of pinned nodes) can never outrun our fold frontier.
        loop {
            let pinned = &self.pin.as_ref().expect("pin present outside of drop").node;
            let next = pinned.payload.read().expect("lock poisoned").next.clone();
            // A node has no successor iff it is the chain's newest: we are caught up.
            let Some(node) = next else { return };
            {
                // Pin and fold under the payload read lock: the writer re-checks
                // `holders` under the payload write lock before merging in place,
                // so we either fold a value the writer will not extend, or the
                // writer sees our pin and appends a fresh node (which we will
                // visit next, or on a later call).
                let payload = node.payload.read().expect("lock poisoned");
                node.holders.fetch_add(1, Ordering::SeqCst);
                if let Some(value) = payload.value.as_ref() {
                    logic(value);
                }
            }
            // Re-pin at the folded node; dropping the old pin decrements its count.
            self.pin = Some(Held::pinned(node));
        }
    }

    /// Indicates whether the reader has folded every atom sent so far.
    ///
    /// `O(1)`: compares the reader's pin against the chain's newest node, so
    /// polling threads can skip work. A `false` may be stale by the time it is
    /// observed, but a `true` is accurate as of the call.
    pub fn is_caught_up(&self) -> bool {
        let newest = self.inner.newest.lock().expect("lock poisoned");
        Arc::ptr_eq(&self.pin.as_ref().expect("pin present outside of drop").node, &newest)
    }
}

impl<T: Chainable> Drop for Reader<T> {
    fn drop(&mut self) {
        // Release the pin first, then heal the adjacency around it, so a departing
        // laggard (e.g. `drop_dataflow`) does not leave a pinned position behind.
        self.pin = None;
        self.inner.sweep();
    }
}

/// A bundle of `W` single-writer chains: one per writer, swept by every reader.
///
/// This is the shape the progress broadcaster would use: each worker holds the
/// [`Writer`] for its own chain and a [`MeshReader`] over all chains; network
/// threads are just more readers, and a receiving network thread is one more
/// writer.
pub struct Mesh<T> {
    chains: Vec<Chain<T>>,
}

impl<T: Chainable> Mesh<T> {
    /// Creates `writers` chains, returning the writer handles and the mesh.
    pub fn new(writers: usize) -> (Vec<Writer<T>>, Mesh<T>) {
        let mut handles = Vec::with_capacity(writers);
        let mut chains = Vec::with_capacity(writers);
        for _ in 0 .. writers {
            let (writer, chain) = Chain::new();
            handles.push(writer);
            chains.push(chain);
        }
        (handles, Mesh { chains })
    }

    /// Creates a reader that sweeps all chains, observing atoms sent after this call.
    pub fn reader(&self) -> MeshReader<T> {
        MeshReader { readers: self.chains.iter().map(Chain::reader).collect() }
    }

    /// The number of nodes currently retained by each chain (a diagnostic).
    pub fn live_lens(&self) -> Vec<usize> {
        self.chains.iter().map(Chain::live_len).collect()
    }
}

/// A reading endpoint over all chains of a [`Mesh`].
pub struct MeshReader<T: Chainable> {
    readers: Vec<Reader<T>>,
}

impl<T: Chainable> MeshReader<T> {
    /// Folds every atom sent on any chain since the last call into `out`.
    pub fn recv(&mut self, out: &mut T) {
        for reader in self.readers.iter_mut() {
            reader.recv(out);
        }
    }

    /// Hands every atom sent on any chain since the last call to `logic`.
    ///
    /// Atoms from one chain are presented oldest first; atoms from different
    /// chains are interleaved arbitrarily (whence the commutativity requirement).
    pub fn recv_with(&mut self, mut logic: impl FnMut(&T)) {
        for reader in self.readers.iter_mut() {
            reader.recv_with(&mut logic);
        }
    }

    /// Indicates whether the reader has folded every atom sent so far, on all chains.
    pub fn is_caught_up(&self) -> bool {
        self.readers.iter().all(Reader::is_caught_up)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Barrier};
    use super::{Chain, Mesh};

    /// A tiny deterministic PRNG (xorshift64*), to avoid a `rand` dependency.
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self { Rng(seed.max(1)) }
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545F4914F6CDD1D)
        }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
    }

    #[test]
    fn single_reader_observes_all() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut reader = chain.reader();
        for i in 1 ..= 100 { writer.send(i); }
        let mut total = 0;
        reader.recv(&mut total);
        assert_eq!(total, 5050);
        assert!(reader.is_caught_up());
    }

    #[test]
    fn multiple_readers_each_observe_all() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut readers = (0 .. 4).map(|_| chain.reader()).collect::<Vec<_>>();
        for i in 1 ..= 100 { writer.send(i); }
        for reader in readers.iter_mut() {
            let mut total = 0;
            reader.recv(&mut total);
            assert_eq!(total, 5050);
        }
    }

    #[test]
    fn late_reader_sees_only_subsequent() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut early = chain.reader();
        for i in 1 ..= 10 { writer.send(i); }
        let mut late = chain.reader();
        assert!(late.is_caught_up());
        for i in 1 ..= 10 { writer.send(100 * i); }
        let (mut early_total, mut late_total) = (0, 0);
        early.recv(&mut early_total);
        late.recv(&mut late_total);
        assert_eq!(early_total, 55 + 5500);
        assert_eq!(late_total, 5500);
    }

    #[test]
    fn repeated_recv_yields_only_new() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut reader = chain.reader();
        writer.send(3);
        let mut total = 0;
        reader.recv(&mut total);
        assert_eq!(total, 3);
        reader.recv(&mut total);
        assert_eq!(total, 3);
        writer.send(4);
        reader.recv(&mut total);
        assert_eq!(total, 7);
    }

    #[test]
    fn empty_recv_is_noop() {
        let (_writer, chain) = Chain::<u64>::new();
        let mut reader = chain.reader();
        assert!(reader.is_caught_up());
        let mut total = 0;
        let mut atoms = 0;
        reader.recv(&mut total);
        reader.recv_with(|_| atoms += 1);
        assert_eq!(total, 0);
        assert_eq!(atoms, 0);
    }

    /// With one laggard pinned at the start and one active reader keeping pace,
    /// the chain length must stay bounded by a small constant (#readers + 2).
    #[test]
    fn compaction_bounds_with_laggard() {
        let (mut writer, chain) = Chain::<u64>::new();
        let laggard = chain.reader();
        let mut active = chain.reader();
        let mut total = 0;
        for _ in 0 .. 10_000 {
            writer.send(1);
            active.recv(&mut total);
            assert!(chain.live_len() <= 4, "live_len {} exceeds bound", chain.live_len());
        }
        assert_eq!(total, 10_000);
        let mut laggard = laggard;
        let mut behind = 0;
        laggard.recv(&mut behind);
        assert_eq!(behind, 10_000);
    }

    /// Sums survive heavy sending with only occasional recvs.
    #[test]
    fn sums_preserved_under_heavy_send() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut reader = chain.reader();
        let mut total = 0;
        let mut expected = 0;
        let mut rng = Rng::new(0xDECAF);
        for i in 0 .. 100_000u64 {
            let value = rng.below(1000);
            expected += value;
            writer.send(value);
            if i % 1017 == 0 { reader.recv(&mut total); }
        }
        reader.recv(&mut total);
        assert_eq!(total, expected);
    }

    /// Dropping a laggard releases its pin, and the chain compacts afterwards.
    #[test]
    fn reader_drop_releases_pin() {
        let (mut writer, chain) = Chain::<u64>::new();
        let laggard = chain.reader();
        let mut active = chain.reader();
        let mut total = 0;
        for _ in 0 .. 1000 {
            writer.send(1);
            active.recv(&mut total);
        }
        // The laggard's pin retains the prefix.
        assert!(chain.live_len() >= 3);
        drop(laggard);
        // The drop's sweep advances past the released pin and compacts; everything
        // behind the active reader's pin is reclaimed.
        assert!(chain.live_len() <= 2, "live_len {} after drop", chain.live_len());
        assert_eq!(total, 1000);
    }

    /// Once all readers have caught up (and pinned the newest node), further sends
    /// allocate exactly one new node and then merge into it in place: the chain is
    /// exactly the pinned caught-up node plus one accumulating newest node.
    #[test]
    fn in_place_merge_fast_path() {
        let (mut writer, chain) = Chain::<u64>::new();
        let mut readers = (0 .. 3).map(|_| chain.reader()).collect::<Vec<_>>();
        for i in 1 ..= 5 { writer.send(i); }
        // Two recvs each: the first folds and re-pins at the newest node; the
        // second's sweep advances `oldest` past the abandoned prefix.
        let mut totals = vec![0; readers.len()];
        for _ in 0 .. 2 {
            for (reader, total) in readers.iter_mut().zip(totals.iter_mut()) {
                reader.recv(total);
            }
        }
        assert_eq!(chain.live_len(), 1);
        // N sends with no recv: the first allocates (the newest node is pinned by
        // all readers); the rest merge in place into the new newest node.
        for _ in 0 .. 1000 { writer.send(1); }
        assert_eq!(chain.live_len(), 2);
        for (reader, total) in readers.iter_mut().zip(totals.iter_mut()) {
            reader.recv(total);
            assert_eq!(*total, 15 + 1000);
        }
    }

    #[test]
    fn mesh_readers_observe_all_writers() {
        let (mut writers, mesh) = Mesh::<u64>::new(3);
        let mut readers = (0 .. 2).map(|_| mesh.reader()).collect::<Vec<_>>();
        for (index, writer) in writers.iter_mut().enumerate() {
            for i in 1 ..= 10 { writer.send((index as u64 + 1) * i); }
        }
        for reader in readers.iter_mut() {
            let mut total = 0;
            reader.recv(&mut total);
            assert_eq!(total, 55 + 110 + 165);
            assert!(reader.is_caught_up());
        }
    }

    /// Randomized stress: `W` writer threads (via `Mesh`), `R` reader threads
    /// recv-ing at random intervals, one deliberate laggard that recvs rarely.
    /// At quiescent checkpoints each chain's length must stay within bounds,
    /// and every reader's final total must equal the sum of all atoms sent.
    #[test]
    fn stress_randomized() {

        const WRITERS: usize = 3;
        const ACTIVES: usize = 3;
        const READERS: usize = ACTIVES + 1;   // plus one laggard
        const PHASES: usize = 50;

        let (writers, mesh) = Mesh::<u64>::new(WRITERS);
        let readers = (0 .. READERS).map(|_| mesh.reader()).collect::<Vec<_>>();

        // Four barrier waits per phase: start, quiesce, checkpoint, done.
        let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));

        let mut threads = Vec::new();

        for (index, mut writer) in writers.into_iter().enumerate() {
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || -> u64 {
                let mut rng = Rng::new(0xC0FFEE + index as u64);
                let mut sent = 0;
                for _ in 0 .. PHASES {
                    barrier.wait();                         // start
                    for _ in 0 .. rng.below(200) {
                        let value = rng.below(100);
                        sent += value;
                        writer.send(value);
                        if rng.below(16) == 0 { std::thread::yield_now(); }
                    }
                    barrier.wait();                         // quiesce
                    barrier.wait();                         // checkpoint
                    barrier.wait();                         // done
                }
                sent
            }));
        }

        for (index, mut reader) in readers.into_iter().enumerate() {
            let barrier = Arc::clone(&barrier);
            let laggard = index == 0;
            threads.push(std::thread::spawn(move || -> u64 {
                let mut rng = Rng::new(0xBEEF + index as u64);
                let mut total = 0;
                for phase in 0 .. PHASES {
                    barrier.wait();                         // start
                    if !laggard {
                        // Recv at random moments while writers are sending.
                        for _ in 0 .. rng.below(4) {
                            std::thread::yield_now();
                            reader.recv(&mut total);
                        }
                    }
                    barrier.wait();                         // quiesce
                    // Drain and heal: two recvs leave each chain fully compacted.
                    // The laggard recvs rarely, pinning old positions for a while.
                    if !laggard {
                        reader.recv(&mut total);
                        reader.recv(&mut total);
                        assert!(reader.is_caught_up());
                    }
                    else if phase % 8 == 7 {
                        reader.recv(&mut total);
                    }
                    barrier.wait();                         // checkpoint
                    barrier.wait();                         // done
                }
                reader.recv(&mut total);                    // final drain
                total
            }));
        }

        // The main thread asserts chain lengths at each quiescent checkpoint.
        for _ in 0 .. PHASES {
            barrier.wait();                                 // start
            barrier.wait();                                 // quiesce
            barrier.wait();                                 // checkpoint
            for len in mesh.live_lens() {
                assert!(len <= READERS + 2, "live_len {} exceeds bound {}", len, READERS + 2);
            }
            barrier.wait();                                 // done
        }

        let outcomes: Vec<u64> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        let expected: u64 = outcomes[.. WRITERS].iter().sum();
        for total in outcomes[WRITERS ..].iter() {
            assert_eq!(*total, expected);
        }
    }
}
