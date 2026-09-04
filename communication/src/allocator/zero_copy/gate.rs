//! Receive-side admission of framed messages.
//!
//! A [`Gate`] sits between the byte queues an allocator physically receives from and the
//! typed channels it surfaces messages into. By default it is open: bytes pass through
//! unchanged, and the gate costs one additional `Vec` move per `receive()`.
//!
//! A gate constructed holding splits received bytes into framed messages and files them by
//! source worker. A held message is physically present but logically undelivered: it is
//! not surfaced to its channel and raises no event, until the holder of the gate releases
//! it. Per-source FIFO order is preserved; the holder chooses only the interleaving
//! across sources, which is exactly the schedule space a real transport can produce.
//! This is the mechanism behind deterministic simulation of multi-worker computations.
//!
//! Messages a worker sends to itself are never held. Real allocators surface them at the
//! next `receive()`, and holding them would admit schedules no deployment can produce.

use std::collections::VecDeque;

use timely_bytes::arc::Bytes;

use crate::networking::MessageHeader;
use super::bytes_exchange::{BytesPull, MergeQueue};

/// Admission control over bytes received from peers.
pub struct Gate {
    /// The owning worker's index, whose own messages are never held.
    index: usize,
    /// Physical sources of bytes. Each may carry messages from several source workers.
    recvs: Vec<MergeQueue>,
    /// Whether received messages are held until released.
    holding: bool,
    /// Held messages, one framed message per entry, indexed by source worker.
    held: Vec<VecDeque<Bytes>>,
    /// Bytes admitted for delivery, each containing whole framed messages.
    admitted: Vec<Bytes>,
}

impl Gate {
    /// Creates a gate over `recvs`, for worker `index` of `peers`, holding if `holding`.
    pub fn new(index: usize, peers: usize, recvs: Vec<MergeQueue>, holding: bool) -> Self {
        Gate {
            index,
            recvs,
            holding,
            held: (0 .. peers).map(|_| VecDeque::new()).collect(),
            admitted: Vec::new(),
        }
    }

    /// Drains the physical sources, admitting or holding what they contain.
    ///
    /// The allocator calls this from `receive()`. A driver may also call it, to see
    /// what has arrived without stepping the worker.
    pub fn fetch(&mut self) {
        if !self.holding {
            for recv in self.recvs.iter_mut() {
                recv.drain_into(&mut self.admitted);
            }
        }
        else {
            let mut staged = Vec::new();
            for recv in self.recvs.iter_mut() {
                recv.drain_into(&mut staged);
            }
            for mut bytes in staged {
                // Received bytes contain whole framed messages; no splitting across allocations.
                while !bytes.is_empty() {
                    let header = MessageHeader::try_read(&bytes[..]).expect("failed to read full header!");
                    let message = bytes.extract_to(header.required_bytes());
                    if header.source == self.index {
                        self.admitted.push(message);
                    }
                    else {
                        self.held[header.source].push_back(message);
                    }
                }
            }
        }
    }

    /// Fetches, then moves all admitted bytes into `into`.
    pub fn receive(&mut self, into: &mut Vec<Bytes>) {
        self.fetch();
        into.append(&mut self.admitted);
    }

    /// The number of held messages from `source`, as of the last fetch.
    pub fn held(&self, source: usize) -> usize {
        self.held[source].len()
    }

    /// Releases up to `count` held messages from `source`, in FIFO order.
    ///
    /// Returns the number released, which is less than `count` if fewer are held.
    /// This clamping keeps any sequence of releases valid, which matters when
    /// shrinking failing schedules.
    pub fn release(&mut self, source: usize, count: usize) -> usize {
        let held = &mut self.held[source];
        let count = std::cmp::min(count, held.len());
        self.admitted.extend(held.drain(.. count));
        count
    }

    /// Releases all held messages, and returns the number released.
    pub fn release_all(&mut self) -> usize {
        (0 .. self.held.len()).map(|source| self.release(source, usize::MAX)).sum()
    }
}
