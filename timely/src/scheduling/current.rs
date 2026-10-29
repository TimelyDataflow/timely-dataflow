//! The operator the current thread is scheduling.
//!
//! Timely records the identifier of each operator for the duration of its `schedule` call, the
//! same identifier that `ScheduleEvent` reports. Code that runs without a handle to the operator,
//! such as a global allocator attributing allocations, can read it with [`operator_id`].
//!
//! The identifier names an operator within one worker. Peer workers of a computation build the
//! same dataflows in the same order, so an identifier names the same operator on each of them,
//! but it means nothing beyond that computation. The value assumes one computation is stepped on
//! a thread at a time. A host that steps several workers of unrelated computations on one thread
//! has to tell them apart itself.
//!
//! The current operator is whoever is running, not whoever owns the data. An allocation made by
//! one operator and handed to another, such as a buffer pushed through `Pipeline`, is attributed
//! to the operator that made it. Work outside any operator, such as communication in
//! `Worker::step` and dataflow construction, has no current operator.
//!
//! Work that happens on behalf of an operator outside its `schedule` call, for example trace
//! maintenance driven by the host application, can claim that operator with [`enter`]. Hold the
//! guard in a scoped binding around that work, and never across a call to `Worker::step`.
//!
//! Reading and writing the value does not allocate where the platform provides native
//! thread-locals. Elsewhere, the first access on a thread may allocate.

use std::cell::Cell;
use std::marker::PhantomData;

thread_local! {
    // The identifier plus one, or zero outside any operator. Const-initialized and without
    // `Drop`, so reading it never allocates or registers a destructor, which a global allocator
    // relies on to avoid reentrancy.
    static CURRENT: Cell<usize> = const { Cell::new(0) };
}

/// The identifier of the innermost operator this thread is scheduling, if any.
#[inline]
pub fn operator_id() -> Option<usize> {
    CURRENT.with(|c| c.get().checked_sub(1))
}

/// Marks `id` as the operator this thread is scheduling until the returned guard drops.
///
/// Guards nest: dropping one restores the operator that was current when it was created. Guards
/// must drop in reverse creation order, which holding them in scoped bindings ensures.
#[inline]
pub fn enter(id: usize) -> Guard {
    Guard { previous: CURRENT.with(|c| c.replace(id + 1)), _not_send: PhantomData }
}

/// Restores the previously current operator on drop. See [`enter`].
///
/// A guard restores the thread it was created on, so it cannot move to another thread:
///
/// ```compile_fail
/// fn assert_send<T: Send>(_: T) {}
/// assert_send(timely::scheduling::current::enter(0));
/// ```
#[must_use = "the operator is current only until the guard drops"]
pub struct Guard {
    previous: usize,
    // Dropped on another thread, the guard would overwrite that thread's current operator.
    _not_send: PhantomData<*const ()>,
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        CURRENT.with(|c| c.set(self.previous));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guards_nest_and_restore() {
        assert_eq!(operator_id(), None);
        {
            let _outer = enter(3);
            assert_eq!(operator_id(), Some(3));
            {
                let _inner = enter(0);
                assert_eq!(operator_id(), Some(0));
            }
            assert_eq!(operator_id(), Some(3));
        }
        assert_eq!(operator_id(), None);
    }
}
