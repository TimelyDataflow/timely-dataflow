//! The operator the current thread is scheduling.
//!
//! Timely records the identifier of each operator for the duration of its `schedule` call, the
//! same identifier that `ScheduleEvent` reports. Code that runs without a handle to the operator,
//! such as a global allocator attributing allocations, can read it with [`operator`].
//!
//! Work that happens on behalf of an operator outside its `schedule` call, for example trace
//! maintenance driven by the host application, can claim that operator with [`enter`].

use std::cell::Cell;

thread_local! {
    // The identifier plus one, or zero outside any operator. Const-initialized and without
    // `Drop`, so reading it never allocates or registers a destructor, which a global allocator
    // relies on to avoid reentrancy.
    static CURRENT: Cell<usize> = const { Cell::new(0) };
}

/// The identifier of the innermost operator this thread is scheduling, if any.
#[inline]
pub fn operator() -> Option<usize> {
    CURRENT.with(|c| c.get().checked_sub(1))
}

/// Marks `id` as the operator this thread is scheduling until the returned guard drops.
///
/// Guards nest: dropping one restores the operator that was current when it was created. Guards
/// must drop in reverse creation order, which holding them in scoped bindings ensures.
#[inline]
pub fn enter(id: usize) -> Guard {
    Guard { previous: CURRENT.with(|c| c.replace(id + 1)) }
}

/// Restores the previously current operator on drop. See [`enter`].
#[must_use = "the operator is current only until the guard drops"]
pub struct Guard {
    previous: usize,
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
        assert_eq!(operator(), None);
        {
            let _outer = enter(3);
            assert_eq!(operator(), Some(3));
            {
                let _inner = enter(0);
                assert_eq!(operator(), Some(0));
            }
            assert_eq!(operator(), Some(3));
        }
        assert_eq!(operator(), None);
    }
}
