use std::ops::Deref;
use std::rc::Rc;
use std::cell::RefCell;
use progress::Timestamp;
use progress::count_map::CountMap;

pub struct Capability<T: Timestamp> {
    time: T,
    internal: Rc<RefCell<CountMap<T>>>,
}

impl<T: Timestamp> Capability<T> {
    #[inline]
    pub fn time(&self) -> T {
        self.time
    }

    #[inline]
    pub fn into_delayed(self, new_time: &T) -> Capability<T> {
        assert!(new_time >= &self.time);
        mint(*new_time, self.internal.clone())
    }
}

pub fn mint<T: Timestamp>(time: T, internal: Rc<RefCell<CountMap<T>>>) -> Capability<T> {
    internal.borrow_mut().update(&time, 1);
    Capability {
        time: time,
        internal: internal
    }
}

impl<T: Timestamp> Drop for Capability<T> {
    fn drop(&mut self) {
        self.internal.borrow_mut().update(&self.time, -1);
    }
}

impl<T: Timestamp> Clone for Capability<T> {
    fn clone(&self) -> Capability<T> {
        mint(self.time, self.internal.clone())
    }
}

impl<T: Timestamp> Deref for Capability<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.time
    }
}
