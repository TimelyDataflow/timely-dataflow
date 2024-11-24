//! A type that can unpark specific threads.

use std::thread::Thread;

/// Can unpark a specific thread.
#[derive(Clone)]
pub struct Buzzer {
    thread: Thread,
}

impl Default for Buzzer {
    fn default() -> Self { Self { thread: std::thread::current() } }
}

impl Buzzer {
    /// Unparks the target thread.
    pub fn buzz(&self) {
        self.thread.unpark()
    }
}