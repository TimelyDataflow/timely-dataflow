
use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;
use std::collections::HashMap;
use std::time::{Instant, Duration};

pub struct Registry {
    /// An instant common to all logging statements.
    time: Instant,
    /// A map from names to typed loggers.
    map: HashMap<String, Box<Any>>,
}

impl Registry {
    /// Binds a log name to an action on log event batches.
    ///
    /// This method also returns any pre-installed action, rather than overwriting it
    /// and pivoting the logging destination mid-stream. New loggers with this name will
    /// use the new destination, and existing loggers will use the old destination.
    ///
    /// The action should respond to a sequence of events with non-decreasing timestamps
    /// (Durations) and well as a timestamp that lower bounds the next event that could be
    /// seen (likely greater or equal to the timestamp of the last event). The end of a
    /// logging stream is indicated only by dropping the associated action, which can be
    /// accomplished with `remove` (or a call to insert, though this is not recommended).
    pub fn insert<T: 'static>(
        &mut self,
        name: String,
        action: Box<Fn(&Duration, &[(Duration, T)])>) -> Option<Box<Any>>
    {
        let logger = Logger::<T>::new(self.time.clone(), action);
        self.map.insert(name, Box::new(logger))
    }

    /// Removes a bound logger.
    ///
    /// This is intended primarily to close a logging stream and let the associated writer
    /// communicate that the stream is closed to any consumers. If a binding is not removed,
    /// then the stream cannot be complete as in principle anyone could acquire a handle to
    /// the logger and start further logging.
    pub fn remove(&mut self, name: &str) -> Option<Box<Any>> {
        self.map.remove(name)
    }

    /// Retrieves a shared logger, if one has been inserted.
    pub fn get<T: 'static>(&self, name: &str) -> Option<Logger<T>> {
        self.map
            .get(name)
            .and_then(|entry| entry.downcast_ref::<Logger<T>>())
            .map(|x| (*x).clone())
    }

    /// Creates a new logger registry.
    pub fn new(time: Instant) -> Self {
        Registry {
            time,
            map: HashMap::new(),
        }
    }
}

/// A buffering logger.
pub struct Logger<T> {
    time:   Instant,                                    // common instant used for all loggers.
    action: Rc<Box<Fn(&Duration, &[(Duration, T)])>>,   // action to take on full log buffers.
    buffer: Rc<RefCell<Vec<(Duration, T)>>>,            // shared buffer; not obviously best design.
}

impl<T> Clone for Logger<T> {
    fn clone(&self) -> Self {
        Logger {
            time: self.time,
            action: self.action.clone(),
            buffer: self.buffer.clone(),
        }
    }
}

impl<T> Logger<T> {
    /// Allocates a new shareable logger bound to a write destination.
    pub fn new(time: Instant, action: Box<Fn(&Duration, &[(Duration, T)])>) -> Self {
        Logger {
            time,
            action: Rc::new(action),
            buffer: Rc::new(RefCell::new(Vec::with_capacity(1024))),
        }
    }

    /// Logs an event.
    ///
    /// The event has its timestamp recorded at the moment of logging, but it may be delayed
    /// due to buffering. It will be written when the logger is next flushed, either due to
    /// the buffer reaching capacity or a direct call to flush.
    ///
    /// This implementation borrows a shared (but thread-local) buffer of log events, to ensure
    /// that the `action` only sees one stream of events with increasing timestamps. This may
    /// have a cost that we don't entirely understand.
    pub fn log(&self, event: T) {
        let mut buffer = self.buffer.borrow_mut();
        buffer.push((self.time.elapsed(), event));
        if buffer.len() == buffer.capacity() {
            // Would call `self.flush()`, but for `RefCell` panic.
            (self.action)(&self.time.elapsed(), &buffer[..]);
            buffer.clear();
        }
    }

    /// Flushes logged messages and communicates the new minimal timestamp.
    pub fn flush(&self) {
        let mut buffer = self.buffer.borrow_mut();
        (self.action)(&self.time.elapsed(), &buffer[..]);
        buffer.clear();
    }
}