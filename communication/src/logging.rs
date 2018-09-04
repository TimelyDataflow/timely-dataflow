//! Types and traits related to logging.

use std::rc::Rc;
use std::cell::RefCell;

static mut PRECISE_TIME_NS_DELTA: Option<i64> = None;

/// Returns the value of an high resolution performance counter, in nanoseconds, rebased to be
/// roughly comparable to an unix timestamp.
/// Useful for comparing and merging logs from different machines (precision is limited by the
/// precision of the wall clock base; clock skew effects should be taken into consideration).
#[inline(always)]
pub fn get_precise_time_ns() -> u64 {
    let delta = unsafe {
        *PRECISE_TIME_NS_DELTA.get_or_insert_with(|| {
            let wall_time = ::time::get_time();
            let wall_time_ns = wall_time.nsec as i64 + wall_time.sec * 1000000000;
            ::time::precise_time_ns() as i64 - wall_time_ns
        })
    };
    (::time::precise_time_ns() as i64 - delta) as u64
}

const BUFFERING_LOGGER_CAPACITY: usize = 1024;

/// Either log data or and end marker.
pub enum LoggerBatch<S: Clone, L: Clone> {
    /// Log data.
    Logs(Vec<(u64, S, L)>),
    /// End of log marker.
    End,
}

/// An active buffering logger.
pub struct ActiveBufferingLogger<S: Clone, L: Clone> {
    setup: S,
    buffer: Vec<(u64, S, L)>,
    pusher: Box<Fn(LoggerBatch<S, L>)->()>,
}

impl<S: Clone, L: Clone> ActiveBufferingLogger<S, L> {
    /// Adds an element to the log.
    pub fn log(&mut self, l: L) {
        let ts = get_precise_time_ns();
        self.buffer.push((ts, self.setup.clone(), l));
        if self.buffer.len() >= BUFFERING_LOGGER_CAPACITY {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let buf = ::std::mem::replace(&mut self.buffer, Vec::new());
        (self.pusher)(LoggerBatch::Logs(buf));
    }
}

/// A possibly inactive buffering logger.
pub struct BufferingLogger<S: Clone, L: Clone> {
    target: Option<RefCell<ActiveBufferingLogger<S, L>>>,
}

impl<S: Clone, L: Clone> BufferingLogger<S, L> {
    /// Creates a new active buffering logger.
    pub fn new(setup: S, pusher: Box<Fn(LoggerBatch<S, L>)->()>) -> Self {
        BufferingLogger {
            target: Some(RefCell::new(ActiveBufferingLogger {
                setup,
                buffer: Vec::with_capacity(BUFFERING_LOGGER_CAPACITY),
                pusher: pusher,
            })),
        }
    }
    /// Creates a new inactive buffering logger.
    pub fn new_inactive() -> Rc<BufferingLogger<S, L>> {
        Rc::new(BufferingLogger {
            target: None,
        })
    }
    /// Invokes the closure if active, and ignores it if not active.
    pub fn when_enabled<F: FnOnce(&mut ActiveBufferingLogger<S, L>)->()>(&self, f: F) {
        if let Some(ref logger) = self.target {
            f(&mut *logger.borrow_mut())
        }
    }
    /// Flushes the logs.
    pub fn flush(&self) {
        if let Some(ref logger) = self.target {
            logger.borrow_mut().flush();
        }
    }
}

impl<S: Clone, L: Clone> Drop for BufferingLogger<S, L> {
    fn drop(&mut self) {
        if let Some(ref logger) = self.target {
            self.flush();
            let ActiveBufferingLogger { ref pusher, .. } = *logger.borrow_mut();
            (pusher)(LoggerBatch::End);
        }
    }
}

/// A log writer for a communication thread.
pub type CommsLogger = Rc<BufferingLogger<CommsSetup, CommsEvent>>;

/// Configuration information about a communication thread.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CommsSetup {
    /// True when this is a send thread (or the receive thread).
    pub sender: bool,
    /// The process id of the thread.
    pub process: usize,
    /// The remote process id.
    pub remote: Option<usize>,
}

/// A communication event, observing a message.
#[derive(Abomonation, Debug, Clone)]
pub struct CommunicationEvent {
    /// true for send event, false for receive event
    pub is_send: bool,
    /// communication channel id
    pub comm_channel: usize,
    /// source worker id
    pub source: usize,
    /// target worker id
    pub target: usize,
    /// sequence number
    pub seqno: usize,
}

#[derive(Abomonation, Debug, Clone)]
/// Serialization
pub struct SerializationEvent {
    /// The sequence number of the message.
    pub seq_no: Option<usize>,
    /// True when this is the start of serialization.
    pub is_start: bool,
}

/// The types of communication events.
#[derive(Debug, Clone, Abomonation)]
pub enum CommsEvent {
    /// A communication event.
    /*  0 */ Communication(CommunicationEvent),
    /// A serialization event.
    /*  1 */ Serialization(SerializationEvent),
}

impl From<CommunicationEvent> for CommsEvent {
    fn from(v: CommunicationEvent) -> CommsEvent { CommsEvent::Communication(v) }
}

impl From<SerializationEvent> for CommsEvent {
    fn from(v: SerializationEvent) -> CommsEvent { CommsEvent::Serialization(v) }
}

