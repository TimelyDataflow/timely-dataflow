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

pub enum LoggerBatch<S: Clone, L: Clone> {
    Logs(Vec<(u64, S, L)>),
    End,
}

pub struct ActiveBufferingLogger<S: Clone, L: Clone> {
    setup: S,
    buffer: Vec<(u64, S, L)>,
    pusher: Box<Fn(LoggerBatch<S, L>)->()>,
}

impl<S: Clone, L: Clone> ActiveBufferingLogger<S, L> {
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

pub struct BufferingLogger<S: Clone, L: Clone> {
    target: Option<RefCell<ActiveBufferingLogger<S, L>>>,
}

impl<S: Clone, L: Clone> BufferingLogger<S, L> {
    pub fn new(setup: S, pusher: Box<Fn(LoggerBatch<S, L>)->()>) -> Self {
        BufferingLogger {
            target: Some(RefCell::new(ActiveBufferingLogger {
                setup,
                buffer: Vec::with_capacity(BUFFERING_LOGGER_CAPACITY),
                pusher: pusher,
            })),
        }
    }

    pub fn new_inactive() -> Rc<BufferingLogger<S, L>> {
        Rc::new(BufferingLogger {
            target: None,
        })
    }

    pub fn when_enabled<F: FnOnce(&mut ActiveBufferingLogger<S, L>)->()>(&self, f: F) {
        if let Some(ref logger) = self.target {
            f(&mut *logger.borrow_mut())
        }
    }

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

#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CommsSetup {
    pub sender: bool,
    pub process: usize,
    pub remote: Option<usize>,
}

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
    pub seq_no: Option<usize>,
    pub is_start: bool,
}

#[derive(Debug, Clone, Abomonation)]
pub enum CommsEvent {
    /*  0 */ Communication(CommunicationEvent),
    /*  1 */ Serialization(SerializationEvent),
}

impl From<CommunicationEvent> for CommsEvent {
    fn from(v: CommunicationEvent) -> CommsEvent { CommsEvent::Communication(v) }
}

impl From<SerializationEvent> for CommsEvent {
    fn from(v: SerializationEvent) -> CommsEvent { CommsEvent::Serialization(v) }
}

