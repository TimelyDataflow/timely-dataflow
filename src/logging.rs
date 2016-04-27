//! Traits, implementations, and macros related to logging timely events.

extern crate time;

use std::cell::RefCell;
use std::io::Write;
use std::fs::File;

use ::Data;

use timely_communication::Allocate;
use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;

use dataflow::scopes::root::Root;
use dataflow::operators::capture::{EventWriter, Event, EventPusher};

use abomonation::Abomonation;

static mut PRECISE_TIME_NS_DELTA: Option<i64> = None;

/// Returns the value of an high resolution performance counter, in nanoseconds, rebased to be
/// roughly comparable to an unix timestamp.
/// Useful for comparing and merging logs from different machines (precision is limited by the
/// precision of the wall clock base; clock skew effects should be taken into consideration).
#[inline(always)]
fn get_precise_time_ns() -> u64 {
    (time::precise_time_ns() as i64 - unsafe { PRECISE_TIME_NS_DELTA.unwrap() }) as u64
}

/// Logs `record` in `logger` if logging is enabled.
pub fn log<T: Logger>(logger: &'static ::std::thread::LocalKey<T>, record: T::Record) {
    if cfg!(feature = "logging") {
        logger.with(|x| x.log(record));
    }
}

/// Logging methods
pub trait Logger {
    /// The type of loggable record.
    type Record;
    /// Adds `record` to the log.
    fn log(&self, record: Self::Record);
    /// Called with some frequency; behavior unspecified.
    fn flush(&self);
}

/// Logs events to an underlying writer.
pub struct EventStreamLogger<T: Data, S: Write> {
    buffer: RefCell<Vec<(T,u64)>>,
    stream: RefCell<Option<EventWriter<Product<RootTimestamp, u64>, (T,u64), S>>>,
    last_time: RefCell<u64>,
}

impl<T: Data, S: Write> Logger for EventStreamLogger<T, S> {
    type Record = T;
    #[inline]
    fn log(&self, record: T) {
        self.buffer.borrow_mut().push((record, get_precise_time_ns()));
    }
    fn flush(&self) {
        // TODO : sends progress update even if nothing happened.
        // TODO : consider a minimum interval from prior update to
        // TODO : cut down on the amount on logging, at the expense 
        // TODO : of freshness on the side of the reader.
        if let Some(ref mut writer) = *self.stream.borrow_mut() {
            let time = get_precise_time_ns();
            if self.buffer.borrow().len() > 0 {
                writer.push(Event::Messages(RootTimestamp::new(*self.last_time.borrow()), self.buffer.borrow().clone()));
            }
            writer.push(Event::Progress(vec![(RootTimestamp::new(*self.last_time.borrow()),-1), (RootTimestamp::new(time), 1)]));
            *self.last_time.borrow_mut() = time;
            self.buffer.borrow_mut().clear();
        }
        else {
            panic!("logging file not initialized!!!!");
        }
    }
}

impl<T: Data, S: Write> EventStreamLogger<T, S> {
    fn new() -> EventStreamLogger<T, S> {
        EventStreamLogger {
            buffer: RefCell::new(Vec::new()),
            stream: RefCell::new(None),
            last_time: RefCell::new(0),
        }
    }
    fn set(&self, stream: S) {
        let mut stream = EventWriter::new(stream);
        stream.push(Event::Progress(vec![(RootTimestamp::new(0), 1)]));
        *self.stream.borrow_mut() = Some(stream);
    }
}

impl<T: Data, S: Write> Drop for EventStreamLogger<T, S> {
    fn drop(&mut self) {
        if let Some(ref mut writer) = *self.stream.borrow_mut() {
            if self.buffer.borrow().len() > 0 {
                writer.push(Event::Messages(RootTimestamp::new(*self.last_time.borrow()), self.buffer.borrow().clone()));
            }
            writer.push(Event::Progress(vec![(RootTimestamp::new(*self.last_time.borrow()),-1)]));
        }
        else {
            panic!("logging file not initialized!!!!");
        }
    }
}

/// Initializes logging; called as part of `Root` initialization.
pub fn initialize<A: Allocate>(root: &mut Root<A>) {

    OPERATES.with(|x| x.set(File::create(format!("logs/operates-{}.abom", root.index())).unwrap()));
    CHANNELS.with(|x| x.set(File::create(format!("logs/channels-{}.abom", root.index())).unwrap()));
    MESSAGES.with(|x| x.set(File::create(format!("logs/messages-{}.abom", root.index())).unwrap()));
    PROGRESS.with(|x| x.set(File::create(format!("logs/progress-{}.abom", root.index())).unwrap()));
    PUSH_PROGRESS.with(|x| x.set(File::create(format!("logs/push_progress-{}.abom", root.index())).unwrap()));
    SCHEDULE.with(|x| x.set(File::create(format!("logs/schedule-{}.abom", root.index())).unwrap()));
    GUARDED_MESSAGE.with(|x| x.set(File::create(format!("logs/guarded_message-{}.abom", root.index())).unwrap()));
    GUARDED_PROGRESS.with(|x| x.set(File::create(format!("logs/guarded_progress-{}.abom", root.index())).unwrap()));

    unsafe {
        PRECISE_TIME_NS_DELTA = Some({
            let wall_time = time::get_time();
            let wall_time_ns = wall_time.nsec as i64 + wall_time.sec * 1000000000;
            time::precise_time_ns() as i64 - wall_time_ns
        });
    }

}

/// Flushes logs; called by `Root::step`.
pub fn flush_logs() {
    OPERATES.with(|x| x.flush());
    CHANNELS.with(|x| x.flush());
    PROGRESS.with(|x| x.flush());
    PUSH_PROGRESS.with(|x| x.flush());
    MESSAGES.with(|x| x.flush());
    SCHEDULE.with(|x| x.flush());
    GUARDED_MESSAGE.with(|x| x.flush());
    GUARDED_PROGRESS.with(|x| x.flush());
}

thread_local!{
    /// Logs operator creation.
    pub static OPERATES: EventStreamLogger<OperatesEvent, File> = EventStreamLogger::new();
    /// Logs channel creation.
    pub static CHANNELS: EventStreamLogger<ChannelsEvent, File> = EventStreamLogger::new();
    /// Logs progress transmission.
    pub static PROGRESS: EventStreamLogger<ProgressEvent, File> = EventStreamLogger::new();
    /// Logs push_external_progress.
    pub static PUSH_PROGRESS: EventStreamLogger<PushProgressEvent, File> = EventStreamLogger::new();
    /// Logs message transmission.
    pub static MESSAGES: EventStreamLogger<MessagesEvent, File> = EventStreamLogger::new();
    /// Logs operator scheduling.
    pub static SCHEDULE: EventStreamLogger<ScheduleEvent, File> = EventStreamLogger::new();
    /// Logs delivery of message to an operator input.
    pub static GUARDED_MESSAGE: EventStreamLogger<bool, File> = EventStreamLogger::new();
    /// Logs delivery of notification to an operator.
    pub static GUARDED_PROGRESS: EventStreamLogger<bool, File> = EventStreamLogger::new();
}

#[derive(Debug, Clone)]
/// The creation of an `Operate` implementor.
pub struct OperatesEvent {
    /// Worker-unique identifier for the operator.
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// A helpful name.
    pub name: String,
}

unsafe_abomonate!(OperatesEvent : id, addr, name);

#[derive(Debug, Clone)]
/// The creation of a channel between operators.
pub struct ChannelsEvent {
    /// Worker-unique identifier for the channel
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub scope_addr: Vec<usize>,
    /// Source descriptor, indicating operator index and output port.
    pub source: (usize, usize),
    /// Target descriptor, indicating operator index and input port.
    pub target: (usize, usize),
}

unsafe_abomonate!(ChannelsEvent : id, scope_addr, source, target);

#[derive(Debug, Clone)]
/// Send or receive of progress information.
pub struct ProgressEvent {
    /// `true` if the event is a send, and `false` if it is a receive.
    pub is_send: bool,
    /// Source worker index.
    pub source: usize,
    /// Message sequence number.
    pub seq_no: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// List of message updates, containing Target descriptor, timestamp as string, and delta.
    pub messages: Vec<(usize, usize, String, i64)>,
    /// List of capability updates, containing Source descriptor, timestamp as string, and delta.
    pub internal: Vec<(usize, usize, String, i64)>,
}

unsafe_abomonate!(ProgressEvent : is_send, source, seq_no, addr, messages, internal);

#[derive(Debug, Clone)]
/// External progress pushed onto an operator
pub struct PushProgressEvent {
    /// Worker-unique operator identifier
    pub op_id: usize,
}

unsafe_abomonate!(PushProgressEvent: op_id);

#[derive(Debug, Clone)]
/// Message send or receive event
pub struct MessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: usize,
    /// Source worker index.
    pub source: usize,
    /// Target worker index.
    pub target: usize,
    /// Message sequence number.
    pub seq_no: usize,
    /// Number of typed records in the message.
    pub length: usize,
}

unsafe_abomonate!(MessagesEvent);

/// Records the starting and stopping of an operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartStop {
    /// Operator starts.
    Start,
    /// Operator stops; did it have any activity?
    Stop { 
        /// Did the operator perform non-trivial work.
        activity: bool 
    },
}

impl Abomonation for StartStop { }

#[test]
fn start_stop_abomonation_roundtrip() {
    use abomonation::{encode, decode};
    fn check(data: StartStop) -> () {
        let mut bytes = Vec::new();
        unsafe {
            encode(&data, &mut bytes).unwrap();
        }
        if let Some((result, remaining)) = unsafe { decode::<StartStop>(&mut bytes) } {
            assert!(result == &data);
            assert!(remaining.len() == 0);
        }
    }
    for data in vec![
        StartStop::Stop { activity: true },
        StartStop::Stop { activity: false },
        StartStop::Start,
    ] {
        check(data);
    }
}

#[derive(Debug, Clone)]
/// Operator start or stop.
pub struct ScheduleEvent {
    /// Worker-unique identifier for the operator, linkable to the identifiers in `OperatesEvent`.
    pub id: usize,
    /// `Start` if the operator is starting, `Stop` if it is stopping.
    /// activiy is true if it looks like some useful work was performed during this call (data was
    /// read or written, notifications were requested / delivered)
    pub start_stop: StartStop,
}

unsafe_abomonate!(ScheduleEvent : id, start_stop);
