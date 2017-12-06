//! Simple timely_communication logging

extern crate time;
#[macro_use] extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

use abomonation::Abomonation;

use std::rc::Rc;
use std::cell::RefCell;

pub struct StructMapWriter;

static mut PRECISE_TIME_NS_DELTA: Option<i64> = None;

/// Returns the value of an high resolution performance counter, in nanoseconds, rebased to be
/// roughly comparable to an unix timestamp.
/// Useful for comparing and merging logs from different machines (precision is limited by the
/// precision of the wall clock base; clock skew effects should be taken into consideration).
#[inline(always)]
pub fn get_precise_time_ns() -> u64 {
    let delta = unsafe {
        *PRECISE_TIME_NS_DELTA.get_or_insert_with(|| {
            let wall_time = time::get_time();
            let wall_time_ns = wall_time.nsec as i64 + wall_time.sec * 1000000000;
            time::precise_time_ns() as i64 - wall_time_ns
        })
    };
    (time::precise_time_ns() as i64 - delta) as u64
}

/// Logging methods
pub trait Logger {
    /// The type of loggable record.
    type Record;
    /// Adds `record` to the log.
    fn log(&self, record: Self::Record);
}

#[derive(Debug, Clone)]
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

unsafe_abomonate!(CommunicationEvent : is_send, comm_channel, source, target, seqno);

#[derive(Debug, Clone)]
/// Serialization
pub struct SerializationEvent {
    pub seq_no: Option<usize>,
    pub is_start: bool,
}

unsafe_abomonate!(SerializationEvent : seq_no, is_start);

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
    /// Communication channel identifier
    pub comm_channel: Option<usize>,
    /// Message sequence number.
    pub seq_no: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// List of message updates, containing Target descriptor, timestamp as string, and delta.
    pub messages: Vec<(usize, usize, String, i64)>,
    /// List of capability updates, containing Source descriptor, timestamp as string, and delta.
    pub internal: Vec<(usize, usize, String, i64)>,
}

unsafe_abomonate!(ProgressEvent : is_send, source, comm_channel, seq_no, addr, messages, internal);

#[derive(Debug, Clone)]
/// External progress pushed onto an operator
pub struct PushProgressEvent {
    /// Worker-unique operator identifier
    pub op_id: usize,
}

unsafe_abomonate!(PushProgressEvent : op_id);

#[derive(Debug, Clone)]
/// Message send or receive event
pub struct MessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: usize,
    /// Communication channel identifier
    pub comm_channel: Option<usize>,
    /// Source worker index.
    pub source: usize,
    /// Target worker index.
    pub target: usize,
    /// Message sequence number.
    pub seq_no: usize,
    /// Number of typed records in the message.
    pub length: usize,
}

unsafe_abomonate!(MessagesEvent : is_send, channel, comm_channel, source, target, seq_no, length);

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

unsafe_abomonate!(StartStop);

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

#[derive(Debug, Clone)]
/// Application-defined code startor stop
pub struct ApplicationEvent {
    /// Unique event type identifier
    pub id: usize,
    /// True when activity begins, false when it stops 
    pub is_start: bool,
}

unsafe_abomonate!(ApplicationEvent : id, is_start);

#[derive(Debug, Clone)]
/// Application-defined code startor stop
pub struct GuardedMessageEvent {
    /// True when activity begins, false when it stops 
    pub is_start: bool,
}

unsafe_abomonate!(GuardedMessageEvent : is_start);

#[derive(Debug, Clone)]
/// Application-defined code startor stop
pub struct GuardedProgressEvent {
    /// True when activity begins, false when it stops 
    pub is_start: bool,
}

unsafe_abomonate!(GuardedProgressEvent : is_start);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct EventsSetup {
    pub index: usize,
}

unsafe_abomonate!(EventsSetup : index);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CommsSetup {
    pub sender: bool,
    pub process: usize,
    pub remote: Option<usize>,
}

unsafe_abomonate!(CommsSetup : sender, process, remote);

#[derive(Debug, Clone)]
pub enum CommChannelKind {
    Progress,
    Data,
}

unsafe_abomonate!(CommChannelKind);

#[derive(Debug, Clone)]
pub struct CommChannelsEvent {
    pub comm_channel: Option<usize>,
    pub comm_channel_kind: CommChannelKind,
}

unsafe_abomonate!(CommChannelsEvent : comm_channel, comm_channel_kind);

#[derive(Debug, Clone)]
pub struct InputEvent {
    pub start_stop: StartStop,
}

unsafe_abomonate!(InputEvent : start_stop);

#[derive(Debug, Clone, Abomonation)]
pub enum Event {
    /*  0 */ Operates(OperatesEvent),
    /*  1 */ Channels(ChannelsEvent),
    /*  2 */ Progress(ProgressEvent),
    /*  3 */ PushProgress(PushProgressEvent),
    /*  4 */ Messages(MessagesEvent),
    /*  5 */ Schedule(ScheduleEvent),
    /*  6 */ Application(ApplicationEvent),
    /*  7 */ GuardedMessage(GuardedMessageEvent),
    /*  8 */ GuardedProgress(GuardedProgressEvent),
    /*  9 */ CommChannels(CommChannelsEvent),
    /* 10 */ Input(InputEvent),
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

impl From<OperatesEvent> for Event {
    fn from(v: OperatesEvent) -> Event { Event::Operates(v) }
}

impl From<ChannelsEvent> for Event {
    fn from(v: ChannelsEvent) -> Event { Event::Channels(v) }
}

impl From<ProgressEvent> for Event {
    fn from(v: ProgressEvent) -> Event { Event::Progress(v) }
}

impl From<PushProgressEvent> for Event {
    fn from(v: PushProgressEvent) -> Event { Event::PushProgress(v) }
}

impl From<MessagesEvent> for Event {
    fn from(v: MessagesEvent) -> Event { Event::Messages(v) }
}

impl From<ScheduleEvent> for Event {
    fn from(v: ScheduleEvent) -> Event { Event::Schedule(v) }
}

impl From<ApplicationEvent> for Event {
    fn from(v: ApplicationEvent) -> Event { Event::Application(v) }
}

impl From<GuardedMessageEvent> for Event {
    fn from(v: GuardedMessageEvent) -> Event { Event::GuardedMessage(v) }
}

impl From<GuardedProgressEvent> for Event {
    fn from(v: GuardedProgressEvent) -> Event { Event::GuardedProgress(v) }
}

impl From<CommChannelsEvent> for Event {
    fn from(v: CommChannelsEvent) -> Event { Event::CommChannels(v) }
}

impl From<InputEvent> for Event {
    fn from(v: InputEvent) -> Event { Event::Input(v) }
}

const BUFFERING_LOGGER_CAPACITY: usize = 1024;

pub enum LoggerBatch<'a, S: Clone+'a, L: Clone+'a> {
    Logs(&'a Vec<(u64, S, L)>),
    End,
}

enum BufferingLoggerInternal<S: Clone, L: Clone> {
    Active {
        setup: S,
        buffer: RefCell<Vec<(u64, S, L)>>,
        pushers: RefCell<Box<Fn(LoggerBatch<S, L>)->()>>,
    },
    Inactive
}

pub struct BufferingLogger<S: Clone, L: Clone> {
    internal: BufferingLoggerInternal<S, L>,
}

impl<S: Clone, L: Clone> BufferingLogger<S, L> {
    pub fn new(setup: S, pushers: Box<Fn(LoggerBatch<S, L>)->()>) -> Self {
        BufferingLogger {
            internal: BufferingLoggerInternal::Active {
                setup: setup,
                buffer: RefCell::new(Vec::with_capacity(BUFFERING_LOGGER_CAPACITY)),
                pushers: RefCell::new(pushers),
            },
        }
    }

    pub fn new_inactive() -> Rc<BufferingLogger<EventsSetup, Event>> {
        Rc::new(BufferingLogger {
            internal: BufferingLoggerInternal::Inactive,
        })
    }

    pub fn log(&self, l: L) {
        match self.internal {
            BufferingLoggerInternal::Active { ref setup, ref buffer, ref pushers } => {
                let ts = get_precise_time_ns();
                let mut buf = buffer.borrow_mut();
                buf.push((ts, setup.clone(), l));
                if buf.len() >= BUFFERING_LOGGER_CAPACITY {
                    (*pushers.borrow_mut())(LoggerBatch::Logs(&buf));
                    buf.clear();
                }
            },
            BufferingLoggerInternal::Inactive => {},
        }
    }
}

impl<S: Clone, L: Clone> Drop for BufferingLogger<S, L> {
    fn drop(&mut self) {
        match self.internal {
            BufferingLoggerInternal::Active { ref buffer, ref pushers, .. } => {
                let mut buf = buffer.borrow_mut();
                if buf.len() > 0 {
                    (*pushers.borrow_mut())(LoggerBatch::Logs(&buf));
                }
                (*pushers.borrow_mut())(LoggerBatch::End);
            },
            BufferingLoggerInternal::Inactive => {},
        }
    }
}
