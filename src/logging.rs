//! Traits, implementations, and macros related to logging timely events.

/// Type alias for logging timely events.
pub type Logger = ::logging_core::Logger<TimelyEvent>;

use std::time::Duration;
use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;
use dataflow::operators::capture::{Event, EventPusher};

/// Logs events as a timely stream, with progress statements.
pub struct TimelyLogger<E, P> where P: EventPusher<Product<RootTimestamp, Duration>, (Duration, E)> {
    // None when the logging stream is closed
    time: Duration,
    event_pusher: P,
    _phantom: ::std::marker::PhantomData<E>,
}

impl<E, P> TimelyLogger<E, P> where P: EventPusher<Product<RootTimestamp, Duration>, (Duration, E)> {
    /// Creates a new batch logger.
    pub fn new(event_pusher: P) -> Self {
        TimelyLogger {
            time: Default::default(),
            event_pusher,
            _phantom: ::std::marker::PhantomData,
        }
    }
    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch(&mut self, time: &Duration, data: &mut Vec<(Duration, E)>) {
        let new_frontier = RootTimestamp::new(time.clone());
        let old_frontier = RootTimestamp::new(self.time.clone());
        self.event_pusher.push(Event::Messages(RootTimestamp::new(self.time), ::std::mem::replace(data, Vec::new())));
        self.event_pusher.push(Event::Progress(vec![(new_frontier, 1), (old_frontier, -1)]));
        self.time = time.clone();
    }
}
impl<E, P> Drop for TimelyLogger<E, P> where P: EventPusher<Product<RootTimestamp, Duration>, (Duration, E)> {
    fn drop(&mut self) {
        self.event_pusher.push(Event::Progress(vec![(RootTimestamp::new(self.time), -1)]));
    }
}

#[derive(Abomonation, Debug, Clone)]
/// The creation of an `Operate` implementor.
pub struct OperatesEvent {
    /// Worker-unique identifier for the operator.
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// A helpful name.
    pub name: String,
}

#[derive(Abomonation, Debug, Clone)]
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

#[derive(Abomonation, Debug, Clone)]
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

#[derive(Abomonation, Debug, Clone)]
/// External progress pushed onto an operator
pub struct PushProgressEvent {
    /// Worker-unique operator identifier
    pub op_id: usize,
}

#[derive(Abomonation, Debug, Clone)]
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

/// Records the starting and stopping of an operator.
#[derive(Abomonation, Debug, Clone, PartialEq, Eq)]
pub enum StartStop {
    /// Operator starts.
    Start,
    /// Operator stops; did it have any activity?
    Stop {
        /// Did the operator perform non-trivial work.
        activity: bool
    },
}

#[derive(Abomonation, Debug, Clone)]
/// Operator start or stop.
pub struct ScheduleEvent {
    /// Worker-unique identifier for the operator, linkable to the identifiers in `OperatesEvent`.
    pub id: usize,
    /// `Start` if the operator is starting, `Stop` if it is stopping.
    /// activity is true if it looks like some useful work was performed during this call (data was
    /// read or written, notifications were requested / delivered)
    pub start_stop: StartStop,
}

#[derive(Abomonation, Debug, Clone)]
/// Application-defined code start or stop
pub struct ApplicationEvent {
    /// Unique event type identifier
    pub id: usize,
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(Abomonation, Debug, Clone)]
/// Application-defined code start or stop
pub struct GuardedMessageEvent {
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(Abomonation, Debug, Clone)]
/// Application-defined code start or stop
pub struct GuardedProgressEvent {
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
/// Identifier of the worker that generated a log line
pub struct TimelySetup {
    /// Worker index
    pub index: usize,
}

#[derive(Abomonation, Debug, Clone)]
/// Kind of communication channel
pub enum CommChannelKind {
    /// Communication channel carrying progress information
    Progress,
    /// Communication channel carrying data
    Data,
}

#[derive(Abomonation, Debug, Clone)]
/// Event on a communication channel
pub struct CommChannelsEvent {
    /// Communication channel identifier
    pub comm_channel: Option<usize>,
    /// Kind of communication channel (progress / data)
    pub comm_channel_kind: CommChannelKind,
}

#[derive(Abomonation, Debug, Clone)]
/// Input logic start/stop
pub struct InputEvent {
    /// True when activity begins, false when it stops
    pub start_stop: StartStop,
}

#[derive(Debug, Clone, Abomonation)]
/// An event in a timely worker
pub enum TimelyEvent {
    /// Operator creation.
    /*  0 */ Operates(OperatesEvent),
    /// Channel creation.
    /*  1 */ Channels(ChannelsEvent),
    /// Progress message send or receive.
    /*  2 */ Progress(ProgressEvent),
    /// Progress propagation (reasoning).
    /*  3 */ PushProgress(PushProgressEvent),
    /// Message send or receive.
    /*  4 */ Messages(MessagesEvent),
    /// Operator start or stop.
    /*  5 */ Schedule(ScheduleEvent),
    /// No clue.
    /*  6 */ Application(ApplicationEvent),
    /// Per-message computation.
    /*  7 */ GuardedMessage(GuardedMessageEvent),
    /// Per-notification computation.
    /*  8 */ GuardedProgress(GuardedProgressEvent),
    /// Communication channel event.
    /*  9 */ CommChannels(CommChannelsEvent),
    /// Input event.
    /* 10 */ Input(InputEvent),
}

impl From<OperatesEvent> for TimelyEvent {
    fn from(v: OperatesEvent) -> TimelyEvent { TimelyEvent::Operates(v) }
}

impl From<ChannelsEvent> for TimelyEvent {
    fn from(v: ChannelsEvent) -> TimelyEvent { TimelyEvent::Channels(v) }
}

impl From<ProgressEvent> for TimelyEvent {
    fn from(v: ProgressEvent) -> TimelyEvent { TimelyEvent::Progress(v) }
}

impl From<PushProgressEvent> for TimelyEvent {
    fn from(v: PushProgressEvent) -> TimelyEvent { TimelyEvent::PushProgress(v) }
}

impl From<MessagesEvent> for TimelyEvent {
    fn from(v: MessagesEvent) -> TimelyEvent { TimelyEvent::Messages(v) }
}

impl From<ScheduleEvent> for TimelyEvent {
    fn from(v: ScheduleEvent) -> TimelyEvent { TimelyEvent::Schedule(v) }
}

impl From<ApplicationEvent> for TimelyEvent {
    fn from(v: ApplicationEvent) -> TimelyEvent { TimelyEvent::Application(v) }
}

impl From<GuardedMessageEvent> for TimelyEvent {
    fn from(v: GuardedMessageEvent) -> TimelyEvent { TimelyEvent::GuardedMessage(v) }
}

impl From<GuardedProgressEvent> for TimelyEvent {
    fn from(v: GuardedProgressEvent) -> TimelyEvent { TimelyEvent::GuardedProgress(v) }
}

impl From<CommChannelsEvent> for TimelyEvent {
    fn from(v: CommChannelsEvent) -> TimelyEvent { TimelyEvent::CommChannels(v) }
}

impl From<InputEvent> for TimelyEvent {
    fn from(v: InputEvent) -> TimelyEvent { TimelyEvent::Input(v) }
}
