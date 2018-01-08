//! Traits, implementations, and macros related to logging timely events.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;

use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;
use ::progress::frontier::MutableAntichain;

use dataflow::operators::capture::{Event, EventPusher};

use timely_communication::logging::{BufferingLogger, LoggerBatch, CommsEvent, CommsSetup};

type LogMessage = (u64, TimelySetup, TimelyEvent);
type CommsMessage = (u64, CommsSetup, CommsEvent);

/// A log writer.
pub type Logger = Rc<BufferingLogger<TimelySetup, TimelyEvent>>;

/// A log writer that does not log anything.
pub fn new_inactive_logger() -> Logger {
    BufferingLogger::<TimelySetup, TimelyEvent>::new_inactive()
}

/// Shared wrapper for log writer constructors.
pub struct LoggerConfig {
    /// Log writer constructors.
    pub timely_logging: Arc<Fn(TimelySetup)->Rc<BufferingLogger<TimelySetup, TimelyEvent>>+Send+Sync>,
    /// Log writer constructors for communication.
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsSetup, CommsEvent>>+Send+Sync>,
}

impl LoggerConfig {
    /// Makes a new `LoggerConfig` wrapper from a `LogManager`.
    pub fn new<P1: 'static, P2: 'static, F1: 'static, F2: 'static>(
        timely_subscription: F1, communication_subscription: F2) -> Self where
        P1: EventPusher<Product<RootTimestamp, u64>, LogMessage> + Send,
        P2: EventPusher<Product<RootTimestamp, u64>, CommsMessage> + Send,
        F1: Fn(TimelySetup)->P1+Send+Sync,
        F2: Fn(CommsSetup)->P2+Send+Sync {

        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: TimelySetup| {
                let logger = RefCell::new(BatchLogger::new((timely_subscription)(events_setup)));
                Rc::new(BufferingLogger::new(events_setup, Box::new(move |data| logger.borrow_mut().publish_batch(data))))
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                let logger = RefCell::new(BatchLogger::new((communication_subscription)(comms_setup)));
                Rc::new(BufferingLogger::new(comms_setup, Box::new(move |data| logger.borrow_mut().publish_batch(data))))
            }),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        LoggerConfig {
            timely_logging: Arc::new(|_setup| BufferingLogger::new_inactive()),
            communication_logging: Arc::new(|_setup| BufferingLogger::new_inactive()),
        }
    }
}

struct BatchLogger<S, E, P> where P: EventPusher<Product<RootTimestamp, u64>, (u64, S, E)> {
    // None when the logging stream is closed
    frontier: Option<Product<RootTimestamp, u64>>,
    event_pusher: P,
    _s: ::std::marker::PhantomData<S>,
    _e: ::std::marker::PhantomData<E>,
}

impl<S, E, P> BatchLogger<S, E, P> where P: EventPusher<Product<RootTimestamp, u64>, (u64, S, E)> {
    fn new(event_pusher: P) -> Self {
        BatchLogger {
            frontier: Some(Default::default()),
            event_pusher,
            _s: ::std::marker::PhantomData,
            _e: ::std::marker::PhantomData,
        }
    }
}

impl<S: Clone, E: Clone, P> BatchLogger<S, E, P> where P: EventPusher<Product<RootTimestamp, u64>, (u64, S, E)> {
    pub fn publish_batch(&mut self, logger_batch: LoggerBatch<S, E>) -> () {
        match logger_batch {
            LoggerBatch::Logs(evs) => {
                if let Some(frontier) = self.frontier {
                    let &(last_ts, _, _) = evs.last().unwrap();
                    self.event_pusher.push(Event::Messages(frontier, evs));
                    let new_frontier = RootTimestamp::new(last_ts);
                    self.event_pusher.push(Event::Progress(vec![(new_frontier, 1), (frontier, -1)]));
                    self.frontier = Some(new_frontier);
                }
            },
            LoggerBatch::End => {
                if let Some(frontier) = self.frontier {
                    self.event_pusher.push(Event::Progress(vec![(frontier, -1)]));
                    self.frontier = None;
                }
            },
        }
    }
}

/// An EventPusher that supports dynamically adding new EventPushers.
///
/// The tee maintains the frontier as the stream of events passes by. When a new pusher
/// arrives it advances the frontier to the current value, and starts to forward events
/// to it as well.
pub struct EventPusherTee<T: ::order::PartialOrder+Ord+Default+Clone+'static, D: Clone> {
    frontier: MutableAntichain<T>,
    listeners: Vec<Box<EventPusher<T, D>+Send>>,
}

impl<T: ::order::PartialOrder+Ord+Default+Clone+'static, D: Clone> EventPusherTee<T, D> {
    /// Construct a new tee with no subscribers.
    pub fn new() -> Self {
        Self {
            frontier: Default::default(),
            listeners: Vec::new(),
        }
    }
    /// Subscribe to this tee.
    pub fn subscribe(&mut self, mut listener: Box<EventPusher<T, D>+Send>) {
        let mut changes = vec![(Default::default(), -1)];
        changes.extend(self.frontier.frontier().iter().map(|x| (x.clone(), 1)));
        listener.push(Event::Progress(changes));
        self.listeners.push(listener);
    }
}

impl<T: ::order::PartialOrder+Ord+Default+Clone, D: Clone> EventPusher<T, D> for EventPusherTee<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        // update the maintained frontier.
        if let &Event::Progress(ref updates) = &event {
            self.frontier.update_iter(updates.iter().cloned());
        }
        // present the event to each listener.
        for listener in self.listeners.iter_mut() {
            listener.push(event.clone());
        }
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
/// Application-defined code startor stop
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
#[allow(missing_docs)]
pub enum TimelyEvent {
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
