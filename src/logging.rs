//! Traits, implementations, and macros related to logging timely events.

use std::io::Write;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;

use std::collections::HashMap;
use std::fmt::Debug;

use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;

use dataflow::operators::capture::{EventWriter, Event, EventPusher};

use abomonation::Abomonation;

use std::net::TcpStream;

use timely_logging::BufferingLogger;
use timely_logging::Event as LogEvent;
use timely_logging::EventsSetup;
use timely_logging::{CommsEvent, CommsSetup};
use timely_logging::LoggerBatch;

type LogMessage = (u64, EventsSetup, LogEvent);
type CommsMessage = (u64, CommsSetup, CommsEvent);

/// A log writer.
pub type Logger = Rc<BufferingLogger<EventsSetup, LogEvent>>;

/// A log writer that does not log anything.
pub fn new_inactive_logger() -> Logger {
    BufferingLogger::<(), ()>::new_inactive()
}

type EventPusherFactory<M> = Arc<Fn()->Box<EventPusher<Product<RootTimestamp, u64>, M>+Send>+Send+Sync>;

/// Manages the logging channel subscriptions and log pushers.
pub struct LogManager {
    // Keeps track of the subscribers of timely log streams for each worker (whose index
    // is tracked by EventsSetup)
    timely_logs: HashMap<
        EventsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>>>,
    // Keeps track of existing subscription requests to timely log streams. This lets us attach
    // subscribers to new log streams.
    timely_subscriptions:
        Vec<(Arc<Fn(&EventsSetup)->bool+Send+Sync>, EventPusherFactory<LogMessage>)>,
    // Keeps track of the subscribers of communication log streams for each comm thread
    // (whose identity is tracked by CommsSetup)
    communication_logs: HashMap<
        CommsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<CommsSetup, CommsEvent>>>>,
    // Keeps track of existing subscription requests to communication log streams. This lets us attach
    // subscribers to new log streams.
    communication_subscriptions:
        Vec<(Arc<Fn(&CommsSetup)->bool+Send+Sync>, EventPusherFactory<CommsMessage>)>,
}

impl LogManager {
    fn add_timely_subscription(&mut self,
                               filter: Arc<Fn(&EventsSetup)->bool+Send+Sync>,
                               pusher: EventPusherFactory<LogMessage>) {

        for (_, ref event_manager) in self.timely_logs.iter().filter(|&(ref setup, _)| filter(setup)) {
            let mut this_pusher = pusher();
            this_pusher.push(Event::Progress(vec![(Default::default(), -1)]));
            event_manager.lock().unwrap().subscribe(this_pusher);
        }
        self.timely_subscriptions.push((filter, pusher));
    }

    fn add_communication_subscription(&mut self,
                                      filter: Arc<Fn(&CommsSetup)->bool+Send+Sync>,
                                      pusher: EventPusherFactory<CommsMessage>) {

        for (_, ref event_manager) in self.communication_logs.iter().filter(|&(ref setup, _)| filter(setup)) {
            let mut this_pusher = pusher();
            this_pusher.push(Event::Progress(vec![(Default::default(), -1)]));
            event_manager.lock().unwrap().subscribe(this_pusher);
        }
        self.communication_subscriptions.push((filter, pusher));
    }
}

struct SharedEventWriter<T, D, W: Write> {
    inner: Arc<Mutex<EventWriter<T, D, W>>>,
}

impl<T, D, W: Write> Clone for SharedEventWriter<T, D, W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, D, W: Write> SharedEventWriter<T, D, W> {
    fn new(w: W) -> Self {
        SharedEventWriter {
            inner: Arc::new(Mutex::new(EventWriter::new(w))),
        }
    }
}

impl<T: Abomonation+Debug, D: Abomonation+Debug, W: Write> EventPusher<T, D> for SharedEventWriter<T, D, W> {
    fn push(&mut self, event: Event<T, D>) {
        let mut inner = self.inner.lock().expect("event pusher poisoned");
        inner.push(event)
    }
}

/// An handle to the LogManager that constructs a filter for the log records.
pub struct FilteredLogManager<S, E> {
    log_manager: Arc<Mutex<LogManager>>,
    filter: Arc<Fn(&S)->bool+Send+Sync>,
    _e: ::std::marker::PhantomData<E>,
}

impl FilteredLogManager<EventsSetup, LogEvent> {
    /// Send the selected timely events to one tcp socket per log sender.
    ///
    /// Destination can be set with the TIMELY_COMM_LOG_TARGET environment variable.
    pub fn to_tcp_sockets(&mut self) {
        let target: String = ::std::env::var("TIMELY_LOG_TARGET").expect("no $TIMELY_LOG_TARGET, e.g. 127.0.0.1:34254");

        // let writer = SharedEventWriter::new(writer);
        let pusher: EventPusherFactory<LogMessage>= Arc::new(move || {
            Box::new(EventWriter::new(TcpStream::connect(target.clone()).expect("failed to connect to logging destination")))
        });

        self.log_manager.lock().unwrap().add_timely_subscription(self.filter.clone(), pusher);
    }

    /// Send the selected timely events to a shared tcp socket.
    ///
    /// Destination can be set with the TIMELY_COMM_LOG_TARGET environment variable.
    pub fn to_shared_tcp_socket(&mut self) {
        let target: String = ::std::env::var("TIMELY_LOG_TARGET").expect("no $TIMELY_LOG_TARGET, e.g. 127.0.0.1:34254");
        let pusher = Box::new(SharedEventWriter::new(TcpStream::connect(target.clone()).expect("failed to connect to logging destination")));
        self.log_manager.lock().unwrap().add_timely_subscription(self.filter.clone(), Arc::new(move || pusher.clone()));
    }
}

impl FilteredLogManager<CommsSetup, CommsEvent> {
    /// Send the selected communication events to a tcp socket.
    ///
    /// Destination can be set with the TIMELY_COMM_LOG_TARGET environment variable.
    pub fn to_tcp_socket(&mut self) {
        let comm_target = ::std::env::var("TIMELY_COMM_LOG_TARGET").expect("no $TIMELY_COMM_LOG_TARGET, e.g. 127.0.0.1:34255");

        let pusher: EventPusherFactory<CommsMessage> = Arc::new(move || {
            Box::new(EventWriter::new(TcpStream::connect(comm_target.clone()).expect("failed to connect to logging destination")))
        });

        self.log_manager.lock().unwrap().add_communication_subscription(self.filter.clone(), pusher);
    }
}

impl LogManager {
    /// Constructs a new LogManager.
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(LogManager {
            timely_logs: HashMap::new(),
            timely_subscriptions: Vec::new(),
            communication_logs: HashMap::new(),
            communication_subscriptions: Vec::new(),
        }))
    }
}

/// Functions to construct log filters. 
pub trait LogFilter {
    /// Get a `FilteredLogManager` for all workers.
    fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent>;

    /// Get a `FilteredLogManager` for all communication threads.
    fn comms(&mut self) -> FilteredLogManager<CommsSetup, CommsEvent>;
}

impl LogFilter for Arc<Mutex<LogManager>> {
    #[inline] fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent> {
        FilteredLogManager {
            log_manager: self.clone(),
            filter: Arc::new(|_| true),
            _e: ::std::marker::PhantomData,
        }
    }
    #[inline] fn comms(&mut self) -> FilteredLogManager<CommsSetup, CommsEvent> {
        FilteredLogManager {
            log_manager: self.clone(),
            filter: Arc::new(|_| true),
            _e: ::std::marker::PhantomData,
        }
    }
}

/// Shared wrapper for log writer constructors.
pub struct LoggerConfig {
    /// Log writer constructors.
    pub timely_logging: Arc<Fn(EventsSetup)->Rc<BufferingLogger<EventsSetup, LogEvent>>+Send+Sync>,
    /// Log writer constructors for communication.
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsSetup, CommsEvent>>+Send+Sync>,
}

impl LoggerConfig {
    fn register_timely_logger(
        log_manager: &mut LogManager,
        events_setup: EventsSetup) -> Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>> = Arc::new(Mutex::new(Default::default()));
        log_manager.timely_logs.insert(events_setup, event_manager.clone());
        for pusher in log_manager.timely_subscriptions.iter().filter(|&&(ref f, _)| f(&events_setup)).map(|&(_, ref p)| p.clone()) {
            event_manager.lock().unwrap().subscribe(pusher());
        }
        println!("pushers: {}", event_manager.lock().unwrap().event_pushers.len());
        event_manager
    }

    fn register_comms_logger(
        log_manager: &mut LogManager,
        comms_setup: CommsSetup) -> Arc<Mutex<EventStreamSubscriptionManager<CommsSetup, CommsEvent>>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<CommsSetup, CommsEvent>>> = Arc::new(Mutex::new(Default::default()));
        log_manager.communication_logs.insert(comms_setup, event_manager.clone());
        for pusher in log_manager.communication_subscriptions.iter().filter(|&&(ref f, _)| f(&comms_setup)).map(|&(_, ref p)| p.clone()) {
            event_manager.lock().unwrap().subscribe(pusher());
        }
        event_manager
    }

    /// Makes a new `LoggerConfig` wrapper from a `LogManager`.
    pub fn new(log_manager: Arc<Mutex<LogManager>>) -> Self {
        let timely_logging_manager = log_manager.clone();
        let communication_logging_manager = log_manager;
        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: EventsSetup| {
                let subscription_manager = LoggerConfig::register_timely_logger(
                    &mut timely_logging_manager.lock().unwrap(), events_setup);
                Rc::new(BufferingLogger::new(events_setup, Box::new(move |data| {
                    subscription_manager.lock().expect("cannot lock mutex").publish_batch(data);
                })))
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                let subscription_manager = LoggerConfig::register_comms_logger(
                    &mut communication_logging_manager.lock().unwrap(), comms_setup);
                Rc::new(BufferingLogger::new(comms_setup, Box::new(move |data| {
                    subscription_manager.lock().expect("cannot lock mutex").publish_batch(data);
                })))
            }),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        LoggerConfig {
            timely_logging: Arc::new(|setup| Rc::new(BufferingLogger::new(setup, Box::new(|_| {})))),
            communication_logging: Arc::new(|setup| Rc::new(BufferingLogger::new(setup, Box::new(|_| {})))),
        }
    }
}

struct EventStreamSubscriptionManager<S, E> {
    // None when the logging stream is closed
    frontier: Option<Product<RootTimestamp, u64>>,
    event_pushers: Vec<Box<EventPusher<Product<RootTimestamp, u64>, (u64, S, E)>+Send>>,
}

impl<S, E> Default for EventStreamSubscriptionManager<S, E> {
    fn default() -> Self {
        EventStreamSubscriptionManager {
            frontier: Some(Default::default()),
            event_pushers: Vec::new(),
        }
    }
}

impl<S: Clone, E: Clone> EventStreamSubscriptionManager<S, E> {
    fn subscribe(&mut self, mut pusher: Box<EventPusher<Product<RootTimestamp, u64>, (u64, S, E)>+Send>) {
        if let Some(frontier) = self.frontier {
            // if this logging stream is open
            pusher.deref_mut().push(Event::Progress(vec![(frontier, 1)]));
        } else {
            eprintln!("logging: subscription to closed stream");
        }
        self.event_pushers.push(pusher);
    }

    pub fn publish_batch(&mut self, logger_batch: LoggerBatch<S, E>) -> () {
        for pusher in (&mut self.event_pushers).iter_mut() {
            match logger_batch {
                LoggerBatch::Logs(evs) => {
                    if let Some(frontier) = self.frontier {
                        pusher.push(Event::Messages(frontier, evs.clone()));
                        let &(last_ts, _, _) = evs.last().unwrap();
                        let new_frontier = RootTimestamp::new(last_ts);
                        pusher.push(Event::Progress(vec![(new_frontier, 1), (frontier, -1)]));
                        self.frontier = Some(new_frontier);
                    }
                },
                LoggerBatch::End => {
                    if let Some(frontier) = self.frontier {
                        pusher.push(Event::Progress(vec![(frontier, -1)]));
                        self.frontier = None;
                    }
                },
            }
        }
    }
}
