//! Traits, implementations, and macros related to logging timely events.

use std::cell::RefCell;
use std::io::Write;
use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, RwLock, Mutex};
use std::hash::Hash;

use std::collections::HashMap;
use std::fmt::Debug; 
use ::Data;

use timely_communication::Allocate;
use timely_communication;
use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;
use ::progress::timestamp::Timestamp;

use byteorder::{LittleEndian, WriteBytesExt};

use dataflow::scopes::root::Root;
use dataflow::operators::capture::{EventWriter, Event, EventPusher};

use abomonation::Abomonation;

use std::io::BufWriter;
use std::net::TcpStream;

use timely_logging::BufferingLogger;
use timely_logging::Event as LogEvent;
use timely_logging::EventsSetup;
use timely_logging::{CommsEvent, CommsSetup};
use timely_logging::LoggerBatch;

type LogMessage = (u64, EventsSetup, LogEvent);
type CommsMessage = (u64, CommsSetup, CommsEvent);

/// TODO(andreal)
pub type Logger = Rc<BufferingLogger<EventsSetup, LogEvent>>;

/// TODO(andreal)
pub struct LogManager {
    timely_logs: HashMap<
        EventsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>>>,
    timely_subscriptions:
        Vec<(Arc<Fn(&EventsSetup)->bool+Send+Sync>, Arc<EventPusher<Product<RootTimestamp, u64>, LogMessage>+Send+Sync>)>,
    communication_logs: HashMap<
        CommsSetup,
        Arc<Mutex<EventStreamSubscriptionManager<CommsSetup, CommsEvent>>>>,
    communication_subscriptions:
        Vec<(Arc<Fn(&CommsSetup)->bool+Send+Sync>, Arc<EventPusher<Product<RootTimestamp, u64>, CommsMessage>+Send+Sync>)>,
}

impl LogManager {
    fn add_timely_subscription(&mut self,
                               filter: Arc<Fn(&EventsSetup)->bool+Send+Sync>,
                               pusher: Arc<EventPusher<Product<RootTimestamp, u64>, LogMessage>+Send+Sync>) {

        pusher.push(Event::Progress(vec![(Default::default(), -1)]));
        for (_, ref event_manager) in self.timely_logs.iter().filter(|&(ref setup, _)| filter(setup)) {
            event_manager.lock().unwrap().subscribe(pusher.clone());
        }
        self.timely_subscriptions.push((filter, pusher));
    }

    fn add_communication_subscription(&mut self,
                                      filter: Arc<Fn(&CommsSetup)->bool+Send+Sync>,
                                      pusher: Arc<EventPusher<Product<RootTimestamp, u64>, CommsMessage>+Send+Sync>) {

        //eprintln!("new comm subscription");
        pusher.push(Event::Progress(vec![(Default::default(), -1)]));
        for (ref setup, ref event_manager) in self.communication_logs.iter().filter(|&(ref setup, _)| filter(setup)) {
            //eprintln!("  to comm logger {:?}", setup);
            event_manager.lock().unwrap().subscribe(pusher.clone());
        }
        self.communication_subscriptions.push((filter, pusher));
    }
}

struct SharedVec {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedVec {
    pub fn new(inner: Arc<Mutex<Vec<u8>>>) -> Self {
        SharedVec { 
            inner: inner,
        }
    }
}

impl Write for SharedVec {
    fn write(&mut self, data: &[u8]) -> Result<usize, ::std::io::Error> {
        self.inner.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> Result<(), ::std::io::Error> {
        Ok(())
    }
}

struct SharedEventWriter<T, D, W: Write> {
    inner: Mutex<EventWriter<T, D, W>>,
}

impl<T, D, W: Write> SharedEventWriter<T, D, W> {
    fn new(w: W) -> Self {
        SharedEventWriter {
            inner: Mutex::new(EventWriter::new(w)),
        }
    }
}

impl<T: Abomonation+Debug, D: Abomonation+Debug, W: Write> EventPusher<T, D> for SharedEventWriter<T, D, W> {
    fn push(&self, event: Event<T, D>) {
        let mut inner = self.inner.lock().expect("event pusher poisoned");
        inner.push(event)
    }
}

/// TODO(andreal)
pub struct FilteredLogManager<S, E> {
    log_manager: Arc<Mutex<LogManager>>,
    filter: Arc<Fn(&S)->bool+Send+Sync>,
    _e: ::std::marker::PhantomData<E>,
}

impl FilteredLogManager<EventsSetup, LogEvent> {
    /// TODO(andreal)
    pub fn to_tcp_socket(&mut self) {
        let target: String = ::std::env::var("TIMELY_LOG_TARGET").expect("no $TIMELY_LOG_TARGET, e.g. 127.0.0.1:34254");
        let writer = /*BufWriter::with_capacity(4096,*/ TcpStream::connect(target).expect("failed to connect to logging destination"); //);

        let writer = SharedEventWriter::new(writer);
        let pusher: Arc<EventPusher<Product<RootTimestamp, u64>, LogMessage>+Send+Sync> = Arc::new(writer);

        self.log_manager.lock().unwrap().add_timely_subscription(self.filter.clone(), pusher);
    }

    /// TODO(andreal)
    pub fn to_bufs(&mut self) -> Vec<Arc<Mutex<Vec<u8>>>> {
        let mut vecs = Vec::new();

        for i in 0..4 {
            let buf = Arc::new(Mutex::new(Vec::<u8>::with_capacity(4_000_000_000)));
            let writer = SharedEventWriter::new(SharedVec::new(buf.clone()));
            let pusher: Arc<EventPusher<Product<RootTimestamp, u64>, LogMessage>+Send+Sync> = Arc::new(writer);
            self.log_manager.lock().unwrap().add_timely_subscription(Arc::new(move |s| s.index == i), pusher);
            vecs.push(buf);
        }

        vecs
    }
}

impl FilteredLogManager<CommsSetup, CommsEvent> {
    /// TODO(andreal)
    pub fn to_tcp_socket(&mut self) {
        let comm_target = ::std::env::var("TIMELY_COMM_LOG_TARGET").expect("no $TIMELY_COMM_LOG_TARGET, e.g. 127.0.0.1:34255");
        let writer = /* BufWriter::with_capacity(4096,*/ TcpStream::connect(comm_target).expect("failed to connect to logging destination"); //);

        let writer = SharedEventWriter::new(writer);
        let pusher: Arc<EventPusher<Product<RootTimestamp, u64>, CommsMessage>+Send+Sync> = Arc::new(writer);

        self.log_manager.lock().unwrap().add_communication_subscription(self.filter.clone(), pusher);
    }
}

impl LogManager {
    /// TODO(andreal)
    pub fn new() -> Arc<Mutex<Self>> {
        ::timely_logging::initialize_precise_time_ns();
        Arc::new(Mutex::new(LogManager {
            timely_logs: HashMap::new(),
            timely_subscriptions: Vec::new(),
            communication_logs: HashMap::new(),
            communication_subscriptions: Vec::new(),
        }))
    }
}

/// TODO(andreal)
pub trait LogFilter {
    /// TODO(andreal)
    fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent>;

    /// TODO(andreal)
    fn comms(&mut self) -> FilteredLogManager<CommsSetup, CommsEvent>;
}

impl LogFilter for Arc<Mutex<LogManager>> {
    /// TODO(andreal)
    #[inline] fn workers(&mut self) -> FilteredLogManager<EventsSetup, LogEvent> {
        FilteredLogManager {
            log_manager: self.clone(),
            filter: Arc::new(|_| true),
            _e: ::std::marker::PhantomData,
        }
    }
 
    /// TODO(andreal)
    #[inline] fn comms(&mut self) -> FilteredLogManager<CommsSetup, CommsEvent> {
        FilteredLogManager {
            log_manager: self.clone(),
            filter: Arc::new(|_| true),
            _e: ::std::marker::PhantomData,
        }
    }
}

/// TODO(andreal)
pub struct LoggerConfig {
    /// TODO(andreal)
    pub timely_logging: Arc<Fn(EventsSetup)->Rc<BufferingLogger<EventsSetup, LogEvent>>+Send+Sync>,
    /// TODO(andreal)
    pub communication_logging: Arc<Fn(CommsSetup)->Rc<BufferingLogger<CommsSetup, CommsEvent>>+Send+Sync>,
}

impl LoggerConfig {
    fn register_timely_logger(
        log_manager: &mut LogManager,
        events_setup: EventsSetup) -> Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>> {

        let event_manager: Arc<Mutex<EventStreamSubscriptionManager<EventsSetup, LogEvent>>> = Arc::new(Mutex::new(Default::default()));
        log_manager.timely_logs.insert(events_setup, event_manager.clone());
        for pusher in log_manager.timely_subscriptions.iter().filter(|&&(ref f, _)| f(&events_setup)).map(|&(_, ref p)| p.clone()) {
            event_manager.lock().unwrap().subscribe(pusher);
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
            event_manager.lock().unwrap().subscribe(pusher);
        }
        event_manager
    }

    /// TODO(andreal)
    pub fn new(log_manager: &mut Arc<Mutex<LogManager>>) -> Self {
        let timely_logging_manager = log_manager.clone();
        let communication_logging_manager = log_manager.clone();
        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: EventsSetup| {
                let subscription_manager = LoggerConfig::register_timely_logger(
                    &mut timely_logging_manager.lock().unwrap(), events_setup);
                //eprintln!("registered timely logger: {:?}", events_setup);
                Rc::new(BufferingLogger::new(events_setup, Box::new(move |data| {
                    subscription_manager.lock().expect("cannot lock mutex").publish_batch(data);
                })))
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                let subscription_manager = LoggerConfig::register_comms_logger(
                    &mut communication_logging_manager.lock().unwrap(), comms_setup);
                //eprintln!("registered comm logger: {:?}", comms_setup);
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
    event_pushers: Vec<Arc<EventPusher<Product<RootTimestamp, u64>, (u64, S, E)>+Send+Sync>>,
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
    fn subscribe(&mut self, pusher: Arc<EventPusher<Product<RootTimestamp, u64>, (u64, S, E)>+Send+Sync>) {
        //eprintln!("pusher subscription to logger");
        // if this logging stream is already closed
        if let Some(frontier) = self.frontier {
            pusher.push(Event::Progress(vec![(frontier, 1)]));
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
