//! Traits, implementations, and macros related to logging timely events.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;

use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;

use dataflow::operators::capture::{Event, EventPusher};

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
    BufferingLogger::<EventsSetup, LogEvent>::new_inactive()
}

/// Manages the logging channel subscriptions and log pushers.
pub struct LogManager<P1, P2, F1, F2> where
P1: EventPusher<Product<RootTimestamp, u64>, LogMessage> + Send,
P2: EventPusher<Product<RootTimestamp, u64>, CommsMessage> + Send,
F1: Fn()->P1+Send+Sync,
F2: Fn()->P2+Send+Sync {

    timely_subscription: Arc<F1>,
    communication_subscription: Arc<F2>,
}

impl<P1, P2, F1, F2> LogManager<P1, P2, F1, F2> where
P1: EventPusher<Product<RootTimestamp, u64>, LogMessage> + Send,
P2: EventPusher<Product<RootTimestamp, u64>, CommsMessage> + Send,
F1: Fn()->P1+Send+Sync,
F2: Fn()->P2+Send+Sync {

    /// Constructs a new LogManager.
    pub fn new(timely_subscription: F1, communication_subscription: F2) -> Self {
        LogManager {
            timely_subscription: Arc::new(timely_subscription),
            communication_subscription: Arc::new(communication_subscription),
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
    /// Makes a new `LoggerConfig` wrapper from a `LogManager`.
    pub fn new<P1: 'static, P2: 'static, F1: 'static, F2: 'static>(log_manager: &mut LogManager<P1, P2, F1, F2>) -> Self where
        P1: EventPusher<Product<RootTimestamp, u64>, LogMessage> + Send,
        P2: EventPusher<Product<RootTimestamp, u64>, CommsMessage> + Send,
        F1: Fn()->P1+Send+Sync,
        F2: Fn()->P2+Send+Sync {

        let timely_subscription = log_manager.timely_subscription.clone();
        let communication_subscription = log_manager.communication_subscription.clone();
        LoggerConfig {
            timely_logging: Arc::new(move |events_setup: EventsSetup| {
                let logger = RefCell::new(BatchLogger::new((timely_subscription)()));
                Rc::new(BufferingLogger::new(events_setup, Box::new(move |data| logger.borrow_mut().publish_batch(data))))
            }),
            communication_logging: Arc::new(move |comms_setup: CommsSetup| {
                let logger = RefCell::new(BatchLogger::new((communication_subscription)()));
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
            event_pusher: event_pusher,
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
                    self.event_pusher.push(Event::Messages(frontier, evs.clone()));
                    let &(last_ts, _, _) = evs.last().unwrap();
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
