//! Traits, implementations, and macros related to logging timely events.


use std::cell::RefCell;
use std::io::Write;
use std::fs::File;
use std::rc::Rc;

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

use timely_logging;
use timely_logging::Event as LogEvent;
use timely_logging::EventsSetup;
use timely_logging::{CommEvent, CommsSetup};

use timely_communication::Logging;

pub use timely_logging::OperatesEvent;
pub use timely_logging::ChannelsEvent;
pub use timely_logging::ProgressEvent;
pub use timely_logging::MessagesEvent;
pub use timely_logging::ScheduleEvent;
pub use timely_logging::StartStop;
pub use timely_logging::PushProgressEvent;
pub use timely_logging::ApplicationEvent;
pub use timely_logging::GuardedProgressEvent;
pub use timely_logging::GuardedMessageEvent;
pub use timely_logging::CommChannelsEvent;

/// Logs `record` in `logger` if logging is enabled.
pub fn log<T: ::timely_logging::Logger>(logger: &'static ::std::thread::LocalKey<T>, record: T::Record) {
    if cfg!(feature = "logging") {
        logger.with(|x| x.log(record));
    }
}

/// TODO(andreal)
pub struct TimelyLogger<V: Abomonation> where LogEvent: From<V> {
    stream: Rc<RefCell<LogEventStream<EventsSetup, LogEvent>>>,
    _v: ::std::marker::PhantomData<V>,
}

impl<V: Abomonation> TimelyLogger<V> where LogEvent: From<V> {
    fn new(stream: Rc<RefCell<LogEventStream<EventsSetup, LogEvent>>>) -> TimelyLogger<V> {
        TimelyLogger {
            stream: stream,
            _v: ::std::marker::PhantomData,
        }
    }
}

impl<V: Abomonation> ::timely_logging::Logger for TimelyLogger<V> where timely_logging::Event: From<V> {
    type Record = V;
    fn log(&self, record: V) {
        self.stream.borrow_mut().log(LogEvent::from(record));
    }
}

/// Initializes logging; called as part of `Root` initialization.
pub fn initialize<A: Allocate>(root: &mut Root<A>) {
    eprintln!("logging: index {}/{}", root.index(), root.peers());
    LOG_EVENT_STREAM.with(|x| {
        x.borrow_mut().setup = Some(EventsSetup {
            index: root.index(),
        });
    });
}

/// Flushes logs; called by `Root::step`.
pub fn flush_logs() {
    LOG_EVENT_STREAM.with(|x| {
        x.borrow_mut().flush();
    });
}

// trait ScheduleLogger {
//     fn contains_active_ops(&self) -> bool;
// }
// 
// impl<S: Write> ScheduleLogger for LogEventStream<ScheduleEvent, S> {
//     fn contains_active_ops(&self) -> bool {
//         self.buffer.borrow().iter().any(|&(ref evt, ts) | {
//             match evt.start_stop {
//                 StartStop::Stop { activity } => activity,
//                 _ => false,
//             }
//         })
//     }
// }

struct LogEventStream<S: Copy, E: Clone> {
    setup: Option<S>,
    writer: Option<Box<EventStreamInput<Product<RootTimestamp, u64>, (u64, S, E)>>>,
    _s: ::std::marker::PhantomData<S>,
    _e: ::std::marker::PhantomData<E>,
}

impl<S: Copy, E: Clone> LogEventStream<S, E> {
    fn new() -> Self {
        LogEventStream {
            setup: None,
            writer: None,
            _s: ::std::marker::PhantomData,
            _e: ::std::marker::PhantomData,
        }
    }

    fn flush(&mut self) {
        if let Some(ref mut writer) = self.writer {
            writer.advance_by(RootTimestamp::new(::timely_logging::get_precise_time_ns()));
        }
    }

    fn log(&mut self, record: E) {
        if let Some(ref mut writer) = self.writer {
            writer.send(
                (::timely_logging::get_precise_time_ns(), self.setup.expect("logging not initialized"), record));
        }
    }
}

trait EventStreamInput<T: Timestamp, V: Clone> {
    fn send(&mut self, value: V);
    fn flush(&mut self);
    fn advance_by(&mut self, timestamp: T);
    fn clear(&mut self);
}

/// Logs events to an underlying writer.
pub struct EventStreamWriter<T: Timestamp, V: Clone, P: EventPusher<T, V>> {
    buffer: Vec<V>,
    pusher: P, // RefCell<Option<Box<EventPusher<Product<RootTimestamp, u64>, (u64, S, E)>+Send>>>,
    cur_time: T,
    _v: ::std::marker::PhantomData<V>,
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> EventStreamWriter<T, V, P> {
    fn new(mut event_pusher: P) -> EventStreamWriter<T, V, P> {
        let cur_time: T = Default::default();
        event_pusher.push(Event::Progress(vec![(cur_time.clone(), 1)]));
        EventStreamWriter {
            buffer: Vec::new(),
            pusher: event_pusher,
            cur_time: cur_time,
            _v: ::std::marker::PhantomData,
        }
    }
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> EventStreamInput<T, V> for EventStreamWriter<T, V, P> {
    fn send(&mut self, value: V) {
        self.buffer.push(value);
    }
    fn flush(&mut self) {
        if self.buffer.len() > 0 {
            self.pusher.push(Event::Messages(self.cur_time.clone(), self.buffer.clone()));
        }
        self.buffer.clear();
    }
    fn advance_by(&mut self, timestamp: T) {
        assert!(self.cur_time.less_equal(&timestamp));
        self.flush();
        self.pusher.push(Event::Progress(vec![(self.cur_time.clone(), -1), (timestamp.clone(), 1)]));
        self.cur_time = timestamp;
    }
    fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl<T: Timestamp, V: Clone, P: EventPusher<T, V>> Drop for EventStreamWriter<T, V, P> {
    fn drop(&mut self) {
        if self.buffer.len() > 0 {
            self.pusher.push(Event::Messages(self.cur_time.clone(), self.buffer.clone()));
            self.buffer.clear();
        }
        self.pusher.push(Event::Progress(vec![(self.cur_time.clone(), -1)]));
    }
}

// /// Special logger for ScheduleEvents
// pub struct ScheduleLogEventStream<S: Write> {
//     logger: LogEventStream<ScheduleEvent, S>,
//     seen_inactive_step: RefCell<bool>,
// }
// 
// impl<S: Write> ScheduleLogEventStream<S> {
//     fn new() -> ScheduleLogEventStream<S> {
//         ScheduleLogEventStream {
//             logger: LogEventStream::new(),
//             seen_inactive_step: RefCell::new(false),
//         }
//     }
// 
//     fn set(&self, stream: S) {
//         self.logger.set(stream)
//     }
// 
//     fn flush_forced(&self) {
//         self.flush();
//         *self.seen_inactive_step.borrow_mut() = false;
//     }
// 
//     fn flush_maybe(&self) {
//         if self.logger.contains_active_ops() {
//             self.flush_forced();
//             return;
//         }
//         if !*self.seen_inactive_step.borrow() {
//             self.flush_forced();
//             *self.seen_inactive_step.borrow_mut() = true
//         } else {
//             self.logger.clear();
//         }
// 
//     }
// }
// 
// impl<S: Write> Logger for ScheduleLogEventStream<S> {
//     type Record = ScheduleEvent;
//     fn log(&self, record: Self::Record) {
//         self.logger.log(record)
//     }
//     fn flush(&self) {
//         self.logger.flush()
//     }
//     // fn record_count(&self) -> usize {
//     //     self.logger.record_count()
//     // }
//     // fn clear(&self) {
//     //     self.logger.clear()
//     // }
// 
// }

/// TODO(andreal)
pub fn blackhole() -> Logging {
    Logging::new(
        None,
        Box::new(move || {
            LOG_EVENT_STREAM.with(|x| {
                x.borrow_mut().writer = None;
            });
        }))
}

/// TODO(andreal)
pub fn to_tcp_socket() -> Logging {
    ::timely_logging::initialize_precise_time_ns();
    let target: String = ::std::env::var("TIMELY_LOG_TARGET").expect("no $TIMELY_LOG_TARGET, e.g. 127.0.0.1:34254");
    let comm_target = ::std::env::var("TIMELY_COMM_LOG_TARGET").expect("no $TIMELY_COMM_LOG_TARGET, e.g. 127.0.0.1:34254");
    let (comms_snd, comms_rcv) = ::std::sync::mpsc::channel();

    // comms
    let comm_writer = BufWriter::with_capacity(4096,
        TcpStream::connect(comm_target).expect("failed to connect to logging destination"));
    let comm_thread = ::std::thread::spawn(move || {
        let mut writer = EventStreamWriter::new(EventWriter::new(comm_writer));
        let mut cur_time = timely_logging::get_precise_time_ns();
        writer.advance_by(RootTimestamp::new(cur_time));

        let mut receiver = comms_rcv;
        loop {
            let mut new_time = timely_logging::get_precise_time_ns();
            while let Some((ts, setup, event)) = match receiver.try_recv() {
                Ok(msg) => {
                    Some(msg)
                },
                Err(::std::sync::mpsc::TryRecvError::Empty) => None,
                Err(::std::sync::mpsc::TryRecvError::Disconnected) => return,
            } {
                writer.send((ts, setup, event));
            }
            writer.flush();
            writer.advance_by(RootTimestamp::new(new_time));
            cur_time = new_time;
        }
    });

    Logging::new(
        Some(comms_snd),
        Box::new(move || {
            // timely
            let socket = BufWriter::with_capacity(4096,
                TcpStream::connect(&target).expect("failed to connect to logging destination"));
            let mut timely_writer = EventStreamWriter::new(EventWriter::new(socket));
            timely_writer.advance_by(RootTimestamp::new(timely_logging::get_precise_time_ns()));
            LOG_EVENT_STREAM.with(|x| {
                x.borrow_mut().writer = Some(Box::new(timely_writer));
            });
        }))
}

thread_local!{
    /// TODO(andreal)
    static LOG_EVENT_STREAM: Rc<RefCell<LogEventStream<EventsSetup, LogEvent>>> = Rc::new(RefCell::new(LogEventStream::new()));
    /// TODO(andreal)
    /// Logs operator creation.
    pub static OPERATES: TimelyLogger<OperatesEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs channel creation.
    pub static CHANNELS: TimelyLogger<ChannelsEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs progress transmission.
    pub static PROGRESS: TimelyLogger<ProgressEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs message transmission.
    pub static MESSAGES: TimelyLogger<MessagesEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs operator scheduling.
    pub static SCHEDULE: TimelyLogger<ScheduleEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs delivery of message to an operator input.
    pub static GUARDED_MESSAGE: TimelyLogger<GuardedMessageEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs delivery of notification to an operator.
    pub static GUARDED_PROGRESS: TimelyLogger<GuardedProgressEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs push_external_progress() calls for each operator
    pub static PUSH_PROGRESS: TimelyLogger<PushProgressEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs application-defined log events
    pub static APPLICATION: TimelyLogger<ApplicationEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
    /// Logs application-defined log events
    pub static COMM_CHANNELS: TimelyLogger<CommChannelsEvent> = LOG_EVENT_STREAM.with(|x| TimelyLogger::new(x.clone()));
}
