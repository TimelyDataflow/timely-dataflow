//! Traits, implementations, and macros related to logging timely events.

extern crate time;

use std::cell::RefCell;

use ::Data;

// use dataflow::operators::input::{Input, Handle};
// use dataflow::operators::inspect::Inspect;

use timely_communication::Allocate;
use dataflow::scopes::root::Root;
use dataflow::Scope;

use std::io::Write;

use abomonation::Abomonation;

// use drain::DrainExt;

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

// /// Logs records to a timely stream.
// pub struct TimelyLogger<T: Data> {
//     buffer: RefCell<Vec<(T,u64)>>,
//     double: RefCell<Vec<(T,u64)>>,
//     stream: RefCell<Option<Handle<u64, (T,u64)>>>,
// }
//
// impl<T: Data> Logger for TimelyLogger<T> {
//     type Record = T;
//     #[inline]
//     fn log(&self, record: T) {
//         self.buffer.borrow_mut().push((record, time::precise_time_ns()));
//     }
//     fn flush(&self) {
//         ::std::mem::swap(&mut *self.buffer.borrow_mut(), &mut *self.double.borrow_mut());
//         let mut queue = self.double.borrow_mut();
//         let mut temp = self.stream.borrow_mut();
//         let mut input = temp.as_mut().unwrap();
//         for item in queue.drain_temp() {
//             input.send(item);
//         }
//         input.advance_to(time::precise_time_ns());
//     }
// }
//
// impl<T: Data> TimelyLogger<T> {
//     fn new() -> TimelyLogger<T> {
//         TimelyLogger {
//             buffer: RefCell::new(Vec::new()),
//             double: RefCell::new(Vec::new()),
//             stream: RefCell::new(None),
//         }
//     }
//     fn set(&self, input: Handle<u64, (T,u64)>) {
//         *self.stream.borrow_mut() = Some(input);
//     }
// }

pub struct FileLogger<T: Data> {
    buffer: RefCell<Vec<(T,u64)>>,
    bytes: RefCell<Vec<u8>>,
    file: RefCell<Option<::std::fs::File>>,
}

impl<T: Data> Logger for FileLogger<T> {
    type Record = T;
    #[inline]
    fn log(&self, record: T) {
        self.buffer.borrow_mut().push((record, time::precise_time_ns()));
    }
    fn flush(&self) {
        if self.buffer.borrow().len() > 0 {
            if let Some(ref mut file) = *self.file.borrow_mut() {
                unsafe { ::abomonation::encode(&(*self.buffer.borrow()), &mut (*self.bytes.borrow_mut())); }
                file.write_all(&self.bytes.borrow()[..]).unwrap();
                self.bytes.borrow_mut().clear();
                self.buffer.borrow_mut().clear();
            }
            else {
                panic!("logging file not initialized!!!!");
            }
        }
    }
}

impl<T: Data> FileLogger<T> {
    fn new() -> FileLogger<T> {
        FileLogger {
            buffer: RefCell::new(Vec::new()),
            bytes: RefCell::new(Vec::new()),
            file: RefCell::new(None),
        }
    }
    fn set(&self, filename: String) {
        let fname = filename.clone();
        *self.file.borrow_mut() = Some(::std::fs::File::create(filename).ok().expect(&fname));
    }
}

// pub struct ConsoleLogger<T: Data> {
//     type Record = T;
//     #[inline]
//     fn log(&self, record: T) {
//         println!("{}:\t{:?}", time::precise_time_ns(), record);
//     }
// }


/// Initializes logging; called as part of `Root` initialization.
pub fn initialize<A: Allocate>(root: &mut Root<A>) {

    OPERATES.with(|x| x.set(format!("logs/operates-{}.abom", root.index())));
    CHANNELS.with(|x| x.set(format!("logs/channels-{}.abom", root.index())));
    MESSAGES.with(|x| x.set(format!("logs/messages-{}.abom", root.index())));
    PROGRESS.with(|x| x.set(format!("logs/progress-{}.abom", root.index())));
    SCHEDULE.with(|x| x.set(format!("logs/schedule-{}.abom", root.index())));
    GUARDED_MESSAGE.with(|x| x.set(format!("logs/guarded_message-{}.abom", root.index())));
    GUARDED_PROGRESS.with(|x| x.set(format!("logs/guarded_progress-{}.abom", root.index())));

    // root.scoped(move |scope| {
    //     let (input_o, stream_o) = scope.new_input();
    //     let (input_c, stream_c) = scope.new_input();
    //     let (input_p, stream_p) = scope.new_input();
    //     let (input_m, stream_m) = scope.new_input();
    //     let (input_s, stream_s) = scope.new_input();
    //     let (input_gm, stream_gm) = scope.new_input();
    //     let (input_gp, stream_gp) = scope.new_input();
    //
    //     stream_o.inspect_batch(|t, x| { println!("OPERATES at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_c.inspect_batch(|t, x| { println!("CHANNELS at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_m.inspect_batch(|t, x| { println!("MESSAGES at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_p.inspect_batch(|t, x| { println!("PROGRESS at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_s.inspect_batch(|t, x| { println!("SCHEDULE at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_gm.inspect_batch(|t, x| { println!("GUARDED_MESSAGE at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //     stream_gp.inspect_batch(|t, x| { println!("GUARDED_PROGRESS at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
    //
    //     OPERATES.with(|x| x.set(input_o));
    //     CHANNELS.with(|x| x.set(input_c));
    //     MESSAGES.with(|x| x.set(input_m));
    //     PROGRESS.with(|x| x.set(input_p));
    //     SCHEDULE.with(|x| x.set(input_s));
    //     GUARDED_MESSAGE.with(|x| x.set(input_gm));
    //     GUARDED_PROGRESS.with(|x| x.set(input_gp));
    // });
}

/// Flushes logs; called by `Root::step`.
pub fn flush_logs() {
    OPERATES.with(|x| x.flush());
    CHANNELS.with(|x| x.flush());
    PROGRESS.with(|x| x.flush());
    MESSAGES.with(|x| x.flush());
    SCHEDULE.with(|x| x.flush());
    GUARDED_MESSAGE.with(|x| x.flush());
    GUARDED_PROGRESS.with(|x| x.flush());
}

// thread_local!(pub static OPERATES: TimelyLogger<OperatesEvent> = TimelyLogger::new());
// thread_local!(pub static CHANNELS: TimelyLogger<ChannelsEvent> = TimelyLogger::new());
// thread_local!(pub static PROGRESS: TimelyLogger<ProgressEvent> = TimelyLogger::new());
// thread_local!(pub static MESSAGES: TimelyLogger<MessagesEvent> = TimelyLogger::new());
// thread_local!(pub static SCHEDULE: TimelyLogger<ScheduleEvent> = TimelyLogger::new());
// thread_local!(pub static GUARDED_MESSAGE: TimelyLogger<bool> = TimelyLogger::new());
// thread_local!(pub static GUARDED_PROGRESS: TimelyLogger<bool> = TimelyLogger::new());

thread_local!(pub static OPERATES: FileLogger<OperatesEvent> = FileLogger::new());
thread_local!(pub static CHANNELS: FileLogger<ChannelsEvent> = FileLogger::new());
thread_local!(pub static PROGRESS: FileLogger<ProgressEvent> = FileLogger::new());
thread_local!(pub static MESSAGES: FileLogger<MessagesEvent> = FileLogger::new());
thread_local!(pub static SCHEDULE: FileLogger<ScheduleEvent> = FileLogger::new());
thread_local!(pub static GUARDED_MESSAGE: FileLogger<bool> = FileLogger::new());
thread_local!(pub static GUARDED_PROGRESS: FileLogger<bool> = FileLogger::new());

#[derive(Debug, Clone)]
/// The creation of an `Operate` implementor.
pub struct OperatesEvent {
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// A helpful name.
    pub name: String,
}

unsafe_abomonate!(OperatesEvent : addr, name);

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
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// List of message updates, containing Target descriptor, timestamp as string, and delta.
    pub messages: Vec<(usize, usize, String, i64)>,
    /// List of capability updates, containing Source descriptor, timestamp as string, and delta.
    pub internal: Vec<(usize, usize, String, i64)>,
}

unsafe_abomonate!(ProgressEvent : is_send, addr, messages, internal);

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


#[derive(Debug, Clone)]
/// Operator start or stop.
pub struct ScheduleEvent {
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// `true` if the operator is starting, `false` if it is stopping.
    pub is_start: bool,
}

unsafe_abomonate!(ScheduleEvent : addr, is_start);
