extern crate time;

use std::cell::RefCell;

use ::Data;

use dataflow::operators::input::InputHelper;
use dataflow::operators::input::Input;
use dataflow::operators::inspect::Inspect;

use timely_communication::Allocate;
use dataflow::scopes::root::Root;
use dataflow::Scope;

use abomonation::Abomonation;

use drain::DrainExt;

pub trait Logger {
    type Record;
    fn log(&self, record: Self::Record);
    fn flush(&self);
}

pub struct TimelyLogger<T: Data> {
    buffer: RefCell<Vec<(T,u64)>>,
    double: RefCell<Vec<(T,u64)>>,
    stream: RefCell<Option<InputHelper<u64, (T,u64)>>>,
}

impl<T: Data> Logger for TimelyLogger<T> {
    type Record = T;
    #[inline]
    fn log(&self, record: T) {
        self.buffer.borrow_mut().push((record, time::precise_time_ns()));
    }
    fn flush(&self) {
        ::std::mem::swap(&mut *self.buffer.borrow_mut(), &mut *self.double.borrow_mut());
        let mut queue = self.double.borrow_mut();
        let mut temp = self.stream.borrow_mut();
        let mut input = temp.as_mut().unwrap();
        for item in queue.drain_temp() {
            input.send(item);
        }
        input.advance_to(time::precise_time_ns());
    }
}

impl<T: Data> TimelyLogger<T> {
    fn new() -> TimelyLogger<T> {
        TimelyLogger {
            buffer: RefCell::new(Vec::new()),
            double: RefCell::new(Vec::new()),
            stream: RefCell::new(None),
        }
    }
    fn set(&self, input: InputHelper<u64, (T,u64)>) {
        *self.stream.borrow_mut() = Some(input);
    }
}

pub fn initialize<A: Allocate>(root: &mut Root<A>) {

    root.scoped(move |scope| {
        let (input_o, stream_o) = scope.new_input();
        let (input_c, stream_c) = scope.new_input();
        let (input_p, stream_p) = scope.new_input();
        let (input_m, stream_m) = scope.new_input();
        let (input_s, stream_s) = scope.new_input();

        stream_o.inspect_batch(|t, x| { println!("OPERATES at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
        stream_c.inspect_batch(|t, x| { println!("CHANNELS at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
        stream_m.inspect_batch(|t, x| { println!("MESSAGES at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
        stream_p.inspect_batch(|t, x| { println!("PROGRESS at {:?}:", t); for elem in x { println!("  {:?}", elem) } });
        stream_s.inspect_batch(|t, x| { println!("SCHEDULE at {:?}:", t); for elem in x { println!("  {:?}", elem) } });

        OPERATES.with(|x| x.set(input_o));
        CHANNELS.with(|x| x.set(input_c));
        MESSAGES.with(|x| x.set(input_m));
        PROGRESS.with(|x| x.set(input_p));
        SCHEDULE.with(|x| x.set(input_s));
    });
}

pub fn flush_logs() {
    OPERATES.with(|x| x.flush());
    CHANNELS.with(|x| x.flush());
    PROGRESS.with(|x| x.flush());
    MESSAGES.with(|x| x.flush());
    SCHEDULE.with(|x| x.flush());
}

thread_local!(pub static OPERATES: TimelyLogger<OperatesEvent> = TimelyLogger::new());
thread_local!(pub static CHANNELS: TimelyLogger<ChannelsEvent> = TimelyLogger::new());
thread_local!(pub static PROGRESS: TimelyLogger<ProgressEvent> = TimelyLogger::new());
thread_local!(pub static MESSAGES: TimelyLogger<MessagesEvent> = TimelyLogger::new());
thread_local!(pub static SCHEDULE: TimelyLogger<ScheduleEvent> = TimelyLogger::new());

#[derive(Debug, Clone)]
pub struct OperatesEvent {
    pub addr: Vec<usize>,
    pub name: String,
}

unsafe_abomonate!(OperatesEvent : addr, name);

#[derive(Debug, Clone)]
pub struct ChannelsEvent {
    pub id: usize,
    pub scope_addr: Vec<usize>,
    pub source: (usize, usize),
    pub target: (usize, usize),
}

unsafe_abomonate!(ChannelsEvent : id, scope_addr, source, target);

#[derive(Debug, Clone)]
pub struct ProgressEvent {
    pub is_send: bool,
    pub addr: Vec<usize>,
    pub messages: Vec<(usize, usize, String, i64)>,
    pub internal: Vec<(usize, usize, String, i64)>,
}

unsafe_abomonate!(ProgressEvent : is_send, addr, messages, internal);

#[derive(Debug, Clone)]
pub struct MessagesEvent {
    pub is_send: bool,
    pub channel: usize,
    pub source: usize,
    pub target: usize,
    pub seq_no: usize,
    pub length: usize,
}

unsafe_abomonate!(MessagesEvent);


#[derive(Debug, Clone)]
pub struct ScheduleEvent {
    pub addr: Vec<usize>,
    pub is_start: bool,
}

unsafe_abomonate!(ScheduleEvent : addr, is_start);
