use std::cell::RefCell;
use std::fmt::Debug;
use std::net::TcpStream;
use std::rc::Rc;

use abomonation::Abomonation;

use timely_logging::{Logger, CommEvent, CommsSetup};
pub use timely_logging::CommunicationEvent;
pub use timely_logging::SerializationEvent;

pub type LogSender = ::std::sync::mpsc::Sender<(u64, CommsSetup, CommEvent)>;

pub fn initialize(process: usize, sender: bool, index: usize,
                  logging: Option<LogSender>) {
    let setup = CommsSetup {
        process: process,
        sender: sender,
        remote: index,
    };
    if let Some(logging) = logging {
        COMMUNICATION.with(|x| {
            *x.borrow_mut() = Some(CommLogger::new(logging.clone(), setup));
        });
        SERIALIZATION.with(|x| {
            *x.borrow_mut() = Some(CommLogger::new(logging.clone(), setup));
        });
    }
}

/// TODO(andreal)
pub struct CommLogger<V: Abomonation> {
    sender: ::std::sync::mpsc::Sender<(u64, CommsSetup, CommEvent)>,
    setup: CommsSetup,
    _v: ::std::marker::PhantomData<V>,
}

impl<V: Abomonation> CommLogger<V> {
    fn new(sender: ::std::sync::mpsc::Sender<(u64, CommsSetup, CommEvent)>,
           setup: CommsSetup) -> Self {
        CommLogger {
            sender: sender,
            setup: setup,
            _v: ::std::marker::PhantomData,
        }
    }
}

impl<R: Abomonation+Debug> ::timely_logging::Logger for CommLogger<R>
where ::timely_logging::CommEvent: From<R> {
    type Record = R;
    fn log(&self, record: Self::Record) {
        self.sender.send(
            (::timely_logging::get_precise_time_ns(), self.setup, CommEvent::from(record)))
                .expect("communication logging failed");
    }
}

pub fn log<R: Abomonation+Debug>(
    logger: &'static ::std::thread::LocalKey<RefCell<Option<CommLogger<R>>>>,
    record: R) where ::timely_logging::CommEvent: From<R> {

    if cfg!(feature = "logging") {
        use ::timely_logging::Logger;
        logger.with(|x| {
            if let Some(ref l) = *x.borrow() {
                l.log(record);
            }
        });
    }
}

/// TODO(andreal)
pub struct Logging {
    /// TODO(andreal)
    comms_snd: Option<::logging::LogSender>,
    /// TODO(andreal)
    timely_init_thread: Box<Fn()->()+Sync+Send>,
}

impl Logging {
    pub fn new(comms_snd: Option<::logging::LogSender>, timely_init_thread: Box<Fn()->()+Sync+Send>) -> Logging {
        Logging {
            comms_snd: comms_snd,
            timely_init_thread: timely_init_thread,
        }
    }

    /// TODO(andreal)
    pub fn unpack(self) -> (Option<::logging::LogSender>, Box<Fn()->()+Sync+Send>) {
        (self.comms_snd, self.timely_init_thread)
    }
}

thread_local!{
    /// TODO(andreal)
    pub static COMMUNICATION: RefCell<Option<CommLogger<CommunicationEvent>>> = RefCell::new(None);
    /// TODO(andreal)
    pub static SERIALIZATION: RefCell<Option<CommLogger<SerializationEvent>>> = RefCell::new(None);
}
