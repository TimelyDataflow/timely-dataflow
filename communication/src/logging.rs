//! Simple timely_communication logging

extern crate time;
extern crate abomonation;

use std::cell::RefCell;
use std::io::Write;
use std::io::Read;
use std::fs::{self, File};
use std::path::Path;
use abomonation::Abomonation;

// Copied from timely's logging system
static mut precise_time_ns_delta: Option<i64> = None;

/// Returns the value of an high resolution performance counter, in nanoseconds, rebased to be
/// roughly comparable to an unix timestamp.
/// Useful for comparing and merging logs from different machines (precision is limited by the
/// precision of the wall clock base; clock skew effects should be taken into consideration).
#[inline(always)]
fn get_precise_time_ns() -> u64 {
    (time::precise_time_ns() as i64 - unsafe { precise_time_ns_delta.unwrap() }) as u64
}

pub fn initialize(process: usize, name: &str, id: usize) {
    if cfg!(feature = "logging") {
        COMMUNICATION.with(|x| x.set(
                    File::create(
                        format!("logs/communication-{}-{}-{}.abom", process, name, id))
                    .unwrap()
                )
            );
        unsafe {
            precise_time_ns_delta = Some({
                let wall_time = time::get_time();
                let wall_time_ns = wall_time.nsec as i64 + wall_time.sec * 1000000000;
                time::precise_time_ns() as i64 - wall_time_ns
            });
        }
    }
}

// timely_communication-specific, influenced by timely's logging but simpler
// XXX: this contains lots of copied/nearly-identical code from timely's logging infrastrcture
// -> maybe merge in the future?

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

unsafe_abomonate!(CommunicationEvent);

impl CommunicationEvent {
    pub fn open_log_readers<P: AsRef<Path>>(path: P) -> Vec<(usize, usize, SimpleLogReader<File>)> {
        let mut readers = Vec::new();
        if let Ok(dir) = fs::read_dir(path) {
            readers = dir.flat_map(|e| e.into_iter())
                         .filter_map(|e| {
                             // this maps the sender/receiver threads to the same "thread id"
                             if let Ok(fname) = e.file_name().into_string() {
                                 let fields: Vec<_>= fname.trim_right_matches(".abom")
                                                          .split('-').collect();
                                 if fields.len() != 4 {
                                     return None;
                                 }
                                 if fields[0] != "communication" {
                                     return None;
                                 }
                                 let pid = match fields[1].parse::<usize>() {
                                     Ok(pid) => pid,
                                     Err(_) => return None,
                                 };
                                 let tid = match fields[3].parse::<usize>() {
                                     Ok(tid) => tid,
                                     Err(_) => return None,
                                 };
                                 return Some((pid, tid, e.path()))
                             }
                             None
                         })
                         .map(|(pid, tid, path)| {
                             (pid, tid, SimpleLogReader::new(File::open(path).unwrap()))
                         }).collect()
        }
        readers
    }
}

pub fn log<T: Abomonation, W: Write>(logger: &'static ::std::thread::LocalKey<SimpleLogger<W>>, record: T) {
    if cfg!(feature = "logging") {
        logger.with(|x| x.log(record));
    }
}

pub struct SimpleLogger<W: Write> {
    output: RefCell<Option<W>>,
    buffer: RefCell<Vec<u8>>,
}

impl<W: Write> SimpleLogger<W> {
    fn new() -> SimpleLogger<W> {
        SimpleLogger {
            output: RefCell::new(None),
            buffer: RefCell::new(Vec::new()),
        }
    }

    fn set(&self, write: W) {
        *self.output.borrow_mut() = Some(write);
    }

    fn log<T: Abomonation>(&self, record: T) {
        let buf = &mut *self.buffer.borrow_mut();
        let ts = get_precise_time_ns();
        unsafe { 
            ::abomonation::encode(&(ts, record), buf);
        }
        if let Some(ref mut writer) = *self.output.borrow_mut() {
            writer.write_all(buf).unwrap();
        } else {
            panic!("logging file not initialized!")
        }
        buf.clear();
    }
}

impl<W: Write> Drop for SimpleLogger<W> {
    fn drop(&mut self) {
        if let Some(ref mut writer) = *self.output.borrow_mut() {
            writer.flush().unwrap();
        }
    }
}

thread_local!(pub static COMMUNICATION: SimpleLogger<File> = SimpleLogger::new());

pub struct SimpleLogReader<R: Read> {
    input: R,
    bytes: Vec<u8>,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    consumed: usize,
    valid: usize,
}

// very similar to EventReader
impl<R: Read> SimpleLogReader<R> {

    pub fn new(read: R) -> SimpleLogReader<R> {
        SimpleLogReader {
            input: read,
            bytes: vec![0u8; 1 << 20],
            buf1: Vec::new(),
            buf2: Vec::new(),
            consumed: 0,
            valid: 0,
        }
    }

    // TODO: proper Iterator, better type safety...
    pub fn next<T: Abomonation>(&mut self) -> Option<&(u64, T)> {
        if unsafe { ::abomonation::decode::<(u64, T)>(&mut self.buf1[self.consumed..])}.is_some() {
            let (item, rest) = unsafe { ::abomonation::decode::<(u64, T)>(&mut self.buf1[self.consumed..])}.unwrap();
            self.consumed = self.valid - rest.len();
            return Some(item);
        }

        if self.consumed > 0 {
            self.buf2.clear();
            self.buf2.write_all(&self.buf1[self.consumed..]).unwrap();
            ::std::mem::swap(&mut self.buf1, &mut self.buf2);
            self.valid = self.buf1.len();
            self.consumed = 0;
        }

        if let Ok(len) = self.input.read(&mut self.bytes[..]) {
            if len == 0 {
                // EOF
                return None;
            }
            self.buf1.write_all(&self.bytes[..len]).unwrap();
            self.valid = self.buf1.len();
            return self.next();
        }

        None
    }
}



