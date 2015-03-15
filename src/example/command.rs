
extern crate libc;

use std::mem::replace;

use std::os::unix::prelude::AsRawFd;

use std::old_io::{MemWriter, MemReader, IoErrorKind};
use std::old_io::process::{Command, Process};

use std::default::Default;
use std::thread;

use progress::frontier::Antichain;
use progress::{Scope, Graph, Timestamp};
use progress::count_map::CountMap;
use communication::channels::{Data};
use communication::{Observer, Communicator};
use example::stream::Stream;

use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

pub trait CommandExtensionTrait { fn command(&mut self, program: String) -> Self; }

impl<G: Graph<Timestamp = ((), u64)>, D: Data, C: Communicator> CommandExtensionTrait for Stream<G, D, C> {
    fn command(&mut self, program: String) -> Stream<G, D, C> {

        let mut process = match Command::new(program.clone()).spawn() {
            Ok(p) => p,
            Err(e) => panic!("Process creation error: {}; program: {}", e, program),
        };

        let fd1 = (&mut process.stdout).as_mut().unwrap().as_raw_fd();
        let fd2 = (&mut process.stdin).as_mut().unwrap().as_raw_fd();

        let command = CommandScope { process: process, buffer: Vec::new() };

        // set stdin and stdout to be non-blocking
        unsafe { libc::fcntl(fd1, libc::F_SETFL, libc::O_NONBLOCK); }
        unsafe { libc::fcntl(fd2, libc::F_SETFL, libc::O_NONBLOCK); }

        let index = self.graph.add_scope(command);

        self.graph.connect(self.name, ScopeInput(index, 0));

        Stream {
            name: ScopeOutput(index, 0),
            ports: Default::default(),
            graph: self.graph.clone(),
            allocator: self.allocator.clone(),
        }
    }
}

struct CommandScope {
    process:    Process,
    buffer:     Vec<u8>,
}

impl Scope<((), u64)> for CommandScope {
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<<((), u64) as Timestamp>::Summary>>>, Vec<CountMap<((), u64)>>) {
        let mut internal = vec![CountMap::new()];
        let stdout = (&mut self.process.stdout).as_mut().unwrap();
        let mut length_buf: Vec<_> = (0..8).map(|_| 0u8).collect();
        let mut read = 0;
        while read < 8 {
            read += match stdout.read(&mut length_buf[read..8]) {
                Ok(len) => len,
                Err(_err) => 0,
            };
        }

        let expected = MemReader::new(length_buf.clone()).read_le_uint().ok().expect("gis read all");
        let mut buffer = Vec::new();
        read = 0;

        while read < expected {
            let just_read = match stdout.push(expected - read, &mut buffer) {
                Ok(len)  => len,
                Err(e) => { if e.kind != IoErrorKind::ResourceUnavailable { panic!("Pull error: {}", e) } else { 0 } },
            };

            read += just_read;
        }

        let mut reader = MemReader::new(buffer);
        for index in range(0, internal.len()) {
            let number = reader.read_le_u64().ok().expect("1");
            for _ in range(0, number) {
                let time = reader.read_le_u64().ok().expect("2");
                let delta = reader.read_le_i64().ok().expect("3");
                println!("Command: intializing {} : {}", time, delta);
                internal[index].update(&((), time), delta);
            }
        }

        (vec![vec![Antichain::from_elem(Default::default())]], internal)
    }

    fn push_external_progress(&mut self, external: &mut Vec<CountMap<((), u64)>>) -> () {
        let mut writer = MemWriter::new();
        for index in range(0, external.len()) {
            writer.write_le_uint(external[index].len()).ok().expect("a");
            while let Some((((), time), delta)) = external[index].pop() {
            // for &(((), time), delta) in external[index].iter() {
                writer.write_le_u64(time).ok().expect("b");
                writer.write_le_i64(delta).ok().expect("c");
            }
        }

        let bytes = writer.into_inner();
        let stdin = (&mut self.process.stdin).as_mut().unwrap();
        match stdin.write_le_u64(bytes.len() as u64) {
            Ok(_) => {},
            Err(e) => { panic!("ERROR: {}",e); }
        }

        stdin.write_all(&bytes[..]).ok().expect("write failure b");
        stdin.flush().ok().expect("flush failure");
    }

    fn pull_internal_progress(&mut self,  internal: &mut Vec<CountMap<((), u64)>>,
                                          consumed: &mut Vec<CountMap<((), u64)>>,
                                          produced: &mut Vec<CountMap<((), u64)>>) -> bool
    {
        let stdout = (&mut self.process.stdout).as_mut().unwrap();

        // push some amount, then try decoding...
        match stdout.push(1024, &mut self.buffer) {
            Ok(_)  => { },
            Err(e) => { if e.kind != IoErrorKind::ResourceUnavailable { panic!("Pull error: {}", e) }
                        else { thread::yield_now(); }},
        }

        let buffer = replace(&mut self.buffer, Vec::new());
        let available = buffer.len();
        let mut reader = MemReader::new(buffer);
        let mut cursor = 0;

        let mut done = false;

        while !done {
            // println!("spinning furiously");
            let read = match reader.read_le_u64() {
                Ok(x) => x,
                Err(_) => { done = true; 0 }
            };

            if done || read + 8 + cursor > available as u64 { done = true; }
            else {
                for index in (0..internal.len()) {
                    let number = reader.read_le_u64().ok().expect("3");
                    for _ in (0..number) {
                        let time = reader.read_le_u64().ok().expect("4");
                        let delta = reader.read_le_i64().ok().expect("5");

                        internal[index].update(&((), time), delta);
                    }
                }

                for index in (0..consumed.len()) {
                    let number = reader.read_le_u64().ok().expect("6");
                    for _ in (0..number) {
                        let time = reader.read_le_u64().ok().expect("7");
                        let delta = reader.read_le_i64().ok().expect("8");

                        consumed[index].update(&((), time), delta);
                    }
                }

                for index in (0..produced.len()) {
                    let number = reader.read_le_u64().ok().expect("9");
                    for _ in (0..number) {
                        let time = reader.read_le_u64().ok().expect("10");
                        let delta = reader.read_le_i64().ok().expect("11");

                        produced[index].update(&((), time), delta);
                    }
                }

                cursor += 8 + read;
            }
        }

        self.buffer = reader.into_inner()[cursor as usize .. available].to_vec();
        return false;
    }

    fn name(&self) -> String { format!("Command") }
    fn notify_me(&self) -> bool { true }
}
