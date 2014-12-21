
extern crate libc;

use std::mem::replace;

use std::os::unix::prelude::AsRawFd;

use std::io::{MemWriter, MemReader, IoErrorKind};
use std::io::process::{Command, Process};

use std::default::Default;
use progress::frontier::Antichain;

use progress::{PathSummary, Scope};
use communication::channels::{Data};
use example::stream::Stream;
use communication::channels::OutputPort;
use progress::count_map::CountMap;


use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

pub trait CommandExtensionTrait { fn command(&mut self, program: String) -> Self; }

impl<S, D> CommandExtensionTrait for Stream<((), uint), S, D>
where S: PathSummary<((), uint)>,
      D: Data
{
    fn command(&mut self, program: String) -> Stream<((), uint), S, D>
    {
        let mut process = match Command::new(program.clone()).spawn()
        {
            Ok(p) => p,
            Err(e) => panic!("Process creation error: {}; program: {}", e, program),
        };


        let fd1 = (&mut process.stdout).as_mut().unwrap().as_raw_fd();
        let fd2 = (&mut process.stdin).as_mut().unwrap().as_raw_fd();

        let command = CommandScope { process: process, buffer: Vec::new() };

        // set stdin and stdout to be non-blocking
        unsafe { libc::fcntl(fd1, libc::F_SETFL, libc::O_NONBLOCK); }
        unsafe { libc::fcntl(fd2, libc::F_SETFL, libc::O_NONBLOCK); }

        let index = self.graph.add_scope(box command);

        self.graph.connect(self.name, ScopeInput(index, 0));

        return self.copy_with(ScopeOutput(index, 0), OutputPort::new().targets);
    }
}

struct CommandScope
{
    process:    Process,
    buffer:     Vec<u8>,
}

impl<S: PathSummary<((), uint)>> Scope<((), uint), S> for CommandScope
{
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(((), uint), i64)>>)
    {
        let mut internal = Vec::from_fn(1, |_| Vec::new());

        let stdout = (&mut self.process.stdout).as_mut().unwrap();

        let mut length_buf = Vec::from_elem(8, 0u8);

        let mut read = 0;
        while read < 8
        {
            read += match stdout.read(length_buf.slice_mut(read, 8))
            {
                Ok(len) => len,
                Err(_err) => 0,
            };
        }

        let expected = MemReader::new(length_buf.clone()).read_le_uint().ok().expect("gis read all");

        // println!("Command: expecting {} bytes", expected);

        let read_bytes = stdout.read_exact(expected).ok().expect("");

        // println!("Read them!");

        let mut reader = MemReader::new(read_bytes);

        for index in range(0, internal.len())
        {
            let number = reader.read_le_uint().ok().expect("");
            for _ in range(0, number)
            {
                let time = reader.read_le_uint().ok().expect("");
                let delta = reader.read_le_i64().ok().expect("");

                // println!("Command: intializing {} : {}", time, delta);

                internal[index].update(((), time), delta);
            }
        }

        // println!("Command: Done init!");

        (Vec::from_fn(1, |_| Vec::from_fn(1, |_| Antichain::from_elem(Default::default()))),
         internal)
    }

    // Reports (out -> in) summaries for the vertex, and initial frontier information.
    // TODO: Update this to be summaries along paths external to the vertex, as this is strictly more informative.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<S>>>, _frontier: &Vec<Vec<(((), uint), i64)>>) -> ()
    {

    }


    fn push_external_progress(&mut self, external: &Vec<Vec<(((), uint), i64)>>) -> ()
    {
        // println!("Command: in push");

        let mut writer = MemWriter::new();

        for index in range(0, external.len())
        {
            writer.write_le_uint(external[index].len()).ok().expect("a");
            for &(((), time), delta) in external[index].iter()
            {
                writer.write_le_uint(time).ok().expect("b");
                writer.write_le_i64(delta).ok().expect("c");

                // println!("Command: pushing {} : {}", time, delta);
            }
        }

        let bytes = writer.into_inner();

        let stdin = (&mut self.process.stdin).as_mut().unwrap();

        match stdin.write_le_uint(bytes.len())
        {
            Ok(_) => {},
            Err(e) => { panic!("ERROR: {}",e); }
        }
            // .ok().expect("write failure a");
        stdin.write(bytes.as_slice()).ok().expect("write failure b");
        stdin.flush().ok().expect("flush failure");
    }

    fn pull_internal_progress(&mut self,  internal: &mut Vec<Vec<(((), uint), i64)>>,
                                          consumed: &mut Vec<Vec<(((), uint), i64)>>,
                                          produced: &mut Vec<Vec<(((), uint), i64)>>) -> bool
    {
        let stdout = (&mut self.process.stdout).as_mut().unwrap();

        // push some amount, then try decoding...
        match stdout.push(1024, &mut self.buffer)
        {
            Ok(_)  => { },
            Err(e) => { if e.kind != IoErrorKind::ResourceUnavailable { panic!("Pull error: {}", e) } },
        }

        let buffer = replace(&mut self.buffer, Vec::new());
        let available = buffer.len();
        let mut reader = MemReader::new(buffer);
        let mut cursor = 0;

        let mut done = false;

        while !done
        {
            // println!("spinning furiously");
            let read = match reader.read_le_uint()
            {
                Ok(x) => x,
                Err(_) => { done = true; 0 }
            };

            if done || read + 8 + cursor > available
            {
                done = true;
            }
            else
            {
                for index in range(0, internal.len())
                {
                    let number = reader.read_le_uint().ok().expect("3");
                    for _ in range(0, number)
                    {
                        let time = reader.read_le_uint().ok().expect("4");
                        let delta = reader.read_le_i64().ok().expect("5");

                        internal[index].update(((), time), delta);
                    }
                }

                for index in range(0, consumed.len())
                {
                    let number = reader.read_le_uint().ok().expect("6");
                    for _ in range(0, number)
                    {
                        let time = reader.read_le_uint().ok().expect("7");
                        let delta = reader.read_le_i64().ok().expect("8");

                        consumed[index].update(((), time), delta);
                    }
                }

                for index in range(0, produced.len())
                {
                    let number = reader.read_le_uint().ok().expect("9");
                    for _ in range(0, number)
                    {
                        let time = reader.read_le_uint().ok().expect("10");
                        let delta = reader.read_le_i64().ok().expect("11");

                        produced[index].update(((), time), delta);
                    }
                }

                cursor += 8 + read;
            }
        }

        // println!("resting");

        self.buffer = reader.into_inner().slice(cursor, available).to_vec();

        return false;
    }

    fn name(&self) -> String { format!("Command") }
    fn notify_me(&self) -> bool { true }
}
