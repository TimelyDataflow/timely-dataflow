// use std::old_io::{TcpListener, TcpStream};
// use std::old_io::{Acceptor, Listener, IoResult, MemReader};
use std::thread::sleep_ms;
use std::io::{Read, Write, Result, BufRead, BufReader};
use std::fs::File;


use std::net::{TcpListener, TcpStream};
use std::mem::size_of;

use std::sync::mpsc::{Sender, Receiver, channel};

use std::thread;
use std::sync::Arc;
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use communication::{Pushable, BinaryCommunicator, ProcessCommunicator};
use drain::DrainExt;

// TODO : Much of this only relates to BinaryWriter/BinaryReader based communication, not networking.
// TODO : Could be moved somewhere less networking-specific.

#[derive(Copy, Clone)]
pub struct MessageHeader {
    pub graph:      u64,   // graph identifier
    pub channel:    u64,   // index of channel
    pub source:     u64,   // index of worker sending message
    pub target:     u64,   // index of worker receiving message
    pub length:     u64,   // number of bytes in message
}

impl MessageHeader {
    // returns a header when there is enough supporting data
    fn try_read(bytes: &mut &[u8]) -> Option<MessageHeader> {
        if bytes.len() > size_of::<MessageHeader>() {
            // capture original in case we need to rewind
            let original = *bytes;

            // unclear what order struct initializers run in, so ...
            let graph = bytes.read_u64::<LittleEndian>().unwrap();
            let channel = bytes.read_u64::<LittleEndian>().unwrap();
            let source = bytes.read_u64::<LittleEndian>().unwrap();
            let target = bytes.read_u64::<LittleEndian>().unwrap();
            let length = bytes.read_u64::<LittleEndian>().unwrap();

            let header = MessageHeader {
                graph: graph,
                channel: channel,
                source: source,
                target: target,
                length: length,
            };

            if bytes.len() >= header.length as usize {
                Some(header)
            }
            else {
                // rewind the reader
                *bytes = original;
                None
            }
        }
        else { None }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        try!(writer.write_u64::<LittleEndian>(self.graph));
        try!(writer.write_u64::<LittleEndian>(self.channel));
        try!(writer.write_u64::<LittleEndian>(self.source));
        try!(writer.write_u64::<LittleEndian>(self.target));
        try!(writer.write_u64::<LittleEndian>(self.length));
        Ok(())
    }
}

// structure in charge of receiving data from a Reader, for example the network
struct BinaryReceiver<R: Read> {
    // targets (and u8 returns) indexed by worker, graph, and channel.
    // option because they get filled progressively; alt design might change that.
    targets:    Vec<Vec<Vec<Option<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>>,

    reader:     R,          // the generic reader
    buffer:     Vec<u8>,    // current working buffer
    double:     Vec<u8>,    // second working buffer

    staging:    Vec<u8>,    // 1 << 20 of buffer to read into

    // how a BinaryReceiver learns about new channels; indices and corresponding channel pairs
    channels:   Receiver<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
}

impl<R: Read> BinaryReceiver<R> {
    fn new(reader: R, targets: u64, channels: Receiver<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>) -> BinaryReceiver<R> {
        BinaryReceiver {
            targets:    (0..targets).map(|_| Vec::new()).collect(),
            reader:     reader,
            buffer:     Vec::new(),
            double:     Vec::new(),
            channels:   channels,

            staging:    vec![0u8; 1 << 20],
        }
    }

    fn recv_loop(&mut self) {
        loop {

            // attempt to read some more bytes into our buffer
            // TODO : We read in to self.staging because extending a Vec<u8> is hard without
            // TODO : using set_len, which is unsafe.
            let read = self.reader.read(&mut self.staging[..]).unwrap_or(0);

            // writing to a Vec<u8> isn't supposed to fail.
            self.buffer.write_all(&self.staging[..read]).unwrap();

            {
                // get a view of available bytes
                let mut slice = &self.buffer[..];

                while let Some(header) = MessageHeader::try_read(&mut slice) {
                    let h_tgt = header.target as usize;  // target worker
                    let h_grp = header.graph as usize;   // target graph
                    let h_chn = header.channel as usize; // target channel
                    let h_len = header.length as usize;  // length in bytes

                    // println!("looking for {} bytes; have {} bytes", h_len, slice.len());

                    while self.targets.len() <= h_tgt { self.targets.push(Vec::new()) ;}
                    while self.targets[h_tgt].len() <= h_grp { self.targets[h_tgt].push(Vec::new()) ;}
                    while self.targets[h_tgt][h_grp].len() <= h_chn { self.targets[h_tgt][h_grp].push(None) ;}

                    // ensure that the destination exists!
                    while let None = self.targets[h_tgt][h_grp][h_chn] {
                        // receive channel descriptions if any
                        let ((t, g, c), s, r) = self.channels.recv().unwrap();
                        while self.targets.len() as u64 <= t { self.targets.push(Vec::new()); }
                        while self.targets[t as usize].len() as u64 <= g { self.targets[t as usize].push(Vec::new()); }
                        while self.targets[t as usize][g as usize].len() as u64 <= c { self.targets[t as usize][g as usize].push(None); }

                        self.targets[t as usize][g as usize][c as usize] = Some((s, r));
                    }

                    let mut buffer = if let Ok(b) = self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().1.try_recv() { b }
                                     else { Vec::new() };

                    buffer.clear();
                    buffer.write_all(&slice[..h_len]).unwrap();

                    slice = &slice[h_len..];

                    self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().0.send(buffer).unwrap();
                }

                // TODO: way inefficient... =/ Fix! :D
                self.double.clear();
                self.double.write_all(slice).unwrap();
            }

            mem::swap(&mut self.buffer, &mut self.double);
        }
    }

    fn ensure(&mut self, target: u64, graph: u64, channel: u64) {
        // println!("starting ensure({}, {}, {})", target, graph, channel);

        while self.targets.len() as u64 <= target { self.targets.push(Vec::new()); }
        while self.targets[target as usize].len() as u64 <= graph { self.targets[target as usize].push(Vec::new()); }
        while self.targets[target as usize][graph as usize].len() as u64 <= channel {
            self.targets[target as usize][graph as usize].push(None);
        }

        while let None = self.targets[target as usize][graph as usize][channel as usize] {
            // receive channel descriptions if any
            let ((t, g, c), s, r) = self.channels.recv().unwrap();
            while self.targets.len() as u64 <= t { self.targets.push(Vec::new()); }
            while self.targets[t as usize].len() as u64 <= g { self.targets[t as usize].push(Vec::new()); }
            while self.targets[t as usize][g as usize].len() as u64 <= c { self.targets[t as usize][g as usize].push(None); }

            self.targets[t as usize][g as usize][c as usize] = Some((s, r));
        }

        // println!("exiting ensure()");
    }
}

// structure in charge of sending data to a Writer, for example the network
struct BinarySender<W: Write> {
    writer:     W,
    sources:    Receiver<(MessageHeader, Vec<u8>)>,
    buffers:    Vec<Vec<Vec<Option<Sender<Vec<u8>>>>>>,
    channels:   Receiver<((u64, u64, u64), Sender<Vec<u8>>)>,
}

impl<W: Write> BinarySender<W> {
    fn new(writer: W,
           targets: u64,
           sources: Receiver<(MessageHeader, Vec<u8>)>,
           channels: Receiver<((u64, u64, u64), Sender<Vec<u8>>)>) -> BinarySender<W> {
        BinarySender {
            writer:     writer,
            sources:    sources,
            buffers:    vec![Vec::new(); targets as usize],
            channels:   channels,
        }
    }

    fn send_loop(&mut self) {
        println!("send loop:\tstarting");
        for (mut header, mut buffer) in self.sources.iter() {
            header.length = buffer.len() as u64;
            header.write_to(&mut self.writer).unwrap();
            self.writer.write_all(&buffer[..]).unwrap();
            buffer.clear();

            // inline because borrow-checker hates me
            let source = header.source as usize;
            let graph = header.graph as usize;
            let channel = header.channel as usize;

            while self.buffers.len() <= source { self.buffers.push(Vec::new()); }
            while self.buffers[source].len() <= graph { self.buffers[source].push(Vec::new()); }
            while self.buffers[source][graph].len() <= channel {
                self.buffers[source][graph].push(None);
            }

            while let None = self.buffers[source][graph][channel] {
                let ((t, g, c), s) = self.channels.recv().unwrap();
                while self.buffers.len() as u64 <= t { self.buffers.push(Vec::new()); }
                while self.buffers[t as usize].len() as u64 <= g { self.buffers[t as usize].push(Vec::new()); }
                while self.buffers[t as usize][g as usize].len() as u64 <= c { self.buffers[t as usize][g as usize].push(None); }
                self.buffers[t as usize][g as usize][c as usize] = Some(s);
            }
            // end-inline

            self.buffers[source][graph][channel].as_ref().unwrap().send(buffer).unwrap();
        }
    }
}

pub fn initialize_networking_from_file(filename: &str, my_index: u64, workers: u64) -> Result<Vec<BinaryCommunicator>> {

    let reader = BufReader::new(try!(File::open(filename)));
    let mut addresses = Vec::new();

    for line in reader.lines() {
        addresses.push(try!(line));
    }

    // println!("addresses.len() : {} vs workers : {}", addresses.len(), workers);
    // assert!(addresses.len() as u64 == workers);
    initialize_networking(addresses, my_index, workers)
}

pub fn initialize_networking(addresses: Vec<String>, my_index: u64, workers: u64) -> Result<Vec<BinaryCommunicator>> {

    let processes = addresses.len() as u64;
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let start_task = thread::spawn(move || start_connections(hosts1, my_index));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index));

    let mut results = try!(await_task.join().unwrap());

    results.push(None);
    let mut to_extend = try!(start_task.join().unwrap());
    results.extend(to_extend.drain_temp());

    println!("worker {}:\tinitialization complete", my_index);

    let mut writers = Vec::new();   // handles to the BinarySenders (to present new channels)
    let mut readers = Vec::new();   // handles to the BinaryReceivers (to present new channels)
    let mut senders = Vec::new();   // destinations for serialized data (to send serialized data)

    // for each process, if a stream exists (i.e. not local) ...
    for index in (0..results.len()) {
        if let Some(stream) = results[index].take() {
            let (writer_channels_s, writer_channels_r) = channel();
            let (reader_channels_s, reader_channels_r) = channel();
            let (sender_channels_s, sender_channels_r) = channel();

            writers.push(writer_channels_s);    //
            readers.push(reader_channels_s);    //
            senders.push(sender_channels_s);    //

            let mut sender = BinarySender::new(stream.try_clone().unwrap(), workers, sender_channels_r, writer_channels_r);
            let mut recver = BinaryReceiver::new(stream.try_clone().unwrap(), workers, reader_channels_r);

            // start senders and receivers associated with this stream
            thread::Builder::new().name(format!("send thread {}", index))
                                  .spawn(move || sender.send_loop())
                                  .unwrap();
            thread::Builder::new().name(format!("recv thread {}", index))
                                  .spawn(move || recver.recv_loop())
                                  .unwrap();

        }
    }

    let proc_comms = ProcessCommunicator::new_vector(workers);

    let mut results = Vec::new();
    for (index, proc_comm) in proc_comms.into_iter().enumerate() {
        results.push(BinaryCommunicator {
            inner:          proc_comm,
            index:          my_index * workers + index as u64,
            peers:          workers * processes,
            graph:          0,          // TODO : Fix this
            allocated:      0,
            writers:        writers.clone(),
            readers:        readers.clone(),
            senders:        senders.clone(),
        });
    }

    return Ok(results);
}

// result contains connections [0, my_index - 1].
fn start_connections(addresses: Arc<Vec<String>>, my_index: u64) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();
    for index in (0..my_index) {
        let mut connected = false;
        while !connected {
            match TcpStream::connect(&addresses[index as usize][..]) {
                Ok(mut stream) => {
                    try!(stream.write_u64::<LittleEndian>(my_index));
                    results[index as usize] = Some(stream);
                    println!("worker {}:\tconnection to worker {}", my_index, index);
                    connected = true;
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep_ms(1000);
                },
            }
        }
    }

    return Ok(results);
}

// result contains connections [my_index + 1, addresses.len() - 1].
fn await_connections(addresses: Arc<Vec<String>>, my_index: u64) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index as usize - 1)).map(|_| None).collect();
    let listener = try!(TcpListener::bind(&addresses[my_index as usize][..]));

    for _ in (my_index as usize + 1 .. addresses.len()) {
        let mut stream = try!(listener.accept()).0;
        let identifier = try!(stream.read_u64::<LittleEndian>()) as usize;
        results[identifier - my_index as usize - 1] = Some(stream);
        println!("worker {}:\tconnection from worker {}", my_index, identifier);
    }

    return Ok(results);
}
