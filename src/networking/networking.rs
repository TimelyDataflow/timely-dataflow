use std::old_io::{TcpListener, TcpStream};
use std::old_io::{Acceptor, Listener, IoResult, MemReader};
use std::old_io::timer::sleep;

use std::sync::mpsc::{Sender, Receiver, channel};

use std::thread;
use std::sync::{Arc, Future};
use std::mem;
use std::time::duration::Duration;

use communication::{Pushable, Communicator, BinaryCommunicator, ProcessCommunicator};

// TODO : Much of this only relates to BinaryWriter/BinaryReader based communication, not networking.
// TODO : Could be moved somewhere less networking-specific.

#[derive(Copy)]
pub struct MessageHeader {
    pub graph:      u64,   // graph identifier
    pub channel:    u64,   // index of channel
    pub source:     u64,   // index of worker sending message
    pub target:     u64,   // index of worker receiving message
    pub length:     u64,   // number of bytes in message
}

impl MessageHeader {
    fn read_from<R: Reader>(reader: &mut R) -> IoResult<MessageHeader> {
        Ok(MessageHeader {
            graph:      try!(reader.read_le_u64()),
            channel:    try!(reader.read_le_u64()),
            source:     try!(reader.read_le_u64()),
            target:     try!(reader.read_le_u64()),
            length:     try!(reader.read_le_u64()),
        })
    }

    fn write_to<W: Writer>(&self, writer: &mut W) -> IoResult<()> {
        try!(writer.write_le_u64(self.graph));
        try!(writer.write_le_u64(self.channel));
        try!(writer.write_le_u64(self.source));
        try!(writer.write_le_u64(self.target));
        try!(writer.write_le_u64(self.length));
        Ok(())
    }
}

// structure in charge of receiving data from a Reader, for example the network
struct BinaryReceiver<R: Reader> {
    // targets (and u8 returns) indexed by worker, graph, and channel.
    // option because they get filled progressively; alt design might change that.
    targets:    Vec<Vec<Vec<Option<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>>,

    reader:     R,          // the generic reader
    buffer:     Vec<u8>,    // current working buffer
    double:     Vec<u8>,    // second working buffer

    // how a BinaryReceiver learns about new channels; indices and corresponding channel pairs
    channels:   Receiver<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
}

impl<R: Reader> BinaryReceiver<R> {
    fn new(reader: R, targets: u64, channels: Receiver<((u64, u64, u64), Sender<Vec<u8>>, Receiver<Vec<u8>>)>) -> BinaryReceiver<R> {
        BinaryReceiver {
            targets:    range(0, targets).map(|_| Vec::new()).collect(),
            reader:     reader,
            buffer:     Vec::new(),
            double:     Vec::new(),
            channels:   channels,
        }
    }

    fn recv_loop(&mut self) {
        loop {
            // push some amount into our buffer, then try decoding...
            self.reader.push(1usize << 20, &mut self.buffer).unwrap();

            let available = self.buffer.len() as u64;
            let mut reader = MemReader::new(mem::replace(&mut self.buffer, Vec::new()));
            let mut cursor = 0u64;

            let mut valid = true;   // true as long as we've not run out of data in self.buffer
            while valid {
                valid = false;
                // attempt to read a header out of the reader
                if let Ok(header) = MessageHeader::read_from(&mut reader) {
                    cursor += mem::size_of::<MessageHeader>() as u64;

                    let h_tgt = header.target as usize;  // target worker
                    let h_grp = header.graph as usize;   // target graph
                    let h_chn = header.channel as usize; // target channel
                    let h_len = header.length as usize;  // length in bytes

                    // if we have at least a full message ...
                    if available - cursor >= header.length {
                        self.ensure(header.target, header.graph, header.channel);

                        let mut buffer = if let Ok(b) = self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().1.try_recv() { b }
                                         else { Vec::new() };

                        reader.push_at_least(h_len, h_len, &mut buffer).unwrap();
                        cursor += header.length;

                        self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().0.send(buffer).unwrap();
                        valid = true;
                    }
                    else {
                        cursor -= mem::size_of::<MessageHeader>() as u64;
                    }
                }
            }

            // TODO: way inefficient... =/ Fix! :D
            self.buffer = reader.into_inner();
            if cursor > 0 {
                for index in (0..(available - cursor)) {
                    self.buffer[index as usize] = self.buffer[(cursor + index) as usize];
                }

                self.buffer.truncate((available - cursor) as usize);
            }

            // thread::yield_now();
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
struct BinarySender<W: Writer> {
    writer:     W,
    sources:    Receiver<(MessageHeader, Vec<u8>)>,
    buffers:    Vec<Vec<Vec<Option<Sender<Vec<u8>>>>>>,
    channels:   Receiver<((u64, u64, u64), Sender<Vec<u8>>)>,
}

impl<W: Writer> BinarySender<W> {
    fn new(writer: W,
           targets: u64,
           sources: Receiver<(MessageHeader, Vec<u8>)>,
           channels: Receiver<((u64, u64, u64), Sender<Vec<u8>>)>) -> BinarySender<W> {
        BinarySender {
            writer:     writer,
            sources:    sources,
            buffers:    range(0, targets).map(|_| Vec::new()).collect(),
            channels:   channels,
        }
    }

    fn send_loop(&mut self) {
        println!("send loop:\tstarting");
        for (mut header, mut buffer) in self.sources.iter() {
            // println!("send loop:\treceived data");
            header.length = buffer.len() as u64;
            // println!("sending {} bytes", header.length);
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

pub fn initialize_networking(addresses: Vec<String>, my_index: u64, workers: u64) -> IoResult<Vec<Communicator>> {

    let processes = addresses.len() as u64;
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let mut start_task = Future::spawn(move || start_connections(hosts1, my_index));
    let mut await_task = Future::spawn(move || await_connections(hosts2, my_index));

    let mut results = try!(await_task.get());

    results.push(None);
    results.push_all(try!(start_task.get()).as_slice());

    println!("worker {}:\tinitialization complete", my_index);

    let mut writers = Vec::new();   // handles to the BinarySenders (to present new channels)
    let mut readers = Vec::new();   // handles to the BinaryReceivers (to present new channels)
    let mut senders = Vec::new();   // destinations for serialized data (to send serialized data)

    // for each process, if a stream exists (i.e. not local) ...
    for index in range(0, results.len()) {
        if let Some(stream) = results[index].take() {
            let (writer_channels_s, writer_channels_r) = channel();
            let (reader_channels_s, reader_channels_r) = channel();
            let (sender_channels_s, sender_channels_r) = channel();

            writers.push(writer_channels_s);    //
            readers.push(reader_channels_s);    //
            senders.push(sender_channels_s);    //

            let mut sender = BinarySender::new(stream.clone(), workers, sender_channels_r, writer_channels_r);
            let mut recver = BinaryReceiver::new(stream.clone(), workers, reader_channels_r);

            // start senders and receivers associated with this stream
            thread::Builder::new().name(format!("send thread {}", index))
                                  .spawn(move || sender.send_loop())
                                  .ok();
            thread::Builder::new().name(format!("recv thread {}", index))
                                  .spawn(move || recver.recv_loop())
                                  .ok();

        }
    }

    let proc_comms = ProcessCommunicator::new_vector(workers);

    let mut results = Vec::new();
    for (index, proc_comm) in proc_comms.into_iter().enumerate() {
        results.push(Communicator::Binary(Box::new(BinaryCommunicator {
            inner:          proc_comm,
            index:          my_index * workers + index as u64,
            peers:          workers * processes,
            graph:          0,          // TODO : Fix this
            allocated:      0,
            writers:        writers.clone(),
            readers:        readers.clone(),
            writer_senders: senders.clone(),
        })));
    }

    return Ok(results);
}

// result contains connections [0, my_index - 1].
fn start_connections(addresses: Arc<Vec<String>>, my_index: u64) -> IoResult<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();
    for index in (0..my_index) {
        let mut connected = false;
        while !connected {
            match TcpStream::connect_timeout(addresses[index as usize].as_slice(), Duration::minutes(1)) {
                Ok(mut stream) => {
                    try!(stream.write_le_u64(my_index));
                    results[index as usize] = Some(stream);
                    println!("worker {}:\tconnection to worker {}", my_index, index);
                    connected = true;
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep(Duration::seconds(1));
                },
            }
        }
    }

    return Ok(results);
}

// result contains connections [my_index + 1, addresses.len() - 1].
fn await_connections(addresses: Arc<Vec<String>>, my_index: u64) -> IoResult<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index as usize - 1)).map(|_| None).collect();
    let listener = TcpListener::bind(addresses[my_index as usize].as_slice());

    let mut acceptor = try!(listener.listen());
    for _ in (my_index as usize + 1 .. addresses.len()) {
        let mut stream = try!(acceptor.accept());
        let identifier = try!(stream.read_le_uint());
        results[identifier - my_index as usize - 1] = Some(stream);
        println!("worker {}:\tconnection from worker {}", my_index, identifier);
    }

    return Ok(results);
}
