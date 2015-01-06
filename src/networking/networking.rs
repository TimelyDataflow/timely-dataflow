use std::io::{TcpListener, TcpStream};
use std::io::{Acceptor, Listener, IoResult, MemReader};
use std::io::timer::sleep;

use std::sync::mpsc::{Sender, Receiver, channel};

use std::thread::Thread;
use std::sync::{Arc, Future};
use std::mem;

use std::time::duration::Duration;

#[deriving(Copy)]
struct MessageHeader {
    graph:      u32,   // graph identifier
    channel:    u32,   // index of channel
    source:     u32,   // index of worker sending message
    target:     u32,   // index of worker receiving message
    length:     u32,   // number of bytes in message
}

impl MessageHeader {
    fn read_from<R: Reader>(reader: &mut R) -> IoResult<MessageHeader> {
        Ok(MessageHeader {
            graph:      try!(reader.read_le_u32()),
            channel:    try!(reader.read_le_u32()),
            source:     try!(reader.read_le_u32()),
            target:     try!(reader.read_le_u32()),
            length:     try!(reader.read_le_u32()),
        })
    }

    fn write_to<W: Writer>(&self, writer: &mut W) -> IoResult<()> {
        try!(writer.write_le_u32(self.graph));
        try!(writer.write_le_u32(self.channel));
        try!(writer.write_le_u32(self.source));
        try!(writer.write_le_u32(self.target));
        try!(writer.write_le_u32(self.length));

        Ok(())
    }
}

// structure in charge of receiving data from a Reader, most likely the network
struct BinaryReceiver<R: Reader> {
    // targets (and u8 returns) indexed by worker, graph, and channel.
    // option because they get filled progressively; alt design might change that.
    targets:    Vec<Vec<Vec<Option<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>>>>,

    reader:     R,          // the generic reader
    buffer:     Vec<u8>,    // current working buffer
    double:     Vec<u8>,    // second working buffer

    // how a BinaryReceiver learns about new channels; indices and corresponding channel pairs
    channels:   Receiver<((uint, uint, uint), Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
}

impl<R: Reader> BinaryReceiver<R>
{
    fn new(reader: R, targets: uint, channels: Receiver<((uint, uint, uint), Sender<Vec<u8>>, Receiver<Vec<u8>>)>) -> BinaryReceiver<R>
    {
        BinaryReceiver
        {
            targets:    range(0, targets).map(|_| Vec::new()).collect(),
            reader:     reader,
            buffer:     Vec::new(),
            double:     Vec::new(),
            channels:   channels,
        }
    }

    fn recv_loop(&mut self)
    {
        loop    // spin indefinitely
        {
            // push some amount into our buffer, then try decoding...
            match self.reader.push(1u << 20, &mut self.buffer)
            {
                Ok(_)  => { },
                Err(e) => { panic!("BinaryReceiver error: {}", e); },
            }

            let available = self.buffer.len() as u32;
            let mut reader = MemReader::new(mem::replace(&mut self.buffer, Vec::new()));
            let mut cursor = 0u32;

            let mut valid = true;   // true as long as we've not run out of data in self.buffer
            while valid
            {
                valid = false;
                // attempt to read a header out of the reader
                if let Ok(header) = MessageHeader::read_from(&mut reader)
                {
                    let h_tgt = header.target as uint;  // target worker
                    let h_grp = header.graph as uint;   // target graph
                    let h_chn = header.channel as uint; // target channel
                    let h_len = header.length as uint;  // length in bytes

                    // if we have at least a full message ...
                    if available - cursor > header.length + mem::size_of::<MessageHeader>() as u32
                    {
                        self.ensure(header.target, header.graph, header.channel);

                        let mut buffer = if let Ok(b) = self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().1.try_recv() { b }
                                         else { Vec::new() };
                        reader.push_at_least(h_len, h_len, &mut buffer).ok().expect("BinaryReader: payload read");

                        self.targets[h_tgt][h_grp][h_chn].as_ref().unwrap().0.send(buffer);
                        cursor += mem::size_of::<MessageHeader>() as u32 + header.length;
                        valid = true;
                    }
                }
            }

            // if we read
            if cursor > 0
            {
                reader.push_at_least((available - cursor) as uint, (available - cursor) as uint, &mut self.double).ok().expect("BinaryReadery");
                mem::swap(&mut self.buffer, &mut self.double);
                self.double = reader.into_inner();
                self.double.clear();
            }
        }
    }

    fn ensure(&mut self, target: u32, graph: u32, channel: u32)
    {
        while self.targets[target as uint].len() as u32 <= graph { self.targets[target as uint].push(Vec::new()); }
        while self.targets[target as uint][graph as uint].len() as u32 <= channel { self.targets[target as uint][graph as uint].push(None); }

        while let None = self.targets[target as uint][graph as uint][channel as uint]
        {
            // receive channel descriptions if any
            let ((t, g, c), s, r) = self.channels.recv().ok().expect("err");

            while self.targets[t as uint].len() <= g { self.targets[t as uint].push(Vec::new()); }
            while self.targets[t as uint][g as uint].len() <= c { self.targets[t as uint][g as uint].push(None); }

            self.targets[t as uint][g as uint][c as uint] = Some((s, r));
        }
    }
}

// structure in charge of sending data to a Writer, most likely the network
struct BinarySender<W: Writer>
{
    writer:     W,
    sources:    Receiver<(MessageHeader, Vec<u8>)>,
    buffers:    Vec<Vec<Vec<Option<Sender<Vec<u8>>>>>>,
    channels:   Receiver<((uint, uint, uint), Sender<Vec<u8>>)>,
}

impl<W: Writer> BinarySender<W>
{
    fn new(writer: W, targets: uint, sources: Receiver<(MessageHeader, Vec<u8>)>, channels: Receiver<((uint, uint, uint), Sender<Vec<u8>>)>) -> BinarySender<W>
    {
        BinarySender
        {
            writer:     writer,
            sources:    sources,
            buffers:    range(0, targets).map(|_| Vec::new()).collect(),
            channels:   channels,
        }
    }

    fn send_loop(&mut self)
    {
        for (header, mut buffer) in self.sources.iter()
        {
            header.write_to(&mut self.writer).ok().expect("BinarySender: header send failure");
            self.writer.write(buffer.as_slice()).ok().expect("BinarySender: payload send failure");
            buffer.clear();

            // inline because borrow-checker hate me
            let source = header.source;
            let graph = header.graph;
            let channel = header.channel;

            while self.buffers[source as uint].len() as u32 <= graph { self.buffers[source as uint].push(Vec::new()); }
            while self.buffers[source as uint][graph as uint].len() as u32 <= channel { self.buffers[source as uint][graph as uint].push(None); }

            while let None = self.buffers[source as uint][graph as uint][channel as uint]
            {
                let ((t, g, c), s) = self.channels.recv().ok().expect("error");

                while self.buffers[t as uint].len() <= g { self.buffers[t as uint].push(Vec::new()); }
                while self.buffers[t as uint][g as uint].len() <= c { self.buffers[t as uint][g as uint].push(None); }

                self.buffers[t as uint][g as uint][c as uint] = Some(s);
            }
            // end-inline

            self.buffers[header.source as uint][header.graph as uint][header.channel as uint].as_ref().unwrap().send(buffer);
        }
    }
}

pub fn initialize_networking(addresses: Vec<String>, my_index: uint, workers: uint) -> IoResult<BinaryChannelAllocator>
{
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
            Thread::spawn(move || sender.send_loop()).detach();
            Thread::spawn(move || recver.recv_loop()).detach();
        }
    }

    return Ok(BinaryChannelAllocator {
        allocated:      0,
        writers:        writers,
        readers:        readers,
        writer_senders: senders,
    });
}

fn start_connections(addresses: Arc<Vec<String>>, my_index: uint) -> IoResult<Vec<Option<TcpStream>>>
{
    // contains connections [0, my_index - 1].
    let mut results: Vec<_> = range(0, my_index).map(|_| None).collect();

    // connect to each worker in turn.
    for index in range(0, my_index)
    {
        let mut connected = false;

        while !connected
        {
            match TcpStream::connect_timeout(addresses[index].as_slice(), Duration::minutes(1))
            {
                Ok(mut stream) =>
                {
                    try!(stream.write_le_uint(my_index));

                    results[index] = Some(stream);

                    println!("worker {}:\tconnection to worker {}", my_index, index);
                    connected = true;
                },
                Err(error) =>
                {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep(Duration::seconds(1));
                },
            }
        }
    }

    return Ok(results);
}

fn await_connections(addresses: Arc<Vec<String>>, my_index: uint) -> IoResult<Vec<Option<TcpStream>>>
{
    // contains connections [my_index + 1, addresses.len() - 1].
    let mut results: Vec<_> = range(0, addresses.len() - my_index).map(|_| None).collect();

    // listen for incoming connections
    let listener = TcpListener::bind(addresses[my_index].as_slice());

    let mut acceptor = try!(listener.listen());
    for _ in range(my_index + 1, addresses.len())
    {
        let mut stream = try!(acceptor.accept());
        let identifier = try!(stream.read_le_uint());

        results[identifier - my_index - 1] = Some(stream);

        println!("worker {}:\tconnection from worker {}", my_index, identifier);
    }

    return Ok(results);
}

#[deriving(Clone)]
pub struct BinaryChannelAllocator
{
    allocated:      uint,                           // indicates how many have been allocated (locally).

    // for loading up state in the networking threads.
    writers:        Vec<Sender<((uint, uint, uint), Sender<Vec<u8>>)>>,                     // (index, back-to-worker)
    readers:        Vec<Sender<((uint, uint, uint), Sender<Vec<u8>>, Receiver<Vec<u8>>)>>,  // (index, data-to-worker, back-from-worker)

    writer_senders: Vec<Sender<(MessageHeader, Vec<u8>)>>
}

impl BinaryChannelAllocator
{
    // returns: (send to net <--> back from net), then (back to net <--> recv from net)
    pub fn new_channel(&mut self, index: uint, graph:uint) -> (Vec<Sender<(MessageHeader, Vec<u8>)>>, Receiver<Vec<u8>>,
                                                               Vec<Sender<Vec<u8>>>,                  Receiver<Vec<u8>>)
    {
        let mut send_to_net = Vec::new();
        let mut back_to_net = Vec::new();

        let (back_to_worker, back_from_net) = channel();
        let (send_to_worker, recv_from_net) = channel();

        for sender in self.writer_senders.iter() {
            send_to_net.push(sender.clone());
        }

        for writer in self.writers.iter() {
            writer.send(((index, graph, self.allocated), back_to_worker.clone()));
        }

        for reader in self.readers.iter() {
            let (b2n, bfw) = channel();
            back_to_net.push(b2n);
            reader.send(((index, graph, self.allocated), send_to_worker.clone(), bfw)).ok().expect("send error")
        }

        self.allocated += 1;

        return (send_to_net, back_from_net, back_to_net, recv_from_net);
    }
}
