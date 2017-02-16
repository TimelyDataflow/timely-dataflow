//! Networking code for sending and receiving fixed size `Vec<u8>` between machines.

use std::io::{Read, Write, Result, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::mem::size_of;
use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

// TODO : Would be nice to remove this dependence
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use allocator::{Process, Binary};
use drain::DrainExt;

// TODO : Much of this only relates to BinaryWriter/BinaryReader based communication, not networking.
// TODO : Could be moved somewhere less networking-specific.

/// Framing data for each `Vec<u8>` transmission, indicating a typed channel, the source and
/// destination workers, and the length in bytes.
#[derive(Copy, Clone)]
pub struct MessageHeader {
    pub channel:    usize,   // index of channel
    pub source:     usize,   // index of worker sending message
    pub target:     usize,   // index of worker receiving message
    pub length:     usize,   // number of bytes in message
    pub seqno:      usize,   // sequence number
}

impl MessageHeader {
    // returns a header when there is enough supporting data
    fn try_read(bytes: &mut &[u8]) -> Option<MessageHeader> {
        if bytes.len() > size_of::<MessageHeader>() {
            // capture original in case we need to rewind
            let original = *bytes;

            // unclear what order struct initializers run in, so ...
            let channel = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let source = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let target = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let length = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let seqno  = bytes.read_u64::<LittleEndian>().unwrap() as usize;

            if bytes.len() >= length {
                Some(MessageHeader {
                    channel: channel,
                    source: source,
                    target: target,
                    length: length,
                    seqno: seqno,
                })
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
        try!(writer.write_u64::<LittleEndian>(self.channel as u64));
        try!(writer.write_u64::<LittleEndian>(self.source as u64));
        try!(writer.write_u64::<LittleEndian>(self.target as u64));
        try!(writer.write_u64::<LittleEndian>(self.length as u64));
        try!(writer.write_u64::<LittleEndian>(self.seqno as u64));
        Ok(())
    }
}

// structure in charge of receiving data from a Reader, for example the network
struct BinaryReceiver<R: Read> {
    reader:     R,          // the generic reader
    buffer:     Vec<u8>,    // current working buffer
    length:     usize,
    targets:    Switchboard<Sender<Vec<u8>>>,
    process:    usize, // process ID this receiver belongs to
    index:      usize, // receiver index
    threads:    usize,
    logger:     Option<::logging::LogSender>,
}

impl<R: Read> BinaryReceiver<R> {
    fn new(
            reader: R,
            channels: Receiver<((usize,
            usize),
            Sender<Vec<u8>>)>,
            process: usize,
            index: usize,
            threads: usize,
            logger: Option<::logging::LogSender>) -> BinaryReceiver<R> {
        eprintln!("process {}, index {}, threads {}", process, index, threads);
        BinaryReceiver {
            reader:     reader,
            buffer:     vec![0u8; 1 << 20],
            length:     0,
            targets:    Switchboard::new(channels),
            process:    process,
            index:      index,
            threads:    threads,
            logger:     logger,
        }
    }

    fn recv_loop(&mut self) {
        ::logging::initialize(self.process, false, self.index, self.logger.clone());
        loop {

            // if we've mostly filled our buffer and still can't read a whole message from it,
            // we'll need more space / to read more at once. let's double the buffer!
            if self.length >= self.buffer.len() / 2 {
                self.buffer.extend(::std::iter::repeat(0u8).take(self.length));
            }

            // attempt to read some more bytes into our buffer
            let read = self.reader.read(&mut self.buffer[self.length..]).unwrap_or(0);
            self.length += read;

            let remaining = {
                let mut slice = &self.buffer[..self.length];
                while let Some(header) = MessageHeader::try_read(&mut slice) {
                    ::logging::log(&::logging::COMMUNICATION,
                                   ::logging::CommunicationEvent {
                                       is_send: false,
                                       comm_channel: header.channel,
                                       source: header.source,
                                       target: header.target,
                                       seqno: header.seqno,
                    });
                    let h_len = header.length as usize;  // length in bytes
                    let target = &mut self.targets.ensure(header.target, header.channel);
                    target.send(slice[..h_len].to_vec()).unwrap();
                    slice = &slice[h_len..];
                }

                slice.len()
            };

            // we consumed bytes, must shift to beginning.
            // this should optimize to copy_overlapping;
            // would just do that if it weren't unsafe =/
            if remaining < self.length {
                let offset = self.length - remaining;
                for index in 0..remaining {
                    self.buffer[index] = self.buffer[index + offset];
                }
                self.length = remaining;
            }
        }
    }
}

// structure in charge of sending data to a Writer, for example the network
struct BinarySender<W: Write> {
    writer:     W,
    sources:    Receiver<(MessageHeader, Vec<u8>)>,
    process:    usize, // process ID this sender belongs to
    index:      usize, // sender index
    logger:     Option<::logging::LogSender>,
}

impl<W: Write> BinarySender<W> {
    fn new(writer: W, sources: Receiver<(MessageHeader, Vec<u8>)>, process: usize, index: usize, logger: Option<::logging::LogSender>) -> BinarySender<W> {
        BinarySender {
            writer:     writer,
            sources:    sources,
            process:    process,
            index:      index,
            logger:     logger,
        }
    }

    fn send_loop(&mut self) {
        ::logging::initialize(self.process, true, self.index, self.logger.clone());
        let mut stash = Vec::new();

        // block until data to recv
        while let Ok((header, buffer)) = self.sources.recv() {

            stash.push((header, buffer));

            // collect any additional outstanding data to send
            while let Ok((header, buffer)) = self.sources.try_recv() {
                stash.push((header, buffer));
            }

            for (header, mut buffer) in stash.drain_temp() {
                assert!(header.length == buffer.len());
                ::logging::log(&::logging::COMMUNICATION,
                               ::logging::CommunicationEvent {
                                   is_send: true,
                                   comm_channel: header.channel,
                                   source: header.source,
                                   target: header.target,
                                   seqno: header.seqno,
                });
                header.write_to(&mut self.writer).unwrap();
                self.writer.write_all(&buffer[..]).unwrap();
                buffer.clear();
            }

            self.writer.flush().unwrap();    // <-- because writer is buffered
        }
    }
}

struct Switchboard<T:Send> {
    source: Receiver<((usize, usize), T)>,
    buffer: Vec<Vec<Option<T>>>,
}

impl<T:Send> Switchboard<T> {
    pub fn new(source: Receiver<((usize, usize), T)>) -> Switchboard<T> {
        Switchboard {
            source: source,
            buffer: Vec::new(),
        }
    }

    pub fn ensure(&mut self, a: usize, b: usize) -> &mut T {

        // ensure a, b, c are indexable
        while self.buffer.len() <= a { self.buffer.push(Vec::new()); }
        while self.buffer[a].len() <= b { self.buffer[a].push(None); }

        // repeatedly pull instructions until a, b, c exists.
        while self.buffer[a][b].is_none() {
            let ((x, y), s) = self.source.recv().unwrap();
            while self.buffer.len() <= x { self.buffer.push(Vec::new()); }
            while self.buffer[x].len() <= y { self.buffer[x].push(None); }
            self.buffer[x][y] = Some(s);
        }

        // we've just ensured that this is not None
        self.buffer[a][b].as_mut().unwrap()
    }
}

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>, my_index: usize, threads: usize, noisy: bool, logger: Option<::logging::LogSender>) -> Result<Vec<Binary>> {

    let processes = addresses.len();
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let start_task = thread::spawn(move || start_connections(hosts1, my_index, noisy));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index, noisy));

    let mut results = try!(start_task.join().unwrap());

    results.push(None);
    let to_extend = try!(await_task.join().unwrap());
    results.extend(to_extend.into_iter());

    if noisy { println!("worker {}:\tinitialization complete", my_index) }

    let mut readers = Vec::new();   // handles to the BinaryReceivers (to present new channels)
    let mut senders = Vec::new();   // destinations for serialized data (to send serialized data)

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {
        if let Some(stream) = results[index].take() {

            let (reader_channels_s, reader_channels_r) = channel();
            let (sender_channels_s, sender_channels_r) = channel();

            readers.push(reader_channels_s);    //
            senders.push(sender_channels_s);    //

            let mut sender = BinarySender::new(BufWriter::with_capacity(1 << 20, stream.try_clone().unwrap()),
                                               sender_channels_r,
                                               my_index,
                                               index,
                                               logger.clone());
            let mut recver = BinaryReceiver::new(stream.try_clone().unwrap(),
                                                 reader_channels_r,
                                                 my_index,
                                                 index,
                                                 threads,
                                                 logger.clone());

            // start senders and receivers associated with this stream
            thread::Builder::new().name(format!("send thread {}", index))
                                  .spawn(move || sender.send_loop())
                                  .unwrap();
            thread::Builder::new().name(format!("recv thread {}", index))
                                  .spawn(move || recver.recv_loop())
                                  .unwrap();

        }
    }

    let proc_comms = Process::new_vector(threads);

    let mut results = Vec::new();
    for (index, proc_comm) in proc_comms.into_iter().enumerate() {
        results.push(Binary {
            inner:          proc_comm,
            index:          my_index * threads + index,
            peers:          threads * processes,
            allocated:      0,
            readers:        readers.clone(),
            senders:        senders.clone(),
        });
    }

    return Ok(results);
}

// result contains connections [0, my_index - 1].
fn start_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();
    for index in 0..my_index {
        let mut connected = false;
        while !connected {
            match TcpStream::connect(&addresses[index][..]) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    try!(stream.write_u64::<LittleEndian>(my_index as u64));
                    results[index as usize] = Some(stream);
                    if noisy { println!("worker {}:\tconnection to worker {}", my_index, index); }
                    connected = true;
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }

    return Ok(results);
}

// result contains connections [my_index + 1, addresses.len() - 1].
fn await_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1)).map(|_| None).collect();
    let listener = try!(TcpListener::bind(&addresses[my_index][..]));

    for _ in (my_index + 1) .. addresses.len() {
        let mut stream = try!(listener.accept()).0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let identifier = try!(stream.read_u64::<LittleEndian>()) as usize;
        results[identifier - my_index - 1] = Some(stream);
        if noisy { println!("worker {}:\tconnection from worker {}", my_index, identifier); }
    }

    return Ok(results);
}
