use std::io::{Read, Write, Result, BufRead, BufReader, BufWriter};
use std::fs::File;
use std::net::{TcpListener, TcpStream};
use std::mem::size_of;
use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::thread::sleep_ms;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use communication::communicator::{Binary, Process};
use drain::DrainExt;

// TODO : Much of this only relates to BinaryWriter/BinaryReader based communication, not networking.
// TODO : Could be moved somewhere less networking-specific.

#[derive(Copy, Clone)]
pub struct MessageHeader {
    pub graph:      usize,   // graph identifier
    pub channel:    usize,   // index of channel
    pub source:     usize,   // index of worker sending message
    pub target:     usize,   // index of worker receiving message
    pub length:     usize,   // number of bytes in message
}

impl MessageHeader {
    // returns a header when there is enough supporting data
    fn try_read(bytes: &mut &[u8]) -> Option<MessageHeader> {
        if bytes.len() > size_of::<MessageHeader>() {
            // capture original in case we need to rewind
            let original = *bytes;

            // unclear what order struct initializers run in, so ...
            let graph = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let channel = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let source = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let target = bytes.read_u64::<LittleEndian>().unwrap() as usize;
            let length = bytes.read_u64::<LittleEndian>().unwrap() as usize;

            if bytes.len() >= length {
                Some(MessageHeader {
                    graph: graph,
                    channel: channel,
                    source: source,
                    target: target,
                    length: length,
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
        try!(writer.write_u64::<LittleEndian>(self.graph as u64));
        try!(writer.write_u64::<LittleEndian>(self.channel as u64));
        try!(writer.write_u64::<LittleEndian>(self.source as u64));
        try!(writer.write_u64::<LittleEndian>(self.target as u64));
        try!(writer.write_u64::<LittleEndian>(self.length as u64));
        Ok(())
    }
}

// // structure in charge of receiving data from a Reader, for example the network
// struct BinaryReceiver<R: Read> {
//     reader:     R,          // the generic reader
//     buffer:     Vec<u8>,    // current working buffer
//     double:     Vec<u8>,    // second working buffer
//     staging:    Vec<u8>,    // 1 << 20 of buffer to read into
//     targets:    Switchboard<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
// }
//
// impl<R: Read> BinaryReceiver<R> {
//     fn new(reader: R, channels: Receiver<((u64, u64, u64), (Sender<Vec<u8>>, Receiver<Vec<u8>>))>) -> BinaryReceiver<R> {
//         BinaryReceiver {
//             reader:     reader,
//             buffer:     Vec::new(),
//             double:     Vec::new(),
//             staging:    vec![0u8; 1 << 20],
//             targets:    Switchboard::new(channels),
//         }
//     }
//
//     fn recv_loop(&mut self) {
//         loop {
//
//             // attempt to read some more bytes into our buffer
//             // TODO : We read in to self.staging because extending a Vec<u8> is hard without
//             // TODO : using set_len, which is unsafe.
//             // TODO : Could consider optimizing for the self.buffer.len() == 0 case, swapping
//             // TODO : self.staging with self.buffer, rather than using write_all.
//             let read = self.reader.read(&mut self.staging[..]).unwrap_or(0);
//             self.buffer.write_all(&self.staging[..read]).unwrap(); // <-- shouldn't fail
//
//             {
//                 // get a view of available bytes
//                 let mut slice = &self.buffer[..];
//
//                 while let Some(header) = MessageHeader::try_read(&mut slice) {
//
//                     let h_len = header.length as usize;  // length in bytes
//                     let target = self.targets.ensure(header.target, header.graph, header.channel);
//                     let mut buffer = target.1.try_recv().unwrap_or(Vec::new());
//
//                     buffer.clear();
//                     buffer.write_all(&slice[..h_len]).unwrap();
//                     slice = &slice[h_len..];
//
//                     target.0.send(buffer).unwrap();
//                 }
//
//                 // TODO: way inefficient... =/ Fix! :D
//                 // if slice.len() < self.buffer.len() {
//                     self.double.clear();
//                     self.double.write_all(slice).unwrap();
//                 // }
//             }
//
//             // if self.double.len() > 0 {
//                 mem::swap(&mut self.buffer, &mut self.double);
//                 // self.double.clear();
//             // }
//         }
//     }
// }

// structure in charge of receiving data from a Reader, for example the network
struct BinaryReceiver<R: Read> {
    reader:     R,          // the generic reader
    buffer:     Vec<u8>,    // current working buffer
    length:     usize,
    targets:    Switchboard<(Sender<Vec<u8>>, Receiver<Vec<u8>>)>,
}

impl<R: Read> BinaryReceiver<R> {
    fn new(reader: R, channels: Receiver<((usize, usize, usize), (Sender<Vec<u8>>, Receiver<Vec<u8>>))>) -> BinaryReceiver<R> {
        BinaryReceiver {
            reader:     reader,
            buffer:     vec![0u8; 1 << 20],
            length:     0,
            targets:    Switchboard::new(channels),
        }
    }

    fn recv_loop(&mut self) {
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
                    let h_len = header.length as usize;  // length in bytes
                    let target = &mut self.targets.ensure(header.target, header.graph, header.channel).0;
                    target.send(slice[..h_len].to_vec()).unwrap();
                    slice = &slice[h_len..];
                }

                slice.len()
            };

            // we consumed bytes, must shift to beginning.
            // this should optimize to copy_overlapping;
            // would just do that if it weren't unsafe =/
            if remaining < self.length {
                for index in 0..remaining {
                    self.buffer[index] = self.buffer[index + self.length - remaining];
                }
                self.length = remaining;
            }
        }
    }
}

// structure in charge of sending data to a Writer, for example the network
struct BinarySender<W: Write> {
    id:         usize,    // destination process
    writer:     W,
    sources:    Receiver<(MessageHeader, Vec<u8>)>,
    returns:    Switchboard<Sender<Vec<u8>>>,
}

impl<W: Write> BinarySender<W> {
    fn new(id: usize,
           writer: W,
           sources: Receiver<(MessageHeader, Vec<u8>)>,
           channels: Receiver<((usize, usize, usize), Sender<Vec<u8>>)>) -> BinarySender<W> {
        BinarySender {
            id:         id,
            writer:     writer,
            sources:    sources,
            returns: Switchboard::new(channels),
        }
    }

    fn send_loop(&mut self) {
        let mut stash = Vec::new();

        // block until data to recv
        while let Ok((header, buffer)) = self.sources.recv() {

            stash.push((header, buffer));

            // collect any additional outstanding data to send
            while let Ok((header, buffer)) = self.sources.try_recv() {
                stash.push((header, buffer));
            }

            // println!("send loop to process {}:\tstarting", self.id);
            for (header, mut buffer) in stash.drain_temp() {
                assert!(header.length == buffer.len());
                header.write_to(&mut self.writer).unwrap();
                self.writer.write_all(&buffer[..]).unwrap();
                buffer.clear();

                // self.returns.ensure(header.source, header.graph, header.channel).send(buffer).unwrap();
            }

            self.writer.flush().unwrap();    // <-- because writer is buffered
        }
    }
}

struct Switchboard<T:Send> {
    source: Receiver<((usize, usize, usize), T)>,
    buffer: Vec<Vec<Vec<Option<T>>>>,
}

impl<T:Send> Switchboard<T> {
    pub fn new(source: Receiver<((usize, usize, usize), T)>) -> Switchboard<T> {
        Switchboard {
            source: source,
            buffer: Vec::new(),
        }
    }

    pub fn ensure(&mut self, a: usize, b: usize, c: usize) -> &mut T {

        while self.buffer.len() <= a { self.buffer.push(Vec::new()); }
        while self.buffer[a].len() <= b { self.buffer[a].push(Vec::new()); }
        while self.buffer[a][b].len() <= c { self.buffer[a][b].push(None); }

        while let None = self.buffer[a][b][c] {
            let ((x, y, z), s) = self.source.recv().unwrap();

            while self.buffer.len() <= x { self.buffer.push(Vec::new()); }
            while self.buffer[x].len() <= y { self.buffer[x].push(Vec::new()); }
            while self.buffer[x][y].len() <= z { self.buffer[x][y].push(None); }
            self.buffer[x][y][z] = Some(s);
        }

        // we've just ensured that this is not None
        self.buffer[a][b][c].as_mut().unwrap()
    }
}

pub fn initialize_networking_from_file(filename: &str, my_index: usize, workers: usize) -> Result<Vec<Binary>> {

    let reader = BufReader::new(try!(File::open(filename)));
    let mut addresses = Vec::new();

    for line in reader.lines() {
        addresses.push(try!(line));
    }

    initialize_networking(addresses, my_index, workers)
}

pub fn initialize_networking(addresses: Vec<String>, my_index: usize, workers: usize) -> Result<Vec<Binary>> {

    let processes = addresses.len();
    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let start_task = thread::spawn(move || start_connections(hosts1, my_index));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index));

    let mut results = try!(start_task.join().unwrap());

    results.push(None);
    let mut to_extend = try!(await_task.join().unwrap());
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

            let mut sender = BinarySender::new(index,
                                               BufWriter::with_capacity(1 << 20, stream.try_clone().unwrap()),
                                               sender_channels_r,
                                               writer_channels_r);
            let mut recver = BinaryReceiver::new(stream.try_clone().unwrap(), reader_channels_r);

            // start senders and receivers associated with this stream
            thread::Builder::new().name(format!("send thread {}", index))
                                  .spawn(move || sender.send_loop())
                                  .unwrap();
            thread::Builder::new().name(format!("recv thread {}", index))
                                  .spawn(move || recver.recv_loop())
                                  .unwrap();

        }
    }

    let proc_comms = Process::new_vector(workers);

    let mut results = Vec::new();
    for (index, proc_comm) in proc_comms.into_iter().enumerate() {
        results.push(Binary {
            inner:          proc_comm,
            index:          my_index * workers + index,
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
fn start_connections(addresses: Arc<Vec<String>>, my_index: usize) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();
    for index in (0..my_index) {
        let mut connected = false;
        while !connected {
            match TcpStream::connect(&addresses[index][..]) {
                Ok(mut stream) => {
                    try!(stream.write_u64::<LittleEndian>(my_index as u64));
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
fn await_connections(addresses: Arc<Vec<String>>, my_index: usize) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1)).map(|_| None).collect();
    let listener = try!(TcpListener::bind(&addresses[my_index][..]));

    for _ in (my_index + 1 .. addresses.len()) {
        let mut stream = try!(listener.accept()).0;
        let identifier = try!(stream.read_u64::<LittleEndian>()) as usize;
        results[identifier - my_index - 1] = Some(stream);
        println!("worker {}:\tconnection from worker {}", my_index, identifier);
    }

    return Ok(results);
}
