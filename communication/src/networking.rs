//! Networking code for sending and receiving fixed size `Vec<u8>` between machines.

use std::io::{Read, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use abomonation::{encode, decode};

/// Framing data for each `Vec<u8>` transmission, indicating a typed channel, the source and
/// destination workers, and the length in bytes.
#[derive(Copy, Clone, Abomonation)]
pub struct MessageHeader {
    /// index of channel.
    pub channel:    usize,
    /// index of worker sending message.
    pub source:     usize,
    /// index of worker receiving message.
    pub target:     usize,
    /// number of bytes in message.
    pub length:     usize,
    /// sequence number.
    pub seqno:      usize,
}

impl MessageHeader {
    /// Returns a header when there is enough supporting data
    #[inline(always)]
    pub fn try_read(bytes: &mut [u8]) -> Option<MessageHeader> {
        unsafe { decode::<MessageHeader>(bytes) }
            .and_then(|(header, remaining)| {
                if remaining.len() >= header.length {
                    Some(header.clone())
                }
                else {
                    None
                }
            })
    }

    /// Writes the header as binary data.
    #[inline(always)]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        unsafe { encode(self, writer) }
    }

    /// The number of bytes required for the header and data.
    #[inline(always)]
    pub fn required_bytes(&self) -> usize {
        ::std::mem::size_of::<MessageHeader>() + self.length
    }
}

/// Creates socket connections from a list of host addresses.
pub fn create_sockets(addresses: Vec<String>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {

    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let start_task = thread::spawn(move || start_connections(hosts1, my_index, noisy));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index, noisy));

    let mut results = start_task.join().unwrap()?;
    results.push(None);
    let to_extend = await_task.join().unwrap()?;
    results.extend(to_extend.into_iter());

    if noisy { println!("worker {}:\tinitialization complete", my_index) }

    Ok(results)
}


/// Result contains connections [0, my_index - 1].
pub fn start_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..my_index).map(|_| None).collect();
    for index in 0..my_index {
        let mut connected = false;
        while !connected {
            match TcpStream::connect(&addresses[index][..]) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    unsafe { encode(&(my_index as u64), &mut stream) }.expect("failed to encode/send worker index");
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

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
pub fn await_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1)).map(|_| None).collect();
    let listener = try!(TcpListener::bind(&addresses[my_index][..]));

    for _ in (my_index + 1) .. addresses.len() {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        let mut buffer = [0u8;8];
        stream.read_exact(&mut buffer).expect("failed to read worker index");
        let identifier = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode worker index").0.clone() as usize;
        results[identifier - my_index - 1] = Some(stream);
        if noisy { println!("worker {}:\tconnection from worker {}", my_index, identifier); }
    }

    Ok(results)
}
