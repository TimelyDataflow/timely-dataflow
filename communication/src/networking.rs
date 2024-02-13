//! Networking code for sending and receiving fixed size `Vec<u8>` between machines.

use std::io;
use std::io::{Read, Result};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use abomonation::{encode, decode};

// This constant is sent along immediately after establishing a TCP stream, so
// that it is easy to sniff out Timely traffic when it is multiplexed with
// other traffic on the same port.
const HANDSHAKE_MAGIC: u64 = 0xc2f1fb770118add9;

/// Framing data for each `Vec<u8>` transmission, indicating a typed channel, the source and
/// destination workers, and the length in bytes.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
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
    #[inline]
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
    #[inline]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        unsafe { encode(self, writer) }
    }

    /// The number of bytes required for the header and data.
    #[inline]
    pub fn required_bytes(&self) -> usize {
        ::std::mem::size_of::<MessageHeader>() + self.length
    }
}

/// Creates socket connections from a list of host addresses.
///
/// The item at index i in the resulting vec, is a Some(TcpSocket) to process i, except
/// for item `my_index` which is None (no socket to self).
pub fn create_sockets(addresses: Vec<String>, my_index: usize, noisy: bool) -> Result<Vec<Option<TcpStream>>> {

    let hosts1 = Arc::new(addresses);
    let hosts2 = hosts1.clone();

    let nonce = Arc::new((Mutex::new(None), Condvar::new()));

    if my_index == 0 {
        let (nonce, _cvar) = &*nonce;
        *nonce.lock().unwrap() = Some(rand::random());
    }

    let nonce1 = Arc::clone(&nonce);

    let start_task = thread::spawn(move || start_connections(hosts1, my_index, noisy, nonce1));
    let await_task = thread::spawn(move || await_connections(hosts2, my_index, noisy, nonce));

    let mut results = start_task.join().unwrap()?;
    results.push(None);
    let to_extend = await_task.join().unwrap()?;
    results.extend(to_extend.into_iter());

    if noisy { println!("worker {}:\tinitialization complete", my_index) }

    Ok(results)
}


/// Result contains connections [0, my_index - 1].
pub fn start_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool, arc: Arc<(Mutex<Option<u64>>, Condvar)>) -> Result<Vec<Option<TcpStream>>> {
    // let arc = &arc;
    let results = addresses.iter().take(my_index).enumerate().map(|(index, address)| {
        loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.set_nodelay(true).expect("set_nodelay call failed");
                    unsafe { encode(&HANDSHAKE_MAGIC, &mut stream) }.expect("failed to encode/send handshake magic");
                    unsafe { encode(&(my_index as u64), &mut stream) }.expect("failed to encode/send worker index");
                    let mut buffer = [0u8;8];
                    stream.read_exact(&mut buffer).expect("failed to read nonce");
                    let received_nonce = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode nonce").0.clone();
                    let (nonce, cvar) = &*arc;
                    let mut nonce = nonce.lock().unwrap();
                    let nonce = if let Some(known_nonce) = *nonce {
                        known_nonce
                    } else {
                        *nonce = Some(received_nonce);
                        cvar.notify_all();
                        received_nonce
                    };
                    unsafe { encode(&nonce, &mut stream) }.expect("failed to encode/send worker index");
                    assert_eq!(nonce, received_nonce, "nonce mismatch");

                    if noisy { println!("worker {}:\tconnection to worker {}", my_index, index); }
                    break Some(stream);
                },
                Err(error) => {
                    println!("worker {}:\terror connecting to worker {}: {}; retrying", my_index, index, error);
                    sleep(Duration::from_secs(1));
                },
            }
        }
    }).collect();

    Ok(results)
}

/// Result contains connections [my_index + 1, addresses.len() - 1].
pub fn await_connections(addresses: Arc<Vec<String>>, my_index: usize, noisy: bool, nonce: Arc<(Mutex<Option<u64>>, Condvar)>) -> Result<Vec<Option<TcpStream>>> {
    let mut results: Vec<_> = (0..(addresses.len() - my_index - 1)).map(|_| None).collect();
    let listener = TcpListener::bind(&addresses[my_index][..])?;

    let (lock, cvar) = &*nonce;
    let mut nonce = lock.lock().unwrap();
    while nonce.is_none() {
        nonce = cvar.wait(nonce).unwrap();
    }
    let nonce = nonce.unwrap();

    for _ in (my_index + 1) .. addresses.len() {
        let mut stream = listener.accept()?.0;
        stream.set_nodelay(true).expect("set_nodelay call failed");
        unsafe { encode(&nonce, &mut stream) }.expect("failed to encode/send nonce");
        let mut buffer = [0u8; 24];
        stream.read_exact(&mut buffer)?;
        let (magic, mut buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode magic");
        if magic != &HANDSHAKE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                "received incorrect timely handshake"));
        }
        let (identifier, mut buffer) = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode worker index");
        results[identifier.clone() as usize - my_index - 1] = Some(stream);
        let child_nonce = unsafe { decode::<u64>(&mut buffer) }.expect("failed to decode child nonce").0.clone();
        assert_eq!(nonce, child_nonce, "nonce mismatch");
        if noisy { println!("worker {}:\tconnection from worker {}", my_index, identifier); }
    }

    Ok(results)
}
