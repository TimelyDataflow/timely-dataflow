//! Abstractions over network streams.

use std::io;
use std::net::{TcpStream, Shutdown};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

/// An abstraction over network streams.
pub trait Stream: Sized + Send + Sync + io::Read + io::Write {
    /// Creates a new independently owned handle to the underlying stream.
    fn try_clone(&self) -> io::Result<Self>;

    /// Moves this stream into or out of nonblocking mode.
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;

    /// Shuts down the read, write, or both halves of this connection.
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl Stream for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}

#[cfg(unix)]
impl Stream for UnixStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}
