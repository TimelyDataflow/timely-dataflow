//! Configuration and events for communication logging.

/// Configuration information about a communication thread.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CommunicationSetup {
    /// True when this is a send thread (or the receive thread).
    pub sender: bool,
    /// The process id of the thread.
    pub process: usize,
    /// The remote process id.
    pub remote: Option<usize>,
}

/// A communication event, observing a message.
#[derive(Abomonation, Debug, Clone)]
pub struct CommunicationEvent {
    /// true for send event, false for receive event
    pub is_send: bool,
    /// communication channel id
    pub comm_channel: usize,
    /// source worker id
    pub source: usize,
    /// target worker id
    pub target: usize,
    /// sequence number
    pub seqno: usize,
}