//! Networking start-up, plus send and receive loops.

pub use networking::networking::initialize_networking;
pub use networking::networking::initialize_networking_from_file;

pub mod networking;
