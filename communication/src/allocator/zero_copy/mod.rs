//! Allocators based on serialized data which avoid copies.
//!
//! These allocators were based on `Abomonation` serialization, and its ability to deserialized
//! typed Rust data in-place. They surfaced references to data, often ultimately referencing the
//! raw binary data they initial received.
//!
//! For the moment, they no longer use Abomonation due to its unsafety, and instead rely on the
//! use of `Message::from_bytes` which .. could .. use Abomonation or something safer, but uses
//! `bincode` at of this writing.

pub mod bytes_slab;
pub mod bytes_exchange;
pub mod tcp;
pub mod allocator;
pub mod allocator_process;
pub mod initialize;
pub mod push_pull;
pub mod stream;