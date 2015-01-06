pub use communication::channels::Data;
pub use communication::allocator::ChannelAllocator;
pub use communication::exchange::{ExchangeReceiver, exchange_with};
pub use communication::observer::Observer;

pub mod channels;
pub mod allocator;
pub mod exchange;
pub mod observer;
