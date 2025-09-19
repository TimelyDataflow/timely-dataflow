pub use self::tee::{Tee, TeeHelper};
pub use self::exchange::Exchange;
pub use self::counter::Counter;

pub mod tee;
pub mod exchange;
pub mod counter;
pub mod progress;

/// An output pusher which validates capabilities, records progress, and tees output.
pub type Output<T, C> = progress::Progress<T, counter::Counter<T, tee::Tee<T, C>>>;
/// An output session that will flush the output when dropped.
pub type OutputSession<'a, T, C> = progress::ProgressSession<'a, T, C, counter::Counter<T, tee::Tee<T, C>>>;
