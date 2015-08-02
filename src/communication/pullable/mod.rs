use communication::Message;

pub use self::counter::Counter;

pub mod counter;

/// The pullable design may need to be upgraded: right now there is no obvious connection between
/// subsequent calls to pull; although multiple calls may produce the same time, they don't need to
/// and defensive implementations must constantly check this. This complicates data exchange, which
/// may conservatively over-flush, if the defensive implementation isn't well done (e.g. now).

pub trait Pullable<T, D> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)>;
}

impl<T, D, P: ?Sized + Pullable<T, D>> Pullable<T, D> for Box<P> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> { (**self).pull() }
}
