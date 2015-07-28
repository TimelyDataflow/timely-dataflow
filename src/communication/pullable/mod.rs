use communication::Message;
use progress::count_map::CountMap;

/// The pullable design may need to be upgraded: right now there is no obvious connection between
/// subsequent calls to pull; although multiple calls may produce the same time, they don't need to
/// and defensive implementations must constantly check this. This complicates data exchange, which
/// may conservatively over-flush, if the defensive implementation isn't well done (e.g. now).

/// An alternate design is for a Pullable<T, D> to return a (&T, Session<D>), where Session<D> is a
/// new type implementing Iterator<Item=Message<D>>, or Iterator<Item=D>, or PullableSession<D>, or
/// something like that. Ideally, the Session<D> notice how many records are consumed, and only
/// treats those res

pub trait Pullable<T, D> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)>;
}

impl<T, D, P: ?Sized + Pullable<T, D>> Pullable<T, D> for Box<P> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> { (**self).pull() }
}

pub struct Counter<T: Eq+Clone+'static, D, P: Pullable<T, D>> {
    pullable:   P,
    consumed:   CountMap<T>,
    phantom: ::std::marker::PhantomData<D>,
}

// NOTE : not implementing pullable, as never composed and users don't
// NOTE : want to have to `use communicator::Pullable;` to use this.
impl<T:Eq+Clone+'static, D, P: Pullable<T, D>> Counter<T, D, P> {
    #[inline]
    pub fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {
        if let Some(pair) = self.pullable.pull() {
            if pair.1.len() > 0 {
                self.consumed.update(&pair.0, pair.1.len() as i64);
                Some(pair)
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Eq+Clone+'static, D, P: Pullable<T, D>> Counter<T, D, P> {
    pub fn new(pullable: P) -> Counter<T, D, P> {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable: pullable,
            consumed: CountMap::new(),
        }
    }
    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>) {
        while let Some((ref time, value)) = self.consumed.pop() {
            consumed.update(time, value);
        }
    }
}
