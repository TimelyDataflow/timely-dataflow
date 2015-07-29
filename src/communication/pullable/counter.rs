use communication::{Message, Pullable};
use progress::count_map::CountMap;


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
