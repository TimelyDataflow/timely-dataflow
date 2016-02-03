use dataflow::channels::Content;
use progress::count_map::CountMap;
use Pull;

pub struct Counter<T: Eq+Clone+'static, D> {
    pullable: Box<Pull<(T, Content<D>)>>,
    consumed: CountMap<T>,
    phantom: ::std::marker::PhantomData<D>,
}

impl<T:Eq+Clone+'static, D> Counter<T, D> {
    #[inline]
    pub fn next(&mut self) -> Option<(&T, &mut Content<D>)> {
        if let Some((ref time, ref mut data)) = *self.pullable.pull() {
            if data.len() > 0 {
                self.consumed.update(time, data.len() as i64);
                Some((time, data))
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Eq+Clone+'static, D> Counter<T, D> {
    pub fn new(pullable: Box<Pull<(T, Content<D>)>>) -> Counter<T, D> {
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
