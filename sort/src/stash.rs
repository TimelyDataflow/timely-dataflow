pub struct Stash<T> {
    stashed: Vec<Vec<T>>,
    default_capacity: usize
}

impl<T> Stash<T> {
    pub fn new(default_capacity: usize) -> Stash<T> {
        Stash {
            stashed: Vec::new(),
            default_capacity: default_capacity
        }
    }

    pub fn get(&mut self) -> Vec<T> {
        self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(self.default_capacity))
    }

    pub fn give(&mut self, vec: Vec<T>) {
        // TODO: determine some discipline for when to keep buffers vs not.
        // if vec.capacity() == self.default_capacity {
        self.stashed.push(vec);
        // }
    }

    pub fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        while self.stashed.len() > intended {
            buffers.push(self.stashed.pop().unwrap());
        }
        while self.stashed.len() < intended && buffers.len() > 0 {
            let mut buffer = buffers.pop().unwrap();
            buffer.clear();
            self.stashed.push(buffer);
        }
    }
}
