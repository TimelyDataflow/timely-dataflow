use bytes::arc::Bytes;

pub struct BytesSlab {
    buffer:         Bytes,                      // current working buffer.
    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Bytes>,                 // reclaimed and resuable buffers.
    shift:          usize,                      // current buffer allocation size.
    valid:          usize,                      // buffer[..valid] are valid bytes.
}

impl BytesSlab {

    pub fn new(shift: usize) -> Self {
        BytesSlab {
            buffer: Bytes::from(vec![0u8; 1 << shift].into_boxed_slice()),
            in_progress: Vec::new(),
            stash: Vec::new(),
            shift,
            valid: 0,
        }
    }

    pub fn empty(&mut self) -> &mut [u8] {
        &mut self.buffer[self.valid..]
    }

    pub fn valid(&mut self) -> &[u8] {
        &mut self.buffer[..self.valid]
    }

    pub fn make_valid(&mut self, bytes: usize) {
        self.valid += bytes;
    }

    pub fn extract(&mut self, bytes: usize) -> Bytes {
        debug_assert!(bytes <= self.valid);
        self.valid -= bytes;
        self.buffer.extract_to(bytes)
    }

    // Retire `self.buffer` and acquire a new buffer of at least `self.size` bytes.
    pub fn ensure_capacity(&mut self, capacity: usize) {

        // Increase allocation if insufficient.
        while self.valid + capacity > (1 << self.shift) {
            self.shift += 1;
            self.stash.clear();
        }

        // Attempt to reclaim shared slices.
        if self.stash.is_empty() {
            for shared in self.in_progress.iter_mut() {
                if let Some(mut bytes) = shared.take() {
                    if bytes.try_regenerate::<Box<[u8]>>() {
                        self.stash.push(bytes);
                    }
                    else {
                        *shared = Some(bytes);
                    }
                }
            }
            self.in_progress.retain(|x| x.is_some());
            self.stash.retain(|x| x.len() == 1 << self.shift);
        }

        let new_buffer = self.stash.pop().unwrap_or_else(|| Bytes::from(vec![0; 1 << self.shift].into_boxed_slice()));
        let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

        self.buffer[.. self.valid].copy_from_slice(&old_buffer[.. self.valid]);
        self.in_progress.push(Some(old_buffer));
    }
}