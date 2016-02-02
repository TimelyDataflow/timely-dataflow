use std::collections::VecDeque;
use progress::frontier::MutableAntichain;
use progress::Timestamp;
use progress::count_map::CountMap;
use dataflow::operators::Capability;

pub struct Notificator<T: Timestamp> {
    pending: Vec<Capability<T>>,
    frontier: Vec<MutableAntichain<T>>,
    available: VecDeque<(Capability<T>, i64)>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn new() -> Notificator<T> {
        Notificator {
            pending: Vec::new(),
            frontier: Vec::new(),
            available: VecDeque::new(),
        }
    }

    /// Updates the `Notificator`'s frontiers from a `CountMap` per input.
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            while let Some((time, delta)) = counts.pop() {
                self.frontier[index].update(&time, delta);
            }
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].elements()
    }

    /// Requests a notification at `time`.
    #[inline]
    pub fn notify_at(&mut self, time: Capability<T>) {
        self.pending.push(time);
    }

    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, i64)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, count);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }

    #[inline]
    fn scan_for_available_notifications(&mut self) {
        let mut last = 0;
        while let Some(position) = (&self.pending[last..]).iter().position(|cap| {
            let time = cap.time();
            !self.frontier.iter().any(|x| x.le(&time))
        }) {
            let newly_available = self.pending.swap_remove(position + last);
            let push_new = {
                let already_available = self.available.iter_mut().find(|&&mut (ref cap, _)| {
                    cap.time() == newly_available.time()
                });
                match already_available {
                    Some(&mut (_, ref mut count)) => {
                        *count += 1;
                        false
                    },
                    None => true,
                }
            };
            if push_new {
                self.available.push_back((newly_available, 1));
            }
            last = position;
        }
    }

}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (Capability<T>, i64);

    fn next(&mut self) -> Option<(Capability<T>, i64)> {
        if self.available.len() == 0 {
            self.scan_for_available_notifications();
        }

        self.available.pop_front()
    }
}

