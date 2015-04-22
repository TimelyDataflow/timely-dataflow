use progress::Timestamp;
use progress::frontier::MutableAntichain;
use progress::count_map::CountMap;

// Notificator is meant to manage the delivery of requested notifications in the presence of
// inputs that may have outstanding messages to deliver. The notificator tracks the frontiers,
// as presented from the outside, for each input. Requested notifications can be served only
// once there are no frontier elements less-or-equal to them, and there are no other pending
// notification requests less than them. Each with be less-or-equal to itself, so we want to
// dodge that corner case.

#[derive(Default)]
pub struct Notificator<T: Timestamp> {
    pending:        MutableAntichain<T>,        // notification requests not yet been delivered
    frontier:       Vec<MutableAntichain<T>>,   // outstanding input, preventing notification
    available:      CountMap<T>,                // notifications available for delivery
    changes:        CountMap<T>,                // change to report through pull_progress
}

impl<T: Timestamp> Notificator<T> {
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        println!("notificator: frontier update {:?}", count_map);
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            while let Some((time, delta)) = counts.pop() {
                self.frontier[index].update(&time, delta);
            }
        }
    }

    pub fn notify_at(&mut self, time: &T) {
        println!("notificator: notify at: {:?}", time);
        self.changes.update(time, 1);
        self.pending.update(time, 1);

        // if !self.frontier.iter().any(|x| x.le(time)) {
            // TODO : Technically you should be permitted to send and notify at the current notification
            // TODO : but this would panic because we have already removed it from the frontier.
            // TODO : A RAII capability for sending/notifying would be good, but the time is almost
            // TODO : exactly that.

            // println!("notificator error? notify_at called with time not le the frontier. {:?}", time);
            // println!("notificator error? {:?} vs {:?}", time, self.frontier);
            // panic!("");
            // self.available.update(time, 1);
        // }
    }

    pub fn pull_progress(&mut self, internal: &mut CountMap<T>) {
        while let Some((time, delta)) = self.changes.pop() {
            internal.update(&time, delta);
        }
    }
}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (T, i64);

    fn next(&mut self) -> Option<(T, i64)> {

        // if nothing obvious available, scan for options
        if self.available.len() == 0 {
            // println!("notificator: none available; pending: {}", self.pending.elements.len());
            for pend in self.pending.elements.iter() {
                // println!("notificator: considering {:?}", pend);
                if !self.frontier.iter().any(|x| {
                    // println!("notificator:   {:?} v {:?} = {}", x, pend, x.le(pend));
                    x.le(pend)}) {
                    // println!("notificator:   works!");
                    if let Some(val) = self.pending.count(pend) {
                        // println!("notificator:   updating!");
                        self.available.update(pend, val);
                    }
                }
            }
        }

        // return an available notification, after cleaning up
        if let Some((time, delta)) =  self.available.pop() {
            println!("notificator: serving notification at {:?}", time);
            self.changes.update(&time, -delta);
            self.pending.update(&time, -delta);
            Some((time, delta))
        }
        else { None }
    }
}
