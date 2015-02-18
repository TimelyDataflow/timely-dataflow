use progress::frontier::Antichain;
use progress::{ Scope, CountMap };
use progress::subgraph::Summary;
use progress::subgraph::Summary::Local;

pub struct BarrierScope {
    pub ready:  bool,
    pub epoch:  u64,
    pub degree: u64,
    pub ttl:    u64,
}

impl Scope<((), u64)> for BarrierScope {
    fn name(&self) -> String { format!("Barrier") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<Summary<(), u64>>>>, Vec<CountMap<((), u64)>>) {
        let mut map = CountMap::new();
        map.update(&((), self.epoch), self.degree as i64);
        return (vec![vec![Antichain::from_elem(Local(1))]], vec![map]);
    }

    fn push_external_progress(&mut self, external: &mut Vec<CountMap<((), u64)>>) -> () {
        while let Some((time, val)) = external[0].pop() {
            if time.1 == self.epoch - 1 && val == -1 {
                self.ready = true;
            }
        }
    }

    fn pull_internal_progress(&mut self, internal: &mut Vec<CountMap<((), u64)>>,
                                        _consumed: &mut Vec<CountMap<((), u64)>>,
                                        _produced: &mut Vec<CountMap<((), u64)>>) -> bool
    {
        if self.ready {
            internal[0].update(&((), self.epoch), -1);

            if self.epoch < self.ttl {
                internal[0].update(&((), self.epoch + 1), 1);
            }

            self.epoch += 1;
            self.ready = false;
        }

        return false;
    }
}
