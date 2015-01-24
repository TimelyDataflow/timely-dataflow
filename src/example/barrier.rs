use progress::frontier::Antichain;
use progress::Scope;
use progress::subgraph::{Summary};
use progress::subgraph::Summary::Local;

pub struct BarrierScope {
    pub ready:  bool,
    pub epoch:  u64,
    pub degree: u64,
    pub ttl:    u64,
}

impl Scope<((), u64), Summary<(), u64>> for BarrierScope {
    fn name(&self) -> String { format!("Barrier") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<Summary<(), u64>>>>, Vec<Vec<(((), u64), i64)>>) {
        return (vec![vec![Antichain::from_elem(Local(1))]],
                vec![vec![(((), self.epoch), self.degree as i64)]]);
    }

    fn push_external_progress(&mut self, external: &Vec<Vec<(((), u64), i64)>>) -> () {
        for &(time, val) in external[0].iter() {
            if time.1 == self.epoch - 1 && val == -1 {
                self.ready = true;
            }
        }
    }

    fn pull_internal_progress(&mut self, internal: &mut Vec<Vec<(((), u64), i64)>>,
                                        _consumed: &mut Vec<Vec<(((), u64), i64)>>,
                                        _produced: &mut Vec<Vec<(((), u64), i64)>>) -> bool
    {
        if self.ready {
            internal[0].push((((), self.epoch), -1));

            if self.epoch < self.ttl {
                internal[0].push((((), self.epoch + 1), 1));
            }

            self.epoch += 1;
            self.ready = false;
        }

        return false;
    }
}
