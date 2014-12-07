use progress::frontier::Antichain;
use progress::Scope;
use progress::subgraph::{Summary};
use progress::subgraph::Summary::Local;

pub struct BarrierScope
{
    pub ready: bool,
    pub epoch: uint,
}

impl Scope<((), uint), Summary<(), uint>> for BarrierScope
{
    fn name(&self) -> String { format!("Barrier") }
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<Summary<(), uint>>>>, Vec<Vec<(((), uint), i64)>>)
    {
        return (vec![vec![Antichain::from_elem(Local(1))]], vec![vec![(((), self.epoch), 1i64)]]);
    }

    fn push_external_progress(&mut self, progress: &Vec<Vec<(((), uint), i64)>>) -> ()
    {
        for &(time, val) in progress[0].iter()
        {
            if time.val1() == self.epoch - 1 && val == -1
            {
                // println!("Ready at epoch {}", self.epoch);
                self.ready = true;
            }
        }
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(((), uint), i64)>>,
                                        _messages_consumed: &mut Vec<Vec<(((), uint), i64)>>,
                                        _messages_produced: &mut Vec<Vec<(((), uint), i64)>>) -> ()
    {

        if self.ready
        {
            frontier_progress[0].push((((), self.epoch), -1));
            frontier_progress[0].push((((), self.epoch + 1), 1));
            self.epoch += 1;
            self.ready = false;
        }
    }
}
