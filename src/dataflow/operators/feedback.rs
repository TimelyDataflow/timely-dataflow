use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use {Data, Push};

use progress::{Timestamp, Operate, PathSummary};
use progress::frontier::Antichain;
use progress::nested::Source::ChildOutput;
use progress::nested::Target::ChildInput;
use progress::count_map::CountMap;

use dataflow::channels::Content;
use dataflow::channels::pushers::{Counter, Tee};

use dataflow::{Stream, Scope};

pub trait LoopVariable<G: Scope> {
    fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
        -> (Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>, Stream<G, D>);
}

impl<G: Scope> LoopVariable<G> for G {
    fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
        -> (Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        let produced: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();
        let consumed: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();

        let feedback_output = Counter::new(targets, produced.clone());
        let feedback_input =  Counter::new(Observer {
            limit: limit, summary: summary, targets: feedback_output
        }, consumed.clone());

        let index = self.add_operator(Operator {
            consumed_messages:  consumed.clone(),
            produced_messages:  produced.clone(),
            summary:            summary,
        });

        let helper = Helper {
            index:  index,
            target: feedback_input,
            phant: ::std::marker::PhantomData,
        };

        (helper, Stream::new(ChildOutput(index, 0), registrar, self.clone()))
    }
}

// implementation of the feedback vertex, essentially, as an observer
pub struct Observer<T: Timestamp, D:Data> {
    limit:      T,
    summary:    T::Summary,
    targets:    Counter<T, D, Tee<T, D>>,
    // active:     bool,
}

impl<T: Timestamp, D: Data> Push<(T, Content<D>)> for Observer<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<(T, Content<D>)>) {
        let active = if let Some((ref mut time, _)) = *message {
            *time = self.summary.results_in(time);
            self.limit.ge(time)
        }
        else { true };

        if active { self.targets.push(message); }
    }
}


pub trait ConnectLoop<G: Scope, D: Data> {
    fn connect_loop(&self, Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>);
}

impl<G: Scope, D: Data> ConnectLoop<G, D> for Stream<G, D> {
    fn connect_loop(&self, helper: Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>) {
        self.connect_to(ChildInput(helper.index, 0), helper.target, usize::max_value());
    }

}

// a handy widget for connecting feedback edges
pub struct Helper<T, D, P: Push<(T, Content<D>)>> {
    index:  usize,
    target: P,
    phant: ::std::marker::PhantomData<(T, D)>,
}

// impl<O: Observer+'static> Helper<O>
// where O::Time: Timestamp, O::Data : Data {
//     pub fn connect_input<G:Scope<Timestamp=O::Time>>(self, source: &Stream<O::Time, O::Data>, builder: &mut G) {
//         builder.enable(source).connect_to(ChildInput(self.index, 0), self.target);
//     }
// }

// the scope that the progress tracker interacts with
pub struct Operator<T:Timestamp> {
    consumed_messages:  Rc<RefCell<CountMap<T>>>,
    produced_messages:  Rc<RefCell<CountMap<T>>>,
    summary:            T::Summary,
}


impl<T:Timestamp> Operate<T> for Operator<T> {
    fn name(&self) -> &str { "Feedback" }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        (vec![vec![Antichain::from_elem(self.summary)]], vec![CountMap::new()])
    }

    fn pull_internal_progress(&mut self, _frontier_progress: &mut [CountMap<T>],
                                          messages_consumed: &mut [CountMap<T>],
                                          messages_produced: &mut [CountMap<T>]) -> bool {

        self.consumed_messages.borrow_mut().drain_into(&mut messages_consumed[0]);
        self.produced_messages.borrow_mut().drain_into(&mut messages_produced[0]);
        return false;
    }

    fn notify_me(&self) -> bool { false }
}
