use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use {Data, Push};

use progress::{Timestamp, Operate, PathSummary};
use progress::frontier::Antichain;
use progress::nested::Source::ChildOutput;
use progress::nested::Target::ChildInput;
use progress::count_map::CountMap;

use progress::nested::product::Product;
use progress::nested::Summary::Local;

use dataflow::channels::Content;
use dataflow::channels::pushers::{Counter, Tee};

use dataflow::{Stream, Scope};
use dataflow::scopes::Child;

pub trait LoopVariable<G: Scope, T: Timestamp> {
    fn loop_variable<D: Data>(&self, limit: T, summary: T::Summary) -> (Helper<G::Timestamp, T, D>, Stream<Child<G, T>, D>);
}

impl<G: Scope, T: Timestamp> LoopVariable<G, T> for Child<G, T> {
    fn loop_variable<D: Data>(&self, limit: T, summary: T::Summary) -> (Helper<G::Timestamp, T, D>, Stream<Child<G, T>, D>) {


        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();
        let produced: Rc<RefCell<CountMap<Product<G::Timestamp, T>>>> = Default::default();
        let consumed: Rc<RefCell<CountMap<Product<G::Timestamp, T>>>> = Default::default();

        let feedback_output = Counter::new(targets, produced.clone());
        let feedback_input =  Counter::new(Observer {
            limit: limit, summary: summary, targets: feedback_output
        }, consumed.clone());

        let index = self.add_operator(Operator {
            consumed_messages:  consumed.clone(),
            produced_messages:  produced.clone(),
            summary:            Local(summary),
        });

        let helper = Helper {
            index:  index,
            target: feedback_input,
            // phant: ::std::marker::PhantomData,
        };

        (helper, Stream::new(ChildOutput(index, 0), registrar, self.clone()))


    }
}

// pub trait LoopVariable<G: Scope> {
//     fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
//         -> (Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>, Stream<G, D>);
// }
//
// impl<G: Scope> LoopVariable<G> for G {
//     fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
//         -> (Helper<G::Timestamp, D, Counter<G::Timestamp, D, Observer<G::Timestamp, D>>>, Stream<G, D>) {
//
//         let (targets, registrar) = Tee::<G::Timestamp, D>::new();
//         let produced: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();
//         let consumed: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();
//
//         let feedback_output = Counter::new(targets, produced.clone());
//         let feedback_input =  Counter::new(Observer {
//             limit: limit, summary: summary, targets: feedback_output
//         }, consumed.clone());
//
//         let index = self.add_operator(Operator {
//             consumed_messages:  consumed.clone(),
//             produced_messages:  produced.clone(),
//             summary:            summary,
//         });
//
//         let helper = Helper {
//             index:  index,
//             target: feedback_input,
//             phant: ::std::marker::PhantomData,
//         };
//
//         (helper, Stream::new(ChildOutput(index, 0), registrar, self.clone()))
//     }
// }

// implementation of the feedback vertex, essentially, as an observer
pub struct Observer<TOuter: Timestamp, TInner: Timestamp, D:Data> {
    limit:      TInner,
    summary:    TInner::Summary,
    targets:    Counter<Product<TOuter, TInner>, D, Tee<Product<TOuter, TInner>, D>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Data> Push<(Product<TOuter, TInner>, Content<D>)> for Observer<TOuter, TInner, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<(Product<TOuter, TInner>, Content<D>)>) {
        let active = if let Some((ref mut time, _)) = *message {
            time.inner = self.summary.results_in(&time.inner);
            self.limit.ge(&time.inner)
        }
        else { true };

        if active { self.targets.push(message); }
    }
}


pub trait ConnectLoop<G: Scope, T: Timestamp, D: Data> {
    fn connect_loop(&self, Helper<G::Timestamp, T, D>);
}

impl<G: Scope, T: Timestamp, D: Data> ConnectLoop<G, T, D> for Stream<Child<G, T>, D> {
    fn connect_loop(&self, helper: Helper<G::Timestamp, T, D>) {
        self.connect_to(ChildInput(helper.index, 0), helper.target, usize::max_value());
    }
}

// , Counter<G::Timestamp, D, Observer<G::Timestamp, D>>

// a handy widget for connecting feedback edges
pub struct Helper<TOuter: Timestamp, TInner: Timestamp, D: Data> {
    index:  usize,
    target: Counter<Product<TOuter, TInner>, D, Observer<TOuter, TInner, D>>
    // phant: ::std::marker::PhantomData<(T, D)>,
}

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
