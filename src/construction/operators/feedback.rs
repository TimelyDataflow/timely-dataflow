use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::{Timestamp, PathSummary, Scope};
use progress::frontier::Antichain;
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::{Data, Message, Observer};
use communication::observer::{Counter, Tee};

use construction::{Stream, GraphBuilder};

pub trait FeedbackExt<G: GraphBuilder> {
    fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
        -> (FeedbackHelper<Counter<FeedbackObserver<G::Timestamp, D>>>, Stream<G, D>);
}

impl<G: GraphBuilder> FeedbackExt<G> for G {
    fn loop_variable<D:Data>(&self, limit: G::Timestamp, summary: <G::Timestamp as Timestamp>::Summary)
        -> (FeedbackHelper<Counter<FeedbackObserver<G::Timestamp, D>>>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        let produced: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();
        let consumed: Rc<RefCell<CountMap<G::Timestamp>>> = Default::default();

        let feedback_output = Counter::new(targets, produced.clone());
        let feedback_input =  Counter::new(FeedbackObserver {
            limit: limit, summary: summary, targets: feedback_output, active: false
        }, consumed.clone());

        let index = self.add_scope(FeedbackScope {
            consumed_messages:  consumed.clone(),
            produced_messages:  produced.clone(),
            summary:            summary,
        });

        let helper = FeedbackHelper {
            index:  index,
            target: feedback_input,
        };

        (helper, Stream::new(ScopeOutput(index, 0), registrar, self.clone()))
    }
}

// implementation of the feedback vertex, essentially, as an observer
pub struct FeedbackObserver<T: Timestamp, D:Data> {
    limit:      T,
    summary:    T::Summary,
    targets:    Counter<Tee<T, D>>,
    active:     bool,
}

impl<T: Timestamp, D: Data> Observer for FeedbackObserver<T, D> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &T) {
        self.active = time.le(&self.limit); // don't send if not less than limit
        if self.active {
            self.targets.open(&self.summary.results_in(time));
        }
    }
    #[inline(always)] fn shut(&mut self, time: &T) {
        if self.active {
            self.targets.shut(&self.summary.results_in(time));
        }
    }
    #[inline(always)] fn give(&mut self, data: &mut Message<D>) {
        if self.active {
            self.targets.give(data);
        }
    }
}


pub trait FeedbackConnectExt<G: GraphBuilder, D: Data> {
    fn connect_loop(&self, FeedbackHelper<Counter<FeedbackObserver<G::Timestamp, D>>>);
}

impl<G: GraphBuilder, D: Data> FeedbackConnectExt<G, D> for Stream<G, D> {
    fn connect_loop(&self, helper: FeedbackHelper<Counter<FeedbackObserver<G::Timestamp, D>>>) {
        self.connect_to(ScopeInput(helper.index, 0), helper.target);
    }

}

// a handy widget for connecting feedback edges
pub struct FeedbackHelper<O: Observer> {
    index:  u64,
    target: O,
}

// impl<O: Observer+'static> FeedbackHelper<O>
// where O::Time: Timestamp, O::Data : Data {
//     pub fn connect_input<G:GraphBuilder<Timestamp=O::Time>>(self, source: &Stream<O::Time, O::Data>, builder: &mut G) {
//         builder.enable(source).connect_to(ScopeInput(self.index, 0), self.target);
//     }
// }

// the scope that the progress tracker interacts with
pub struct FeedbackScope<T:Timestamp> {
    consumed_messages:  Rc<RefCell<CountMap<T>>>,
    produced_messages:  Rc<RefCell<CountMap<T>>>,
    summary:            T::Summary,
}


impl<T:Timestamp> Scope<T> for FeedbackScope<T> {
    fn name(&self) -> String { format!("Feedback") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

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
