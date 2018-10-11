//! Types to combine data in notificators.

use std::marker::PhantomData;

/// Combiner
pub trait Combiner<D> {

    /// Combine two values
    fn combine(&mut D, D);
}

/// Adding combinator
///
/// # Examples
/// ```
/// use timely::dataflow::operators::{ToStream, FrontierNotificator};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::operators::generic::combiner::AddCombiner;
/// use timely::dataflow::channels::pact::Pipeline;
///
/// timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .unary_frontier(Pipeline, "example", |_, _| {
///                let mut notificator = FrontierNotificator::<_, AddCombiner<_>, _>::new();
///                move |input, output| {
///                    input.for_each(|cap, data| {
///                        output.session(&cap).give_vec(&mut data.replace(Vec::new()));
///                        let mut time = cap.time().clone() + 1;
///                        notificator.notify_at(cap.delayed(&time));
///                        assert_eq!(notificator.pending().filter(|t| t.0.time() == &time).count(), 1);
///                    });
///                    notificator.for_each(&[input.frontier()], |cap, _| {
///                        println!("done with time: {:?}", cap.time());
///                    });
///                }
///            });
/// });
/// ```
pub struct AddCombiner<D: ::std::ops::AddAssign<D>> {
    _phantom_data: PhantomData<D>,
}

impl<D: ::std::ops::AddAssign<D>> Combiner<D> for AddCombiner<D> {
    fn combine(target: &mut D, other: D) {
        *target += other
    }
}

/// Max combinator
///
/// # Examples
/// ```
/// use timely::dataflow::operators::{ToStream, FrontierNotificator};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::operators::generic::combiner::MaxCombiner;
/// use timely::dataflow::channels::pact::Pipeline;
///
/// timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .unary_frontier(Pipeline, "example", |_, _| {
///                let mut notificator = FrontierNotificator::<_, MaxCombiner<_>, _>::new();
///                move |input, output| {
///                    input.for_each(|cap, data| {
///                        output.session(&cap).give_vec(&mut data.replace(Vec::new()));
///                        let mut time = cap.time().clone() + 1;
///                        notificator.notify_at(cap.delayed(&time));
///                        assert_eq!(notificator.pending().filter(|t| t.0.time() == &time).count(), 1);
///                    });
///                    notificator.for_each(&[input.frontier()], |cap, _| {
///                        println!("done with time: {:?}", cap.time());
///                    });
///                }
///            });
/// });
/// ```
pub struct MaxCombiner<D: Ord + Copy> {
    _phantom_data: PhantomData<D>,
}

impl<D: Ord + Copy> Combiner<D> for MaxCombiner<D> {
    fn combine(target: &mut D, other: D) {
        *target = ::std::cmp::max(*target, other)
    }
}

impl<D> Combiner<Vec<D>> for Vec<D> {
    fn combine(target: &mut Vec<D>, other: Vec<D>) {
        target.extend(other)
    }
}
