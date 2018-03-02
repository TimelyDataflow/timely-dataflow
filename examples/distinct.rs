extern crate timely;

use std::collections::HashMap;

use timely::dataflow::operators::{ToStream, Inspect};
use timely::dataflow::operators::generic::unary::Unary;
use timely::dataflow::channels::pact::Pipeline;

fn main() {
    timely::example(|scope| {
        let mut counts_by_time = HashMap::new();
        vec![0, 1, 2, 2, 2, 3, 3, 4]
            .into_iter()
            .to_stream(scope)
            .unary_stream(Pipeline, "Distinct", move |input, output| {
                input.for_each(|time, data| {
                    let counts = counts_by_time.entry(time.time().clone())
                                               .or_insert(HashMap::new());
                    let mut session = output.session(&time);
                    for &datum in data.iter() {
                        let count = counts.entry(datum)
                                          .or_insert(0);
                        if *count == 0 {
                           session.give(datum);
                        }
                        *count += 1;
                    }
                })
            })
            .inspect(|x| println!("hello: {:?}", x));
    });
}
