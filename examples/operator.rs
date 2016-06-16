extern crate timely;
extern crate timely_communication;

use std::collections::HashMap;

use timely_communication::Configuration;
use timely::dataflow::operators::{ToStream, Input};
use timely::dataflow::operators::{Operator, FrontierNotificator};
use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::{InputHandle, OutputHandle, Inspect};
use timely::progress::Antichain;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::scopes::Root;
use timely::dataflow::Scope;

fn main() {
    timely::execute(Configuration::Thread, |root| {
        root.scoped(|scope| {
            (0u64..10).to_stream(scope)
                .unary_frontier(Pipeline, "example", |mut builder| {
                    let mut cap = Some(builder.get_cap(RootTimestamp::new(12)));
                    move |(input, _frontier), output| {
                        cap = None;
                        while let Some((time, data)) = input.next() {
                            output.session(&time).give_content(data);
                        }
                    }
                }).inspect(|x| println!("{:?}", x));
        });
    }).unwrap();

    timely::execute(Configuration::Thread, |root| {
        root.scoped(|scope| {
            (0u64..10).to_stream(scope)
                .unary(Pipeline, "example", |mut builder| {
                    let mut cap = Some(builder.get_cap(RootTimestamp::new(12)));
                    move |input, output| {
                        if let Some(ref c) = cap.take() {
                            output.session(&c).give(100);
                        }
                        while let Some((time, data)) = input.next() {
                            output.session(&time).give_content(data);
                        }
                    }
                }).inspect(|x| println!("{:?}", x));
        });
    }).unwrap();

    timely::execute(Configuration::Thread, |root| {
        root.scoped(|scope| {
            let stream2 = (0u64..10).to_stream(scope);
            (0u64..10).to_stream(scope)
                .binary(&stream2, Pipeline, Pipeline, "example", |mut builder| {
                    let mut cap = Some(builder.get_cap(RootTimestamp::new(12)));
                    move |input1, input2, output| {
                        if let Some(ref c) = cap.take() {
                            output.session(&c).give(100);
                        }
                        while let Some((time, data)) = input1.next() {
                            output.session(&time).give_content(data);
                        }
                        while let Some((time, data)) = input2.next() {
                            output.session(&time).give_content(data);
                        }
                    }
                }).inspect(|x| println!("{:?}", x));
        });
    }).unwrap();

    timely::execute(Configuration::Thread, |root| {
        let (mut in1, mut in2) = root.scoped(|scope| {
            let (in1_handle, in1) = scope.new_input();
            let (in2_handle, in2) = scope.new_input();
            in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut builder| {
                let mut notificator = FrontierNotificator::new();
                let mut stash = HashMap::new();
                move |(input1, frontier1), (input2, frontier2), output| {
                    while let Some((time, data)) = input1.next() {
                        stash.entry(time.time()).or_insert(Vec::new()).extend(data.drain(..));
                        notificator.notify_at(time);
                    }
                    while let Some((time, data)) = input2.next() {
                        stash.entry(time.time()).or_insert(Vec::new()).extend(data.drain(..));
                        notificator.notify_at(time);
                    }
                    for time in notificator.iter(&[frontier1, frontier2]) {
                        if let Some(mut vec) = stash.remove(&time.time()) {
                            output.session(&time).give_iterator(vec.drain(..));
                        }
                    }
                }
            }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));

            (in1_handle, in2_handle)
        });

        for i in 1..10 {
            in1.send(i - 1);
            in1.advance_to(i);
            in2.send(i - 1);
            in2.advance_to(i);
        }
    }).unwrap();
}
