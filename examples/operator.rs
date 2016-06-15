extern crate timely;
extern crate timely_communication;

use timely_communication::Configuration;
use timely::dataflow::operators::{ToStream};
use timely::dataflow::operators::{Operator};
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
}
