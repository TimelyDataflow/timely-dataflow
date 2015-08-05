#![feature(test)]

extern crate timely;
extern crate test;

use test::Bencher;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;

use timely::*;
use timely::dataflow::*;
use timely::dataflow::operators::*;

#[bench]
fn barrier_x100_000(bencher: &mut Bencher) {

    bencher.iter(|| {

        timely::execute(Configuration::Thread, |root| {

            root.scoped(|graph| {
                let (handle, stream) = graph.loop_variable::<u64>(RootTimestamp::new(100_000), Local(1));
                stream.unary_notify(Pipeline,
                                    "Barrier",
                                    vec![RootTimestamp::new(0u64)],
                                    |_, _, notificator| {
                          while let Some((mut time, _count)) = notificator.next() {
                            //   println!("iterating");
                              time.inner += 1;
                              if time.inner < 100_000 {
                                  notificator.notify_at(&time);
                              }
                          }
                      })
                      .connect_loop(handle);
            });

            // spin
            while root.step() { }
        })
    });
}
