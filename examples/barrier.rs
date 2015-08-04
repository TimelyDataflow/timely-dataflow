extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;

use timely::dataflow::*;
use timely::dataflow::operators::*;

fn main() {

    timely::execute_from_args(std::env::args(), |root| {

        root.scoped(|graph| {
            let (handle, stream) = graph.loop_variable::<u64>(RootTimestamp::new(1_000_000), Local(1));
            stream.unary_notify(Pipeline,
                                "Barrier",
                                vec![RootTimestamp::new(0u64)],
                                |_, _, notificator| {
                      while let Some((mut time, _count)) = notificator.next() {
                          println!("iterating");
                          time.inner += 1;
                          notificator.notify_at(&time);
                      }
                  })
                  .connect_loop(handle);
        });

        // spin
        while root.step() { }
    })
}
