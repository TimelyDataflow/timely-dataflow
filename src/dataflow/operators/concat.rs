use Data;
use dataflow::channels::pact::Pipeline;
use dataflow::{Stream, Scope};
use dataflow::operators::binary::Binary;

// NOTE : These used to have more exotic implementations that connected observers in a tangle.
// NOTE : It was defective when used with the Observer protocol, because it just forwarded open and
// NOTE : shut messages, without concern for the fact that there would be multiple opens and shuts,
// NOTE : from each of its inputs. This implementation does more staging of data, but should be
// NOTE : less wrong.

// NOTE : Observers don't exist any more, so maybe it could become a horrible tangle again! :D
// NOTE : Not clear that the tangle buys much, as buffers are simply passed along without copying.

pub trait Concat<G: Scope, D: Data> {
    fn concat(&self, &Stream<G, D>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Concat<G, D> for Stream<G, D> {
    fn concat(&self, other: &Stream<G, D>) -> Stream<G, D> {
        self.binary_stream(other, Pipeline, Pipeline, "Concat", |input1, input2, output| {
            while let Some((time, data)) = input1.next() {
                output.session(time).give_content(data);
            }
            while let Some((time, data)) = input2.next() {
                output.session(time).give_content(data);
            }
        })
    }
}

pub trait Concatenate<G: Scope, D: Data> {
    fn concatenate(&self, Vec<Stream<G, D>>) -> Stream<G, D>;
}

impl<G: Scope, D: Data> Concatenate<G, D> for G {
    fn concatenate(&self, mut sources: Vec<Stream<G, D>>) -> Stream<G, D> {
        if let Some(mut result) = sources.pop() {
            while let Some(next) = sources.pop() {
                result = result.concat(&next);
            }
            result
        }
        else {
            panic!("must pass at least one stream to concatenate");
        }
    }
}
