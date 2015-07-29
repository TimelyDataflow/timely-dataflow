use communication::Data;
use communication::pact::Pipeline;
use construction::{Stream, GraphBuilder};
use construction::operators::binary::BinaryStreamExt;

// NOTE : These used to have more exotic implementations that connected observers in a tangle.
// NOTE : It was defective when used with the Observer protocol, because it just forwarded open and
// NOTE : shut messages, without concern for the fact that there would be multiple opens and shuts,
// NOTE : from each of its inputs. This implementation does more staging of data, but should be
// NOTE : less wrong.

pub trait ConcatExt<G: GraphBuilder, D: Data> {
    fn concat(&self, &Stream<G, D>) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> ConcatExt<G, D> for Stream<G, D> {
    fn concat(&self, other: &Stream<G, D>) -> Stream<G, D> {
        self.binary_stream(other, Pipeline, Pipeline, "concat", |input1, input2, output| {
            while let Some((time, data)) = input1.pull() {
                output.session(time).give_message(data);
            }
            while let Some((time, data)) = input2.pull() {
                output.session(time).give_message(data);
            }
        })
    }
}

pub trait ConcatVecExt<G: GraphBuilder, D: Data> {
    fn concatenate(&self, Vec<Stream<G, D>>) -> Stream<G, D>;
}

impl<G: GraphBuilder, D: Data> ConcatVecExt<G, D> for G {
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
