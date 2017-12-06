//! Traits and types for extracting captured timely dataflow streams.

use super::Event;

/// Supports extracting a sequence of timestamp and data.
pub trait Extract<T: Ord, D: Ord> {
    /// Converts `self` into a sequence of timestamped data.
    /// 
    /// Currently this is only implemented for `Receiver<Event<T, D>>`, and is used only
    /// to easily pull data out of a timely dataflow computation once it has completed.
    ///
    /// #Examples
    ///
    /// ```rust,ignore
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLink, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Configuration::Thread, move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLink::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    fn extract(self) -> Vec<(T, Vec<D>)>;
}

impl<T: Ord, D: Ord> Extract<T,D> for ::std::sync::mpsc::Receiver<Event<T, D>> {
    fn extract(self) -> Vec<(T, Vec<D>)> {
        let mut result = Vec::new();
        for event in self {
            if let Event::Messages(time, data) = event {
                result.push((time, data));
            }
        }
        result.sort_by(|x,y| x.0.cmp(&y.0));

        let mut current = 0;
        for i in 1 .. result.len() {
            if result[current].0 == result[i].0 {
                let dataz = ::std::mem::replace(&mut result[i].1, Vec::new());
                result[current].1.extend(dataz);
            }
            else {
                current = i;
            }
        }

        for &mut (_, ref mut data) in &mut result {
            data.sort();
        }
        result.retain(|x| !x.1.is_empty());
        result
    }
}
