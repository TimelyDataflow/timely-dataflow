//! Traits and types for extracting captured timely dataflow streams.

use super::Event;
use crate::{container::{PushContainer, PushInto}};

/// Supports extracting a sequence of timestamp and data.
pub trait Extract<T, C> {
    /// Converts `self` into a sequence of timestamped data.
    ///
    /// Currently this is only implemented for `Receiver<Event<T, C>>`, and is used only
    /// to easily pull data out of a timely dataflow computation once it has completed.
    ///
    /// # Examples
    ///
    /// ```rust
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
    /// timely::execute(timely::Config::thread(), move |worker| {
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
    /// assert_eq!(recv.extract().into_iter().flat_map(|x| x.1).collect::<Vec<_>>(), (0..10).collect::<Vec<_>>());
    /// ```
    fn extract(self) -> Vec<(T, C)>;
}

impl<T: Ord, C: PushContainer> Extract<T, C> for ::std::sync::mpsc::Receiver<Event<T, C>>
where
    for<'a> C::Item<'a>: PushInto<C> + Ord,
{
    fn extract(self) -> Vec<(T, C)> {
        let mut staged = std::collections::BTreeMap::new();
        for event in self {
            if let Event::Messages(time, data) = event {
                staged.entry(time)
                      .or_insert_with(Vec::new)
                      .push(data);
            }
        }
        let mut result = Vec::new();
        for (time, mut dataz) in staged.into_iter() {
            let mut to_sort = Vec::new();
            for data in dataz.iter_mut() {
                to_sort.extend(data.drain());
            }
            to_sort.sort();
            let mut sorted = C::default();
            for datum in to_sort.into_iter() {
                sorted.push(datum);
            }
            if !sorted.is_empty() {
                result.push((time, sorted));
            }
        }
        result
    }
}
