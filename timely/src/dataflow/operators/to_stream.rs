//! Conversion to the `Stream` type from iterators.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::dataflow::channels::Message;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::{Scope, Stream};
use crate::progress::Timestamp;
use crate::scheduling::SyncActivator;
use crate::Data;

/// Converts to a timely `Stream`.
pub trait ToStream<T: Timestamp, D: Data> {
    /// Converts to a timely `Stream`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

impl<T: Timestamp, I: IntoIterator+'static> ToStream<T, I::Item> for I where I::Item: Data {
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, I::Item> {

        source(scope, "ToStream", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                    for element in iterator.by_ref().take((256 * Message::<T, I::Item>::default_length()) - 1) {
                        session.give(element);
                    }
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}

/// Errors encountered during event streaming
#[derive(Clone, Copy, Debug)]
pub enum StreamError {
    /// Encountered time less than the most recent Progress event
    TimeMovedBackwards,
    /// The error returned when activation fails across thread boundaries because the receiving end
    /// has hung up.
    SyncActivationError,
}

/// Data and progress events of the native stream.
pub enum Event<T, D> {
    /// Indicates that timestamps have advanced to time T
    Progress(T),
    /// Indicates that event D happened at time T
    Message(T, D),
}

struct SharedState {
    waker: Option<Waker>,
    state: Poll<Result<(), StreamError>>,
}

/// Interop between native async futures and timely's fibers
///
/// This structs holds on to both wakers (Waker and SyncActivator) and translates wakes and results
/// between the two systems
pub struct TimelyFuture {
    shared_state: Arc<Mutex<SharedState>>,
    activator: SyncActivator,
}

impl Future for TimelyFuture {
    type Output = Result<(), StreamError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Look at the shared state to see if the operator has already completed.
        let mut shared_state = self.shared_state.lock().unwrap();

        match shared_state.state {
            // The operator has finished, just return its result
            Poll::Ready(result) => Poll::Ready(result),
            // There is some work for the operator to do
            Poll::Pending => {
                // Set waker so that timely can wake up the current task when the work has
                // completed, ensuring that the future is polled again and sees that `state =
                // Poll::Ready(_)`.
                let new_waker = cx.waker();
                match &mut shared_state.waker {
                    Some(waker) if waker.will_wake(new_waker) => (),
                    waker => *waker = Some(new_waker.clone()),
                };

                // Release the mutex and activate the operator to do some work
                drop(shared_state);
                match self.activator.activate() {
                    Ok(()) => Poll::Pending,
                    Err(_) => {
                        let mut shared_state = self.shared_state.lock().unwrap();
                        let err_state = Poll::Ready(Err(StreamError::SyncActivationError));
                        shared_state.waker = None;
                        shared_state.state = err_state;

                        err_state
                    }
                }
            }
        }
    }
}

/// Converts to a timely `Stream`.
pub trait ToStreamAsync<T: Timestamp, D: Data> {
    /// Converts a [native `Stream`](futures_util::stream::Stream) of [`Event`s](Event) into a [timely
    /// `Stream`](crate::dataflow::Stream). It returns a pair of future and timely stream. The
    /// future resolves when the entirety of the native stream has been consumed or an error has
    /// occured.
    ///
    /// It is necessary to poll the returned future to drive the stream, usually by spawning as a
    /// separate task with an executor;
    ///
    /// # Examples
    ///
    /// ```notest,rust
    /// use timely::dataflow::operators::{Event, ToStreamAsync};
    /// use timely::dataflow::operators::Inspect;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let iter = (0..4)
    ///         .map(|x| Event::Message(0, x))
    ///         .chain(Some(Event::Progress(1)));
    ///     let stream = futures_util::stream::iter(iter);
    ///
    ///     timely::example(move |scope| {
    ///         let (future, stream) = stream.to_stream(scope);
    ///         tokio::spawn(future);
    ///         stream.inspect(|x| println!("seen: {:?}", x));
    ///     });
    /// }
    /// ```
    fn to_stream<S: Scope<Timestamp = T>>(self: Pin<Box<Self>>, scope: &S) -> (TimelyFuture, Stream<S, D>);
}

impl<T, D, I> ToStreamAsync<T, D> for I
where
    T: Timestamp,
    D: Data,
    I: futures_core::Stream<Item = Event<T, D>> + ?Sized + 'static,
{
    fn to_stream<S: Scope<Timestamp = T>>(self: Pin<Box<Self>>, scope: &S) -> (TimelyFuture, Stream<S, D>) {
        let mut activator = None;
        let shared_state = Arc::new(Mutex::new(SharedState {
            state: Poll::Pending,
            waker: None,
        }));

        // Closure state
        let mut source_stream = self;
        let source_shared_state = shared_state.clone();
        let source_activator = &mut activator;
        let source_scope = &scope;

        let timely_stream = source(scope, "ToStreamAsync", move |capability, info| {
            // Acquire an activator, so that the operator can reschedule itself.
            *source_activator = Some(source_scope.sync_activator_for(&info.address[..]));

            let mut capability = Some(capability);
            move |output| {
                let mut shared_state = source_shared_state.lock().unwrap();

                // If the outer future hasn't been polled yet, there is nothing to do
                if let (Some(waker), Poll::Pending) =
                    (shared_state.waker.take(), shared_state.state)
                {
                    let mut context = Context::from_waker(&waker);

                    // Consume all the ready items of the source_stream and issue them to the operator
                    while let Poll::Ready(item) = source_stream.as_mut().poll_next(&mut context) {
                        match item {
                            Some(Event::Progress(time)) => {
                                let cap = capability.as_mut().unwrap();
                                if !cap.time().less_equal(&time) {
                                    shared_state.state =
                                        Poll::Ready(Err(StreamError::TimeMovedBackwards));
                                    break;
                                }
                                cap.downgrade(&time);
                            }
                            Some(Event::Message(time, data)) => {
                                let cap = capability.as_ref().unwrap();
                                if !cap.time().less_equal(&time) {
                                    shared_state.state =
                                        Poll::Ready(Err(StreamError::TimeMovedBackwards));
                                    break;
                                }
                                output.session(&cap.delayed(&time)).give(data);
                            }
                            None => {
                                capability = None;
                                shared_state.state = Poll::Ready(Ok(()));
                                break;
                            }
                        }
                    }
                    if let Poll::Ready(_) = shared_state.state {
                        // Release the mutex and wake up the native task to communicate progress
                        drop(shared_state);
                        waker.wake();
                    }
                }
            }
        });

        // `source` promises to call the provided closure before returning,
        // so we are guaranteed that `activator` is non-None.
        let activator = activator.unwrap();
        let future = TimelyFuture {
            shared_state,
            activator,
        };

        (future, timely_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_stream() {
        use crate::dataflow::operators::Capture;
        use crate::dataflow::operators::capture::Extract;
        use crate::example;

        let native_stream = futures_util::stream::iter((0..3).map(|x| Event::Message(0, x)));
        let native_stream = Box::pin(native_stream);

        let (data1, data2) = example(|scope| {
            let (future, stream) = native_stream.to_stream(scope);
            tokio::spawn(future);

            let data1 = stream.capture();
            let data2 = vec![0,1,2].to_stream(scope).capture();

            (data1, data2)
        });

        assert_eq!(data1.extract(), data2.extract());
    }
}
