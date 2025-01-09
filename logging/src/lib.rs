
use std::rc::Rc;
use std::cell::RefCell;
use std::any::Any;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::fmt::{self, Debug};
use std::marker::PhantomData;

use timely_container::{ContainerBuilder, PushInto};

pub struct Registry {
    /// A map from names to typed loggers.
    map: HashMap<String, (Box<dyn Any>, Box<dyn Flush>)>,
    /// An instant common to all logging statements.
    time: Instant,
}

impl Registry {
    /// Binds a log name to an action on log event batches.
    ///
    /// This method also returns any pre-installed action, rather than overwriting it
    /// and pivoting the logging destination mid-stream. New loggers with this name will
    /// use the new destination, and existing loggers will use the old destination.
    ///
    /// The action should respond to a sequence of events with non-decreasing timestamps
    /// (Durations) and well as a timestamp that lower bounds the next event that could be
    /// seen (likely greater or equal to the timestamp of the last event). The end of a
    /// logging stream is indicated only by dropping the associated action, which can be
    /// accomplished with `remove` (or a call to insert, though this is not recommended).
    pub fn insert<CB: ContainerBuilder, F: FnMut(&Duration, &mut CB::Container)+'static>(
        &mut self,
        name: &str,
        action: F) -> Option<Box<dyn Any>>
    {
        let logger = Logger::<CB>::new(self.time, Duration::default(), action);
        self.insert_logger(name, logger)
    }

    /// Binds a log name to a logger.
    pub fn insert_logger<CB: ContainerBuilder>(&mut self, name: &str, logger: Logger<CB>) -> Option<Box<dyn Any>> {
        self.map.insert(name.to_owned(), (Box::new(logger.clone()), Box::new(logger))).map(|x| x.0)
    }

    /// Removes a bound logger.
    ///
    /// This is intended primarily to close a logging stream and let the associated writer
    /// communicate that the stream is closed to any consumers. If a binding is not removed,
    /// then the stream cannot be complete as in principle anyone could acquire a handle to
    /// the logger and start further logging.
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn Any>> {
        self.map.remove(name).map(|x| x.0)
    }

    /// Retrieves a shared logger, if one has been inserted.
    pub fn get<CB: ContainerBuilder>(&self, name: &str) -> Option<Logger<CB>> {
        self.map
            .get(name)
            .and_then(|entry| entry.0.downcast_ref::<Logger<CB>>())
            .map(|x| (*x).clone())
    }

    /// Creates a new logger registry.
    pub fn new(time: Instant) -> Self {
        Registry {
            time,
            map: HashMap::new(),
        }
    }

    /// Flushes all registered logs.
    pub fn flush(&mut self) {
        <Self as Flush>::flush(self);
    }
}

impl Flush for Registry {
    fn flush(&self) {
        for value in self.map.values() {
            value.1.flush();
        }
    }
}

/// A buffering logger.
pub struct Logger<CB: ContainerBuilder> {
    inner: Rc<RefCell<LoggerInner<CB, dyn FnMut(&Duration, &mut CB::Container)>>>,
}

impl<CB: ContainerBuilder> Clone for Logger<CB> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<CB: ContainerBuilder + Debug> Debug for Logger<CB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logger")
            .field("inner", &self.inner)
            .finish()
    }
}

struct LoggerInner<CB: ContainerBuilder, A: ?Sized + FnMut(&Duration, &mut CB::Container)> {
    /// common instant used for all loggers.
    time:   Instant,
    /// offset to allow re-calibration.
    offset: Duration,
    /// container builder to produce buffers of accumulated log events
    builder: CB,
    /// True if we logged an event since the last flush.
    /// Used to avoid sending empty buffers on drop.
    dirty: bool,
    /// action to take on full log buffers.
    action: A,
}

impl<CB: ContainerBuilder> Logger<CB> {
    /// Allocates a new shareable logger bound to a write destination.
    pub fn new<F>(time: Instant, offset: Duration, action: F) -> Self
    where
        F: FnMut(&Duration, &mut CB::Container)+'static
    {
        let inner = LoggerInner {
            time,
            offset,
            action,
            builder: CB::default(),
            dirty: false,
        };
        let inner = Rc::new(RefCell::new(inner));
        Logger { inner }
    }

    /// Logs an event.
    ///
    /// The event has its timestamp recorded at the moment of logging, but it may be delayed
    /// due to buffering. It will be written when the logger is next flushed, either due to
    /// the buffer reaching capacity or a direct call to flush.
    ///
    /// This implementation borrows a shared (but thread-local) buffer of log events, to ensure
    /// that the `action` only sees one stream of events with increasing timestamps. This may
    /// have a cost that we don't entirely understand.
    pub fn log<T>(&self, event: T) where CB: PushInto<(Duration, T)> {
        self.log_many(Some(event));
    }

    /// Logs multiple events.
    ///
    /// The event has its timestamp recorded at the moment of logging, but it may be delayed
    /// due to buffering. It will be written when the logger is next flushed, either due to
    /// the buffer reaching capacity or a direct call to flush.
    ///
    /// All events in this call will have the same timestamp. This can be more performant due
    /// to fewer `time.elapsed()` calls, but it also allows some logged events to appear to be
    /// "transactional", occurring at the same moment.
    ///
    /// This implementation borrows a shared (but thread-local) buffer of log events, to ensure
    /// that the `action` only sees one stream of events with increasing timestamps. This may
    /// have a cost that we don't entirely understand.
    pub fn log_many<I>(&self, events: I)
    where I: IntoIterator, CB: PushInto<(Duration, I::Item)>
    {
        self.inner.borrow_mut().log_many(events)
    }

    /// Flushes logged messages and communicates the new minimal timestamp.
    pub fn flush(&self) {
        <Self as Flush>::flush(self);
    }

    /// Obtain a typed logger.
    pub fn into_typed<T>(self) -> TypedLogger<CB, T> {
        self.into()
    }
}

/// A logger that's typed to specific events. Its `log` functions accept events that can be
/// converted into `T`. Dereferencing a `TypedLogger` gives you a [`Logger`] that can log any
/// compatible type.
///
/// Construct a `TypedLogger` with [`Logger::into_typed`] or by calling `into` on a `Logger`.
#[derive(Debug)]
pub struct TypedLogger<CB: ContainerBuilder, T> {
    inner: Logger<CB>,
    _marker: PhantomData<T>,
}

impl<CB: ContainerBuilder, T> TypedLogger<CB, T> {
    /// Logs an event. Equivalent to [`Logger::log`], with the exception that it converts the
    /// event to `T` before logging.
    pub fn log<S: Into<T>>(&self, event: S)
    where
        CB: PushInto<(Duration, T)>,
    {
        self.inner.log(event.into());
    }

    /// Logs multiple events. Equivalent to [`Logger::log_many`], with the exception that it
    /// converts the events to `T` before logging.
    pub fn log_many<I>(&self, events: I)
    where
        I: IntoIterator, I::Item: Into<T>,
        CB: PushInto<(Duration, T)>,
    {
        self.inner.log_many(events.into_iter().map(Into::into));
    }
}

impl<CB: ContainerBuilder, T> Clone for TypedLogger<CB, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<CB: ContainerBuilder, T> From<Logger<CB>> for TypedLogger<CB, T> {
    fn from(inner: Logger<CB>) -> Self {
        TypedLogger {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<CB: ContainerBuilder, T> std::ops::Deref for TypedLogger<CB, T> {
    type Target = Logger<CB>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<CB: ContainerBuilder, A: ?Sized + FnMut(&Duration, &mut CB::Container)> LoggerInner<CB, A> {
    fn log_many<I>(&mut self, events: I)
        where I: IntoIterator, CB: PushInto<(Duration, I::Item)>,
    {
        let elapsed = self.time.elapsed() + self.offset;
        for event in events {
            self.dirty = true;
            self.builder.push_into((elapsed, event.into()));
            while let Some(container) = self.builder.extract() {
                (self.action)(&elapsed, container);
            }
        }
    }

    fn flush(&mut self) {
        let elapsed = self.time.elapsed() + self.offset;

        let mut action_ran = false;
        while let Some(buffer) = self.builder.finish() {
            (self.action)(&elapsed, buffer);
            action_ran = true;
        }

        if !action_ran {
            // Send an empty container to indicate progress.
            (self.action)(&elapsed, &mut CB::Container::default());
        }

        self.dirty = false;
    }
}

/// Flush on the *last* drop of a logger.
impl<CB: ContainerBuilder, A: ?Sized + FnMut(&Duration, &mut CB::Container)> Drop for LoggerInner<CB, A> {
    fn drop(&mut self) {
        // Avoid sending out empty buffers just because of drops.
        if self.dirty {
            self.flush();
        }
    }
}

impl<CB, A: ?Sized + FnMut(&Duration, &mut CB::Container)> Debug for LoggerInner<CB, A>
where
    CB: ContainerBuilder + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggerInner")
            .field("time", &self.time)
            .field("offset", &self.offset)
            .field("dirty", &self.dirty)
            .field("action", &"FnMut")
            .field("builder", &self.builder)
            .finish()
    }
}

/// Types that can be flushed.
trait Flush {
    /// Flushes buffered data.
    fn flush(&self);
}

impl<CB: ContainerBuilder> Flush for Logger<CB> {
    fn flush(&self) {
        self.inner.borrow_mut().flush()
    }
}
