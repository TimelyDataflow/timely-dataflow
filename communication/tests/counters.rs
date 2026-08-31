use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc::{channel, TryRecvError};
use std::time::Duration;

use timely_communication::allocator::counters::{ArcPusher, Pusher};
use timely_communication::Push;

struct RecordingPusher<T> {
    pushed: Rc<RefCell<Vec<Option<T>>>>,
}

impl<T> Push<T> for RecordingPusher<T> {
    fn push(&mut self, element: &mut Option<T>) {
        self.pushed.borrow_mut().push(element.take());
    }
}

struct ChannelPusher<T>(std::sync::mpsc::Sender<T>);

impl<T> Push<T> for ChannelPusher<T> {
    fn push(&mut self, element: &mut Option<T>) {
        if let Some(element) = element.take() {
            let _ = self.0.send(element);
        }
    }
}

#[test]
fn pusher_coalesces_non_empty_batches() {
    let pushed = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut pusher = Pusher::new(
        RecordingPusher {
            pushed: Rc::clone(&pushed),
        },
        7,
        Rc::clone(&events),
    );

    pusher.send(1);
    pusher.send(2);
    assert_eq!(&*events.borrow(), &[7]);

    pusher.done();
    assert_eq!(&*events.borrow(), &[7, 7]);
    assert_eq!(&*pushed.borrow(), &[Some(1), Some(2), None]);

    pusher.done();
    assert_eq!(&*events.borrow(), &[7, 7]);

    pusher.send(3);
    assert_eq!(&*events.borrow(), &[7, 7, 7]);
    pusher.done();
    assert_eq!(&*events.borrow(), &[7, 7, 7]);
}

#[test]
fn arc_pusher_coalesces_non_empty_batches() {
    let pushed = Rc::new(RefCell::new(Vec::new()));
    let (events_tx, events_rx) = channel();
    let mut pusher = ArcPusher::new(
        RecordingPusher {
            pushed: Rc::clone(&pushed),
        },
        11,
        events_tx,
        timely_communication::buzzer::Buzzer::default(),
    );

    pusher.send(1);
    assert_eq!(events_rx.recv().unwrap(), 11);
    pusher.send(2);
    assert_eq!(events_rx.try_recv(), Err(TryRecvError::Empty));

    pusher.done();
    assert_eq!(events_rx.recv().unwrap(), 11);
    assert_eq!(events_rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(&*pushed.borrow(), &[Some(1), Some(2), None]);

    pusher.done();
    assert_eq!(events_rx.try_recv(), Err(TryRecvError::Empty));

    pusher.send(3);
    assert_eq!(events_rx.recv().unwrap(), 11);
    pusher.done();
    assert_eq!(events_rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn arc_pusher_notifies_messages_arriving_after_the_first_wake() {
    let timeout = Duration::from_secs(5);
    let (data_tx, data_rx) = channel();
    let (events_tx, events_rx) = channel();
    let (continue_tx, continue_rx) = channel();
    let buzzer = timely_communication::buzzer::Buzzer::default();

    let sender = std::thread::spawn(move || {
        let mut pusher = ArcPusher::new(ChannelPusher(data_tx), 13, events_tx, buzzer);
        pusher.send(1);
        continue_rx.recv_timeout(timeout).unwrap();
        pusher.send(2);
        pusher.done();
    });

    assert_eq!(events_rx.recv_timeout(timeout).unwrap(), 13);
    assert_eq!(data_rx.recv_timeout(timeout).unwrap(), 1);
    continue_tx.send(()).unwrap();
    assert_eq!(events_rx.recv_timeout(timeout).unwrap(), 13);
    assert_eq!(data_rx.recv_timeout(timeout).unwrap(), 2);

    sender.join().unwrap();
}
