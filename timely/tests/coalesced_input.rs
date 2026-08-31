use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::operators::{Input, Inspect};

#[test]
fn input_can_send_again_at_the_same_epoch_after_idle() {
    timely::execute_directly(|worker| {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let output = Rc::clone(&seen);
        let (mut input, ()) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Vec<u64>>();
            stream.inspect(move |item| output.borrow_mut().push(*item));
            (input, ())
        });

        input.send_batch(&mut vec![1]);
        worker.step();
        worker.step();
        assert_eq!(&*seen.borrow(), &[1]);

        input.send_batch(&mut vec![2]);
        worker.step();
        assert_eq!(&*seen.borrow(), &[1, 2]);

        worker.step();
        input.send(3);
        input.flush();
        worker.step();
        assert_eq!(&*seen.borrow(), &[1, 2, 3]);

        worker.step();
        input.send(4);
        input.flush();
        worker.step();
        assert_eq!(&*seen.borrow(), &[1, 2, 3, 4]);
    });
}
