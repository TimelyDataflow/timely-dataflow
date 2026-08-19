//! Tests for messages stamped by multiple, or zero, capabilities.

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::order::Product;
use timely::progress::Stamp;

/// A message stamped by two incomparable timestamps traverses a channel whole,
/// is observed with both stamp elements, can be re-sent via `retain_stamp`,
/// and the computation drains (the progress books balance).
#[test]
fn multi_capability_stamps() {
    let seen = timely::execute_directly(move |worker| {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = Rc::clone(&seen);
        worker.dataflow::<u64, _, _>(move |scope| {
            scope.iterative::<u64, _, _>(move |inner| {
                // A source holding two incomparable capabilities, sending one
                // message stamped by both.
                let mut builder = OperatorBuilder::new("source".to_owned(), inner.clone());
                let (mut output, stream) = builder.new_output::<Vec<u64>>();
                builder.build(move |mut init_caps| {
                    let cap = init_caps.pop().unwrap();
                    let mut caps = Some(CapabilitySet::from(vec![
                        cap.delayed(&Product::new(0, 1)),
                        cap.delayed(&Product::new(1, 0)),
                    ]));
                    move |_frontiers| {
                        if let Some(caps) = caps.take() {
                            let mut data = vec![1u64, 2, 3];
                            output.activate().give(&caps, &mut data);
                        }
                    }
                });

                // A relay that receives the stamped message and forwards it under
                // the capability set minted from the message's stamp.
                let mut builder = OperatorBuilder::new("relay".to_owned(), inner.clone());
                let (mut output, forwarded) = builder.new_output::<Vec<u64>>();
                let mut input = builder.new_input(stream, Pipeline);
                builder.build(move |_init_caps| {
                    move |_frontiers| {
                        let mut output = output.activate();
                        input.for_each(|cap, data| {
                            let caps = cap.retain_stamp(0);
                            // A stamp element must justify times beyond it.
                            let _upper = cap.delayed(&Product::new(1, 1), 0);
                            output.give(&caps, data);
                        });
                    }
                });

                // A sink recording the stamps and data it observes.
                let mut builder = OperatorBuilder::new("sink".to_owned(), inner.clone());
                let mut input = builder.new_input(forwarded, Pipeline);
                builder.build(move |_init_caps| {
                    move |_frontiers| {
                        input.for_each(|cap, data| {
                            seen2.borrow_mut().push((cap.stamp().clone(), std::mem::take(data)));
                        });
                    }
                });
            });
        });
        while worker.step() { }
        Rc::try_unwrap(seen).unwrap().into_inner()
    });

    let expected_stamp: Stamp<Product<u64, u64>> =
        vec![Product::new(0, 1), Product::new(1, 0)].into_iter().collect();
    assert_eq!(seen, vec![(expected_stamp, vec![1, 2, 3])]);
}

/// A message sent under an empty capability set is delivered with an empty
/// stamp, despite the sender holding no capabilities at all, and despite the
/// message making no progress claims.
#[test]
fn zero_capability_stamps() {
    let seen = timely::execute_directly(move |worker| {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = Rc::clone(&seen);
        worker.dataflow::<u64, _, _>(move |scope| {
            let mut builder = OperatorBuilder::new("source".to_owned(), scope.clone());
            let (mut output, stream) = builder.new_output::<Vec<u64>>();
            builder.build(move |init_caps| {
                let mut once = Some(init_caps);
                move |_frontiers| {
                    if let Some(init_caps) = once.take() {
                        // Drop all capabilities, then send anyway.
                        drop(init_caps);
                        let empty = CapabilitySet::<u64>::new();
                        let mut data = vec![4u64, 5, 6];
                        output.activate().give(&empty, &mut data);
                    }
                }
            });

            let mut builder = OperatorBuilder::new("sink".to_owned(), scope.clone());
            let mut input = builder.new_input(stream, Pipeline);
            builder.build(move |_init_caps| {
                move |_frontiers| {
                    input.for_each(|cap, data| {
                        seen2.borrow_mut().push((cap.stamp().clone(), std::mem::take(data)));
                    });
                }
            });
        });
        while worker.step() { }
        Rc::try_unwrap(seen).unwrap().into_inner()
    });

    assert_eq!(seen, vec![(Stamp::new(), vec![4, 5, 6])]);
}

/// A multi-stamp message crossing a data exchange arrives at each worker with
/// the stamp intact, partitioned by the exchange function.
#[test]
fn multi_stamps_exchange() {
    let guards = timely::execute(timely::Config::process(2), move |worker| {
        let index = worker.index();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = Rc::clone(&seen);
        worker.dataflow::<u64, _, _>(move |scope| {
            scope.iterative::<u64, _, _>(move |inner| {
                let mut builder = OperatorBuilder::new("source".to_owned(), inner.clone());
                let (mut output, stream) = builder.new_output::<Vec<u64>>();
                builder.build(move |mut init_caps| {
                    let cap = init_caps.pop().unwrap();
                    let mut caps = (index == 0).then(|| CapabilitySet::from(vec![
                        cap.delayed(&Product::new(0, 1)),
                        cap.delayed(&Product::new(1, 0)),
                    ]));
                    move |_frontiers| {
                        if let Some(caps) = caps.take() {
                            let mut data = (0..10u64).collect::<Vec<_>>();
                            output.activate().give(&caps, &mut data);
                        }
                    }
                });

                let mut builder = OperatorBuilder::new("sink".to_owned(), inner.clone());
                let mut input = builder.new_input(stream, Exchange::new(|x: &u64| *x));
                builder.build(move |_init_caps| {
                    move |_frontiers| {
                        input.for_each(|cap, data| {
                            seen2.borrow_mut().push((cap.stamp().clone(), std::mem::take(data)));
                        });
                    }
                });
            });
        });
        while worker.step() { }
        Rc::try_unwrap(seen).unwrap().into_inner()
    }).unwrap();

    let expected_stamp: Stamp<Product<u64, u64>> =
        vec![Product::new(0, 1), Product::new(1, 0)].into_iter().collect();
    let results = guards.join().into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
    for (worker, seen) in results.iter().enumerate() {
        let mut received = Vec::new();
        for (stamp, data) in seen.iter() {
            assert_eq!(stamp, &expected_stamp);
            received.extend(data.iter().copied());
        }
        received.sort();
        let expected = (0..10u64).filter(|x| (*x as usize) % 2 == worker).collect::<Vec<_>>();
        assert_eq!(received, expected);
    }
}

/// Stamps restore minimality and canonical order under insertion and mapping.
#[test]
fn frame_canonical_form() {
    let mut stamp = Stamp::new();
    assert!(stamp.insert(Product::new(1u64, 0u64)));
    assert!(stamp.insert(Product::new(0, 1)));
    // Dominated by (0, 1).
    assert!(!stamp.insert(Product::new(1, 1)));
    assert_eq!(stamp.elements(), &[Product::new(0, 1), Product::new(1, 0)]);

    // Projecting away the inner coordinate collapses the antichain.
    let outer = stamp.map_into(|time| Some(time.outer));
    assert_eq!(outer.elements(), &[0u64]);

    // Mapping may discard elements entirely.
    let filtered = stamp.map_into(|time| if time.outer == 0 { None } else { Some(time.clone()) });
    assert_eq!(filtered.elements(), &[Product::new(1, 0)]);
}

/// A multi-stamp message leaving a scope must remain accounted at one outer
/// pointstamp per inner stamp element, even when projecting away the inner
/// timestamp coordinate makes elements comparable. Collapsing the stamp here
/// would strand produced counts at the parent and wedge the computation.
#[test]
fn multi_stamp_leave_collapse() {
    use timely::dataflow::operators::Leave;
    let seen = timely::execute_directly(move |worker| {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen2 = Rc::clone(&seen);
        worker.dataflow::<u64, _, _>(move |scope| {
            let outer = scope.clone();
            let stream = scope.iterative::<u64, _, _>(move |inner| {
                let mut builder = OperatorBuilder::new("source".to_owned(), inner.clone());
                let (mut output, stream) = builder.new_output::<Vec<u64>>();
                builder.build(move |mut init_caps| {
                    let cap = init_caps.pop().unwrap();
                    let mut caps = Some(CapabilitySet::from(vec![
                        cap.delayed(&Product::new(0, 1)),
                        cap.delayed(&Product::new(1, 0)),
                    ]));
                    move |_frontiers| {
                        if let Some(caps) = caps.take() {
                            let mut data = vec![7u64];
                            output.activate().give(&caps, &mut data);
                        }
                    }
                });
                stream.leave(outer)
            });

            let mut builder = OperatorBuilder::new("sink".to_owned(), stream.scope());
            let mut input = builder.new_input(stream, Pipeline);
            builder.build(move |_init_caps| {
                move |_frontiers| {
                    input.for_each(|cap, data| {
                        seen2.borrow_mut().push((cap.stamp().clone(), std::mem::take(data)));
                    });
                }
            });
        });
        while worker.step() { }
        Rc::try_unwrap(seen).unwrap().into_inner()
    });

    // The outer stamp retains both elements, unminimized: the message remains
    // accounted at both outer times 0 and 1.
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0.elements(), &[0u64, 1]);
    assert_eq!(seen[0].1, vec![7u64]);
}
