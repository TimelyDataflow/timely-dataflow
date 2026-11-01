use timely::order::Product;
use timely::progress::reachability::PortInformation;

#[test]
fn visibility_respects_dominance_and_multiplicity() {
    let mut port = PortInformation::<Product<usize, usize>>::new();
    let a = Product::new(0, 4);
    let b = Product::new(4, 0);
    let later = Product::new(5, 5);
    port.implications.update_iter([(a, 2), (b, 1), (later, 1)]);
    assert!(!port.is_global(&a));
    assert!(port.is_global(&b));
    assert!(!port.is_global(&later));

    port.implications.update_iter([(a, -1)]);
    assert!(port.is_global(&a));
    port.implications.update_iter([(a, -1), (b, -1)]);
    assert!(port.is_global(&later));
}
