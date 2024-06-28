//! Traits and types for partially ordered sets.

/// A type that is partially ordered.
///
/// This trait is distinct from Rust's `PartialOrd` trait, because the implementation
/// of that trait precludes a distinct `Ord` implementation. We need an independent
/// trait if we want to have a partially ordered type that can also be sorted.
pub trait PartialOrder<Rhs: ?Sized = Self>: PartialEq<Rhs> {
    /// Returns true iff one element is strictly less than the other.
    fn less_than(&self, other: &Rhs) -> bool {
        self.less_equal(other) && self != other
    }
    /// Returns true iff one element is less than or equal to the other.
    fn less_equal(&self, other: &Rhs) -> bool;
}

/// A type that is totally ordered.
///
/// This trait is a "carrier trait", in the sense that it adds no additional functionality
/// over `PartialOrder`, but instead indicates that the `less_than` and `less_equal` methods
/// are total, meaning that `x.less_than(&y)` is equivalent to `!y.less_equal(&x)`.
///
/// This trait is distinct from Rust's `Ord` trait, because several implementors of
/// `PartialOrd` also implement `Ord` for efficient canonicalization, deduplication,
/// and other sanity-maintaining operations.
pub trait TotalOrder : PartialOrder { }

/// A type that does not affect total orderedness.
///
/// This trait is not useful, but must be made public and documented or else Rust
/// complains about its existence in the constraints on the implementation of
/// public traits for public types.
pub trait Empty : PartialOrder { }

impl Empty for () { }

macro_rules! implement_partial {
    ($($index_type:ty,)*) => (
        $(
            impl PartialOrder for $index_type {
                #[inline] fn less_than(&self, other: &Self) -> bool { self < other }
                #[inline] fn less_equal(&self, other: &Self) -> bool { self <= other }
            }
        )*
    )
}

macro_rules! implement_total {
    ($($index_type:ty,)*) => (
        $(
            impl TotalOrder for $index_type { }
        )*
    )
}

implement_partial!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, (), ::std::time::Duration,);
implement_total!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, (), ::std::time::Duration,);

pub use product::Product;
pub use product::flatcontainer::ProductRegion as FlatProductRegion;
/// A pair of timestamps, partially ordered by the product order.
mod product {
    use std::fmt::{Formatter, Error, Debug};

    use crate::container::columnation::{Columnation, Region};
    use crate::order::{Empty, TotalOrder};
    use crate::progress::Timestamp;
    use crate::progress::timestamp::PathSummary;
    use crate::progress::timestamp::Refines;

    /// A nested pair of timestamps, one outer and one inner.
    ///
    /// We use `Product` rather than `(TOuter, TInner)` so that we can derive our own `PartialOrder`,
    /// because Rust just uses the lexicographic total order.
    #[derive(Abomonation, Copy, Clone, Hash, Eq, PartialEq, Default, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct Product<TOuter, TInner> {
        /// Outer timestamp.
        pub outer: TOuter,
        /// Inner timestamp.
        pub inner: TInner,
    }

    impl<TOuter, TInner> Product<TOuter, TInner> {
        /// Creates a new product from outer and inner coordinates.
        pub fn new(outer: TOuter, inner: TInner) -> Self {
            Product {
                outer,
                inner,
            }
        }
    }

    // Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Product<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.outer, self.inner))
        }
    }

    use super::PartialOrder;
    impl<TOuter, TOuter2, TInner, TInner2> PartialOrder<Product<TOuter2, TInner2>> for Product<TOuter, TInner>
    where
        TOuter: PartialOrder<TOuter2>,
        TInner: PartialOrder<TInner2>,
        Self: PartialEq<Product<TOuter2, TInner2>>,
    {
        #[inline]
        fn less_equal(&self, other: &Product<TOuter2, TInner2>) -> bool {
            self.outer.less_equal(&other.outer) && self.inner.less_equal(&other.inner)
        }
    }

    impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for Product<TOuter, TInner> {
        type Summary = Product<TOuter::Summary, TInner::Summary>;
        fn minimum() -> Self { Self { outer: TOuter::minimum(), inner: TInner::minimum() }}
    }

    impl<TOuter: Timestamp, TInner: Timestamp> PathSummary<Product<TOuter, TInner>> for Product<TOuter::Summary, TInner::Summary> {
        #[inline]
        fn results_in(&self, product: &Product<TOuter, TInner>) -> Option<Product<TOuter, TInner>> {
            self.outer.results_in(&product.outer)
                .and_then(|outer|
                    self.inner.results_in(&product.inner)
                        .map(|inner| Product::new(outer, inner))
                )
        }
        #[inline]
        fn followed_by(&self, other: &Self) -> Option<Self> {
            self.outer.followed_by(&other.outer)
                .and_then(|outer|
                    self.inner.followed_by(&other.inner)
                        .map(|inner| Product::new(outer, inner))
                )
        }
    }

    impl<TOuter: Timestamp, TInner: Timestamp> Refines<TOuter> for Product<TOuter, TInner> {
        fn to_inner(other: TOuter) -> Self {
            Product::new(other, TInner::minimum())
        }
        fn to_outer(self: Product<TOuter, TInner>) -> TOuter {
            self.outer
        }
        fn summarize(path: <Self as Timestamp>::Summary) -> <TOuter as Timestamp>::Summary {
            path.outer
        }
    }

    impl<T1: Empty, T2: Empty> Empty for Product<T1, T2> { }
    impl<T1, T2> TotalOrder for Product<T1, T2> where T1: Empty, T2: TotalOrder { }

    impl<T1: Columnation, T2: Columnation> Columnation for Product<T1, T2> {
        type InnerRegion = ProductRegion<T1::InnerRegion, T2::InnerRegion>;
    }

    #[derive(Default)]
    pub struct ProductRegion<T1, T2> {
        outer_region: T1,
        inner_region: T2,
    }

    impl<T1: Region, T2: Region> Region for ProductRegion<T1, T2> {
        type Item = Product<T1::Item, T2::Item>;

        #[inline]
        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            Product { outer: self.outer_region.copy(&item.outer), inner: self.inner_region.copy(&item.inner) }
        }

        fn clear(&mut self) {
            self.outer_region.clear();
            self.inner_region.clear();
        }

        fn reserve_items<'a, I>(&mut self, items1: I) where Self: 'a, I: Iterator<Item=&'a Self::Item> + Clone {
            let items2 = items1.clone();
            self.outer_region.reserve_items(items1.map(|x| &x.outer));
            self.inner_region.reserve_items(items2.map(|x| &x.inner))
        }

        fn reserve_regions<'a, I>(&mut self, regions1: I) where Self: 'a, I: Iterator<Item=&'a Self> + Clone {
            let regions2 = regions1.clone();
            self.outer_region.reserve_regions(regions1.map(|r| &r.outer_region));
            self.inner_region.reserve_regions(regions2.map(|r| &r.inner_region));
        }

        fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            self.outer_region.heap_size(&mut callback);
            self.inner_region.heap_size(callback);
        }
    }

    pub mod flatcontainer {
        use timely_container::flatcontainer::{IntoOwned, Push, Region, RegionPreference, ReserveItems};
        use super::Product;

        impl<TO: RegionPreference, TI: RegionPreference> RegionPreference for Product<TO, TI> {
            type Owned = Product<TO::Owned, TI::Owned>;
            type Region = ProductRegion<TO::Region, TI::Region>;
        }

        /// Region to store [`Product`] timestamps.
        #[derive(Default, Clone, Debug)]
        pub struct ProductRegion<RO: Region, RI: Region> {
            outer_region: RO,
            inner_region: RI,
        }

        impl<RO: Region, RI: Region> Region for ProductRegion<RO, RI> {
            type Owned = Product<RO::Owned, RI::Owned>;
            type ReadItem<'a> = Product<RO::ReadItem<'a>, RI::ReadItem<'a>> where Self: 'a;
            type Index = (RO::Index, RI::Index);

            #[inline]
            fn merge_regions<'a>(regions: impl Iterator<Item=&'a Self> + Clone) -> Self where Self: 'a {
                let outer_region = RO::merge_regions(regions.clone().map(|r| &r.outer_region));
                let inner_region = RI::merge_regions(regions.map(|r| &r.inner_region));
                Self { outer_region, inner_region }
            }

            #[inline]
            fn index(&self, (outer, inner): Self::Index) -> Self::ReadItem<'_> {
                Product::new(self.outer_region.index(outer), self.inner_region.index(inner))
            }

            #[inline]
            fn reserve_regions<'a, I>(&mut self, regions: I) where Self: 'a, I: Iterator<Item=&'a Self> + Clone {
                self.outer_region.reserve_regions(regions.clone().map(|r| &r.outer_region));
                self.inner_region.reserve_regions(regions.map(|r| &r.inner_region));
            }

            #[inline]
            fn clear(&mut self) {
                self.outer_region.clear();
                self.inner_region.clear();
            }

            #[inline]
            fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
                self.outer_region.heap_size(&mut callback);
                self.inner_region.heap_size(callback);
            }

            #[inline]
            fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> where Self: 'a {
                Product::new(RO::reborrow(item.outer), RI::reborrow(item.inner))
            }
        }

        impl<'a, TOuter, TInner> IntoOwned<'a> for Product<TOuter, TInner>
        where
            TOuter: IntoOwned<'a>,
            TInner: IntoOwned<'a>,
        {
            type Owned = Product<TOuter::Owned, TInner::Owned>;

            fn into_owned(self) -> Self::Owned {
                Product::new(self.outer.into_owned(), self.inner.into_owned())
            }

            fn clone_onto(self, other: &mut Self::Owned) {
                self.outer.clone_onto(&mut other.outer);
                self.inner.clone_onto(&mut other.inner);
            }

            fn borrow_as(owned: &'a Self::Owned) -> Self {
                Product::new(IntoOwned::borrow_as(&owned.outer), IntoOwned::borrow_as(&owned.inner))
            }
        }

        impl<'a, RO, RI> ReserveItems<Product<RO::ReadItem<'a>, RI::ReadItem<'a>>> for ProductRegion<RO, RI>
        where
            RO: Region + ReserveItems<<RO as Region>::ReadItem<'a>> + 'a,
            RI: Region + ReserveItems<<RI as Region>::ReadItem<'a>> + 'a,
        {
            #[inline]
            fn reserve_items<I>(&mut self, items: I) where I: Iterator<Item=Product<RO::ReadItem<'a>, RI::ReadItem<'a>>> + Clone {
                self.outer_region.reserve_items(items.clone().map(|i| i.outer));
                self.inner_region.reserve_items(items.clone().map(|i| i.inner));
            }
        }

        impl<TO, TI, RO, RI> Push<Product<TO, TI>> for ProductRegion<RO, RI>
        where
            RO: Region + Push<TO>,
            RI: Region + Push<TI>,
        {
            #[inline]
            fn push(&mut self, item: Product<TO, TI>) -> Self::Index {
                (
                    self.outer_region.push(item.outer),
                    self.inner_region.push(item.inner)
                )
            }
        }

        impl<'a, TO, TI, RO, RI> Push<&'a Product<TO, TI>> for ProductRegion<RO, RI>
        where
            RO: Region + Push<&'a TO>,
            RI: Region + Push<&'a TI>,
        {
            #[inline]
            fn push(&mut self, item: &'a Product<TO, TI>) -> Self::Index {
                (
                    self.outer_region.push(&item.outer),
                    self.inner_region.push(&item.inner)
                )
            }
        }

        impl<'a, TO, TI, RO, RI> Push<&&'a Product<TO, TI>> for ProductRegion<RO, RI>
        where
            RO: Region + Push<&'a TO>,
            RI: Region + Push<&'a TI>,
        {
            #[inline]
            fn push(&mut self, item: && 'a Product<TO, TI>) -> Self::Index {
                (
                    self.outer_region.push(&item.outer),
                    self.inner_region.push(&item.inner)
                )
            }
        }
    }
}

/// Rust tuple ordered by the lexicographic order.
mod tuple {

    use super::PartialOrder;
    impl<TOuter, TOuter2, TInner, TInner2> PartialOrder<(TOuter2, TInner2)> for (TOuter, TInner)
    where
        TOuter: PartialOrder<TOuter2>,
        TInner: PartialOrder<TInner2>,
        (TOuter, TInner): PartialEq<(TOuter2, TInner2)>,
    {
        #[inline]
        fn less_equal(&self, other: &(TOuter2, TInner2)) -> bool {
            // We avoid Rust's `PartialOrd` implementation, for reasons of correctness.
            self.0.less_than(&other.0) || (self.0.eq(&other.0) && self.1.less_equal(&other.1))
        }
    }

    use super::TotalOrder;
    impl<T1, T2> TotalOrder for (T1, T2) where T1: TotalOrder, T2: TotalOrder { }

    use crate::progress::Timestamp;
    impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for (TOuter, TInner) {
        type Summary = (TOuter::Summary, TInner::Summary);
        fn minimum() -> Self { (TOuter::minimum(), TInner::minimum()) }
    }

    use crate::progress::timestamp::PathSummary;
    impl<TOuter: Timestamp, TInner: Timestamp> PathSummary<(TOuter, TInner)> for (TOuter::Summary, TInner::Summary) {
        #[inline]
        fn results_in(&self, (outer, inner): &(TOuter, TInner)) -> Option<(TOuter, TInner)> {
            self.0.results_in(outer)
                .and_then(|outer|
                    self.1.results_in(inner)
                        .map(|inner| (outer, inner))
                )
        }
        #[inline]
        fn followed_by(&self, (outer, inner): &(TOuter::Summary, TInner::Summary)) -> Option<(TOuter::Summary, TInner::Summary)> {
            self.0.followed_by(outer)
                .and_then(|outer|
                    self.1.followed_by(inner)
                        .map(|inner| (outer, inner))
                )
        }
    }

    use crate::progress::timestamp::Refines;
    impl<TOuter: Timestamp, TInner: Timestamp> Refines<TOuter> for (TOuter, TInner) {
        fn to_inner(other: TOuter) -> Self {
            (other, TInner::minimum())
        }
        fn to_outer(self: (TOuter, TInner)) -> TOuter {
            self.0
        }
        fn summarize(path: <Self as Timestamp>::Summary) -> <TOuter as Timestamp>::Summary {
            path.0
        }
    }

    use super::Empty;
    impl<T1: Empty, T2: Empty> Empty for (T1, T2) { }
}
