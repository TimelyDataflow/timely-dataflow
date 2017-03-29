mod lsb;
mod lsb_swc;
mod msb;
mod stash;

pub use lsb::RadixSorter as LSBRadixSorter;
pub use lsb_swc::RadixSorter as LSBSWCRadixSorter;
pub use msb::RadixSorter as MSBRadixSorter;

pub trait Unsigned : Ord {
    fn bytes() -> usize;
    fn as_u64(&self) -> u64;
}

impl Unsigned for  u8 { #[inline]fn bytes() -> usize { 1 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u16 { #[inline]fn bytes() -> usize { 2 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u32 { #[inline]fn bytes() -> usize { 4 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u64 { #[inline]fn bytes() -> usize { 8 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for usize { #[inline]fn bytes() -> usize { ::std::mem::size_of::<usize>() } #[inline]fn as_u64(&self) -> u64 { *self as u64 } }
