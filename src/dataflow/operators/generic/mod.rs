//! Generic operators defined by user-provided closures.

// pub mod unary;
// pub mod binary;
pub mod combiner;
pub mod operator;
pub mod builder_rc;
pub mod builder_raw;
// pub mod builder_ref;
mod handles;
mod notificator;
mod operator_info;

pub use self::handles::{InputHandle, FrontieredInputHandle, OutputHandle};
pub use self::notificator::{Notificator, FrontierNotificator, Zero};

// pub use self::unary::Unary;
// pub use self::binary::Binary;
pub use self::operator::{Operator, source};
pub use self::operator_info::OperatorInfo;
