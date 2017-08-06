//! Generic operators defined by user-provided closures.

pub mod unary;
pub mod binary;
pub mod operator;
mod handles;
mod notificator;

pub use self::handles::{InputHandle, FrontieredInputHandle, OutputHandle};
pub use self::notificator::{Notificator, FrontierNotificator};

pub use self::unary::Unary;
pub use self::binary::Binary;
pub use self::operator::{Operator, source};