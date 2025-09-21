//! Generic operators defined by user-provided closures.

pub mod operator;
pub mod builder_rc;
pub mod builder_raw;
// pub mod builder_ref;
mod handles;
mod notificator;
mod operator_info;

pub use self::handles::{InputHandleCore, OutputBuilder, OutputBuilderSession, Session};
pub use self::notificator::{Notificator, FrontierNotificator};

pub use self::operator::{Operator, source};
pub use self::operator_info::OperatorInfo;
