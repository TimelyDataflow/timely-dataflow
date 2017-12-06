use std::rc::Rc;

use timely_logging::{CommsEvent, CommsSetup};
pub use timely_logging::CommunicationEvent;
pub use timely_logging::SerializationEvent;

/// TODO(andreal)
pub type CommsLogger = Rc<::timely_logging::BufferingLogger<CommsSetup, CommsEvent>>;
