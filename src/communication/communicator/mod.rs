pub use self::thread::Thread;
pub use self::process::Process;
pub use self::binary::Binary;
pub use self::generic::Generic;

use communication::{Data, Pullable};
use communication::observer::BoxedObserver;
use serialization::Serializable;

pub mod thread;
pub mod process;
pub mod binary;
pub mod generic;

// The Communicator trait presents the interface a worker has to the outside world.
// The worker can see its index, the total number of peers, and acquire channels to and from the other workers.
// There is an assumption that each worker performs the same channel allocation logic; things go wrong otherwise.
pub trait Communicator: 'static {
    fn index(&self) -> u64;     // number out of peers
    fn peers(&self) -> u64;     // number of peers
    fn new_channel<T, D>(&mut self) -> (Vec<BoxedObserver<T, D>>, Box<Pullable<T, D>>)
        where T: Data+Serializable, D: Data+Serializable;
}
