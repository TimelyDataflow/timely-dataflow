//! Test using lgalloc as the allocator for zero-copy data transfer.
//! Note that this example only works on Linux and MacOS.

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod example {
    use std::ops::{Deref, DerefMut};
    use std::ptr::NonNull;
    use timely_communication::{Allocate, Bytesable};
    use timely_communication::allocator::zero_copy::bytes_slab::BytesRefill;

    /// A wrapper that indicates the serialization/deserialization strategy.
    pub struct Message {
        /// Text contents.
        pub payload: String,
    }

    impl Bytesable for Message {
        fn from_bytes(bytes: timely_bytes::arc::Bytes) -> Self {
            Message { payload: std::str::from_utf8(&bytes[..]).unwrap().to_string() }
        }

        fn length_in_bytes(&self) -> usize {
            self.payload.len()
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            writer.write_all(self.payload.as_bytes()).unwrap();
        }
    }

    fn lgalloc_refill(size: usize) -> Box<LgallocHandle> {
        let (pointer, capacity, handle) = lgalloc::allocate::<u8>(size).unwrap();
        let handle = Some(handle);
        Box::new(LgallocHandle { handle, pointer, capacity })
    }

    struct LgallocHandle {
        handle: Option<lgalloc::Handle>,
        pointer: NonNull<u8>,
        capacity: usize,
    }

    impl Deref for LgallocHandle {
        type Target = [u8];
        #[inline(always)]
        fn deref(&self) -> &Self::Target {
            unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.capacity) }
        }
    }

    impl DerefMut for LgallocHandle {
        #[inline(always)]
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { std::slice::from_raw_parts_mut(self.pointer.as_ptr(), self.capacity) }
        }
    }

    impl Drop for LgallocHandle {
        fn drop(&mut self) {
            lgalloc::deallocate(self.handle.take().unwrap());
        }
    }

    pub(crate) fn main() {
        let mut lgconfig = lgalloc::LgAlloc::new();
        lgconfig.enable().with_path(std::env::temp_dir());
        lgalloc::lgalloc_set_config(&lgconfig);

        let refill = BytesRefill {
            logic: std::sync::Arc::new(|size| lgalloc_refill(size) as Box<dyn DerefMut<Target=[u8]>>),
            limit: None,
        };

        // extract the configuration from user-supplied arguments, initialize the computation.
        let config = timely_communication::Config::ProcessBinary(4);
        let (allocators, others) = config.try_build_with(refill).unwrap();
        let guards = timely_communication::initialize_from(allocators, others, |mut allocator| {

            println!("worker {} of {} started", allocator.index(), allocator.peers());

            // allocates a pair of senders list and one receiver.
            let (mut senders, mut receiver) = allocator.allocate(0);

            // send typed data along each channel
            for (i, sender) in senders.iter_mut().enumerate() {
                sender.send(Message { payload: format!("hello, {}", i)});
                sender.done();
            }

            // no support for termination notification,
            // we have to count down ourselves.
            let mut received = 0;
            while received < allocator.peers() {

                allocator.receive();

                if let Some(message) = receiver.recv() {
                    println!("worker {}: received: <{}>", allocator.index(), message.payload);
                    received += 1;
                }

                allocator.release();
            }

            allocator.index()
        });

        // computation runs until guards are joined or dropped.
        if let Ok(guards) = guards {
            for guard in guards.join() {
                println!("result: {:?}", guard);
            }
        }
        else { println!("error in computation"); }
    }
}

// Disable entirely on non-Linux and non-MacOS platforms.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod example {
   pub fn main() { }
}

pub fn main() {
    example::main();
}
