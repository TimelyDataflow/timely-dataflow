use std::ops::{Deref, DerefMut};
use timely_communication::{Allocate, Bytesable};

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
    let (ptr, cap, handle) = lgalloc::allocate::<u8>(size).unwrap();
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), cap) };
    let handle = Some(handle);
    Box::new(LgallocHandle { handle, slice: slice.into() })
}

struct LgallocHandle {
    handle: Option<lgalloc::Handle>,
    slice: Box<[u8]>
}

impl Deref for LgallocHandle {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.slice
    }
}

impl DerefMut for LgallocHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slice
    }
}

impl Drop for LgallocHandle {
    fn drop(&mut self) {
        Box::leak(std::mem::take(&mut self.slice));
        lgalloc::deallocate(self.handle.take().unwrap());
    }
}

fn main() {
    let mut config = lgalloc::LgAlloc::new();
    config.enable().with_path(std::env::temp_dir()); 
    lgalloc::lgalloc_set_config(&config);

    let refill = std::sync::Arc::new(|size| lgalloc_refill(size) as Box<dyn DerefMut<Target=[u8]>>);

    // extract the configuration from user-supplied arguments, initialize the computation.
    let config = timely_communication::Config::ProcessBinary(4);
    let (allocators, others) = config.try_build_with(refill).unwrap();
    let guards = timely_communication::initialize_from(allocators, others, |mut allocator| {

        println!("worker {} of {} started", allocator.index(), allocator.peers());

        // allocates a pair of senders list and one receiver.
        let (mut senders, mut receiver) = allocator.allocate(0);

        // send typed data along each channel
        for i in 0 .. allocator.peers() {
            senders[i].send(Message { payload: format!("hello, {}", i)});
            senders[i].done();
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
