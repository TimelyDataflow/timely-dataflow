use abomonation::Abomonation;
use timely::dataflow::operators::capture::event::{Event, EventPusher, EventIterator};

use rdkafka::Message;
use rdkafka::producer::{BaseProducer, ProducerContext};
use rdkafka::consumer::{BaseConsumer, ConsumerContext};


/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventProducer<C: ProducerContext, T, D> {
    topic: String,
    buffer: Vec<u8>,
    producer: BaseProducer<C>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<C: ProducerContext, T, D> EventProducer<C, T, D> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(p: BaseProducer<C>, topic: String) -> Self {
        EventProducer {
            topic: topic,
            buffer: vec![],
            producer: p,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<C: ProducerContext, T: Abomonation, D: Abomonation> EventPusher<T, D> for EventProducer<C, T, D> {
    fn push(&mut self, event: Event<T, D>) {
        unsafe { ::abomonation::encode(&event, &mut self.buffer); }
        self.producer.send_copy::<[u8],()>(self.topic.as_str(), None, Some(&self.buffer[..]), None,  None, None).unwrap();
        self.buffer.clear();
    }
}


/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventConsumer<C: ConsumerContext, T, D> {
    // topic: String,
    consumer: BaseConsumer<C>,
    buffer: Vec<u8>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<C: ConsumerContext, T, D> EventConsumer<C, T, D> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(c: BaseConsumer<C>) -> Self {
        EventConsumer {
            consumer: c,
            buffer: Vec::new(),
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<C: ConsumerContext, T: Abomonation, D: Abomonation> EventIterator<T, D> for EventConsumer<C, T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {

        let buffer = &mut self.buffer;
        match self.consumer.poll(0) {
            Ok(result) =>  { 
                result.map(move |message| {
                    buffer.extend_from_slice(message.payload().unwrap());
                    unsafe { ::abomonation::decode::<Event<T,D>>(&mut buffer[..]).unwrap().0 }
                })
            },
            Err(err) => {
                println!("KafkaConsumer error: {:?}", err);
                None
            },
        }
    }
}