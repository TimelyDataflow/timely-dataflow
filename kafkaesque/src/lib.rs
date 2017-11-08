extern crate rdkafka;
extern crate timely;
extern crate abomonation;

use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use abomonation::Abomonation;
use timely::dataflow::operators::capture::event::{Event, EventPusher, EventIterator};

use rdkafka::Message;
use rdkafka::client::Context;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, ProducerContext, DeliveryResult};
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};

use rdkafka::config::FromClientConfigAndContext;

struct OutstandingCounterContext {
    outstanding: Arc<AtomicIsize>,
}

impl Context for OutstandingCounterContext { }

impl ProducerContext for OutstandingCounterContext {
    type DeliveryContext = ();
    fn delivery(&self, _report: &DeliveryResult, _: Self::DeliveryContext) {
        self.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

impl OutstandingCounterContext {
    pub fn new(counter: &Arc<AtomicIsize>) -> Self {
        OutstandingCounterContext {
            outstanding: counter.clone()
        }
    }
}

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventProducer<T, D> {
    topic: String,
    buffer: Vec<u8>,
    producer: BaseProducer<OutstandingCounterContext>,
    counter: Arc<AtomicIsize>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D> EventProducer<T, D> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        let counter = Arc::new(AtomicIsize::new(0));
        let context = OutstandingCounterContext::new(&counter);
        let producer = BaseProducer::<OutstandingCounterContext>::from_config_and_context(&config, context).expect("Couldn't create producer");
        println!("allocating producer for topic {:?}", topic);
        EventProducer {
            topic: topic,
            buffer: vec![],
            producer: producer,
            counter: counter,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation> EventPusher<T, D> for EventProducer<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        unsafe { ::abomonation::encode(&event, &mut self.buffer); }
        // println!("sending {:?} bytes", self.buffer.len());
        self.producer.send_copy::<[u8],()>(self.topic.as_str(), None, Some(&self.buffer[..]), None,  None, None).unwrap();
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.producer.poll(0);
        self.buffer.clear();
    }
}

impl<T, D> Drop for EventProducer<T, D> {
    fn drop(&mut self) {
        while self.counter.load(Ordering::SeqCst) > 0 {
            self.producer.poll(10);
        }
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventConsumer<T, D> {
    consumer: BaseConsumer<EmptyConsumerContext>,
    buffer: Vec<u8>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D> EventConsumer<T, D> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        println!("allocating consumer for topic {:?}", topic);
        let consumer : BaseConsumer<EmptyConsumerContext> = config.create().expect("Couldn't create consumer");
        consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");
        EventConsumer {
            consumer: consumer,
            buffer: Vec::new(),
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation> EventIterator<T, D> for EventConsumer<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        let buffer = &mut self.buffer;
        if let Some(result) = self.consumer.poll(0) {
            match result {
                Ok(message) =>  {
                    buffer.clear();
                    buffer.extend_from_slice(message.payload().unwrap());
                    Some(unsafe { ::abomonation::decode::<Event<T,D>>(&mut buffer[..]).unwrap().0 })
                },
                Err(err) => {
                    println!("KafkaConsumer error: {:?}", err);
                    None
                },
            }
        }
        else { None }
    }
}