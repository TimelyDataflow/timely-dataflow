extern crate clap;
extern crate rdkafka;
extern crate timely;
extern crate abomonation;

use clap::{App, Arg};

use rdkafka::Message;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::producer::BaseProducer;
use rdkafka::consumer::BaseConsumer;

// mod kafka_event;

// fn test_timely(brokers: String, source: String, target: String) {

//     use std::rc::Rc;
//     use std::net::{TcpListener, TcpStream};
//     use std::sync::{Arc, Mutex};
//     use timely::dataflow::Scope;
//     use timely::dataflow::operators::{Capture, ToStream, Inspect};
//     use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay, Extract};

//     use kafka_event::{EventProducer, EventConsumer};

//     // get send and recv endpoints, wrap send to share
//     let (send0, recv0) = ::std::sync::mpsc::channel();
//     let send0 = Arc::new(Mutex::new(send0));
    
//     timely::execute(timely::Configuration::Thread, move |worker| {
    
//         // Create Kafka stuff.
//         let mut topic_config = TopicConfig::new();
//         topic_config
//             .set("produce.offset.report", "true")
//             .finalize();

//         let mut producer_config = ClientConfig::new();
//         producer_config
//             .set("bootstrap.servers", &brokers)
//             .set_default_topic_config(topic_config.clone());

//         let mut consumer_config = ClientConfig::new();
//         consumer_config
//             .set("group.id", "example")
//             .set("enable.partition.eof", "false")
//             .set("auto.offset.reset", "earliest")
//             .set("session.timeout.ms", "6000")
//             .set("bootstrap.servers", &brokers)
//             .set_default_topic_config(topic_config);

//         let producer: BaseProducer<_> = try!(producer_config.create());
//         let consumer: BaseConsumer<_> = try!(consumer_config.create());
//         // Done creating Kafka stuff.

//         // Our "source" is a consumer, from Kafka's point of view.
//         let source = EventConsumer::new(consumer, format!("{}-{:?}", source, worker.index());
//         let target = EventProducer::new(producer, format!("{}-{:?}", target, worker.index());
    
//         worker.dataflow::<u64,_,_>(|scope| {
//             Some(source)
//                 .replay_into(scope)
//                 .inspect(|x| println!("timely says: {:?}", x))
//                 .capture_into(target);
//         });
//     }).unwrap();
// }

fn round_trip(brokers: &str, topic_name: &str) -> Result<(), rdkafka::error::KafkaError> {

    let mut topic_config = TopicConfig::new();
    topic_config
        .set("produce.offset.report", "true")
        .finalize();

    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", brokers)
        .set_default_topic_config(topic_config.clone());

    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("group.id", "example")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("bootstrap.servers", brokers)
        .set_default_topic_config(topic_config);

    let producer: BaseProducer<_> = try!(producer_config.create());
    let consumer: BaseConsumer<_> = try!(consumer_config.create());

    try!(consumer.subscribe(&[topic_name]));

    // give each a chance to sync up?
    try!(consumer.poll(1000));
    producer.poll(1000);

    let text = format!("{:?}", 0);
    try!(producer.send_copy::<str,()>(topic_name, None, Some(text.as_str()), None,  None, None));
    println!("{:?}:\tsend {:?}", ::std::time::Instant::now(), text);

    let mut some_recv: u64 = 0;
    let mut none_recv: u64 = 0;

    while some_recv < 10 {

        producer.poll(0);
        match try!(consumer.poll(0)) {
            // this *never* seems to trigger.
            Some(result) => {
                some_recv += 1;
                println!("{:?}:\trecv {:?}", ::std::time::Instant::now(), result.payload_view::<str>());
                if some_recv < 10 {
                    let text = format!("{}{:?}", result.payload_view::<str>().unwrap().unwrap(), some_recv);
                    try!(producer.send_copy::<str,()>(topic_name, None, Some(text.as_str()), None,  None, None));
                    println!("{:?}:\tsend {:?}", ::std::time::Instant::now(), text);
                }
            },
            // this happens lots.
            None => {
                none_recv += 1;
                if none_recv & (none_recv - 1) == 0 {
                    // print for power-of-two `none_recv`.
                    println!("received .. None ({:?} times)", none_recv);
                }
            }
        }
    }

    Ok(())
}

fn main() {
    let matches = App::new("producer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line producer")
        .arg(Arg::with_name("brokers")
             .short("b")
             .long("brokers")
             .help("Broker list in kafka format")
             .takes_value(true)
             .default_value("localhost:9092"))
        .arg(Arg::with_name("log-conf")
             .long("log-conf")
             .help("Configure the logging format (example: 'rdkafka=trace')")
             .takes_value(true))
        .arg(Arg::with_name("topic")
             .short("t")
             .long("topic")
             .help("Destination topic")
             .takes_value(true)
             .required(true))
        .get_matches();

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();

    match round_trip(brokers, topic) {
        Ok(_) => println!("{:?}:\texit: success!", ::std::time::Instant::now()),
        Err(x) => println!("{:?}:\texit: error: {:?} =/", ::std::time::Instant::now(), x),
    };
}
