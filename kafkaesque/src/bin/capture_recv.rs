extern crate timely;

extern crate rdkafka;
extern crate kafkaesque;

use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::Replay;

use rdkafka::config::{ClientConfig, TopicConfig};

use kafkaesque::EventConsumer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        let topic = std::env::args().nth(2).unwrap();
        let brokers = "localhost:9092";

        // Create Kafka stuff.
        let mut topic_config = TopicConfig::new();
        topic_config
            .set("produce.offset.report", "true")
            .finalize();

        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("group.id", "example")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &brokers)
            .set_default_topic_config(topic_config);

        // create replayers from disjoint partition of source worker identifiers.
        let replayers = 
        (0 .. source_peers)
            .filter(|i| i % worker.peers() == worker.index())
            .map(|i| {
                // let consumer = consumer_config.create().expect("Couldn't create consumer");
                EventConsumer::<_,u64>::new(consumer_config.clone(), format!("{}-{:?}", topic, i))
            })
            .collect::<Vec<_>>();

        worker.dataflow::<u64,_,_>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
