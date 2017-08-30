extern crate timely;
extern crate rdkafka;
extern crate kafkaesque;

use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::capture::Capture;

use rdkafka::config::{ClientConfig, TopicConfig};

use kafkaesque::EventProducer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // target topic name.
        let topic = std::env::args().nth(1).unwrap();
        let count = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();
        let brokers = "localhost:9092";

        // Create Kafka stuff.
        let mut topic_config = TopicConfig::new();
        topic_config
            .set("produce.offset.report", "true")
            .finalize();

        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", brokers)
            .set_default_topic_config(topic_config.clone());

        let topic = format!("{}-{:?}", topic, worker.index());
        let producer = EventProducer::new(producer_config, topic);

        worker.dataflow::<u64,_,_>(|scope|
            (0 .. count)
                .to_stream(scope)
                .capture_into(producer)
        );
    }).unwrap();
}
