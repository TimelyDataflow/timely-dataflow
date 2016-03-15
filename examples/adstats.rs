extern crate time;
extern crate timely;
#[macro_use]
extern crate abomonation;
extern crate rand;
extern crate magnetic;

use std::time::Duration;
use std::sync::Arc;
use std::thread;
use std::collections::{HashMap, HashSet};
use rand::Rng;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use abomonation::{encode, decode, Abomonation};
use magnetic::mpmc::mpmc_queue;
use magnetic::buffer::dynamic::DynamicBufferP2;
use magnetic::{Producer, Consumer};

/// Ad event types
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AdEventType {
    Request = 1,
    Impression = 2,
    Click = 3,
    Purchase = 4,
}

impl Abomonation for AdEventType {}


/// AdEvent represents a loggable, immutable event in time related
/// to advertising
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct AdEvent {
    ad_event_type: AdEventType,
    request_id: u64,
    advert_id: u64,
    client_id: u64,
    timestamp: u64,
}

unsafe_abomonate!(AdEvent : ad_event_type, request_id, advert_id, client_id, timestamp);


/// Simulated Ad Event Generator (Request, Impression, Click, Purchase)
struct AdEventGenerator {
    rng: rand::XorShiftRng,
    timestamp: u64,
    current_ad_event: AdEvent,
    impression_rate: f64,
    click_rate: f64,
    purchase_rate: f64,
    clients: Vec<u64>,
    adverts: Vec<u64>,
}

impl AdEventGenerator {
    /// Create an AdEventGenerator with a number of clients and adverts
    /// Note: The rates at which impressions, clicks, and purchases occur
    /// are semi-realistic hard coded values.
    fn new(nclients: u64, nadverts: u64) -> AdEventGenerator {
        let mut rng = rand::weak_rng();
        let clients: Vec<u64> = (0..nclients).into_iter().collect();
        let adverts: Vec<u64> = (0..nadverts).into_iter().collect();
        let request_id = rng.next_u64();
        let client_id = *rng.choose(&clients).unwrap();
        let advert_id = *rng.choose(&adverts).unwrap();
        let timestamp = 0;
        AdEventGenerator{
            rng: rng,
            timestamp: timestamp,
            current_ad_event: AdEvent{
                ad_event_type: AdEventType::Request,
                request_id: request_id,
                advert_id: advert_id,
                client_id: client_id,
                timestamp: timestamp,
            },
            impression_rate: 0.98,
            click_rate: 0.01,
            purchase_rate: 0.002,
            clients: clients,
            adverts: adverts,
        }
    }

    /// Create a new AdEvent
    fn new_event(&mut self) -> AdEvent {
        AdEvent{
            ad_event_type: AdEventType::Request,
            request_id: self.rng.next_u64(),
            advert_id: *self.rng.choose(&self.adverts).unwrap(),
            client_id: *self.rng.choose(&self.clients).unwrap(),
            timestamp: self.timestamp,
        }
    }

    /// Generates the next AdEvent based on a state machine and probability
    /// rates of going from one state to another.
    fn next(&mut self) -> AdEvent {
        self.timestamp += 1;
        let rnum = self.rng.next_f64();
        let mut next = self.current_ad_event.clone();
        match self.current_ad_event.ad_event_type {
            AdEventType::Request => {
                if rnum <= self.impression_rate {
                    next.ad_event_type = AdEventType::Impression;
                    next.timestamp = self.timestamp;
                } else {
                    next = self.new_event();
                }
            }
            AdEventType::Impression => {
                if rnum <= self.click_rate {
                    next.ad_event_type = AdEventType::Click;
                    next.timestamp = self.timestamp;
                } else {
                    next = self.new_event()
                }
            }
            AdEventType::Click => {
                if rnum <= self.purchase_rate {
                    next.ad_event_type = AdEventType::Purchase;
                    next.timestamp = self.timestamp;
                } else {
                    next = self.new_event()
                }
            }
            AdEventType::Purchase => {
                next = self.new_event()
            }
        }
        self.current_ad_event = next;
        next
    }
}

/// AdvertStats
#[derive(Debug)]
struct AdvertStats {
    requests: u64,
    impressions: u64,
    clicks: u64,
    purchases: u64,
    unique_clients: HashSet<u64>,
}

/// ClientStats
#[derive(Debug)]
struct ClientStats {
    requests: u64,
    impressions: u64,
    clicks: u64,
    purchases: u64,
    unique_adverts: HashSet<u64>,
}

/// Ad Stats simulates a scenario in which we have some stats processing
/// cluster which is performing calculations on common digital advertising
/// events. This cluster periodically persists the stats to some storage
/// medium.
fn main() {
    // Multi-Producer Multi-Consumer thread for pushing data into timely
    let (queue_tx, queue_rx) = mpmc_queue(DynamicBufferP2::new(4096).unwrap());
    let nadverts = 10000;
    let nclients = 1000000;
    let nevents = 100000;
    let ngenerators = 10;

    let queue_tx0 = Arc::new(queue_tx);
    let queue_rx0 = Arc::new(queue_rx);

        // Spawn N many event generators simulating something like a threaded
    // server receiving many messages from advert servers
    let mut handles = Vec::with_capacity(ngenerators);
    for generator_id in 0..ngenerators {
        println!("spawning generator {}", generator_id);
        let queue_tx = queue_tx0.clone();
        handles.push(thread::spawn(move || {
            let mut generator = AdEventGenerator::new(nclients, nadverts);
            for event_id in 0..nevents {
                queue_tx.push(generator.next());
            }
        }));
    };

    timely::execute_from_args(std::env::args(), move |computation| {
        let mut client_stats: HashMap<u64, ClientStats> = HashMap::new();
        let mut advert_stats: HashMap<u64, AdvertStats> = HashMap::new();
        let queue_rx = queue_rx0.clone();

        let mut input = computation.scoped(move |builder| {
            //let index = builder.index();
            let (input, input_stream) = builder.new_input();
            // create streams for doing "group by" client and advert ids
            let client_group_stream = input_stream.exchange(|event: &AdEvent| event.client_id);
            let inspected_client_group = client_group_stream.inspect(move |event: &AdEvent| {
                let event_stats = client_stats.entry(event.client_id).or_insert(ClientStats{
                    requests: 0,
                    impressions: 0,
                    clicks: 0,
                    purchases: 0,
                    unique_adverts: HashSet::new(),
                });
                match event.ad_event_type {
                    AdEventType::Request => event_stats.requests += 1,
                    AdEventType::Impression => event_stats.impressions += 1,
                    AdEventType::Click => event_stats.clicks += 1,
                    AdEventType::Purchase => event_stats.purchases += 1,
                }
                event_stats.unique_adverts.insert(event.advert_id);
            });

            let advert_group_stream = input_stream.exchange(|event: &AdEvent| event.advert_id);
            let inspected_advert_group = advert_group_stream.inspect(move |event: &AdEvent| {
                let event_stats = advert_stats.entry(event.advert_id).or_insert(AdvertStats{
                    requests: 0,
                    impressions: 0,
                    clicks: 0,
                    purchases: 0,
                    unique_clients: HashSet::new(),
                });
                match event.ad_event_type {
                    AdEventType::Request => event_stats.requests += 1,
                    AdEventType::Impression => event_stats.impressions += 1,
                    AdEventType::Click => event_stats.clicks += 1,
                    AdEventType::Purchase => event_stats.purchases += 1,
                }
                event_stats.unique_clients.insert(event.client_id);
            });
            input
        });
       
        let mut step = 0;
        let mut misses = 0;
        let mut finished: HashSet<usize> = HashSet::new();
        loop {
            //println!("Processed {} events...", step);
            match queue_rx.try_pop() {
                Some(event) => {
                    input.send(event);
                    input.advance_to(step+1);
                    computation.step();
                    step += 1;
                    misses = 0;
                },
                None => {
                    misses +=1;
                },
            }
            if misses > 100 {
                thread::sleep(Duration::from_millis(1));
            }
            if misses > 10000 {
                println!("Starved or Done!");
                break;
            }
        }
    });
    
    for handle in handles {
        handle.join();
    }
}
