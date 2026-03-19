//! Live dataflow visualization over WebSocket.
//!
//! This module provides a [`Server`] that streams timely logging events as JSON
//! over WebSocket to the browser-based visualizer (`visualizer/index.html`).
//!
//! # Usage
//!
//! ```ignore
//! use timely::visualizer::Server;
//!
//! let server = Server::start(51371);
//!
//! timely::execute_from_args(std::env::args(), move |worker| {
//!     server.register(worker);
//!     // ... build and run dataflows ...
//! }).unwrap();
//! ```
//!
//! Then open `visualizer/index.html` in a browser and connect to `ws://localhost:51371`.
//!
//! Requires the `visualizer` feature flag.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use tungstenite::{accept, Message};

use crate::logging::{TimelyEvent, TimelyEventBuilder, StartStop};
use crate::worker::Worker;
use crate::communication::Allocate;

/// A handle to the visualization WebSocket server.
///
/// The server accepts browser connections and streams timely events as JSON.
/// Structural events (`Operates`, `Channels`) are replayed to late-connecting
/// clients so they can reconstruct the dataflow graph.
///
/// `Server` is cheaply cloneable — all clones share the same underlying channel
/// and server thread. The server thread runs until all `Server` handles (and
/// their associated senders) are dropped.
#[derive(Clone)]
pub struct Server {
    tx: Arc<Mutex<mpsc::Sender<String>>>,
}

impl Server {
    /// Start the WebSocket visualization server on the given port.
    ///
    /// Spawns a background thread that accepts WebSocket connections and
    /// broadcasts events. The thread exits when all `Server` handles are
    /// dropped.
    ///
    /// # Panics
    ///
    /// Panics if the port cannot be bound.
    pub fn start(port: u16) -> Self {
        let (tx, rx) = mpsc::channel::<String>();

        thread::spawn(move || run_ws_server(port, rx));

        eprintln!("Visualizer WebSocket server on ws://localhost:{port}");
        eprintln!("Open visualizer/index.html and connect to the address above.");

        Server {
            tx: Arc::new(Mutex::new(tx)),
        }
    }

    /// Register the timely event logger for this worker.
    ///
    /// This installs a logging callback on the `"timely"` log stream that
    /// serializes events as JSON and sends them to the WebSocket server.
    pub fn register<A: Allocate>(&self, worker: &mut Worker<A>) {
        let tx = Arc::clone(&self.tx);
        let worker_index = worker.index();

        worker.log_register().unwrap().insert::<TimelyEventBuilder, _>(
            "timely",
            move |_time, data| {
                if let Some(data) = data {
                    let tx = tx.lock().unwrap();
                    for (elapsed, event) in data.iter() {
                        let duration_ns =
                            elapsed.as_secs() as u64 * 1_000_000_000
                            + elapsed.subsec_nanos() as u64;
                        if let Some(json) = event_to_json(worker_index, duration_ns, event) {
                            let _ = tx.send(json);
                        }
                    }
                }
            },
        );
    }
}

/// Convert a timely event to a JSON string: `[worker, duration_ns, event]`.
fn event_to_json(worker: usize, duration_ns: u64, event: &TimelyEvent) -> Option<String> {
    let event_json = match event {
        TimelyEvent::Operates(e) => {
            let addr: Vec<String> = e.addr.iter().map(|a| a.to_string()).collect();
            let name = e.name.replace('\\', "\\\\").replace('"', "\\\"");
            format!(
                r#"{{"Operates": {{"id": {}, "addr": [{}], "name": "{}"}}}}"#,
                e.id, addr.join(", "), name,
            )
        }
        TimelyEvent::Channels(e) => {
            let scope_addr: Vec<String> = e.scope_addr.iter().map(|a| a.to_string()).collect();
            let typ = e.typ.replace('\\', "\\\\").replace('"', "\\\"");
            format!(
                r#"{{"Channels": {{"id": {}, "scope_addr": [{}], "source": [{}, {}], "target": [{}, {}], "typ": "{}"}}}}"#,
                e.id, scope_addr.join(", "),
                e.source.0, e.source.1, e.target.0, e.target.1, typ,
            )
        }
        TimelyEvent::Schedule(e) => {
            let ss = match e.start_stop {
                StartStop::Start => "\"Start\"",
                StartStop::Stop => "\"Stop\"",
            };
            format!(
                r#"{{"Schedule": {{"id": {}, "start_stop": {}}}}}"#,
                e.id, ss,
            )
        }
        TimelyEvent::Messages(e) => {
            format!(
                r#"{{"Messages": {{"is_send": {}, "channel": {}, "source": {}, "target": {}, "seq_no": {}, "record_count": {}}}}}"#,
                e.is_send, e.channel, e.source, e.target, e.seq_no, e.record_count,
            )
        }
        TimelyEvent::Shutdown(e) => {
            format!(r#"{{"Shutdown": {{"id": {}}}}}"#, e.id)
        }
        _ => return None,
    };
    Some(format!("[{worker}, {duration_ns}, {event_json}]"))
}

const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Run the WebSocket server. Batches events and replays structural events to
/// late-connecting clients.
fn run_ws_server(port: u16, rx: mpsc::Receiver<String>) {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .unwrap_or_else(|e| panic!("Failed to bind to port {port}: {e}"));
    listener.set_nonblocking(true).expect("Cannot set non-blocking");

    let mut clients: Vec<tungstenite::WebSocket<TcpStream>> = Vec::new();
    let mut batch: Vec<String> = Vec::new();
    let mut replay: Vec<String> = Vec::new();
    let mut done = false;

    loop {
        let deadline = Instant::now() + FLUSH_INTERVAL;

        // Accept pending connections (non-blocking).
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    eprintln!("Visualizer client connected from {addr}");
                    stream.set_nonblocking(false).ok();
                    match accept(stream) {
                        Ok(mut ws) => {
                            if !replay.is_empty() {
                                let payload = format!("[{}]", replay.join(","));
                                let msg = Message::Text(payload.into());
                                if ws.send(msg).is_err() {
                                    eprintln!("Failed to send replay; dropping client");
                                    continue;
                                }
                            }
                            clients.push(ws);
                        }
                        Err(e) => eprintln!("WebSocket handshake failed: {e}"),
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("Accept error: {e}");
                    break;
                }
            }
        }

        // Drain the channel until the flush deadline.
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(remaining) {
                Ok(json) => {
                    if json.contains("\"Operates\"") || json.contains("\"Channels\"") {
                        replay.push(json.clone());
                    }
                    batch.push(json);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => { done = true; break; }
            }
        }

        // Flush batch to all connected clients.
        if !batch.is_empty() && !clients.is_empty() {
            let payload = format!("[{}]", batch.join(","));
            let msg = Message::Text(payload.into());
            clients.retain_mut(|ws| {
                match ws.send(msg.clone()) {
                    Ok(_) => true,
                    Err(_) => {
                        eprintln!("Visualizer client disconnected");
                        false
                    }
                }
            });
        }
        batch.clear();

        if done { break; }
    }

    // Close all clients gracefully.
    for ws in clients.iter_mut() {
        let _ = ws.close(None);
        loop {
            match ws.read() {
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    }
}
