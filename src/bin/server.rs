use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    process,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use log::{debug, error, info, warn};
use rand::RngExt;
use yp_streaming_quotes::{error::RequestError, quote::StockQuote};
use yp_streaming_quotes::{
    protocol::{Message, Response},
    tickers,
};

const CLIENT_TIMEOUT_SECS: u64 = 5;
const WATCHER_FREQUENCY_SECS: u64 = 5;
const GENERATION_INTERVAL_MS: u64 = 50;

// Subscribers is just a thread safe map with shared ownership (Arc<RwLock<>>)
// with separate read and write locks to avoid blocking when possible.
// The map itself is just SockerAddr [works as client id here] -> (tx, tickers)
// tx is a handle to send relevant quotes to the client thread, tickers - tickers filter
type Subscribers = Arc<RwLock<HashMap<SocketAddr, (mpsc::Sender<StockQuote>, HashSet<String>)>>>;

// KeepAlive is a thread safe map with shared ownership (Arc<RwLock<>>)
// it contains SocketAddr and an AtomicU64 [for last_seen].
// The reason we chose AtomicU64 instead of an instant or just u64 is
// we want to update last_seen without blocking the map on every single "PING\n" received.
// Write lock only taken in 2 cases:
//   1. TCP subscribe is initiated, then we add the new client to the map
//   2. When WATCHER thread detects a client that didn't send PINGs for prolonged time
//      it takes the write lock to remove said client.
type KeepAlive = Arc<RwLock<HashMap<SocketAddr, Arc<AtomicU64>>>>;

#[derive(Parser, Debug)]
struct Args {
    /// TCP port for the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,

    #[arg(short, long, default_value = "assets/tickers.txt")]
    tickers_file: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    env_logger::builder()
        .parse_filters("info")
        .target(env_logger::Target::Stdout)
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    let tickers_file = File::open(&args.tickers_file)?;
    let valid_tickers = Arc::new(tickers::load_tickers(tickers_file)?);
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0")?);
    let subscribers: Subscribers = Arc::new(RwLock::new(HashMap::new()));
    let keep_alive: KeepAlive = Arc::new(RwLock::new(HashMap::new()));

    {
        let subscribers = Arc::clone(&subscribers);
        let valid_tickers = Arc::clone(&valid_tickers);
        thread::spawn(move || {
            run_broadcaster(subscribers, valid_tickers);
        });
    }

    {
        let udp_socket = Arc::clone(&udp_socket);
        let keep_alive = Arc::clone(&keep_alive);
        thread::spawn(move || {
            run_ping_handler(keep_alive, udp_socket);
        });
    }

    {
        let subscribers = Arc::clone(&subscribers);
        let keep_alive = Arc::clone(&keep_alive);
        thread::spawn(move || {
            run_watcher(subscribers, keep_alive);
        });
    }

    let address = format!("0.0.0.0:{}", args.port);
    let listener = TcpListener::bind(address)?;
    info!("[MAIN] Server is listening on port {}", args.port);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let subscribers = Arc::clone(&subscribers);
                let keep_alive = Arc::clone(&keep_alive);
                let valid_tickers = Arc::clone(&valid_tickers);
                let udp_server_socket = Arc::clone(&udp_socket);
                thread::spawn(move || {
                    let _ = handle_client(
                        stream,
                        subscribers,
                        keep_alive,
                        valid_tickers,
                        udp_server_socket,
                    );
                });
            }
            Err(e) => error!("[MAIN] Connection failed: {}", e),
        }
    }

    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    subscribers: Subscribers,
    keep_alive: KeepAlive,
    valid_tickers: Arc<HashSet<String>>,
    udp_server_socket: Arc<UdpSocket>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut request = String::new();

    reader.read_line(&mut request)?;
    match request.parse::<Message>() {
        Ok(Message::SubscribeRequest {
            udp_address,
            tickers,
        }) => {
            let unknown_tickers: Vec<String> =
                tickers.difference(&valid_tickers).cloned().collect();
            if !unknown_tickers.is_empty() {
                warn!(
                    "[SUBSCRIBE HANDLER] {} sent unknown tickers {:?}",
                    udp_address, unknown_tickers
                );
                let response = format!(
                    "{}\n",
                    Response::Error(RequestError::UnknownTicker(unknown_tickers.join(", ")))
                );
                stream.write_all(response.as_bytes())?;
                stream.flush()?;

                return Ok(());
            }

            let (tx, rx) = mpsc::channel::<StockQuote>();

            // Register subscriber
            {
                let mut subs = match subscribers.write() {
                    Ok(guard) => guard,
                    Err(_) => {
                        error!(
                            "[SUBSCRIBE HANDLER] RwLock of Subscribers is poisoned. Shutting down.."
                        );
                        process::exit(1);
                    }
                };
                subs.insert(udp_address, (tx, tickers));
            }

            // Register KeepAlive
            {
                let mut kl = match keep_alive.write() {
                    Ok(guard) => guard,
                    Err(_) => {
                        error!(
                            "[SUBSCRIBE HANDLER] RwLock of KeepAlive is poisoned. Shutting down.."
                        );
                        process::exit(1);
                    }
                };
                let now = current_timestamp_seconds();
                kl.insert(udp_address, Arc::new(AtomicU64::new(now)));
            }

            // Start client stream thread
            {
                let udp_server_socket = Arc::clone(&udp_server_socket);
                thread::spawn(move || {
                    run_client_stream(udp_address, rx, udp_server_socket);
                });
            }

            info!("[SUBSCRIBE HANDLER] {} just subscribed", udp_address);
            let response = format!("{}\n", Response::Ok);
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            let _ = udp_server_socket.send_to(b"HELLO\n", udp_address);
        }
        Ok(Message::Ping) => {
            let response = format!("{}\n", Response::Error(RequestError::InvalidCommand));
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
        }
        Err(e) => {
            let response = format!("{}\n", Response::Error(e));
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
        }
    }

    Ok(())
}

fn run_broadcaster(subscribers: Subscribers, valid_tickers: Arc<HashSet<String>>) {
    let mut rng = rand::rng();
    let mut current_prices: HashMap<String, f64> = HashMap::new();
    // Converting to Vec for fast random pick
    let ticker_list: Vec<String> = valid_tickers.iter().cloned().collect();

    info!("[BROADCASTER] Broadcaster started");
    loop {
        let random_index = rng.random_range(0..ticker_list.len());
        let ticker = &ticker_list[random_index];

        let old_price = *current_prices.entry(ticker.clone()).or_insert(1000.0);
        let change_percent = rng.random_range(-0.01..0.01);
        let mut new_price = old_price * (1.0 + change_percent);

        if new_price < 1.0 {
            new_price = 1000.0;
        }
        current_prices.insert(ticker.clone(), new_price);

        let volume = match ticker.as_str() {
            "AAPL" | "MSFT" | "TSLA" | "YNDX" | "YDEX" => rng.random_range(1000..6000),
            _ => rng.random_range(100..1100),
        };

        let quote = StockQuote {
            ticker: ticker.clone(),
            price: new_price,
            volume,
            timestamp_ms: current_timestamp_millis(),
        };

        {
            let subs = match subscribers.read() {
                Ok(guard) => guard,
                Err(_) => {
                    error!("[BROADCASTER] RwLock of Subscribers is poisoned. Shutting down..");
                    process::exit(1);
                }
            };

            for (_udp_addr, (tx, ticker_set)) in subs.iter() {
                if ticker_set.contains(&quote.ticker) {
                    let _ = tx.send(quote.clone());
                }
            }
        }

        thread::sleep(std::time::Duration::from_millis(GENERATION_INTERVAL_MS));
    }
}

fn run_ping_handler(keep_alive: KeepAlive, udp_socket: Arc<UdpSocket>) {
    let mut buf = [0u8; 64];
    info!("[PING HANDLER] UDP Ping receiver started");

    loop {
        match udp_socket.recv_from(&mut buf) {
            Ok((amount, src_addr)) => {
                if &buf[..amount] == b"PING\n" {
                    let kl = match keep_alive.read() {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!(
                                "[PING HANDLER] RwLock of KeepAlive is poisoned. Shutting down.."
                            );
                            process::exit(1);
                        }
                    };

                    if let Some(atomic_timestamp) = kl.get(&src_addr) {
                        let now = current_timestamp_seconds();
                        atomic_timestamp.store(now, Ordering::Relaxed);
                        debug!("[PING HANDLER] Updated last_seen for {}", src_addr);
                    } else {
                        warn!(
                            "[PING HANDLER] Received PING from unknown client: {}",
                            src_addr
                        );
                    }
                }
            }
            Err(e) => warn!("[PING HANDLER] Error receiving UDP packet: {}", e),
        }
    }
}

fn run_watcher(subscribers: Subscribers, keep_alive: KeepAlive) {
    info!("[WATCHER] Watcher started");

    loop {
        thread::sleep(Duration::from_secs(WATCHER_FREQUENCY_SECS));

        let now = current_timestamp_seconds();
        let mut expired_clients = Vec::new();

        // Only taking read lock to find if there are any clients to remove
        {
            let kl = match keep_alive.read() {
                Ok(guard) => guard,
                Err(_) => {
                    error!("[WATCHER] KeepAlive RwLock is poisoned. Shutting down..");
                    process::exit(1);
                }
            };

            for (addr, atomic_timestamp) in kl.iter() {
                let last_seen = atomic_timestamp.load(Ordering::Relaxed);
                if now - last_seen > CLIENT_TIMEOUT_SECS {
                    expired_clients.push(*addr);
                }
            }
        }

        if expired_clients.is_empty() {
            // back to sleep if no clients to remove
            continue;
        }

        // clean KeepAlive
        {
            let mut kl = match keep_alive.write() {
                Ok(guard) => guard,
                Err(_) => {
                    error!("[WATCHER] KeepAlive RwLock is poisoned. Shutting down..");
                    process::exit(1);
                }
            };

            kl.retain(|k, _| !expired_clients.contains(k));
        }

        // clean Subscribers
        {
            let mut subs = match subscribers.write() {
                Ok(guard) => guard,
                Err(_) => {
                    error!("[WATCHER] Subscribers RwLock is poisoned. Shutting down..");
                    process::exit(1);
                }
            };

            subs.retain(|k, _| !expired_clients.contains(k));
        }
    }
}

fn run_client_stream(
    udp_address: SocketAddr,
    rx: mpsc::Receiver<StockQuote>,
    udp_socket: Arc<UdpSocket>,
) {
    info!(
        "[UDP STREAM : {0}] Started stream for client {0}",
        udp_address
    );

    while let Ok(quote) = rx.recv() {
        let message_to_send = format!("{}\n", quote);
        let message_bytes = message_to_send.as_bytes();
        let _ = udp_socket.send_to(message_bytes, udp_address);
    }

    info!(
        "[UDP STREAM : {0}] Connection closed. Stream for client {0} is stopped",
        udp_address
    );
}

fn current_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Backwards time flow isn't supported yet")
        .as_secs()
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Backwards time flow isn't supported yet")
        .as_millis() as u64
}
