use std::{
    collections::HashSet,
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    process,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use clap::Parser;
use log::{debug, error, info, warn};
use yp_streaming_quotes::{protocol, quote::StockQuote, tickers::load_tickers};

const PING_INTERVAL_SECS: u64 = 2;
const SUBSCRIBE_TIMEOUT: u64 = 5;

#[derive(Parser, Debug)]
struct Args {
    /// TCP port for the server
    #[arg(short, long, default_value = "127.0.0.1:7878")]
    server_address: String,

    #[arg(short, long, conflicts_with = "tickers_file", value_delimiter = ',', num_args = 1..)]
    tickers: Option<Vec<String>>,

    #[arg(short = 'f', long = "file", conflicts_with = "tickers")]
    tickers_file: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .parse_filters("info")
        .target(env_logger::Target::Stdout)
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    let tickers: HashSet<String> = parse_tickers(&args)?;

    let udp_socket = Arc::new(UdpSocket::bind("127.0.0.1:0")?);
    subscribe(args.server_address, udp_socket.local_addr()?, tickers)?;

    // channel to pass server upd adress for ping once.
    // I would rather send the server udp address in the TCP reply above,
    // so instead of "OK\n" I would go for something like "OK 9091\n"
    // and then start the ping channel with port known,
    // but I'll comply to the protocol provided in the task
    let (tx, rx) = mpsc::channel::<SocketAddr>();
    {
        let udp_socket = Arc::clone(&udp_socket);
        thread::spawn(move || {
            keep_alive(udp_socket, rx);
        });
    }
    receive_handshake(&udp_socket, tx)?;

    let mut buf = [0u8; 256];
    loop {
        match udp_socket.recv_from(&mut buf) {
            Ok((amount, _)) => process_message(&buf[..amount]),
            Err(e) => error!("[MAIN] Error while receiving UDP-packet: {}", e),
        }
    }
}

fn subscribe(
    server_address: String,
    local_adress: SocketAddr,
    tickers: HashSet<String>,
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(server_address)?;
    stream.set_read_timeout(Some(Duration::from_secs(SUBSCRIBE_TIMEOUT)))?;

    let subscribe_request = protocol::Message::SubscribeRequest {
        udp_address: local_adress,
        tickers,
    };

    let message = format!("{}\n", subscribe_request);

    stream.write_all(message.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response)?;

    let trimmed = response.trim();

    if trimmed == "OK" {
        info!("[SUBSCRIBE] Successfully subscribed");
        return Ok(());
    }

    if let Some(error_message) = trimmed.strip_prefix("ERR ") {
        error!("[SUBSCRIBE] Failed to subscribe: {}", error_message);
        return Err("Failed to subscribe".into());
    }

    error!("[SUBSCRIBE] Received an unknown response: {}", trimmed);
    Err("Failed to subscribe".into())
}

fn keep_alive(udp_socket: Arc<UdpSocket>, rx: mpsc::Receiver<SocketAddr>) {
    info!("[PING] Initializing PING thread..");
    let server_addr = match rx.recv() {
        Ok(addr) => addr,
        Err(_) => {
            error!("[PING] Failed to initialize PING thread");
            process::exit(1);
        }
    };

    // Release rx here to free the channel.
    drop(rx);

    loop {
        match udp_socket.send_to(b"PING\n", server_addr) {
            Ok(_) => (),
            Err(_) => warn!("[PING] failed to send a PING to the server"),
        };
        thread::sleep(Duration::from_secs(PING_INTERVAL_SECS));
    }
}

fn process_message(buf: &[u8]) {
    let received_message = String::from_utf8_lossy(buf);
    let received_message = received_message.trim();

    // ignore the handshake packet that exists to recive the server udp addrs for the ping
    // as mentioned above, I would rather have that handeled in TCP "OK\n",
    // but oh well, won't change the protocol described in the task
    if received_message == "HELLO" {
        debug!("[MAIN] Received HELLO handshake from server");
        return;
    }

    match received_message.parse::<StockQuote>() {
        Ok(quote) => info!("{}", quote),
        Err(e) => error!("[MAIN] Failed to parse quote: {}", e),
    }
}

fn receive_handshake(
    udp_socket: &UdpSocket,
    tx: mpsc::Sender<SocketAddr>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = [0u8; 256];

    let (amount, src_addr) = udp_socket.recv_from(&mut buf).map_err(|e| {
        error!("[MAIN] Failed to receive initial UDP-packet: {}.", e);
        e
    })?;

    tx.send(src_addr)?;
    process_message(&buf[..amount]);

    Ok(())
}

fn parse_tickers(args: &Args) -> Result<HashSet<String>, Box<dyn Error>> {
    if let Some(file_path) = &args.tickers_file {
        info!("[MAIN] Loading tickers from file: {}", file_path);
        let file = File::open(file_path)?;
        match load_tickers(file) {
            Ok(t) => Ok(t),
            Err(e) => {
                error!("[MAIN] Failed to load tickers from file: {}", e);
                process::exit(1);
            }
        }
    } else if let Some(tickers_vec) = &args.tickers {
        Ok(tickers_vec
            .iter()
            .map(|s| s.trim().to_uppercase())
            .collect())
    } else {
        info!("[MAIN] No tickers specified. Using default set.");
        Ok(["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "YNDX", "YDEX"]
            .into_iter()
            .map(String::from)
            .collect())
    }
}
