# UDP Streaming Quotes System

Market data distribution system. It utilizes a hybrid network architecture: **TCP** for connection handshake/subscription management and **UDP** for low-latency quote streaming and keep-alive heartbeats.

---

## 📑 Protocol Specification & Format Rules

### 1. Data Format (UDP Datagram)
The server streams market updates as plain text strings. **Each stock quote is strictly transmitted within a single UDP datagram.**
*   **Format Layout:** `ticker|price|volume|timestamp_ms`
*   **Rule:** No whitespace are allowed inside any data field (they will be ignored).
*   **Example Packet:** `AAPL|150.5|1000|1718123654876`

### 2. TCP Subscription Lifecycle
*   The client opens a TCP connection to the server.
*   The client sends a single request line: `STREAM <udp_addr> <ticker1>,<ticker2>...\n`
*   The server validates tickers and responds with a single line: `OK\n` or `ERR <reason>\n`.
*   **The TCP connection is immediately closed after the response.** All subsequent interaction occurs strictly over UDP.

### 3. Buffers & Keep-Alive Logic
*   **UDP Receive Buffer:** The client utilizes **256-byte** stack buffer (sufficient for single quote frame without overhead).
*   **Heartbeats:** The client transmits a UDP `PING\n` packet every **2 seconds**. If the server detects no PINGs from a subscriber for **5 seconds**, a background watcher thread automatically drops the subscription and terminates the client's individual streaming thread.

---

## 🚀 Manual Execution & Verification Scenario

Follow these steps to run and verify the system in your terminal environment.

### Step 1: Start the Server (Terminal A)
The server loads the directory of valid tickers and begins listening for TCP handshakes.
```bash
cargo run --bin server -- --port 7878 --tickers-file assets/tickers.txt
```

### Step 2: Run the First Client (Terminal B)
Launch a client with a manual list of filtered tickers using the `-t` flag. The client automatically binds to an ephemeral local UDP port.
```bash
cargo run --bin client -- --server-address 127.0.0.1:7878 -t AAPL,MSFT,TSLA
```
*   **Expected Behavior:** Real-time stock updates start streaming into the console, filtered **strictly** by the requested tickers (AAPL, MSFT, TSLA).

### Step 3: Run an Independent Second Client (Terminal C)
Launch another client concurrently, feeding its filter requirements from a configuration file via the `-f` / `--file` flag.
```bash
cargo run --bin client -- --server-address 127.0.0.1:7878 --file assets/tickers.txt
```
*   **Expected Behavior:** Both clients process data concurrently and entirely independently. Each receives only its respective subset of market updates.

### Step 4: Test Connection Timeout & Cleanup
Terminate the first client in Terminal B (`Ctrl + C`).
*   **Expected Behavior:** The client stops broadcasting PINGs. Within **5–6 seconds**, the server's watcher thread detects the timeout, unregisters the subscriber, and gracefully shuts down its dedicated UDP streaming thread. The second client (Terminal C) continues streaming uninterrupted.

---

## 🧪 Tests
Run the test suite validating quote serialization, contract protocols, and reading tickers from <impl Read>:
```bash
cargo test
```
