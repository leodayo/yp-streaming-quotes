use std::{collections::HashSet, fmt, net::SocketAddr, str::FromStr};

use crate::error::RequestError;

pub enum Message {
    SubscribeRequest {
        udp_address: SocketAddr,
        tickers: HashSet<String>,
    },
    Ping,
}

pub enum Response {
    Ok,
    Error(RequestError),
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::SubscribeRequest {
                udp_address,
                tickers,
            } => {
                let tickers: Vec<&str> = tickers.iter().map(|s| s.as_str()).collect();
                write!(f, "STREAM {} {}", udp_address, tickers.join(", "))
            }
            Message::Ping => write!(f, "PING"),
        }
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Ok => write!(f, "OK"),
            Response::Error(err) => write!(f, "ERR {}", err),
        }
    }
}

impl FromStr for Message {
    type Err = RequestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();

        if trimmed == "PING" {
            return Ok(Message::Ping);
        }

        if !trimmed.starts_with("STREAM ") {
            return Err(RequestError::InvalidCommand);
        }

        let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();

        if parts.len() < 2 {
            return Err(RequestError::InvalidCommand);
        }

        let udp_address: SocketAddr = parts[1]
            .parse()
            .map_err(|_| RequestError::InvalidUdpAddress)?;

        if parts.len() == 2 {
            return Err(RequestError::EmptyTickerList);
        }

        let tickers: HashSet<String> = parts[2]
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect();

        if tickers.is_empty() {
            return Err(RequestError::EmptyTickerList);
        }

        Ok(Message::SubscribeRequest {
            udp_address,
            tickers,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_subscribe_request() -> Result<(), Box<dyn std::error::Error>> {
        let raw_stream = "STREAM 127.0.0.1:5001 AAPL, MSFT, TSLA";
        let expected_addr = "127.0.0.1:5001".parse::<SocketAddr>()?;

        let parsed = Message::from_str(raw_stream)?;

        let Message::SubscribeRequest {
            udp_address,
            tickers,
        } = parsed
        else {
            panic!("Expected Message::SubscribeRequest, но получили другой вариант");
        };

        assert_eq!(udp_address, expected_addr);
        assert_eq!(tickers.len(), 3);
        assert!(tickers.contains("AAPL"));
        assert!(tickers.contains("MSFT"));
        assert!(tickers.contains("TSLA"));

        Ok(())
    }

    #[test]
    fn test_parse_ping() {
        assert!(matches!(Message::from_str("PING"), Ok(Message::Ping)));
    }

    #[test]
    fn test_message_parse_errors() {
        assert!(matches!(
            Message::from_str("INVALID_CMD 127.0.0.1:5001 AAPL"),
            Err(RequestError::InvalidCommand)
        ));

        assert!(matches!(
            Message::from_str("STREAM 999.999.999.999:5001 AAPL"),
            Err(RequestError::InvalidUdpAddress)
        ));

        assert!(matches!(
            Message::from_str("STREAM 127.0.0.1:5001"),
            Err(RequestError::EmptyTickerList)
        ));

        assert!(matches!(
            Message::from_str("STREAM 127.0.0.1:5001 , , "),
            Err(RequestError::EmptyTickerList)
        ));
    }
}
