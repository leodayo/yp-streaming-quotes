use std::{error, fmt};

#[derive(Debug, PartialEq)]
pub enum QuoteParseError {
    InvalidFormat,
    InvalidField { field: &'static str, reason: String },
}

impl error::Error for QuoteParseError {}

impl fmt::Display for QuoteParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuoteParseError::InvalidFormat => write!(
                f,
                "Invalid format. Expected: \"ticker|price|volume|timestamp_ms\""
            ),
            QuoteParseError::InvalidField { field, reason } => {
                write!(f, "Invalid field '{}': {}", field, reason)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RequestError {
    InvalidCommand,
    InvalidUdpAddress,
    EmptyTickerList,
    UnknownTicker(String),
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::InvalidCommand => write!(f, "invalid command"),
            RequestError::InvalidUdpAddress => write!(f, "invalid udp address"),
            RequestError::EmptyTickerList => write!(f, "empty ticker list"),
            RequestError::UnknownTicker(ticker) => write!(f, "unknown ticker: {}", ticker),
        }
    }
}

impl error::Error for RequestError {}
