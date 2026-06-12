use std::{fmt, str};

use crate::error::QuoteParseError;

#[derive(Debug, PartialEq, Clone)]
pub struct StockQuote {
    pub ticker: String,
    // questionable choice in my opinion,
    // but the protocol spec in the task specifically requested this type.
    // Otherwise I would just use two uints or signed ints depending on the underlying logic.
    pub price: f64,
    pub volume: u32,
    pub timestamp_ms: u64,
}

impl fmt::Display for StockQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp_ms
        )
    }
}

impl str::FromStr for StockQuote {
    type Err = QuoteParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = s.split('|').map(|s| s.trim()).collect();
        if fields.len() != 4 {
            return Err(QuoteParseError::InvalidFormat);
        }

        if fields[0].is_empty() {
            return Err(QuoteParseError::InvalidField {
                field: "ticker",
                reason: "the field can't be empty".to_string(),
            });
        }

        Ok(StockQuote {
            ticker: fields[0].to_string(),
            price: fields[1]
                .parse()
                .map_err(|_| QuoteParseError::InvalidField {
                    field: "price",
                    reason: "the field must be a valid floating point number".to_string(),
                })?,
            volume: fields[2]
                .parse()
                .map_err(|_| QuoteParseError::InvalidField {
                    field: "volume",
                    reason: "the field must be a valid unsigned integer".to_string(),
                })?,
            timestamp_ms: fields[3]
                .parse()
                .map_err(|_| QuoteParseError::InvalidField {
                    field: "timestamp_ms",
                    reason: "the field must be a valid unsigned integer".to_string(),
                })?,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_quote_round_trip() {
        let original = StockQuote {
            ticker: "AAPL".to_string(),
            price: 150.50,
            volume: 1000,
            timestamp_ms: 1718123654876,
        };

        let serialized = original.to_string();
        assert_eq!(serialized, "AAPL|150.5|1000|1718123654876");

        let deserialized = serialized.parse::<StockQuote>();
        assert!(deserialized.is_ok());
        assert_eq!(deserialized.unwrap(), original);
    }

    #[test]
    fn test_quote_parse_errors() {
        let bad_format = "AAPL|150.5|1000";
        assert!(matches!(
            bad_format.parse::<StockQuote>(),
            Err(QuoteParseError::InvalidFormat)
        ));

        let empty_ticker = "|150.5|1000|1718123654876";
        assert!(matches!(
            empty_ticker.parse::<StockQuote>(),
            Err(QuoteParseError::InvalidField {
                field: "ticker",
                ..
            })
        ));

        let bad_price = "AAPL|abc|1000|1718123654876";
        assert!(matches!(
            bad_price.parse::<StockQuote>(),
            Err(QuoteParseError::InvalidField { field: "price", .. })
        ));
    }
}
