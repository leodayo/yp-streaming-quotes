use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Read},
};

pub fn load_tickers(reader: impl Read) -> std::io::Result<HashSet<String>> {
    let reader = BufReader::new(reader);
    let mut tickers = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        let cleaned = line.trim().to_uppercase();
        if !cleaned.is_empty() {
            tickers.insert(cleaned);
        }
    }

    Ok(tickers)
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_load_tickers() {
        let data = "  aapl  \nMSFT\n\ntsla\n";
        let cursor = Cursor::new(data);

        let tickers = load_tickers(cursor).unwrap();
        assert_eq!(tickers.len(), 3);

        assert!(tickers.contains("AAPL"));
        assert!(tickers.contains("MSFT"));
        assert!(tickers.contains("TSLA"));
    }
}
