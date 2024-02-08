use std::{
    error::Error,
    fmt,
    time::{Duration, Instant},
};

use binance::rest_model::KlineSummary;
use chrono::{DateTime, Utc};
use reqwest::Response;
use serde::Deserialize;
#[derive(Debug, Deserialize)]
pub struct BinanceKline {
    #[serde(rename = "0")]
    pub open_time: i64,
    #[serde(rename = "1")]
    pub open: String,
    #[serde(rename = "2")]
    pub high: String,
    #[serde(rename = "3")]
    pub low: String,
    #[serde(rename = "4")]
    pub close: String,
    #[serde(rename = "5")]
    pub volume: String,
    #[serde(rename = "6")]
    pub close_time: i64,
    #[serde(rename = "7")]
    pub interval: String,
    // Other fields from Binance can be added here if necessary
}

impl From<KlineSummary> for BinanceKline {
    fn from(summary: KlineSummary) -> Self {
        BinanceKline {
            open_time: summary.open_time,
            open: summary.open.to_string(),
            high: summary.high.to_string(),
            low: summary.low.to_string(),
            close: summary.close.to_string(),
            volume: summary.volume.to_string(),
            close_time: summary.close_time,
            interval: String::new(), // Assuming interval is not part of KlineSummary, set to empty string or handle accordingly
        }
    }
}

#[derive(Debug)]
pub struct Gap {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceResponse {
    #[serde(rename = "t")]
    pub open_time: i64,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "T")]
    pub close_time: i64,
}

#[derive(Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<Symbol>,
}

#[derive(Deserialize)]
pub struct Symbol {
    pub symbol: String,
    pub status: String,
}

pub struct RateLimitInfo {
    pub limit: usize,
    pub remaining: usize,
    pub reset: Instant,
}

impl RateLimitInfo {
    // Function to parse rate limit info from response headers
    pub fn from_response(resp: &Response) -> Self {
        let limit = resp
            .headers()
            .get("X-MBX-USED-WEIGHT-1M")
            .and_then(|header| header.to_str().ok())
            .and_then(|header_str| header_str.parse().ok())
            .unwrap_or(1200);

        let remaining = resp
            .headers()
            .get("X-MBX-ORDER-COUNT-1M")
            .and_then(|header| header.to_str().ok())
            .and_then(|header_str| header_str.parse().ok())
            .unwrap_or(limit);

        let reset = resp
            .headers()
            .get("Retry-After")
            .and_then(|header| header.to_str().ok())
            .and_then(|header_str| header_str.parse().ok())
            .map(|secs: u64| Instant::now() + Duration::from_secs(secs))
            .unwrap_or_else(|| Instant::now());

        RateLimitInfo {
            limit,
            remaining,
            reset,
        }
    }

    // Function to determine if we need to wait for rate limit reset
    pub fn should_wait(&self) -> Option<Duration> {
        if self.remaining == 0 {
            Some(self.reset.duration_since(Instant::now()))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum FetchError {
    ReqwestError(reqwest::Error),
    StdError(binance::errors::Error),
}

impl Error for FetchError {}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::ReqwestError(err) => write!(f, "Reqwest error: {}", err),
            FetchError::StdError(err) => write!(f, "Binance error: {}", err),
        }
    }
}
