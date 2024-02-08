use std::{error::Error, fmt};

use chrono::{DateTime, Utc};
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
