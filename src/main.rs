use binance::api::*;
use binance::market::*;
use dotenv::dotenv;
use reqwest::Client;

use log::{error, info};

use sqlx::{Pool, Postgres};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

mod types;

mod helpers;
use crate::api::binance::fetch_usdt_based_symbols;
use crate::helpers::data_processing::process_symbol;
use crate::helpers::logging::setup_logging;
use crate::helpers::rate_limiter::RateLimiter;

mod api;

mod database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging().expect("Failed to initialize logging.");

    dotenv().ok(); // Load .env file if exists
    info!("Loaded .env file successfully.");

    let database_url = env::var("DATABASE_URL")?;
    info!("Connecting to database...");
    let db_pool = Pool::<Postgres>::connect(&database_url).await?;
    info!("Connected to database successfully.");

    let http_client = Client::new();
    info!("HTTP client created.");

    let market: Market = Binance::new(None, None);
    info!("Binance market API created.");

    let semaphore = Arc::new(Semaphore::new(10)); // Limit concurrent tasks
    info!("Semaphore with 10 permits created.");

    let rate_limiter = Arc::new(RateLimiter::new(3000, Duration::from_secs(60)));
    info!("Rate limiter created.");

    let symbols = fetch_usdt_based_symbols(&http_client, rate_limiter.clone()).await?;
    info!("Fetched {} distinct symbols", symbols.len());

    let (tx, mut rx) = mpsc::channel(32); // Channel for handling task results
    info!("Channel for handling task results created.");

    for symbol in symbols.into_iter() {
        let tx = tx.clone();
        let db_pool = db_pool.clone();
        let semaphore = semaphore.clone();
        let market_api = market.clone();
        let rate_limiter_ = rate_limiter.clone();

        task::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap(); // Acquire semaphore permit
            info!("Processing symbol: {}", symbol);

            let result = process_symbol(&db_pool, &market_api, &symbol, rate_limiter_).await;

            match result {
                Ok(_) => {
                    info!("Successfully processed symbol: {}", symbol);
                    tx.send(Ok(symbol)).await.unwrap();
                }
                Err(e) => {
                    error!("Failed to process symbol: {}. Error: {}", symbol, e);
                    tx.send(Err((symbol, Box::<dyn std::error::Error + Send>::from(e))))
                        .await
                        .unwrap();
                }
            }
        });
    }

    drop(tx); // Close the channel
    info!("Task submission complete, channel closed.");

    // Process results
    while let Some(result) = rx.recv().await {
        match result {
            Ok(symbol) => info!("Completed processing for symbol: {}", symbol),
            Err((symbol, e)) => error!("Failed to process symbol: {}. Error: {}", symbol, e),
        }
    }

    info!("All tasks completed.");
    Ok(())
}

/* async fn fetch_binance_data(
    market: &Market,
    symbol: &str,
    gap: &Gap,
) -> Result<Vec<BinanceKline>, FetchError> {
    info!(
        "Fetching data for symbol: {} from {} to {}",
        symbol, gap.start_time, gap.end_time
    );

    let data = market
        .get_klines(
            symbol,
            "1m",
            None,
            Some((gap.start_time.timestamp() as u64) * 1000),
            Some((gap.end_time.timestamp() as u64) * 1000),
        )
        .await;

    let formatted_data = match data {
        Ok(data) => data,
        Err(err) => {
            return match err {
                binance::errors::Error::BinanceError { response } => match response.code {
                    -1000_i32 => {
                        println!("An unknown error occured while processing the request");
                        Err(FetchError::StdError(reqwest::Error()))
                    }
                    _ => {
                        println!("Unknown code {}: {}", response.code, response.msg);
                        Err(FetchError::StdError(err.into()))
                    }
                },
                _ => {
                    println!("Other errors: {}.", err);
                    Err(FetchError::StdError(err.into()))
                }
            }
        }
    };

    let formatted_data = match formatted_data {
        binance::rest_model::KlineSummaries::AllKlineSummaries(klines) => klines,
    };

    // Convert BinanceResponse into BinanceKline
    let klines: Vec<BinanceKline> = formatted_data
        .into_iter()
        .map(|kline| BinanceKline {
            open_time: kline.open_time,
            open: kline.open.to_string(),
            high: kline.high.to_string(),
            low: kline.low.to_string(),
            close: kline.close.to_string(),
            volume: kline.volume.to_string(),
            close_time: kline.close_time,
            interval: "1 minute".to_string(),
        })
        .collect();

    Ok(klines)
} */
