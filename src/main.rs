use binance::api::*;
use binance::futures::market;
use binance::market::*;
use binance::util::build_request;
use chrono::{Local, NaiveDateTime, TimeZone, Utc};
use dotenv::dotenv;
use reqwest::Client;

use log::{debug, error, info, warn, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::filter::threshold::ThresholdFilter;

use sqlx::types::BigDecimal;
use sqlx::{Pool, Postgres, QueryBuilder};
use std::path::Path;
use std::sync::Arc;
use std::{env, fs};
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

mod types;
use types::{BinanceKline, Gap};

use crate::types::BinanceResponse;
use chrono::DateTime;

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

    let symbols = fetch_distinct_symbols(&db_pool).await?;
    info!("Fetched {} distinct symbols", symbols.len());

    let (tx, mut rx) = mpsc::channel(32); // Channel for handling task results
    info!("Channel for handling task results created.");

    for symbol in symbols.into_iter() {
        let tx = tx.clone();
        let db_pool = db_pool.clone();
        let http_client = http_client.clone();
        let semaphore = semaphore.clone();
        let market_api = market.clone();

        task::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap(); // Acquire semaphore permit
            info!("Processing symbol: {}", symbol);

            let result = process_symbol(&db_pool, &market_api, &symbol).await;

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

fn setup_logging() -> Result<(), Box<dyn std::error::Error>> {
    let pattern = "{d(%Y-%m-%d %H:%M:%S)} [{t}] {l} - {m}\n";

    ensure_log_directory_exists("log")?;

    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build("log/output.log")?;

    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

    log4rs::init_config(config)?;

    Ok(())
}

fn ensure_log_directory_exists(log_directory: &str) -> Result<(), std::io::Error> {
    let path = Path::new(log_directory);
    if !path.exists() {
        // Create the directory and any necessary parent directories.
        fs::create_dir_all(path)?;
    }
    Ok(())
}

async fn process_symbol(
    db_pool: &Pool<Postgres>,
    market_api: &Market,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let gaps = find_gaps(db_pool, symbol)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    for gap in gaps {
        let data = fetch_binance_data(market_api, symbol, &gap)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        insert_data(db_pool, &data, symbol, "1 minute")
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    }
    Ok(())
}

async fn fetch_distinct_symbols(db_pool: &Pool<Postgres>) -> Result<Vec<String>, sqlx::Error> {
    info!("Fetching distinct symbols from database");
    sqlx::query!("SELECT symbol FROM ticker")
        .fetch_all(db_pool)
        .await
        .map(|rows| rows.into_iter().map(|row| row.symbol).collect())
}

async fn find_gaps(db_pool: &Pool<Postgres>, symbol: &str) -> Result<Vec<Gap>, sqlx::Error> {
    info!("Finding gaps for symbol: {}", symbol);
    let mut gaps = Vec::new();
    let mut last_end = 0;

    let rows = sqlx::query!(
        "SELECT opentime, closetime FROM candle WHERE (tickersymbol = $1 and interval = $2::interval) ORDER BY opentime ASC",
        symbol, &"1 minute" as &str
    )
    .fetch_all(db_pool)
    .await?;

    let one_minute = 60;

    for row in rows {
        let current_start = row.opentime.timestamp();
        if (last_end == 0) && (current_start > 0) {
            last_end = current_start;
            continue;
        }
        let prev_end = last_end;
        if current_start > prev_end + one_minute + 1 {
            gaps.push(Gap {
                start_time: Utc.timestamp_millis_opt((prev_end - 1) * 1000).unwrap(),
                end_time: Utc
                    .timestamp_millis_opt((current_start - 1) * 1000)
                    .unwrap(),
            });
            info!(
                "Gap found for symbol: {} from {} to {}",
                symbol,
                NaiveDateTime::from_timestamp_millis(prev_end * 1000).unwrap(),
                NaiveDateTime::from_timestamp_millis(current_start * 1000).unwrap(),
            );
        }
        last_end = row.closetime.timestamp();
    }

    if gaps.is_empty() {
        info!("No gaps found for symbol: {}", symbol);
    } else {
        info!("Found {} gaps for symbol: {}", gaps.len(), symbol);
    }

    Ok(gaps)
}

enum FetchError {
    ReqwestError(reqwest::Error),
    BinanceError(binance::errors::Error),
}

async fn fetch_binance_data(
    market: &Market,
    symbol: &str,
    gap: &Gap,
) -> Result<Vec<BinanceKline>, reqwest::Error> {
    info!(
        "Fetching data for symbol: {} from {} to {}",
        symbol, gap.start_time, gap.end_time
    );

    let data = match market
        .get_klines(
            symbol,
            "1m",
            None,
            Some((gap.start_time.timestamp() as u64) * 1000),
            Some((gap.end_time.timestamp() as u64) * 1000),
        )
        .await
        .map_err(|e| info!("Error: {}", e))
        .unwrap()
    {
        binance::rest_model::KlineSummaries::AllKlineSummaries(data) => data,
    };

    // Convert BinanceResponse into BinanceKline
    let klines: Vec<BinanceKline> = data
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
}

async fn insert_data(
    pool: &Pool<Postgres>,
    data: &[BinanceKline],
    ticker_symbol: &str,
    interval: &str,
) -> Result<(), sqlx::Error> {
    if data.is_empty() {
        info!("No data to insert.");
        return Ok(());
    }

    // Organize your BinanceKline data into separate vectors for each column
    let open_times: Vec<NaiveDateTime> = data
        .iter()
        .map(|k| NaiveDateTime::from_timestamp_millis(k.open_time).unwrap())
        .collect();
    let open_times_utc: Vec<DateTime<Utc>> = open_times
        .iter()
        .map(|ndt| DateTime::<Utc>::from_utc(*ndt, Utc))
        .collect();
    let close_times: Vec<NaiveDateTime> = data
        .iter()
        .map(|k| NaiveDateTime::from_timestamp_millis(k.close_time).unwrap())
        .collect();
    let close_times_utc: Vec<DateTime<Utc>> = close_times
        .iter()
        .map(|ndt| DateTime::<Utc>::from_utc(*ndt, Utc))
        .collect();
    let opens: Vec<BigDecimal> = data
        .iter()
        .map(|k| k.open.parse::<BigDecimal>().unwrap())
        .collect();
    let highs: Vec<BigDecimal> = data
        .iter()
        .map(|k| k.high.parse::<BigDecimal>().unwrap())
        .collect();
    let lows: Vec<BigDecimal> = data
        .iter()
        .map(|k| k.low.parse::<BigDecimal>().unwrap())
        .collect();
    let closes: Vec<BigDecimal> = data
        .iter()
        .map(|k| k.close.parse::<BigDecimal>().unwrap())
        .collect();
    let volumes: Vec<BigDecimal> = data
        .iter()
        .map(|k| k.volume.parse::<BigDecimal>().unwrap())
        .collect();
    let intervals: Vec<String> = vec![interval.to_string(); data.len()]; // Assuming the interval is the same for all records
    let symbols: Vec<String> = vec![ticker_symbol.to_string(); data.len()];

    // Use UNNEST to perform the bulk insert
    sqlx::query!(
    "
        INSERT INTO candle (opentime, closetime, open, high, low, close, volume, interval, tickersymbol)
        SELECT opentime, closetime, open, high, low, close, volume, interval_text::interval, tickersymbol
        FROM UNNEST(
            $1::timestamptz[], $2::timestamptz[], $3::numeric[], $4::numeric[], $5::numeric[], $6::numeric[], $7::numeric[], $8::text[], $9::text[]
        ) AS t(opentime, closetime, open, high, low, close, volume, interval_text, tickersymbol) ON CONFLICT (opentime, interval, tickersymbol) DO NOTHING
    ",
        &open_times_utc,
        &close_times_utc,
        &opens,
        &highs,
        &lows,
        &closes,
        &volumes,
        &intervals, // This should be a Vec<String> where each string is a valid interval representation
        &symbols
    )
    .execute(pool)
    .await?;

    info!(
        "Successfully inserted/updated data for symbol: {}",
        ticker_symbol
    );
    Ok(())
}
