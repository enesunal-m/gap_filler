use chrono::naive::NaiveDateTime;
use chrono::{DateTime, TimeZone, Utc};
use dotenv::dotenv;
use reqwest::Client;
use sqlx::types::BigDecimal;
use sqlx::{Pool, Postgres};
use std::env;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

mod types;
use types::{BinanceKline, Gap};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok(); // Load .env file if exists
    println!("Loaded .env file successfully.");

    let database_url = env::var("DATABASE_URL")?;
    println!("Connecting to database...");
    let db_pool = Pool::<Postgres>::connect(&database_url).await?;
    println!("Connected to database successfully.");

    let http_client = Client::new();
    println!("HTTP client created.");

    let semaphore = Arc::new(Semaphore::new(10)); // Limit concurrent tasks
    println!("Semaphore with 10 permits created.");

    let symbols = fetch_distinct_symbols(&db_pool).await?;
    println!("Fetched {} distinct symbols", symbols.len());

    let (tx, mut rx) = mpsc::channel(32); // Channel for handling task results
    println!("Channel for handling task results created.");

    for symbol in symbols.into_iter() {
        let tx = tx.clone();
        let db_pool = db_pool.clone();
        let http_client = http_client.clone();
        let semaphore = semaphore.clone();

        task::spawn(async move {
            println!("Processing symbol: {}", symbol);
            let _permit = semaphore.acquire().await.unwrap(); // Acquire semaphore permit
            println!("Semaphore permit acquired for symbol: {}", symbol);

            let result = process_symbol(&db_pool, &http_client, &symbol).await;

            match result {
                Ok(_) => {
                    println!("Successfully processed symbol: {}", symbol);
                    tx.send(Ok(symbol)).await.unwrap();
                }
                Err(e) => {
                    eprintln!("Failed to process symbol: {}. Error: {}", symbol, e);
                    tx.send(Err((symbol, Box::<dyn std::error::Error + Send>::from(e))))
                        .await
                        .unwrap();
                }
            }
        });
    }

    drop(tx); // Close the channel
    println!("Task submission complete, channel closed.");

    // Process results
    while let Some(result) = rx.recv().await {
        match result {
            Ok(symbol) => println!("Completed processing for symbol: {}", symbol),
            Err((symbol, e)) => eprintln!("Failed to process symbol: {}. Error: {}", symbol, e),
        }
    }

    println!("All tasks completed.");
    Ok(())
}

async fn process_symbol(
    db_pool: &Pool<Postgres>,
    http_client: &Client,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let gaps = find_gaps(db_pool, symbol)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    for gap in gaps {
        let data = fetch_binance_data(http_client, symbol, &gap)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        insert_data(db_pool, &data, symbol, "1m")
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    }
    Ok(())
}

async fn fetch_distinct_symbols(db_pool: &Pool<Postgres>) -> Result<Vec<String>, sqlx::Error> {
    println!("Fetching distinct symbols from database");
    sqlx::query!("SELECT symbol FROM ticker")
        .fetch_all(db_pool)
        .await
        .map(|rows| rows.into_iter().map(|row| row.symbol).collect())
}

async fn find_gaps(db_pool: &Pool<Postgres>, symbol: &str) -> Result<Vec<Gap>, sqlx::Error> {
    println!("Finding gaps for symbol: {}", symbol);
    let mut gaps = Vec::new();
    let mut last_end: Option<DateTime<Utc>> = None;

    let rows = sqlx::query!(
        "SELECT opentime, closetime FROM candle WHERE tickersymbol = $1 ORDER BY opentime ASC",
        symbol
    )
    .fetch_all(db_pool)
    .await?;

    for row in rows {
        let current_start = Utc.timestamp_millis_opt(row.opentime.timestamp()).unwrap();
        if let Some(prev_end) = last_end {
            if current_start > prev_end + chrono::Duration::minutes(1) {
                gaps.push(Gap {
                    start_time: prev_end + chrono::Duration::minutes(1),
                    end_time: current_start - chrono::Duration::minutes(1),
                });
                println!(
                    "Gap found for symbol: {} from {} to {}",
                    symbol, prev_end, current_start
                );
            }
        }
        last_end = Some(Utc.timestamp_millis_opt(row.closetime.timestamp()).unwrap());
    }

    if gaps.is_empty() {
        println!("No gaps found for symbol: {}", symbol);
    } else {
        println!("Found {} gaps for symbol: {}", gaps.len(), symbol);
    }

    Ok(gaps)
}

async fn fetch_binance_data(
    client: &Client,
    symbol: &str,
    gap: &Gap,
) -> Result<Vec<BinanceKline>, reqwest::Error> {
    println!(
        "Fetching Binance data for symbol: {} for gap from {} to {}",
        symbol, gap.start_time, gap.end_time
    );
    let response = client
        .get("https://api.binance.com/api/v3/klines")
        .query(&[
            ("symbol", symbol),
            ("interval", "1m"),
            (
                "startTime",
                &(gap.start_time.timestamp_millis().to_string()),
            ),
            ("endTime", &(gap.end_time.timestamp_millis().to_string())),
        ])
        .send()
        .await?
        .json::<Vec<Vec<String>>>()
        .await?;

    println!(
        "Fetched {} klines for symbol: {} for the specified gap",
        response.len(),
        symbol
    );

    // Convert the response to Vec<BinanceKline>
    let data: Vec<BinanceKline> = response
        .into_iter()
        .map(|kline| BinanceKline {
            open_time: kline[0].parse().unwrap(),
            open: kline[1].clone(),
            high: kline[2].clone(),
            low: kline[3].clone(),
            close: kline[4].clone(),
            volume: kline[5].clone(),
            close_time: kline[6].parse().unwrap(),
            interval: "1m".to_string(),
        })
        .collect();

    Ok(data)
}

async fn insert_data(
    db_pool: &Pool<Postgres>,
    data: &[BinanceKline],
    ticker_symbol: &str,
    interval: &str,
) -> Result<(), sqlx::Error> {
    println!(
        "Inserting data for symbol: {} with {} entries",
        ticker_symbol,
        data.len()
    );
    for kline in data {
        let result = sqlx::query!(
            "INSERT INTO candle (opentime, closetime, open, high, low, close, volume, interval, tickersymbol) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (opentime, interval, tickersymbol) DO NOTHING",
            NaiveDateTime::from_timestamp_millis(kline.open_time),
            NaiveDateTime::from_timestamp_millis(kline.close_time),
            kline.open.parse::<BigDecimal>().unwrap(),
            kline.high.parse::<BigDecimal>().unwrap(),
            kline.low.parse::<BigDecimal>().unwrap(),
            kline.close.parse::<BigDecimal>().unwrap(),
            kline.volume.parse::<BigDecimal>().unwrap(),
            &interval as &str,
            ticker_symbol
        )
        .execute(db_pool)
        .await;

        match result {
            Ok(_) => println!(
                "Successfully inserted/updated candle for symbol: {}",
                ticker_symbol
            ),
            Err(e) => eprintln!(
                "Failed to insert/update candle for symbol: {}. Error: {}",
                ticker_symbol, e
            ),
        }
    }

    println!(
        "Data insertion/update complete for symbol: {}",
        ticker_symbol
    );
    Ok(())
}
