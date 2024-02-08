use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use log::info;
use sqlx::{types::BigDecimal, Pool, Postgres};

use crate::types::BinanceKline;

pub async fn insert_data(
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
        .map(|ndt| Utc.from_utc_datetime(&ndt))
        .collect();
    let close_times: Vec<NaiveDateTime> = data
        .iter()
        .map(|k| NaiveDateTime::from_timestamp_millis(k.close_time).unwrap())
        .collect();
    let close_times_utc: Vec<DateTime<Utc>> = close_times
        .iter()
        .map(|ndt| Utc.from_utc_datetime(&ndt))
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
