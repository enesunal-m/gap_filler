use std::sync::Arc;

use binance::market::Market;
use chrono::{NaiveDateTime, TimeZone, Utc};
use log::info;
use sqlx::{Pool, Postgres};

use crate::{
    api::binance::fetch_binance_data_with_rate_limiting, database::insert_data, types::Gap,
};

use super::rate_limiter::RateLimiter;

pub async fn process_symbol(
    db_pool: &Pool<Postgres>,
    market_api: &Market,
    symbol: &str,
    rate_limiter: Arc<RateLimiter>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let gaps = find_gaps(db_pool, symbol)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    for gap in gaps {
        let data =
            fetch_binance_data_with_rate_limiting(market_api, symbol, &gap, rate_limiter.clone())
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        insert_data(db_pool, &data, symbol, "1 minute")
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
    }
    Ok(())
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
