use binance::market::Market;
use reqwest::Client;
use std::future::Future;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::helpers::rate_limiter::RateLimiter;
use crate::types::{self, BinanceKline, ExchangeInfo, FetchError, Gap, RateLimitInfo};

const MAX_RETRIES: u32 = 5; // Maximum number of retries

async fn retry_with_backoff<F, Fut, T, E>(mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempts = 0;
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= MAX_RETRIES {
                    return Err(e);
                }
                let wait_time = 2u64.pow(attempts) * 100; // Exponential backoff
                sleep(Duration::from_millis(wait_time)).await;
            }
        }
    }
}

pub async fn fetch_binance_data_with_rate_limiting(
    market: &Market,
    symbol: &str,
    gap: &Gap,
    rate_limiter: Arc<RateLimiter>,
) -> Result<Vec<BinanceKline>, FetchError> {
    rate_limiter.acquire().await;

    let fetch_data = || async {
        let data = market
            .get_klines(
                symbol,
                "1m",
                None,
                Some(gap.start_time.timestamp_millis() as u64),
                Some(gap.end_time.timestamp_millis() as u64),
            )
            .await;

        match data {
            Ok(klines) => match klines {
                binance::rest_model::KlineSummaries::AllKlineSummaries(data) => {
                    let result: Vec<BinanceKline> =
                        data.into_iter().map(|k| BinanceKline::from(k)).collect();
                    Ok(result)
                }
            },
            Err(e) => {
                // Handle specific Binance errors here, for simplicity we'll retry on any error
                Err(FetchError::from(types::FetchError::StdError(e)))
            }
        }
    };

    retry_with_backoff(fetch_data).await
}

pub async fn fetch_usdt_based_symbols(
    client: &Client,
    rate_limiter: Arc<RateLimiter>,
) -> Result<Vec<String>, reqwest::Error> {
    let url = "https://api.binance.com/api/v3/exchangeInfo";

    rate_limiter.acquire().await;

    let response = client.get(url).send().await?;

    let rate_limit_info = RateLimitInfo::from_response(&response);

    if let Some(wait_time) = rate_limit_info.should_wait() {
        sleep(wait_time).await;
    }

    let exchange_info: ExchangeInfo = response.json().await?;

    let symbols = exchange_info
        .symbols
        .into_iter()
        .filter(|s| s.symbol.ends_with("USDT") && s.status == "TRADING")
        .map(|s| s.symbol)
        .collect();

    Ok(symbols)
}
