// src/helpers/rate_limiter.rs
use log::info;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant};

pub struct RateLimiter {
    semaphore: Arc<Semaphore>,
    period_start: Mutex<Instant>,
    remaining: AtomicUsize,
    max_requests: usize,
    period_duration: Duration,
}

impl RateLimiter {
    pub fn new(max_requests: usize, period_duration: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_requests)),
            period_start: Mutex::new(Instant::now()),
            remaining: AtomicUsize::new(max_requests),
            max_requests,
            period_duration,
        }
    }

    pub async fn acquire(&self) {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("Failed to acquire semaphore permit");

        // Decrement remaining requests atomically
        let remaining = self.remaining.fetch_sub(1, Ordering::SeqCst);

        println!("Remaining requests: {}", remaining);

        if remaining == 0 {
            let mut start = self.period_start.lock().unwrap();
            if start.elapsed() >= self.period_duration {
                // Reset the period start time and remaining requests atomically
                *start = Instant::now();
                self.remaining.store(self.max_requests, Ordering::SeqCst);
                self.semaphore.add_permits(self.max_requests); // Reset the permits
            }
        }
    }
}
