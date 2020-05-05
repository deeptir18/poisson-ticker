//! Exponentially distributed timer for your Poisson-arrivals needs.
//!
//! # Example
//! ```rust
//! #[tokio::main]
//! async fn main() {
//!     let mut t = poisson_ticker::Ticker::new(std::time::Duration::from_millis(10));
//!     let now = std::time::Instant::now();
//!     for _ in 0usize..5 {
//!         (&mut t).await;
//!     }
//!     println!("elapsed: {:?}", now.elapsed());
//! }
//! ```

use async_timer::oneshot::{Oneshot, Timer};
use core::task::{Context, Poll};
use futures_util::stream::Stream;
use rand_distr::{Distribution, Exp};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub struct Ticker {
    should_restart: bool,
    curr_timer: Timer,
    distr: Exp<f64>,
    mean_ns: u64,
    id: Option<usize>,
}

impl Ticker {
    pub fn new(d: Duration) -> Self {
        let mean_ns = d.as_nanos() as _;
        let lambda = 1. / d.as_nanos() as f64;
        let r = Exp::new(lambda).expect("Make exponential distr");
        Self {
            should_restart: true,
            curr_timer: Timer::new(d),
            distr: r,
            mean_ns,
            id: None,
        }
    }

    pub fn new_with_log_id(d: Duration, id: usize) -> Self {
        let mean_ns = d.as_nanos() as _;
        let lambda = 1. / d.as_nanos() as f64;
        let r = Exp::new(lambda).expect("Make exponential distr");
        Self {
            should_restart: true,
            curr_timer: Timer::new(d),
            distr: r,
            mean_ns,
            id: Some(id),
        }
    }
}

impl Future for Ticker {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        let t = &mut this.curr_timer;
        if this.should_restart {
            let mut rng = rand::thread_rng();
            let next_interarrival_ns = this.distr.sample(&mut rng) as u64;
            if next_interarrival_ns > this.mean_ns * 10 || next_interarrival_ns == 0 {
                tracing::warn!(
                    id = ?this.id,
                    sampled_wait_ns = ?next_interarrival_ns,
                    mean_ns = ?this.mean_ns,
                    "suspicious wait"
                );
            }

            if next_interarrival_ns < 100 {
                tracing::debug!(
                    id = ?this.id,
                    sampled_wait_ns = ?next_interarrival_ns,
                    mean_ns = ?this.mean_ns,
                    "wait too short, return immediately"
                );

                this.should_restart = true;
                return Poll::Ready(());
            }

            t.restart(
                Duration::from_nanos(next_interarrival_ns as u64),
                cx.waker(),
            );

            this.should_restart = false;
        }

        tracing::trace!(
            id = ?this.id,
            mean_ns = ?this.mean_ns,
            "polling pacing timer"
        );

        let tp = Pin::new(t);
        match tp.poll(cx) {
            x @ Poll::Pending => x,
            x @ Poll::Ready(_) => {
                this.should_restart = true;
                x
            }
        }
    }
}

impl Stream for Ticker {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll(cx).map(Some)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn reuse() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut t = super::Ticker::new(std::time::Duration::from_millis(10));
            let now = std::time::Instant::now();
            for _ in 0usize..5 {
                (&mut t).await;
            }
            println!("elapsed: {:?}", now.elapsed());
        });
    }
}
