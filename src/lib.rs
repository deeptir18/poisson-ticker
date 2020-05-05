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
    mean_ns: u128,
}

impl Ticker {
    pub fn new(d: Duration) -> Self {
        let mean_ns = d.as_nanos();
        let lambda = 1. / d.as_nanos() as f64;
        let r = Exp::new(lambda).expect("Make exponential distr");
        Self {
            should_restart: true,
            curr_timer: Timer::new(d),
            distr: r,
            mean_ns,
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
            let next_interarrival_ns = this.distr.sample(&mut rng);
            if next_interarrival_ns as u128 > this.mean_ns * 10 {
                tracing::warn!(
                    sampled_wait_ns = ?next_interarrival_ns,
                    mean_ns = ?this.mean_ns,
                    "long wait"
                );
            }

            t.restart(
                Duration::from_nanos(next_interarrival_ns as u64),
                cx.waker(),
            );

            this.should_restart = false;
        }

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
