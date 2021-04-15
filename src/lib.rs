//! Exponentially distributed timer for your Poisson-arrivals needs.

use core::task::{Context, Poll};
use futures_util::stream::Stream;
use rand_distr::{Distribution, Exp};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

/// Calls `tokio::task::yield_now()` in a loop for each tick.
///
/// # Example
/// ```rust
/// # #[tokio::main]
/// # async fn main() {
/// let mut t = poisson_ticker::SpinTicker::new(std::time::Duration::from_millis(10));
/// let now = std::time::Instant::now();
/// for _ in 0usize..5 {
///     (&mut t).await;
/// }
/// assert!(now.elapsed() > std::time::Duration::from_millis(50));
/// # }
/// ```
pub struct SpinTicker(
    SpinTimer,
    Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
);

impl Future for SpinTicker {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let None = self.1 {
            self.1 = Some(Box::pin(self.0.wait()));
        }

        futures_util::ready!(self.1.as_mut().unwrap().as_mut().poll(cx));
        self.1 = None;
        Poll::Ready(())
    }
}

impl Stream for SpinTicker {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll(cx).map(Some)
    }
}

impl SpinTicker {
    pub fn new(d: Duration) -> Self {
        Self(SpinTimer::new(d), None)
    }

    pub fn new_with_log_id(d: Duration, id: usize) -> Self {
        Self(SpinTimer::new_with_log_id(d, id), None)
    }
}

struct SpinTimer {
    distr: Exp<f64>,
    mean_ns: u64,
    id: Option<usize>,
}

impl SpinTimer {
    fn new(d: Duration) -> Self {
        let mean_ns = d.as_nanos() as _;
        let lambda = 1. / d.as_nanos() as f64;
        let r = Exp::new(lambda).expect("Make exponential distr");
        Self {
            distr: r,
            mean_ns,
            id: None,
        }
    }

    fn new_with_log_id(d: Duration, id: usize) -> Self {
        let mean_ns = d.as_nanos() as _;
        let lambda = 1. / d.as_nanos() as f64;
        let r = Exp::new(lambda).expect("Make exponential distr");
        Self {
            distr: r,
            mean_ns,
            id: Some(id),
        }
    }

    fn wait(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let mut rng = rand::thread_rng();
        let next_interarrival_ns = self.distr.sample(&mut rng) as u64;
        if next_interarrival_ns < 100 {
            tracing::trace!(
                id = ?self.id,
                sampled_wait_ns = ?next_interarrival_ns,
                mean_ns = ?self.mean_ns,
                "wait too short"
            );

            // have a min wait or return immediately?
            //next_interarrival_ns = 100;
            return Box::pin(futures_util::future::ready(()));
        }

        let next_dur = Duration::from_nanos(next_interarrival_ns);
        let start = Instant::now();
        let next_time = start + next_dur;
        let id = self.id;
        Box::pin(async move {
            while Instant::now() < next_time {
                tokio::task::yield_now().await;
            }

            let elapsed = start.elapsed();
            if elapsed.as_nanos() > next_interarrival_ns as u128 * 10 {
                tracing::trace!(
                    ?id,
                    ?elapsed,
                    sampled_wait_ns = ?next_interarrival_ns,
                    "suspicious wait"
                );
            }
        })
    }
}
