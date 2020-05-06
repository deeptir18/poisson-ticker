//! Exponentially distributed timer for your Poisson-arrivals needs.

use async_timer::oneshot::{Oneshot, Timer};
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
/// println!("elapsed: {:?}", now.elapsed());
/// # }
/// ```
pub struct SpinTicker(SpinTimer, Option<Pin<Box<dyn Future<Output = ()>>>>);

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

    fn wait(&mut self) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        let mut rng = rand::thread_rng();
        let mut next_interarrival_ns = self.distr.sample(&mut rng) as u64;
        if next_interarrival_ns > self.mean_ns * 10 {
            tracing::warn!(
                id = ?self.id,
                sampled_wait_ns = ?next_interarrival_ns,
                mean_ns = ?self.mean_ns,
                "suspicious wait"
            );
        }

        if next_interarrival_ns < 100 {
            tracing::debug!(
                id = ?self.id,
                sampled_wait_ns = ?next_interarrival_ns,
                mean_ns = ?self.mean_ns,
                "wait too short"
            );

            next_interarrival_ns = 100;
        }

        let next_dur = Duration::from_nanos(next_interarrival_ns);
        let next_time = Instant::now() + next_dur;
        Box::pin(async move {
            while Instant::now() < next_time {
                tokio::task::yield_now().await;
            }
        })
    }
}

/// Naive Ticker which calls a timer, no adjustment.
///
/// # Example
/// ```rust
/// # #[tokio::main]
/// # async fn main() {
/// let mut t = poisson_ticker::TimerTicker::new(std::time::Duration::from_millis(10));
/// let now = std::time::Instant::now();
/// for _ in 0usize..5 {
///     (&mut t).await;
/// }
/// println!("elapsed: {:?}", now.elapsed());
/// # }
/// ```
pub struct TimerTicker {
    should_restart: bool,
    curr_timer: Timer,
    distr: Exp<f64>,
    mean_ns: u64,
    id: Option<usize>,
}

impl TimerTicker {
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

impl Future for TimerTicker {
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

impl Stream for TimerTicker {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll(cx).map(Some)
    }
}
