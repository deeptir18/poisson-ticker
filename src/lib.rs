//! Exponentially distributed timer for your Poisson-arrivals needs.
pub mod requests;
use core::task::{Context, Poll};
use futures_util::stream::Stream;
use requests::RequestSchedule;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use std::time::Instant;
use tracing::trace;

/// Calls `tokio::task::yield_now()` in a loop for each tick.
///
/// # Example
/// ```rust
/// # #[tokio::main]
/// # async fn main() {
/// # use tracing_subscriber::prelude::*; use tracing::info;
/// # let subscriber = tracing_subscriber::fmt().with_test_writer()
/// #    .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE).finish().set_default();
/// let schedule = poisson_ticker::requests::RequestSchedule::new(1000, 200, poisson_ticker::requests::DistributionType::Uniform).expect("Failed to initialize schedule");
/// let mut t = poisson_ticker::SpinTicker::new(schedule);
/// let now = std::time::Instant::now();
/// # info!(?now, "start");
/// for _ in 0usize..250 {
///     (&mut t).await;
/// }
/// let el = now.elapsed();
/// # info!(?el, "end");
/// assert!(el > std::time::Duration::from_millis(40));
/// assert!(el < std::time::Duration::from_millis(60));
/// # }
/// ```
pub struct SpinTicker<T>(
    T,
    Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
);

impl<T: Timer + Unpin> Future for SpinTicker<T> {
    type Output = Option<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // if the timer is done, return None
        if self.0.done() {
            return Poll::Ready(None);
        }
        if let None = self.1 {
            self.1 = Some(Box::pin(self.0.wait()));
        }
        futures_util::ready!(self.1.as_mut().unwrap().as_mut().poll(cx));
        self.1 = None;
        Poll::Ready(Some(()))
    }
}

impl<T: Timer + Unpin> Stream for SpinTicker<T> {
    type Item = ();
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        self.poll(cx)
    }
}

impl SpinTicker<()> {
    pub fn new(r: RequestSchedule, end_time: Duration) -> SpinTicker<SpinTimer> {
        SpinTicker(SpinTimer::new(r, end_time), None)
    }

    pub fn new_with_log_id(
        r: RequestSchedule,
        end_time: Duration,
        id: usize,
    ) -> SpinTicker<SpinTimer> {
        SpinTicker(SpinTimer::new_with_log_id(r, end_time, id), None)
    }
}

pub trait Timer {
    fn wait(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
    fn done(&self) -> bool;
}

pub struct SpinTimer {
    schedule: RequestSchedule,
    deficit_ns: Arc<AtomicU64>,
    id: Option<usize>,
    cur_idx: Arc<AtomicU64>,
    start_time: Instant,
    end_time: Duration,
}

impl SpinTimer {
    fn new(request_schedule: RequestSchedule, end_time: Duration) -> Self {
        Self::new_with_log_id(request_schedule, end_time, None)
    }

    fn new_with_log_id(
        r: RequestSchedule,
        end_time: Duration,
        id: impl Into<Option<usize>>,
    ) -> Self {
        Self {
            schedule: r,
            deficit_ns: Default::default(),
            id: id.into(),
            cur_idx: Default::default(),
            start_time: Instant::now(),
            end_time: end_time,
        }
    }
}

impl Timer for SpinTimer {
    fn done(&self) -> bool {
        let cur_idx = self.cur_idx.load(Ordering::Acquire);
        cur_idx as usize > self.schedule.len() || self.start_time.elapsed() >= self.end_time
    }

    fn wait(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let start = Instant::now();
        if self.done() {
            return Box::pin(futures_util::future::ready(()));
        }

        let cur_idx = self.cur_idx.load(Ordering::Acquire);
        let next_interarrival_ns = self.schedule.get(cur_idx as _).as_nanos() as u64;
        if self.deficit_ns.load(Ordering::Acquire) > next_interarrival_ns {
            // load doesn't matter, since we don't care about the read
            let deficit = self
                .deficit_ns
                .fetch_sub(next_interarrival_ns, Ordering::Release);
            trace!(?deficit, "returning immediately from deficit");
            return Box::pin(futures_util::future::ready(()));
        }

        let next_dur = Duration::from_nanos(next_interarrival_ns);
        let next_time = start + next_dur;
        let id = self.id;
        let deficit_ns = Arc::clone(&self.deficit_ns);
        Box::pin(async move {
            while Instant::now() < next_time {
                tokio::task::yield_now().await;
            }

            let elapsed = start.elapsed();
            let elapsed_ns = elapsed.as_nanos() as u64;
            let deficit =
                deficit_ns.fetch_add(elapsed_ns - next_interarrival_ns, Ordering::Release);
            trace!(
                ?id,
                ?elapsed,
                ?deficit,
                sampled_wait_ns = ?next_interarrival_ns,
                "waited"
            );
            return ();
        })
    }
}
