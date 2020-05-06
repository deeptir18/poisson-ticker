use futures_util::stream::{Stream, StreamExt};
use std::time::{Duration, Instant};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

async fn do_ticks(mut tckr: impl Stream<Item = ()> + Unpin) -> Vec<Duration> {
    let mut durs = vec![];
    for _ in 0..1000 {
        let bfr = Instant::now();
        tckr.next().await;
        durs.push(bfr.elapsed());
    }

    durs
}

#[tokio::main]
async fn main() -> Result<(), StdError> {
    let durations: Vec<_> = vec![100, 1000, 2000, 8000]
        .into_iter()
        .map(Duration::from_micros)
        .collect();

    let mut f = std::fs::File::create("./distr.data")?;
    use std::io::Write;
    write!(&mut f, "Ticker Target_us Actual_us\n")?;

    for d in durations {
        let durs = do_ticks(poisson_ticker::SpinTicker::new(d)).await;
        let sum: Duration = durs.iter().sum();
        let mean: Duration = sum / durs.len() as u32;
        println!("spin mean: {:?} vs {:?}", mean, d);
        for o in durs {
            write!(&mut f, "spin {} {}\n", d.as_micros(), o.as_micros())?;
        }

        let durs = do_ticks(poisson_ticker::TimerTicker::new(d)).await;
        let sum: Duration = durs.iter().sum();
        let mean: Duration = sum / durs.len() as u32;
        println!("timer mean: {:?} vs {:?}", mean, d);
        for o in durs {
            write!(&mut f, "timer {} {}\n", d.as_micros(), o.as_micros())?;
        }
    }

    Ok(())
}
