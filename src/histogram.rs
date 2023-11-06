use color_eyre::eyre::{bail, ensure, Result};
use std::fs::File;
use std::io::Write;
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct ManualHistogram {
    current_count: usize,
    latencies: Vec<u64>,
    sorted_latencies: Vec<u64>,
    is_sorted: bool,
}

impl ManualHistogram {
    pub fn new_from_vec(latencies: Vec<u64>) -> Self {
        ManualHistogram {
            current_count: latencies.len(),
            latencies: latencies,
            sorted_latencies: Vec::default(),
            is_sorted: false,
        }
    }
    pub fn new(num_values: usize) -> Self {
        ManualHistogram {
            current_count: 0,
            latencies: vec![0u64; num_values as usize],
            sorted_latencies: Vec::default(),
            is_sorted: false,
        }
    }

    pub fn len(&self) -> usize {
        self.current_count
    }

    pub fn init(rate_pps: u64, total_time_sec: u64) -> Self {
        // ten percent over
        let num_values = ((rate_pps * total_time_sec) as f64 * 1.10) as usize;
        ManualHistogram {
            current_count: 0,
            latencies: vec![0u64; num_values],
            sorted_latencies: Vec::default(),
            is_sorted: false,
        }
    }

    pub fn is_sorted(&self) -> bool {
        self.is_sorted
    }

    pub fn record(&mut self, val: u64) {
        if self.current_count == self.latencies.len() {
            self.latencies.push(val);
        } else {
            self.latencies[self.current_count] = val;
        }
        self.current_count += 1;
    }

    pub fn sort(&mut self) -> Result<()> {
        self.sort_and_truncate(0)
    }

    pub fn sort_and_truncate(&mut self, start: usize) -> Result<()> {
        if self.is_sorted {
            return Ok(());
        }
        ensure!(
            start < self.current_count,
            format!(
                "Cannot truncate entire array: start: {}, count: {}",
                start, self.current_count
            )
        );

        self.sorted_latencies = self.latencies.as_slice()[start..self.current_count].to_vec();
        self.sorted_latencies.sort();
        self.is_sorted = true;
        Ok(())
    }

    pub fn value_at_quantile(&self, quantile: f64) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }
        let index = (self.sorted_latencies.len() as f64 * quantile) as usize;
        Ok(self.sorted_latencies[index])
    }

    /// Logs all the latencies to a file
    pub fn log_to_file(&self, path: &str) -> Result<()> {
        self.log_truncated_to_file(path, 0)
    }

    pub fn log_truncated_to_file(&self, path: &str, start: usize) -> Result<()> {
        tracing::info!("Logging rtts to {}", path);
        let mut file = File::create(path)?;
        tracing::info!(len = self.current_count, "logging to {}", path);
        for idx in start..self.current_count {
            writeln!(file, "{}", self.latencies[idx])?;
        }
        Ok(())
    }

    fn mean(&self) -> Result<f64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        // TODO: use iterative algorithm that won't overflow
        let sum: u64 = self.sorted_latencies.iter().sum();
        Ok(sum as f64 / (self.sorted_latencies.len() as f64))
    }

    fn max(&self) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        Ok(self.sorted_latencies[self.sorted_latencies.len() - 1])
    }

    fn min(&self) -> Result<u64> {
        if self.sorted_latencies.len() == 0 {
            bail!("Cannot run value_at_quantile until sort() has been called.");
        }

        Ok(self.sorted_latencies[0])
    }

    pub fn dump(&self, msg: &str) -> Result<()> {
        if self.current_count == 0 {
            return Ok(());
        }

        tracing::info!(
            msg,
            p5_ms = self.value_at_quantile(0.05)? / 1_000_000 as u64,
            p25_ms = self.value_at_quantile(0.25)? / 1_000_000 as u64,
            p50_ms = self.value_at_quantile(0.5)? / 1_000_000 as u64,
            p75_ms = self.value_at_quantile(0.75)? / 1_000_000 as u64,
            p95_ms = self.value_at_quantile(0.95)? / 1_000_000 as u64,
            p99_ms = self.value_at_quantile(0.99)? / 1_000_000 as u64,
            p999_ms = self.value_at_quantile(0.999)? / 1_000_000 as u64,
            requests_received = self.current_count,
            min_ms = self.min()? / 1_000_000 as u64,
            max_ms = self.max()? / 1_000_000 as u64,
            avg_ms = ?self.mean()? / 1_000_000.0f64
        );
        Ok(())
    }
}
