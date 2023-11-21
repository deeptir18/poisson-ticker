use super::histogram::ManualHistogram;
use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::to_writer;
use std::{collections::BTreeMap, fs::File};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SummaryHistogram {
    // Precision in terms of nanoseconds
    pub precision: u64,
    // Map from latency marker to count
    pub map: BTreeMap<u64, u64>,
    // Count of total latencies
    pub count: usize,
}

impl Default for SummaryHistogram {
    fn default() -> Self {
        SummaryHistogram {
            precision: 1000000,
            map: BTreeMap::default(),
            count: 0,
        }
    }
}

impl SummaryHistogram {
    fn from_manual(precision: u64, manual_hist: &ManualHistogram) -> Result<Self> {
        let mut hist = Self::default();
        hist.precision = precision;
        for lat in manual_hist.latencies_vec().iter() {
            hist.record(*lat);
        }
        Ok(hist)
    }

    pub fn record(&mut self, latency: u64) {
        let divisor = latency / self.precision;
        let bucket = (divisor + 1) * self.precision;
        *self.map.entry(bucket).or_insert(0) += 1;
        self.count += 1;
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SummaryStats {
    pub histogram: SummaryHistogram,
    pub total_objects: usize,
    pub send_time: f64,
    pub receive_time: f64,
}

impl SummaryStats {
    pub fn new(
        manual_histogram: &ManualHistogram,
        total_send_time: std::time::Duration,
        total_recv_time: std::time::Duration,
    ) -> Result<Self> {
        Ok(SummaryStats {
            total_objects: manual_histogram.len(),
            send_time: total_send_time.as_secs_f64(),
            receive_time: total_recv_time.as_secs_f64(),
            histogram: SummaryHistogram::from_manual(1_000_000, &manual_histogram)?,
        })
    }
}

pub fn write_to_file(summary_stats: &SummaryStats, path: String) -> Result<()> {
    to_writer(&File::create(&path)?, summary_stats)?;
    Ok(())
}
