use super::histogram::{LatencyMap, ManualHistogram};
use color_eyre::eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::to_writer;
use std::{collections::BTreeMap, fs::File};

// This takes a manual histogram and stores it with less precision.
// Useful when rates are very high.
// When precision is None, is a normal histogram.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SummaryHistogram {
    // Precision in terms of nanoseconds
    // when precision is none, record all items.
    pub precision: Option<u64>,
    // Map from latency marker to count
    pub map: BTreeMap<u64, u64>,
    // Count of total latencies
    pub count: usize,
}

impl Default for SummaryHistogram {
    fn default() -> Self {
        SummaryHistogram {
            precision: None,
            map: BTreeMap::default(),
            count: 0,
        }
    }
}

impl SummaryHistogram {
    fn from_manual(precision: Option<u64>, manual_hist: &ManualHistogram) -> Result<Self> {
        let mut hist = Self::default();
        hist.precision = precision;
        for lat in manual_hist.latencies_vec().iter() {
            hist.record(*lat);
        }
        Ok(hist)
    }

    pub fn record(&mut self, latency: u64) {
        if let Some(precision) = self.precision {
            let divisor = latency / precision;
            let bucket = (divisor + 1) * precision;
            *self.map.entry(bucket).or_insert(0) += 1;
            self.count += 1;
            return;
        } else {
            *self.map.entry(latency).or_insert(0) += 1;
            self.count += 1;
        }
    }

    pub fn value_at_quantile(&self, quantile: f64) -> Result<u64> {
        let mut count = 0;
        let total = self.count as f64;
        for (lat, lat_count) in self.map.iter() {
            count += lat_count;
            if count as f64 >= total * quantile {
                return Ok(*lat);
            }
        }
        bail!("Quantile not found: {:?}", quantile);
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SummaryStats {
    pub histogram: SummaryHistogram,
    pub total_objects_sent: usize,
    pub total_objects_recv: usize,
    pub send_time: f64,
    pub receive_time: f64,
}

impl SummaryStats {
    pub fn new(
        rate_seconds: f64,
        exp_time_seconds: usize,
        latency_map: &LatencyMap,
        warmup_time_seconds: usize,
        cooldown_time_seconds: usize,
        use_time_window: bool,
        histogram_precision: Option<u64>,
    ) -> Result<Self> {
        // check warmup and cooldown times are valid
        if warmup_time_seconds > (exp_time_seconds - cooldown_time_seconds) {
            bail!("Warmup time must be less than experiment time - cooldown time");
        }

        // calculate ID window using the warmup and cooldown times
        let start_id = (rate_seconds * warmup_time_seconds as f64).ceil() as usize;
        let mut end_id =
            (rate_seconds * (exp_time_seconds - cooldown_time_seconds) as f64).floor() as usize;

        // if cooldown is 0, possible that end_id is greater than length of map
        if end_id > (latency_map.len() - 1) {
            end_id = latency_map.len() - 1;
        }
        tracing::info!(
            "Start ID: {}, End ID: {}, map len: {}",
            start_id,
            end_id,
            latency_map.len()
        );

        // get histogram, total sent, total recv, sent time, receive time from latency map
        let (histogram, total_sent, total_recv, send_time, recv_time) =
            latency_map.histogram_from_id_range(start_id, end_id, use_time_window)?;

        let summary_histogram = SummaryHistogram::from_manual(histogram_precision, &histogram)?;

        Ok(SummaryStats {
            histogram: summary_histogram,
            total_objects_sent: total_sent,
            total_objects_recv: total_recv,
            send_time,
            receive_time: recv_time,
        })
    }
}

pub fn write_to_file(summary_stats: &SummaryStats, path: String) -> Result<()> {
    to_writer(&File::create(&path)?, summary_stats)?;
    Ok(())
}
