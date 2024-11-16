use color_eyre::eyre::{bail, Result};
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct LatencyMap {
    // map from request ID to (start time, end time)
    // end time is None if request was dropped before exp. ended
    map: std::collections::BTreeMap<usize, (Instant, Option<Instant>)>,
}

impl LatencyMap {
    pub fn new() -> Self {
        LatencyMap {
            map: std::collections::BTreeMap::default(),
        }
    }

    pub fn from_sent_and_recv_times(
        sent_times: &std::collections::HashMap<usize, Instant>,
        recv_times: &std::collections::HashMap<usize, Instant>,
    ) -> Result<Self> {
        let mut map = std::collections::BTreeMap::new();
        for (id, sent_time) in sent_times.iter() {
            if let Some(recv_time) = recv_times.get(id) {
                map.insert(*id, (*sent_time, Some(*recv_time)));
            } else {
                map.insert(*id, (*sent_time, None));
            }
        }
        Ok(LatencyMap { map })
    }

    pub fn record(
        &mut self,
        request_id: usize,
        start: Instant,
        end: Option<Instant>,
    ) -> Result<()> {
        if let Some(end_time) = end {
            if end_time.checked_duration_since(start).is_none() {
                bail!(
                    "End time is before start time: id {}, start {:?}, end {:?}",
                    request_id,
                    start,
                    end_time
                );
            }
        }
        self.map.insert(request_id, (start, end));
        Ok(())
    }

    pub fn get(&self, request_id: usize) -> Option<&(Instant, Option<Instant>)> {
        self.map.get(&request_id)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn dump(&self, msg: &str) {
        tracing::info!(msg, len = self.len());
    }

    pub fn log_to_file(&self, path: &str) -> Result<()> {
        let mut file = File::create(path)?;
        for (request_id, (start, recvd_time)) in self.map.iter() {
            if let Some(end) = recvd_time {
                let latency = end.checked_duration_since(*start);
                if latency.is_none() {
                    bail!(
                        "End time is before start time: id {}, start {:?}, end {:?}",
                        request_id,
                        start,
                        end
                    );
                }
                writeln!(file, "{},{:?}", request_id, latency.unwrap().as_secs_f64())?;
            } else {
                writeln!(file, "{}, DROPPED", request_id)?;
            }
        }
        Ok(())
    }

    pub fn histogram_from_id_range(
        &self,
        start_id: usize,
        end_id: usize,
        use_time_window: bool,
    ) -> Result<(ManualHistogram, usize, usize, f64, f64)> {
        // This function considers a specific ID range
        // And returns histogram, number of requests sent, number of requests received, sent time, and receive time
        // If use_time_window is true, function considers window of time between when start_id was
        // sent and end_id was sent (so sent and receive times are the same)
        // Otherwise, it considers time for all responses to arrive (so sent and receive time
        // may be different)
        // If drops are present, only use_time_window makes sense
        // returns sent and received time in seconds

        if start_id >= end_id {
            bail!("start_id must be less than end_id");
        }

        if !self.map.contains_key(&start_id) {
            bail!("start_id not found in map: {}", start_id);
        }

        if !self.map.contains_key(&end_id) {
            bail!("end_id not found in map : {}", end_id);
        }

        let mut histogram = ManualHistogram::new((end_id - start_id) as usize);
        let start_time = self.map.get(&start_id).unwrap().0;
        let last_sent_time = self.map.get(&end_id).unwrap().0;

        if last_sent_time.checked_duration_since(start_time).is_none() {
            bail!(
                "End time is before start time: start_id {}, end_id {}",
                start_id,
                end_id
            );
        }

        if !use_time_window {
            // calculate time to send and receive all the data

            let mut max_end_time: Option<(Instant, Duration)> = None;
            for id in start_id..end_id {
                let entry = self.map.get(&id);
                if let Some((send_time, recv_time)) = entry {
                    if recv_time.is_none() {
                        bail!(
                            "ID has no recv time: {}; cannot use id-based window with drops",
                            id
                        );
                    }
                    // record for latency histogram
                    let rtt = recv_time.unwrap().duration_since(*send_time);
                    histogram.record(rtt.as_nanos() as u64);

                    if recv_time
                        .unwrap()
                        .checked_duration_since(start_time)
                        .is_none()
                    {
                        bail!(
                            "For id {}, recv_time is before send time of first id: {:?}, {:?}",
                            id,
                            recv_time.unwrap(),
                            start_time
                        );
                    }

                    // update max end time
                    if let Some((_cur_max_end, cur_max_time_since_start)) = max_end_time {
                        if recv_time.unwrap().duration_since(start_time) > cur_max_time_since_start
                        {
                            max_end_time = Some((
                                recv_time.unwrap(),
                                recv_time.unwrap().duration_since(start_time),
                            ));
                        }
                    } else {
                        max_end_time = Some((
                            recv_time.unwrap(),
                            recv_time.unwrap().duration_since(start_time),
                        ));
                    }
                } else {
                    bail!("ID not found in map: {}", id);
                }
            }
            // return histogram, num sent, num received, sent time, received time
            // histogram only contains data received within the time between first ID was sent and
            // last ID was sent
            // TODO: does num_sent = 1 + num_received?
            let num_sent = end_id - start_id;
            let num_received = histogram.len();
            if histogram.len() != (end_id - start_id) {
                bail!(
                    "Histogram length does not match expected: {}, {}",
                    histogram.len(),
                    end_id - start_id
                );
            }
            let sent_time = last_sent_time.duration_since(start_time).as_secs_f64();
            let received_time = max_end_time.unwrap().1.as_secs_f64();
            return Ok((histogram, num_sent, num_received, sent_time, received_time));
        } else {
            let num_sent = end_id - start_id + 1;
            let mut num_received = 0;
            // calculate number of objects sent and received between these IDs
            // consider time as between when start_id is sent and end_id is sent
            for id in start_id..end_id {
                let entry = self.map.get(&id);
                if let Some((send_time, recv_time_option)) = entry {
                    if let Some(recv_time) = recv_time_option {
                        // if receive time is within the sent time, count it
                        if !last_sent_time.checked_duration_since(*recv_time).is_none() {
                            num_received += 1;
                            let rtt = recv_time.duration_since(*send_time);
                            histogram.record(rtt.as_nanos() as u64);
                        }
                    }
                } else {
                    bail!("ID not found in map: {}", id);
                }
            }
            let sent_time = last_sent_time.duration_since(start_time).as_secs_f64();
            let received_time = last_sent_time.duration_since(start_time).as_secs_f64();
            return Ok((histogram, num_sent, num_received, sent_time, received_time));
        }
    }
}

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

    pub fn latencies_vec(&self) -> &[u64] {
        &self.latencies[0..self.current_count]
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
        self.sorted_latencies = self.latencies.as_slice()[0..self.current_count].to_vec();
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
