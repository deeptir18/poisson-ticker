use color_eyre::eyre::{bail, Result, WrapErr};
use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use std::time::Duration;

#[inline]
pub fn rate_pps_to_interarrival_nanos(rate: u64) -> f64 {
    tracing::debug!("Nanos intersend: {:?}", 1_000_000_000.0 / rate as f64);
    1_000_000_000.0 / rate as f64
}

#[inline]
pub fn nanos_to_hz(hz: u64, nanos: u64) -> u64 {
    ((hz as f64 / 1_000_000_000.0) * (nanos as f64)) as u64
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DistributionType {
    Uniform,
    Exponential,
}

#[derive(Debug, Copy, Clone)]
pub enum PacketDistribution {
    Uniform(u64),
    Exponential(f64),
}

impl std::str::FromStr for DistributionType {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<DistributionType> {
        Ok(match s {
            "uniform" | "Uniform" | "UNIFORM" => DistributionType::Uniform,
            "exponential" | "Exponential" | "EXPONENTIAL" | "exp" | "EXP" => {
                DistributionType::Exponential
            }
            x => bail!("{} distribution type unknown", x),
        })
    }
}

impl PacketDistribution {
    fn new(typ: DistributionType, rate_pps: u64) -> Result<Self> {
        let interarrival_nanos = rate_pps_to_interarrival_nanos(rate_pps);
        match typ {
            DistributionType::Uniform => Ok(PacketDistribution::Uniform(interarrival_nanos as u64)),
            DistributionType::Exponential => {
                let l = interarrival_nanos;
                Ok(PacketDistribution::Exponential(l))
            }
        }
    }

    fn get_interarrival_avg(&self) -> u64 {
        match self {
            PacketDistribution::Uniform(x) => *x,
            PacketDistribution::Exponential(x) => *x as u64,
        }
    }

    fn sample(&self) -> u64 {
        // TODO: how do we know the thread rngs are initialized?
        let mut rng = thread_rng();
        match *self {
            PacketDistribution::Uniform(interarrival_nanos) => interarrival_nanos,
            PacketDistribution::Exponential(l) => {
                let exp = Exp::new(1.0 / l).expect("Not able to make exponential distribution");
                exp.sample(&mut rng) as u64
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct RequestSchedule {
    pub interarrivals: Vec<Duration>,
    pub avg_interarrival: u64,
}

impl RequestSchedule {
    pub fn new(num_requests: usize, rate_pps: u64, dist_type: DistributionType) -> Result<Self> {
        tracing::debug!("Initializing packet schedule for {} requests", num_requests);
        let distribution = PacketDistribution::new(dist_type, rate_pps)
            .wrap_err("Failed to initialize distribution")?;
        let mut interarrivals: Vec<Duration> = Vec::with_capacity(num_requests);
        for _ in 0..num_requests {
            interarrivals.push(Duration::from_nanos(distribution.sample()));
        }

        Ok(RequestSchedule {
            interarrivals,
            avg_interarrival: distribution.get_interarrival_avg(),
        })
    }

    pub fn get_avg_interarrival(&self) -> u64 {
        self.avg_interarrival
    }

    pub fn len(&self) -> usize {
        self.interarrivals.len()
    }

    pub fn get(&self, idx: usize) -> Duration {
        self.interarrivals[idx]
    }
}
