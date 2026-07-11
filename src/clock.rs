use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};

const CHRONYC_TIMEOUT: Duration = Duration::from_secs(2);
pub const CLOCK_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
pub const CLOCK_SAMPLE_MAX_AGE: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_CLOCK_OFFSET_MS: f64 = 5.0;

#[derive(Debug, Clone)]
pub struct ClockHealthSnapshot {
    pub checked_at_wall_ms: u64,
    pub source: &'static str,
    pub verified: bool,
    pub synchronized: bool,
    pub offset_ms: Option<f64>,
    pub error_bound_ms: Option<f64>,
    pub max_offset_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockAssessment {
    pub healthy: bool,
    pub status: &'static str,
    pub age_ms: Option<u64>,
}

impl ClockHealthSnapshot {
    pub fn unavailable(checked_at: SystemTime, max_offset_ms: f64) -> Self {
        Self {
            checked_at_wall_ms: system_time_ms(checked_at),
            source: "chrony",
            verified: false,
            synchronized: false,
            offset_ms: None,
            error_bound_ms: None,
            max_offset_ms,
        }
    }

    #[cfg(target_os = "linux")]
    fn from_tracking(checked_at: SystemTime, max_offset_ms: f64, tracking: ChronyTracking) -> Self {
        Self {
            checked_at_wall_ms: system_time_ms(checked_at),
            source: "chrony",
            verified: true,
            synchronized: tracking.synchronized,
            offset_ms: Some(tracking.offset_ms),
            error_bound_ms: Some(tracking.error_bound_ms),
            max_offset_ms,
        }
    }

    pub fn assess(&self, wall_now: SystemTime) -> ClockAssessment {
        let wall_now_ms = system_time_ms(wall_now);
        let age_ms = (self.checked_at_wall_ms > 0 && self.checked_at_wall_ms <= wall_now_ms)
            .then(|| wall_now_ms - self.checked_at_wall_ms);
        if age_ms.is_none_or(|age| age > CLOCK_SAMPLE_MAX_AGE.as_millis() as u64) {
            return ClockAssessment {
                healthy: false,
                status: "clock-check-stale",
                age_ms,
            };
        }
        if !self.verified {
            return ClockAssessment {
                healthy: false,
                status: "clock-unverified",
                age_ms,
            };
        }
        if !self.synchronized {
            return ClockAssessment {
                healthy: false,
                status: "clock-unsynchronized",
                age_ms,
            };
        }
        let Some(offset_ms) = self.offset_ms.filter(|value| value.is_finite()) else {
            return ClockAssessment {
                healthy: false,
                status: "clock-unverified",
                age_ms,
            };
        };
        if offset_ms.abs() > self.max_offset_ms {
            return ClockAssessment {
                healthy: false,
                status: "clock-offset-exceeded",
                age_ms,
            };
        }
        let Some(error_bound_ms) = self.error_bound_ms.filter(|value| value.is_finite()) else {
            return ClockAssessment {
                healthy: false,
                status: "clock-unverified",
                age_ms,
            };
        };
        if error_bound_ms > self.max_offset_ms {
            return ClockAssessment {
                healthy: false,
                status: "clock-error-bound-exceeded",
                age_ms,
            };
        }
        ClockAssessment {
            healthy: true,
            status: "healthy",
            age_ms,
        }
    }
}

pub fn validate_max_clock_offset_ms(value: f64) -> Result<f64> {
    if !value.is_finite() || value <= 0.0 || value > 1_000.0 {
        bail!("maximum clock offset must be a finite value in (0, 1000] milliseconds");
    }
    Ok(value)
}

pub async fn sample(max_offset_ms: f64) -> ClockHealthSnapshot {
    let checked_at = SystemTime::now();
    #[cfg(target_os = "linux")]
    {
        let output = tokio::time::timeout(
            CHRONYC_TIMEOUT,
            tokio::process::Command::new("chronyc")
                .args(["-c", "tracking"])
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let tracking = output
            .ok()
            .and_then(Result::ok)
            .filter(|output| output.status.success())
            .and_then(|output| std::str::from_utf8(&output.stdout).ok()?.parse().ok());
        tracking.map_or_else(
            || ClockHealthSnapshot::unavailable(checked_at, max_offset_ms),
            |tracking| ClockHealthSnapshot::from_tracking(checked_at, max_offset_ms, tracking),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = CHRONYC_TIMEOUT;
        ClockHealthSnapshot::unavailable(checked_at, max_offset_ms)
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy)]
struct ChronyTracking {
    synchronized: bool,
    offset_ms: f64,
    error_bound_ms: f64,
}

#[cfg(any(target_os = "linux", test))]
impl std::str::FromStr for ChronyTracking {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let fields = value.trim().split(',').collect::<Vec<_>>();
        if fields.len() != 14 {
            bail!(
                "chronyc tracking returned {} fields, expected 14",
                fields.len()
            );
        }
        let reference_id = fields[0];
        let stratum = parse_finite::<u32>(fields[2], "stratum")?;
        let reference_time = parse_finite::<f64>(fields[3], "reference time")?;
        let offset_seconds = parse_finite::<f64>(fields[4], "system time offset")?;
        let root_delay_seconds = parse_finite::<f64>(fields[10], "root delay")?;
        let root_dispersion_seconds = parse_finite::<f64>(fields[11], "root dispersion")?;
        if root_delay_seconds < 0.0 || root_dispersion_seconds < 0.0 {
            bail!("chronyc returned a negative root delay or dispersion");
        }
        let offset_ms = offset_seconds * 1_000.0;
        // Chrony's documented conservative clock-accuracy bound is the remaining
        // correction plus root dispersion plus half the root delay.
        let error_bound_ms =
            (offset_seconds.abs() + root_dispersion_seconds + 0.5 * root_delay_seconds) * 1_000.0;
        if !reference_time.is_finite() || !offset_ms.is_finite() || !error_bound_ms.is_finite() {
            bail!("chronyc returned a non-finite clock measurement");
        }
        let external_reference = reference_time > 0.0
            && !reference_id.eq_ignore_ascii_case("00000000")
            && !reference_id.eq_ignore_ascii_case("7F7F0101");
        Ok(Self {
            synchronized: (1..16).contains(&stratum)
                && external_reference
                && fields[13].eq_ignore_ascii_case("Normal"),
            offset_ms,
            error_bound_ms,
        })
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_finite<T>(value: &str, field: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .parse::<T>()
        .map_err(anyhow::Error::new)
        .map_err(|error| error.context(format!("parse chronyc {field}")))
}

fn system_time_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEALTHY_TRACKING: &str = "50484330,PHC0,1,1709727533.139866748,0.000004315,-0.000003372,0.000002759,1.274,-0.005,0.161,0.000000001,0.000029493,8.0,Normal";

    fn snapshot(
        synchronized: bool,
        offset_ms: Option<f64>,
        error_bound_ms: Option<f64>,
        max_offset_ms: f64,
    ) -> ClockHealthSnapshot {
        ClockHealthSnapshot {
            checked_at_wall_ms: 10_000,
            source: "chrony",
            verified: true,
            synchronized,
            offset_ms,
            error_bound_ms,
            max_offset_ms,
        }
    }

    #[test]
    fn chrony_csv_yields_offset_and_conservative_error_bound() {
        let tracking: ChronyTracking = HEALTHY_TRACKING.parse().unwrap();
        assert!(tracking.synchronized);
        assert!((tracking.offset_ms - 0.004315).abs() < 0.000_001);
        assert!((tracking.error_bound_ms - 0.033_808_5).abs() < 0.000_001);
    }

    #[test]
    fn chrony_unsynchronized_and_malformed_records_fail_closed() {
        let unsynchronized = HEALTHY_TRACKING.replace("Normal", "Not synchronised");
        assert!(
            !unsynchronized
                .parse::<ChronyTracking>()
                .unwrap()
                .synchronized
        );
        let local_mode = HEALTHY_TRACKING.replacen("50484330", "7F7F0101", 1);
        assert!(!local_mode.parse::<ChronyTracking>().unwrap().synchronized);
        let zero_reference_time = HEALTHY_TRACKING.replacen("1709727533.139866748", "0", 1);
        assert!(
            !zero_reference_time
                .parse::<ChronyTracking>()
                .unwrap()
                .synchronized
        );
        assert!("too,few,fields".parse::<ChronyTracking>().is_err());
        assert!(
            HEALTHY_TRACKING
                .replace("0.000004315", "NaN")
                .parse::<ChronyTracking>()
                .is_err()
        );
    }

    #[test]
    fn assessment_rejects_unsynchronized_excessive_and_stale_clocks() {
        let now = UNIX_EPOCH + Duration::from_millis(10_001);
        assert_eq!(
            snapshot(false, Some(0.1), Some(0.2), 5.0)
                .assess(now)
                .status,
            "clock-unsynchronized"
        );
        assert_eq!(
            snapshot(true, Some(5.1), Some(5.1), 5.0).assess(now).status,
            "clock-offset-exceeded"
        );
        assert_eq!(
            snapshot(true, Some(0.1), Some(5.1), 5.0).assess(now).status,
            "clock-error-bound-exceeded"
        );
        assert_eq!(
            snapshot(true, Some(0.1), None, 5.0).assess(now).status,
            "clock-unverified"
        );
        assert_eq!(
            snapshot(true, Some(0.1), Some(0.2), 5.0)
                .assess(UNIX_EPOCH + Duration::from_millis(40_001))
                .status,
            "clock-check-stale"
        );
    }

    #[test]
    fn assessment_accepts_the_error_threshold_boundary() {
        let assessment = snapshot(true, Some(4.0), Some(5.0), 5.0)
            .assess(UNIX_EPOCH + Duration::from_millis(10_001));
        assert!(assessment.healthy);
        assert_eq!(assessment.status, "healthy");
    }

    #[test]
    fn invalid_thresholds_are_rejected() {
        for value in [f64::NAN, f64::INFINITY, -1.0, 0.0, 1_001.0] {
            assert!(validate_max_clock_offset_ms(value).is_err());
        }
        assert_eq!(validate_max_clock_offset_ms(5.0).unwrap(), 5.0);
    }
}
