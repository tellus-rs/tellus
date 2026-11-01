use std::time::Duration;
use thiserror::Error;

/// The bounds of an exponential backoff: a nonzero `min` and a `max` not below it, whether built
/// in code or deserialized from a config.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedBackoff")
)]
pub struct Backoff {
    min: Duration,
    max: Duration,
}

impl Backoff {
    /// The `min` of [Backoff::default].
    pub const DEFAULT_MIN: Duration = Duration::from_millis(250);

    /// The `max` of [Backoff::default].
    pub const DEFAULT_MAX: Duration = Duration::from_secs(4);

    /// The bounds of an exponential backoff starting at `min` and capped at `max`.
    ///
    /// # Errors
    /// Fails if `min` is zero or if `max` is below `min`.
    pub fn new(min: Duration, max: Duration) -> Result<Self, InvalidBackoff> {
        if min.is_zero() {
            return Err(InvalidBackoff::ZeroMin);
        }
        if max < min {
            return Err(InvalidBackoff::MaxBelowMin { min, max });
        }

        Ok(Self { min, max })
    }

    /// The delay of the first step, doubled on every further one; never zero.
    pub fn min(self) -> Duration {
        self.min
    }

    /// The upper bound for the delay, never below [Backoff::min].
    pub fn max(self) -> Duration {
        self.max
    }

    pub(crate) fn duration(self, step: u32) -> Duration {
        let factor = 1u32.checked_shl(step).unwrap_or(u32::MAX);
        self.min.saturating_mul(factor).min(self.max)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MIN, Self::DEFAULT_MAX).expect("the default bounds are valid")
    }
}

/// The bounds given to [Backoff::new] are invalid.
#[derive(Debug, Error)]
pub enum InvalidBackoff {
    /// The minimum is zero, which would make every step zero.
    #[error("min backoff is zero")]
    ZeroMin,

    /// The bounds contradict each other.
    #[error("max backoff {max:?} below min backoff {min:?}")]
    MaxBelowMin {
        /// The minimum which was given.
        min: Duration,

        /// The maximum which was given.
        max: Duration,
    },
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedBackoff {
    #[serde(with = "humantime_serde")]
    min: Duration,

    #[serde(with = "humantime_serde")]
    max: Duration,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedBackoff> for Backoff {
    type Error = InvalidBackoff;

    fn try_from(unchecked: UncheckedBackoff) -> Result<Self, Self::Error> {
        Self::new(unchecked.min, unchecked.max)
    }
}

#[cfg(test)]
mod tests {
    use crate::backoff::{Backoff, InvalidBackoff};
    use std::time::Duration;

    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(3);

    /// The first step is not delayed beyond the minimum, every further one doubles, and the cap
    /// holds; a step wide enough to overflow the shift must saturate into the cap, not wrap.
    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let backoff = Backoff::new(MIN, MAX).expect("the bounds are valid");

        assert_eq!(backoff.duration(0), MIN);
        assert_eq!(backoff.duration(1), MIN * 2);
        assert_eq!(backoff.duration(2), MIN * 4);

        assert_eq!(backoff.duration(64), MAX);
        assert_eq!(backoff.duration(u32::MAX), MAX);
    }

    /// A zero minimum would make every step zero, so the restart loop would never await and
    /// spin through its whole limit; it is refused instead of yielding a backoff which is none.
    #[test]
    fn a_zero_minimum_is_rejected() {
        assert!(matches!(
            Backoff::new(Duration::ZERO, MAX),
            Err(InvalidBackoff::ZeroMin)
        ));
        assert!(matches!(
            Backoff::new(Duration::ZERO, Duration::ZERO),
            Err(InvalidBackoff::ZeroMin)
        ));
    }

    /// Every reachable backoff delays, whatever the step: what lets the restart loop await it
    /// unconditionally.
    #[test]
    fn no_reachable_backoff_is_zero() {
        let backoff = Backoff::new(Duration::from_nanos(1), MAX).expect("the bounds are valid");

        for step in [0, 1, 5, u32::MAX] {
            assert!(!backoff.duration(step).is_zero());
        }
    }

    /// A cap below the minimum is rejected rather than repaired, so no [Backoff] contradicts
    /// itself and a config file naming inverted bounds is reported instead of quietly rewritten.
    #[test]
    fn a_cap_below_the_minimum_is_rejected() {
        assert!(matches!(
            Backoff::new(MAX, MIN),
            Err(InvalidBackoff::MaxBelowMin { .. })
        ));
        assert!(Backoff::new(MIN, MIN).is_ok());
    }

    /// The `try_from` container attribute, the humantime field codecs and the validation bridge
    /// are only reachable through serde; this crosses that boundary in both directions.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_validates_the_bounds() {
        let backoff = serde_json::from_str::<Backoff>(r#"{ "min": "250ms", "max": "3s" }"#)
            .expect("the bounds are valid");
        assert_eq!(backoff.min(), MIN);
        assert_eq!(backoff.max(), MAX);

        assert!(serde_json::from_str::<Backoff>(r#"{ "min": "3s", "max": "250ms" }"#).is_err());
        assert!(serde_json::from_str::<Backoff>(r#"{ "min": "0s", "max": "3s" }"#).is_err());
    }

    /// A misspelled key must be an error, not a silently applied default.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_rejects_unknown_fields() {
        let backoff =
            serde_json::from_str::<Backoff>(r#"{ "min": "250ms", "max": "3s", "mni": "1s" }"#);
        assert!(backoff.is_err());
    }
}
