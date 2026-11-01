//! Deciding whether a member is still reachable, fed by heartbeats: [FailureDetector] is what
//! [EndpointConfig::failure_detector](crate::cluster::EndpointConfig::failure_detector) takes, the
//! default is the adaptive [PhiAccrualFailureDetector]. An unreachable member is not yet a dead
//! one; that is the [downing](crate::cluster::downing) decision.

use crate::{
    cluster::node::NodeId,
    sync::{lock, read, write},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
    time::{Duration, Instant},
};
use thiserror::Error;

/// Creates the [FailureDetector] for a peer node.
pub type FailureDetectorFactory = Arc<dyn Fn() -> Box<dyn FailureDetector> + Send + Sync>;

/// Decides whether a peer node is still available, fed by heartbeats: every frame received from
/// the node counts as one. One detector instance exists per member incarnation, armed when the
/// member is admitted or learned through gossip and polled once per heartbeat interval; the
/// default is [PhiAccrualFailureDetector].
pub trait FailureDetector
where
    Self: Send + 'static,
{
    /// Record a heartbeat observed at the given instant.
    fn record_heartbeat(&mut self, at: Instant);

    /// Whether the node is to be considered available at the given instant. A node not heard from
    /// yet is available, so arming a detector never condemns the member it was armed for.
    fn is_available(&self, at: Instant) -> bool;
}

/// The deterministic [FailureDetector], e.g. for tests: a node is available as long as the last
/// heartbeat is no older than the given deadline. The default is [PhiAccrualFailureDetector],
/// which learns that deadline instead of cutting a fixed one.
#[derive(Debug)]
pub struct DeadlineFailureDetector {
    config: Deadline,
    last_heartbeat: Option<Instant>,
}

impl DeadlineFailureDetector {
    /// A detector tuned by the given configuration.
    pub fn new(config: Deadline) -> Self {
        Self {
            config,
            last_heartbeat: None,
        }
    }
}

impl FailureDetector for DeadlineFailureDetector {
    fn record_heartbeat(&mut self, at: Instant) {
        self.last_heartbeat = Some(at);
    }

    fn is_available(&self, at: Instant) -> bool {
        match self.last_heartbeat {
            Some(last_heartbeat) => at.duration_since(last_heartbeat) <= self.config.duration(),
            None => true,
        }
    }
}

/// The tuning of a [DeadlineFailureDetector], validated on construction so an invalid
/// configuration is unrepresentable, whether it comes from code or from a deserialized config.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedDeadline")
)]
pub struct Deadline(Duration);

impl Deadline {
    /// A deadline of the given duration.
    ///
    /// # Errors
    /// Fails if `deadline` is zero, which every instant after the heartbeat itself already
    /// exceeds, hence would declare a peer unavailable one tick after it was heard from.
    pub fn new(deadline: Duration) -> Result<Self, InvalidDeadline> {
        if deadline.is_zero() {
            return Err(InvalidDeadline::Zero);
        }

        Ok(Self(deadline))
    }

    /// The duration a heartbeat's age must stay within.
    pub fn duration(self) -> Duration {
        self.0
    }
}

/// The tuning given to [Deadline] is invalid.
#[derive(Debug, Error)]
pub enum InvalidDeadline {
    /// The deadline is zero.
    #[error("deadline is zero")]
    Zero,
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct UncheckedDeadline(#[serde(with = "humantime_serde")] Duration);

#[cfg(feature = "serde")]
impl TryFrom<UncheckedDeadline> for Deadline {
    type Error = InvalidDeadline;

    fn try_from(unchecked: UncheckedDeadline) -> Result<Self, Self::Error> {
        Self::new(unchecked.0)
    }
}

/// A [FailureDetector] accruing suspicion instead of cutting a deadline, after Hayashibara et
/// al.: it learns the peer's heartbeat inter-arrival distribution over a sliding window and
/// derives phi, a logarithmic measure of how improbable the current silence is given that
/// history. The node is available while phi stays below the threshold. The threshold is set as a
/// false positive rate rather than as a duration, so one threshold gives a jittery link a longer
/// effective deadline and a steady one a shorter one.
///
/// Every inbound frame counts as a heartbeat, so a burst of traffic shrinks the learned
/// intervals; the acceptable pause is added to the learned mean before any suspicion accrues,
/// absorbing the silence until the next periodic heartbeat after such a burst, provided the
/// longer of the heartbeat interval and the watch refresh interval stays below the acceptable pause
/// plus the margin the threshold and the minimum standard deviation add.
/// [start_endpoint](crate::cluster::start_endpoint) refuses a configuration which does not. Before
/// two intervals are observed the detector falls back to the warmup deadline.
#[derive(Debug)]
pub struct PhiAccrualFailureDetector {
    config: PhiAccrual,
    intervals: VecDeque<f64>,
    sum: f64,
    squared_sum: f64,
    last_heartbeat: Option<Instant>,
}

impl PhiAccrualFailureDetector {
    /// A detector tuned by the given configuration.
    pub fn new(config: PhiAccrual) -> Self {
        Self {
            config,
            intervals: VecDeque::new(),
            sum: 0.0,
            squared_sum: 0.0,
            last_heartbeat: None,
        }
    }

    /// The logistic approximation of the normal CDF: the threshold's calibration assumes it.
    fn phi(&self, elapsed: Duration) -> f64 {
        let n = self.intervals.len() as f64;
        let mean = self.sum / n + millis(self.config.acceptable_pause);
        let variance = (self.squared_sum / n - (self.sum / n).powi(2)).max(0.0);
        let std_deviation = variance.sqrt().max(millis(self.config.min_std_deviation));

        let x = millis(elapsed);
        let y = (x - mean) / std_deviation;
        let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
        let p = if x > mean {
            e / (1.0 + e)
        } else {
            1.0 - 1.0 / (1.0 + e)
        };
        -p.log10()
    }
}

impl Default for PhiAccrualFailureDetector {
    fn default() -> Self {
        Self::new(PhiAccrual::default())
    }
}

/// The tuning of a [PhiAccrualFailureDetector], validated on construction so an invalid
/// configuration is unrepresentable, whether it comes from code or from a deserialized config.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedPhiAccrual")
)]
pub struct PhiAccrual {
    threshold: f64,
    max_sample_size: usize,
    min_std_deviation: Duration,
    acceptable_pause: Duration,
    warmup_deadline: Duration,
}

impl PhiAccrual {
    /// The `threshold` of [PhiAccrual::default].
    pub const DEFAULT_THRESHOLD: f64 = 8.0;

    /// The `max_sample_size` of [PhiAccrual::default].
    pub const DEFAULT_MAX_SAMPLE_SIZE: usize = 200;

    /// The `min_std_deviation` of [PhiAccrual::default].
    pub const DEFAULT_MIN_STD_DEVIATION: Duration = Duration::from_millis(100);

    /// The `acceptable_pause` of [PhiAccrual::default].
    pub const DEFAULT_ACCEPTABLE_PAUSE: Duration = Duration::from_secs(3);

    /// The `warmup_deadline` of [PhiAccrual::default].
    pub const DEFAULT_WARMUP_DEADLINE: Duration = Duration::from_secs(5);

    /// The smallest window phi is defined for: a variance needs two intervals, and a smaller
    /// window pins the detector to its warmup deadline forever.
    pub const MIN_MAX_SAMPLE_SIZE: usize = 2;

    /// Replace the threshold phi must stay below: 8 means roughly a one in `10^8` chance that
    /// the learned distribution produced the silence which is declared a failure.
    ///
    /// # Errors
    /// Fails if `threshold` is not finite and positive.
    pub fn with_threshold(mut self, threshold: f64) -> Result<Self, InvalidPhiAccrual> {
        if !threshold.is_finite() || threshold <= 0.0 {
            return Err(InvalidPhiAccrual::Threshold(threshold));
        }

        self.threshold = threshold;
        Ok(self)
    }

    /// Replace the number of inter-arrival intervals the sliding window holds.
    ///
    /// # Errors
    /// Fails if `max_sample_size` is below [PhiAccrual::MIN_MAX_SAMPLE_SIZE].
    pub fn with_max_sample_size(
        mut self,
        max_sample_size: usize,
    ) -> Result<Self, InvalidPhiAccrual> {
        if max_sample_size < Self::MIN_MAX_SAMPLE_SIZE {
            return Err(InvalidPhiAccrual::MaxSampleSize(max_sample_size));
        }

        self.max_sample_size = max_sample_size;
        Ok(self)
    }

    /// Replace the floor under the learned standard deviation, which keeps a history of nearly
    /// identical intervals from making the detector suspect a peer after a barely longer than
    /// usual silence.
    ///
    /// # Errors
    /// Fails if `min_std_deviation` is zero, which divides by zero once the learned variance is
    /// zero.
    pub fn with_min_std_deviation(
        mut self,
        min_std_deviation: Duration,
    ) -> Result<Self, InvalidPhiAccrual> {
        if min_std_deviation.is_zero() {
            return Err(InvalidPhiAccrual::MinStdDeviation);
        }

        self.min_std_deviation = min_std_deviation;
        Ok(self)
    }

    /// Replace the pause added to the learned mean before any suspicion accrues.
    ///
    /// Zero is allowed: it adds no pause, so suspicion accrues against the learned mean alone.
    pub fn with_acceptable_pause(mut self, acceptable_pause: Duration) -> Self {
        self.acceptable_pause = acceptable_pause;
        self
    }

    /// Replace the deadline used while fewer than two intervals have been observed.
    ///
    /// # Errors
    /// Fails if `warmup_deadline` is zero, which would declare a peer unavailable one tick after
    /// its first heartbeat while the detector still has fewer than two intervals.
    pub fn with_warmup_deadline(
        mut self,
        warmup_deadline: Duration,
    ) -> Result<Self, InvalidPhiAccrual> {
        if warmup_deadline.is_zero() {
            return Err(InvalidPhiAccrual::ZeroWarmupDeadline);
        }

        self.warmup_deadline = warmup_deadline;
        Ok(self)
    }

    /// The threshold phi must stay below.
    pub fn threshold(self) -> f64 {
        self.threshold
    }

    /// The number of inter-arrival intervals the sliding window holds.
    pub fn max_sample_size(self) -> usize {
        self.max_sample_size
    }

    /// The floor under the learned standard deviation.
    pub fn min_std_deviation(self) -> Duration {
        self.min_std_deviation
    }

    /// The pause added to the learned mean before any suspicion accrues.
    pub fn acceptable_pause(self) -> Duration {
        self.acceptable_pause
    }

    /// The deadline used while fewer than two intervals have been observed.
    pub fn warmup_deadline(self) -> Duration {
        self.warmup_deadline
    }
}

impl Default for PhiAccrual {
    fn default() -> Self {
        Self {
            threshold: Self::DEFAULT_THRESHOLD,
            max_sample_size: Self::DEFAULT_MAX_SAMPLE_SIZE,
            min_std_deviation: Self::DEFAULT_MIN_STD_DEVIATION,
            acceptable_pause: Self::DEFAULT_ACCEPTABLE_PAUSE,
            warmup_deadline: Self::DEFAULT_WARMUP_DEADLINE,
        }
    }
}

/// The tuning given to [PhiAccrual] is invalid.
#[derive(Debug, Error)]
pub enum InvalidPhiAccrual {
    /// The threshold is not finite and positive.
    #[error("threshold {0} not finite and positive")]
    Threshold(f64),

    /// The sliding window is below [PhiAccrual::MIN_MAX_SAMPLE_SIZE].
    #[error("max sample size {0} below 2")]
    MaxSampleSize(usize),

    /// The minimum standard deviation is zero.
    #[error("min std deviation is zero")]
    MinStdDeviation,

    /// The warmup deadline is zero.
    #[error("warmup deadline is zero")]
    ZeroWarmupDeadline,
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct UncheckedPhiAccrual {
    threshold: f64,

    max_sample_size: usize,

    #[serde(with = "humantime_serde")]
    min_std_deviation: Duration,

    #[serde(with = "humantime_serde")]
    acceptable_pause: Duration,

    #[serde(with = "humantime_serde")]
    warmup_deadline: Duration,
}

#[cfg(feature = "serde")]
impl Default for UncheckedPhiAccrual {
    fn default() -> Self {
        Self {
            threshold: PhiAccrual::DEFAULT_THRESHOLD,
            max_sample_size: PhiAccrual::DEFAULT_MAX_SAMPLE_SIZE,
            min_std_deviation: PhiAccrual::DEFAULT_MIN_STD_DEVIATION,
            acceptable_pause: PhiAccrual::DEFAULT_ACCEPTABLE_PAUSE,
            warmup_deadline: PhiAccrual::DEFAULT_WARMUP_DEADLINE,
        }
    }
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedPhiAccrual> for PhiAccrual {
    type Error = InvalidPhiAccrual;

    fn try_from(unchecked: UncheckedPhiAccrual) -> Result<Self, Self::Error> {
        Self::default()
            .with_threshold(unchecked.threshold)?
            .with_max_sample_size(unchecked.max_sample_size)?
            .with_min_std_deviation(unchecked.min_std_deviation)
            .and_then(|config| {
                config
                    .with_acceptable_pause(unchecked.acceptable_pause)
                    .with_warmup_deadline(unchecked.warmup_deadline)
            })
    }
}

impl FailureDetector for PhiAccrualFailureDetector {
    fn record_heartbeat(&mut self, at: Instant) {
        if let Some(last_heartbeat) = self.last_heartbeat {
            let interval = millis(at.duration_since(last_heartbeat));
            self.intervals.push_back(interval);
            self.sum += interval;
            self.squared_sum += interval * interval;

            while self.intervals.len() > self.config.max_sample_size {
                let evicted = self.intervals.pop_front().expect("window is not empty");
                self.sum -= evicted;
                self.squared_sum -= evicted * evicted;
            }
        }

        self.last_heartbeat = Some(at);
    }

    fn is_available(&self, at: Instant) -> bool {
        let Some(last_heartbeat) = self.last_heartbeat else {
            return true;
        };
        let elapsed = at.duration_since(last_heartbeat);

        if self.intervals.len() < 2 {
            return elapsed <= self.config.warmup_deadline;
        }

        self.phi(elapsed) < self.config.threshold
    }
}

#[cfg(feature = "serde")]
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureDetectorConfig {
    PhiAccrual(PhiAccrual),
    Deadline(Deadline),
}

#[cfg(feature = "serde")]
impl FailureDetectorConfig {
    pub(crate) fn factory(self) -> FailureDetectorFactory {
        match self {
            Self::PhiAccrual(config) => {
                Arc::new(move || Box::new(PhiAccrualFailureDetector::new(config)))
            }

            Self::Deadline(config) => {
                Arc::new(move || Box::new(DeadlineFailureDetector::new(config)))
            }
        }
    }
}

pub(crate) struct Liveness(Mutex<HashMap<NodeId, PeerLiveness>>);

impl Liveness {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    /// Idempotent; the initial heartbeat starts the deadline. The only way to obtain a gate:
    /// inbound delivery must never run ungated.
    pub(crate) fn track(&self, node: NodeId, factory: &FailureDetectorFactory) -> PeerLiveness {
        lock(&self.0)
            .entry(node)
            .or_insert_with(|| {
                let mut detector = factory();
                detector.record_heartbeat(Instant::now());
                PeerLiveness::new(detector)
            })
            .clone()
    }

    /// Clones the peer out before asking it, else this map's lock is held across its detector's.
    pub(crate) fn is_available(&self, node: NodeId) -> bool {
        let peer = lock(&self.0).get(&node).cloned();
        peer.is_none_or(|peer| peer.is_available())
    }

    /// Takes the gates only after releasing this map's lock, else it deadlocks against a delivery.
    pub(crate) fn quiesce_fenced(&self, fence: NodeId) {
        let fenced = lock(&self.0)
            .iter()
            .filter(|(node, _)| fence.covers(**node))
            .map(|(_, peer)| peer.clone())
            .collect::<Vec<_>>();

        for peer in fenced {
            peer.quiesce();
        }
    }

    pub(crate) fn untrack_fenced(&self, fence: NodeId) {
        lock(&self.0).retain(|node, _| !fence.covers(*node));
    }

    /// Compute the kept set under this map's lock, else a freshly tracked peer is deleted for good;
    /// a gate an inbound reader still holds is kept regardless.
    pub(crate) fn retain_with<F>(&self, keep: F)
    where
        F: FnOnce() -> HashSet<NodeId>,
    {
        let mut entries = lock(&self.0);
        let keep = keep();
        entries.retain(|node, peer| keep.contains(node) || peer.in_use());
    }
}

/// One peer's failure detector plus the delivery gate every inbound delivery holds, which is what
/// [Liveness::quiesce_fenced] waits out.
#[must_use = "an inbound delivery must hold the gate for its whole duration"]
#[derive(Clone)]
pub(crate) struct PeerLiveness(Arc<Shared>);

impl PeerLiveness {
    pub(crate) fn enter(&self) -> RwLockReadGuard<'_, ()> {
        read(&self.0.gate)
    }

    pub(crate) fn record_heartbeat(&self) {
        lock(&self.0.detector).record_heartbeat(Instant::now());
    }

    fn new(detector: Box<dyn FailureDetector>) -> Self {
        Self(Arc::new(Shared {
            detector: Mutex::new(detector),
            gate: RwLock::new(()),
        }))
    }

    fn is_available(&self) -> bool {
        lock(&self.0.detector).is_available(Instant::now())
    }

    /// Returns only once no delivery holds the gate anymore.
    fn quiesce(&self) {
        drop(write(&self.0.gate));
    }

    /// Whether anything beyond the [Liveness] table itself still holds this peer.
    fn in_use(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }
}

pub(crate) fn tolerates_silence(factory: &FailureDetectorFactory, silence: Duration) -> bool {
    const BURST: usize = 1_024;

    let mut detector = factory();
    let at = Instant::now();
    for _ in 0..BURST {
        detector.record_heartbeat(at);
    }
    detector.is_available(at + silence)
}

struct Shared {
    detector: Mutex<Box<dyn FailureDetector>>,
    gate: RwLock<()>,
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::failure::FailureDetectorConfig;
    use crate::cluster::{
        failure::{
            Deadline, DeadlineFailureDetector, FailureDetector, FailureDetectorFactory,
            InvalidDeadline, InvalidPhiAccrual, Liveness, PhiAccrual, PhiAccrualFailureDetector,
        },
        node::NodeId,
    };
    use std::{
        collections::HashSet,
        sync::{Arc, mpsc},
        thread,
        time::{Duration, Instant},
    };

    const DEADLINE: Duration = Duration::from_secs(5);
    const TICK: Duration = Duration::from_millis(1);
    const HOLD: Duration = Duration::from_millis(50);
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn factory() -> FailureDetectorFactory {
        Arc::new(|| Box::new(DeadlineFailureDetector::new(deadline())))
    }

    fn deadline() -> Deadline {
        Deadline::new(DEADLINE).expect("5s is not zero")
    }

    fn node() -> NodeId {
        NodeId::new("127.0.0.1:1234".parse().expect("valid address"))
    }

    /// A node which has not been heard from yet is available, so a peer is not declared dead
    /// before it ever had a chance to send a heartbeat.
    #[test]
    fn a_node_without_heartbeats_is_available() {
        let detector = DeadlineFailureDetector::new(deadline());

        assert!(detector.is_available(Instant::now() + DEADLINE * 2));
    }

    /// The deadline is inclusive: a node is available up to and including it, and unavailable
    /// from the first tick beyond.
    #[test]
    fn the_deadline_is_inclusive() {
        let now = Instant::now();
        let mut detector = DeadlineFailureDetector::new(deadline());
        detector.record_heartbeat(now);

        assert!(detector.is_available(now));
        assert!(detector.is_available(now + DEADLINE));
        assert!(!detector.is_available(now + DEADLINE + TICK));
    }

    /// A heartbeat restarts the deadline, which is what keeps a live but quiet node alive.
    #[test]
    fn a_heartbeat_restarts_the_deadline() {
        let now = Instant::now();
        let mut detector = DeadlineFailureDetector::new(deadline());
        detector.record_heartbeat(now);
        assert!(!detector.is_available(now + DEADLINE + TICK));

        detector.record_heartbeat(now + DEADLINE);

        assert!(detector.is_available(now + DEADLINE + TICK));
        assert!(!detector.is_available(now + DEADLINE * 2 + TICK));
    }

    /// A node whose gate an inbound reader still holds survives retention even when the keep set
    /// omits it, and a node in the keep set survives without any held gate; only an unheld,
    /// unkept one is dropped.
    #[test]
    fn retention_keeps_held_gates_and_the_keep_set() {
        let liveness = Liveness::new();
        let node = node();

        let gate = liveness.track(node, &factory());
        let entry = Arc::downgrade(&gate.0);

        liveness.retain_with(HashSet::new);
        assert!(
            entry.upgrade().is_some(),
            "a held gate must survive retention"
        );

        drop(gate);
        liveness.retain_with(|| HashSet::from([node]));
        assert!(
            entry.upgrade().is_some(),
            "a kept node must survive retention"
        );

        liveness.retain_with(HashSet::new);
        assert!(
            entry.upgrade().is_none(),
            "an unheld, unkept node must be dropped"
        );
    }

    /// Heartbeats at the given cadence, returning the instant of the last one.
    fn beaten(
        detector: &mut PhiAccrualFailureDetector,
        start: Instant,
        interval: Duration,
        count: usize,
    ) -> Instant {
        let mut at = start;
        for _ in 0..count {
            detector.record_heartbeat(at);
            at += interval;
        }
        at - interval
    }

    /// A steady cadence keeps the peer available through the acceptable pause and condemns it
    /// well beyond: the ordinary lifecycle of a healthy then crashed peer.
    #[test]
    fn a_steady_cadence_stays_available_until_silence() {
        let mut detector = PhiAccrualFailureDetector::default();
        let last = beaten(&mut detector, Instant::now(), Duration::from_secs(1), 10);

        assert!(detector.is_available(last + Duration::from_secs(1)));
        assert!(detector.is_available(last + Duration::from_secs(3)));
        assert!(!detector.is_available(last + Duration::from_secs(30)));
    }

    /// The same silence condemns a steady peer and spares a jittery one: the learned deviation
    /// is what a fixed deadline cannot express.
    #[test]
    fn a_jittery_history_earns_slack() {
        let silence = Duration::from_secs(8);

        let mut steady = PhiAccrualFailureDetector::default();
        let last = beaten(&mut steady, Instant::now(), Duration::from_secs(1), 10);
        assert!(!steady.is_available(last + silence));

        let mut jittery = PhiAccrualFailureDetector::default();
        let mut at = Instant::now();
        for i in 0..10 {
            jittery.record_heartbeat(at);
            at += if i % 2 == 0 {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(7)
            };
        }
        let last = at - Duration::from_secs(7);
        assert!(jittery.is_available(last + silence));
    }

    /// Identical intervals learn zero variance; with the floor, a small overshoot beyond the
    /// acceptable pause does not raise phi past the threshold, while a large one still does.
    #[test]
    fn the_min_std_deviation_floors_the_variance() {
        let mut detector = PhiAccrualFailureDetector::default();
        let last = beaten(&mut detector, Instant::now(), Duration::from_secs(1), 10);

        assert!(detector.is_available(last + Duration::from_millis(4_100)));
        assert!(!detector.is_available(last + Duration::from_millis(5_000)));
    }

    /// Before two intervals are observed there is no distribution to judge by, so the detector
    /// behaves like a deadline detector with the warmup deadline.
    #[test]
    fn warmup_falls_back_to_the_deadline() {
        let now = Instant::now();
        let warmup = Duration::from_secs(5);
        let mut detector = PhiAccrualFailureDetector::default();
        assert!(detector.is_available(now + warmup * 10));

        detector.record_heartbeat(now);
        assert!(detector.is_available(now + warmup));
        assert!(!detector.is_available(now + warmup + TICK));

        detector.record_heartbeat(now + Duration::from_secs(1));
        assert!(!detector.is_available(now + Duration::from_secs(1) + warmup + TICK));
    }

    /// The window slides: an outlier ages out with the intervals after it, so it does not grant
    /// slack forever.
    #[test]
    fn old_intervals_age_out_of_the_window() {
        let config = PhiAccrual::default()
            .with_max_sample_size(2)
            .expect("2 is a valid sample size")
            .with_acceptable_pause(Duration::ZERO);
        let mut detector = PhiAccrualFailureDetector::new(config);

        let mut at = Instant::now();
        detector.record_heartbeat(at);
        at += Duration::from_secs(100);
        detector.record_heartbeat(at);
        at += Duration::from_secs(1);
        detector.record_heartbeat(at);
        at += Duration::from_secs(1);
        detector.record_heartbeat(at);

        assert!(!detector.is_available(at + Duration::from_secs(10)));
    }

    /// The tuning is validated on construction, so a configuration which would silently disable
    /// the detector cannot be built, whether it comes from code or from a deserialized config.
    #[test]
    fn an_invalid_tuning_is_refused() {
        assert!(matches!(
            PhiAccrual::default().with_max_sample_size(1),
            Err(InvalidPhiAccrual::MaxSampleSize(1))
        ));
        assert!(matches!(
            PhiAccrual::default().with_max_sample_size(0),
            Err(InvalidPhiAccrual::MaxSampleSize(0))
        ));
        assert!(PhiAccrual::default().with_max_sample_size(2).is_ok());

        assert!(matches!(
            PhiAccrual::default().with_threshold(f64::NAN),
            Err(InvalidPhiAccrual::Threshold(_))
        ));
        assert!(matches!(
            PhiAccrual::default().with_threshold(f64::INFINITY),
            Err(InvalidPhiAccrual::Threshold(_))
        ));
        assert!(matches!(
            PhiAccrual::default().with_threshold(0.0),
            Err(InvalidPhiAccrual::Threshold(_))
        ));
        assert!(PhiAccrual::default().with_threshold(8.0).is_ok());

        assert!(matches!(
            PhiAccrual::default().with_min_std_deviation(Duration::ZERO),
            Err(InvalidPhiAccrual::MinStdDeviation)
        ));

        assert!(matches!(
            PhiAccrual::default().with_warmup_deadline(Duration::ZERO),
            Err(InvalidPhiAccrual::ZeroWarmupDeadline)
        ));
        assert!(PhiAccrual::default()
            .with_warmup_deadline(Duration::from_secs(5))
            .is_ok());
    }

    /// A zero deadline would declare a peer unavailable one tick after it was heard from, which
    /// is the silent degradation the validation exists to prevent.
    #[test]
    fn a_zero_deadline_is_refused() {
        assert!(matches!(
            Deadline::new(Duration::ZERO),
            Err(InvalidDeadline::Zero)
        ));
        assert_eq!(deadline().duration(), DEADLINE);
    }

    /// The deadline tuning is a human readable duration in a config file, and the validation
    /// holds across that boundary too.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_validates_the_deadline() {
        let config = serde_json::from_str::<Deadline>(r#""3s""#).expect("the deadline is valid");
        assert_eq!(config.duration(), Duration::from_secs(3));

        assert!(serde_json::from_str::<Deadline>(r#""0s""#).is_err());
    }

    /// The detector is chosen by name in a config file, each variant carrying its own tuning;
    /// the built detectors are told apart by what they do, since both are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_failure_detector_is_selected_by_name() {
        let config = serde_json::from_str::<FailureDetectorConfig>(r#"{ "deadline": "3s" }"#)
            .expect("the deadline detector is valid");
        let mut detector = config.factory()();
        let now = Instant::now();
        detector.record_heartbeat(now);
        assert!(detector.is_available(now + Duration::from_secs(3)));
        assert!(!detector.is_available(now + Duration::from_secs(3) + TICK));

        let config = serde_json::from_str::<FailureDetectorConfig>(r#"{ "phi_accrual": {} }"#)
            .expect("the default phi accrual tuning is valid");
        let mut detector = config.factory()();
        detector.record_heartbeat(now);
        assert!(detector.is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE));
        assert!(!detector.is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE + TICK));

        assert!(serde_json::from_str::<FailureDetectorConfig>(r#"{ "deadlien": "3s" }"#).is_err());
    }

    /// The `try_from` container attribute, the humantime field codecs and the validation bridge
    /// are only reachable through serde; this crosses that boundary in both directions.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_validates_the_tuning() {
        let json = r#"{
            "threshold": 12.0,
            "max_sample_size": 500,
            "min_std_deviation": "50ms",
            "acceptable_pause": "1s",
            "warmup_deadline": "10s"
        }"#;

        let config = serde_json::from_str::<PhiAccrual>(json).expect("the tuning is valid");
        assert_eq!(config.threshold(), 12.0);
        assert_eq!(config.max_sample_size(), 500);
        assert_eq!(config.min_std_deviation(), Duration::from_millis(50));
        assert_eq!(config.acceptable_pause(), Duration::from_secs(1));
        assert_eq!(config.warmup_deadline(), Duration::from_secs(10));

        let json = json.replace("\"max_sample_size\": 500", "\"max_sample_size\": 1");
        assert!(serde_json::from_str::<PhiAccrual>(&json).is_err());

        let json = r#"{
            "threshold": 12.0,
            "max_sample_size": 500,
            "min_std_deviation": "50ms",
            "acceptable_pause": "1s",
            "warmup_deadline": "0s"
        }"#;
        assert!(serde_json::from_str::<PhiAccrual>(json).is_err());

        let json = r#"{
            "threshold": 12.0,
            "max_sample_size": 500,
            "min_std_deviation": "50ms",
            "acceptable_pause": "0s",
            "warmup_deadline": "10s"
        }"#;
        let config = serde_json::from_str::<PhiAccrual>(json).expect("zero acceptable_pause is valid");
        assert_eq!(config.acceptable_pause(), Duration::ZERO);

        let config = serde_json::from_str::<PhiAccrual>("{}").expect("every key defaults");
        assert_eq!(config.threshold(), PhiAccrual::DEFAULT_THRESHOLD);
        assert_eq!(
            config.max_sample_size(),
            PhiAccrual::DEFAULT_MAX_SAMPLE_SIZE
        );
        assert_eq!(
            config.min_std_deviation(),
            PhiAccrual::DEFAULT_MIN_STD_DEVIATION
        );
        assert_eq!(
            config.acceptable_pause(),
            PhiAccrual::DEFAULT_ACCEPTABLE_PAUSE
        );
        assert_eq!(
            config.warmup_deadline(),
            PhiAccrual::DEFAULT_WARMUP_DEADLINE
        );

        assert!(serde_json::from_str::<PhiAccrual>(r#"{ "trheshold": 12.0 }"#).is_err());
    }

    /// A fence quiesces and untracks every incarnation it covers, so no covered delivery races
    /// the settling; a newer incarnation at the address keeps its gate.
    #[test]
    fn a_fence_quiesces_and_untracks_every_covered_incarnation() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let liveness = Liveness::new();
        let older = NodeId::new(addr);
        let fence = NodeId::new(addr);
        let newer = NodeId::new(addr);
        let older_gate = Arc::downgrade(&liveness.track(older, &factory()).0);
        let fence_gate = Arc::downgrade(&liveness.track(fence, &factory()).0);
        let newer_gate = liveness.track(newer, &factory());

        liveness.quiesce_fenced(fence);
        liveness.untrack_fenced(fence);

        assert!(
            older_gate.upgrade().is_none(),
            "a covered gate must be gone"
        );
        assert!(
            fence_gate.upgrade().is_none(),
            "the fence's gate must be gone"
        );
        assert!(liveness.is_available(newer));
        drop(newer_gate);
    }

    /// Quiescing waits out every delivery holding the gate and returns once the last guard is
    /// dropped.
    #[test]
    fn quiesce_waits_out_deliveries() {
        let liveness = Liveness::new();
        let node = node();
        let gate = liveness.track(node, &factory());

        let guard = gate.enter();
        thread::scope(|scope| {
            let (quiesced_tx, quiesced_rx) = mpsc::channel();
            let liveness = &liveness;
            scope.spawn(move || {
                liveness.quiesce_fenced(node);
                let _ = quiesced_tx.send(());
            });

            assert!(
                quiesced_rx.recv_timeout(HOLD).is_err(),
                "quiesce returned while a delivery held the gate"
            );

            drop(guard);
            quiesced_rx
                .recv_timeout(TIMEOUT)
                .expect("quiesce did not return after the gate was released");
        });
    }
}
