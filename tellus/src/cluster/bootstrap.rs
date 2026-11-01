#[cfg(feature = "serde")]
use crate::cluster::formation::FormationProviderConfig;
use crate::cluster::{
    endpoint::{self, EndpointInner, FormError},
    formation::{
        Decision, Declined, Formation, FormationProvider, FormationProviderFactory, Majority,
    },
    membership::{JoinRound, join_round},
};
use derive_more::Debug;
use std::{
    collections::BTreeSet,
    convert::Infallible,
    error::Error,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error as ThisError;
use tokio::{
    task,
    time::{self as tokio_time, MissedTickBehavior, interval, sleep, timeout_at},
};
use tracing::{debug, warn};

/// A source of seed addresses for [bootstrap], e.g. a static list ([FixedSeeds]), DNS (the
/// `tellus-bootstrap-dns` crate) or Kubernetes pods (the `tellus-bootstrap-k8s` crate).
#[trait_variant::make(Send)]
pub trait SeedDiscovery
where
    Self: Send + 'static,
{
    /// The type of the discovery's failures.
    type Error: Error + Send + Sync + 'static;

    /// Resolve the current seed addresses, this node's own included when it is discoverable;
    /// order and duplicates are irrelevant. Called repeatedly by [bootstrap], so a failure is
    /// retried, not fatal.
    ///
    /// # Errors
    /// Implementations fail if the seeds cannot be resolved right now, e.g. the resolver or the
    /// API server does not answer.
    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, Self::Error>;
}

/// The [SeedDiscovery] for a statically configured cluster: resolves to the same fixed
/// addresses every time.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize), serde(transparent))]
pub struct FixedSeeds(Vec<SocketAddr>);

impl FixedSeeds {
    /// A discovery resolving to exactly the given addresses.
    pub fn new(seeds: Vec<SocketAddr>) -> Self {
        Self(seeds)
    }
}

impl SeedDiscovery for FixedSeeds {
    type Error = Infallible;

    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, Infallible> {
        Ok(self.0.clone())
    }
}

/// Configuration for [bootstrap], deserializable with the `serde` feature: every field falls
/// back to its value in [BootstrapConfig::new], and the formation provider is chosen by name, see
/// docs/cluster.md.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedBootstrapConfig")
)]
pub struct BootstrapConfig {
    /// The minimum size of the universe, the resolved addresses united with this node's own,
    /// before a formation decision is taken; one allows a deliberately single-node cluster.
    /// Counting the universe rather than the resolved set keeps the floor meaningful whether or
    /// not discovery happens to resolve this node's own address. Nodes deciding on discovery
    /// views with different lowest addresses can form separate clusters, see [bootstrap]; a
    /// higher minimum shrinks that window.
    ///
    /// Defaults to [BootstrapConfig::DEFAULT_MIN_PEERS].
    pub min_peers: NonZeroUsize,

    /// How long the resolved address set must stay unchanged before the formation decision is
    /// taken; joins are attempted on every resolution regardless. A new set is never settled, so
    /// zero takes the decision on the second resolution seeing the same addresses, not on the
    /// first. Defaults to [BootstrapConfig::DEFAULT_SETTLE].
    pub settle: Duration,

    /// The pause between two resolutions. Must not be zero, which would resolve in a busy loop;
    /// defaults to [BootstrapConfig::DEFAULT_RESOLVE_INTERVAL].
    pub resolve_interval: Duration,

    /// Creates the [FormationProvider] deciding whether this node forms a new cluster when no
    /// discovered address is a member of one; the default is [Majority].
    #[debug(skip)]
    pub formation: FormationProviderFactory,

    /// The interval at which, once this node is a member, the discovery is resolved again and
    /// every resolved address which is neither this node nor an Up member is probed for the
    /// cluster it belongs to. A cluster other than this node's shows up in
    /// [foreign_clusters](crate::cluster::ClusterState::foreign_clusters) and is logged at error
    /// level. `None` disables the probe; `Some` must not be zero. Defaults to
    /// [BootstrapConfig::DEFAULT_PROBE_INTERVAL].
    pub probe_interval: Option<Duration>,
}

impl BootstrapConfig {
    /// The `min_peers` of [BootstrapConfig::new].
    pub const DEFAULT_MIN_PEERS: NonZeroUsize = NonZeroUsize::new(2).expect("2 is not zero");

    /// The `settle` of [BootstrapConfig::new].
    pub const DEFAULT_SETTLE: Duration = Duration::from_secs(3);

    /// The `resolve_interval` of [BootstrapConfig::new].
    pub const DEFAULT_RESOLVE_INTERVAL: Duration = Duration::from_secs(1);

    /// The `probe_interval` of [BootstrapConfig::new].
    pub const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(30);

    /// A configuration with every field taken from its `DEFAULT_*` constant, forming by
    /// [Majority].
    pub fn new() -> Self {
        Self {
            min_peers: Self::DEFAULT_MIN_PEERS,
            settle: Self::DEFAULT_SETTLE,
            resolve_interval: Self::DEFAULT_RESOLVE_INTERVAL,
            formation: Arc::new(|| Box::new(Majority)),
            probe_interval: Some(Self::DEFAULT_PROBE_INTERVAL),
        }
    }
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The [BootstrapConfig] given to [bootstrap] is invalid.
#[derive(Debug, ThisError)]
pub enum InvalidBootstrapConfig {
    /// The configured `resolve_interval` is zero.
    #[error("resolve_interval is zero")]
    ZeroResolveInterval,

    /// The configured `probe_interval` is zero.
    #[error("probe_interval is zero")]
    ZeroProbeInterval,
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedBootstrapConfig {
    #[serde(default)]
    min_peers: Option<NonZeroUsize>,

    #[serde(default, with = "humantime_serde")]
    settle: Option<Duration>,

    #[serde(default, with = "humantime_serde")]
    resolve_interval: Option<Duration>,

    #[serde(default)]
    formation: Option<FormationProviderConfig>,

    #[serde(default, deserialize_with = "deserialize_probe_interval")]
    probe_interval: Option<Option<Duration>>,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedBootstrapConfig> for BootstrapConfig {
    type Error = InvalidBootstrapConfig;

    fn try_from(unchecked: UncheckedBootstrapConfig) -> Result<Self, Self::Error> {
        let defaults = Self::new();

        let config = Self {
            min_peers: unchecked.min_peers.unwrap_or(defaults.min_peers),
            settle: unchecked.settle.unwrap_or(defaults.settle),
            resolve_interval: unchecked
                .resolve_interval
                .unwrap_or(defaults.resolve_interval),
            formation: unchecked
                .formation
                .map_or(defaults.formation, FormationProviderConfig::factory),
            probe_interval: unchecked.probe_interval.unwrap_or(defaults.probe_interval),
        };
        validate(&config)?;

        Ok(config)
    }
}

/// The cluster cannot be bootstrapped.
#[derive(Debug, ThisError)]
pub enum BootstrapError {
    /// The given configuration is invalid.
    #[error(transparent)]
    Config(#[from] InvalidBootstrapConfig),

    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// This node's incarnation has been downed or has left; only a restarted process, with a
    /// fresh incarnation, can join again, so the caller should exit.
    #[error("this node has been downed")]
    Downed,
}

/// Discover the cluster and become a member of it, once: resolve seed addresses with the given
/// discovery every [resolve_interval](BootstrapConfig::resolve_interval) and attempt to join
/// through every resolved address but this node's own on every resolution. A started endpoint is
/// no cluster and refuses joins, so nodes starting together admit each other nowhere. Only the
/// formation decision waits for the view to settle: the resolved set, this node's own address
/// included, must have held at least [min_peers](BootstrapConfig::min_peers) addresses unchanged
/// for [settle](BootstrapConfig::settle). When no address of a settled view is a member of a
/// cluster, the [FormationProvider] decides whether this node forms one. By default it forms at
/// the lowest address of the universe once a majority of it is known to be no cluster. Requiring a
/// majority keeps a partitioned node from coming back as a cluster of its own. With `min_peers` of
/// one, a resolved set holding nothing but this node's own address forms a cluster of one under
/// [Majority]. A provider's decline is logged at warn level when it or the universe differs from
/// the previous decline, at debug level otherwise. A formation the endpoint refuses since a join
/// attempt ended ambiguously is logged the same way, see [FormError::JoinAmbiguous].
///
/// Returns at once when this node is already a member, e.g. through [form](crate::cluster::form),
/// and otherwise once this node is a member. Membership owns failure detection and downing from
/// then on. With a [probe_interval](BootstrapConfig::probe_interval), a task keeps the discovery
/// and probes every resolved address which is neither this node nor an Up member for the cluster
/// it belongs to, until this node is downed. [BootstrapError::Downed] means the caller should
/// exit so a restart mints the fresh incarnation. A resolve failure is logged and retried, like a
/// seed which is not a member yet, and restarts the settle window, since nothing was observed
/// during it. A settled view holding fewer than `min_peers` addresses is logged at warn level on
/// every resolution, since it blocks the formation decision and would otherwise be silent. There
/// is no internal timeout, so wrap the call in [timeout](tokio::time::timeout) to bound
/// bootstrap. Nodes deciding on discovery views with different lowest addresses can still form
/// separate clusters, which never merge on their own; the settle window and the `min_peers` floor
/// are the guard, so size `min_peers` to the deployment when possible.
///
/// # Errors
/// Fails if the configuration is invalid, if the endpoint is not started, or if this node has
/// been downed or has left.
pub async fn bootstrap<D>(discovery: D, config: BootstrapConfig) -> Result<(), BootstrapError>
where
    D: SeedDiscovery,
{
    validate(&config)?;

    let endpoint = endpoint::get().ok_or(BootstrapError::EndpointNotStarted)?;
    run(endpoint, discovery, config).await
}

pub(crate) async fn run<D>(
    endpoint: &'static EndpointInner,
    mut discovery: D,
    config: BootstrapConfig,
) -> Result<(), BootstrapError>
where
    D: SeedDiscovery,
{
    become_member(endpoint, &mut discovery, &config).await?;

    if let Some(probe_interval) = config.probe_interval {
        task::spawn(probe_foreign_clusters(endpoint, discovery, probe_interval));
    }
    Ok(())
}

fn validate(config: &BootstrapConfig) -> Result<(), InvalidBootstrapConfig> {
    if config.resolve_interval.is_zero() {
        return Err(InvalidBootstrapConfig::ZeroResolveInterval);
    }
    if config
        .probe_interval
        .is_some_and(|interval| interval.is_zero())
    {
        return Err(InvalidBootstrapConfig::ZeroProbeInterval);
    }

    Ok(())
}

async fn become_member<D>(
    endpoint: &'static EndpointInner,
    discovery: &mut D,
    config: &BootstrapConfig,
) -> Result<(), BootstrapError>
where
    D: SeedDiscovery,
{
    let self_addr = endpoint.node().addr();
    let mut settle = Settle::new(config.settle);
    let mut formation = (config.formation)();
    let mut declines = Declines::default();
    loop {
        if endpoint.downed() {
            return Err(BootstrapError::Downed);
        }
        // Nothing left to decide, and rejoining churns this node's lane to the seed.
        if endpoint.formed() {
            return Ok(());
        }

        match discovery.resolve().await {
            Ok(resolved) => {
                let universe = universe(resolved, self_addr);
                let settled = settle.settled(&universe, now());
                let enough_peers = universe.len() >= config.min_peers.get();

                if settled && !enough_peers {
                    warn!(
                        universe = universe.len(),
                        min_peers = config.min_peers.get(),
                        "waiting for discovery to resolve more addresses"
                    );
                }
                let decides = settled && enough_peers;
                if decides {
                    release_gone(endpoint, &universe);
                }

                let seeds = universe.iter().copied().collect::<Vec<_>>();
                let round = join_round(endpoint, &seeds).await;
                match step(round, universe, self_addr, now()) {
                    Step::Joined => return Ok(()),

                    Step::Downed => return Err(BootstrapError::Downed),

                    Step::Observe(observed) => formation.observe(&observed),

                    Step::Decide(decision) => {
                        formation.observe(&decision);
                        if decides && decide(endpoint, formation.as_mut(), decision, &mut declines)?
                        {
                            return Ok(());
                        }
                    }
                }
            }

            Err(error) => {
                settle.reset();
                warn!(%error, "cannot resolve seed addresses");
            }
        }
        sleep(config.resolve_interval).await;
    }
}

fn universe(resolved: Vec<SocketAddr>, self_addr: SocketAddr) -> BTreeSet<SocketAddr> {
    let mut universe = BTreeSet::from_iter(resolved);
    universe.insert(self_addr);
    universe
}

struct Settle {
    settle: Duration,
    current: Option<(BTreeSet<SocketAddr>, Instant)>,
}

impl Settle {
    fn new(settle: Duration) -> Self {
        Self {
            settle,
            current: None,
        }
    }

    fn settled(&mut self, resolved: &BTreeSet<SocketAddr>, at: Instant) -> bool {
        match &self.current {
            Some((current, unchanged_since)) if current == resolved => {
                at.duration_since(*unchanged_since) >= self.settle
            }

            _ => {
                self.current = Some((resolved.clone(), at));
                false
            }
        }
    }

    fn reset(&mut self) {
        self.current = None;
    }
}

/// Tokio's clock, so paused time drives the settle window and the provider's `now` alike.
fn now() -> Instant {
    tokio_time::Instant::now().into_std()
}

fn release_gone(endpoint: &'static EndpointInner, universe: &BTreeSet<SocketAddr>) {
    if let Some(released) = endpoint.release_pin_if_gone(universe) {
        debug!(cluster_addr = %released, "released the pinned cluster, discovery dropped it");
    }
    for released in endpoint.release_ambiguous_joins_if_gone(universe) {
        debug!(seed_addr = %released, "released an ambiguous join attempt, discovery dropped it");
    }
}

/// What a join round leaves to bootstrap: every round which admitted nowhere is observed, but
/// only a round no seed admitted is a formation decision.
enum Step {
    Joined,
    Downed,
    Observe(Formation),
    Decide(Formation),
}

fn step(
    round: JoinRound,
    universe: BTreeSet<SocketAddr>,
    self_addr: SocketAddr,
    now: Instant,
) -> Step {
    let formation = |outcomes| Formation {
        universe,
        self_addr,
        outcomes,
        now,
    };

    match round {
        JoinRound::Joined => {
            debug!("bootstrap decided: joined an existing cluster");
            Step::Joined
        }

        JoinRound::Downed => Step::Downed,

        JoinRound::ClusterSeen(addr, outcomes) => {
            debug!(cluster_addr = %addr, "a cluster admitted this node, retrying it");
            Step::Observe(formation(outcomes))
        }

        JoinRound::Held(addr, outcomes) => {
            debug!(cluster_addr = %addr, "a cluster holds back joins, retrying it");
            Step::Observe(formation(outcomes))
        }

        JoinRound::ForeignCluster(outcomes) => {
            debug!("every seed is a member of another cluster than this node's");
            Step::Observe(formation(outcomes))
        }

        JoinRound::NoAdmission(outcomes) => Step::Decide(formation(outcomes)),
    }
}

/// `true` once this node is a member; `false` while the provider declines or a join attempt
/// could be making this node a member of another cluster, so the decision has to be retaken.
fn decide(
    endpoint: &'static EndpointInner,
    provider: &mut dyn FormationProvider,
    formation: Formation,
    declines: &mut Declines,
) -> Result<bool, BootstrapError> {
    let declined = match provider.form(&formation) {
        Decision::Form => match endpoint.form_cluster() {
            Ok(()) => {
                debug!("bootstrap decided: formed a cluster");
                return Ok(true);
            }

            // A concurrent join or an explicit form got there first, which is the same outcome.
            Err(FormError::AlreadyFormed) => return Ok(true),

            Err(FormError::Downed) => return Err(BootstrapError::Downed),

            Err(FormError::JoinInFlight | FormError::ClusterPinned(_)) => {
                debug!("formation postponed, a join attempt is unresolved");
                return Ok(false);
            }

            Err(FormError::JoinAmbiguous(addr)) => Declined::Ambiguous { addr },

            Err(FormError::EndpointNotStarted) => unreachable!("the endpoint is started"),
        },

        Decision::Decline(declined) => declined,
    };

    declines.report(declined, formation.universe);
    Ok(false)
}

#[derive(Default)]
struct Declines {
    last: Option<(Declined, BTreeSet<SocketAddr>)>,
}

impl Declines {
    /// `true` if the decline was logged at warn level.
    fn report(&mut self, declined: Declined, universe: BTreeSet<SocketAddr>) -> bool {
        if self
            .last
            .as_ref()
            .is_some_and(|last| last.0 == declined && last.1 == universe)
        {
            debug!(%declined, "not forming a cluster");
            return false;
        }

        warn!(%declined, universe = universe.len(), "not forming a cluster");
        self.last = Some((declined, universe));
        true
    }
}

async fn probe_foreign_clusters<D>(
    endpoint: &'static EndpointInner,
    mut discovery: D,
    probe_interval: Duration,
) where
    D: SeedDiscovery,
{
    endpoint.retain_foreign_clusters_for(probe_interval.saturating_mul(3));
    let mut ticks = interval(probe_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let deadline = ticks.tick().await + probe_interval;
        if endpoint.downed() {
            debug!("foreign cluster probe stopped, this node is downed");
            return;
        }

        let resolved = match discovery.resolve().await {
            Ok(resolved) => resolved,

            Err(error) => {
                warn!(%error, "cannot resolve seed addresses to probe");
                continue;
            }
        };
        let up = endpoint
            .membership()
            .up_nodes()
            .iter()
            .map(|node| node.addr())
            .collect::<BTreeSet<_>>();
        let probes = BTreeSet::from_iter(resolved)
            .into_iter()
            .filter(|addr| *addr != endpoint.node().addr() && !up.contains(addr))
            .map(|addr| (addr, endpoint.request_probe(addr)))
            .collect::<Vec<_>>();

        for (addr, probe) in probes {
            match timeout_at(deadline, probe).await {
                Ok(Ok(Ok(cluster))) => endpoint.sight_cluster(addr, cluster),

                Ok(Ok(Err(error))) => debug!(probe_addr = %addr, %error, "cannot probe address"),

                Ok(Err(_)) => warn!(probe_addr = %addr, "probe dropped unanswered"),

                Err(_) => debug!(probe_addr = %addr, "probe timed out"),
            }
        }
    }
}

#[cfg(feature = "serde")]
fn deserialize_probe_interval<'de, D>(deserializer: D) -> Result<Option<Option<Duration>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    humantime_serde::option::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::{
        bootstrap::SeedDiscovery,
        formation::{Decision, Formation},
    };
    use crate::cluster::{
        bootstrap::{
            BootstrapConfig, Declines, FixedSeeds, InvalidBootstrapConfig, Settle, Step, bootstrap,
            probe_foreign_clusters, run, step, universe, validate,
        },
        endpoint::{EndpointConfig, EndpointInner},
        formation::{Declined, JoinOutcome, MajorityWithFallback},
        frame::RefusalReason,
        membership::JoinRound,
        node::{ClusterId, NodeId},
        peer::{ConnectError, JoinRequest},
        state::ForeignCluster,
    };
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::{
        task::{self, JoinHandle},
        time::{self as tokio_time, sleep, timeout},
    };

    const SETTLE: Duration = Duration::from_secs(10);
    const RESOLVE_INTERVAL: Duration = Duration::from_secs(1);

    /// A discovery omitting this node's own address still yields the full universe, so a
    /// `min_peers` sized to the deployment is met either way.
    #[test]
    fn the_universe_adds_this_node_whether_or_not_discovery_resolved_it() {
        let one = addr(1);
        let two = addr(2);
        let three = addr(3);

        assert_eq!(
            universe(vec![two, three], one),
            BTreeSet::from([one, two, three])
        );
        assert_eq!(
            universe(vec![one, two, three], one),
            BTreeSet::from([one, two, three])
        );
        assert_eq!(universe(vec![], one), BTreeSet::from([one]));
    }

    #[test]
    fn test_settle() {
        let settle_duration = Duration::from_secs(3);
        let mut settle = Settle::new(settle_duration);
        let now = Instant::now();
        let one = BTreeSet::from_iter([addr(1)]);
        let one_two = BTreeSet::from_iter([addr(1), addr(2)]);

        assert!(!settle.settled(&one, now));
        assert!(!settle.settled(&one, now + Duration::from_secs(1)));
        assert!(settle.settled(&one, now + settle_duration));

        assert!(!settle.settled(&one_two, now + settle_duration));
        assert!(!settle.settled(&one_two, now + Duration::from_secs(5)));
        assert!(settle.settled(&one_two, now + settle_duration + settle_duration));

        settle.reset();
        let observed = now + settle_duration + settle_duration + Duration::from_secs(1);
        assert!(!settle.settled(&one_two, observed));
        assert!(!settle.settled(&one_two, now + settle_duration * 3));
        assert!(settle.settled(&one_two, observed + settle_duration));
    }

    /// A held round proves a cluster exists at the seed, so it is only observed and never a
    /// formation decision. The same holds for a round which saw a cluster or nothing but foreign
    /// ones. Only a round no seed admitted is a formation decision.
    #[test]
    fn only_a_round_without_admission_is_a_formation_decision() {
        let universe = BTreeSet::from([addr(1), addr(2), addr(3)]);
        let now = Instant::now();
        let held = vec![
            (addr(2), JoinOutcome::Unavailable),
            (addr(3), JoinOutcome::Held),
        ];
        let unavailable = vec![(addr(2), JoinOutcome::Unavailable)];

        assert!(matches!(
            step(JoinRound::Held(addr(3), held.clone()), universe.clone(), addr(1), now),
            Step::Observe(observed) if observed.outcomes == held && observed.now == now
        ));
        assert!(matches!(
            step(
                JoinRound::ClusterSeen(addr(2), unavailable.clone()),
                universe.clone(),
                addr(1),
                now
            ),
            Step::Observe(observed) if observed.outcomes == unavailable
        ));
        assert!(matches!(
            step(
                JoinRound::ForeignCluster(unavailable.clone()),
                universe.clone(),
                addr(1),
                now
            ),
            Step::Observe(observed) if observed.outcomes == unavailable
        ));
        assert!(matches!(
            step(
                JoinRound::NoAdmission(vec![(addr(2), JoinOutcome::NoCluster)]),
                universe,
                addr(1),
                now
            ),
            Step::Decide(decision)
                if decision.outcomes == [(addr(2), JoinOutcome::NoCluster)] && decision.now == now
        ));
    }

    /// A decline is warned about once; repeating it for the same universe is not, while another
    /// reason or another universe is.
    #[test]
    fn a_decline_is_warned_once_per_reason_and_universe() {
        let mut declines = Declines::default();
        let universe = BTreeSet::from([addr(1), addr(2)]);
        let not_lowest = Declined::NotLowest { lowest: addr(1) };

        assert!(declines.report(not_lowest.clone(), universe.clone()));
        assert!(!declines.report(not_lowest.clone(), universe.clone()));
        assert!(!declines.report(not_lowest.clone(), universe.clone()));

        let larger = BTreeSet::from([addr(1), addr(2), addr(3)]);
        assert!(declines.report(not_lowest.clone(), larger.clone()));
        assert!(declines.report(Declined::Explicit, larger.clone()));
        assert!(!declines.report(Declined::Explicit, larger));
    }

    /// Joining does not wait for the view to settle: a seed which is a member admits this node on
    /// the first resolution.
    #[tokio::test(start_paused = true)]
    async fn bootstrap_joins_before_the_view_has_settled() {
        let (endpoint, _lane_dials, join_requests) = endpoint(addr(2), EndpointConfig::new);
        let start = tokio_time::Instant::now();
        let mut config = config();
        config.settle = Duration::from_secs(3_600);
        let bootstrap = spawn_run(endpoint, vec![addr(1), addr(2)], config);

        let request = next_join(&join_requests).await;
        assert_eq!(request.addr, addr(1));
        assert!(request.result_tx.send(Ok(())).is_ok());

        finished(bootstrap).await;
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    /// Rounds run from the first resolution, but the formation decision waits until the view has
    /// settled, however clearly the rounds before allow it.
    #[tokio::test(start_paused = true)]
    async fn bootstrap_forms_only_once_the_view_has_settled() {
        let (endpoint, _lane_dials, join_requests) = endpoint(addr(1), EndpointConfig::new);
        let start = tokio_time::Instant::now();
        let bootstrap = spawn_run(endpoint, vec![addr(1), addr(2)], config());

        let mut rounds = 0;
        while !bootstrap.is_finished() {
            let Ok(request) = timeout(SETTLE * 2, join_requests.recv_async()).await else {
                break;
            };
            let request = request.expect("bootstrap requests joins");
            assert!(!endpoint.formed());
            rounds += 1;
            assert!(
                request
                    .result_tx
                    .send(Err(ConnectError::Refused(RefusalReason::NoCluster)))
                    .is_ok()
            );
        }

        finished(bootstrap).await;
        assert!(endpoint.formed());
        assert!(start.elapsed() >= SETTLE);
        assert!(
            rounds > 1,
            "joins ran before the view settled, {rounds} rounds"
        );
    }

    /// A held round proves that a cluster exists at the seed, so it never reaches the formation
    /// provider, settled view or not.
    #[tokio::test(start_paused = true)]
    async fn a_held_round_postpones_formation() {
        let (endpoint, _lane_dials, join_requests) = endpoint(addr(1), EndpointConfig::new);
        let mut config = config();
        config.settle = Duration::ZERO;
        let bootstrap = spawn_run(endpoint, vec![addr(1), addr(2)], config);

        for _ in 0..5 {
            let request = next_join(&join_requests).await;
            assert!(
                request
                    .result_tx
                    .send(Err(ConnectError::Refused(RefusalReason::JoinsHeld)))
                    .is_ok()
            );
        }
        let request = next_join(&join_requests).await;
        assert!(!endpoint.formed(), "a held round formed a cluster");
        assert!(
            request
                .result_tx
                .send(Err(ConnectError::Refused(RefusalReason::NoCluster)))
                .is_ok()
        );

        finished(bootstrap).await;
        assert!(endpoint.formed());
    }

    /// A held round never forms, but its answer resets the fallback timer of the address which
    /// gave it: the lowest address unavailable again after a minute of holding back joins is
    /// passed over only after a fresh `after`.
    #[tokio::test(start_paused = true)]
    async fn a_held_answer_resets_the_fallback_timer() {
        const AFTER: Duration = Duration::from_secs(60);
        const HELD_FROM: Duration = Duration::from_secs(2);
        const HELD_UNTIL: Duration = Duration::from_secs(63);

        let (endpoint, _lane_dials, join_requests) = endpoint(addr(2), EndpointConfig::new);
        let start = tokio_time::Instant::now();
        let mut config = config();
        config.settle = Duration::ZERO;
        config.formation = Arc::new(|| Box::new(MajorityWithFallback::new(AFTER)));
        let bootstrap = spawn_run(endpoint, vec![addr(1), addr(2), addr(3)], config);

        let mut unavailable_again = None;
        let mut last_answer = Duration::ZERO;
        while !bootstrap.is_finished() {
            let Ok(request) = timeout(RESOLVE_INTERVAL * 5, join_requests.recv_async()).await
            else {
                break;
            };
            let request = request.expect("bootstrap requests joins");
            assert!(!endpoint.formed());
            last_answer = start.elapsed();

            let held = (HELD_FROM..HELD_UNTIL).contains(&last_answer);
            if !held && last_answer >= HELD_UNTIL {
                unavailable_again.get_or_insert(last_answer);
            }
            let answer = match (request.addr == addr(1), held) {
                (true, true) => Err(ConnectError::Refused(RefusalReason::JoinsHeld)),
                (true, false) => Err(ConnectError::Closed),
                (false, _) => Err(ConnectError::Refused(RefusalReason::NoCluster)),
            };
            assert!(request.result_tx.send(answer).is_ok());
        }

        finished(bootstrap).await;
        assert!(endpoint.formed());
        let unavailable_again =
            unavailable_again.expect("the lowest address was unavailable again");
        assert!(
            last_answer >= unavailable_again + AFTER,
            "formed {:?} after the lowest address was unavailable again",
            last_answer - unavailable_again
        );
        assert!(last_answer <= unavailable_again + AFTER + RESOLVE_INTERVAL * 2);
    }

    /// An ambiguous join attempt postpones the formation the provider decided, until its bound
    /// has passed.
    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_join_postpones_formation() {
        let bound = Duration::from_secs(5);
        let (endpoint, _lane_dials, join_requests) = endpoint(addr(1), |addr| {
            let mut config = EndpointConfig::new(addr);
            config.ambiguous_join_bound = bound;
            config
        });
        assert!(endpoint.enter_join());
        endpoint.record_ambiguous_join(addr(2));
        assert!(
            endpoint
                .finish_join(addr(2), Err(ConnectError::Closed))
                .is_err()
        );
        let start = tokio_time::Instant::now();
        let mut config = config();
        config.settle = Duration::ZERO;
        let bootstrap = spawn_run(endpoint, vec![addr(1), addr(2)], config);

        while !bootstrap.is_finished() {
            let Ok(request) = timeout(bound * 2, join_requests.recv_async()).await else {
                break;
            };
            let request = request.expect("bootstrap requests joins");
            assert!(!endpoint.formed());
            assert!(
                request
                    .result_tx
                    .send(Err(ConnectError::Refused(RefusalReason::NoCluster)))
                    .is_ok()
            );
        }

        finished(bootstrap).await;
        assert!(endpoint.formed());
        assert!(start.elapsed() >= bound);
    }

    /// The probe asks every resolved address which is neither this node nor an Up member, and
    /// one answering with another cluster is published as a foreign cluster.
    #[tokio::test(start_paused = true)]
    async fn the_probe_records_a_foreign_cluster() {
        let (endpoint, _lane_dials, _join_requests, probe_requests) =
            EndpointInner::for_tests_with_probes(EndpointConfig::new(addr(1)), 0);
        endpoint.form_cluster().expect("an unformed endpoint forms");
        endpoint.update(|membership, _| membership.add_up(NodeId::new(addr(3))));
        let other = ClusterId::new();
        let seeds = FixedSeeds::new(vec![addr(1), addr(2), addr(3)]);
        let probe = task::spawn(probe_foreign_clusters(endpoint, seeds, RESOLVE_INTERVAL));

        for _ in 0..3 {
            let request = timeout(RESOLVE_INTERVAL * 2, probe_requests.recv_async())
                .await
                .expect("a probe per interval")
                .expect("the probe task requests probes");
            assert_eq!(request.addr, addr(2));
            assert!(request.result_tx.send(Ok(Some(other))).is_ok());
            assert!(
                probe_requests.is_empty(),
                "this node and an Up member are not probed"
            );
        }
        assert_eq!(
            endpoint.state_rx().borrow().foreign_clusters(),
            [ForeignCluster::new(addr(2), other)]
        );

        endpoint.self_down();
        timeout(RESOLVE_INTERVAL * 2, probe)
            .await
            .expect("the probe stops once this node is downed")
            .expect("the probe does not panic");
    }

    /// A zero resolve interval would poll discovery in a busy loop, and a zero probe interval
    /// would probe in one, so both are refused before anything is resolved.
    #[tokio::test]
    async fn a_zero_interval_is_refused() {
        let mut config = BootstrapConfig::new();
        config.resolve_interval = Duration::ZERO;

        assert!(matches!(
            validate(&config),
            Err(InvalidBootstrapConfig::ZeroResolveInterval)
        ));

        let seeds = FixedSeeds::new(vec![addr(1), addr(2)]);
        assert!(
            bootstrap(seeds, config).await.is_err(),
            "bootstrap must refuse the config before it resolves anything"
        );

        let mut config = BootstrapConfig::new();
        config.probe_interval = Some(Duration::ZERO);
        assert!(matches!(
            validate(&config),
            Err(InvalidBootstrapConfig::ZeroProbeInterval)
        ));
        config.probe_interval = None;
        assert!(validate(&config).is_ok());
    }

    /// The documented config form, which a config file provides: every field is optional, the
    /// formation provider is chosen by name and told apart by what it decides, and a `null`
    /// probe interval disables the probe.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<BootstrapConfig>(
            r#"{
                "min_peers": 5,
                "settle": "500ms",
                "resolve_interval": "100ms",
                "formation": "explicit",
                "probe_interval": "10s"
            }"#,
        )
        .expect("the documented config form deserializes");

        assert_eq!(config.min_peers.get(), 5);
        assert_eq!(config.settle, Duration::from_millis(500));
        assert_eq!(config.resolve_interval, Duration::from_millis(100));
        assert_eq!(config.probe_interval, Some(Duration::from_secs(10)));

        let alone = Formation {
            universe: BTreeSet::from([addr(1)]),
            self_addr: addr(1),
            outcomes: vec![(addr(2), JoinOutcome::NoCluster)],
            now: Instant::now(),
        };
        assert_eq!(
            (config.formation)().form(&alone),
            Decision::Decline(Declined::Explicit),
            "the explicit provider never forms"
        );

        let defaults = serde_json::from_str::<BootstrapConfig>("{}").expect("every field defaults");
        assert_eq!(defaults.min_peers, BootstrapConfig::DEFAULT_MIN_PEERS);
        assert_eq!(defaults.settle, BootstrapConfig::DEFAULT_SETTLE);
        assert_eq!(
            defaults.resolve_interval,
            BootstrapConfig::DEFAULT_RESOLVE_INTERVAL
        );
        assert_eq!(
            defaults.probe_interval,
            Some(BootstrapConfig::DEFAULT_PROBE_INTERVAL)
        );
        assert_eq!(
            (defaults.formation)().form(&alone),
            Decision::Form,
            "the default majority provider forms at the lowest address of the universe"
        );

        let without_probe =
            serde_json::from_str::<BootstrapConfig>(r#"{ "probe_interval": null }"#)
                .expect("a null probe interval disables the probe");
        assert_eq!(without_probe.probe_interval, None);

        assert!(
            serde_json::from_str::<BootstrapConfig>(r#"{ "min_pears": 5 }"#).is_err(),
            "a misspelled key must not be a silent default"
        );
        assert!(
            serde_json::from_str::<BootstrapConfig>(r#"{ "resolve_interval": "0s" }"#).is_err()
        );
        assert!(serde_json::from_str::<BootstrapConfig>(r#"{ "probe_interval": "0s" }"#).is_err());
    }

    /// A fixed seed list is a plain sequence of addresses in a config file.
    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn fixed_seeds_deserialize_from_a_sequence() {
        let mut seeds =
            serde_json::from_str::<FixedSeeds>(r#"["127.0.0.1:4242", "127.0.0.2:4242"]"#)
                .expect("a sequence of addresses deserializes");

        assert_eq!(seeds.resolve().await, Ok(vec![addr(1), addr(2)]));
    }

    fn addr(n: u8) -> SocketAddr {
        format!("127.0.0.{n}:4242").parse().expect("valid address")
    }

    fn endpoint<F>(
        self_addr: SocketAddr,
        config: F,
    ) -> (
        &'static EndpointInner,
        flume::Receiver<crate::cluster::peer::DialRequest>,
        flume::Receiver<JoinRequest>,
    )
    where
        F: FnOnce(SocketAddr) -> EndpointConfig,
    {
        EndpointInner::for_tests(config(self_addr))
    }

    fn config() -> BootstrapConfig {
        BootstrapConfig {
            settle: SETTLE,
            resolve_interval: RESOLVE_INTERVAL,
            probe_interval: None,
            ..BootstrapConfig::new()
        }
    }

    fn spawn_run(
        endpoint: &'static EndpointInner,
        seeds: Vec<SocketAddr>,
        config: BootstrapConfig,
    ) -> JoinHandle<()> {
        task::spawn(async move {
            run(endpoint, FixedSeeds::new(seeds), config)
                .await
                .expect("bootstrap succeeds");
        })
    }

    async fn next_join(join_requests: &flume::Receiver<JoinRequest>) -> JoinRequest {
        timeout(SETTLE, join_requests.recv_async())
            .await
            .expect("a join request within the timeout")
            .expect("bootstrap requests joins")
    }

    async fn finished(bootstrap: JoinHandle<()>) {
        timeout(SETTLE, async {
            while !bootstrap.is_finished() {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("bootstrap finishes");
        bootstrap.await.expect("bootstrap does not panic");
    }
}
