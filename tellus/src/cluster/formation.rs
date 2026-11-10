//! Deciding whether this node forms a new cluster when a [bootstrap](fn@crate::cluster::bootstrap)
//! round found none. [FormationProvider] is what
//! [BootstrapConfig::formation](crate::cluster::BootstrapConfig::formation) takes, and the default
//! is [Majority]. Its majority rule stops a downed minority from re-forming inside a partition,
//! and its lowest-address rule makes the forming node unique.

use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    fmt::{self, Debug, Display, Formatter},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

/// Creates the [FormationProvider] for a [bootstrap](fn@crate::cluster::bootstrap) run.
pub type FormationProviderFactory = Arc<dyn Fn() -> Box<dyn FormationProvider> + Send + Sync>;

/// Decides whether this node forms a new cluster, asked by
/// [bootstrap](fn@crate::cluster::bootstrap) once a round of join attempts over a settled universe
/// of at least [min_peers](crate::cluster::BootstrapConfig::min_peers) addresses has been admitted
/// nowhere; the default is [Majority].
///
/// A started endpoint is not a cluster, so forming one is a decision, and the wrong one splits
/// the cluster in two. A node forming while it is merely partitioned from the others comes back
/// as a cluster of its own, and clusters never merge. See docs/cluster.md.
pub trait FormationProvider
where
    Self: Send + 'static,
{
    /// Takes note of what a round of join attempts saw, for a rule which depends on how long an
    /// outcome has lasted. Bootstrap calls this on every round which did not admit this node, in
    /// the order of the rounds. That includes the rounds which must not form, e.g. one over a
    /// view which has not settled or one a cluster answered by holding back joins. On a round
    /// which is a formation decision, it is called before [form](FormationProvider::form). Does
    /// nothing by default.
    fn observe(&mut self, _: &Formation) {}

    /// Whether this node forms a cluster now, and if not, why not. Bootstrap calls this only on a
    /// round over a settled universe of at least
    /// [min_peers](crate::cluster::BootstrapConfig::min_peers) addresses which no seed admitted,
    /// after [observe](FormationProvider::observe) for the same round. Bootstrap logs a decline
    /// at warn level when it or the universe differs from the previous decline, at debug level
    /// otherwise.
    fn form(&mut self, formation: &Formation) -> Decision;
}

/// The answer of a [FormationProvider].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// This node forms a cluster now.
    Form,

    /// This node does not form a cluster now, for the given reason.
    Decline(Declined),
}

/// Why this node does not form a cluster now.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Declined {
    /// Another address is the one which may form: the lowest of the universe for [Majority] and
    /// [Unanimous], the lowest not passed over for [MajorityWithFallback].
    NotLowest {
        /// The address which may form.
        lowest: SocketAddr,
    },

    /// Fewer addresses of the universe than the provider's rule requires are known to be no
    /// cluster: a strict majority for [Majority] and [MajorityWithFallback], every one for
    /// [Unanimous].
    NoMajority {
        /// How many addresses of the universe are known to be no cluster, this node included.
        no_cluster: usize,

        /// The size of the universe.
        universe: usize,
    },

    /// The provider never forms, see [Explicit].
    Explicit,

    /// A join attempt via this address ended without proof whether it admitted this node, so the
    /// endpoint refuses to form until the address answers definitively, leaves the universe or
    /// [ambiguous_join_bound](crate::cluster::EndpointConfig::ambiguous_join_bound) passes.
    /// Reported by bootstrap rather than by a provider.
    Ambiguous {
        /// The address of the ambiguous join attempt.
        addr: SocketAddr,
    },

    /// A reason of a provider outside tellus.
    Other(Cow<'static, str>),
}

impl Display for Declined {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLowest { lowest } => write!(f, "{lowest} is the address which may form"),

            Self::NoMajority {
                no_cluster,
                universe,
            } => write!(
                f,
                "{no_cluster} of {universe} addresses are known to be no cluster"
            ),

            Self::Explicit => write!(f, "formation is explicit"),

            Self::Ambiguous { addr } => {
                write!(f, "the join attempt via {addr} may have admitted this node")
            }

            Self::Other(reason) => write!(f, "{reason}"),
        }
    }
}

/// What one round of join attempts saw, next to the addresses it was taken over.
#[derive(Debug, Clone)]
pub struct Formation {
    /// Every address discovery resolved, this node's own included: the denominator of any
    /// counting rule and the set the lowest address is taken from. Deciding on the universe
    /// rather than on who answered is what keeps a partitioned node from forming.
    pub universe: BTreeSet<SocketAddr>,

    /// This node's own address.
    pub self_addr: SocketAddr,

    /// What each attempted address answered. Addresses of the universe which were not attempted
    /// are absent, e.g. while this node is pinned to a cluster or after an address answered
    /// [Held](JoinOutcome::Held), so a rule must count against the universe rather than against
    /// this.
    pub outcomes: Vec<(SocketAddr, JoinOutcome)>,

    /// When the round ended, for a rule which depends on how long an outcome has lasted.
    pub now: Instant,
}

impl Formation {
    /// Whether this node holds the lowest address of the universe.
    pub fn is_lowest(&self) -> bool {
        self.universe.first() == Some(&self.self_addr)
    }

    /// How many addresses of the universe are known to be no cluster, this node included, since
    /// a node running bootstrap is none itself.
    pub fn no_cluster_count(&self) -> usize {
        1 + self
            .outcomes
            .iter()
            .filter(|(_, outcome)| *outcome == JoinOutcome::NoCluster)
            .count()
    }

    fn lowest_decline(&self) -> Option<Declined> {
        match self.universe.first() {
            Some(lowest) if *lowest != self.self_addr => {
                Some(Declined::NotLowest { lowest: *lowest })
            }

            _ => None,
        }
    }

    fn no_majority(&self) -> Declined {
        Declined::NoMajority {
            no_cluster: self.no_cluster_count(),
            universe: self.universe.len(),
        }
    }
}

/// What one join attempt against one address yielded, short of admitting this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOutcome {
    /// It answered that it is a member of no cluster, so it counts toward forming one.
    NoCluster,

    /// It answered that it is a member of a cluster which holds back joins: one of its members is
    /// unreachable, or the answering member is leaving. A cluster exists there. The round ends
    /// with this answer and does not form.
    Held,

    /// It did not answer, or answered something which is not an admission: unreachable, but also
    /// a refused identity, a protocol mismatch, an undecodable frame or a member of another
    /// cluster. Nothing about whether a cluster this node may join exists there follows from it.
    Unavailable,
}

/// The default [FormationProvider]: forms once a strict majority of the universe, this node
/// included, is known to be no cluster, and only at the lowest address of the universe.
///
/// The lowest address makes the forming node unique, so nodes deciding on the same universe cannot
/// form two clusters. The majority is what keeps a partitioned minority from forming one at all,
/// which is the failure this exists for: its nodes are downed, restart, and would otherwise come
/// back as a cluster of their own. Both conditions are on the universe rather than on who answered,
/// so a node which cannot reach the lowest address forms nothing; formation liveness is hence a
/// requirement on discovery, to stop resolving nodes which are gone.
#[derive(Debug, Default)]
pub struct Majority;

impl FormationProvider for Majority {
    fn form(&mut self, formation: &Formation) -> Decision {
        if let Some(declined) = formation.lowest_decline() {
            return Decision::Decline(declined);
        }
        if formation.no_cluster_count() * 2 <= formation.universe.len() {
            return Decision::Decline(formation.no_majority());
        }

        Decision::Form
    }
}

/// The opt-in [FormationProvider] trading uniqueness for liveness. Like [Majority] it forms only
/// once a strict majority of the universe, this node included, is known to be no cluster. But the
/// address which may form is the lowest one of the universe minus every address which has
/// answered nothing but [Unavailable](JoinOutcome::Unavailable) for at least `after`.
///
/// An address's timer starts with the first round it is unavailable in. Any other answer, or a
/// round which did not attempt it, resets the timer. The timers follow every round passed to
/// [observe](FormationProvider::observe), including the rounds which must not form. A
/// [Held](JoinOutcome::Held) answer hence resets the timer, and the next unavailable round starts
/// a fresh `after`. A lowest address which is gone, but still resolved by discovery, no longer
/// blocks a cold start forever. The price is uniqueness: a lowest address which is merely
/// unreachable from this node, not gone, may form a cluster of its own beside the one this node
/// forms. The two clusters never merge. Every member reports the other one in
/// [foreign_clusters](crate::cluster::ClusterState::foreign_clusters) as soon as bootstrap's probe
/// reaches it, see docs/cluster.md. The default remains [Majority].
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct MajorityWithFallback {
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    after: Duration,

    #[cfg_attr(feature = "serde", serde(skip))]
    unavailable_since: HashMap<SocketAddr, Instant>,
}

impl MajorityWithFallback {
    /// A provider passing over an address once it has been continuously unavailable for the given
    /// duration.
    pub fn new(after: Duration) -> Self {
        Self {
            after,
            unavailable_since: HashMap::new(),
        }
    }

    fn passed_over(&self, addr: SocketAddr, now: Instant) -> bool {
        self.unavailable_since
            .get(&addr)
            .is_some_and(|since| now.saturating_duration_since(*since) >= self.after)
    }
}

impl FormationProvider for MajorityWithFallback {
    fn observe(&mut self, formation: &Formation) {
        let unavailable = formation
            .outcomes
            .iter()
            .filter(|(addr, outcome)| {
                *outcome == JoinOutcome::Unavailable && formation.universe.contains(addr)
            })
            .map(|(addr, _)| *addr)
            .collect::<BTreeSet<_>>();
        self.unavailable_since
            .retain(|addr, _| unavailable.contains(addr));
        for addr in unavailable {
            self.unavailable_since.entry(addr).or_insert(formation.now);
        }
    }

    fn form(&mut self, formation: &Formation) -> Decision {
        let lowest =
            formation.universe.iter().copied().find(|addr| {
                *addr == formation.self_addr || !self.passed_over(*addr, formation.now)
            });
        if let Some(lowest) = lowest
            && lowest != formation.self_addr
        {
            return Decision::Decline(Declined::NotLowest { lowest });
        }
        if formation.no_cluster_count() * 2 <= formation.universe.len() {
            return Decision::Decline(formation.no_majority());
        }

        Decision::Form
    }
}

/// The strict [FormationProvider]: forms only once every address of the universe is known to be
/// no cluster, and only at the lowest of them. Any one unreachable address blocks a cold start,
/// so no cluster is formed while any part of the universe is unaccounted for.
#[derive(Debug, Default)]
pub struct Unanimous;

impl FormationProvider for Unanimous {
    fn form(&mut self, formation: &Formation) -> Decision {
        if let Some(declined) = formation.lowest_decline() {
            return Decision::Decline(declined);
        }
        if formation.no_cluster_count() < formation.universe.len() {
            return Decision::Decline(formation.no_majority());
        }

        Decision::Form
    }
}

/// The [FormationProvider] which never forms: the cluster comes into being through
/// [form](crate::cluster::form), called by the deployment, and bootstrap only ever joins it.
#[derive(Debug, Default)]
pub struct Explicit;

impl FormationProvider for Explicit {
    fn form(&mut self, _: &Formation) -> Decision {
        Decision::Decline(Declined::Explicit)
    }
}

#[cfg(feature = "serde")]
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FormationProviderConfig {
    Majority,
    MajorityWithFallback(MajorityWithFallback),
    Unanimous,
    Explicit,
}

#[cfg(feature = "serde")]
impl FormationProviderConfig {
    pub(crate) fn factory(self) -> FormationProviderFactory {
        match self {
            Self::Majority => Arc::new(|| Box::new(Majority)),

            Self::MajorityWithFallback(provider) => {
                let after = provider.after;
                Arc::new(move || Box::new(MajorityWithFallback::new(after)))
            }

            Self::Unanimous => Arc::new(|| Box::new(Unanimous)),

            Self::Explicit => Arc::new(|| Box::new(Explicit)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::formation::FormationProviderConfig;
    use crate::cluster::formation::{
        Decision, Declined, Explicit, Formation, FormationProvider, JoinOutcome, Majority,
        MajorityWithFallback, Unanimous,
    };
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        time::{Duration, Instant},
    };

    const AFTER: Duration = Duration::from_secs(60);

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn formation(self_port: u16, ports: &[u16], outcomes: &[(u16, JoinOutcome)]) -> Formation {
        formation_at(self_port, ports, outcomes, Instant::now())
    }

    fn formation_at(
        self_port: u16,
        ports: &[u16],
        outcomes: &[(u16, JoinOutcome)],
        now: Instant,
    ) -> Formation {
        let mut universe = ports
            .iter()
            .map(|port| addr(*port))
            .collect::<BTreeSet<_>>();
        universe.insert(addr(self_port));

        Formation {
            universe,
            self_addr: addr(self_port),
            outcomes: outcomes
                .iter()
                .map(|(port, outcome)| (addr(*port), *outcome))
                .collect(),
            now,
        }
    }

    fn observed_decision(provider: &mut dyn FormationProvider, formation: &Formation) -> Decision {
        provider.observe(formation);
        provider.form(formation)
    }

    #[test]
    fn majority_forms_at_the_lowest_address_of_a_settled_universe() {
        let formation = formation(
            1,
            &[1, 2, 3, 4, 5],
            &[
                (2, JoinOutcome::NoCluster),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert_eq!(Majority.form(&formation), Decision::Form);
    }

    /// The whole point: the minority side of a partition, restarted, must not come back as a
    /// cluster of its own, however completely it can see itself.
    #[test]
    fn majority_refuses_a_partitioned_minority() {
        let formation = formation(
            1,
            &[1, 2, 3, 4, 5],
            &[
                (2, JoinOutcome::NoCluster),
                (3, JoinOutcome::Unavailable),
                (4, JoinOutcome::Unavailable),
                (5, JoinOutcome::Unavailable),
            ],
        );

        assert_eq!(
            Majority.form(&formation),
            Decision::Decline(Declined::NoMajority {
                no_cluster: 2,
                universe: 5
            })
        );
    }

    /// Only one node may form, else two of the same majority form two clusters.
    #[test]
    fn majority_refuses_every_address_but_the_lowest() {
        let formation = formation(
            2,
            &[1, 2, 3, 4, 5],
            &[
                (1, JoinOutcome::NoCluster),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert_eq!(
            Majority.form(&formation),
            Decision::Decline(Declined::NotLowest { lowest: addr(1) })
        );
    }

    /// The lowest address is taken from the universe, not from who answered, so a node which
    /// cannot reach it waits rather than electing itself.
    #[test]
    fn majority_refuses_while_the_lowest_address_is_unavailable() {
        let formation = formation(
            2,
            &[1, 2, 3, 4, 5],
            &[
                (1, JoinOutcome::Unavailable),
                (3, JoinOutcome::NoCluster),
                (4, JoinOutcome::NoCluster),
                (5, JoinOutcome::NoCluster),
            ],
        );

        assert_eq!(
            Majority.form(&formation),
            Decision::Decline(Declined::NotLowest { lowest: addr(1) })
        );
    }

    /// A round with one `NoCluster` answer cannot reach a majority of a universe of five.
    #[test]
    fn majority_refuses_a_round_which_asked_one_address() {
        let formation = formation(1, &[1, 2, 3, 4, 5], &[(2, JoinOutcome::NoCluster)]);

        assert!(matches!(
            Majority.form(&formation),
            Decision::Decline(Declined::NoMajority { .. })
        ));
    }

    /// A discovery resolving nothing but this node is a cluster of one, deliberately.
    #[test]
    fn majority_forms_a_single_node_cluster() {
        let formation = formation(1, &[], &[]);

        assert_eq!(Majority.form(&formation), Decision::Form);
    }

    /// Discovery need not resolve this node's own address, so the universe adds it.
    #[test]
    fn majority_counts_a_universe_the_local_address_is_missing_from() {
        let formation = formation(1, &[2, 3], &[(2, JoinOutcome::NoCluster)]);

        assert_eq!(Majority.form(&formation), Decision::Form);
    }

    /// The fallback over `now`: the lowest address blocks until it has been unavailable for the
    /// whole `after`, an answer or a round which did not attempt it restarts its timer, and the
    /// strict majority holds throughout.
    #[test]
    fn majority_with_fallback_passes_over_an_address_unavailable_for_long_enough() {
        let start = Instant::now();
        let mut provider = MajorityWithFallback::new(AFTER);
        let lowest_unavailable = [
            (1, JoinOutcome::Unavailable),
            (3, JoinOutcome::NoCluster),
            (4, JoinOutcome::NoCluster),
        ];
        let universe = [1, 2, 3, 4];
        let not_lowest = Decision::Decline(Declined::NotLowest { lowest: addr(1) });

        let decide = |provider: &mut MajorityWithFallback, outcomes: &[_], at| {
            observed_decision(provider, &formation_at(2, &universe, outcomes, start + at))
        };

        assert_eq!(
            decide(&mut provider, &lowest_unavailable, Duration::ZERO),
            not_lowest
        );
        assert_eq!(
            decide(&mut provider, &lowest_unavailable, AFTER / 2),
            not_lowest
        );
        assert_eq!(
            decide(&mut provider, &lowest_unavailable, AFTER),
            Decision::Form
        );

        let lowest_answered = [
            (1, JoinOutcome::NoCluster),
            (3, JoinOutcome::NoCluster),
            (4, JoinOutcome::NoCluster),
        ];
        assert_eq!(
            decide(&mut provider, &lowest_answered, AFTER * 2),
            not_lowest
        );
        assert_eq!(
            decide(&mut provider, &lowest_unavailable, AFTER * 3),
            not_lowest
        );
        assert_eq!(
            decide(
                &mut provider,
                &lowest_unavailable,
                AFTER * 4 - Duration::from_secs(1)
            ),
            not_lowest
        );
        assert_eq!(
            decide(&mut provider, &lowest_unavailable, AFTER * 4),
            Decision::Form
        );

        let lowest_not_attempted = [(3, JoinOutcome::NoCluster), (4, JoinOutcome::NoCluster)];
        assert_eq!(
            decide(&mut provider, &lowest_not_attempted, AFTER * 5),
            not_lowest
        );
        assert_eq!(
            decide(&mut provider, &lowest_unavailable, AFTER * 5),
            not_lowest
        );

        let no_majority = [
            (1, JoinOutcome::Unavailable),
            (3, JoinOutcome::NoCluster),
            (4, JoinOutcome::Unavailable),
        ];
        assert_eq!(
            decide(&mut provider, &no_majority, AFTER * 7),
            Decision::Decline(Declined::NoMajority {
                no_cluster: 2,
                universe: 4
            })
        );
    }

    /// A round which must not form still counts: a held answer resets the timer, so the lowest
    /// address unavailable again after more than `after` of held rounds is passed over only after
    /// a fresh `after`.
    #[test]
    fn majority_with_fallback_restarts_the_timer_after_a_held_answer() {
        let start = Instant::now();
        let mut provider = MajorityWithFallback::new(AFTER);
        let universe = [1, 2, 3];
        let unavailable = [(1, JoinOutcome::Unavailable), (3, JoinOutcome::NoCluster)];
        let not_lowest = Decision::Decline(Declined::NotLowest { lowest: addr(1) });

        assert_eq!(
            observed_decision(
                &mut provider,
                &formation_at(2, &universe, &unavailable, start)
            ),
            not_lowest
        );
        for second in 1..=61 {
            let held = [(1, JoinOutcome::Held)];
            provider.observe(&formation_at(
                2,
                &universe,
                &held,
                start + Duration::from_secs(second),
            ));
        }

        let again = start + Duration::from_secs(62);
        assert_eq!(
            observed_decision(
                &mut provider,
                &formation_at(2, &universe, &unavailable, again)
            ),
            not_lowest
        );
        assert_eq!(
            observed_decision(
                &mut provider,
                &formation_at(
                    2,
                    &universe,
                    &unavailable,
                    again + AFTER - Duration::from_secs(1)
                )
            ),
            not_lowest
        );
        assert_eq!(
            observed_decision(
                &mut provider,
                &formation_at(2, &universe, &unavailable, again + AFTER)
            ),
            Decision::Form
        );
    }

    /// Passing over an address never makes a node lowest which is not: an address below it that
    /// keeps answering still blocks it.
    #[test]
    fn majority_with_fallback_keeps_an_answering_lower_address() {
        let start = Instant::now();
        let mut provider = MajorityWithFallback::new(AFTER);
        let outcomes = [
            (1, JoinOutcome::Unavailable),
            (2, JoinOutcome::NoCluster),
            (4, JoinOutcome::NoCluster),
        ];

        assert!(matches!(
            observed_decision(
                &mut provider,
                &formation_at(3, &[1, 2, 3, 4], &outcomes, start)
            ),
            Decision::Decline(_)
        ));
        assert_eq!(
            observed_decision(
                &mut provider,
                &formation_at(3, &[1, 2, 3, 4], &outcomes, start + AFTER)
            ),
            Decision::Decline(Declined::NotLowest { lowest: addr(2) })
        );
    }

    #[test]
    fn unanimous_forms_only_once_every_address_answered() {
        let all = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::NoCluster)],
        );
        let one_missing = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::Unavailable)],
        );

        assert_eq!(Unanimous.form(&all), Decision::Form);
        assert_eq!(
            Unanimous.form(&one_missing),
            Decision::Decline(Declined::NoMajority {
                no_cluster: 2,
                universe: 3
            })
        );
    }

    #[test]
    fn explicit_never_forms() {
        let formation = formation(1, &[], &[]);

        assert_eq!(
            Explicit.form(&formation),
            Decision::Decline(Declined::Explicit)
        );
    }

    /// The provider is chosen by name in a config file, the fallback with its duration; the built
    /// providers are told apart by what they decide, since all of them are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_formation_provider_is_selected_by_name() {
        let majority = formation(
            1,
            &[1, 2, 3],
            &[(2, JoinOutcome::NoCluster), (3, JoinOutcome::Unavailable)],
        );

        let config = serde_json::from_str::<FormationProviderConfig>(r#""majority""#)
            .expect("the majority provider is valid");
        assert_eq!(config.factory()().form(&majority), Decision::Form);

        let config = serde_json::from_str::<FormationProviderConfig>(r#""unanimous""#)
            .expect("the unanimous provider is valid");
        assert!(matches!(
            config.factory()().form(&majority),
            Decision::Decline(_)
        ));

        let config = serde_json::from_str::<FormationProviderConfig>(r#""explicit""#)
            .expect("the explicit provider is valid");
        assert!(matches!(
            config.factory()().form(&majority),
            Decision::Decline(Declined::Explicit)
        ));

        assert!(serde_json::from_str::<FormationProviderConfig>(r#""majorty""#).is_err());
    }

    /// The fallback's documented form names its duration, which is required; a factory hands out
    /// a provider without history, so its timers start with the first round.
    #[cfg(feature = "serde")]
    #[test]
    fn majority_with_fallback_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<FormationProviderConfig>(
            r#"{ "majority_with_fallback": { "after": "60s" } }"#,
        )
        .expect("the fallback provider is valid");
        let FormationProviderConfig::MajorityWithFallback(provider) = &config else {
            panic!("expected the fallback provider, got {config:?}");
        };
        assert_eq!(provider.after, AFTER);

        let start = Instant::now();
        let mut provider = config.factory()();
        let outcomes = [(1, JoinOutcome::Unavailable), (3, JoinOutcome::NoCluster)];
        assert!(matches!(
            observed_decision(
                provider.as_mut(),
                &formation_at(2, &[1, 2, 3], &outcomes, start)
            ),
            Decision::Decline(Declined::NotLowest { .. })
        ));
        assert_eq!(
            observed_decision(
                provider.as_mut(),
                &formation_at(2, &[1, 2, 3], &outcomes, start + AFTER)
            ),
            Decision::Form
        );

        assert!(
            serde_json::from_str::<FormationProviderConfig>(r#"{ "majority_with_fallback": {} }"#)
                .is_err(),
            "the fallback duration is required"
        );
        assert!(
            serde_json::from_str::<FormationProviderConfig>(
                r#"{ "majority_with_fallback": { "after": "60s", "aftr": "1s" } }"#
            )
            .is_err(),
            "a misspelled key must not be a silent default"
        );
    }
}
