//! Deciding when an unreachable member is dead for good: [DowningProvider] is what
//! [EndpointConfig::downing_provider](crate::cluster::EndpointConfig::downing_provider) takes, the
//! default is [KeepMajority], which resolves a partition towards its majority side.

use crate::cluster::membership::{Member, MemberState};
use std::{
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

/// Creates the [DowningProvider] for the endpoint.
pub type DowningProviderFactory = Arc<dyn Fn() -> Box<dyn DowningProvider> + Send + Sync>;

/// Decides when an unreachable member is downed, polled once per heartbeat interval; the default
/// is [KeepMajority]. A provider sees the whole membership, so it can resolve a split brain by
/// downing this node rather than the members it cannot reach; see docs/cluster.md on what a
/// partition does to the guarantees either way.
pub trait DowningProvider
where
    Self: Send + 'static,
{
    /// The verdict for now, given all members including this node, the ones outside its
    /// reachability component and the clock.
    fn down(&mut self, members: &[Member], disconnected: Disconnected<'_>, at: Instant) -> Downing;
}

/// The members outside this node's connected reachability component, each with the instant it left
/// it. Not a direct unreachability mark: a broken direct link leaves a member here only if no path
/// through another member remains, so an alternate route keeps it out of a [DowningProvider]'s
/// view.
#[derive(Debug, Clone, Copy)]
pub struct Disconnected<'a>(&'a [(Member, Instant)]);

impl<'a> Disconnected<'a> {
    /// The members outside the component, e.g. to drive a [DowningProvider] from a test.
    pub fn new(members: &'a [(Member, Instant)]) -> Self {
        Self(members)
    }
}

impl Deref for Disconnected<'_> {
    type Target = [(Member, Instant)];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

/// A [DowningProvider]'s verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Downing {
    /// Down these members; empty means nothing is downed now. Naming this node is a
    /// [SelfDown](Downing::SelfDown).
    Members(Vec<Member>),

    /// Down this node: it severs every connection and only a restarted process, with a fresh
    /// incarnation, can rejoin. The side of a partition which gives way says this.
    SelfDown,
}

/// The default [DowningProvider], resolving a split brain. Once every unreachable member has been
/// unreachable for the given duration, the side seeing a strict majority of the Up members downs
/// the others and any other side downs itself. An even split goes to the side holding the member
/// with the lowest address. At most one side of a partition survives, provided the member lists
/// agreed before it: two sides which never gossiped a new member to each other can both count
/// themselves the majority. A partition into more than two sides can leave none, when no side
/// holds a strict majority, nor exactly half including the lowest address.
///
/// In a cluster of two every failure is an even split, so the lower address survives and the
/// higher one downs itself, whichever of them actually failed: when the lower one crashes, the
/// cluster is gone. Two nodes cannot tell a crash from a partition, so a two node deployment
/// which must survive either one failing needs a third member.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(default, deny_unknown_fields)
)]
pub struct KeepMajority {
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    after: Duration,
}

impl KeepMajority {
    /// The `after` of [KeepMajority::default].
    pub const DEFAULT_AFTER: Duration = Duration::from_secs(10);

    /// A provider deciding once every unreachable member has been unreachable for the given
    /// duration; a member falling silent or answering again postpones the decision, so a flapping
    /// link is waited out rather than resolved.
    pub fn new(after: Duration) -> Self {
        Self { after }
    }
}

impl Default for KeepMajority {
    fn default() -> Self {
        Self::new(Self::DEFAULT_AFTER)
    }
}

impl DowningProvider for KeepMajority {
    fn down(&mut self, members: &[Member], disconnected: Disconnected<'_>, at: Instant) -> Downing {
        let (unreachable, since) = disconnected
            .iter()
            .filter(|(member, _)| member.state() == MemberState::Up)
            .copied()
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let settled = !unreachable.is_empty()
            && since
                .iter()
                .all(|since| at.duration_since(*since) >= self.after);
        if !settled {
            return Downing::Members(Vec::new());
        }

        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .collect::<Vec<_>>();
        let reachable = up
            .iter()
            .filter(|member| !unreachable.contains(member))
            .count();
        let holds_lowest = up
            .iter()
            .min_by_key(|member| member.addr())
            .is_some_and(|member| !unreachable.contains(member));

        if reachable * 2 > up.len() || (reachable * 2 == up.len() && holds_lowest) {
            Downing::Members(unreachable)
        } else {
            Downing::SelfDown
        }
    }
}

/// A [DowningProvider] downing every member unreachable for the given duration. Deliberately
/// simple and unilateral: during a network partition each side downs the other and one cluster
/// becomes two, each of them satisfying its own synthesized signals. Not a production choice, but
/// a development and testing one, where downing on nothing but a deadline is what makes a node
/// death reproducible; the default is [KeepMajority].
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct DownAfterDeadline {
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
    after: Duration,
}

impl DownAfterDeadline {
    /// A provider downing members unreachable for the given duration.
    pub fn new(after: Duration) -> Self {
        Self { after }
    }
}

impl DowningProvider for DownAfterDeadline {
    fn down(
        &mut self,
        _members: &[Member],
        disconnected: Disconnected<'_>,
        at: Instant,
    ) -> Downing {
        Downing::Members(
            disconnected
                .iter()
                .filter(|(_, since)| at.duration_since(*since) >= self.after)
                .map(|(member, _)| *member)
                .collect(),
        )
    }
}

#[cfg(feature = "serde")]
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DowningProviderConfig {
    KeepMajority(KeepMajority),
    DownAfterDeadline(DownAfterDeadline),
}

#[cfg(feature = "serde")]
impl DowningProviderConfig {
    pub(crate) fn factory(self) -> DowningProviderFactory {
        match self {
            Self::KeepMajority(provider) => Arc::new(move || Box::new(provider)),
            Self::DownAfterDeadline(provider) => Arc::new(move || Box::new(provider)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::downing::DowningProviderConfig;
    use crate::cluster::{
        downing::{Disconnected, DownAfterDeadline, Downing, DowningProvider, KeepMajority},
        membership::{Member, MemberState},
        node::NodeId,
    };
    use std::{
        net::SocketAddr,
        time::{Duration, Instant},
    };

    const AFTER: Duration = Duration::from_secs(10);

    fn member(port: u16, state: MemberState) -> Member {
        let addr = format!("127.0.0.1:{port}").parse::<SocketAddr>();
        Member::new(NodeId::new(addr.expect("valid address")), state)
    }

    /// A member which is already Down is filtered out of the unreachable set, so a mark left over
    /// from before it was downed cannot drive a second decision.
    #[test]
    fn keep_majority_ignores_members_which_are_already_down() {
        let mut provider = KeepMajority::new(AFTER);
        let members = [
            member(1, MemberState::Up),
            member(2, MemberState::Up),
            member(3, MemberState::Down),
        ];
        let since = Instant::now();

        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since)]),
                since + AFTER
            ),
            Downing::Members(vec![])
        );
    }

    /// The decision waits for the whole unreachable set, so a member falling silent while another
    /// one is still running out postpones it instead of deciding on a view which is still moving.
    #[test]
    fn keep_majority_decides_once_every_member_reaches_the_deadline() {
        let mut provider = KeepMajority::new(AFTER);
        let members = [
            member(1, MemberState::Up),
            member(2, MemberState::Up),
            member(3, MemberState::Up),
        ];
        let since = Instant::now();

        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since)]),
                since + AFTER - Duration::from_secs(1)
            ),
            Downing::Members(vec![])
        );
        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since), (members[1], since + AFTER)]),
                since + AFTER
            ),
            Downing::Members(vec![])
        );
        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since)]),
                since + AFTER
            ),
            Downing::Members(vec![members[2]])
        );
    }

    /// A strict majority downs what it cannot reach; the minority downs itself, which is what
    /// keeps a partition from becoming two clusters.
    #[test]
    fn keep_majority_downs_the_minority_and_self_downs_in_it() {
        let mut provider = KeepMajority::new(AFTER);
        let members = [
            member(1, MemberState::Up),
            member(2, MemberState::Up),
            member(3, MemberState::Up),
        ];
        let since = Instant::now();
        let at = since + AFTER;

        assert_eq!(
            provider.down(&members, Disconnected::new(&[(members[2], since)]), at),
            Downing::Members(vec![members[2]])
        );
        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[1], since), (members[2], since)]),
                at
            ),
            Downing::SelfDown
        );
    }

    /// An even split has no majority, so the member with the lowest address decides which half
    /// survives; both halves apply the rule to the same member list and reach opposite verdicts.
    #[test]
    fn keep_majority_resolves_an_even_split_by_the_lowest_address() {
        let mut provider = KeepMajority::new(AFTER);
        let members = [
            member(1, MemberState::Up),
            member(2, MemberState::Up),
            member(3, MemberState::Up),
            member(4, MemberState::Up),
        ];
        let since = Instant::now();
        let at = since + AFTER;

        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since), (members[3], since)]),
                at
            ),
            Downing::Members(vec![members[2], members[3]])
        );
        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[0], since), (members[1], since)]),
                at
            ),
            Downing::SelfDown
        );
    }

    /// Down members are tombstones, not votes: counting them would let a cluster which outlived
    /// half of itself find a majority in the dead.
    #[test]
    fn keep_majority_counts_up_members_only() {
        let mut provider = KeepMajority::new(AFTER);
        let members = [
            member(1, MemberState::Down),
            member(2, MemberState::Up),
            member(3, MemberState::Up),
            member(4, MemberState::Up),
        ];
        let since = Instant::now();

        assert_eq!(
            provider.down(
                &members,
                Disconnected::new(&[(members[2], since), (members[3], since)]),
                since + AFTER
            ),
            Downing::SelfDown
        );
    }

    /// The deadline provider downs a member once it has been unreachable for the deadline, and no
    /// earlier.
    #[test]
    fn down_after_deadline_downs_at_the_deadline() {
        let mut provider = DownAfterDeadline::new(AFTER);
        let member = member(2, MemberState::Up);
        let since = Instant::now();

        assert_eq!(
            provider.down(
                &[member],
                Disconnected::new(&[(member, since)]),
                since + AFTER - Duration::from_secs(1)
            ),
            Downing::Members(vec![])
        );
        assert_eq!(
            provider.down(
                &[member],
                Disconnected::new(&[(member, since)]),
                since + AFTER
            ),
            Downing::Members(vec![member])
        );
    }

    /// The provider is chosen by name in a config file, each variant carrying its own deadline;
    /// the built providers are told apart by what they decide, since both are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_downing_provider_is_selected_by_name() {
        let members = [
            member(1, MemberState::Up),
            member(2, MemberState::Up),
            member(3, MemberState::Up),
        ];
        let since = Instant::now();
        let disconnected = [(members[0], since), (members[1], since)];
        let at = since + AFTER;

        let config = serde_json::from_str::<DowningProviderConfig>(
            r#"{ "keep_majority": { "after": "10s" } }"#,
        )
        .expect("the keep majority provider is valid");
        let mut provider = config.factory()();
        assert_eq!(
            provider.down(&members, Disconnected::new(&disconnected), at),
            Downing::SelfDown
        );

        let config = serde_json::from_str::<DowningProviderConfig>(
            r#"{ "down_after_deadline": { "after": "10s" } }"#,
        )
        .expect("the down after deadline provider is valid");
        let mut provider = config.factory()();
        assert_eq!(
            provider.down(&members, Disconnected::new(&disconnected), at),
            Downing::Members(vec![members[0], members[1]])
        );

        let config = serde_json::from_str::<DowningProviderConfig>(r#"{ "keep_majority": {} }"#)
            .expect("the deadline defaults");
        assert!(matches!(
            config,
            DowningProviderConfig::KeepMajority(provider)
                if provider.after == KeepMajority::DEFAULT_AFTER
        ));

        assert!(
            serde_json::from_str::<DowningProviderConfig>(
                r#"{ "keep_majority": { "atfer": "10s" } }"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<DowningProviderConfig>(r#"{ "down_after_deadline": {} }"#)
                .is_err(),
            "the deliberate test provider must not default its deadline"
        );
    }
}
