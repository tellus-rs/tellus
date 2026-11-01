use crate::{
    cluster::{
        membership::{Member, MemberState},
        node::NodeId,
        state::BrokenLink,
    },
    sync::lock,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// One observer's latest statement about one direct peer. Versions are owned by the observer;
/// relays only merge and forward them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireReachability {
    pub(crate) observer: NodeId,
    pub(crate) subject: NodeId,
    pub(crate) version: u64,
    pub(crate) reachable: bool,
}

impl WireReachability {
    /// Two maximum-width IPv6 node identities plus the version and state, pinned by a test.
    const MAX_ENCODED_LEN: usize = 96;
    const ENVELOPE_LEN: usize = 16;
}

pub(crate) const MIN_FRAME_SIZE: usize =
    WireReachability::MAX_ENCODED_LEN + WireReachability::ENVELOPE_LEN;

/// Records parked for a membership which has not caught up yet, bounding what an ill-behaved peer
/// can make this node hold.
const MAX_PENDING: usize = 4_096;

pub(crate) struct Reachability {
    node: NodeId,
    next_version: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    observations: HashMap<(NodeId, NodeId), WireReachability>,
    pending: HashMap<(NodeId, NodeId), Parked>,
    forward: HashMap<(NodeId, NodeId), WireReachability>,
    unreachable_since: HashMap<NodeId, Instant>,
}

impl State {
    /// The version alone decides admission, so a relay never reorders an observer.
    fn admit(&mut self, record: WireReachability) -> bool {
        let key = (record.observer, record.subject);
        if self
            .observations
            .get(&key)
            .is_some_and(|current| current.version >= record.version)
        {
            return false;
        }

        self.observations.insert(key, record);
        self.forward.insert(key, record);
        true
    }

    /// Capped, since a peer can name any node: past the cap a new edge is dropped rather than
    /// evicting one which is only waiting for gossip.
    fn park(&mut self, record: WireReachability) {
        let key = (record.observer, record.subject);
        let parked = Parked {
            record,
            since: Instant::now(),
        };
        match self.pending.get(&key).map(|current| current.record.version) {
            Some(version) if version < record.version => {
                self.pending.insert(key, parked);
            }

            Some(_) => {}

            None if self.pending.len() < MAX_PENDING => {
                self.pending.insert(key, parked);
            }

            None => {}
        }
    }
}

impl Reachability {
    pub(crate) fn new(node: NodeId) -> Self {
        Self {
            node,
            next_version: AtomicU64::new(1),
            state: Mutex::new(State::default()),
        }
    }

    /// Returns whether this direct edge changed, queueing the new statement for the next push.
    pub(crate) fn observe(&self, subject: NodeId, reachable: bool) -> bool {
        let mut state = lock(&self.state);
        let previous = state
            .observations
            .get(&(self.node, subject))
            .map(|record| record.reachable);
        if previous.unwrap_or(true) == reachable {
            return false;
        }

        state.admit(WireReachability {
            observer: self.node,
            subject,
            version: self.next_version.fetch_add(1, Ordering::Relaxed),
            reachable,
        })
    }

    /// Merges peer statements, parking the ones naming a node which is not a member here yet. Their
    /// observer resends only to its own peers, so a record which arrived through a relay has no
    /// other way back once the relay has deduplicated it.
    pub(crate) fn merge(&self, records: &[WireReachability], up: &HashSet<NodeId>) {
        let mut state = lock(&self.state);
        for record in records.iter().copied() {
            if record.observer == record.subject || record.observer == self.node {
                continue;
            }
            if up.contains(&record.observer) && up.contains(&record.subject) {
                state.admit(record);
            } else {
                state.park(record);
            }
        }
    }

    /// Admits the parked records whose endpoints have become members, which queues them for the
    /// next push and hence resumes the relay that dropped them.
    pub(crate) fn promote_pending(&self, up: &HashSet<NodeId>) {
        let mut state = lock(&self.state);
        let ready = state
            .pending
            .values()
            .map(|parked| parked.record)
            .filter(|record| up.contains(&record.observer) && up.contains(&record.subject))
            .map(|record| ((record.observer, record.subject), record))
            .collect::<Vec<_>>();

        for (key, record) in ready {
            state.pending.remove(&key);
            state.admit(record);
        }
    }

    /// Everything to push this tick, drained: the statements accepted since the last push plus
    /// this node's standing unreachable ones, which are re-asserted so a peer that never received
    /// one at all still converges.
    pub(crate) fn take_outbound(&self, up: &HashSet<NodeId>) -> Vec<WireReachability> {
        let mut state = lock(&self.state);
        let mut outbound = take(&mut state.forward);
        outbound.retain(|_, record| up.contains(&record.observer) && up.contains(&record.subject));
        for record in state.observations.values() {
            if record.observer == self.node && !record.reachable && up.contains(&record.subject) {
                outbound.insert((record.observer, record.subject), *record);
            }
        }

        sorted(outbound.into_values().collect())
    }

    pub(crate) fn snapshot(&self, up: &HashSet<NodeId>) -> Vec<WireReachability> {
        let records = lock(&self.state)
            .observations
            .values()
            .filter(|record| up.contains(&record.observer) && up.contains(&record.subject))
            .copied()
            .collect();

        sorted(records)
    }

    pub(crate) fn prune_pending<F>(&self, is_down: F, max_age: Duration)
    where
        F: Fn(NodeId) -> bool,
    {
        let named = {
            let state = lock(&self.state);
            if state.pending.is_empty() {
                return;
            }
            state
                .pending
                .keys()
                .flat_map(|(observer, subject)| [*observer, *subject])
                .collect::<HashSet<_>>()
        };
        let down = named
            .into_iter()
            .filter(|node| is_down(*node))
            .collect::<HashSet<_>>();

        lock(&self.state)
            .pending
            .retain(|(observer, subject), parked| {
                !down.contains(observer)
                    && !down.contains(subject)
                    && parked.since.elapsed() < max_age
            });
    }

    pub(crate) fn prune_fenced(&self, fence: NodeId) {
        let mut state = lock(&self.state);
        let covered = |(observer, subject): &(NodeId, NodeId)| {
            fence.covers(*observer) || fence.covers(*subject)
        };
        state.observations.retain(|edge, _| !covered(edge));
        state.pending.retain(|edge, _| !covered(edge));
        state.forward.retain(|edge, _| !covered(edge));
        state
            .unreachable_since
            .retain(|node, _| !fence.covers(*node));
    }

    /// Members outside this node's component are what a downing provider sees as unreachable.
    pub(crate) fn unreachable_members(&self, members: &[Member]) -> Vec<(Member, Instant)> {
        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .map(Member::node)
            .collect::<HashSet<_>>();
        let mut state = lock(&self.state);
        let connected = component(&state.observations, self.node, &up);

        let now = Instant::now();
        state
            .unreachable_since
            .retain(|node, _| up.contains(node) && !connected.contains(node));
        for node in up.iter().filter(|node| !connected.contains(node)) {
            state.unreachable_since.entry(*node).or_insert(now);
        }

        members
            .iter()
            .filter_map(|member| {
                state
                    .unreachable_since
                    .get(&member.node())
                    .map(|since| (*member, *since))
            })
            .collect()
    }

    /// The unreachable statements between two Up members which are both inside this node's
    /// component, ordered by observer, then subject, each by address and incarnation.
    pub(crate) fn broken_links(&self, members: &[Member]) -> Vec<BrokenLink> {
        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .map(|member| (member.node(), *member))
            .collect::<HashMap<_, _>>();
        let nodes = up.keys().copied().collect::<HashSet<_>>();
        let state = lock(&self.state);
        let connected = component(&state.observations, self.node, &nodes);

        let records = state
            .observations
            .values()
            .filter(|record| {
                !record.reachable
                    && connected.contains(&record.observer)
                    && connected.contains(&record.subject)
            })
            .copied()
            .collect();

        sorted(records)
            .into_iter()
            .filter_map(|record| {
                let observer = up.get(&record.observer)?;
                let subject = up.get(&record.subject)?;
                Some(BrokenLink::new(*observer, *subject))
            })
            .collect()
    }

    /// An Up member outside this node's component, if any; a broken link between two members
    /// inside it does not count.
    pub(crate) fn outside_component(&self, up: &HashSet<NodeId>) -> Option<NodeId> {
        let state = lock(&self.state);
        let connected = component(&state.observations, self.node, up);
        up.iter().copied().find(|node| !connected.contains(node))
    }
}

struct Parked {
    record: WireReachability,
    since: Instant,
}

fn sorted(mut records: Vec<WireReachability>) -> Vec<WireReachability> {
    records.sort_by_key(|record| {
        (
            record.observer.addr(),
            record.observer.incarnation(),
            record.subject.addr(),
            record.subject.incarnation(),
        )
    });
    records
}

fn component(
    observations: &HashMap<(NodeId, NodeId), WireReachability>,
    node: NodeId,
    up: &HashSet<NodeId>,
) -> HashSet<NodeId> {
    let mut connected = HashSet::from([node]);
    let mut queue = VecDeque::from([node]);
    while let Some(from) = queue.pop_front() {
        for to in up.iter().copied() {
            if connected.contains(&to) || edge_is_down(observations, from, to) {
                continue;
            }
            connected.insert(to);
            queue.push_back(to);
        }
    }
    connected
}

fn edge_is_down(
    observations: &HashMap<(NodeId, NodeId), WireReachability>,
    left: NodeId,
    right: NodeId,
) -> bool {
    observations
        .get(&(left, right))
        .is_some_and(|record| !record.reachable)
        || observations
            .get(&(right, left))
            .is_some_and(|record| !record.reachable)
}

/// Chunked once, so a push fanning out to every peer splits the records only once; a [Frame]
/// owns its records, hence the clone per peer.
pub(crate) fn snapshot_chunks(
    records: Vec<WireReachability>,
    max_frame_size: usize,
) -> Vec<Vec<WireReachability>> {
    if records.is_empty() {
        return Vec::new();
    }
    let per_chunk = max_frame_size
        .saturating_sub(WireReachability::ENVELOPE_LEN)
        .div_euclid(WireReachability::MAX_ENCODED_LEN)
        .max(1);
    records
        .chunks(per_chunk)
        .map(|chunk| chunk.to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{MAX_PENDING, MIN_FRAME_SIZE, Reachability, WireReachability, snapshot_chunks};
    use crate::{
        cluster::{
            downing::{Disconnected, Downing, DowningProvider, KeepMajority},
            frame::Frame,
            membership::{Member, MemberState},
            node::NodeId,
            state::BrokenLink,
        },
        sync::lock,
    };
    use std::{collections::HashSet, net::SocketAddr, time::Duration};

    fn node(port: u16) -> NodeId {
        NodeId::new(SocketAddr::from(([127, 0, 0, 1], port)))
    }

    fn members(nodes: &[NodeId]) -> Vec<Member> {
        nodes
            .iter()
            .map(|node| Member::new(*node, MemberState::Up))
            .collect()
    }

    fn up(nodes: &[NodeId]) -> HashSet<NodeId> {
        nodes.iter().copied().collect()
    }

    /// Merging plus the tick's promotion, which is what a node actually applies per round.
    fn merge_all(table: &Reachability, records: &[WireReachability], nodes: &[NodeId]) {
        table.merge(records, &up(nodes));
        table.promote_pending(&up(nodes));
    }

    fn observe(table: &Reachability, subject: NodeId, reachable: bool) -> WireReachability {
        assert!(table.observe(subject, reachable), "the edge must change");
        table
            .take_outbound(&up(&[table.node, subject]))
            .into_iter()
            .find(|record| record.subject == subject)
            .expect("the new statement is queued for the next push")
    }

    /// A broken direct A-B edge still leaves one component through C, so neither endpoint may
    /// manufacture the other's tombstone.
    #[test]
    fn a_single_broken_link_stays_connected_through_a_third_member() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let c = Reachability::new(nodes[2]);
        let records = [observe(&a, nodes[1], false), observe(&b, nodes[0], false)];
        for table in [&a, &b, &c] {
            merge_all(table, &records, &nodes);
            assert!(table.unreachable_members(&members(&nodes)).is_empty());
        }
    }

    /// A broken A-B edge inside one component is a broken link at every node, once per observer
    /// statement, while no member is unreachable.
    #[test]
    fn a_broken_link_inside_the_component_is_derived() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let c = Reachability::new(nodes[2]);
        let records = [observe(&a, nodes[1], false), observe(&b, nodes[0], false)];
        let all = members(&nodes);
        let link = |observer: usize, subject: usize| BrokenLink::new(all[observer], all[subject]);

        for table in [&a, &b, &c] {
            merge_all(table, &records, &nodes);
            assert_eq!(table.broken_links(&all), [link(0, 1), link(1, 0)]);
            assert!(table.unreachable_members(&all).is_empty());
        }
    }

    /// In an A | B,C,D split with a broken B-C edge, A's statements name a member outside the
    /// component of B, C and D, so only B-C is a broken link there. At A, whose component is A
    /// alone, there is none. A member which is not Up is never an end either.
    #[test]
    fn a_link_with_an_end_outside_the_component_is_not_broken() {
        let nodes = [node(1), node(2), node(3), node(4)];
        let tables = nodes.map(Reachability::new);
        let records = [
            observe(&tables[0], nodes[1], false),
            observe(&tables[0], nodes[2], false),
            observe(&tables[0], nodes[3], false),
            observe(&tables[1], nodes[0], false),
            observe(&tables[1], nodes[2], false),
            observe(&tables[2], nodes[0], false),
            observe(&tables[3], nodes[0], false),
        ];
        let all = members(&nodes);
        for table in &tables {
            merge_all(table, &records, &nodes);
        }

        assert!(tables[0].broken_links(&all).is_empty());
        for table in &tables[1..] {
            assert_eq!(table.broken_links(&all), [BrokenLink::new(all[1], all[2])]);
        }

        let mut left = all.clone();
        left[2] = Member::new(nodes[2], MemberState::Left);
        assert!(tables[1].broken_links(&left).is_empty());
    }

    /// A genuine A | B,C split yields the same two components at every node after relaying.
    #[test]
    fn a_partition_is_derived_from_the_shared_graph() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let c = Reachability::new(nodes[2]);
        let records = [
            observe(&a, nodes[1], false),
            observe(&a, nodes[2], false),
            observe(&b, nodes[0], false),
            observe(&c, nodes[0], false),
        ];
        for table in [&a, &b, &c] {
            merge_all(table, &records, &nodes);
        }

        let all_members = members(&nodes);
        let unreachable_a = a.unreachable_members(&all_members);
        let outside_a = unreachable_a
            .iter()
            .copied()
            .map(|(member, _)| member.node())
            .collect::<HashSet<_>>();
        assert_eq!(outside_a, HashSet::from([nodes[1], nodes[2]]));
        let after = Duration::from_secs(10);
        let at = unreachable_a[0].1 + after;
        assert_eq!(
            KeepMajority::new(after).down(&all_members, Disconnected::new(&unreachable_a), at),
            Downing::SelfDown
        );
        for table in [&b, &c] {
            let unreachable = table.unreachable_members(&all_members);
            let outside = unreachable
                .iter()
                .copied()
                .map(|(member, _)| member.node())
                .collect::<HashSet<_>>();
            assert_eq!(outside, HashSet::from([nodes[0]]));
            assert_eq!(
                KeepMajority::new(after).down(
                    &all_members,
                    Disconnected::new(&unreachable),
                    unreachable[0].1 + after
                ),
                Downing::Members(vec![all_members[0]])
            );
        }
    }

    /// A record which arrives through a relay is the case the observer's own resend cannot repair:
    /// the observer only ever pushes to its own peers, and the relay deduplicates the resend by
    /// version. Parking it until membership catches up is what keeps the graph from staying wrong,
    /// and the promoted record must be queued again so the relay continues past this node.
    #[test]
    fn a_relayed_record_parked_while_membership_lags_is_promoted_and_relayed_on() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let b_to_c = observe(&b, nodes[2], false);
        let a_to_c = observe(&a, nodes[2], false);

        let lagging = up(&[nodes[0], nodes[1]]);
        a.merge(&[b_to_c], &lagging);
        a.promote_pending(&lagging);
        assert!(a.take_outbound(&lagging).is_empty());
        assert!(a.unreachable_members(&members(&nodes)).is_empty());

        a.promote_pending(&up(&nodes));

        assert_eq!(a.take_outbound(&up(&nodes)), vec![a_to_c, b_to_c]);
        let outside = a
            .unreachable_members(&members(&nodes))
            .iter()
            .map(|(member, _)| member.node())
            .collect::<Vec<_>>();
        assert_eq!(outside, vec![nodes[2]]);
    }

    /// A push carries what was learned since the last one, plus this node's own standing failures,
    /// which are re-asserted so a peer that received none of them at all still converges. A
    /// relayed record is forwarded once; a recovery revokes its failure rather than standing.
    #[test]
    fn a_push_drains_what_was_learned_and_re_asserts_own_failures() {
        let nodes = [node(1), node(2), node(3)];
        let a = Reachability::new(nodes[0]);
        let b = Reachability::new(nodes[1]);
        let all = up(&nodes);

        let a_to_b = observe(&a, nodes[1], false);
        observe(&a, nodes[2], false);
        observe(&a, nodes[2], true);
        let b_to_c = observe(&b, nodes[2], false);
        a.merge(&[b_to_c], &all);

        assert_eq!(a.take_outbound(&all), vec![a_to_b, b_to_c]);
        assert_eq!(a.take_outbound(&all), vec![a_to_b]);
    }

    /// A peer can name any node, so what this node parks for it is capped.
    #[test]
    fn the_pending_table_is_bounded() {
        let local = node(1);
        let table = Reachability::new(local);
        let observer = node(9_000);
        let records = (0..MAX_PENDING + 8)
            .map(|version| WireReachability {
                observer,
                subject: NodeId::new(
                    format!("127.0.0.1:{}", 10_000 + version)
                        .parse()
                        .expect("valid address"),
                ),
                version: version as u64,
                reachable: false,
            })
            .collect::<Vec<_>>();

        table.merge(&records, &up(&[local]));

        assert_eq!(lock(&table.state).pending.len(), MAX_PENDING);
    }

    #[test]
    fn parked_records_naming_the_dead_or_outliving_the_age_are_dropped() {
        let local = node(1);
        let (dead, waiting) = (node(2), node(3));
        let observer = node(4);
        let table = Reachability::new(local);
        let record = |subject| WireReachability {
            observer,
            subject,
            version: 1,
            reachable: false,
        };
        table.merge(&[record(dead), record(waiting)], &up(&[local]));
        assert_eq!(lock(&table.state).pending.len(), 2);

        table.prune_pending(|node| node == dead, Duration::MAX);
        let parked = lock(&table.state)
            .pending
            .keys()
            .map(|(_, subject)| *subject)
            .collect::<Vec<_>>();
        assert_eq!(parked, vec![waiting]);

        table.prune_pending(|_| false, Duration::ZERO);
        assert!(lock(&table.state).pending.is_empty());
    }

    /// A newer recovery wins even if an old failure is relayed afterward.
    #[test]
    fn versions_make_recovery_monotone() {
        let nodes = [node(1), node(2)];
        let owner = Reachability::new(nodes[0]);
        let replica = Reachability::new(nodes[1]);
        let failed = observe(&owner, nodes[1], false);
        let recovered = observe(&owner, nodes[1], true);
        merge_all(&replica, &[recovered, failed], &nodes);

        assert!(replica.unreachable_members(&members(&nodes)).is_empty());
        assert_eq!(replica.snapshot(&HashSet::from(nodes)), vec![recovered]);
    }

    /// The conservative fixed-size bound keeps every reachability snapshot chunk within the
    /// endpoint's configured frame limit.
    #[test]
    fn reachability_snapshots_respect_the_frame_limit() {
        let widest = || {
            NodeId::new(
                "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("valid address"),
            )
        };
        let record = WireReachability {
            observer: widest(),
            subject: widest(),
            version: u64::MAX,
            reachable: false,
        };
        assert!(
            postcard::experimental::serialized_size(&record).expect("record size")
                <= WireReachability::MAX_ENCODED_LEN
        );
        for observations in snapshot_chunks(vec![record; 10], MIN_FRAME_SIZE) {
            let frame = Frame::Reachability { observations };
            assert!(frame.encode_into(Vec::new()).expect("frame encodes").len() <= MIN_FRAME_SIZE);
        }
    }
}
