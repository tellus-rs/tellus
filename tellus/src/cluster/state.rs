use crate::cluster::{
    endpoint::{self, Lifecycle},
    membership::{Member, MemberState},
    node::{ClusterId, Incarnation, NodeId},
    receptionist::{PublishedChange, Settlement, SnapshotSource},
};
use std::{collections::VecDeque, future::pending, net::SocketAddr};
use thiserror::Error;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    watch,
};

/// A versioned snapshot of this node's view of the cluster: the member list, the Up members this
/// node currently derives as unreachable and the broken links between reachable members, all of
/// the same version, plus this node's own identity, the ID of its cluster and the other clusters
/// seen at discovered addresses. Published on every change through [cluster_state]; consumers
/// derive from the current value, never from a history, since a receiver which reads less often
/// than the state changes skips intermediate values. With the `serde` feature it serializes as
/// `{version, this: {addr, incarnation}, cluster, members, unreachable, broken_links: [{observer,
/// subject}], foreign_clusters: [{addr, cluster}]}`, the cluster `null` while there is none; a
/// deserialized value is a mirror which nothing in tellus consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClusterState {
    version: u64,
    this: NodeId,
    cluster: Option<ClusterId>,
    members: Vec<Member>,
    unreachable: Vec<Member>,
    broken_links: Vec<BrokenLink>,
    foreign_clusters: Vec<ForeignCluster>,
}

impl ClusterState {
    /// Strictly increasing by one per observable change of this node's view.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The address this node advertises.
    pub fn this_addr(&self) -> SocketAddr {
        self.this.addr()
    }

    /// This node's incarnation, which tells its own entry from a predecessor's at the same
    /// address.
    pub fn this_incarnation(&self) -> Incarnation {
        self.this.incarnation()
    }

    /// The ID of the cluster this node is a member of: minted by [form](crate::cluster::form),
    /// adopted by a join once the cluster's member snapshot is complete, and `None` before
    /// either. Every member of one cluster reports the same ID.
    pub fn cluster(&self) -> Option<ClusterId> {
        self.cluster
    }

    /// This node's own entry, `None` once its Left or Down entry has been swept.
    pub fn this_member(&self) -> Option<&Member> {
        self.members
            .iter()
            .find(|member| member.node() == self.this)
    }

    /// The transitions from `previous` to this value, member by member: an entry which appeared
    /// is [Up](Transition::Up), [Left](Transition::Left) or [Down](Transition::Down) by its state,
    /// a Left or Down entry which disappeared is [Forgotten](Transition::Forgotten), an entry
    /// which entered the unreachable set is [Unreachable](Transition::Unreachable), and one which
    /// left it while still listed Up is [Reachable](Transition::Reachable); one downed instead
    /// yields its Down alone, as does a Left entry downed. A restart is the predecessor's Down
    /// beside the successor's Up.
    pub fn diff(&self, previous: &ClusterState) -> Vec<Transition> {
        let mut transitions = Vec::new();
        for member in &self.members {
            if !previous.members.contains(member) {
                transitions.push(match member.state() {
                    MemberState::Up => Transition::Up(*member),
                    MemberState::Left => Transition::Left(*member),
                    MemberState::Down => Transition::Down(*member),
                });
            }
        }
        for member in &previous.members {
            if member.state() != MemberState::Up
                && !self
                    .members
                    .iter()
                    .any(|listed| listed.node() == member.node())
            {
                transitions.push(Transition::Forgotten(*member));
            }
        }
        for member in &self.unreachable {
            if !previous.unreachable.contains(member) {
                transitions.push(Transition::Unreachable(*member));
            }
        }
        for member in &previous.unreachable {
            if !self.unreachable.contains(member) && self.members.contains(member) {
                transitions.push(Transition::Reachable(*member));
            }
        }
        transitions
    }

    /// The members as this node sees them, this node included, ordered by address and
    /// incarnation, exactly what [members](crate::cluster::members) answers at this version.
    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// The Up members outside this node's component of the reachability graph, in the order of
    /// [members](ClusterState::members).
    pub fn unreachable(&self) -> &[Member] {
        &self.unreachable
    }

    /// The direct links which one Up member reports broken while both of its ends are still
    /// inside this node's component of the reachability graph, ordered by observer, then subject.
    /// Both ends reach each other through other members, so no downing provider acts on such a
    /// link and it lasts until it heals or an operator removes one end with
    /// [down](crate::cluster::down). Messages between the two ends are queued meanwhile, asks
    /// between them time out and watches between them do not fire.
    pub fn broken_links(&self) -> &[BrokenLink] {
        &self.broken_links
    }

    /// The addresses which answered as members of another cluster than this node's, ordered by
    /// address: found by the probe [bootstrap](fn@crate::cluster::bootstrap) keeps running, or by
    /// a handshake this node refused as foreign, or by a dial answered by another cluster. An
    /// entry is dropped once it has not been confirmed for three probe intervals, 90 s without a
    /// probe, or once its address is an Up member here. Two clusters never merge, so a non-empty
    /// list is a split for an operator to resolve; it is logged at error level every five minutes
    /// while it lasts. Empty before this node has committed a cluster.
    pub fn foreign_clusters(&self) -> &[ForeignCluster] {
        &self.foreign_clusters
    }

    /// The Up members, this node included.
    pub fn up(&self) -> impl Iterator<Item = &Member> {
        self.members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
    }

    pub(crate) fn new(
        this: NodeId,
        cluster: Option<ClusterId>,
        version: u64,
        members: Vec<Member>,
        unreachable: Vec<Member>,
        broken_links: Vec<BrokenLink>,
        foreign_clusters: Vec<ForeignCluster>,
    ) -> Self {
        Self {
            version,
            this,
            cluster,
            members,
            unreachable,
            broken_links,
            foreign_clusters,
        }
    }
}

/// A direct link between two Up members which the observer reports broken, see
/// [ClusterState::broken_links].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BrokenLink {
    observer: Member,
    subject: Member,
}

impl BrokenLink {
    /// The member which cannot reach the subject directly.
    pub fn observer(&self) -> &Member {
        &self.observer
    }

    /// The member the observer cannot reach directly.
    pub fn subject(&self) -> &Member {
        &self.subject
    }

    pub(crate) fn new(observer: Member, subject: Member) -> Self {
        Self { observer, subject }
    }
}

/// An address which answered as a member of another cluster, see
/// [ClusterState::foreign_clusters].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ForeignCluster {
    addr: SocketAddr,
    cluster: ClusterId,
}

impl ForeignCluster {
    /// The address which answered.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The ID of the cluster it answered as a member of.
    pub fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub(crate) fn new(addr: SocketAddr, cluster: ClusterId) -> Self {
        Self { addr, cluster }
    }
}

/// One transition between two consecutive [ClusterState]s, see [ClusterState::diff].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "kind", content = "member", rename_all = "snake_case")
)]
pub enum Transition {
    /// The member is listed Up, having not been listed before.
    Up(Member),

    /// The member is listed Left, having been listed Up or not at all before.
    Left(Member),

    /// The member is listed Down, having been listed Up or Left or not at all before.
    Down(Member),

    /// The member's Left or Down entry was swept after its retention.
    Forgotten(Member),

    /// The Up member became unreachable.
    Unreachable(Member),

    /// The Up member became reachable again.
    Reachable(Member),
}

impl Transition {
    /// The member the transition is about.
    pub fn member(&self) -> &Member {
        match self {
            Self::Up(member)
            | Self::Left(member)
            | Self::Down(member)
            | Self::Forgotten(member)
            | Self::Unreachable(member)
            | Self::Reachable(member) => member,
        }
    }
}

/// One item of [changes]: a whole cluster state per version, a settled verdict per change of
/// it, or the loss a subscriber which fell behind is told about before it is handed the current
/// state and verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "kind", rename_all = "snake_case")
)]
pub enum Change {
    /// The cluster state at a new version.
    State(ClusterState),

    /// The receptionist's verdict, sent whenever the verdict or the version it judges changes, or
    /// repeated after a gap.
    Settled(Settlement),

    /// The subscriber fell behind: this many changes were overwritten before it read them. The
    /// current state and verdict follow, superseding whatever the channel still held.
    Gap {
        /// How many changes were overwritten.
        dropped: u64,
    },
}

/// The subscription [changes] returns: every version of this node's cluster state and every
/// change of the receptionist's verdict or of the version it judges, in publication order, from a
/// bounded channel of [change_capacity](crate::cluster::EndpointConfig::change_capacity) entries.
///
/// The first two items are the current state and its verdict. A verdict never precedes the state
/// of its version, and during uninterrupted delivery each published verdict arrives once. State
/// versions are consecutive except across a [Change::Gap], which is always followed by the
/// current state and verdict, so no consumer needs a catch-up of its own; those repeat what may
/// already have been seen.
pub struct Changes {
    rx: broadcast::Receiver<PublishedChange>,
    source: &'static dyn SnapshotSource,
    queued: VecDeque<Change>,
    last_version: u64,
    last_seq: u64,
}

impl Changes {
    /// The next change; pending forever once the endpoint is gone.
    pub async fn next(&mut self) -> Change {
        loop {
            if let Some(change) = self.queued.pop_front() {
                return change;
            }
            match self.rx.recv().await {
                Ok(PublishedChange::State(state)) if state.version() > self.last_version => {
                    self.last_version = state.version();
                    return Change::State(state);
                }

                Ok(PublishedChange::Settled { settlement, seq }) if seq > self.last_seq => {
                    self.last_seq = seq;
                    return Change::Settled(settlement);
                }

                // Represented by the snapshot already.
                Ok(_) => {}

                Err(RecvError::Lagged(dropped)) => {
                    self.queued.push_back(Change::Gap { dropped });
                    self.recover();
                }

                Err(RecvError::Closed) => pending::<()>().await,
            }
        }
    }

    /// The receiver must be subscribed before the snapshot is taken.
    pub(crate) fn new(
        rx: broadcast::Receiver<PublishedChange>,
        source: &'static dyn SnapshotSource,
    ) -> Self {
        let mut changes = Self {
            rx,
            source,
            queued: VecDeque::new(),
            last_version: 0,
            last_seq: 0,
        };
        changes.recover();
        changes
    }

    /// Taken after the loss was observed, so everything the channel still holds is older.
    fn recover(&mut self) {
        let (state, settlement, seq) = self.source.snapshot();
        self.last_version = state.version();
        self.last_seq = seq;
        self.queued.push_back(Change::State(state));
        self.queued.push_back(Change::Settled(settlement));
    }
}

/// The cluster state cannot be observed.
#[derive(Debug, Error)]
pub enum ClusterStateError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,
}

/// Subscribe to this node's view of the cluster. The receiver always holds the current
/// [ClusterState]; `changed` resolves whenever a new version has been published.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn cluster_state() -> Result<watch::Receiver<ClusterState>, ClusterStateError> {
    endpoint::get()
        .map(|endpoint| endpoint.state_rx())
        .ok_or(ClusterStateError::EndpointNotStarted)
}

/// Where the endpoint stands: no cluster yet, a member, or downed for good.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn lifecycle() -> Result<Lifecycle, ClusterStateError> {
    endpoint::get()
        .map(|endpoint| endpoint.lifecycle())
        .ok_or(ClusterStateError::EndpointNotStarted)
}

/// Subscribe to every change of this node's view: each cluster state version as a whole value
/// and each change of the receptionist's verdict or of the version it judges, see [Changes].
/// Unlike [cluster_state], which a slow reader may find skipped ahead, nothing here is skipped
/// silently.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn changes() -> Result<Changes, ClusterStateError> {
    endpoint::get()
        .map(|endpoint| endpoint.changes())
        .ok_or(ClusterStateError::EndpointNotStarted)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::{
        node::ClusterId,
        state::{BrokenLink, ForeignCluster},
    };
    use crate::{
        cluster::{
            membership::{Member, MemberState},
            node::NodeId,
            receptionist::{PublishedChange, Settlement, SnapshotSource},
            state::{Change, Changes, ClusterState, Transition},
        },
        sync::lock,
    };
    #[cfg(feature = "serde")]
    use serde_json::{Value, json};
    use std::{collections::VecDeque, net::SocketAddr, sync::Mutex, time::Duration};
    use tokio::{sync::broadcast, time::timeout};

    struct Scripted(Mutex<VecDeque<(ClusterState, Settlement, u64)>>);

    impl SnapshotSource for Scripted {
        fn snapshot(&self) -> (ClusterState, Settlement, u64) {
            lock(&self.0).pop_front().expect("a scripted snapshot")
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn up(node: NodeId) -> Member {
        Member::new(node, MemberState::Up)
    }

    fn left(node: NodeId) -> Member {
        Member::new(node, MemberState::Left)
    }

    fn down(node: NodeId) -> Member {
        Member::new(node, MemberState::Down)
    }

    fn state(this: NodeId, version: u64, members: Vec<Member>) -> ClusterState {
        ClusterState::new(
            this,
            None,
            version,
            members,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn source(snapshots: Vec<(ClusterState, Settlement, u64)>) -> &'static Scripted {
        Box::leak(Box::new(Scripted(Mutex::new(snapshots.into()))))
    }

    fn settled(version: u64, settled: bool, seq: u64) -> PublishedChange {
        PublishedChange::Settled {
            settlement: Settlement::new(version, settled),
            seq,
        }
    }

    async fn nothing_pending(changes: &mut Changes) {
        assert!(
            timeout(Duration::from_millis(50), changes.next())
                .await
                .is_err(),
            "nothing further is expected"
        );
    }

    #[tokio::test]
    async fn the_first_items_are_the_snapshot() {
        let this = NodeId::new(addr(1));
        let (_tx, rx) = broadcast::channel(8);
        let first = state(this, 1, vec![up(this)]);
        let mut changes = Changes::new(
            rx,
            source(vec![(first.clone(), Settlement::new(1, true), 1)]),
        );

        assert_eq!(changes.next().await, Change::State(first));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, true))
        );
        nothing_pending(&mut changes).await;
    }

    /// Entries the snapshot already represents are dropped, later ones pass: the construction
    /// race between subscribing and snapshotting cannot reorder or repeat.
    #[tokio::test]
    async fn entries_the_snapshot_represents_are_dropped() {
        let this = NodeId::new(addr(1));
        let peer = NodeId::new(addr(2));
        let (tx, rx) = broadcast::channel(8);
        let first = state(this, 1, vec![up(this)]);
        let second = state(this, 2, vec![up(this), up(peer)]);
        tx.send(PublishedChange::State(first.clone()))
            .expect("subscribed");
        tx.send(settled(1, true, 1)).expect("subscribed");
        let mut changes = Changes::new(
            rx,
            source(vec![(first.clone(), Settlement::new(1, true), 1)]),
        );
        tx.send(settled(1, false, 2)).expect("subscribed");
        tx.send(PublishedChange::State(second.clone()))
            .expect("subscribed");

        assert_eq!(changes.next().await, Change::State(first));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, true))
        );
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, false))
        );
        assert_eq!(changes.next().await, Change::State(second));
        nothing_pending(&mut changes).await;
    }

    #[tokio::test]
    async fn a_lagging_subscriber_gets_a_gap_and_the_current_pair() {
        let this = NodeId::new(addr(1));
        let (tx, rx) = broadcast::channel(2);
        let initial = state(this, 0, vec![up(this)]);
        let fifth = state(this, 5, vec![up(this)]);
        let sixth = state(this, 6, vec![up(this)]);
        let mut changes = Changes::new(
            rx,
            source(vec![
                (initial, Settlement::new(0, true), 0),
                (fifth.clone(), Settlement::new(5, true), 3),
            ]),
        );
        changes.next().await;
        changes.next().await;

        for version in 1..=5 {
            tx.send(PublishedChange::State(state(this, version, vec![up(this)])))
                .expect("subscribed");
        }

        assert_eq!(changes.next().await, Change::Gap { dropped: 3 });
        assert_eq!(changes.next().await, Change::State(fifth));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(5, true))
        );
        nothing_pending(&mut changes).await;

        tx.send(PublishedChange::State(sixth.clone()))
            .expect("subscribed");
        assert_eq!(changes.next().await, Change::State(sixth));
    }

    /// With room for one change the state is overwritten by its own verdict; the recovery still
    /// hands out the state rather than a bare verdict.
    #[tokio::test]
    async fn capacity_one_recovers_to_the_state() {
        let this = NodeId::new(addr(1));
        let (tx, rx) = broadcast::channel(1);
        let initial = state(this, 0, vec![up(this)]);
        let first = state(this, 1, vec![up(this)]);
        let mut changes = Changes::new(
            rx,
            source(vec![
                (initial, Settlement::new(0, true), 0),
                (first.clone(), Settlement::new(1, true), 1),
            ]),
        );
        changes.next().await;
        changes.next().await;

        tx.send(PublishedChange::State(first.clone()))
            .expect("subscribed");
        tx.send(settled(1, true, 1)).expect("subscribed");

        assert_eq!(changes.next().await, Change::Gap { dropped: 1 });
        assert_eq!(changes.next().await, Change::State(first));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, true))
        );
        nothing_pending(&mut changes).await;

        tx.send(settled(1, false, 2)).expect("subscribed");
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, false))
        );
    }

    #[tokio::test]
    async fn verdict_flips_evicting_the_last_state_still_end_in_the_current_state() {
        let this = NodeId::new(addr(1));
        let (tx, rx) = broadcast::channel(2);
        let initial = state(this, 0, vec![up(this)]);
        let first = state(this, 1, vec![up(this)]);
        let second = state(this, 2, vec![up(this)]);
        let mut changes = Changes::new(
            rx,
            source(vec![
                (initial, Settlement::new(0, true), 0),
                (first.clone(), Settlement::new(1, false), 3),
            ]),
        );
        changes.next().await;
        changes.next().await;

        tx.send(PublishedChange::State(first.clone()))
            .expect("subscribed");
        tx.send(settled(1, false, 1)).expect("subscribed");
        tx.send(settled(1, true, 2)).expect("subscribed");
        tx.send(settled(1, false, 3)).expect("subscribed");

        assert_eq!(changes.next().await, Change::Gap { dropped: 2 });
        assert_eq!(changes.next().await, Change::State(first));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, false))
        );
        nothing_pending(&mut changes).await;

        tx.send(PublishedChange::State(second.clone()))
            .expect("subscribed");
        assert_eq!(changes.next().await, Change::State(second));
    }

    /// Recovery repeats the current pair even when the subscriber saw it already: the pair is
    /// the consumer's re-seed, not news.
    #[tokio::test]
    async fn a_post_gap_snapshot_repeats_what_was_seen() {
        let this = NodeId::new(addr(1));
        let (tx, rx) = broadcast::channel(1);
        let first = state(this, 1, vec![up(this)]);
        let mut changes = Changes::new(
            rx,
            source(vec![
                (first.clone(), Settlement::new(1, true), 1),
                (first.clone(), Settlement::new(1, true), 3),
            ]),
        );
        changes.next().await;
        changes.next().await;

        tx.send(settled(1, false, 2)).expect("subscribed");
        tx.send(settled(1, true, 3)).expect("subscribed");

        assert_eq!(changes.next().await, Change::Gap { dropped: 1 });
        assert_eq!(changes.next().await, Change::State(first));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(1, true))
        );
        nothing_pending(&mut changes).await;
    }

    #[test]
    fn a_restart_is_the_predecessors_down_beside_the_successors_up() {
        let this = NodeId::new(addr(1));
        let older = NodeId::new(addr(2));
        let younger = NodeId::new(addr(2));
        let before = state(this, 1, vec![up(this), up(older)]);
        let restarted = state(this, 2, vec![up(this), down(older), up(younger)]);
        let forgotten = state(this, 3, vec![up(this), up(younger)]);

        assert_eq!(
            restarted.diff(&before),
            vec![Transition::Down(down(older)), Transition::Up(up(younger))]
        );
        assert_eq!(
            forgotten.diff(&restarted),
            vec![Transition::Forgotten(down(older))]
        );
    }

    #[test]
    fn a_departure_downed_later_is_its_down_and_forgotten_once() {
        let this = NodeId::new(addr(1));
        let peer = NodeId::new(addr(2));
        let listed = state(this, 1, vec![up(this), up(peer)]);
        let departed = state(this, 2, vec![up(this), left(peer)]);
        let downed = state(this, 3, vec![up(this), down(peer)]);
        let forgotten = state(this, 4, vec![up(this)]);

        assert_eq!(departed.diff(&listed), vec![Transition::Left(left(peer))]);
        assert_eq!(downed.diff(&departed), vec![Transition::Down(down(peer))]);
        assert_eq!(
            forgotten.diff(&downed),
            vec![Transition::Forgotten(down(peer))]
        );
        assert_eq!(
            forgotten.diff(&departed),
            vec![Transition::Forgotten(left(peer))]
        );
    }

    #[test]
    fn an_unreachable_member_downed_is_not_reachable_again() {
        let this = NodeId::new(addr(1));
        let peer = NodeId::new(addr(2));
        let listed = state(this, 1, vec![up(this), up(peer)]);
        let unreachable = ClusterState::new(
            this,
            None,
            2,
            vec![up(this), up(peer)],
            vec![up(peer)],
            Vec::new(),
            Vec::new(),
        );
        let downed = state(this, 3, vec![up(this), down(peer)]);

        assert_eq!(
            unreachable.diff(&listed),
            vec![Transition::Unreachable(up(peer))]
        );
        assert_eq!(
            downed.diff(&unreachable),
            vec![Transition::Down(down(peer))]
        );
    }

    #[test]
    fn an_unreachable_member_still_up_is_reachable_again() {
        let this = NodeId::new(addr(1));
        let peer = NodeId::new(addr(2));
        let unreachable = ClusterState::new(
            this,
            None,
            1,
            vec![up(this), up(peer)],
            vec![up(peer)],
            Vec::new(),
            Vec::new(),
        );
        let reachable = state(this, 2, vec![up(this), up(peer)]);

        assert_eq!(
            reachable.diff(&unreachable),
            vec![Transition::Reachable(up(peer))]
        );
    }

    /// The flat shape is the contract consumers parse; the incarnation is the UUID as a string.
    #[cfg(feature = "serde")]
    #[test]
    fn a_member_serializes_flat_and_round_trips() {
        let node = NodeId::new(addr(1));
        let member = Member::new(node, MemberState::Up);

        let value = serde_json::to_value(member).expect("serializes");
        assert_eq!(value["addr"], json!("127.0.0.1:1"));
        assert_eq!(value["incarnation"], json!(node.incarnation().to_string()));
        assert_eq!(value["state"], json!("Up"));
        assert_eq!(value.as_object().map(|object| object.len()), Some(3));

        let parsed = serde_json::from_value::<Member>(value).expect("deserializes");
        assert_eq!(parsed, member);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn a_cluster_state_names_this_node_and_round_trips() {
        let this = NodeId::new(addr(1));
        let peer = NodeId::new(addr(2));
        let observer = NodeId::new(addr(4));
        let cluster = ClusterId::new();
        let foreign = ClusterId::new();
        let link = BrokenLink::new(up(observer), up(this));
        let state = ClusterState::new(
            this,
            Some(cluster),
            7,
            vec![up(this), up(peer), up(observer)],
            vec![up(peer)],
            vec![link],
            vec![ForeignCluster::new(addr(3), foreign)],
        );

        let value = serde_json::to_value(&state).expect("serializes");
        assert_eq!(value["version"], json!(7));
        assert_eq!(value["this"]["addr"], json!("127.0.0.1:1"));
        assert_eq!(
            value["this"]["incarnation"],
            json!(this.incarnation().to_string())
        );
        assert_eq!(value["cluster"], json!(cluster.to_string()));
        assert_eq!(value["members"].as_array().map(Vec::len), Some(3));
        assert_eq!(
            value["unreachable"],
            Value::Array(vec![value["members"][1].clone()])
        );
        assert_eq!(
            value["broken_links"],
            json!([{
                "observer": value["members"][2].clone(),
                "subject": value["members"][0].clone(),
            }])
        );
        assert_eq!(
            value["foreign_clusters"],
            json!([{ "addr": "127.0.0.1:3", "cluster": foreign.to_string() }])
        );

        let parsed = serde_json::from_value::<ClusterState>(value).expect("deserializes");
        assert_eq!(parsed, state);
        assert_eq!(parsed.this_addr(), addr(1));
        assert_eq!(parsed.this_incarnation(), this.incarnation());
        assert_eq!(parsed.cluster(), Some(cluster));
        assert_eq!(
            parsed.foreign_clusters(),
            [ForeignCluster::new(addr(3), foreign)]
        );
        assert_eq!(parsed.broken_links(), [link]);
        assert_eq!(parsed.broken_links()[0].observer(), &up(observer));
        assert_eq!(parsed.broken_links()[0].subject(), &up(this));
        assert_eq!(
            parsed.this_member(),
            Some(&Member::new(this, MemberState::Up))
        );
    }
}
