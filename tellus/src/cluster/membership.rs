use crate::{
    cluster::{
        endpoint::{self, EndpointInner},
        formation::JoinOutcome,
        frame::{Frame, RefusalReason},
        node::{Incarnation, NodeId},
        peer::{ConnectError, JoinRequest},
        reachability::Reachability,
    },
    sync::{read, write},
};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::RwLock,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{sync::oneshot, time::sleep};
use tracing::{debug, info, warn};

/// A member's three-point state lattice: `Up < Left < Down`, with removal represented by absence.
/// A downing after a departure wins, since Down is the greater state.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MemberState {
    /// A member: heartbeated, gossiped, connectable.
    #[display("up")]
    Up,

    /// Departed through [leave](fn@crate::cluster::leave): dead for good like a Down member, but
    /// a node holding a connection from it keeps delivering what that connection still carries
    /// before running the node death sequence.
    #[display("left")]
    Left,

    /// Dead for good: its handshakes are refused and sends toward it fail, which is what makes
    /// the synthesized terminated signals true.
    #[display("down")]
    Down,
}

/// A member of the cluster as this node sees it. It serializes flat, `{addr, incarnation, state}`,
/// which is also how gossip carries it.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "MemberRepr", from = "MemberRepr")]
#[display("{node} ({state})")]
pub struct Member {
    node: NodeId,
    state: MemberState,
}

impl Member {
    /// The address the member advertises.
    pub fn addr(&self) -> SocketAddr {
        self.node.addr()
    }

    /// The incarnation of the process at that address, which tells a restarted member apart from
    /// its predecessor.
    pub fn incarnation(&self) -> Incarnation {
        self.node.incarnation()
    }

    /// The member's state.
    pub fn state(&self) -> MemberState {
        self.state
    }

    pub(crate) fn new(node: NodeId, state: MemberState) -> Self {
        Self { node, state }
    }

    pub(crate) fn node(&self) -> NodeId {
        self.node
    }
}

impl Member {
    /// The largest encoding of one member, pinned by a test.
    pub(crate) const MAX_ENCODED_LEN: usize = 40;

    /// Headroom for the `Gossip` envelope: its variant tag, length varint and `more` flag.
    const ENVELOPE_LEN: usize = 16;
}

#[derive(Serialize, Deserialize)]
struct MemberRepr {
    addr: SocketAddr,
    incarnation: Incarnation,
    state: MemberState,
}

impl From<Member> for MemberRepr {
    fn from(member: Member) -> Self {
        Self {
            addr: member.addr(),
            incarnation: member.incarnation(),
            state: member.state(),
        }
    }
}

impl From<MemberRepr> for Member {
    fn from(repr: MemberRepr) -> Self {
        Self::new(NodeId::from_parts(repr.addr, repr.incarnation), repr.state)
    }
}

/// A cluster which cannot be joined.
#[derive(Debug, Error)]
pub enum JoinError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No seed addresses other than this node's own were given, and joining never forms.
    #[error("no seed addresses")]
    NoSeeds,

    /// This node's incarnation has been downed or has left; only a restarted process, with a
    /// fresh incarnation, can join again.
    #[error("this node has been downed")]
    Downed,

    /// Every seed tried is a member of another cluster than this node's. Two clusters never
    /// merge, so a node which has formed or joined one is never admitted by another.
    #[error("every seed is a member of another cluster")]
    ForeignCluster,
}

/// The members cannot be listed.
#[derive(Debug, Error)]
pub enum MembersError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,
}

/// A member cannot be downed.
#[derive(Debug, Error)]
pub enum DownError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No Up member advertises this address; during bootstrap gossip may not have carried that
    /// node's join yet, hence worth retrying.
    #[error("no Up member at {0}")]
    NotAMember(SocketAddr),

    /// The address is this node's own. A node ends its own membership with
    /// [leave](fn@crate::cluster::leave), which announces the departure instead of staging a death.
    #[error("cannot down this node, leave instead")]
    ThisNode,
}

/// Join the cluster one of the seed addresses is a member of. A started endpoint is no cluster,
/// and it becomes a member of one only through this function, [form](crate::cluster::form) or
/// [bootstrap](fn@crate::cluster::bootstrap), which itself forms or joins. A cluster is hence the
/// transitive closure of joins, and independent clusters must not share seeds.
///
/// The seeds are tried in order, with the reconnect backoff between rounds, until one admits
/// this node. There is no internal timeout, so wrap the call in [timeout](tokio::time::timeout)
/// to bound it. A seed which is a member of no cluster refuses, so a seed list shared by nodes
/// which are all still starting forms nothing on its own. This node's own address is skipped in
/// every position. Once a cluster has admitted this node, only that cluster is tried, even if the
/// attempt failed before its member snapshot arrived. No other seed can be proven to be that same
/// cluster, and another cluster refuses this node from then on. Joining again when already a
/// member refreshes the member snapshot, but the seed replaces its inbound connection from this
/// node with the join's, so this node's lane to that seed reconnects. A seed of another cluster
/// refuses a member. A seed holds back joins from new addresses while one of its members is
/// unreachable, and the round is retried until that member is reachable again or downed.
///
/// Only a restarted process recovers from [JoinError::Downed].
///
/// # Errors
/// Fails if the endpoint is not started, if the seeds name nothing but this node's own address,
/// or if this node has been downed or has left. Fails as well if every seed tried in a round is a
/// member of another cluster than this node's.
pub async fn join(seeds: &[SocketAddr]) -> Result<(), JoinError> {
    let endpoint = endpoint::get().ok_or(JoinError::EndpointNotStarted)?;
    if seeds.iter().all(|seed| *seed == endpoint.node().addr()) {
        return Err(JoinError::NoSeeds);
    }

    let mut attempts = 0u32;
    loop {
        match join_round(endpoint, seeds).await {
            JoinRound::Joined => return Ok(()),
            JoinRound::Downed => return Err(JoinError::Downed),
            JoinRound::ForeignCluster(_) => return Err(JoinError::ForeignCluster),
            JoinRound::ClusterSeen(..) | JoinRound::Held(..) | JoinRound::NoAdmission(_) => {}
        }

        attempts += 1;
        sleep(endpoint.reconnect_backoff(attempts)).await;
    }
}

/// What one pass over the seeds saw, which is what a formation decision is taken on.
pub(crate) enum JoinRound {
    Joined,

    /// The cluster at this address may count this node already, so only it may be tried and
    /// nothing may be formed until the attempt resolves.
    ClusterSeen(SocketAddr, Vec<(SocketAddr, JoinOutcome)>),

    /// The cluster at this address holds back joins while one of its members is unreachable, so
    /// it exists and nothing may be formed beside it.
    Held(SocketAddr, Vec<(SocketAddr, JoinOutcome)>),

    NoAdmission(Vec<(SocketAddr, JoinOutcome)>),

    /// Every seed tried is a member of another cluster than this node's.
    ForeignCluster(Vec<(SocketAddr, JoinOutcome)>),

    Downed,
}

/// One pass over the seeds, this node's own address skipped in every position: a formed node
/// dialing itself would admit itself and never reach the real seeds.
pub(crate) async fn join_round(
    endpoint: &'static EndpointInner,
    seeds: &[SocketAddr],
) -> JoinRound {
    let seeds = match endpoint.pinned_cluster() {
        Some(pinned) => vec![pinned],

        None => seeds
            .iter()
            .copied()
            .filter(|seed| *seed != endpoint.node().addr())
            .collect(),
    };

    let mut outcomes = Vec::with_capacity(seeds.len());
    let mut foreign = 0;
    for seed in seeds {
        if endpoint.downed() {
            return JoinRound::Downed;
        }

        match join_via(endpoint, seed).await {
            Ok(()) => return JoinRound::Joined,

            Err(ConnectError::Refused(RefusalReason::Down) | ConnectError::SelfDowned) => {
                return JoinRound::Downed;
            }

            Err(ConnectError::Refused(RefusalReason::NoCluster)) => {
                outcomes.push((seed, JoinOutcome::NoCluster));
            }

            Err(ConnectError::Refused(RefusalReason::JoinsHeld)) => {
                info!(seed_addr = %seed, "join held back, a member of the cluster is unreachable");
                outcomes.push((seed, JoinOutcome::Held));
                return JoinRound::Held(seed, outcomes);
            }

            Err(error) => {
                debug!(seed_addr = %seed, %error, "cannot join via seed");
                outcomes.push((seed, JoinOutcome::Unavailable));
                if let Some(pinned) = endpoint.pinned_cluster() {
                    return JoinRound::ClusterSeen(pinned, outcomes);
                }
                if matches!(
                    error,
                    ConnectError::Refused(RefusalReason::ForeignCluster)
                        | ConnectError::ForeignCluster(_)
                ) {
                    foreign += 1;
                }
            }
        }
    }

    if foreign > 0 && foreign == outcomes.len() {
        return JoinRound::ForeignCluster(outcomes);
    }
    JoinRound::NoAdmission(outcomes)
}

/// The members of the cluster as this node sees them, this node included, ordered by address and
/// incarnation. A member this node saw Up is listed Left or Down until its retention expires; one
/// which was Left or Down before this node learned of it is kept only as its address's fence,
/// hence not listed.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn members() -> Result<Vec<Member>, MembersError> {
    endpoint::get()
        .map(|endpoint| endpoint.state_rx().borrow().members().to_vec())
        .ok_or(MembersError::EndpointNotStarted)
}

/// Down the Up member at the given address, running the node death sequence: pending asks toward
/// it fail, its watchers receive synthesized terminated signals, and its incarnation is refused
/// from then on.
///
/// # Errors
/// Fails if the endpoint is not started, if no Up member advertises the address, or if the
/// address is this node's own, which [leave](fn@crate::cluster::leave) is for.
pub fn down(addr: SocketAddr) -> Result<(), DownError> {
    let endpoint = endpoint::get().ok_or(DownError::EndpointNotStarted)?;

    let node = endpoint
        .membership()
        .up_member_at(addr)
        .ok_or(DownError::NotAMember(addr))?;
    if node == endpoint.node() {
        return Err(DownError::ThisNode);
    }

    warn!(peer = %node, "node death, downed manually");
    endpoint.node_death(node);

    Ok(())
}

/// Entry state is a monotone lattice per incarnation (`Up < Left < Down`), so gossip merge
/// converges.
pub(crate) struct Membership {
    node: NodeId,
    entries: RwLock<Entries>,
}

impl Membership {
    pub(crate) fn new(
        node: NodeId,
        down_watermarks: NonZeroUsize,
        down_retention: Duration,
    ) -> Self {
        let mut members = HashMap::new();
        members.insert(node, MemberEntry::up());

        Self {
            node,
            entries: RwLock::new(Entries {
                members,
                version: 0,
                fences: HashMap::new(),
                down_watermarks,
                down_retention,
                above_cap: false,
            }),
        }
    }

    pub(crate) fn is_up(&self, node: NodeId) -> bool {
        self.while_up(node, |up| up)
    }

    /// Left counts as dead here: only the receive side treats a departure differently.
    pub(crate) fn is_down(&self, node: NodeId) -> bool {
        read(&self.entries).is_down(node)
    }

    /// The node's own state, from its detailed entry or else from a fence naming it exactly.
    pub(crate) fn state(&self, node: NodeId) -> Option<MemberState> {
        let entries = read(&self.entries);
        let detailed = entries.members.get(&node).map(|entry| entry.state);
        let fenced = entries
            .fences
            .get(&node.addr())
            .filter(|fence| fence.node == node)
            .map(|fence| fence.state);
        detailed.max(fenced)
    }

    /// Runs `f` with the node's Up state while no membership change can land, so what `f`
    /// stores or publishes is consistent with the state it was given.
    pub(crate) fn while_up<R>(&self, node: NodeId, f: impl FnOnce(bool) -> R) -> R {
        let entries = read(&self.entries);
        let up = entries
            .members
            .get(&node)
            .is_some_and(|entry| entry.state == MemberState::Up);
        f(up)
    }

    pub(crate) fn up_member_at(&self, addr: SocketAddr) -> Option<NodeId> {
        read(&self.entries).up_at(addr)
    }

    pub(crate) fn has_up_member_at(&self, addr: SocketAddr) -> bool {
        self.up_member_at(addr).is_some()
    }

    pub(crate) fn up_peers(&self) -> Vec<NodeId> {
        read(&self.entries)
            .members
            .iter()
            .filter(|(node, entry)| entry.state == MemberState::Up && **node != self.node)
            .map(|(node, _)| *node)
            .collect()
    }

    pub(crate) fn up_nodes(&self) -> HashSet<NodeId> {
        read(&self.entries).up_nodes()
    }

    pub(crate) fn members(&self) -> Vec<Member> {
        read(&self.entries).members()
    }

    /// The members together with the version counting their changes, read at once.
    pub(crate) fn versioned_members(&self) -> (u64, Vec<Member>) {
        let entries = read(&self.entries);
        (entries.version, entries.members())
    }

    /// Only connection setup may send this: watermarks in the heartbeat would grow forever.
    pub(crate) fn handshake_snapshot(&self) -> Vec<Member> {
        let entries = read(&self.entries);
        entries
            .members
            .iter()
            .map(|(node, entry)| Member::new(*node, entry.state))
            .chain(
                entries
                    .fences
                    .values()
                    .filter(|fence| !entries.members.contains_key(&fence.node))
                    .map(|fence| Member::new(fence.node, fence.state)),
            )
            .collect()
    }

    /// Holds back a join from a new address while an Up member is outside this node's component,
    /// see [Reachability::outside_component].
    pub(crate) fn admit_join(&self, node: NodeId, reachability: &Reachability) -> JoinAdmission {
        let now = Instant::now();
        let mut entries = write(&self.entries);
        let admission = entries.admit_join(self.node, node, reachability, now);
        entries.evict_fences(now);
        admission
    }

    #[cfg(test)]
    pub(crate) fn add_up(&self, node: NodeId) -> bool {
        let mut entries = write(&self.entries);
        if entries.is_down(node) {
            return false;
        }

        match entries.members.entry(node) {
            Entry::Occupied(_) => false,

            Entry::Vacant(entry) => {
                entry.insert(MemberEntry::up());
                entries.version += 1;
                true
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn fences(&self) -> HashSet<NodeId> {
        read(&self.entries)
            .fences
            .values()
            .map(|fence| fence.node)
            .collect()
    }

    pub(crate) fn down(&self, node: NodeId, now: Instant) -> DownOutcome {
        self.retire(node, MemberState::Down, now)
    }

    pub(crate) fn leave(&self, node: NodeId, now: Instant) -> DownOutcome {
        self.retire(node, MemberState::Left, now)
    }

    pub(crate) fn merge(&self, members: &[Member], now: Instant) -> Merge {
        let mut entries = write(&self.entries);
        // No Down before the scan ends, else a later explicit Down of the same node is not flushed.
        let scan = entries.scan(self.node, members, now);
        let downed = scan
            .to_down
            .iter()
            .map(|(node, state)| (*node, entries.retire(*node, *state, now)))
            .collect();
        if scan.self_down {
            entries.down(self.node, now);
        }
        entries.evict_fences(now);

        Merge {
            new_up: scan.new_up,
            downed,
            to_flush: scan.to_flush,
            self_down: scan.self_down,
        }
    }

    /// Drops detailed Down entries older than the retention; their address watermarks remain,
    /// unless they are above the cap and older than the retention too.
    pub(crate) fn sweep(&self, now: Instant) {
        let mut entries = write(&self.entries);
        let retention = entries.down_retention;
        let before = entries.members.len();
        entries.members.retain(|_, entry| {
            let expired =
                entry.state != MemberState::Up && now.duration_since(entry.changed_at) >= retention;
            !expired
        });
        if entries.members.len() != before {
            entries.version += 1;
        }
        entries.evict_fences(now);
    }

    fn retire(&self, node: NodeId, state: MemberState, now: Instant) -> DownOutcome {
        let mut entries = write(&self.entries);
        let outcome = entries.retire(node, state, now);
        entries.evict_fences(now);
        outcome
    }
}

/// The most watermarks a node holds, twice `down_watermarks`.
pub(crate) fn max_fences(down_watermarks: NonZeroUsize) -> usize {
    down_watermarks.get().saturating_mul(2)
}

/// The smallest `max_frame_size` [snapshot_frames] can respect, one member plus its envelope.
pub(crate) const MIN_FRAME_SIZE: usize = Member::MAX_ENCODED_LEN + Member::ENVELOPE_LEN;

/// Chunks a snapshot into `Gossip` frames, all but the last marked `more`; arrival order is free.
pub(crate) fn snapshot_frames(members: Vec<Member>, max_frame_size: usize) -> Vec<Frame<'static>> {
    let per_chunk = max_frame_size
        .saturating_sub(Member::ENVELOPE_LEN)
        .div_euclid(Member::MAX_ENCODED_LEN)
        .max(1);
    if members.len() <= per_chunk {
        return vec![Frame::Gossip {
            members,
            more: false,
        }];
    }

    let chunks = members.len().div_ceil(per_chunk);
    members
        .chunks(per_chunk)
        .enumerate()
        .map(|(index, chunk)| Frame::Gossip {
            members: chunk.to_vec(),
            more: index + 1 < chunks,
        })
        .collect()
}

/// What [Membership::merge] changed, for its caller to act on outside the membership lock.
pub(crate) struct Merge {
    pub(crate) new_up: Vec<NodeId>,
    pub(crate) downed: Vec<(NodeId, DownOutcome)>,
    pub(crate) to_flush: Vec<NodeId>,
    pub(crate) self_down: bool,
}

/// `state` is what the node holds afterward, Left or Down.
#[derive(Clone, Copy)]
pub(crate) struct DownOutcome {
    pub(crate) member_changed: bool,
    pub(crate) watermark_changed: bool,
    pub(crate) state: MemberState,
}

pub(crate) enum JoinAdmission {
    Admitted {
        superseded: Option<(NodeId, DownOutcome)>,
    },
    AlreadyUp,
    Dead,
    Held {
        unreachable: NodeId,
    },
    Stale {
        outcome: DownOutcome,
    },
    SupersedesThisNode,
}

/// The merge's consequences run out here, where no membership lock is held. A departure is
/// deferred until the node's inbound connections here have drained, including one it is yet to
/// open to drain, see [EndpointInner::defer_departure].
pub(crate) fn on_gossip(endpoint: &'static EndpointInner, members: &[Member]) {
    let (merge, deferred) = endpoint
        .update(|membership, reachability| {
            let mut departures = endpoint.departures();
            let merge = membership.merge(members, Instant::now());
            for fence in merge
                .downed
                .iter()
                .map(|(node, _)| node)
                .chain(&merge.to_flush)
            {
                reachability.prune_fenced(*fence);
            }

            let deferred = merge
                .downed
                .iter()
                .map(|(node, outcome)| (*node, Some(outcome.state), true))
                .chain(
                    merge
                        .to_flush
                        .iter()
                        .map(|node| (*node, membership.state(*node), false)),
                )
                .filter(|(_, state, _)| *state == Some(MemberState::Left))
                .map(|(node, _, was_up)| {
                    let open = endpoint.defer_departure(&mut departures, node, was_up);
                    (node, open)
                })
                .collect::<HashMap<_, _>>();
            (merge, deferred)
        })
        .result;

    if merge.self_down {
        for peer in merge.to_flush {
            endpoint.flush_fenced(peer);
        }
        endpoint.self_down();
        return;
    }

    let changed = !merge.new_up.is_empty() || !merge.downed.is_empty();
    for node in merge.new_up {
        endpoint.unrefuse(node.addr());
        // Called for the arming side effect; no inbound delivery to gate here.
        let _ = endpoint.track_liveness(node);
        endpoint.receptionist().origin_up(node);
    }
    for &(node, outcome) in &merge.downed {
        match deferred.get(&node) {
            Some(true) => {
                info!(peer = %node, "node departed, delivering what its connection still carries");
            }

            Some(false) => {
                info!(peer = %node, "node departed, waiting for its connection to drain");
            }

            None => {
                warn!(peer = %node, "node death, downed via gossip");
                endpoint.death_consequences(node, outcome);
            }
        }
    }
    for peer in merge.to_flush {
        if !deferred.contains_key(&peer) && !merge.downed.iter().any(|(node, _)| *node == peer) {
            endpoint.flush_fenced(peer);
        }
    }

    if changed {
        endpoint.push_gossip();
    }
}

struct Entries {
    members: HashMap<NodeId, MemberEntry>,
    /// Counts the changes to `members`.
    version: u64,
    /// The greatest Left or Down incarnation per address.
    fences: HashMap<SocketAddr, Fence>,
    down_watermarks: NonZeroUsize,
    down_retention: Duration,
    above_cap: bool,
}

impl Entries {
    fn admit_join(
        &mut self,
        this_node: NodeId,
        node: NodeId,
        reachability: &Reachability,
        now: Instant,
    ) -> JoinAdmission {
        if self.is_down(node) {
            return JoinAdmission::Dead;
        }
        if self.members.contains_key(&node) {
            return JoinAdmission::AlreadyUp;
        }

        let superseded = match self.up_at(node.addr()) {
            Some(previous) if previous == this_node => {
                return if previous.incarnation() < node.incarnation() {
                    JoinAdmission::SupersedesThisNode
                } else {
                    JoinAdmission::Stale {
                        outcome: self.down(node, now),
                    }
                };
            }

            Some(previous) if node.incarnation() < previous.incarnation() => {
                return JoinAdmission::Stale {
                    outcome: self.down(node, now),
                };
            }

            Some(previous) => Some((previous, self.down(previous, now))),

            None => {
                if let Some(unreachable) = reachability.outside_component(&self.up_nodes()) {
                    return JoinAdmission::Held { unreachable };
                }
                None
            }
        };

        self.members.insert(node, MemberEntry::up());
        self.version += 1;
        JoinAdmission::Admitted { superseded }
    }

    fn scan(&mut self, this_node: NodeId, members: &[Member], now: Instant) -> Scan {
        let mut scan = Scan::default();

        for member in members {
            match member.state {
                MemberState::Left if member.node == this_node => {}

                MemberState::Left | MemberState::Down => {
                    if self.merge_fence(member.node, member.state, now) {
                        scan.to_flush.push(member.node);
                    }
                    if member.node.addr() == this_node.addr()
                        && this_node.incarnation() <= member.node.incarnation()
                    {
                        scan.self_down = true;
                    } else {
                        scan.to_down.extend(
                            self.members
                                .iter()
                                .filter(|(node, entry)| {
                                    member.node.covers(**node)
                                        && (entry.state == MemberState::Up
                                            || **node == member.node && entry.state < member.state)
                                })
                                .map(|(node, _)| {
                                    let state = if *node == member.node {
                                        member.state
                                    } else {
                                        MemberState::Down
                                    };
                                    (*node, state)
                                }),
                        );
                    }
                }

                MemberState::Up => {
                    if member.node == this_node
                        || self.is_down(member.node)
                        || self.members.contains_key(&member.node)
                    {
                        continue;
                    }

                    if let Some(superseded) = self.up_at(member.node.addr()) {
                        if superseded.incarnation() < member.node.incarnation() {
                            if superseded == this_node {
                                scan.self_down = true;
                                return scan;
                            }

                            scan.to_down.push((superseded, MemberState::Down));
                        } else {
                            scan.to_down.push((member.node, MemberState::Down));
                            continue;
                        }
                    }

                    self.members.insert(member.node, MemberEntry::up());
                    self.version += 1;
                    scan.new_up.push(member.node);
                }
            }
        }

        scan
    }

    fn down(&mut self, node: NodeId, now: Instant) -> DownOutcome {
        self.retire(node, MemberState::Down, now)
    }

    /// Ordered by address, then incarnation.
    fn members(&self) -> Vec<Member> {
        let mut members = self
            .members
            .iter()
            .map(|(node, entry)| Member::new(*node, entry.state))
            .collect::<Vec<_>>();
        members.sort_by_key(|member| (member.node.addr(), member.node.incarnation()));
        members
    }

    /// Moves the node to `state`, Left or Down, never backward along the lattice.
    fn retire(&mut self, node: NodeId, state: MemberState, now: Instant) -> DownOutcome {
        let watermark_changed = self.merge_fence(node, state, now);
        let member_changed = self
            .members
            .get_mut(&node)
            .filter(|entry| entry.state < state)
            .is_some_and(|entry| {
                entry.state = state;
                entry.changed_at = now;
                true
            });
        if member_changed {
            self.version += 1;
        }
        let state = self
            .members
            .get(&node)
            .map_or(state, |entry| entry.state.max(state));
        DownOutcome {
            member_changed,
            watermark_changed,
            state,
        }
    }

    fn is_down(&self, node: NodeId) -> bool {
        self.fences
            .get(&node.addr())
            .is_some_and(|fence| fence.node.covers(node))
            || self
                .members
                .get(&node)
                .is_some_and(|entry| entry.state != MemberState::Up)
    }

    fn up_nodes(&self) -> HashSet<NodeId> {
        self.members
            .iter()
            .filter(|(_, entry)| entry.state == MemberState::Up)
            .map(|(node, _)| *node)
            .collect()
    }

    fn up_at(&self, addr: SocketAddr) -> Option<NodeId> {
        self.members
            .iter()
            .find(|(node, entry)| node.addr() == addr && entry.state == MemberState::Up)
            .map(|(node, _)| *node)
    }

    /// Reports a newly learned death even if the ceiling evicts its fence: its state needs
    /// flushing.
    fn merge_fence(&mut self, node: NodeId, state: MemberState, now: Instant) -> bool {
        let fence = Fence {
            node,
            state,
            learned_at: now,
        };
        match self.fences.entry(node.addr()) {
            Entry::Occupied(mut entry) if entry.get().node.incarnation() < node.incarnation() => {
                entry.insert(fence);
                true
            }

            Entry::Occupied(mut entry) if entry.get().node == node && entry.get().state < state => {
                entry.get_mut().state = state;
                true
            }

            Entry::Occupied(_) => false,

            Entry::Vacant(entry) => {
                entry.insert(fence);
                true
            }
        }
    }

    /// Above the cap, evicts the earliest learned fences which have outlived the retention, and
    /// above the ceiling the earliest learned regardless.
    fn evict_fences(&mut self, now: Instant) {
        let cap = self.down_watermarks.get();
        if self.fences.len() <= cap {
            self.above_cap = false;
            return;
        }

        let mut fences = self.fences.values().copied().collect::<Vec<_>>();
        fences.sort_unstable_by_key(|fence| (fence.learned_at, fence.node.incarnation()));
        let expired = fences
            .iter()
            .take_while(|fence| now.duration_since(fence.learned_at) >= self.down_retention)
            .count();
        let above_ceiling = fences
            .len()
            .saturating_sub(max_fences(self.down_watermarks));
        let evicted = (fences.len() - cap).min(expired).max(above_ceiling);
        for fence in &fences[..evicted] {
            self.fences.remove(&fence.node.addr());
        }

        let above_cap = self.fences.len() > cap;
        if above_cap && !self.above_cap {
            warn!(
                fences = self.fences.len(),
                down_watermarks = cap,
                "down watermarks above down_watermarks, keeping those learned within down_retention"
            );
        }
        self.above_cap = above_cap;
    }
}

/// A Left or Down incarnation fencing its address, and when this node learned of it.
#[derive(Clone, Copy)]
struct Fence {
    node: NodeId,
    state: MemberState,
    learned_at: Instant,
}

struct MemberEntry {
    state: MemberState,
    changed_at: Instant,
}

#[derive(Default)]
struct Scan {
    new_up: Vec<NodeId>,
    to_down: Vec<(NodeId, MemberState)>,
    to_flush: Vec<NodeId>,
    self_down: bool,
}

impl MemberEntry {
    fn up() -> Self {
        Self {
            state: MemberState::Up,
            changed_at: Instant::now(),
        }
    }
}

async fn join_via(endpoint: &'static EndpointInner, addr: SocketAddr) -> Result<(), ConnectError> {
    let (result_tx, result_rx) = oneshot::channel();
    endpoint.request_join(JoinRequest { addr, result_tx });

    match result_rx.await {
        Ok(result) => result,
        Err(_) => Err(ConnectError::Closed),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, MailboxCapacity,
        cluster::{
            endpoint::{EndpointConfig, EndpointInner},
            frame::Frame,
            membership::{
                JoinAdmission, MIN_FRAME_SIZE, Member, MemberState, Membership, Merge, on_gossip,
                snapshot_frames,
            },
            node::NodeId,
            reachability::{Reachability, WireReachability},
        },
        mailbox::make_mailbox,
        watch::Watcher,
    };
    use std::{
        collections::HashSet,
        net::SocketAddr,
        num::NonZeroUsize,
        time::{Duration, Instant},
    };

    const RETENTION: Duration = Duration::from_secs(300);

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    /// The member list starts with this node Up, which is what the endpoint gossips and counts
    /// once it is a member; being a member at all is the endpoint's lifecycle, not this.
    #[test]
    fn a_new_membership_lists_this_node_up() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);

        assert!(membership.is_up(node));
        assert_eq!(membership.members().len(), 1);
        assert!(membership.up_peers().is_empty());
    }

    /// The lattice never moves backward: Down wins over Up, once Down always Down, and merging
    /// is idempotent, so duplicated or reordered gossip cannot flip a state.
    #[test]
    fn down_is_terminal_and_merge_is_idempotent() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let peer = NodeId::new(addr(2));

        assert!(membership.add_up(peer));
        assert!(!membership.add_up(peer));
        assert!(membership.down(peer, Instant::now()).member_changed);
        assert!(!membership.down(peer, Instant::now()).member_changed);
        assert!(!membership.add_up(peer));
        assert!(membership.is_down(peer));

        let merge = membership.merge(&[Member::new(peer, MemberState::Up)], Instant::now());
        assert!(merge.new_up.is_empty());
        assert!(merge.downed.is_empty());

        let merge = membership.merge(&[Member::new(peer, MemberState::Down)], Instant::now());
        assert!(merge.downed.is_empty());
    }

    /// Merge folds unknown Down entries into the watermark map without detailed membership.
    #[test]
    fn merge_applies_up_and_folds_downs_into_the_watermark() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);
        let (up, dead) = (NodeId::new(addr(2)), NodeId::new(addr(3)));

        let merge = membership.merge(
            &[
                Member::new(up, MemberState::Up),
                Member::new(dead, MemberState::Down),
                Member::new(node, MemberState::Up),
            ],
            Instant::now(),
        );
        assert_eq!(merge.new_up, vec![up]);
        assert!(merge.downed.is_empty());
        assert_eq!(merge.to_flush, vec![dead]);
        assert!(!merge.self_down);
        assert!(membership.is_up(up));
        assert!(membership.is_down(dead));
        assert!(
            !membership
                .members()
                .iter()
                .any(|member| member.node == dead)
        );

        let merge = membership.merge(&[Member::new(node, MemberState::Down)], Instant::now());
        assert!(merge.self_down);
    }

    /// A departure is gossiped as the leaver's own entry moved to Left, and the receiver merges
    /// it as a node death which it may defer; its own Left echoed back is no news to the leaver.
    #[test]
    fn a_departure_is_gossiped_left_and_merged_as_a_node_death() {
        let leaver = NodeId::new(addr(1));
        let membership = Membership::new(leaver, NonZeroUsize::MAX, RETENTION);
        assert!(membership.leave(leaver, Instant::now()).member_changed);

        let snapshot = membership.members();
        assert_eq!(snapshot, vec![Member::new(leaver, MemberState::Left)]);
        let echoed = membership.merge(&snapshot, Instant::now());
        assert!(!echoed.self_down);
        assert!(echoed.downed.is_empty());

        let peer = Membership::new(NodeId::new(addr(2)), NonZeroUsize::MAX, RETENTION);
        peer.add_up(leaver);

        let merge = peer.merge(&snapshot, Instant::now());
        assert_eq!(downed_nodes(&merge), vec![leaver]);
        assert!(matches!(merge.downed[0].1.state, MemberState::Left));
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
        assert!(peer.is_down(leaver));
        assert_eq!(peer.state(leaver), Some(MemberState::Left));
    }

    /// The lattice is `Up < Left < Down` per incarnation, for detailed entries and fences alike:
    /// a downing after a departure wins, a departure after a downing changes nothing, and Up
    /// never returns.
    #[test]
    fn a_downing_after_a_departure_wins_and_never_reverts() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let (detailed, fenced) = (NodeId::new(addr(2)), NodeId::new(addr(3)));
        assert!(membership.add_up(detailed));

        let merge = membership.merge(
            &[
                Member::new(detailed, MemberState::Left),
                Member::new(fenced, MemberState::Left),
            ],
            Instant::now(),
        );
        assert_eq!(outcomes(&merge), vec![(detailed, (true, false))]);
        assert_eq!(merge.to_flush, vec![detailed, fenced]);
        assert_eq!(membership.state(detailed), Some(MemberState::Left));
        assert_eq!(membership.state(fenced), Some(MemberState::Left));

        let merge = membership.merge(&[Member::new(detailed, MemberState::Up)], Instant::now());
        assert!(merge.new_up.is_empty() && merge.downed.is_empty());

        let merge = membership.merge(
            &[
                Member::new(detailed, MemberState::Down),
                Member::new(fenced, MemberState::Down),
            ],
            Instant::now(),
        );
        assert_eq!(outcomes(&merge), vec![(detailed, (true, false))]);
        assert!(matches!(merge.downed[0].1.state, MemberState::Down));
        assert_eq!(merge.to_flush, vec![detailed, fenced]);
        assert_eq!(membership.state(detailed), Some(MemberState::Down));
        assert_eq!(membership.state(fenced), Some(MemberState::Down));

        let merge = membership.merge(
            &[
                Member::new(detailed, MemberState::Left),
                Member::new(fenced, MemberState::Left),
            ],
            Instant::now(),
        );
        assert!(merge.downed.is_empty() && merge.to_flush.is_empty());
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(fenced, MemberState::Down))
        );
    }

    /// A departure of an incarnation downs its predecessors at the address: only the leaver
    /// itself departed.
    #[test]
    fn a_departure_downs_the_predecessors_it_covers() {
        let (membership, predecessor, successor) = superseded_pair();

        let merge = membership.merge(&[Member::new(successor, MemberState::Left)], Instant::now());

        assert_eq!(downed_nodes(&merge), vec![predecessor]);
        assert!(matches!(merge.downed[0].1.state, MemberState::Down));
        assert_eq!(membership.state(predecessor), Some(MemberState::Down));
        assert_eq!(membership.state(successor), Some(MemberState::Left));
    }

    /// A swept departure keeps its state in the fence, so a node learning it from the setup
    /// snapshot learns a departure, not a downing.
    #[test]
    fn a_swept_departure_stays_left_in_the_handshake_snapshot() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let leaver = NodeId::new(addr(2));
        membership.add_up(leaver);
        membership.leave(leaver, Instant::now());

        membership.sweep(Instant::now() + RETENTION);

        assert!(membership.is_down(leaver));
        assert_eq!(membership.members().len(), 1);
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(leaver, MemberState::Left))
        );
    }

    /// An older incarnation learned after the younger one at its address is downed rather than
    /// adopted, so both sides of a gossip exchange converge on the same member.
    #[test]
    fn merge_supersedes_toward_the_younger_incarnation() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        assert!(older.incarnation() < younger.incarnation());

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        assert!(membership.add_up(younger));
        let merge = membership.merge(&[Member::new(older, MemberState::Up)], Instant::now());
        assert_eq!(downed_nodes(&merge), vec![older]);
        assert!(membership.is_up(younger));
        assert!(!membership.is_up(older));
    }

    #[test]
    fn a_join_supersedes_toward_the_younger_incarnation() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        assert!(matches!(
            admit_join(&membership, older),
            JoinAdmission::Admitted { superseded: None }
        ));
        assert!(matches!(
            admit_join(&membership, younger),
            JoinAdmission::Admitted { superseded: Some((superseded, outcome)) }
                if superseded == older && outcome.member_changed
        ));
        assert!(membership.is_up(younger));
        assert!(membership.is_down(older));

        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        assert!(matches!(
            admit_join(&membership, younger),
            JoinAdmission::Admitted { superseded: None }
        ));
        assert!(matches!(
            admit_join(&membership, older),
            JoinAdmission::Stale { outcome } if outcome.watermark_changed
        ));
        assert!(membership.is_up(younger));
        assert!(membership.is_down(older));
        assert!(matches!(
            admit_join(&membership, older),
            JoinAdmission::Dead
        ));
        assert!(matches!(
            admit_join(&membership, younger),
            JoinAdmission::AlreadyUp
        ));
    }

    /// A join from a new address is held while an Up member is outside this node's component, but
    /// a younger incarnation at that member's address supersedes it all the same. A broken link
    /// between two members inside the component holds nothing.
    #[test]
    fn a_join_is_held_while_a_member_is_unreachable_unless_it_supersedes() {
        let node = NodeId::new(addr(1));
        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);
        let reachability = Reachability::new(node);
        let (relay, unreachable) = (NodeId::new(addr(2)), NodeId::new(addr(3)));
        assert!(membership.add_up(relay));
        assert!(membership.add_up(unreachable));
        reachability.observe(unreachable, false);
        reachability.merge(
            &[WireReachability {
                observer: relay,
                subject: unreachable,
                version: 1,
                reachable: false,
            }],
            &membership.up_nodes(),
        );

        let joiner = NodeId::new(addr(4));
        assert!(matches!(
            membership.admit_join(joiner, &reachability),
            JoinAdmission::Held { unreachable: held } if held == unreachable
        ));
        assert!(!membership.is_up(joiner));

        let successor = NodeId::new(addr(3));
        assert!(matches!(
            membership.admit_join(successor, &reachability),
            JoinAdmission::Admitted { superseded: Some((superseded, _)) } if superseded == unreachable
        ));
        assert!(membership.is_up(successor));

        reachability.merge(
            &[WireReachability {
                observer: relay,
                subject: successor,
                version: 2,
                reachable: false,
            }],
            &membership.up_nodes(),
        );
        assert!(matches!(
            membership.admit_join(joiner, &reachability),
            JoinAdmission::Admitted { superseded: None }
        ));
        assert!(membership.is_up(joiner));
    }

    #[test]
    fn a_join_at_this_nodes_address_never_adds_a_second_incarnation() {
        let shared = addr(1);
        let older = NodeId::new(shared);
        let node = NodeId::new(shared);
        let younger = NodeId::new(shared);

        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);
        assert!(matches!(
            admit_join(&membership, younger),
            JoinAdmission::SupersedesThisNode
        ));
        assert!(matches!(
            admit_join(&membership, older),
            JoinAdmission::Stale { .. }
        ));
        assert!(membership.is_up(node));
        assert!(!membership.is_up(younger));
        assert!(!membership.is_up(older));
    }

    /// This node's own address takes part in supersession: a predecessor gossiped Up without its
    /// tombstone, e.g. by a seed which never learned of it, is downed. Adopted instead, it would
    /// be a phantom member this node would dial and list as a registration origin.
    #[test]
    fn merge_downs_a_predecessor_at_this_nodes_address() {
        let shared = addr(1);
        let predecessor = NodeId::new(shared);
        let node = NodeId::new(shared);
        assert!(predecessor.incarnation() < node.incarnation());

        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);

        let merge = membership.merge(&[Member::new(predecessor, MemberState::Up)], Instant::now());

        assert_eq!(downed_nodes(&merge), vec![predecessor]);
        assert!(merge.new_up.is_empty());
        assert!(!merge.self_down);
        assert!(!membership.is_up(predecessor));
        assert!(membership.up_peers().is_empty());
    }

    /// Retention forgets the entry but keeps its watermark, which leaves the periodic snapshot.
    #[test]
    fn retention_sweeps_down_entries_but_keeps_the_watermark() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let peer = NodeId::new(addr(2));
        membership.add_up(peer);
        membership.down(peer, Instant::now());
        assert!(membership.is_down(peer));

        membership.sweep(Instant::now());
        assert!(membership.is_down(peer));
        assert!(
            membership
                .members()
                .contains(&Member::new(peer, MemberState::Down))
        );

        membership.sweep(Instant::now() + RETENTION);
        assert!(membership.is_down(peer));
        assert_eq!(membership.members().len(), 1);
        assert_eq!(
            membership.members(),
            vec![Member::new(membership.node, MemberState::Up)]
        );

        let handshake = membership.handshake_snapshot();
        assert_eq!(handshake.len(), 2);
        assert!(handshake.contains(&Member::new(peer, MemberState::Down)));
        assert!(handshake.contains(&Member::new(membership.node, MemberState::Up)));
    }

    /// A watermark fences its incarnation and every predecessor, but admits a restart.
    #[test]
    fn a_down_watermark_fences_older_incarnations_and_admits_a_restart() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let stale = NodeId::new(addr(2));
        let downed = NodeId::new(addr(2));
        let restarted = NodeId::new(addr(2));

        assert!(stale.incarnation() < downed.incarnation());
        assert!(downed.incarnation() < restarted.incarnation());
        assert!(membership.add_up(downed));
        assert!(membership.down(downed, Instant::now()).member_changed);
        membership.sweep(Instant::now() + RETENTION);

        assert!(membership.is_down(stale));
        assert!(membership.is_down(downed));
        assert!(!membership.add_up(stale));
        assert!(!membership.add_up(downed));
        assert!(membership.add_up(restarted));
        assert!(membership.is_up(restarted));
    }

    /// A newer Down watermark fences and terminates an older local Up incarnation.
    #[test]
    fn a_newer_down_watermark_terminates_an_older_up_member() {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let older = NodeId::new(addr(2));
        let newer = NodeId::new(addr(2));
        assert!(older.incarnation() < newer.incarnation());
        assert!(membership.add_up(older));

        let merge = membership.merge(&[Member::new(newer, MemberState::Down)], Instant::now());

        assert_eq!(outcomes(&merge), vec![(older, (true, false))]);
        assert_eq!(merge.to_flush, vec![newer]);
        assert!(membership.is_down(older));
        assert!(membership.is_down(newer));
        assert!(!membership.down(older, Instant::now()).member_changed);
        assert!(!membership.is_up(older));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(newer, MemberState::Down))
        );
    }

    /// The widest member must fit the bound the chunking divides by, else a chunk oversizes.
    #[test]
    fn a_member_never_encodes_beyond_the_chunking_bound() {
        let widest = Member {
            node: NodeId::new(
                "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                    .parse()
                    .expect("valid address"),
            ),
            state: MemberState::Down,
        };

        let len = postcard::experimental::serialized_size(&widest).expect("member size");

        assert!(len <= Member::MAX_ENCODED_LEN, "{len} exceeds the bound");
    }

    /// Chunking bounds every frame, loses no member, keeps the order, and marks every chunk but
    /// the last `more`, which is what stops a joiner reading a truncated snapshot.
    #[test]
    fn chunking_bounds_each_gossip_frame_and_marks_all_but_the_last() {
        let members = (0..100)
            .map(|port| Member::new(NodeId::new(addr(port + 1)), MemberState::Down))
            .collect::<Vec<_>>();
        let max_frame_size = 1_000;

        let frames = snapshot_frames(members.clone(), max_frame_size);

        assert!(frames.len() > 1);
        let mut rejoined = Vec::<Member>::new();
        for (index, frame) in frames.iter().enumerate() {
            let len = frame.encoded_len().expect("frame size");
            assert!(len <= max_frame_size, "{len} exceeds {max_frame_size}");
            let Frame::Gossip { members, more } = frame else {
                panic!("not a gossip frame");
            };
            assert_eq!(*more, index + 1 < frames.len());
            rejoined.extend(members.iter().copied());
        }
        assert_eq!(rejoined, members);

        let single = snapshot_frames(members, 1_000_000);
        assert_eq!(single.len(), 1);
        assert!(matches!(single[0], Frame::Gossip { more: false, .. }));
        assert!(matches!(
            snapshot_frames(Vec::new(), 1).as_slice(),
            [Frame::Gossip { members, more: false }] if members.is_empty()
        ));
    }

    /// At the smallest size [start_endpoint](crate::cluster::start_endpoint) admits, a chunk of the
    /// widest possible members still encodes within it, so the floor of one member per chunk
    /// cannot oversize.
    #[test]
    fn the_minimum_frame_size_holds_a_chunk_of_the_widest_members() {
        let widest = NodeId::new(
            "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"
                .parse()
                .expect("valid address"),
        );
        let members = vec![Member::new(widest, MemberState::Down); 10];

        let frames = snapshot_frames(members, MIN_FRAME_SIZE);

        assert!(frames.len() > 1);
        for frame in &frames {
            let len = frame.encoded_len().expect("frame size");
            assert!(len <= MIN_FRAME_SIZE, "{len} exceeds {MIN_FRAME_SIZE}");
        }
    }

    /// A node absent past the retention learns the death from the connection snapshot alone.
    #[test]
    fn a_swept_fence_reaches_an_absent_node_through_the_handshake_snapshot() {
        let downed = NodeId::new(addr(2));
        let cluster = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        cluster.add_up(downed);
        cluster.down(downed, Instant::now());
        cluster.sweep(Instant::now() + RETENTION);

        let absent = Membership::new(NodeId::new(addr(3)), NonZeroUsize::MAX, RETENTION);
        absent.add_up(downed);

        let merge = absent.merge(&cluster.members(), Instant::now());
        assert!(merge.downed.is_empty());
        assert!(merge.to_flush.is_empty());
        assert!(absent.is_up(downed));

        let merge = absent.merge(&cluster.handshake_snapshot(), Instant::now());

        assert_eq!(outcomes(&merge), vec![(downed, (true, false))]);
        assert_eq!(merge.to_flush, vec![downed]);
        assert!(absent.is_down(downed));
        assert!(!absent.down(downed, Instant::now()).member_changed);
        assert!(!absent.is_up(downed));
    }

    /// A covering watermark at this node's address makes this node down itself.
    #[test]
    fn merge_self_downs_against_a_down_watermark_at_its_address() {
        let shared = addr(1);
        let node = NodeId::new(shared);
        let successor = NodeId::new(shared);
        assert!(node.incarnation() < successor.incarnation());
        let membership = Membership::new(node, NonZeroUsize::MAX, RETENTION);

        let merge = membership.merge(&[Member::new(successor, MemberState::Down)], Instant::now());

        assert!(merge.self_down);
        assert_eq!(merge.to_flush, vec![successor]);
        assert!(membership.is_down(node));
        assert!(
            membership
                .handshake_snapshot()
                .contains(&Member::new(successor, MemberState::Down))
        );
    }

    /// A detailed Down entry refuses its incarnation by itself, so a fence lost to the ceiling
    /// within the retention cannot readmit it, neither by a join nor by gossip.
    #[test]
    fn a_retained_down_entry_refuses_its_node_after_its_fence_is_evicted() {
        let membership = capped(1);
        let start = Instant::now();
        let evicted = NodeId::new(addr(2));
        let downed = [evicted, NodeId::new(addr(3)), NodeId::new(addr(4))];
        for (node, secs) in downed.into_iter().zip(0..) {
            assert!(membership.add_up(node));
            membership.down(node, start + Duration::from_secs(secs));
        }
        assert!(!membership.fences().contains(&evicted));

        assert!(membership.is_down(evicted));
        assert!(matches!(
            admit_join(&membership, evicted),
            JoinAdmission::Dead
        ));
        let merge = membership.merge(&[Member::new(evicted, MemberState::Up)], start);
        assert!(merge.new_up.is_empty());
        assert!(!membership.is_up(evicted));
    }

    /// Above the cap the fence learned first and held past the retention goes, whatever its
    /// incarnation, and hearing it again does not renew it.
    #[test]
    fn a_later_learned_fence_outlives_an_expired_one() {
        let membership = capped(1);
        let start = Instant::now();
        let older = NodeId::new(addr(3));
        let expired = NodeId::new(addr(2));
        assert!(older.incarnation() < expired.incarnation());

        membership.merge(&[Member::new(expired, MemberState::Down)], start);
        membership.merge(
            &[Member::new(expired, MemberState::Down)],
            start + RETENTION,
        );
        membership.merge(&[Member::new(older, MemberState::Down)], start + RETENTION);

        assert_eq!(membership.fences(), HashSet::from([older]));
    }

    /// A newer incarnation at a fenced address restarts the retention of that address's fence.
    #[test]
    fn a_newer_incarnation_renews_its_fence() {
        let membership = capped(1);
        let start = Instant::now();
        let other = NodeId::new(addr(3));
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));

        membership.merge(&[Member::new(predecessor, MemberState::Down)], start);
        membership.merge(
            &[Member::new(successor, MemberState::Down)],
            start + RETENTION,
        );
        membership.merge(&[Member::new(other, MemberState::Down)], start + RETENTION);

        assert_eq!(membership.fences(), HashSet::from([successor, other]));
    }

    /// Fences within the retention are kept above the cap up to the ceiling, which evicts the
    /// earliest learned first.
    #[test]
    fn the_ceiling_evicts_the_earliest_learned_fences() {
        let membership = capped(2);
        let start = Instant::now();
        let mut nodes = (2..7)
            .map(|port| NodeId::new(addr(port)))
            .collect::<Vec<_>>();
        nodes.reverse();

        for (node, secs) in nodes.iter().zip(0..) {
            membership.merge(
                &[Member::new(*node, MemberState::Down)],
                start + Duration::from_secs(secs),
            );
        }

        assert_eq!(
            membership.fences(),
            nodes[1..].iter().copied().collect::<HashSet<_>>()
        );
    }

    /// A sweep evicts the fences above the cap which have outlived the retention.
    #[test]
    fn a_sweep_evicts_expired_fences_above_the_cap() {
        let membership = capped(1);
        let start = Instant::now();
        let (expired, kept) = (NodeId::new(addr(2)), NodeId::new(addr(3)));
        membership.merge(&[Member::new(expired, MemberState::Down)], start);
        membership.merge(
            &[Member::new(kept, MemberState::Down)],
            start + Duration::from_secs(1),
        );

        membership.sweep(start + RETENTION);

        assert_eq!(membership.fences(), HashSet::from([kept]));
    }

    /// Once both the detailed entry and the fence are gone, the address has lost its memory of
    /// the death and admits the zombie again, the documented fallback of the cap.
    #[test]
    fn a_zombie_is_admitted_once_its_entry_is_swept_and_its_fence_evicted() {
        let membership = capped(1);
        let start = Instant::now();
        let zombie = NodeId::new(addr(2));
        assert!(membership.add_up(zombie));
        membership.down(zombie, start);
        membership.sweep(start + RETENTION);
        assert!(membership.is_down(zombie));

        membership.down(NodeId::new(addr(3)), start + RETENTION);

        assert!(!membership.is_down(zombie));
        assert!(matches!(
            admit_join(&membership, zombie),
            JoinAdmission::Admitted { superseded: None }
        ));
    }

    fn capped(down_watermarks: usize) -> Membership {
        Membership::new(
            NodeId::new(addr(1)),
            NonZeroUsize::new(down_watermarks).expect("the cap is not zero"),
            RETENTION,
        )
    }

    fn outcomes(merge: &Merge) -> Vec<(NodeId, (bool, bool))> {
        merge
            .downed
            .iter()
            .map(|(node, outcome)| (*node, (outcome.member_changed, outcome.watermark_changed)))
            .collect()
    }

    fn admit_join(membership: &Membership, node: NodeId) -> JoinAdmission {
        membership.admit_join(node, &Reachability::new(membership.node))
    }

    fn downed_nodes(merge: &Merge) -> Vec<NodeId> {
        merge.downed.iter().map(|(node, _)| *node).collect()
    }

    fn superseded_pair() -> (Membership, NodeId, NodeId) {
        let membership = Membership::new(NodeId::new(addr(1)), NonZeroUsize::MAX, RETENTION);
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));
        assert!(predecessor.incarnation() < successor.incarnation());
        assert!(membership.add_up(predecessor));
        (membership, predecessor, successor)
    }

    #[test]
    fn a_merged_supersession_downs_the_predecessor_after_the_scan() {
        let (membership, predecessor, successor) = superseded_pair();

        let merge = membership.merge(&[Member::new(successor, MemberState::Up)], Instant::now());
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(downed, vec![(predecessor, (true, true))]);
        assert!(merge.to_flush.is_empty());
        assert!(!merge.self_down);
        assert!(membership.is_down(predecessor));
        assert!(membership.is_up(successor));
    }

    #[test]
    fn an_explicit_down_after_a_supersession_still_fences_the_predecessor() {
        let (membership, predecessor, successor) = superseded_pair();

        let merge = membership.merge(
            &[
                Member::new(successor, MemberState::Up),
                Member::new(predecessor, MemberState::Down),
            ],
            Instant::now(),
        );
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(
            downed,
            vec![(predecessor, (true, false)), (predecessor, (false, false))]
        );
        assert_eq!(merge.to_flush, vec![predecessor]);
        assert!(!merge.self_down);
    }

    #[test]
    fn a_self_down_with_a_supersession_still_fences_the_predecessor() {
        let (membership, predecessor, successor) = superseded_pair();
        let node = membership.node;

        let merge = membership.merge(
            &[
                Member::new(successor, MemberState::Up),
                Member::new(predecessor, MemberState::Down),
                Member::new(node, MemberState::Down),
            ],
            Instant::now(),
        );
        let downed = outcomes(&merge);

        assert_eq!(
            downed,
            vec![(predecessor, (true, false)), (predecessor, (false, false))]
        );
        assert_eq!(merge.to_flush, vec![predecessor, node]);
        assert!(merge.self_down);
        assert!(membership.is_down(node));
        assert!(membership.is_down(predecessor));
    }

    /// A younger incarnation at this node's address makes this node the stale one, which is what
    /// every other node concludes about it. So it downs itself rather than staying Up against a
    /// cluster which has moved on, and it still downs what the snapshot superseded before.
    #[test]
    fn a_self_down_on_the_early_return_still_downs_the_earlier_supersession() {
        let (membership, predecessor, successor) = superseded_pair();
        let node = membership.node;
        let node_successor = NodeId::new(node.addr());

        let merge = membership.merge(
            &[
                Member::new(successor, MemberState::Up),
                Member::new(node_successor, MemberState::Up),
            ],
            Instant::now(),
        );
        let downed = outcomes(&merge);

        assert_eq!(merge.new_up, vec![successor]);
        assert_eq!(downed, vec![(predecessor, (true, true))]);
        assert!(merge.to_flush.is_empty());
        assert!(merge.self_down);
        assert!(membership.is_down(node));
        assert!(!membership.is_up(node_successor));
    }

    #[test]
    fn a_self_down_snapshot_runs_no_effect_for_a_superseded_member() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));
        let witness = NodeId::new(addr(3));
        endpoint.update(|membership, _| {
            membership.add_up(predecessor);
            membership.add_up(witness);
        });

        endpoint
            .send(
                witness,
                Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                },
            )
            .expect("the lane is opened");
        let witness_lane = dial_requests.try_recv().expect("the witness is dialed");
        endpoint.bind_lane(witness.addr(), witness_lane.lane_id, witness);
        while witness_lane.control_rx.try_recv().is_ok() {}

        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (incoming_rx, _watcher_registration) = mailbox.split();
        endpoint.watchers().add(
            predecessor,
            ActorId::new(),
            Watcher::new(ActorId::new(), mailbox_handle.terminated_handler()),
        );
        let version = endpoint.state_rx().borrow().version();

        on_gossip(
            endpoint,
            &[
                Member::new(successor, MemberState::Up),
                Member::new(endpoint.node(), MemberState::Down),
            ],
        );

        let state = endpoint.state_rx().borrow().clone();
        assert_eq!(state.version(), version + 1);
        let down = state
            .members()
            .iter()
            .filter(|member| member.state() == MemberState::Down)
            .map(|member| member.node)
            .collect::<Vec<_>>();
        assert!(down.contains(&predecessor));
        assert!(down.contains(&endpoint.node()));
        assert!(incoming_rx.is_empty());
        assert!(witness_lane.control_rx.try_recv().is_err());
    }
}
