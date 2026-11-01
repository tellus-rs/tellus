//! A cluster-wide directory of registered actors: every node replicates the set of its own
//! registrations to every Up member, and [lookup], [subscribe], [settled] and [settlement] answer
//! from the replicated sets held here, see docs/cluster.md.

use crate::{
    ActorId, ActorRef,
    cluster::{
        discovery::{Key, KeyName, WireKey},
        endpoint::{self, EndpointInner},
        frame::Frame,
        manifest::Manifest,
        membership::Member,
        node::NodeId,
        registry::LocalRefError,
        state::ClusterState,
        wire,
    },
    sync::lock,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map},
    future::pending,
    marker::PhantomData,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tracing::{debug, warn};

const ENVELOPE_LEN: usize = 32;

pub(crate) const MAX_REGISTRATIONS: usize = 1 << 20;

// Counted as if every registration were a group of its own, which bounds any grouping.
pub(crate) const MAX_SNAPSHOT_BYTES: usize = 64 << 20;

/// Every actor registered under the key on every Up member, this node included, as far as
/// their registrations have reached this node. Answered locally, from the replicated sets: no
/// frame is sent, and a member's registrations are visible once its snapshot has arrived here,
/// never before and never after that member is Left or Down. An empty answer means that no
/// registration under the key has reached this node yet, which [settled] tells apart from a
/// complete answer.
///
/// # Errors
/// Fails if the endpoint is not started, or if no Up member has the name registered for `M` but
/// one has it registered for another message type.
pub fn lookup<M>(key: &Key<M>) -> Result<Vec<ActorRef<M>>, ReceptionistError>
where
    M: Manifest + Serialize + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    resolve_key(endpoint, key)
}

/// Every actor registered under the key on the Up member advertising the given address, as far
/// as its registrations have reached this node. Answered locally like [lookup], so an address no
/// Up member advertises and a member whose snapshot has not arrived here both answer an empty
/// set. [settled] only says that a snapshot from every Up member has arrived, not that it is
/// current. A registration or termination after that snapshot shows up with the next
/// replication, so a caller waiting for a specific actor polls this or uses [subscribe]. This
/// node's own address answers its own registrations.
///
/// # Errors
/// Fails if the endpoint is not started, or if the name is registered at that address only for
/// other message types.
pub fn lookup_at<M>(key: &Key<M>, addr: SocketAddr) -> Result<Vec<ActorRef<M>>, ReceptionistError>
where
    M: Manifest + Serialize + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    resolve_key_at(endpoint, key, addr)
}

/// Subscribe to the set [lookup] answers for the key, updated as registrations, terminations,
/// joins and node deaths come in.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn subscribe<M>(key: &Key<M>) -> Result<Subscription<M>, ReceptionistError>
where
    M: Manifest + 'static,
{
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    Ok(subscribe_at(endpoint, key))
}

/// Whether every member Up at the given [cluster state](crate::cluster::cluster_state) version
/// has delivered its registrations to this node. It is `true` iff `version` is the current one
/// and lookups answer from a snapshot of every member Up in it, other than this node. A later
/// version is judged on its own: a member's snapshot is evicted with its node death, and a member
/// Up in a later version has to deliver one first.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn settled(version: u64) -> Result<bool, ReceptionistError> {
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    Ok(settled_at(endpoint, version))
}

/// The current verdict: whether the receptionist is [settled] at the current cluster state
/// version, together with that version. The same judgment rides
/// [changes](crate::cluster::changes) as [Change::Settled](crate::cluster::Change::Settled)
/// whenever the verdict or the version it judges changes.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn settlement() -> Result<Settlement, ReceptionistError> {
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    Ok(endpoint.receptionist().settlement())
}

/// The receptionist's verdict for one cluster state version, see [settled] and [settlement].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Settlement {
    version: u64,
    settled: bool,
}

impl Settlement {
    /// The cluster state version the verdict is about.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Whether every member Up at that version, other than this node, has delivered the
    /// registrations lookups answer from.
    pub fn settled(&self) -> bool {
        self.settled
    }

    pub(crate) fn new(version: u64, settled: bool) -> Self {
        Self { version, settled }
    }
}

/// A subscription to the set of actors registered under one key across the cluster, see
/// [subscribe].
pub struct Subscription<M> {
    endpoint: &'static EndpointInner,
    listing: watch::Receiver<Arc<Listing>>,
    message: PhantomData<fn() -> M>,
}

impl<M> Subscription<M> {
    /// Resolves once the set has changed since the last [current](Subscription::current). Cancel
    /// safe: dropping the future before it completes marks no change as seen.
    pub async fn changed(&mut self) {
        if self.listing.changed().await.is_err() {
            pending::<()>().await;
        }
    }

    /// The set as of now, resolved into references and filtered against the Up members; marks
    /// the set as seen, so [changed](Subscription::changed) waits for the next change.
    ///
    /// # Errors
    /// Fails if no Up member has the name registered for `M` but one has it registered for
    /// another message type.
    pub fn current(&mut self) -> Result<Vec<ActorRef<M>>, ReceptionistError>
    where
        M: Manifest + Serialize + Send + 'static,
    {
        let listing = self.listing.borrow_and_update().clone();
        resolve(self.endpoint, &listing)
    }
}

impl<M> std::fmt::Debug for Subscription<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("entries", &self.listing.borrow().entries.len())
            .finish()
    }
}

/// The receptionist cannot answer.
#[derive(Debug, Error)]
pub enum ReceptionistError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// No Up member has the name registered for the requested message type, but one has it
    /// registered for another.
    #[error("name registered for another message type")]
    TypeMismatch,
}

/// The replicas lock is never held across an update, a registry lock or the cluster state watch.
pub(crate) struct Receptionist {
    node: NodeId,
    changes_tx: broadcast::Sender<PublishedChange>,
    replicas: Mutex<Replicas>,
}

impl Receptionist {
    pub(crate) fn new(
        node: NodeId,
        state: ClusterState,
        changes_tx: broadcast::Sender<PublishedChange>,
    ) -> Self {
        let map = HashMap::new();
        let settlement = Settlement::new(state.version(), compute_settled(&state, node, &map));
        Self {
            node,
            changes_tx,
            replicas: Mutex::new(Replicas {
                map,
                state,
                settlement,
                seq: 0,
                subscriptions: HashMap::new(),
                #[cfg(test)]
                republished: Vec::new(),
            }),
        }
    }

    pub(crate) fn settlement(&self) -> Settlement {
        let mut replicas = lock(&self.replicas);
        self.resettle(&mut replicas);
        replicas.settlement
    }

    /// Call after the watch was written, never while it is held. The state's channel send and the
    /// verdict for it happen under the replicas lock alone, so no verdict for the new version
    /// precedes the state and no verdict for the old one follows it.
    pub(crate) fn publish(&self, state: ClusterState) {
        let mut replicas = lock(&self.replicas);
        replicas.state = state.clone();
        let _ = self.changes_tx.send(PublishedChange::State(state));
        self.resettle(&mut replicas);
    }

    /// For a remote origin, call under the membership lock which decided `visible`
    /// (Membership::while_up), else a node death landing in between publishes a set the
    /// eviction then takes back a second time.
    pub(crate) fn apply(
        &self,
        origin: NodeId,
        version: u64,
        groups: Vec<WireGroup>,
        visible: bool,
    ) -> bool {
        let mut replicas = lock(&self.replicas);
        if replicas
            .map
            .get(&origin)
            .is_some_and(|replica| replica.version >= version)
        {
            return false;
        }
        let previously_visible = replicas
            .map
            .get(&origin)
            .is_some_and(|replica| replica.visible);
        // A visible origin found Down keeps its published set, which the eviction takes back.
        if previously_visible && !visible {
            return false;
        }

        let mut names = HashMap::<KeyName, HashMap<String, Vec<ActorId>>>::new();
        for WireGroup { key, ids } in groups {
            let (key_name, manifest) = key.into_parts();
            names
                .entry(key_name)
                .or_default()
                .entry(manifest)
                .or_default()
                .extend(ids);
        }
        for ids in names.values_mut().flat_map(HashMap::values_mut) {
            ids.sort_unstable();
            ids.dedup();
        }

        let previous = replicas.map.insert(
            origin,
            Replica {
                version,
                visible,
                names,
            },
        );
        if !visible {
            self.resettle(&mut replicas);
            return true;
        }
        // Diffed against what was published, so a hidden set made visible here counts as new.
        let changed = match previous.filter(|previous| previous.visible) {
            Some(published) => {
                let current = &replicas.map[&origin].names;
                published
                    .names
                    .keys()
                    .chain(current.keys())
                    .filter(|name| published.names.get(*name) != current.get(*name))
                    .cloned()
                    .collect::<HashSet<_>>()
            }

            None => replicas.map[&origin].names.keys().cloned().collect(),
        };
        replicas.republish(&changed);
        self.resettle(&mut replicas);
        true
    }

    pub(crate) fn add_own(&self, version: u64, registration: Registration) -> bool {
        let mut replicas = lock(&self.replicas);
        let Some(replica) = replicas.map.get_mut(&self.node) else {
            return false;
        };
        if !replica.visible || replica.version + 1 != version {
            return false;
        }

        let (key_name, manifest) = registration.key.into_parts();
        let ids = replica
            .names
            .entry(key_name.clone())
            .or_default()
            .entry(manifest)
            .or_default();
        if let Err(index) = ids.binary_search(&registration.id) {
            ids.insert(index, registration.id);
        }
        replica.version = version;

        replicas.republish(&HashSet::from([key_name]));
        self.resettle(&mut replicas);
        true
    }

    pub(crate) fn evict_fenced(&self, fence: NodeId) {
        let mut replicas = lock(&self.replicas);
        let evicted = replicas
            .map
            .keys()
            .filter(|origin| fence.covers(**origin))
            .copied()
            .collect::<Vec<_>>();
        let mut changed = HashSet::new();
        for origin in evicted {
            if let Some(replica) = replicas.map.remove(&origin) {
                changed.extend(replica.names.into_keys());
            }
        }
        replicas.republish(&changed);
        self.resettle(&mut replicas);
    }

    pub(crate) fn origin_up(&self, origin: NodeId) {
        let mut replicas = lock(&self.replicas);
        let Some(replica) = replicas.map.get_mut(&origin) else {
            return;
        };
        if replica.visible {
            return;
        }
        replica.visible = true;
        let shown = replica.names.keys().cloned().collect::<HashSet<_>>();
        replicas.republish(&shown);
        self.resettle(&mut replicas);
    }

    pub(crate) fn listing(&self, key: &WireKey) -> Listing {
        compute_listing(&lock(&self.replicas).map, key)
    }

    pub(crate) fn subscribe(&self, key: WireKey) -> watch::Receiver<Arc<Listing>> {
        let mut replicas = lock(&self.replicas);
        let listing = compute_listing(&replicas.map, &key);
        replicas
            .subscriptions
            .entry(key)
            .or_insert_with(|| watch::Sender::new(Arc::new(listing)))
            .subscribe()
    }

    pub(crate) fn applied_version(&self, origin: NodeId) -> Option<u64> {
        lock(&self.replicas)
            .map
            .get(&origin)
            .map(|replica| replica.version)
    }

    #[cfg(test)]
    pub(crate) fn republished(&self) -> Vec<Vec<ActorId>> {
        lock(&self.replicas).republished.clone()
    }

    #[cfg(test)]
    pub(crate) fn has_replicas(&self, origins: impl IntoIterator<Item = NodeId>) -> bool {
        let replicas = lock(&self.replicas);
        origins
            .into_iter()
            .all(|origin| replicas.map.contains_key(&origin))
    }

    fn resettle(&self, replicas: &mut Replicas) {
        let settlement = Settlement::new(
            replicas.state.version(),
            compute_settled(&replicas.state, self.node, &replicas.map),
        );
        if settlement == replicas.settlement {
            return;
        }
        replicas.settlement = settlement;
        replicas.seq += 1;
        let _ = self.changes_tx.send(PublishedChange::Settled {
            settlement,
            seq: replicas.seq,
        });
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Listing {
    entries: Vec<Entry>,
    other_types: Vec<NodeId>,
}

impl Listing {
    fn at(mut self, addr: SocketAddr) -> Self {
        self.entries.retain(|entry| entry.origin.addr() == addr);
        self.other_types.retain(|origin| origin.addr() == addr);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    origin: NodeId,
    id: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Registration {
    pub(crate) key: WireKey,
    pub(crate) id: ActorId,
}

// Never empty; a key's group may be split across the chunks of one snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WireGroup {
    pub(crate) key: WireKey,
    pub(crate) ids: Vec<ActorId>,
}

#[derive(Debug)]
pub(crate) struct Snapshot {
    pub(crate) version: u64,
    pub(crate) frames: Vec<Frame<'static>>,
}

#[derive(Debug, Default)]
pub(crate) struct Assembler(Option<Partial>);

impl Assembler {
    pub(crate) fn push<S>(
        &mut self,
        peer: NodeId,
        version: u64,
        chunk: u32,
        chunks: u32,
        groups: Vec<WireGroup>,
        is_stale: S,
    ) -> Option<Vec<WireGroup>>
    where
        S: FnOnce(u64) -> bool,
    {
        // Only a snapshot of one chunk may be empty, and no group is, so chunks never outnumber
        // the registrations.
        if chunks == 0
            || chunk >= chunks
            || (chunks > 1 && groups.is_empty())
            || groups.iter().any(|group| group.ids.is_empty())
        {
            warn!(%peer, version, chunk, chunks, "dropping malformed registrations chunk");
            return None;
        }
        // Before the chunk touches the buffer, so a stale chunk never resets it.
        if is_stale(version) {
            debug!(%peer, version, "ignoring a stale registrations snapshot");
            return None;
        }

        let entries = groups.iter().map(|group| group.ids.len()).sum::<usize>();
        let bytes = groups
            .iter()
            .map(|group| group.ids.len().saturating_mul(registration_len(&group.key)))
            .fold(0, usize::saturating_add);
        let partial = &mut self.0;
        match partial {
            Some(open) if open.version > version => {
                debug!(%peer, version, "ignoring a chunk of a superseded registrations snapshot");
                return None;
            }

            Some(open) if open.version == version => {
                if open.chunks != chunks {
                    warn!(%peer, version, "dropping a registrations snapshot whose chunk count changed");
                    *partial = None;
                    return None;
                }
                if let btree_map::Entry::Vacant(vacant) = open.received.entry(chunk) {
                    open.entries += entries;
                    open.bytes = open.bytes.saturating_add(bytes);
                    vacant.insert(groups);
                }
            }

            _ => {
                *partial = Some(Partial {
                    version,
                    chunks,
                    received: BTreeMap::from([(chunk, groups)]),
                    entries,
                    bytes,
                });
            }
        }
        if partial
            .as_ref()
            .is_some_and(|open| open.entries > MAX_REGISTRATIONS || open.bytes > MAX_SNAPSHOT_BYTES)
        {
            warn!(
                %peer,
                version,
                max_registrations = MAX_REGISTRATIONS,
                max_bytes = MAX_SNAPSHOT_BYTES,
                "dropping a registrations snapshot over the limit"
            );
            *partial = None;
            return None;
        }

        let complete = partial
            .as_ref()
            .is_some_and(|open| open.received.len() == open.chunks as usize);
        if !complete {
            return None;
        }
        partial.take().map(|open| merge(open.received))
    }
}

#[derive(Debug)]
struct Partial {
    version: u64,
    chunks: u32,
    received: BTreeMap<u32, Vec<WireGroup>>,
    entries: usize,
    bytes: usize,
}

pub(crate) fn snapshot(version: u64, groups: Vec<WireGroup>, max_frame_size: usize) -> Snapshot {
    let budget = max_frame_size.saturating_sub(ENVELOPE_LEN);
    let id_len = encoded_len(&ActorId::new());
    let mut chunks = vec![Vec::<WireGroup>::new()];
    let mut used = 0;
    for WireGroup { key, ids } in groups {
        let key_len = encoded_len(&key);
        let mut open = false;
        for id in ids {
            let chunk = chunks.last_mut().expect("one chunk is always open");
            let grown = match chunk.last() {
                Some(group) if open => {
                    group_len(key_len, group.ids.len() + 1, id_len)
                        - group_len(key_len, group.ids.len(), id_len)
                }

                _ => group_len(key_len, 1, id_len),
            };
            if !chunk.is_empty() && used + grown > budget {
                chunks.push(vec![WireGroup {
                    key: key.clone(),
                    ids: vec![id],
                }]);
                used = group_len(key_len, 1, id_len);
            } else {
                match chunk.last_mut() {
                    Some(group) if open => group.ids.push(id),

                    _ => chunk.push(WireGroup {
                        key: key.clone(),
                        ids: vec![id],
                    }),
                }
                used += grown;
            }
            open = true;
        }
    }

    let count = u32::try_from(chunks.len()).expect("a snapshot has fewer chunks than u32::MAX");
    let frames = chunks
        .into_iter()
        .enumerate()
        .map(|(index, groups)| Frame::Registrations {
            version,
            chunk: u32::try_from(index).expect("a chunk index is below the count"),
            chunks: count,
            groups,
        })
        .collect();
    Snapshot { version, frames }
}

pub(crate) fn framed_len(key: &WireKey) -> usize {
    registration_len(key) + ENVELOPE_LEN
}

pub(crate) fn registration_len(key: &WireKey) -> usize {
    group_len(encoded_len(key), 1, encoded_len(&ActorId::new()))
}

pub(crate) fn on_registrations(
    endpoint: &EndpointInner,
    peer: NodeId,
    assembler: &mut Assembler,
    version: u64,
    chunk: u32,
    chunks: u32,
    groups: Vec<WireGroup>,
) {
    let receptionist = endpoint.receptionist();
    let Some(groups) = assembler.push(peer, version, chunk, chunks, groups, |version| {
        receptionist
            .applied_version(peer)
            .is_some_and(|applied| version <= applied)
    }) else {
        return;
    };

    let applied = endpoint
        .membership()
        .while_up(peer, |up| receptionist.apply(peer, version, groups, up));
    if applied {
        debug!(%peer, version, "registrations snapshot applied");
    }
}

pub(crate) fn resolve_key<M>(
    endpoint: &EndpointInner,
    key: &Key<M>,
) -> Result<Vec<ActorRef<M>>, ReceptionistError>
where
    M: Manifest + Serialize + Send + 'static,
{
    let listing = endpoint.receptionist().listing(&key.wire());
    resolve(endpoint, &listing)
}

fn resolve_key_at<M>(
    endpoint: &EndpointInner,
    key: &Key<M>,
    addr: SocketAddr,
) -> Result<Vec<ActorRef<M>>, ReceptionistError>
where
    M: Manifest + Serialize + Send + 'static,
{
    // The listing first, the Up filter in `resolve` after, else a death in between answers.
    let listing = endpoint.receptionist().listing(&key.wire()).at(addr);
    resolve(endpoint, &listing)
}

fn subscribe_at<M>(endpoint: &'static EndpointInner, key: &Key<M>) -> Subscription<M>
where
    M: Manifest + 'static,
{
    Subscription {
        endpoint,
        listing: endpoint.receptionist().subscribe(key.wire()),
        message: PhantomData,
    }
}

fn settled_at(endpoint: &EndpointInner, version: u64) -> bool {
    let receptionist = endpoint.receptionist();
    let replicas = lock(&receptionist.replicas);
    replicas.state.version() == version
        && compute_settled(&replicas.state, receptionist.node, &replicas.map)
}

fn compute_settled(state: &ClusterState, node: NodeId, map: &HashMap<NodeId, Replica>) -> bool {
    state
        .up()
        .map(Member::node)
        .filter(|origin| *origin != node)
        .all(|origin| map.get(&origin).is_some_and(|replica| replica.visible))
}

fn resolve<M>(
    endpoint: &EndpointInner,
    listing: &Listing,
) -> Result<Vec<ActorRef<M>>, ReceptionistError>
where
    M: Serialize + Send + 'static,
{
    let up = endpoint.membership().up_nodes();
    let refs = listing
        .entries
        .iter()
        .filter(|entry| up.contains(&entry.origin))
        .filter_map(|entry| match wire::resolve::<M>(endpoint, entry.origin, entry.id) {
            Ok(actor_ref) => Some(actor_ref),

            Err(LocalRefError::Unbound) => None,

            Err(error) => {
                warn!(origin = %entry.origin, actor_id = %entry.id, %error, "cannot resolve a registration");
                None
            }
        })
        .collect::<Vec<_>>();
    if refs.is_empty() && listing.other_types.iter().any(|origin| up.contains(origin)) {
        return Err(ReceptionistError::TypeMismatch);
    }
    Ok(refs)
}

fn compute_listing(replicas: &HashMap<NodeId, Replica>, key: &WireKey) -> Listing {
    let mut entries = Vec::new();
    let mut other_types = Vec::new();
    for (origin, replica) in replicas {
        if !replica.visible {
            continue;
        }
        let Some(manifests) = replica.names.get(key.key_name()) else {
            continue;
        };
        for (manifest, ids) in manifests {
            if manifest == key.manifest() {
                entries.extend(ids.iter().map(|id| Entry {
                    origin: *origin,
                    id: *id,
                }));
            } else if !other_types.contains(origin) {
                other_types.push(*origin);
            }
        }
    }
    entries
        .sort_unstable_by_key(|entry| (entry.origin.addr(), entry.origin.incarnation(), entry.id));
    other_types.sort_unstable_by_key(|origin| (origin.addr(), origin.incarnation()));
    Listing {
        entries,
        other_types,
    }
}

fn merge(received: BTreeMap<u32, Vec<WireGroup>>) -> Vec<WireGroup> {
    let mut merged = BTreeMap::<WireKey, Vec<ActorId>>::new();
    for WireGroup { key, ids } in received.into_values().flatten() {
        merged.entry(key).or_default().extend(ids);
    }
    merged
        .into_iter()
        .map(|(key, ids)| WireGroup { key, ids })
        .collect()
}

fn group_len(key_len: usize, ids: usize, id_len: usize) -> usize {
    key_len + encoded_len(&ids) + ids * id_len
}

fn encoded_len<T>(value: &T) -> usize
where
    T: Serialize,
{
    postcard::experimental::serialized_size(value).expect("a registration is serializable")
}

/// What one change of the cluster publishes on the channel behind
/// [changes](crate::cluster::changes); the verdict's sequence orders verdicts for a subscriber
/// which took a snapshot.
#[derive(Debug, Clone)]
pub(crate) enum PublishedChange {
    State(ClusterState),
    Settled { settlement: Settlement, seq: u64 },
}

/// The coordinated snapshot a subscriber starts from and recovers to: the current state, the
/// verdict for it and that verdict's sequence, all under one lock.
pub(crate) trait SnapshotSource: Send + Sync {
    fn snapshot(&self) -> (ClusterState, Settlement, u64);
}

impl SnapshotSource for Receptionist {
    fn snapshot(&self) -> (ClusterState, Settlement, u64) {
        let mut replicas = lock(&self.replicas);
        self.resettle(&mut replicas);
        (replicas.state.clone(), replicas.settlement, replicas.seq)
    }
}

struct Replicas {
    map: HashMap<NodeId, Replica>,
    state: ClusterState,
    settlement: Settlement,
    seq: u64,
    subscriptions: HashMap<WireKey, watch::Sender<Arc<Listing>>>,
    #[cfg(test)]
    republished: Vec<Vec<ActorId>>,
}

impl Replicas {
    fn republish(&mut self, names: &HashSet<KeyName>) {
        let Replicas {
            map,
            subscriptions,
            #[cfg(test)]
            republished,
            ..
        } = self;
        subscriptions.retain(|key, listing_tx| {
            if listing_tx.is_closed() {
                return false;
            }
            if names.contains(key.key_name()) {
                let listing = compute_listing(map, key);
                listing_tx.send_if_modified(|current| {
                    if **current == listing {
                        return false;
                    }
                    #[cfg(test)]
                    republished.push(listing.entries.iter().map(|entry| entry.id).collect());
                    *current = Arc::new(listing);
                    true
                });
            }
            true
        });
    }
}

struct Replica {
    version: u64,
    visible: bool,
    names: HashMap<KeyName, HashMap<String, Vec<ActorId>>>,
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, MailboxCapacity,
        actor_ref::SelfRef,
        cluster::{
            discovery::{Key, KeyKind, WireKey},
            endpoint::{EndpointConfig, EndpointInner},
            frame::Frame,
            manifest::Manifest,
            node::NodeId,
            receptionist::{
                Assembler, MAX_REGISTRATIONS, MAX_SNAPSHOT_BYTES, PublishedChange,
                ReceptionistError, Registration, Settlement, SnapshotSource, WireGroup,
                on_registrations, resolve_key, resolve_key_at, settled_at, snapshot, subscribe_at,
            },
        },
    };
    use serde::Serialize;
    use std::{
        net::SocketAddr,
        thread,
        time::{Duration, Instant},
    };
    use tokio::sync::broadcast;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn endpoint() -> &'static EndpointInner {
        EndpointInner::for_tests(EndpointConfig::new(addr(1))).0
    }

    fn up(endpoint: &EndpointInner, node: NodeId) {
        endpoint.update(|membership, _| membership.add_up(node));
    }

    fn apply(
        endpoint: &EndpointInner,
        origin: NodeId,
        version: u64,
        groups: Vec<WireGroup>,
    ) -> bool {
        endpoint.membership().while_up(origin, |up| {
            endpoint.receptionist().apply(origin, version, groups, up)
        })
    }

    fn expect_state(published: &mut broadcast::Receiver<PublishedChange>, version: u64) {
        match published.try_recv() {
            Ok(PublishedChange::State(state)) => assert_eq!(state.version(), version),
            other => panic!("expected the state {version}, got {other:?}"),
        }
    }

    fn expect_settled(
        published: &mut broadcast::Receiver<PublishedChange>,
        expected: Settlement,
        expected_seq: u64,
    ) {
        match published.try_recv() {
            Ok(PublishedChange::Settled { settlement, seq }) => {
                assert_eq!((settlement, seq), (expected, expected_seq));
            }

            other => panic!("expected the verdict {expected:?}, got {other:?}"),
        }
    }

    fn group<M>(name: &str, id: ActorId) -> WireGroup
    where
        M: Manifest,
    {
        WireGroup {
            key: WireKey::new::<M>(KeyKind::Discovery, name),
            ids: vec![id],
        }
    }

    fn registration(name: &str, id: ActorId) -> Registration {
        Registration {
            key: WireKey::new::<u64>(KeyKind::Discovery, name),
            id,
        }
    }

    fn groups(frame: &Frame<'static>) -> (u32, u32, Vec<WireGroup>) {
        match frame {
            Frame::Registrations {
                chunk,
                chunks,
                groups,
                ..
            } => (*chunk, *chunks, groups.clone()),
            frame => panic!("not a registrations frame: {frame:?}"),
        }
    }

    /// An origin's set is replaced by a higher version only; the first snapshot always applies,
    /// version zero included.
    #[test]
    fn a_stale_version_is_ignored_and_a_first_snapshot_applies() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let first = ActorId::new();
        let second = ActorId::new();
        let key = Key::<u64>::new("pool");

        assert!(apply(
            endpoint,
            origin,
            0,
            vec![group::<u64>("pool", first)]
        ));
        assert!(apply(
            endpoint,
            origin,
            2,
            vec![group::<u64>("pool", second)]
        ));
        assert!(!apply(
            endpoint,
            origin,
            1,
            vec![group::<u64>("pool", first)]
        ));
        assert!(!apply(endpoint, origin, 2, Vec::new()));

        let ids = resolve_key(endpoint, &key)
            .expect("the key resolves")
            .iter()
            .map(|actor_ref| actor_ref.actor_id())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![second]);
    }

    /// An empty snapshot removes an origin's entries but keeps its replica, which is what keeps
    /// a member with nothing registered counted as delivered.
    #[test]
    fn an_empty_snapshot_removes_the_entries_but_keeps_the_replica() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let key = Key::<u64>::new("pool");
        apply(
            endpoint,
            origin,
            1,
            vec![group::<u64>("pool", ActorId::new())],
        );
        assert_eq!(resolve_key(endpoint, &key).expect("resolves").len(), 1);

        apply(endpoint, origin, 2, Vec::new());

        assert!(resolve_key(endpoint, &key).expect("resolves").is_empty());
        assert!(endpoint.receptionist().has_replicas([origin]));
    }

    /// A fence evicts every incarnation it covers and spares a younger one at the address.
    #[test]
    fn a_fence_evicts_the_covered_incarnations() {
        let endpoint = endpoint();
        let older = NodeId::new(addr(2));
        let younger = NodeId::new(addr(2));
        let other = NodeId::new(addr(3));
        for origin in [older, younger, other] {
            up(endpoint, origin);
            apply(
                endpoint,
                origin,
                1,
                vec![group::<u64>("pool", ActorId::new())],
            );
        }

        endpoint.receptionist().evict_fenced(older);

        assert!(!endpoint.receptionist().has_replicas([older]));
        assert!(endpoint.receptionist().has_replicas([younger, other]));
        assert_eq!(
            resolve_key(endpoint, &Key::<u64>::new("pool"))
                .expect("resolves")
                .len(),
            2
        );
    }

    /// A subscription wakes for its own name only, and only when the set actually changed.
    #[test]
    fn a_subscription_is_published_for_changed_names_only() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let id = ActorId::new();
        apply(endpoint, origin, 1, vec![group::<u64>("pool", id)]);
        let mut pool = subscribe_at(endpoint, &Key::<u64>::new("pool"));
        let mut other = subscribe_at(endpoint, &Key::<u64>::new("other"));
        assert_eq!(pool.current().expect("resolves").len(), 1);
        assert!(other.current().expect("resolves").is_empty());

        apply(
            endpoint,
            origin,
            2,
            vec![
                group::<u64>("pool", id),
                group::<u64>("other", ActorId::new()),
            ],
        );

        assert!(!pool.listing.has_changed().expect("sender alive"));
        assert!(other.listing.has_changed().expect("sender alive"));
        assert_eq!(other.current().expect("resolves").len(), 1);
    }

    /// Settled holds for the current version only, and needs a replica from every Up peer,
    /// this node excluded.
    #[test]
    fn settled_requires_the_current_version_and_every_up_peer() {
        let endpoint = endpoint();
        let first = NodeId::new(addr(2));
        let second = NodeId::new(addr(3));
        up(endpoint, first);
        up(endpoint, second);
        let version = endpoint.state_rx().borrow().version();
        assert!(!settled_at(endpoint, version));

        apply(endpoint, first, 1, Vec::new());
        assert!(!settled_at(endpoint, version));

        apply(endpoint, second, 1, Vec::new());
        assert!(settled_at(endpoint, version));
        assert!(!settled_at(endpoint, version - 1));
        assert!(!settled_at(endpoint, version + 1));
    }

    /// The verdict rides the channel whenever it or the version it judges changes, in order with
    /// the states; a hidden replica does not settle, and a snapshot of a current verdict sends
    /// nothing.
    #[test]
    fn the_verdict_follows_membership_and_visibility() {
        let endpoint = endpoint();
        let mut published = endpoint.published_changes();
        let first = NodeId::new(addr(2));
        assert_eq!(
            endpoint.receptionist().settlement(),
            Settlement::new(0, true)
        );

        up(endpoint, first);
        expect_state(&mut published, 1);
        expect_settled(&mut published, Settlement::new(1, false), 1);

        assert!(endpoint.receptionist().apply(first, 1, Vec::new(), false));
        assert!(!settled_at(endpoint, 1));
        assert!(published.try_recv().is_err());

        endpoint.receptionist().origin_up(first);
        expect_settled(&mut published, Settlement::new(1, true), 2);
        assert!(settled_at(endpoint, 1));

        let (state, settlement, seq) = endpoint.receptionist().snapshot();
        assert_eq!(state.version(), 1);
        assert_eq!((settlement, seq), (Settlement::new(1, true), 2));
        assert!(published.try_recv().is_err());

        endpoint.receptionist().evict_fenced(first);
        expect_settled(&mut published, Settlement::new(1, false), 3);

        endpoint.update(|membership, _| membership.down(first, Instant::now()));
        expect_state(&mut published, 2);
        expect_settled(&mut published, Settlement::new(2, true), 4);
        assert!(published.try_recv().is_err());
    }

    /// A key nobody subscribes to anymore is dropped on the next change of its name.
    #[test]
    fn unsubscribed_keys_are_pruned() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let subscription = subscribe_at(endpoint, &Key::<u64>::new("pool"));
        assert_eq!(
            crate::sync::lock(&endpoint.receptionist().replicas)
                .subscriptions
                .len(),
            1
        );

        drop(subscription);
        apply(
            endpoint,
            origin,
            1,
            vec![group::<u64>("pool", ActorId::new())],
        );

        assert!(
            crate::sync::lock(&endpoint.receptionist().replicas)
                .subscriptions
                .is_empty()
        );
    }

    /// Interleaved chunks of two versions on one connection: exactly the higher version is
    /// applied, once and complete, whatever the order.
    #[test]
    fn interleaved_chunks_reassemble_the_higher_version_only() {
        for order in [
            [(2, 0), (3, 0), (2, 1), (3, 1)],
            [(3, 1), (2, 0), (3, 0), (2, 0)],
        ] {
            let endpoint = endpoint();
            let origin = NodeId::new(addr(2));
            up(endpoint, origin);
            let ids = [ActorId::new(), ActorId::new()];
            let chunk = |version: u64, index: u32| {
                let name = format!("v{version}-{index}");
                vec![group::<u64>(&name, ids[index as usize])]
            };
            let mut assembler = Assembler::default();

            for (version, index) in order {
                on_registrations(
                    endpoint,
                    origin,
                    &mut assembler,
                    version,
                    index,
                    2,
                    chunk(version, index),
                );
            }

            assert_eq!(endpoint.receptionist().applied_version(origin), Some(3));
            for index in 0..2 {
                let key = Key::<u64>::new(format!("v3-{index}"));
                assert_eq!(resolve_key(endpoint, &key).expect("resolves").len(), 1);
                let key = Key::<u64>::new(format!("v2-{index}"));
                assert!(resolve_key(endpoint, &key).expect("resolves").is_empty());
            }
            assert!(assembler.0.is_none());
        }
    }

    /// Malformed metadata is dropped without touching an open buffer, and a declared chunk count
    /// allocates nothing: a buffer holds exactly what was received.
    #[test]
    fn malformed_chunks_are_dropped_and_a_declared_count_allocates_nothing() {
        let origin = NodeId::new(addr(2));
        let entry = || vec![group::<u64>("pool", ActorId::new())];
        let fresh = |_| false;
        let mut assembler = Assembler::default();

        assert!(assembler.push(origin, 1, 0, 0, entry(), fresh).is_none());
        assert!(assembler.0.is_none());
        assert!(assembler.push(origin, 1, 2, 2, entry(), fresh).is_none());
        assert!(assembler.0.is_none());

        assert!(
            assembler
                .push(origin, 1, 0, u32::MAX, entry(), fresh)
                .is_none()
        );
        let open = assembler.0.as_ref().expect("a buffer is open");
        assert_eq!(open.received.len(), 1);
        assert!(assembler.push(origin, 1, 5, 5, entry(), fresh).is_none());
        let open = assembler.0.as_ref().expect("still open");
        assert_eq!(open.received.len(), 1);

        let completed = assembler.push(origin, 2, 0, 1, entry(), fresh);
        assert_eq!(completed.map(|groups| groups.len()), Some(1));
        assert!(assembler.0.is_none());
    }

    #[test]
    fn own_registrations_apply_one_at_a_time_without_a_gap() {
        let endpoint = endpoint();
        let own = endpoint.node();
        let listed = |name: &str| {
            endpoint
                .receptionist()
                .listing(&Key::<u64>::new(name).wire())
                .entries
                .len()
        };
        let first = registration("pool", ActorId::new());
        assert!(!endpoint.receptionist().add_own(1, first.clone()));

        apply(
            endpoint,
            own,
            1,
            vec![WireGroup {
                key: first.key,
                ids: vec![first.id],
            }],
        );
        let second = registration("pool", ActorId::new());
        assert!(endpoint.receptionist().add_own(2, second));
        assert_eq!(listed("pool"), 2);

        let gapped = registration("other", ActorId::new());
        assert!(!endpoint.receptionist().add_own(4, gapped));
        assert_eq!(listed("other"), 0);
    }

    #[test]
    fn an_empty_chunk_of_a_multipart_snapshot_is_dropped() {
        let origin = NodeId::new(addr(2));
        let fresh = |_| false;
        let mut assembler = Assembler::default();

        assert!(
            assembler
                .push(origin, 1, 0, u32::MAX, Vec::new(), fresh)
                .is_none()
        );
        assert!(assembler.0.is_none());

        let completed = assembler.push(origin, 1, 0, 1, Vec::new(), fresh);
        assert_eq!(completed, Some(Vec::new()));
    }

    #[test]
    fn a_snapshot_over_the_limit_is_dropped() {
        let origin = NodeId::new(addr(2));
        let entry = || vec![group::<u64>("pool", ActorId::new())];
        let fresh = |_| false;
        let mut assembler = Assembler::default();

        assert!(assembler.push(origin, 1, 0, 3, entry(), fresh).is_none());
        assembler.0.as_mut().expect("a buffer is open").entries = MAX_REGISTRATIONS;
        assert!(assembler.push(origin, 1, 1, 3, entry(), fresh).is_none());
        assert!(assembler.0.is_none());

        assert!(assembler.push(origin, 2, 0, 1, entry(), fresh).is_some());
    }

    /// The bytes are capped like the count, counting every registration with its key.
    #[test]
    fn a_snapshot_over_the_byte_limit_is_dropped() {
        let origin = NodeId::new(addr(2));
        let entry = || vec![group::<u64>("pool", ActorId::new())];
        let fresh = |_| false;
        let mut assembler = Assembler::default();

        assert!(assembler.push(origin, 1, 0, 3, entry(), fresh).is_none());
        assembler.0.as_mut().expect("a buffer is open").bytes = MAX_SNAPSHOT_BYTES;
        assert!(assembler.push(origin, 1, 1, 3, entry(), fresh).is_none());
        assert!(assembler.0.is_none());

        assert!(assembler.push(origin, 2, 0, 1, entry(), fresh).is_some());
    }

    /// An empty group is malformed, whatever else the chunk holds, and leaves an open buffer
    /// untouched.
    #[test]
    fn a_chunk_with_an_empty_group_is_dropped() {
        let origin = NodeId::new(addr(2));
        let fresh = |_| false;
        let mut assembler = Assembler::default();
        assert!(
            assembler
                .push(
                    origin,
                    1,
                    0,
                    2,
                    vec![group::<u64>("pool", ActorId::new())],
                    fresh
                )
                .is_none()
        );

        let empty = WireGroup {
            key: WireKey::new::<u64>(KeyKind::Discovery, "other"),
            ids: Vec::new(),
        };
        assert!(
            assembler
                .push(
                    origin,
                    1,
                    1,
                    2,
                    vec![group::<u64>("pool", ActorId::new()), empty],
                    fresh
                )
                .is_none()
        );

        let open = assembler.0.as_ref().expect("still open");
        assert_eq!((open.version, open.received.len()), (1, 1));
    }

    /// A key whose IDs outgrow a frame is split across chunks, each within the limit, and the
    /// chunks reassemble into the one group they were split from.
    #[test]
    fn a_split_group_reassembles_into_one() {
        let mut ids = (0..20).map(|_| ActorId::new()).collect::<Vec<_>>();
        ids.sort_unstable();
        let input = vec![
            WireGroup {
                key: WireKey::new::<u64>(KeyKind::Discovery, "pool"),
                ids: ids.clone(),
            },
            group::<u64>("solo", ActorId::new()),
        ];
        let chunked = snapshot(1, input.clone(), 200);
        assert!(chunked.frames.len() > 2, "the pool group must be split");

        let origin = NodeId::new(addr(2));
        let mut assembler = Assembler::default();
        let mut completed = None;
        for frame in &chunked.frames {
            assert!(frame.encoded_len().expect("frame encodes") <= 200);
            let (chunk, chunks, groups) = groups(frame);
            assert!(groups.iter().all(|group| !group.ids.is_empty()));
            completed = assembler.push(origin, 1, chunk, chunks, groups, |_| false);
        }

        assert_eq!(completed, Some(input));
    }

    /// Matching is by manifest, never by Rust type: another type naming the registered manifest
    /// answers, a type naming another manifest is the other type.
    #[test]
    fn a_lookup_matches_the_manifest_not_the_rust_type() {
        struct Registered;

        impl Manifest for Registered {
            const MANIFEST: &'static str = "tests.registered";
        }

        struct Renamed;

        impl Manifest for Renamed {
            const MANIFEST: &'static str = "tests.registered";
        }

        struct Other;

        impl Manifest for Other {
            const MANIFEST: &'static str = "tests.other";
        }

        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let id = ActorId::new();
        assert!(apply(
            endpoint,
            origin,
            1,
            vec![group::<Registered>("pool", id)]
        ));
        let listing = |key: WireKey| endpoint.receptionist().listing(&key);

        let renamed = listing(WireKey::new::<Renamed>(KeyKind::Discovery, "pool"));
        assert_eq!(
            renamed
                .entries
                .iter()
                .map(|entry| entry.id)
                .collect::<Vec<_>>(),
            [id]
        );
        assert!(renamed.other_types.is_empty());

        let other = listing(WireKey::new::<Other>(KeyKind::Discovery, "pool"));
        assert!(other.entries.is_empty());
        assert_eq!(other.other_types, [origin]);
    }

    /// A topic and a discovery key of one name and type are distinct: neither lists the other's
    /// registrations, and neither turns the other into a type mismatch.
    #[test]
    fn topics_and_discovery_keys_do_not_see_each_other() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let (discovered, subscribed) = (ActorId::new(), ActorId::new());
        assert!(apply(
            endpoint,
            origin,
            1,
            vec![
                group::<u64>("pool", discovered),
                WireGroup {
                    key: WireKey::new::<u64>(KeyKind::Topic, "pool"),
                    ids: vec![subscribed],
                },
            ]
        ));
        let ids = |key: &Key<u64>| {
            resolve_key(endpoint, key)
                .expect("resolves")
                .iter()
                .map(|actor_ref| actor_ref.actor_id())
                .collect::<Vec<_>>()
        };

        assert_eq!(ids(&Key::new("pool")), [discovered]);
        assert_eq!(ids(&Key::topic("pool")), [subscribed]);

        assert!(apply(
            endpoint,
            origin,
            2,
            vec![WireGroup {
                key: WireKey::new::<u32>(KeyKind::Topic, "pool"),
                ids: vec![subscribed],
            }]
        ));
        assert!(ids(&Key::new("pool")).is_empty());
    }

    #[test]
    fn a_stale_chunk_leaves_an_open_snapshot_untouched() {
        let origin = NodeId::new(addr(2));
        let entry = || vec![group::<u64>("pool", ActorId::new())];
        let mut assembler = Assembler::default();
        assert!(
            assembler
                .push(origin, 3, 0, 2, entry(), |_| false)
                .is_none()
        );

        assert!(assembler.push(origin, 4, 0, 1, entry(), |_| true).is_none());

        let open = assembler.0.as_ref().expect("still open");
        assert_eq!((open.version, open.received.len()), (3, 1));
    }

    /// A name held on Up members only under other types is a mismatch; one held under the
    /// asked type as well answers; one held under another type on a Down member only is empty.
    #[test]
    fn a_wrong_type_is_a_mismatch_unless_the_name_is_also_held_under_the_asked_type() {
        let endpoint = endpoint();
        let first = NodeId::new(addr(2));
        let second = NodeId::new(addr(3));
        up(endpoint, first);
        up(endpoint, second);
        apply(
            endpoint,
            first,
            1,
            vec![group::<u64>("pool", ActorId::new())],
        );

        assert!(matches!(
            resolve_key(endpoint, &Key::<u32>::new("pool")),
            Err(ReceptionistError::TypeMismatch)
        ));
        let mut subscription = subscribe_at(endpoint, &Key::<u32>::new("pool"));
        assert!(matches!(
            subscription.current(),
            Err(ReceptionistError::TypeMismatch)
        ));

        apply(
            endpoint,
            second,
            1,
            vec![group::<u32>("pool", ActorId::new())],
        );
        assert_eq!(
            resolve_key(endpoint, &Key::<u32>::new("pool"))
                .expect("the u32 registration answers")
                .len(),
            1
        );

        endpoint.node_death(second);
        assert!(matches!(
            resolve_key(endpoint, &Key::<u32>::new("pool")),
            Err(ReceptionistError::TypeMismatch)
        ));
        endpoint.node_death(first);
        assert!(
            resolve_key(endpoint, &Key::<u32>::new("pool"))
                .expect("nothing Up holds the name")
                .is_empty()
        );
    }

    /// A snapshot which arrives before its origin is Up here wakes nobody, since the set it
    /// answers does not change; the origin going Up is the one change, whether the subscription
    /// predates the snapshot or not.
    #[test]
    fn a_replica_of_a_not_yet_up_origin_becomes_visible_with_the_origin() {
        for subscribe_first in [true, false] {
            let endpoint = endpoint();
            let origin = NodeId::new(addr(2));
            let key = Key::<u64>::new("pool");
            let mut early = subscribe_first.then(|| subscribe_at(endpoint, &key));
            if let Some(early) = &mut early {
                assert!(early.current().expect("resolves").is_empty());
            }

            assert!(apply(
                endpoint,
                origin,
                1,
                vec![group::<u64>("pool", ActorId::new())]
            ));
            let mut subscription = early.unwrap_or_else(|| subscribe_at(endpoint, &key));
            assert!(!subscription.listing.has_changed().expect("sender alive"));
            assert!(subscription.current().expect("resolves").is_empty());
            assert!(resolve_key(endpoint, &key).expect("resolves").is_empty());

            up(endpoint, origin);
            endpoint.receptionist().origin_up(origin);

            assert!(subscription.listing.has_changed().expect("sender alive"));
            assert_eq!(subscription.current().expect("resolves").len(), 1);
            endpoint.receptionist().origin_up(origin);
            assert!(!subscription.listing.has_changed().expect("sender alive"));
        }
    }

    /// A frame already past the gate can arrive after its origin's Down: it is discarded and the
    /// published set stays, so the eviction which follows is the one change subscribers see, an
    /// empty snapshot included.
    #[test]
    fn a_snapshot_arriving_after_a_down_is_discarded_and_the_eviction_wakes_once() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let key = Key::<u64>::new("pool");
        assert!(apply(
            endpoint,
            origin,
            1,
            vec![group::<u64>("pool", ActorId::new())]
        ));
        let mut subscription = subscribe_at(endpoint, &key);
        assert_eq!(subscription.current().expect("resolves").len(), 1);

        endpoint.update(|membership, _| membership.down(origin, Instant::now()));
        assert!(!apply(endpoint, origin, 2, Vec::new()));
        assert_eq!(endpoint.receptionist().applied_version(origin), Some(1));
        assert!(!subscription.listing.has_changed().expect("sender alive"));

        endpoint.receptionist().evict_fenced(origin);

        assert!(subscription.listing.has_changed().expect("sender alive"));
        assert!(subscription.current().expect("resolves").is_empty());
        assert!(!subscription.listing.has_changed().expect("sender alive"));
    }

    /// A hidden set made visible by a later apply, the origin having gone Up in between, is
    /// published whole even if it equals the hidden one, and the `origin_up` which follows adds
    /// nothing.
    #[test]
    fn a_hidden_set_made_visible_by_a_later_apply_is_published_once() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        let key = Key::<u64>::new("pool");
        let id = ActorId::new();
        assert!(apply(endpoint, origin, 1, vec![group::<u64>("pool", id)]));
        let mut subscription = subscribe_at(endpoint, &key);
        assert!(subscription.current().expect("resolves").is_empty());

        up(endpoint, origin);
        assert!(apply(endpoint, origin, 2, vec![group::<u64>("pool", id)]));

        assert!(subscription.listing.has_changed().expect("sender alive"));
        assert_eq!(subscription.current().expect("resolves").len(), 1);
        endpoint.receptionist().origin_up(origin);
        assert!(!subscription.listing.has_changed().expect("sender alive"));
    }

    /// A node death landing while a snapshot applies waits for the apply: the set is published
    /// under the Up view it was judged against, and the eviction which follows is a second, real
    /// change, never a duplicate of the first. Inside the guard only the watch receiver is read,
    /// since the membership lock is held there.
    #[test]
    fn a_death_landing_during_an_apply_waits_for_it() {
        let endpoint = endpoint();
        let origin = NodeId::new(addr(2));
        up(endpoint, origin);
        let key = Key::<u64>::new("pool");
        let mut subscription = subscribe_at(endpoint, &key);
        assert!(subscription.current().expect("resolves").is_empty());

        let death = endpoint.membership().while_up(origin, |up| {
            assert!(up);
            let death = thread::spawn(move || endpoint.node_death(origin));
            thread::sleep(Duration::from_millis(200));
            assert!(!death.is_finished(), "the death must wait for the guard");
            assert!(endpoint.receptionist().apply(
                origin,
                1,
                vec![group::<u64>("pool", ActorId::new())],
                up
            ));
            assert!(subscription.listing.has_changed().expect("sender alive"));
            assert_eq!(subscription.listing.borrow_and_update().entries.len(), 1);
            death
        });
        death
            .join()
            .expect("the death completes once the guard is released");

        assert!(subscription.listing.has_changed().expect("sender alive"));
        assert!(subscription.current().expect("resolves").is_empty());
        assert!(!subscription.listing.has_changed().expect("sender alive"));
        assert!(!endpoint.receptionist().has_replicas([origin]));
    }

    /// An empty set is one frame, and a set which outgrows a frame is chunked within the limit
    /// and concatenates back to the input.
    #[test]
    fn a_snapshot_is_chunked_within_the_frame_size_and_never_empty() {
        let empty = snapshot(1, Vec::new(), 200);
        assert_eq!(empty.frames.len(), 1);
        assert_eq!(groups(&empty.frames[0]), (0, 1, Vec::new()));

        let input = (0..20)
            .map(|index| group::<u64>(&format!("a-long-name-{index:03}"), ActorId::new()))
            .collect::<Vec<_>>();
        let chunked = snapshot(2, input.clone(), 200);
        assert!(chunked.frames.len() > 1);

        let mut concatenated = Vec::new();
        for (index, frame) in chunked.frames.iter().enumerate() {
            assert!(frame.encoded_len().expect("frame encodes") <= 200);
            let (chunk, chunks, groups) = groups(frame);
            assert_eq!(chunk as usize, index);
            assert_eq!(chunks as usize, chunked.frames.len());
            concatenated.extend(groups);
        }
        assert_eq!(concatenated, input);
    }

    fn pool_at<M>(endpoint: &EndpointInner, port: u16) -> Result<Vec<ActorId>, ReceptionistError>
    where
        M: Manifest + Serialize + Send + 'static,
    {
        resolve_key_at(endpoint, &Key::<M>::new("pool"), addr(port))
            .map(|refs| refs.iter().map(|actor_ref| actor_ref.actor_id()).collect())
    }

    fn up_with_pool<M>(endpoint: &EndpointInner, port: u16) -> (NodeId, ActorId)
    where
        M: Manifest,
    {
        let origin = NodeId::new(addr(port));
        let id = ActorId::new();
        up(endpoint, origin);
        assert!(apply(endpoint, origin, 1, vec![group::<M>("pool", id)]));
        (origin, id)
    }

    /// An address answers the registrations of the member there and nobody else's.
    #[test]
    fn lookup_at_answers_the_member_at_the_address_only() {
        let endpoint = endpoint();
        let (_, second) = up_with_pool::<u64>(endpoint, 2);
        let (_, third) = up_with_pool::<u64>(endpoint, 3);

        assert_eq!(pool_at::<u64>(endpoint, 2).expect("resolves"), vec![second]);
        assert_eq!(pool_at::<u64>(endpoint, 3).expect("resolves"), vec![third]);
        assert_eq!(pool_at::<u64>(endpoint, 4).expect("resolves"), Vec::new());
    }

    /// The Up filter alone keeps a Down incarnation out while its replica is still present, as it
    /// is between a down and its eviction, and after the eviction its address still answers the
    /// younger incarnation.
    #[test]
    fn lookup_at_skips_a_down_incarnation_at_the_address() {
        let endpoint = endpoint();
        let (older, _) = up_with_pool::<u64>(endpoint, 2);
        let (younger, younger_id) = up_with_pool::<u64>(endpoint, 2);
        let (_, other_id) = up_with_pool::<u64>(endpoint, 3);
        assert!(older.incarnation() < younger.incarnation());

        endpoint.update(|membership, _| membership.down(older, Instant::now()));
        assert_eq!(
            pool_at::<u64>(endpoint, 2).expect("resolves"),
            vec![younger_id]
        );

        endpoint.receptionist().evict_fenced(older);
        assert_eq!(
            pool_at::<u64>(endpoint, 2).expect("resolves"),
            vec![younger_id]
        );
        assert_eq!(
            pool_at::<u64>(endpoint, 3).expect("resolves"),
            vec![other_id]
        );

        endpoint.update(|membership, _| membership.down(younger, Instant::now()));
        endpoint.receptionist().evict_fenced(younger);
        assert_eq!(pool_at::<u64>(endpoint, 2).expect("resolves"), Vec::new());
        assert_eq!(
            pool_at::<u64>(endpoint, 3).expect("resolves"),
            vec![other_id]
        );
    }

    /// A type mismatch is judged at the address alone: a name held under another type elsewhere
    /// does not turn an empty answer into a mismatch, and a Down origin never does.
    #[test]
    fn lookup_at_judges_a_type_mismatch_at_the_address_only() {
        let endpoint = endpoint();
        let (u32_origin, as_u32) = up_with_pool::<u32>(endpoint, 2);
        let (_, as_u64) = up_with_pool::<u64>(endpoint, 3);
        let both = NodeId::new(addr(4));
        let (both_u32, both_u64) = (ActorId::new(), ActorId::new());
        up(endpoint, both);
        assert!(apply(
            endpoint,
            both,
            1,
            vec![
                group::<u32>("pool", both_u32),
                group::<u64>("pool", both_u64),
            ],
        ));

        assert!(matches!(
            pool_at::<u64>(endpoint, 2),
            Err(ReceptionistError::TypeMismatch)
        ));
        assert_eq!(pool_at::<u32>(endpoint, 2).expect("resolves"), vec![as_u32]);
        assert!(matches!(
            pool_at::<u32>(endpoint, 3),
            Err(ReceptionistError::TypeMismatch)
        ));
        assert_eq!(pool_at::<u64>(endpoint, 3).expect("resolves"), vec![as_u64]);
        assert_eq!(
            pool_at::<u64>(endpoint, 4).expect("resolves"),
            vec![both_u64]
        );
        assert_eq!(
            pool_at::<u32>(endpoint, 4).expect("resolves"),
            vec![both_u32]
        );
        assert_eq!(pool_at::<u64>(endpoint, 5).expect("resolves"), Vec::new());

        endpoint.update(|membership, _| membership.down(u32_origin, Instant::now()));
        assert_eq!(pool_at::<u64>(endpoint, 2).expect("resolves"), Vec::new());
    }

    /// This node's own address answers its own registrations, resolved into local references;
    /// an id no longer bound in the registry is skipped.
    #[test]
    fn lookup_at_answers_this_nodes_own_registrations_at_its_address() {
        let endpoint = endpoint();
        let own = endpoint.node();
        let bound = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u64>::new(bound, MailboxCapacity::Unbounded);
        endpoint.registry().bind(self_ref.actor_ref());
        let unbound = ActorId::new();
        assert!(apply(
            endpoint,
            own,
            1,
            vec![group::<u64>("pool", bound), group::<u64>("pool", unbound),],
        ));

        let refs =
            resolve_key_at(endpoint, &Key::<u64>::new("pool"), own.addr()).expect("resolves");
        assert_eq!(refs, vec![self_ref.actor_ref().clone()]);
    }
}
