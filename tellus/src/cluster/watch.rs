use crate::{
    ActorId,
    cluster::{
        endpoint::{self, EndpointInner},
        frame::Frame,
        node::NodeId,
    },
    sync::lock,
    watch::{ActorTerminated, TerminatedHandler, Watcher},
};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};
use tracing::{debug, error};

type Watchers = HashMap<ActorId, Watcher>;

pub(crate) struct WatcherTable(PeerKeyed<Watchers>);

impl WatcherTable {
    pub(crate) fn new() -> Self {
        Self(PeerKeyed::new())
    }

    /// `true` if a `Watch` frame is due; the wire watch is per watcher, not per target.
    pub(crate) fn add(&self, peer: NodeId, target: ActorId, watcher: Watcher) -> bool {
        match self
            .0
            .entries()
            .entry(peer)
            .or_default()
            .entry(target)
            .or_default()
            .entry(watcher.watcher_id())
        {
            Entry::Vacant(entry) => {
                entry.insert(watcher);
                true
            }

            Entry::Occupied(_) => false,
        }
    }

    /// `true` if the watcher was registered, i.e. an `Unwatch` frame is due.
    pub(crate) fn remove(&self, peer: NodeId, target: ActorId, watcher_id: ActorId) -> bool {
        let mut peers = self.0.entries();
        let Some(targets) = peers.get_mut(&peer) else {
            return false;
        };
        let Some(watchers) = targets.get_mut(&target) else {
            return false;
        };

        if watchers.remove(&watcher_id).is_none() {
            return false;
        }

        if watchers.is_empty() {
            PeerKeyed::prune(&mut peers, peer, target);
        }
        true
    }

    pub(crate) fn take_watcher(
        &self,
        peer: NodeId,
        target: ActorId,
        watcher_id: ActorId,
    ) -> Option<Watcher> {
        let mut peers = self.0.entries();
        let targets = peers.get_mut(&peer)?;
        let watchers = targets.get_mut(&target)?;

        let watcher = watchers.remove(&watcher_id);
        if watchers.is_empty() {
            PeerKeyed::prune(&mut peers, peer, target);
        }
        watcher
    }

    pub(crate) fn take_target(&self, peer: NodeId, target: ActorId) -> Vec<Watcher> {
        self.0
            .take_target(peer, target)
            .map(|watchers| watchers.into_values().collect())
            .unwrap_or_default()
    }

    pub(crate) fn take_addr(&self, addr: SocketAddr) -> Vec<(ActorId, Vec<Watcher>)> {
        listed(self.0.take_addr(addr))
    }

    pub(crate) fn take_fenced(&self, fence: NodeId) -> Vec<(ActorId, Vec<Watcher>)> {
        listed(self.0.take_fenced(fence))
    }

    pub(crate) fn peers(&self) -> Vec<NodeId> {
        self.0.peers()
    }

    pub(crate) fn watches(&self, peer: NodeId) -> Vec<(ActorId, ActorId)> {
        self.0
            .entries()
            .get(&peer)
            .map(|targets| {
                targets
                    .iter()
                    .flat_map(|(target, watchers)| {
                        watchers.keys().map(|watcher| (*target, *watcher))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The wire watches other nodes hold on local actors.
pub(crate) struct WireWatchTable(PeerKeyed<WireWatch>);

impl WireWatchTable {
    pub(crate) fn new() -> Self {
        Self(PeerKeyed::new())
    }

    /// Only the registered watches have a watcher to deregister.
    pub(crate) fn take_fenced(&self, fence: NodeId) -> Vec<RegisteredWatch> {
        self.0
            .take_fenced(fence)
            .into_iter()
            .filter_map(Self::registered)
            .collect()
    }

    fn registered((target, watch): (ActorId, WireWatch)) -> Option<RegisteredWatch> {
        match watch {
            WireWatch::Registered {
                wire_watcher_id, ..
            } => Some(RegisteredWatch {
                target,
                wire_watcher_id,
            }),

            WireWatch::Pending(_) => None,
        }
    }

    /// `true` if a registration is due, i.e. this is the target's first watcher on that peer.
    fn add(&self, peer: NodeId, target: ActorId, watcher: ActorId) -> bool {
        match self.0.entries().entry(peer).or_default().entry(target) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().watchers().insert(watcher);
                false
            }

            Entry::Vacant(entry) => {
                entry.insert(WireWatch::Pending(HashSet::from([watcher])));
                true
            }
        }
    }

    fn confirm(&self, peer: NodeId, target: ActorId, wire_watcher_id: ActorId) -> bool {
        let mut peers = self.0.entries();
        let Some(entry) = peers
            .get_mut(&peer)
            .and_then(|targets| targets.get_mut(&target))
        else {
            return false;
        };

        let watchers = mem::take(entry.watchers());
        *entry = WireWatch::Registered {
            wire_watcher_id,
            watchers,
        };
        true
    }

    /// The wire watcher to deregister once the last watcher of the target on that peer is gone.
    fn remove(&self, peer: NodeId, target: ActorId, watcher: ActorId) -> Option<ActorId> {
        let mut peers = self.0.entries();
        let targets = peers.get_mut(&peer)?;
        let entry = targets.get_mut(&target)?;

        entry.watchers().remove(&watcher);
        if !entry.watchers().is_empty() {
            return None;
        }

        let wire_watcher_id = match entry {
            WireWatch::Registered {
                wire_watcher_id, ..
            } => Some(*wire_watcher_id),
            WireWatch::Pending(_) => None,
        };
        PeerKeyed::prune(&mut peers, peer, target);
        wire_watcher_id
    }

    fn take(&self, peer: NodeId, target: ActorId) -> Option<WireWatch> {
        self.0.take_target(peer, target)
    }
}

pub(crate) struct RegisteredWatch {
    pub(crate) target: ActorId,
    pub(crate) wire_watcher_id: ActorId,
}

/// A watch on a dead incarnation fires at once; the membership is reread after registering,
/// else a node death in between leaves the watcher unsignaled. A departed incarnation whose
/// connection still drains is not dead yet: its watchers fire with its deferred death.
pub(crate) fn watch_remote(peer: NodeId, target: ActorId, watcher: Watcher) {
    let Some(endpoint) = endpoint::get() else {
        error!(
            watcher_id = %watcher.watcher_id(),
            other_id = %target,
            "cannot watch a remote actor, remoting endpoint not started"
        );
        return;
    };

    if !endpoint.delivers_from(peer) {
        fire(&watcher, target);
        return;
    }

    let watcher_id = watcher.watcher_id();
    let watch_due = endpoint.watchers().add(peer, target, watcher);

    if !endpoint.delivers_from(peer) {
        fire_all(endpoint.watchers().take_target(peer, target), target);
        return;
    }

    if watch_due
        && !endpoint.membership().is_down(peer)
        && endpoint
            .send(
                peer,
                Frame::Watch {
                    target,
                    watcher: watcher_id,
                },
            )
            .is_err()
    {
        // `take_watcher`, not `take_target`: the others' registrations survive this failed send.
        if let Some(watcher) = endpoint.watchers().take_watcher(peer, target, watcher_id) {
            fire(&watcher, target);
        }
    }
}

/// A given-up lane owes its watchers a signal, for every incarnation at its address.
pub(crate) fn fail_watchers_at(endpoint: &EndpointInner, addr: SocketAddr) {
    for (target, watchers) in endpoint.watchers().take_addr(addr) {
        fire_all(watchers, target);
    }
}

pub(crate) fn unwatch_remote(peer: NodeId, target: ActorId, watcher_id: ActorId) {
    let Some(endpoint) = endpoint::get() else {
        return;
    };

    if endpoint.watchers().remove(peer, target, watcher_id)
        && let Err(error) = endpoint.send(
            peer,
            Frame::Unwatch {
                target,
                watcher: watcher_id,
            },
        )
    {
        debug!(%peer, actor_id = %target, %error, "cannot revert the wire watch");
    }
}

/// Called synchronously by the reader task, so the signal stays ordered behind the messages.
pub(crate) fn on_terminated(
    endpoint: &EndpointInner,
    peer: NodeId,
    target: ActorId,
    watcher_id: ActorId,
) {
    if let Some(watcher) = endpoint.watchers().take_watcher(peer, target, watcher_id) {
        fire(&watcher, target);
    }
}

/// An unknown or terminated target is answered with a `Terminated` frame right away.
pub(crate) fn on_watch(
    endpoint: &'static EndpointInner,
    peer: NodeId,
    target: ActorId,
    watcher_id: ActorId,
) {
    if !endpoint.wire_watches().add(peer, target, watcher_id) {
        eprintln!("DIAG {} {:.3} on_watch {target} by {watcher_id} from {peer}: already known", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64());
        return;
    }
    eprintln!("DIAG {} {:.3} on_watch {target} by {watcher_id} from {peer}: new", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64());

    let registered = endpoint
        .registry()
        .watcher_registry(target)
        .and_then(|watcher_registry| {
            let wire_watcher_id = ActorId::new();
            let handler = Arc::new(WireTerminatedHandler { endpoint, peer });
            watcher_registry
                .add(Watcher::new(wire_watcher_id, handler))
                .ok()
                .map(|()| (watcher_registry, wire_watcher_id))
        });

    match registered {
        Some((watcher_registry, wire_watcher_id)) => {
            if !endpoint
                .wire_watches()
                .confirm(peer, target, wire_watcher_id)
            {
                watcher_registry.remove(wire_watcher_id);
            }
        }

        None => {
            eprintln!("DIAG {} {:.3} on_watch {target}: no registry, answering", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64());
            let watchers = endpoint.wire_watches().take(peer, target);
            let _ = answer_terminated(endpoint, peer, target, watchers);
        }
    }
}

pub(crate) fn on_unwatch(
    endpoint: &EndpointInner,
    peer: NodeId,
    target: ActorId,
    watcher_id: ActorId,
) {
    if let Some(wire_watcher_id) = endpoint.wire_watches().remove(peer, target, watcher_id)
        && let Some(watcher_registry) = endpoint.registry().watcher_registry(target)
    {
        watcher_registry.remove(wire_watcher_id);
    }
}

/// No empty per-peer level: a removal prunes a peer whose last target went away.
struct PeerKeyed<V>(Mutex<HashMap<NodeId, HashMap<ActorId, V>>>);

impl<V> PeerKeyed<V> {
    fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<NodeId, HashMap<ActorId, V>>> {
        lock(&self.0)
    }

    fn peers(&self) -> Vec<NodeId> {
        self.entries().keys().copied().collect()
    }

    fn take_target(&self, peer: NodeId, target: ActorId) -> Option<V> {
        Self::prune(&mut self.entries(), peer, target)
    }

    fn take_addr(&self, addr: SocketAddr) -> Vec<(ActorId, V)> {
        self.take_where(|node| node.addr() == addr)
    }

    fn take_fenced(&self, fence: NodeId) -> Vec<(ActorId, V)> {
        self.take_where(|node| fence.covers(node))
    }

    fn take_where<F>(&self, matches: F) -> Vec<(ActorId, V)>
    where
        F: Fn(NodeId) -> bool,
    {
        let mut peers = self.entries();
        let taken = peers
            .keys()
            .copied()
            .filter(|node| matches(*node))
            .collect::<Vec<_>>();

        taken
            .into_iter()
            .filter_map(|node| peers.remove(&node))
            .flatten()
            .collect()
    }

    fn prune(
        peers: &mut HashMap<NodeId, HashMap<ActorId, V>>,
        peer: NodeId,
        target: ActorId,
    ) -> Option<V> {
        let targets = peers.get_mut(&peer)?;

        let value = targets.remove(&target);
        if targets.is_empty() {
            peers.remove(&peer);
        }
        value
    }
}

/// Both states retain local watchers; only `Registered` carries a `wire_watcher_id`.
enum WireWatch {
    Pending(HashSet<ActorId>),

    Registered {
        wire_watcher_id: ActorId,
        watchers: HashSet<ActorId>,
    },
}

impl WireWatch {
    fn watchers(&mut self) -> &mut HashSet<ActorId> {
        match self {
            WireWatch::Registered { watchers, .. } => watchers,
            WireWatch::Pending(watchers) => watchers,
        }
    }

    fn into_watchers(self) -> HashSet<ActorId> {
        match self {
            WireWatch::Registered { watchers, .. } => watchers,
            WireWatch::Pending(watchers) => watchers,
        }
    }
}

struct WireTerminatedHandler {
    endpoint: &'static EndpointInner,
    peer: NodeId,
}

impl TerminatedHandler for WireTerminatedHandler {
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        let target = actor_id;
        eprintln!("DIAG {} {:.3} wire handler fires for {target} toward {}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(), self.peer);
        let watch = self.endpoint.wire_watches().take(self.peer, target);
        answer_terminated(self.endpoint, self.peer, target, watch)
    }
}

fn fire_all(watchers: Vec<Watcher>, target: ActorId) {
    for watcher in watchers {
        fire(&watcher, target);
    }
}

fn fire(watcher: &Watcher, target: ActorId) {
    if let Err(error) = watcher.handle_terminated(target) {
        debug!(
            watcher_id = %watcher.watcher_id(),
            other_id = %target,
            %error,
            "cannot send terminated signal"
        );
    }
}

/// Fails if any watcher was left unanswered; each needs its own frame to ride its own stream.
fn answer_terminated(
    endpoint: &EndpointInner,
    peer: NodeId,
    target: ActorId,
    watch: Option<WireWatch>,
) -> Result<(), ActorTerminated> {
    let mut answered = Ok(());

    for watcher in watch.map(WireWatch::into_watchers).unwrap_or_default() {
        eprintln!("DIAG {} {:.3} answer terminated {target} to {watcher} at {peer}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64());
        if let Err(error) = endpoint.send(peer, Frame::Terminated { target, watcher }) {
            debug!(
                %peer,
                actor_id = %target,
                watcher_id = %watcher,
                %error,
                "cannot send terminated signal to node"
            );
            answered = Err(ActorTerminated);
        }
    }

    answered
}

fn listed(taken: Vec<(ActorId, Watchers)>) -> Vec<(ActorId, Vec<Watcher>)> {
    taken
        .into_iter()
        .map(|(target, watchers)| (target, watchers.into_values().collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, MailboxCapacity,
        cluster::{
            node::NodeId,
            watch::{WatcherTable, WireWatchTable},
        },
        mailbox::make_mailbox,
        watch::Watcher,
    };

    fn watcher(id: ActorId) -> Watcher {
        let (mailbox_handle, _mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        Watcher::new(id, mailbox_handle.terminated_handler())
    }

    fn peer() -> NodeId {
        NodeId::new("127.0.0.1:1234".parse().expect("valid address"))
    }

    /// A given-up lane belongs to an address, so its watchers are taken by address: every
    /// incarnation there is covered, while watchers of a peer elsewhere stay untouched.
    #[test]
    fn take_addr_takes_every_incarnation_at_the_address() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let table = WatcherTable::new();
        let (target, elsewhere_target) = (ActorId::new(), ActorId::new());

        table.add(NodeId::new(addr), target, watcher(ActorId::new()));
        table.add(NodeId::new(addr), target, watcher(ActorId::new()));
        table.add(
            NodeId::new("127.0.0.1:5678".parse().expect("valid address")),
            elsewhere_target,
            watcher(ActorId::new()),
        );

        let taken = table.take_addr(addr);

        assert_eq!(taken.len(), 2);
        assert!(taken.iter().all(|(taken, _)| *taken == target));
        assert!(table.take_addr(addr).is_empty());
        assert_eq!(table.peers().len(), 1);
    }

    /// A fence takes the watches it covers; a newer incarnation keeps its watchers.
    #[test]
    fn take_fenced_spares_a_newer_incarnation_at_the_address() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let table = WatcherTable::new();
        let older = NodeId::new(addr);
        let fence = NodeId::new(addr);
        let newer = NodeId::new(addr);
        assert!(older.incarnation() < fence.incarnation());
        assert!(fence.incarnation() < newer.incarnation());
        let (target, newer_target) = (ActorId::new(), ActorId::new());

        table.add(older, target, watcher(ActorId::new()));
        table.add(fence, target, watcher(ActorId::new()));
        table.add(newer, newer_target, watcher(ActorId::new()));

        let taken = table.take_fenced(fence);

        assert_eq!(taken.len(), 2);
        assert!(taken.iter().all(|(taken, _)| *taken == target));
        assert_eq!(table.peers(), vec![newer]);
    }

    /// A watcher arriving between `add` and `confirm` must survive the pending-to-registered
    /// transition, else it is silently lost and never signaled.
    #[test]
    fn confirm_keeps_watchers_added_while_pending() {
        let table = WireWatchTable::new();
        let peer = peer();
        let target = ActorId::new();
        let (first, second) = (ActorId::new(), ActorId::new());

        assert!(table.add(peer, target, first));
        assert!(!table.add(peer, target, second));

        let wire_watcher_id = ActorId::new();
        assert!(table.confirm(peer, target, wire_watcher_id));

        assert_eq!(table.remove(peer, target, first), None);
        assert_eq!(table.remove(peer, target, second), Some(wire_watcher_id));
    }

    #[test]
    fn confirming_a_vanished_watch_reports_it() {
        let table = WireWatchTable::new();
        let peer = peer();
        let target = ActorId::new();

        assert!(table.add(peer, target, ActorId::new()));
        assert!(table.take_fenced(peer).is_empty());

        assert!(!table.confirm(peer, target, ActorId::new()));
        assert!(table.take(peer, target).is_none());
    }

    /// Taking a registered wire watch hands back all its watchers, so each gets its own
    /// terminated frame.
    #[test]
    fn take_hands_back_all_watchers() {
        let table = WireWatchTable::new();
        let peer = peer();
        let target = ActorId::new();
        let (first, second) = (ActorId::new(), ActorId::new());

        table.add(peer, target, first);
        table.add(peer, target, second);
        assert!(table.confirm(peer, target, ActorId::new()));

        let watchers = table
            .take(peer, target)
            .expect("the wire watch is registered")
            .into_watchers();
        assert_eq!(watchers.len(), 2);
        assert!(watchers.contains(&first));
        assert!(watchers.contains(&second));

        assert!(table.take(peer, target).is_none());
    }

    /// Every watcher of a target makes a `Watch` frame due and every unwatch an `Unwatch` frame:
    /// the wire watch is per watcher, since a terminated signal must name the watcher to ride its
    /// stream.
    #[test]
    fn every_watcher_moves_the_wire_watch() {
        let table = WatcherTable::new();
        let peer = peer();
        let target = ActorId::new();
        let (first, second) = (ActorId::new(), ActorId::new());

        assert!(table.add(peer, target, watcher(first)));
        assert!(table.add(peer, target, watcher(second)));

        assert!(table.remove(peer, target, first));
        assert!(table.remove(peer, target, second));
        assert!(!table.remove(peer, target, second));
    }

    /// Registering the same watcher twice registers it once, so its terminated signal is sent
    /// once, mirroring the local watcher registry.
    #[test]
    fn adding_a_watcher_twice_registers_once() {
        let table = WatcherTable::new();
        let peer = peer();
        let target = ActorId::new();
        let id = ActorId::new();

        assert!(table.add(peer, target, watcher(id)));
        assert!(!table.add(peer, target, watcher(id)));

        assert_eq!(table.take_target(peer, target).len(), 1);
    }

    /// Removing an unknown watcher, target or peer reports that nothing is due, so no stray
    /// `Unwatch` frame is sent.
    #[test]
    fn removing_unknown_entries_is_a_noop() {
        let table = WatcherTable::new();
        let peer = peer();
        let target = ActorId::new();

        assert!(!table.remove(peer, target, ActorId::new()));
        assert!(table.take_target(peer, target).is_empty());
        assert!(table.take_fenced(peer).is_empty());

        assert!(table.add(peer, target, watcher(ActorId::new())));
        assert!(!table.remove(peer, ActorId::new(), ActorId::new()));
    }

    /// Taking a target or a fence takes its watchers and forgets the peer, so it is no longer
    /// heartbeated once nothing is watched on it.
    #[test]
    fn taking_watchers_forgets_the_peer() {
        let table = WatcherTable::new();
        let peer = peer();
        let (first, second) = (ActorId::new(), ActorId::new());

        assert!(table.add(peer, first, watcher(ActorId::new())));
        assert!(table.add(peer, second, watcher(ActorId::new())));
        assert_eq!(table.peers(), vec![peer]);
        assert_eq!(table.watches(peer).len(), 2);

        assert_eq!(table.take_target(peer, first).len(), 1);
        assert_eq!(table.take_fenced(peer).len(), 1);
        assert!(table.peers().is_empty());
    }

    /// A terminated signal takes only the watcher it names, so the other watchers of the same
    /// target keep waiting for theirs.
    #[test]
    fn taking_one_watcher_keeps_the_others() {
        let table = WatcherTable::new();
        let peer = peer();
        let target = ActorId::new();
        let (first, second) = (ActorId::new(), ActorId::new());

        table.add(peer, target, watcher(first));
        table.add(peer, target, watcher(second));

        assert!(table.take_watcher(peer, target, first).is_some());
        assert!(table.take_watcher(peer, target, first).is_none());
        assert_eq!(table.watches(peer), vec![(target, second)]);

        assert!(table.take_watcher(peer, target, second).is_some());
        assert!(table.peers().is_empty());
    }
}
