use crate::{
    ActorId,
    cluster::{
        frame::{Frame, StreamKey},
        node::NodeId,
        peer::DialRequest,
        receptionist::Snapshot,
    },
    quota::{CountedSendError, CountedSender, Quota},
    sync::{read, write},
};
use flume::Sender;
use std::{
    collections::{
        HashMap,
        hash_map::{Entry, OccupiedEntry},
    },
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use thiserror::Error;
use tokio::sync::{oneshot, watch};
use tracing::error;
use uuid::Uuid;

/// A task and the entry it was started for must agree, so a stale task cannot close a newer one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Generation(u64);

#[derive(Debug, Error)]
pub(crate) enum LaneError {
    #[error("node {0} unreachable")]
    NodeUnreachable(NodeId),

    #[error("outbound queue toward node {0} full")]
    OutboundQueueFull(NodeId),
}

/// How a lane drained for a departure ended: the peer acknowledged the drain marker, or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Drain {
    Acknowledged,
    Unacknowledged,
}

pub(crate) struct Lanes {
    lanes: RwLock<HashMap<SocketAddr, Lane>>,
    next_generation: AtomicU64,
    data_streams: usize,
    outbound_capacity: NonZeroUsize,
    dial_request_tx: Sender<DialRequest>,
}

impl Lanes {
    pub(crate) fn new(
        data_streams: usize,
        outbound_capacity: NonZeroUsize,
        dial_request_tx: Sender<DialRequest>,
    ) -> Self {
        Self {
            lanes: RwLock::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
            data_streams,
            outbound_capacity,
            dial_request_tx,
        }
    }

    /// A bound lane refuses any incarnation but its own.
    pub(crate) fn enqueue<P, A>(
        &self,
        peer: NodeId,
        frame: Frame<'static>,
        prepare: P,
        may_open: A,
    ) -> Result<(), LaneError>
    where
        P: FnOnce() -> Result<Arc<Snapshot>, LaneError>,
        A: FnOnce() -> bool,
    {
        {
            let lanes = read(&self.lanes);
            if let Some(outbound_tx) = matching_lane(&lanes, peer, &frame)? {
                return try_send(outbound_tx, frame, peer);
            }
        }

        let snapshot = prepare()?;
        let mut lanes = write(&self.lanes);
        if let Some(outbound_tx) = matching_lane(&lanes, peer, &frame)? {
            return try_send(outbound_tx, frame, peer);
        }
        if !may_open() {
            return Err(LaneError::NodeUnreachable(peer));
        }

        let outbound_tx = self.open(&mut lanes, peer.addr(), &frame, snapshot);
        try_send(outbound_tx, frame, peer)
    }

    pub(crate) fn offer_snapshot(&self, addr: SocketAddr, snapshot: &Arc<Snapshot>) {
        let lanes = read(&self.lanes);
        let Some(lane) = lanes.get(&addr) else {
            return;
        };
        lane.snapshot_tx.send_if_modified(|pending| {
            let behind = pending
                .as_ref()
                .is_none_or(|pending| pending.version < snapshot.version);
            if behind {
                *pending = Some(snapshot.clone());
            }
            behind
        });
    }

    /// `None` if no lane exists.
    pub(crate) fn connected(&self, addr: SocketAddr) -> Option<bool> {
        read(&self.lanes).get(&addr).map(|lane| lane.connected)
    }

    /// The data stream the frames delivered to the actor ride, `None` if every frame rides the
    /// control stream.
    pub(crate) fn data_stream(&self, actor: ActorId) -> Option<usize> {
        NonZeroUsize::new(self.data_streams)
            .map(|streams| stream_index(StreamKey::Actor(actor), streams))
    }

    /// A dead peer's lane is removed rather than bound, so a successor opens a fresh one; `is_down`
    /// runs under the lanes lock, else a node death in between is missed.
    pub(crate) fn bind<D>(&self, addr: SocketAddr, generation: Generation, peer: NodeId, is_down: D)
    where
        D: FnOnce() -> bool,
    {
        let mut lanes = write(&self.lanes);
        let down = is_down();
        if let Some(mut lane) = current(&mut lanes, addr, generation) {
            if down {
                lane.remove();
            } else {
                let lane = lane.get_mut();
                lane.peer = Some(peer);
                lane.connected = true;
            }
        }
    }

    pub(crate) fn disconnect(&self, addr: SocketAddr, generation: Generation) {
        if let Some(mut lane) = current(&mut write(&self.lanes), addr, generation) {
            lane.get_mut().connected = false;
        }
    }

    pub(crate) fn is_open(&self, addr: SocketAddr, generation: Generation) -> bool {
        read(&self.lanes)
            .get(&addr)
            .is_some_and(|lane| lane.id == generation)
    }

    /// `true` if this call removed the lane, i.e. it was still the address's current one.
    pub(crate) fn remove(&self, addr: SocketAddr, generation: Generation) -> bool {
        current(&mut write(&self.lanes), addr, generation)
            .map(OccupiedEntry::remove)
            .is_some()
    }

    pub(crate) fn remove_fenced(&self, fence: NodeId) {
        // An unbound lane is still dialing and may already serve the successor incarnation!
        let mut lanes = write(&self.lanes);
        if lanes
            .get(&fence.addr())
            .is_some_and(|lane| lane.peer.is_some_and(|peer| fence.covers(peer)))
        {
            lanes.remove(&fence.addr());
        }
    }

    pub(crate) fn drain(&self) -> Vec<(SocketAddr, oneshot::Receiver<Drain>)> {
        write(&self.lanes)
            .drain()
            .map(|(addr, lane)| (addr, lane.done_rx))
            .collect()
    }

    /// One [Quota] for the whole lane, so the capacity holds however many streams share it.
    fn open<'a>(
        &self,
        lanes: &'a mut HashMap<SocketAddr, Lane>,
        addr: SocketAddr,
        frame: &Frame,
        snapshot: Arc<Snapshot>,
    ) -> &'a CountedSender<Frame<'static>> {
        let quota = Quota::bounded(self.outbound_capacity);
        let lane_id = Generation(self.next_generation.fetch_add(1, Ordering::Relaxed));

        let (control_tx, control_rx) = flume::unbounded();
        let control_tx = CountedSender::new(control_tx, quota.clone());
        let (data_tx, data_rx) = (0..self.data_streams)
            .map(|_| {
                let (data_tx, data_rx) = flume::unbounded();
                (CountedSender::new(data_tx, quota.clone()), data_rx)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let (snapshot_tx, snapshot_rx) = watch::channel(Some(snapshot));
        let (done_tx, done_rx) = oneshot::channel();
        let lane = Lane {
            id: lane_id,
            peer: None,
            connected: false,
            control_tx,
            data_tx,
            snapshot_tx,
            done_rx,
        };
        lanes.insert(addr, lane);

        let dial_request = DialRequest {
            addr,
            lane_id,
            control_rx,
            data_rx,
            quota,
            snapshot_rx,
            done_tx,
        };
        if self.dial_request_tx.send(dial_request).is_err() {
            error!(peer_addr = %addr, "dial loop gone, lane will never be connected");
            debug_assert!(false, "dial loop gone");
        }

        lanes
            .get(&addr)
            .expect("lane was just inserted")
            .outbound_tx(frame)
    }
}

struct Lane {
    id: Generation,
    peer: Option<NodeId>,
    connected: bool,
    control_tx: CountedSender<Frame<'static>>,
    data_tx: Vec<CountedSender<Frame<'static>>>,
    snapshot_tx: watch::Sender<Option<Arc<Snapshot>>>,
    done_rx: oneshot::Receiver<Drain>,
}

impl Lane {
    /// System frames bypass the outbound capacity; a frame's [StreamKey] picks the stream, hence
    /// FIFO per recipient.
    fn outbound_tx(&self, frame: &Frame) -> &CountedSender<Frame<'static>> {
        let data_tx = frame
            .stream_key()
            .zip(NonZeroUsize::new(self.data_tx.len()))
            .map(|(key, streams)| &self.data_tx[stream_index(key, streams)]);

        data_tx.unwrap_or(&self.control_tx)
    }
}

fn current(
    lanes: &mut HashMap<SocketAddr, Lane>,
    addr: SocketAddr,
    generation: Generation,
) -> Option<OccupiedEntry<'_, SocketAddr, Lane>> {
    match lanes.entry(addr) {
        Entry::Occupied(lane) if lane.get().id == generation => Some(lane),
        _ => None,
    }
}

fn matching_lane<'a>(
    lanes: &'a HashMap<SocketAddr, Lane>,
    peer: NodeId,
    frame: &Frame,
) -> Result<Option<&'a CountedSender<Frame<'static>>>, LaneError> {
    match lanes.get(&peer.addr()) {
        Some(lane) if lane.peer.is_none_or(|bound| bound == peer) => {
            Ok(Some(lane.outbound_tx(frame)))
        }

        Some(_) => Err(LaneError::NodeUnreachable(peer)),

        None => Ok(None),
    }
}

fn try_send(
    outbound_tx: &CountedSender<Frame<'static>>,
    frame: Frame<'static>,
    peer: NodeId,
) -> Result<(), LaneError> {
    let result = if frame.is_counted() {
        outbound_tx.try_send_counted(frame)
    } else {
        outbound_tx
            .try_send_uncounted(frame)
            .map_err(CountedSendError::from)
    };

    result.map_err(|error| match error {
        CountedSendError::Full(_) => LaneError::OutboundQueueFull(peer),
        CountedSendError::Disconnected(_) => LaneError::NodeUnreachable(peer),
    })
}

/// The mapping only has to agree with itself: nothing about it travels the wire.
fn stream_index(key: StreamKey, streams: NonZeroUsize) -> usize {
    let value = match key {
        StreamKey::Actor(actor) => Uuid::from(actor).as_u128() as u64,
        StreamKey::Nonce(nonce) => nonce.as_u64(),
    };
    value as usize % streams
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        cluster::{
            discovery::Nonce,
            frame::{Frame, StreamKey},
            lanes::{Generation, Lane, LaneError, Lanes, matching_lane, stream_index, try_send},
            node::NodeId,
        },
        quota::{CountedSender, Quota},
        sync::read,
    };
    use flume::Receiver;
    use std::{
        borrow::Cow,
        collections::HashMap,
        net::SocketAddr,
        num::NonZeroUsize,
        sync::{RwLock, atomic::AtomicU64},
    };
    use tokio::sync::{oneshot, watch};

    fn lanes_with(addr: SocketAddr, lane: Lane) -> Lanes {
        Lanes {
            lanes: RwLock::new(HashMap::from([(addr, lane)])),
            next_generation: AtomicU64::new(1),
            data_streams: 0,
            outbound_capacity: NonZeroUsize::MIN,
            dial_request_tx: flume::unbounded().0,
        }
    }

    fn gossip() -> Frame<'static> {
        Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
    }

    fn message(target: ActorId) -> Frame<'static> {
        Frame::Message {
            target,
            reply_tags: Vec::new(),
            payload: Cow::Borrowed(&[]),
        }
    }

    fn lane(
        streams: usize,
    ) -> (
        Lane,
        Receiver<Frame<'static>>,
        Vec<Receiver<Frame<'static>>>,
    ) {
        lane_with_quota(streams, Quota::unbounded())
    }

    fn lane_with_quota(
        streams: usize,
        quota: Quota,
    ) -> (
        Lane,
        Receiver<Frame<'static>>,
        Vec<Receiver<Frame<'static>>>,
    ) {
        let (control_tx, control_rx) = flume::unbounded();
        let control_tx = CountedSender::new(control_tx, quota.clone());
        let (data_tx, data_rx) = (0..streams)
            .map(|_| {
                let (data_tx, data_rx) = flume::unbounded();
                (CountedSender::new(data_tx, quota.clone()), data_rx)
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let lane = Lane {
            id: Generation(0),
            peer: None,
            connected: false,
            control_tx,
            data_tx,
            snapshot_tx: watch::Sender::new(None),
            done_rx: oneshot::channel().1,
        };
        (lane, control_rx, data_rx)
    }

    fn send(lane: &Lane, frame: Frame<'static>) {
        lane.outbound_tx(&frame)
            .try_send_uncounted(frame)
            .expect("the queue is open");
    }

    fn try_send_on(lane: &Lane, frame: Frame<'static>, peer: NodeId) -> Result<(), LaneError> {
        try_send(lane.outbound_tx(&frame), frame, peer)
    }

    /// Sequential nonces spread round-robin without a general-purpose hash, while a key always
    /// maps to the same stream.
    #[test]
    fn nonce_streams_use_the_nonce_value_directly() {
        let streams = NonZeroUsize::new(8).expect("eight is nonzero");
        let next = AtomicU64::new(0);
        let indices = (0..8)
            .map(|_| stream_index(StreamKey::Nonce(Nonce::mint(&next)), streams))
            .collect::<Vec<_>>();
        assert_eq!(indices, (0..8).collect::<Vec<_>>());
    }

    /// A lane bound to one incarnation refuses frames for any other, so a frame for a dead
    /// incarnation never rides its successor's connection.
    #[test]
    fn a_frame_for_another_incarnation_is_refused() {
        let (mut lane, _control_rx, _data_rx) = lane(0);
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        lane.peer = Some(peer);
        let lanes = HashMap::from([(addr, lane)]);

        assert!(matches!(
            matching_lane(&lanes, peer, &gossip()),
            Ok(Some(_))
        ));

        let successor = NodeId::new(addr);
        assert!(matches!(
            matching_lane(&lanes, successor, &gossip()),
            Err(LaneError::NodeUnreachable(_))
        ));

        let unknown = NodeId::new("127.0.0.1:2553".parse().expect("valid address"));
        assert!(matches!(
            matching_lane(&lanes, unknown, &gossip()),
            Ok(None)
        ));
    }

    /// A transport without data streams puts every frame on the control stream, which is one
    /// ordered lane per peer carrying everything: the guarantees hold there by the same argument,
    /// not as a special case.
    #[test]
    fn without_data_streams_everything_rides_the_control_stream() {
        let (lane, control_rx, _data_rx) = lane(0);
        let (target, watcher) = (ActorId::new(), ActorId::new());

        assert!(lane.data_tx.is_empty(), "the lane has no data streams");

        send(
            &lane,
            Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            },
        );
        send(&lane, Frame::Terminated { target, watcher });
        send(&lane, Frame::Watch { target, watcher });
        send(&lane, gossip());

        assert_eq!(control_rx.len(), 4);
    }

    /// A terminated signal rides its watcher's stream, the one the messages to that watcher ride:
    /// that shared queue is the whole mechanism behind the ordering guarantee.
    #[test]
    fn a_terminated_signal_shares_its_watchers_stream() {
        let streams = NonZeroUsize::new(8).expect("8 is not zero");
        let (lane, control_rx, data_rx) = lane(streams.get());
        let watcher = ActorId::new();

        send(
            &lane,
            Frame::Message {
                target: watcher,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            },
        );
        send(
            &lane,
            Frame::Terminated {
                target: ActorId::new(),
                watcher,
            },
        );

        assert_eq!(
            data_rx[stream_index(StreamKey::Actor(watcher), streams)].len(),
            2
        );
        assert!(control_rx.is_empty());
    }

    /// Per-node frames stay on the control stream even where data streams exist: only frames
    /// delivered to an actor have a stream to pick.
    #[test]
    fn per_node_frames_stay_on_the_control_stream() {
        let (lane, control_rx, data_rx) = lane(8);
        let (target, watcher) = (ActorId::new(), ActorId::new());

        send(&lane, Frame::Watch { target, watcher });
        send(&lane, Frame::Unwatch { target, watcher });
        send(&lane, gossip());

        assert_eq!(control_rx.len(), 3);
        assert!(data_rx.iter().all(Receiver::is_empty));
    }

    /// Message frames are counted against the outbound capacity and become dead letters once it
    /// is exhausted, while system frames bypass the count: a terminated signal must never be
    /// dropped for want of capacity. A queued frame holds its slot until the receiver dequeues
    /// it, so the second message is refused although the first is still in the queue.
    #[test]
    fn messages_are_counted_against_the_outbound_capacity_and_system_frames_bypass_it() {
        let capacity = NonZeroUsize::new(1).expect("1 is not zero");
        let (lane, control_rx, _data_rx) = lane_with_quota(0, Quota::bounded(capacity));
        let (target, watcher) = (ActorId::new(), ActorId::new());
        let peer = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));

        assert!(try_send_on(&lane, message(target), peer).is_ok());
        assert!(matches!(
            try_send_on(&lane, message(target), peer),
            Err(LaneError::OutboundQueueFull(full)) if full == peer
        ));

        assert!(try_send_on(&lane, Frame::Terminated { target, watcher }, peer).is_ok());
        assert!(try_send_on(&lane, Frame::Watch { target, watcher }, peer).is_ok());
        assert!(try_send_on(&lane, gossip(), peer).is_ok());

        assert_eq!(control_rx.len(), 4);
    }

    /// A send toward a peer whose lane is gone is unreachable, not full: the two failures steer
    /// different callers, dead-lettering versus giving up on the node.
    #[test]
    fn a_disconnected_lane_reports_the_node_unreachable() {
        let capacity = NonZeroUsize::new(1).expect("1 is not zero");
        let (lane, control_rx, _data_rx) = lane_with_quota(0, Quota::bounded(capacity));
        let target = ActorId::new();
        let peer = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));
        drop(control_rx);

        assert!(matches!(
            try_send_on(&lane, message(target), peer),
            Err(LaneError::NodeUnreachable(unreachable)) if unreachable == peer
        ));
        assert!(matches!(
            try_send_on(&lane, gossip(), peer),
            Err(LaneError::NodeUnreachable(unreachable)) if unreachable == peer
        ));
    }

    /// A dial which lost the race against node death removes its lane instead of binding it to
    /// the dead incarnation, so a send to the successor opens a fresh lane rather than being
    /// refused until the dead connection breaks.
    #[test]
    fn binding_a_tombstoned_peer_removes_the_lane() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let lanes = lanes_with(addr, lane(0).0);

        lanes.bind(addr, Generation(0), peer, || true);

        assert!(read(&lanes.lanes).is_empty());
    }

    /// Binding a live peer names it on the lane, and a lane under another ID or address stays
    /// untouched either way: it belongs to a successor dial, which a stale task must not close.
    #[test]
    fn binding_names_the_peer_and_spares_other_lanes() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let lanes = lanes_with(addr, lane(0).0);
        let bound = |lanes: &Lanes| read(&lanes.lanes).get(&addr).and_then(|lane| lane.peer);

        lanes.bind(addr, Generation(0), peer, || false);
        assert_eq!(bound(&lanes), Some(peer));
        assert_eq!(lanes.connected(addr), Some(true));

        lanes.bind(addr, Generation(1), NodeId::new(addr), || true);
        assert_eq!(bound(&lanes), Some(peer));

        let other_addr = "127.0.0.1:2553".parse().expect("valid address");
        lanes.bind(other_addr, Generation(0), peer, || true);
        assert_eq!(read(&lanes.lanes).len(), 1);
    }

    /// Only the current generation may disconnect or remove a lane, so a stale dial task cannot
    /// close the lane a successor dial serves.
    #[test]
    fn a_stale_generation_neither_disconnects_nor_removes_the_lane() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let lanes = lanes_with(addr, lane(0).0);
        lanes.bind(addr, Generation(0), NodeId::new(addr), || false);

        lanes.disconnect(addr, Generation(1));
        assert_eq!(lanes.connected(addr), Some(true));
        assert!(!lanes.remove(addr, Generation(1)));
        assert!(lanes.is_open(addr, Generation(0)));

        lanes.disconnect(addr, Generation(0));
        assert_eq!(lanes.connected(addr), Some(false));
        assert!(lanes.remove(addr, Generation(0)));
        assert!(!lanes.is_open(addr, Generation(0)));
    }
}
