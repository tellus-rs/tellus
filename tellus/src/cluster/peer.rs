use crate::{
    ActorId,
    cluster::{
        endpoint::{EndpointInner, InboundSlot, MemberAdmission},
        failure::PeerLiveness,
        frame::{
            Frame, Handshake, HandshakeEnvelope, HandshakeError, HandshakeIntent, HandshakeV1,
            RefusalReason,
        },
        lanes::{Drain, Generation},
        membership::{self, JoinAdmission},
        node::{ClusterId, NodeId},
        receptionist::{self, Assembler, Snapshot},
        reply,
        transport::{
            ConnectedControl, Connection, FrameReceiver, FrameSender, PeerIdentity, Transport,
            TransportError,
        },
        watch,
    },
    quota::Quota,
};
use flume::Receiver;
use std::{
    future::pending,
    iter, mem,
    net::SocketAddr,
    slice,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    select,
    sync::oneshot,
    task::{self, JoinError, JoinSet},
    time::{Instant, sleep, timeout},
};
use tracing::{debug, error, info, warn};

#[cfg(test)]
use std::{
    collections::HashSet,
    sync::{LazyLock, Mutex},
};

const BATCH_FRAMES: usize = 64;
const BATCH_LEN: usize = 64 * 1_024;

const MAX_SNAPSHOT_MEMBERS: usize = 1 << 16;

/// Every dead letter this module logs, so a test can prove one happened.
#[cfg(test)]
static DEAD_LETTERS: LazyLock<Mutex<HashSet<ActorId>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) struct DialRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) lane_id: Generation,
    pub(crate) control_rx: Receiver<Frame<'static>>,
    pub(crate) data_rx: Vec<Receiver<Frame<'static>>>,
    pub(crate) quota: Quota,
    pub(crate) snapshot_rx: tokio::sync::watch::Receiver<Option<Arc<Snapshot>>>,
    pub(crate) done_tx: oneshot::Sender<Drain>,
}

pub(crate) struct JoinRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) result_tx: oneshot::Sender<Result<(), ConnectError>>,
}

pub(crate) struct ProbeRequest {
    pub(crate) addr: SocketAddr,
    pub(crate) result_tx: oneshot::Sender<Result<Option<ClusterId>, ConnectError>>,
}

/// A transport failure or a silent peer is transient, everything else is final.
#[derive(Debug, Error)]
pub(crate) enum ConnectError {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error("connection closed before handshake")]
    Closed,

    #[error("timed out opening {0} data streams")]
    DataStreams(usize),

    #[error("timed out exchanging handshakes")]
    HandshakeTimeout,

    #[error("first frame is not a handshake")]
    NotAHandshake,

    #[error("cannot decode handshake frame")]
    Decode(#[from] postcard::Error),

    #[error(transparent)]
    Handshake(#[from] HandshakeError),

    #[error("peer refused the handshake: {0}")]
    Refused(RefusalReason),

    #[error("peer identity does not cover its advertised address")]
    Identity,

    #[error("node dialed at {dialed} advertises {advertised}")]
    AddressMismatch {
        advertised: SocketAddr,
        dialed: SocketAddr,
    },

    #[error("this node has been downed")]
    SelfDowned,

    #[error("join attempt abandoned by its caller")]
    Abandoned,

    #[error("dead node incarnation")]
    Dead,

    #[error("member snapshot exceeds {0} entries")]
    SnapshotTooLarge(usize),

    #[error("node is a member of the other cluster {0}")]
    ForeignCluster(ClusterId),

    #[error("join answered without a cluster ID")]
    NoClusterId,
}

impl ConnectError {
    /// A refusal, and a reply naming another cluster, prove that the peer did not admit a join.
    pub(crate) fn proves_no_admission(&self) -> bool {
        matches!(
            self,
            ConnectError::Refused(_) | ConnectError::ForeignCluster(_)
        )
    }

    /// A refusal as an unknown member is retryable: gossip may not have reached it yet. So is
    /// one from a node which is no cluster yet, until it has formed one or joined one, one holding
    /// back joins, until its unreachable member is reachable again or downed, and so is a dead
    /// incarnation, whose successor may come up at the same address.
    fn is_retryable(&self) -> bool {
        match self {
            ConnectError::Transport(TransportError::FrameTooLarge { .. }) => false,

            ConnectError::Transport(_)
            | ConnectError::Closed
            | ConnectError::DataStreams(_)
            | ConnectError::HandshakeTimeout
            | ConnectError::Refused(RefusalReason::UnknownMember)
            | ConnectError::Refused(RefusalReason::NoCluster)
            | ConnectError::Refused(RefusalReason::JoinsHeld)
            | ConnectError::Dead => true,

            ConnectError::NotAHandshake
            | ConnectError::SnapshotTooLarge(_)
            | ConnectError::Decode(_)
            | ConnectError::Handshake(_)
            | ConnectError::Refused(RefusalReason::Down)
            | ConnectError::Refused(RefusalReason::ForeignCluster)
            | ConnectError::ForeignCluster(_)
            | ConnectError::NoClusterId
            | ConnectError::Identity
            | ConnectError::AddressMismatch { .. }
            | ConnectError::SelfDowned
            | ConnectError::Abandoned => false,
        }
    }

    fn refuses_address(&self, member_at_addr: bool) -> bool {
        !self.is_retryable() && !member_at_addr
    }

    /// Meaningful only for a failure after the `Join` handshake was sent.
    fn leaves_join_ambiguous(&self) -> bool {
        match self {
            ConnectError::Transport(_)
            | ConnectError::Closed
            | ConnectError::DataStreams(_)
            | ConnectError::HandshakeTimeout
            | ConnectError::NotAHandshake
            | ConnectError::Decode(_)
            | ConnectError::Identity
            | ConnectError::AddressMismatch { .. }
            | ConnectError::Dead
            | ConnectError::SnapshotTooLarge(_)
            | ConnectError::NoClusterId => true,

            ConnectError::Refused(_)
            | ConnectError::ForeignCluster(_)
            | ConnectError::Handshake(_)
            | ConnectError::SelfDowned
            | ConnectError::Abandoned => false,
        }
    }
}

pub(crate) async fn accept_loop<T>(transport: Arc<T>, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    loop {
        match transport
            .accept(endpoint.config().max_frame_size.get())
            .await
        {
            Ok(connection) => {
                task::spawn(run_accepted(connection, endpoint));
            }

            Err(error) => {
                error!(%error, "remoting endpoint cannot accept connections");
                break;
            }
        }
    }
}

pub(crate) async fn dial_loop<T>(
    transport: Arc<T>,
    dial_request_rx: Receiver<DialRequest>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    while let Ok(request) = dial_request_rx.recv_async().await {
        task::spawn(run_peer(transport.clone(), request, endpoint));
    }
}

pub(crate) async fn join_loop<T>(
    transport: Arc<T>,
    join_request_rx: Receiver<JoinRequest>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    while let Ok(request) = join_request_rx.recv_async().await {
        let addr = request.addr;
        let admission_possible = Arc::new(AtomicBool::new(false));
        let join = run_join(
            transport.clone(),
            request,
            admission_possible.clone(),
            endpoint,
        );
        if let Err(error) = task::spawn(join).await {
            error!(seed_addr = %addr, %error, "join attempt failed unexpectedly");
            if admission_possible.load(Ordering::Acquire) {
                endpoint.record_ambiguous_join(addr);
            }
            let _ = endpoint.finish_join(addr, Err(ConnectError::Closed));
        }
    }
}

pub(crate) async fn probe_loop<T>(
    transport: Arc<T>,
    probe_request_rx: Receiver<ProbeRequest>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    while let Ok(request) = probe_request_rx.recv_async().await {
        task::spawn(run_probe(transport.clone(), request, endpoint));
    }
}

/// The data streams are ordered as their queues were, which is how [EndpointInner::send] indexes.
struct Connected<C>
where
    C: Connection,
{
    frame_senders: Vec<C::Sender>,
    frame_receiver: C::Receiver,
    greeting: Greeting,
    identity: Option<PeerIdentity>,
}

/// A peer's handshake, its body read with the schema of the negotiated version.
struct Greeting {
    version: u16,
    node: NodeId,
    intent: HandshakeIntent,
    cluster: Option<ClusterId>,
}

/// Whether this node is leaving, and the peer's answer to the drain marker of its departure.
struct LeaveSignals {
    leaving: &'static AtomicBool,
    drain_ack_rx: oneshot::Receiver<()>,
}

enum WriterEnd {
    LaneClosed,
    Acknowledged,
    Unacknowledged,
    ConnectionLost,
}

enum ReadEnd {
    Closed,
    Poisoned,
    Drained(u32),
    DrainAcknowledged,
}

/// What a departed peer announced it drained, and how many of its data streams have ended.
#[derive(Default)]
struct InboundDrain {
    announced: Option<u32>,
    ended: u32,
    acknowledged: bool,
}

impl InboundDrain {
    fn is_complete(&self) -> bool {
        !self.acknowledged && self.announced == Some(self.ended)
    }
}

/// A spoofed identity gets no answer, so the connection is dropped silently rather than refused
/// with a message.
enum Admission {
    Admit(PeerLiveness, InboundSlot),
    Probe,
    Refuse(RefusalReason),
    Drop,
}

/// What an admitted connection hands its reader: the peer's liveness and its place among the
/// peer's inbound connections.
struct Admitted {
    liveness: PeerLiveness,
    inbound: InboundSlot,
}

/// The handshake is bounded by the heartbeat interval, so an unauthenticated dialer cannot pin
/// this task; the admission decides before the reply handshake.
async fn run_accepted<C>(connection: C, endpoint: &'static EndpointInner)
where
    C: Connection,
{
    let establishing = timeout(endpoint.config().heartbeat_interval, async {
        let (mut frame_sender, mut frame_receiver) = match connection.accept_control().await {
            Ok(halves) => halves,
            Err(error) => {
                warn!(%error, "cannot open inbound connection");
                return None;
            }
        };

        let greeting = match recv_handshake(&mut frame_receiver).await {
            Ok(greeting) => greeting,

            Err(ConnectError::Handshake(HandshakeError::Incompatible { ours, theirs })) => {
                warn!(
                    ?ours,
                    ?theirs,
                    "refusing connection, no common protocol version"
                );
                let envelope = HandshakeEnvelope::this_build().encode();
                answer_and_close(&mut frame_sender, &mut frame_receiver, envelope).await;
                return None;
            }

            Err(error) => {
                warn!(%error, "cannot receive inbound handshake");
                return None;
            }
        };
        let Greeting {
            version,
            node: peer,
            intent,
            cluster,
        } = greeting;

        let admission = admit_inbound(peer, intent, cluster, connection.peer_identity(), endpoint);
        let (liveness, inbound) = match admission {
            Admission::Admit(liveness, inbound) => (liveness, inbound),

            Admission::Probe => {
                let reply =
                    Frame::Handshake(Handshake::new(handshake(endpoint, HandshakeIntent::Member)));
                let reply = reply.encode_into(Vec::new());
                answer_and_close(&mut frame_sender, &mut frame_receiver, reply).await;
                return None;
            }

            Admission::Refuse(reason) => {
                let refusal = Frame::Refused { reason }.encode_into(Vec::new());
                answer_and_close(&mut frame_sender, &mut frame_receiver, refusal).await;
                return None;
            }

            Admission::Drop => return None,
        };

        if let Err(error) = send_handshake(
            &mut frame_sender,
            handshake(endpoint, HandshakeIntent::Member),
        )
        .await
        {
            warn!(%peer, %error, "cannot send outbound handshake");
            return None;
        }
        for frame in endpoint.snapshot_frames(endpoint.membership().handshake_snapshot()) {
            if let Err(error) = send_frame(&mut frame_sender, &frame).await {
                warn!(%peer, %error, "cannot send member snapshot");
                return None;
            }
        }
        for frame in endpoint.reachability_snapshot_frames() {
            if let Err(error) = send_frame(&mut frame_sender, &frame).await {
                warn!(%peer, %error, "cannot send reachability snapshot");
                return None;
            }
        }
        // A joiner stops reading after the member snapshot, so only a member gets the rest.
        if intent == HandshakeIntent::Member {
            for frame in &endpoint.registrations_snapshot().frames {
                if let Err(error) = send_frame(&mut frame_sender, frame).await {
                    warn!(%peer, %error, "cannot send registrations snapshot");
                    return None;
                }
            }
        }

        Some((
            frame_sender,
            frame_receiver,
            peer,
            version,
            liveness,
            inbound,
        ))
    })
    .await;

    let established = match establishing {
        Ok(established) => established,

        Err(_) => {
            debug!("timed out establishing inbound connection");
            return;
        }
    };
    let Some((frame_sender, frame_receiver, peer, version, liveness, inbound)) = established else {
        return;
    };
    debug!(%peer, version, "inbound connection established");
    endpoint.unrefuse(peer.addr());

    endpoint
        .supersede_inbound_reader(peer.addr(), |shutdown_rx| {
            task::spawn(read_streams(
                connection,
                frame_receiver,
                frame_sender,
                peer,
                Admitted { liveness, inbound },
                endpoint,
                shutdown_rx,
            ))
        })
        .await;
}

/// The [JoinSet] is awaited on every exit, else a successor reader overlaps a delivery draining
/// here; a poisoned stream ends the whole connection. Neither a finished stream nor a closed
/// connection ends a reader: each runs until its own stream ends, so a departed peer's last
/// frames are delivered before its death. A departed peer's drain marker is acknowledged once
/// exactly as many data streams as it announces have ended cleanly, those which ended before it
/// included.
async fn read_streams<C>(
    connection: C,
    control_rx: C::Receiver,
    mut frame_sender: C::Sender,
    peer: NodeId,
    admitted: Admitted,
    endpoint: &'static EndpointInner,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
) where
    C: Connection,
{
    let Admitted {
        liveness,
        mut inbound,
    } = admitted;
    inbound.reading();
    let mut sever_rx = endpoint.sever_rx();

    let mut readers = JoinSet::new();
    let control = readers
        .spawn(read_frames(control_rx, peer, liveness.clone(), endpoint))
        .id();
    let mut drain = InboundDrain::default();
    let mut open = true;

    while open || !readers.is_empty() {
        select! {
            biased;

            _ = shutdown_rx.changed() => break,

            _ = sever_rx.changed() => break,

            joined = readers.join_next_with_id(), if !readers.is_empty() => {
                let (id, end) = match joined {
                    Some(Ok(ended)) => ended,

                    Some(Err(error)) => {
                        error!(%peer, %error, "closing connection, stream reader failed");
                        break;
                    }

                    None => continue,
                };

                match end {
                    ReadEnd::Closed if id != control => drain.ended = drain.ended.saturating_add(1),

                    ReadEnd::Closed => {}

                    ReadEnd::Drained(data_streams)
                        if id == control && endpoint.departure_deferred(peer) =>
                    {
                        drain.announced = Some(data_streams);
                    }

                    ReadEnd::Poisoned => break,

                    ReadEnd::Drained(_) | ReadEnd::DrainAcknowledged => {
                        warn!(%peer, "closing connection, a drain frame outside a deferred departure");
                        break;
                    }
                }

                if drain.is_complete() && endpoint.departure_deferred(peer) {
                    drain.acknowledged = true;
                    match send_frame(&mut frame_sender, &Frame::DrainAck).await {
                        Ok(()) => debug!(%peer, data_streams = drain.ended, "departure drained"),

                        Err(error) => warn!(%peer, %error, "cannot acknowledge the departure's drain"),
                    }
                }
            },

            accepted = connection.accept_data(), if open => match accepted {
                Ok(Some(data_rx)) => {
                    readers.spawn(read_frames(data_rx, peer, liveness.clone(), endpoint));
                }

                Ok(None) => open = false,

                Err(error) => {
                    debug!(%peer, %error, "no further inbound stream");
                    open = false;
                }
            },
        }
    }

    readers.shutdown().await;
    drop(frame_sender);
    drop(inbound);
}

/// Abort and await the reader before the next dial, or two readers can overlap.
async fn run_peer<T>(transport: Arc<T>, request: DialRequest, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    let DialRequest {
        addr,
        lane_id,
        control_rx,
        data_rx,
        quota,
        snapshot_rx,
        done_tx,
    } = request;
    let streams = data_rx.len();
    let mut drain = Drain::Unacknowledged;
    let outbound_rx = iter::once(control_rx).chain(data_rx).collect::<Vec<_>>();
    let mut sever_rx = endpoint.sever_rx();
    let mut attempts = 0u32;

    loop {
        let established = match connect(transport.as_ref(), addr, streams, endpoint).await {
            Ok(mut connected) => admit_outbound(
                connected.greeting.node,
                connected.greeting.cluster,
                addr,
                connected.identity.take(),
                endpoint,
            )
            .map(|liveness| (connected, liveness)),

            Err(error) => Err(error),
        };

        match established {
            Ok((connected, liveness)) => {
                let Connected {
                    frame_senders,
                    frame_receiver,
                    greeting,
                    identity: _,
                } = connected;
                let peer = greeting.node;
                let (poisoned_tx, mut poisoned_rx) = oneshot::channel();
                let (drain_ack_tx, drain_ack_rx) = oneshot::channel();
                let reader = task::spawn(async move {
                    match read_frames(frame_receiver, peer, liveness, endpoint).await {
                        ReadEnd::Closed => {}

                        ReadEnd::DrainAcknowledged if endpoint.leaving() => {
                            let _ = drain_ack_tx.send(());
                        }

                        ReadEnd::Poisoned => {
                            let _ = poisoned_tx.send(());
                        }

                        ReadEnd::Drained(_) | ReadEnd::DrainAcknowledged => {
                            warn!(%peer, "closing connection, a drain frame outside a departure");
                            let _ = poisoned_tx.send(());
                        }
                    }
                });

                endpoint.bind_lane(addr, lane_id, peer);
                let connected_at = Instant::now();
                debug!(%peer, version = greeting.version, "outbound connection established");

                endpoint.reset_gossiped_version(peer);
                // The heartbeat omits the watermarks, so this is the only path carrying this
                // side's fences to the acceptor!
                for frame in endpoint.snapshot_frames(endpoint.membership().handshake_snapshot()) {
                    if let Err(error) = endpoint.send(peer, frame) {
                        warn!(%peer, %error, "cannot send member snapshot");
                    }
                }
                for frame in endpoint.reachability_snapshot_frames() {
                    if let Err(error) = endpoint.send(peer, frame) {
                        warn!(%peer, %error, "cannot send reachability snapshot");
                    }
                }

                // Watch frames lost with a connection must be re-sent here; idempotent remotely.
                for (target, watcher) in endpoint.watchers().watches(peer) {
                    if let Err(error) = endpoint.send(peer, Frame::Watch { target, watcher }) {
                        warn!(%peer, actor_id = %target, %error, "cannot re-establish remote watch");
                    }
                }
                let end = select! {
                    end = write_streams(
                        frame_senders,
                        &outbound_rx,
                        &quota,
                        addr,
                        endpoint.config().max_frame_size.get(),
                        snapshot_rx.clone(),
                        LeaveSignals {
                            leaving: endpoint.leaving_flag(),
                            drain_ack_rx,
                        },
                    ) => end,

                    _ = sever_rx.changed() => WriterEnd::ConnectionLost,

                    Ok(()) = &mut poisoned_rx => WriterEnd::ConnectionLost,
                };
                endpoint.mark_lane_disconnected(addr, lane_id);
                reader.abort();
                let _ = reader.await;

                match end {
                    WriterEnd::Acknowledged => {
                        drain = Drain::Acknowledged;
                        break;
                    }

                    WriterEnd::LaneClosed | WriterEnd::Unacknowledged => break,

                    WriterEnd::ConnectionLost => {}
                }

                // One heartbeat is the shortest lifetime proving the peer answered here.
                if connected_at.elapsed() >= endpoint.config().heartbeat_interval {
                    attempts = 0;
                } else {
                    attempts += 1;

                    if !backoff_or_give_up(endpoint, addr, attempts).await {
                        break;
                    }
                }
            }

            Err(ConnectError::Refused(RefusalReason::Down)) => {
                endpoint.self_down();
                break;
            }

            Err(ConnectError::SelfDowned) => break,

            Err(error) if error.refuses_address(endpoint.membership().has_up_member_at(addr)) => {
                let backoff = endpoint.refuse(addr);
                warn!(peer_addr = %addr, %error, ?backoff, "giving up connecting to node, refusing its address");
                break;
            }

            Err(error) => {
                attempts += 1;
                if error.is_retryable() {
                    debug!(peer_addr = %addr, attempts, %error, "cannot connect to node");
                } else {
                    warn!(peer_addr = %addr, attempts, %error, "cannot connect to member");
                }

                if !backoff_or_give_up(endpoint, addr, attempts).await {
                    break;
                }
            }
        }

        if !endpoint.is_lane_open(addr, lane_id) && !endpoint.leaving() {
            break;
        }
    }

    if endpoint.remove_lane(addr, lane_id) {
        endpoint.pending_replies().fail_addr(addr);
        watch::fail_watchers_at(endpoint, addr);
    }
    for outbound_rx in outbound_rx {
        drain_dead_letters(outbound_rx, addr).await;
    }
    let _ = done_tx.send(drain);
}

/// A member's address is retried forever: downing, not the attempt count, settles its fate.
async fn backoff_or_give_up(endpoint: &EndpointInner, addr: SocketAddr, attempts: u32) -> bool {
    if !endpoint.membership().has_up_member_at(addr)
        && attempts >= endpoint.config().max_connect_attempts.get()
    {
        warn!(peer_addr = %addr, "giving up connecting to node");
        return false;
    }

    let backoff = sleep(endpoint.reconnect_backoff(attempts));
    if endpoint.leaving() {
        backoff.await;
    } else {
        let mut leave_rx = endpoint.leave_rx();
        select! {
            () = backoff => {}
            _ = leave_rx.wait_for(|leaving| *leaving) => {}
        }
    }
    true
}

async fn connect<T>(
    transport: &T,
    addr: SocketAddr,
    streams: usize,
    endpoint: &EndpointInner,
) -> Result<Connected<T::Connection>, ConnectError>
where
    T: Transport,
{
    let ConnectedControl {
        connection,
        mut control_tx,
        control_rx: mut frame_receiver,
    } = transport
        .connect(addr, endpoint.config().max_frame_size.get())
        .await?;

    let greeting = exchange_handshakes(
        &mut control_tx,
        &mut frame_receiver,
        handshake(endpoint, HandshakeIntent::Member),
        endpoint.config().heartbeat_interval,
    )
    .await?;

    let mut frame_senders = vec![control_tx];
    frame_senders.extend(open_data_streams(&connection, streams, endpoint).await?);

    let identity = connection.peer_identity();
    Ok(Connected {
        frame_senders,
        frame_receiver,
        greeting,
        identity,
    })
}

/// Bounded, so a peer which connects and never speaks cannot pin this lane's dial forever.
async fn exchange_handshakes<S, R>(
    frame_sender: &mut S,
    frame_receiver: &mut R,
    handshake: HandshakeV1,
    within: Duration,
) -> Result<Greeting, ConnectError>
where
    R: FrameReceiver,
    S: FrameSender,
{
    let exchange = async {
        send_handshake(frame_sender, handshake).await?;
        recv_handshake(frame_receiver).await.inspect_err(|error| {
            if let ConnectError::Handshake(HandshakeError::Incompatible { ours, theirs }) = error {
                warn!(?ours, ?theirs, "cannot connect, no common protocol version");
            }
        })
    };

    match timeout(within, exchange).await {
        Ok(peer) => peer,
        Err(_) => Err(ConnectError::HandshakeTimeout),
    }
}

/// Opened before the lane carries anything, so a peer admitting fewer streams fails here.
async fn open_data_streams<C>(
    connection: &C,
    streams: usize,
    endpoint: &EndpointInner,
) -> Result<Vec<C::Sender>, ConnectError>
where
    C: Connection,
{
    let open = async {
        let mut frame_senders = Vec::with_capacity(streams);
        for _ in 0..streams {
            frame_senders.push(connection.open_data().await?);
        }
        Ok::<_, ConnectError>(frame_senders)
    };

    match timeout(endpoint.config().heartbeat_interval, open).await {
        Ok(frame_senders) => frame_senders,

        Err(_) => Err(ConnectError::DataStreams(streams)),
    }
}

/// Leaving, every data stream is finished first, then the control stream carries the drain
/// marker, and the lane is acknowledged only once the peer answers the marker.
async fn write_streams<S>(
    frame_senders: Vec<S>,
    outbound_rx: &[Receiver<Frame<'static>>],
    quota: &Quota,
    addr: SocketAddr,
    max_frame_size: usize,
    snapshot_rx: tokio::sync::watch::Receiver<Option<Arc<Snapshot>>>,
    leave: LeaveSignals,
) -> WriterEnd
where
    S: FrameSender,
{
    let LeaveSignals {
        leaving,
        drain_ack_rx,
    } = leave;
    debug_assert_eq!(
        frame_senders.len(),
        outbound_rx.len(),
        "one writer per queue"
    );
    let data_streams = frame_senders.len().saturating_sub(1);

    // Only the control stream carries snapshots, so only its writer drains the slot.
    let mut snapshot_rx = Some(snapshot_rx);
    let mut writers = JoinSet::new();
    for (frame_sender, outbound_rx) in frame_senders.into_iter().zip(outbound_rx) {
        let snapshot_rx = snapshot_rx.take();
        let control = snapshot_rx.is_some();
        let writer = write_frames(
            frame_sender,
            outbound_rx.clone(),
            quota.clone(),
            addr,
            max_frame_size,
            snapshot_rx,
        );
        writers.spawn(async move {
            let mut frame_sender = writer.await?;
            if control {
                return Ok(Some(frame_sender));
            }
            if leaving.load(Ordering::Relaxed)
                && let Err(error) = frame_sender.finish().await
            {
                warn!(peer_addr = %addr, %error, "stream not acknowledged before leaving");
                return Err(WriterEnd::Unacknowledged);
            }
            Ok(None)
        });
    }

    let mut written = match writers.join_next().await {
        Some(joined) => writer_result(joined, addr),
        None => Err(WriterEnd::ConnectionLost),
    };
    if !leaving.load(Ordering::Relaxed) || matches!(written, Err(WriterEnd::ConnectionLost)) {
        // Await the aborted siblings, else a successor lane starts while a dequeued frame has
        // still not been dead-lettered.
        writers.shutdown().await;
        return written.err().unwrap_or(WriterEnd::LaneClosed);
    }

    let mut control_sender = None;
    let mut finished = true;
    loop {
        match written {
            Ok(Some(frame_sender)) => control_sender = Some(frame_sender),
            Ok(None) => {}
            Err(_) => finished = false,
        }
        let Some(joined) = writers.join_next().await else {
            break;
        };
        written = writer_result(joined, addr);
    }
    let Some(mut control_sender) = control_sender.filter(|_| finished) else {
        return WriterEnd::Unacknowledged;
    };

    let data_streams = match u32::try_from(data_streams) {
        Ok(data_streams) => data_streams,

        Err(error) => {
            warn!(peer_addr = %addr, %error, "cannot announce the drain of every data stream");
            return WriterEnd::Unacknowledged;
        }
    };
    if let Err(error) = send_frame(&mut control_sender, &Frame::Drained { data_streams }).await {
        warn!(peer_addr = %addr, %error, "cannot send the drain marker");
        return WriterEnd::Unacknowledged;
    }
    match drain_ack_rx.await {
        Ok(()) => WriterEnd::Acknowledged,

        Err(_) => {
            warn!(peer_addr = %addr, "connection ended before the drain was acknowledged");
            WriterEnd::Unacknowledged
        }
    }
}

/// `Ok` once the queue is closed and everything dequeued has reached the transport, handing the
/// sender back. The reservation is released on dequeue, before the send, else an abort leaks a
/// slot.
async fn write_frames<S>(
    mut frame_sender: S,
    outbound_rx: Receiver<Frame<'static>>,
    quota: Quota,
    addr: SocketAddr,
    max_frame_size: usize,
    mut snapshot_rx: Option<tokio::sync::watch::Receiver<Option<Arc<Snapshot>>>>,
) -> Result<S, WriterEnd>
where
    S: FrameSender,
{
    let mut buffer = Vec::new();
    let mut encoded = Vec::<Vec<u8>>::new();
    let mut in_flight_batch = Vec::new();

    // Read the slot through borrow_and_update here and after every change, else a snapshot
    // replaced between the change and the read is written twice.
    let initial = snapshot_rx
        .as_mut()
        .and_then(|snapshot_rx| snapshot_rx.borrow_and_update().clone());
    if let Some(snapshot) = initial {
        buffer = write_snapshot(&mut frame_sender, &snapshot, buffer, addr, max_frame_size).await?;
    }

    loop {
        let frame = select! {
            frame = outbound_rx.recv_async() => frame,

            snapshot = snapshot_changed(&mut snapshot_rx) => {
                if let Some(snapshot) = snapshot {
                    buffer = write_snapshot(
                        &mut frame_sender,
                        &snapshot,
                        buffer,
                        addr,
                        max_frame_size,
                    )
                    .await?;
                }
                continue;
            }
        };
        let Ok(frame) = frame else {
            return Ok(frame_sender);
        };
        // Whatever is queued already rides along, so a busy queue costs one write per batch.
        let mut next = Some(frame);
        let mut used = 0;
        let mut batched_len = 0;
        while let Some(frame) = next.take() {
            let mut in_flight = InFlightFrame::new(frame, addr);
            if in_flight.frame().is_counted() {
                quota.unreserve();
            }

            if encoded.len() == used {
                encoded.push(Vec::new());
            }
            let slot = used;
            match in_flight.frame().encode_into(mem::take(&mut encoded[slot])) {
                Ok(bytes) => {
                    // An oversize frame must never reach the transport: the receiver's refusal
                    // kills the connection!
                    if bytes.len() > max_frame_size {
                        in_flight.discard_oversize();
                    } else {
                        batched_len += bytes.len();
                        used += 1;
                        in_flight_batch.push(in_flight);
                    }
                    encoded[slot] = bytes;
                }

                Err(error) => in_flight.discard_unencodable(&error),
            }

            if used < BATCH_FRAMES && batched_len < BATCH_LEN {
                next = outbound_rx.try_recv().ok();
            }
        }
        if used == 0 {
            continue;
        }

        if let Err(error) = frame_sender.send_batch(&encoded[..used]).await {
            warn!(peer_addr = %addr, %error, "connection lost");
            return Err(WriterEnd::ConnectionLost);
        }
        for mut in_flight in in_flight_batch.drain(..) {
            in_flight.disarm();
        }
    }
}

async fn snapshot_changed(
    snapshot_rx: &mut Option<tokio::sync::watch::Receiver<Option<Arc<Snapshot>>>>,
) -> Option<Arc<Snapshot>> {
    let Some(snapshot_rx) = snapshot_rx else {
        return pending().await;
    };
    if snapshot_rx.changed().await.is_err() {
        return pending().await;
    }
    snapshot_rx.borrow_and_update().clone()
}

/// Every chunk goes out back to back from this one writer, else a snapshot is not contiguous.
async fn write_snapshot<S>(
    frame_sender: &mut S,
    snapshot: &Snapshot,
    mut buffer: Vec<u8>,
    addr: SocketAddr,
    max_frame_size: usize,
) -> Result<Vec<u8>, WriterEnd>
where
    S: FrameSender,
{
    for frame in &snapshot.frames {
        buffer = match frame.encode_into(buffer) {
            Ok(bytes) => bytes,

            Err(error) => {
                warn!(peer_addr = %addr, %error, "cannot encode registrations chunk");
                buffer = Vec::new();
                continue;
            }
        };
        if buffer.len() > max_frame_size {
            warn!(peer_addr = %addr, len = buffer.len(), max_frame_size, "dropping an oversize registrations chunk");
            continue;
        }
        if let Err(error) = frame_sender.send(&buffer).await {
            warn!(peer_addr = %addr, %error, "connection lost");
            return Err(WriterEnd::ConnectionLost);
        }
    }
    Ok(buffer)
}

/// A failed writer task counts as a lost connection.
fn writer_result<S>(
    joined: Result<Result<Option<S>, WriterEnd>, JoinError>,
    addr: SocketAddr,
) -> Result<Option<S>, WriterEnd> {
    joined.unwrap_or_else(|error| {
        error!(peer_addr = %addr, %error, "stream writer failed");
        Err(WriterEnd::ConnectionLost)
    })
}

/// Each delivery holds the peer's gate and rechecks the tombstone, in arrival order.
async fn read_frames<R>(
    mut frame_receiver: R,
    peer: NodeId,
    liveness: PeerLiveness,
    endpoint: &'static EndpointInner,
) -> ReadEnd
where
    R: FrameReceiver,
{
    let mut assembler = Assembler::default();

    loop {
        let bytes = match frame_receiver.recv().await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                debug!(%peer, "connection closed by peer");
                return ReadEnd::Closed;
            }
            Err(error @ TransportError::FrameTooLarge { .. }) => {
                warn!(%peer, %error, "closing connection, oversize frame");
                return ReadEnd::Poisoned;
            }
            Err(error) => {
                debug!(%peer, %error, "connection lost");
                return ReadEnd::Poisoned;
            }
        };

        let frame = match Frame::from_bytes(bytes) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(%peer, %error, "closing connection, cannot decode frame");
                return ReadEnd::Poisoned;
            }
        };

        liveness.record_heartbeat();

        // Handle gossip before entering the gate: its merge may run a node death, which
        // write-locks a gate!
        if let Frame::Gossip { members, .. } = &frame {
            membership::on_gossip(endpoint, members);
            if !endpoint.delivers_from(peer) {
                debug!(%peer, "closing connection to a dead node incarnation");
                return ReadEnd::Poisoned;
            }
            continue;
        }

        let _guard = liveness.enter();
        if !endpoint.delivers_from(peer) {
            debug!(%peer, "closing connection to a dead node incarnation");
            return ReadEnd::Poisoned;
        }

        match frame {
            Frame::Message {
                target,
                reply_tags,
                payload,
            } => {
                if let Err(error) = endpoint
                    .registry()
                    .deliver(target, &payload, endpoint.codec())
                {
                    warn!(%peer, actor_id = %target, %error, "dead letter");
                    reply::on_undeliverable(endpoint, peer, &reply_tags);
                }
            }

            Frame::Multicast { targets, payload } => {
                if targets.is_empty() {
                    warn!(%peer, "skipping a multicast without targets");
                }
                for target in targets {
                    if let Err(error) =
                        endpoint
                            .registry()
                            .deliver(target, &payload, endpoint.codec())
                    {
                        warn!(%peer, actor_id = %target, %error, "dead letter");
                    }
                }
            }

            Frame::Watch { target, watcher } => watch::on_watch(endpoint, peer, target, watcher),

            Frame::Unwatch { target, watcher } => {
                watch::on_unwatch(endpoint, peer, target, watcher)
            }

            Frame::Terminated { target, watcher } => {
                watch::on_terminated(endpoint, peer, target, watcher)
            }

            Frame::Reply {
                nonce,
                recipient: _,
                payload,
            } => reply::on_reply(endpoint, peer, nonce, &payload),

            Frame::ReplyDropped {
                nonce,
                recipient: _,
            } => reply::on_reply_dropped(endpoint, peer, nonce),

            Frame::ReplyCancelled { nonce, stream: _ } => {
                reply::on_reply_cancelled(endpoint, peer, nonce)
            }

            Frame::Gossip { .. } => unreachable!("gossip is handled before the gate"),

            // Merged now, so this node's own decisions see it at once; forwarding waits for the
            // next membership tick, which fans out everything learned since the last one.
            Frame::Reachability { observations } => endpoint.merge_reachability(&observations),

            Frame::Registrations {
                version,
                chunk,
                chunks,
                groups,
            } => receptionist::on_registrations(
                endpoint,
                peer,
                &mut assembler,
                version,
                chunk,
                chunks,
                groups,
            ),

            Frame::Drained { data_streams } => return ReadEnd::Drained(data_streams),

            Frame::DrainAck => return ReadEnd::DrainAcknowledged,

            Frame::Handshake(_) | Frame::Refused { .. } => {
                warn!(%peer, "closing connection, unexpected establishment frame");
                return ReadEnd::Poisoned;
            }
        }
    }
}

/// The identity check comes first, so an unproven address reaches nothing, not even the
/// supersession. The cluster check precedes the Down check, so a node of another cluster never
/// meets this cluster's Down fences; both precede any admission.
fn admit_inbound(
    peer: NodeId,
    intent: HandshakeIntent,
    cluster: Option<ClusterId>,
    identity: Option<PeerIdentity>,
    endpoint: &'static EndpointInner,
) -> Admission {
    if endpoint.downed() {
        return Admission::Drop;
    }
    if !identity_binds(peer, identity) {
        warn!(%peer, "dropping connection, certificate does not cover the advertised address");
        return Admission::Drop;
    }
    if intent == HandshakeIntent::Probe {
        debug!(%peer, "answering probe");
        return Admission::Probe;
    }
    if let Some(this_cluster) = endpoint.effective_cluster()
        && let Some(cluster) = cluster
        && cluster != this_cluster
    {
        error!(%peer, %cluster, %this_cluster, "refusing connection, a member of another cluster");
        endpoint.sight_cluster(peer.addr(), Some(cluster));
        return Admission::Refuse(RefusalReason::ForeignCluster);
    }
    if intent != HandshakeIntent::Member && endpoint.membership().is_down(peer) {
        warn!(%peer, "refusing connection, dead node incarnation");
        return Admission::Refuse(RefusalReason::Down);
    }

    match intent {
        HandshakeIntent::Member => match endpoint.admit_member(peer) {
            MemberAdmission::Member(inbound) => {
                Admission::Admit(endpoint.track_liveness(peer), inbound)
            }

            MemberAdmission::Draining(inbound) => {
                debug!(%peer, "admitting a departed node to drain its connection");
                Admission::Admit(endpoint.track_liveness(peer), inbound)
            }

            MemberAdmission::Dead => {
                warn!(%peer, "refusing connection, dead node incarnation");
                Admission::Refuse(RefusalReason::Down)
            }

            MemberAdmission::Unknown => {
                debug!(%peer, "refusing connection, not (yet) a member");
                Admission::Refuse(RefusalReason::UnknownMember)
            }
        },

        HandshakeIntent::Join => {
            if !endpoint.formed() {
                debug!(%peer, "refusing join, this node is not a member of any cluster");
                return Admission::Refuse(RefusalReason::NoCluster);
            }

            let (admission, inbound) = endpoint
                .update(|membership, reachability| {
                    let mut departures = endpoint.departures();
                    let admission = membership.admit_join(peer, reachability);
                    let inbound = matches!(
                        admission,
                        JoinAdmission::Admitted { .. } | JoinAdmission::AlreadyUp
                    )
                    .then(|| endpoint.count_inbound(&mut departures, peer));
                    (admission, inbound)
                })
                .result;
            match (admission, inbound) {
                (JoinAdmission::Admitted { superseded }, Some(inbound)) => {
                    if let Some((previous, outcome)) = superseded {
                        warn!(%peer, %previous, "node death, address superseded by a joining incarnation");
                        endpoint.death_consequences(previous, outcome);
                    }
                    debug!(%peer, "member joined");
                    endpoint.unrefuse(peer.addr());
                    endpoint.receptionist().origin_up(peer);
                    endpoint.push_gossip();
                    Admission::Admit(endpoint.track_liveness(peer), inbound)
                }

                (JoinAdmission::AlreadyUp, Some(inbound)) => {
                    Admission::Admit(endpoint.track_liveness(peer), inbound)
                }

                (JoinAdmission::Admitted { .. } | JoinAdmission::AlreadyUp, None) => {
                    unreachable!("an admitted join is counted as an inbound connection")
                }

                (JoinAdmission::Dead, _) => {
                    warn!(%peer, "refusing join, dead node incarnation");
                    Admission::Refuse(RefusalReason::Down)
                }

                (JoinAdmission::Held { unreachable }, _) => {
                    info!(%peer, %unreachable, "holding back join, a member is unreachable");
                    Admission::Refuse(RefusalReason::JoinsHeld)
                }

                (JoinAdmission::Stale { outcome }, _) => {
                    warn!(%peer, "refusing join, stale incarnation for its address");
                    endpoint.death_consequences(peer, outcome);
                    Admission::Refuse(RefusalReason::Down)
                }

                (JoinAdmission::SupersedesThisNode, _) => {
                    warn!(%peer, "a younger incarnation joins at this node's address");
                    endpoint.self_down();
                    Admission::Drop
                }
            }
        }

        HandshakeIntent::Probe => Admission::Probe,
    }
}

/// No Up check on the dial side; whether this node may connect as a member is the responder's
/// decision.
fn admit_outbound(
    peer: NodeId,
    cluster: Option<ClusterId>,
    dialed: SocketAddr,
    identity: Option<PeerIdentity>,
    endpoint: &EndpointInner,
) -> Result<PeerLiveness, ConnectError> {
    if endpoint.downed() {
        return Err(ConnectError::SelfDowned);
    }
    check_dialed(peer, cluster, dialed, identity, endpoint)?;

    Ok(endpoint.track_liveness(peer))
}

fn check_dialed(
    peer: NodeId,
    cluster: Option<ClusterId>,
    dialed: SocketAddr,
    identity: Option<PeerIdentity>,
    endpoint: &EndpointInner,
) -> Result<(), ConnectError> {
    if peer.addr() != dialed {
        return Err(ConnectError::AddressMismatch {
            advertised: peer.addr(),
            dialed,
        });
    }
    if !identity_binds(peer, identity) {
        return Err(ConnectError::Identity);
    }
    if let Some(this_cluster) = endpoint.effective_cluster()
        && let Some(cluster) = cluster
        && cluster != this_cluster
    {
        error!(%peer, %cluster, %this_cluster, "refusing connection, a member of another cluster");
        endpoint.sight_cluster(peer.addr(), Some(cluster));
        return Err(ConnectError::ForeignCluster(cluster));
    }
    if endpoint.membership().is_down(peer) {
        return Err(ConnectError::Dead);
    }

    Ok(())
}

fn identity_binds(peer: NodeId, identity: Option<PeerIdentity>) -> bool {
    identity.is_none_or(|identity| {
        let addrs = identity
            .uris
            .iter()
            .map(String::as_str)
            .filter_map(tellus_uri_addr)
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            identity.ip_addresses.contains(&peer.addr().ip())
        } else {
            addrs.contains(&peer.addr())
        }
    })
}

fn tellus_uri_addr(uri: &str) -> Option<SocketAddr> {
    let (scheme, addr) = uri.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("tellus") {
        return None;
    }

    addr.parse()
        .inspect_err(|error| warn!(uri, %error, "ignoring tellus URI, not an IP address and port"))
        .ok()
}

async fn run_join<T>(
    transport: Arc<T>,
    request: JoinRequest,
    admission_possible: Arc<AtomicBool>,
    endpoint: &'static EndpointInner,
) where
    T: Transport,
{
    let JoinRequest {
        addr,
        mut result_tx,
    } = request;
    if result_tx.is_closed() {
        debug!(seed_addr = %addr, "dropping join request, its caller is gone");
        return;
    }

    if !endpoint.enter_join() {
        let _ = result_tx.send(Err(ConnectError::SelfDowned));
        return;
    }
    let result = try_join(
        transport.as_ref(),
        addr,
        &mut result_tx,
        &admission_possible,
        endpoint,
    )
    .await;
    let result = endpoint.finish_join(addr, result);
    let _ = result_tx.send(result);
}

/// Cancellable until the Join handshake is sent; afterward the peer may already count this node,
/// so the attempt must finish.
async fn try_join<T>(
    transport: &T,
    addr: SocketAddr,
    result_tx: &mut oneshot::Sender<Result<(), ConnectError>>,
    admission_possible: &AtomicBool,
    endpoint: &'static EndpointInner,
) -> Result<(), ConnectError>
where
    T: Transport,
{
    let connected = select! {
        connected = transport.connect(addr, endpoint.config().max_frame_size.get()) => connected?,

        () = result_tx.closed() => return Err(ConnectError::Abandoned),
    };

    admission_possible.store(true, Ordering::Release);
    complete_join::<T>(connected, addr, endpoint)
        .await
        .inspect_err(|error| {
            if error.leaves_join_ambiguous() {
                endpoint.record_ambiguous_join(addr);
            }
        })
}

async fn complete_join<T>(
    connected: ConnectedControl<T::Connection>,
    addr: SocketAddr,
    endpoint: &'static EndpointInner,
) -> Result<(), ConnectError>
where
    T: Transport,
{
    let ConnectedControl {
        connection,
        mut control_tx,
        control_rx: mut frame_receiver,
    } = connected;

    let Greeting {
        node: peer,
        cluster,
        ..
    } = exchange_handshakes(
        &mut control_tx,
        &mut frame_receiver,
        handshake(endpoint, HandshakeIntent::Join),
        endpoint.config().heartbeat_interval,
    )
    .await?;

    check_dialed(peer, cluster, addr, connection.peer_identity(), endpoint).inspect_err(
        |error| {
            if matches!(error, ConnectError::Identity) {
                warn!(%peer, "refusing join, certificate does not cover the advertised address");
            }
        },
    )?;
    let cluster = cluster.ok_or(ConnectError::NoClusterId)?;

    // The answer proves a cluster admitted this node, whether or not its snapshot arrives.
    endpoint.pin_cluster(addr, cluster);

    let max_members = MAX_SNAPSHOT_MEMBERS
        .saturating_add(membership::max_fences(endpoint.config().down_watermarks));
    let members = timeout(endpoint.config().heartbeat_interval, async {
        let mut members = Vec::new();
        loop {
            let frame = frame_receiver
                .recv()
                .await?
                .ok_or(ConnectError::Closed)
                .and_then(|bytes| Frame::from_bytes(bytes).map_err(ConnectError::from))?;

            match frame {
                Frame::Gossip {
                    members: page,
                    more,
                } => {
                    members.extend(page);
                    if members.len() > max_members {
                        return Err(ConnectError::SnapshotTooLarge(max_members));
                    }
                    if !more {
                        return Ok(members);
                    }
                }

                Frame::Refused { reason } => return Err(ConnectError::Refused(reason)),

                _ => return Err(ConnectError::NotAHandshake),
            }
        }
    })
    .await
    .map_err(|_| ConnectError::HandshakeTimeout)??;

    // Applied as one snapshot: a connection lost mid stream must leave no fragment behind.
    endpoint.commit_cluster(cluster);
    membership::on_gossip(endpoint, &members);
    Ok(())
}

async fn run_probe<T>(transport: Arc<T>, request: ProbeRequest, endpoint: &'static EndpointInner)
where
    T: Transport,
{
    let ProbeRequest {
        addr,
        mut result_tx,
    } = request;
    let result = probe(transport.as_ref(), addr, &mut result_tx, endpoint).await;
    let _ = result_tx.send(result);
}

/// Returning drops the connection, which is the close the answering side waits for.
async fn probe<T>(
    transport: &T,
    addr: SocketAddr,
    result_tx: &mut oneshot::Sender<Result<Option<ClusterId>, ConnectError>>,
    endpoint: &'static EndpointInner,
) -> Result<Option<ClusterId>, ConnectError>
where
    T: Transport,
{
    let connected = select! {
        connected = transport.connect(addr, endpoint.config().max_frame_size.get()) => connected?,

        () = result_tx.closed() => return Err(ConnectError::Abandoned),
    };
    let ConnectedControl {
        connection,
        mut control_tx,
        control_rx: mut frame_receiver,
    } = connected;

    let Greeting {
        node: peer,
        cluster,
        ..
    } = exchange_handshakes(
        &mut control_tx,
        &mut frame_receiver,
        handshake(endpoint, HandshakeIntent::Probe),
        endpoint.config().heartbeat_interval,
    )
    .await?;
    if peer.addr() != addr {
        return Err(ConnectError::AddressMismatch {
            advertised: peer.addr(),
            dialed: addr,
        });
    }
    if !identity_binds(peer, connection.peer_identity()) {
        return Err(ConnectError::Identity);
    }

    Ok(cluster)
}

/// Carries the effective cluster ID, so a pinned joiner is refused by any other cluster.
fn handshake(endpoint: &EndpointInner, intent: HandshakeIntent) -> HandshakeV1 {
    HandshakeV1 {
        node: endpoint.node(),
        intent,
        cluster: endpoint.effective_cluster(),
    }
}

async fn send_handshake<S>(
    frame_sender: &mut S,
    handshake: HandshakeV1,
) -> Result<(), TransportError>
where
    S: FrameSender,
{
    send_frame(frame_sender, &Frame::Handshake(Handshake::new(handshake))).await
}

/// Returning drops the connection, which discards the answer before the transport flushed it;
/// the peer's own close proves it read the answer.
async fn answer_and_close<S, R>(
    frame_sender: &mut S,
    frame_receiver: &mut R,
    answer: Result<Vec<u8>, postcard::Error>,
) where
    R: FrameReceiver,
    S: FrameSender,
{
    let sent = match answer {
        Ok(answer) => frame_sender.send(&answer).await,
        Err(error) => Err(TransportError::other(error)),
    };

    match sent {
        Ok(()) => {
            let _ = frame_receiver.recv().await;
        }

        Err(error) => debug!(%error, "cannot answer before closing"),
    }
}

async fn send_frame<S>(frame_sender: &mut S, frame: &Frame<'_>) -> Result<(), TransportError>
where
    S: FrameSender,
{
    let bytes = frame
        .encode_into(Vec::new())
        .map_err(TransportError::other)?;
    frame_sender.send(&bytes).await
}

/// The envelope is negotiated before the body is read, since only the negotiated version says
/// which schema the body has.
async fn recv_handshake<R>(frame_receiver: &mut R) -> Result<Greeting, ConnectError>
where
    R: FrameReceiver,
{
    let bytes = frame_receiver.recv().await?.ok_or(ConnectError::Closed)?;

    let Some(envelope) = HandshakeEnvelope::decode(bytes)? else {
        return match Frame::from_bytes(bytes)? {
            Frame::Refused { reason } => Err(ConnectError::Refused(reason)),
            _ => Err(ConnectError::NotAHandshake),
        };
    };
    let version = HandshakeEnvelope::this_build().negotiate(envelope)?;
    let HandshakeV1 {
        node,
        intent,
        cluster,
    } = HandshakeV1::decode(bytes)?;
    Ok(Greeting {
        version,
        node,
        intent,
        cluster,
    })
}

async fn drain_dead_letters(outbound_rx: Receiver<Frame<'static>>, addr: SocketAddr) {
    while let Ok(frame) = outbound_rx.recv_async().await {
        dead_letter(frame, addr, "node unreachable");
    }
}

fn dead_letter(frame: Frame<'static>, addr: SocketAddr, reason: &'static str) {
    discard_reply_tags(&frame);

    for target in delivered_targets(&frame) {
        #[cfg(test)]
        crate::sync::lock(&DEAD_LETTERS).insert(*target);
        warn!(actor_id = %target, peer_addr = %addr, reason, "dead letter");
    }
}

fn oversize_dead_letter(frame: &Frame<'_>, addr: SocketAddr) {
    let targets = delivered_targets(frame);
    if targets.is_empty() {
        warn!(peer_addr = %addr, "dropping a frame exceeding the maximum frame size");
    }
    for target in targets {
        warn!(actor_id = %target, peer_addr = %addr, "dead letter, frame exceeds the maximum frame size");
    }
}

/// The actors a frame carries a message to, empty for every other frame.
fn delivered_targets<'a>(frame: &'a Frame<'_>) -> &'a [ActorId] {
    match frame {
        Frame::Message { target, .. } => slice::from_ref(target),

        Frame::Multicast { targets, .. } => targets,

        _ => &[],
    }
}

/// A request dropped here never reaches its peer, so its asks resolve now, not on timeout.
fn discard_reply_tags(frame: &Frame<'_>) {
    if let Frame::Message { reply_tags, .. } = frame {
        reply::discard_replies(reply_tags);
    }
}

/// Aborting a sibling writer drops this guard, giving locally lost frames the same cleanup as a
/// send error.
struct InFlightFrame {
    frame: Option<Frame<'static>>,
    addr: SocketAddr,
}

impl InFlightFrame {
    fn new(frame: Frame<'static>, addr: SocketAddr) -> Self {
        Self {
            frame: Some(frame),
            addr,
        }
    }

    fn frame(&self) -> &Frame<'static> {
        self.frame.as_ref().expect("in-flight frame is armed")
    }

    fn discard_oversize(&mut self) {
        let frame = self.frame.take().expect("in-flight frame is armed");
        oversize_dead_letter(&frame, self.addr);
        discard_reply_tags(&frame);
    }

    fn discard_unencodable(&mut self, error: &postcard::Error) {
        let frame = self.frame.take().expect("in-flight frame is armed");
        warn!(peer_addr = %self.addr, %error, "dead letter, frame cannot be encoded");
        discard_reply_tags(&frame);
    }

    fn disarm(&mut self) {
        self.frame = None;
    }
}

impl Drop for InFlightFrame {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            dead_letter(frame, self.addr, "frame never reached the transport");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Backoff, Incoming, MailboxCapacity,
        actor_ref::SelfRef,
        cluster::{
            discovery::{KeyKind, WireKey},
            endpoint::{EndpointConfig, EndpointInner, FormError, MemberAdmission},
            frame::{
                Frame, Handshake, HandshakeEnvelope, HandshakeError, HandshakeIntent, HandshakeV1,
                RefusalReason,
                synthetic::{self, Body, HandshakeV2},
            },
            membership::{self, JoinRound, Member, MemberState, Membership},
            node::{ClusterId, NodeId},
            peer::{
                Admission, ConnectError, DialRequest, JoinRequest, LeaveSignals,
                MAX_SNAPSHOT_MEMBERS, ReadEnd, WriterEnd, admit_inbound, check_dialed, dead_letter,
                dial_loop, exchange_handshakes, identity_binds, join_loop, probe_loop, read_frames,
                recv_handshake, run_accepted, write_frames, write_streams,
            },
            receptionist::{self, WireGroup},
            state::ForeignCluster,
            transport::{
                ConnectedControl, Connection, FrameReceiver, FrameSender, PeerIdentity, Transport,
                TransportError,
            },
        },
        mailbox::{Mailbox, Queued},
        quota::Quota,
        sync::lock,
        watch::WatcherRegistration,
    };
    use std::{
        borrow::Cow,
        collections::{BTreeSet, HashSet, VecDeque},
        future::pending,
        io, iter,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        sync::{Arc, Mutex, atomic::AtomicBool, mpsc},
        time::Duration,
    };
    use tokio::{
        sync::{
            Notify,
            mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
            oneshot,
        },
        task::{self, JoinHandle},
        time::{sleep, timeout},
    };

    static NOT_LEAVING: AtomicBool = AtomicBool::new(false);
    static LEAVING: AtomicBool = AtomicBool::new(true);

    struct FakeReceiver {
        frames: VecDeque<Vec<u8>>,
        current: Vec<u8>,
    }

    impl FakeReceiver {
        fn new(frames: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                current: Vec::new(),
            }
        }
    }

    impl FrameReceiver for FakeReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            match self.frames.pop_front() {
                Some(frame) => {
                    self.current = frame;
                    Ok(Some(&self.current))
                }

                None => Ok(None),
            }
        }
    }

    struct FailingReceiver;

    impl FrameReceiver for FailingReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            Err(TransportError::FrameTooLarge { len: 2, max: 1 })
        }
    }

    #[tokio::test]
    async fn only_a_finished_stream_ends_quietly() {
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let peer = peer();
        let liveness = endpoint.track_liveness(peer);

        let finished = read_frames(FakeReceiver::new([]), peer, liveness.clone(), endpoint).await;
        assert!(matches!(finished, ReadEnd::Closed));

        let failed = read_frames(FailingReceiver, peer, liveness, endpoint).await;
        assert!(matches!(failed, ReadEnd::Poisoned));
    }

    /// A multicast is delivered to each target in turn: a target which cannot take it is a dead
    /// letter of its own and spares the targets after it, and one without targets is skipped
    /// rather than closing the connection.
    #[tokio::test]
    async fn a_multicast_is_delivered_to_each_target() {
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let peer = peer();
        let (first, first_mailbox) =
            SelfRef::<u64>::new(ActorId::new(), MailboxCapacity::Unbounded);
        let (second, second_mailbox) =
            SelfRef::<u64>::new(ActorId::new(), MailboxCapacity::Unbounded);
        endpoint.registry().bind(first.actor_ref());
        endpoint.registry().bind(second.actor_ref());
        let payload = endpoint.codec().encode(&42_u64).expect("encodes");
        let frames = [
            Frame::Multicast {
                targets: vec![
                    first.actor_ref().actor_id(),
                    ActorId::new(),
                    second.actor_ref().actor_id(),
                ],
                payload: Cow::Borrowed(&payload),
            },
            Frame::Multicast {
                targets: Vec::new(),
                payload: Cow::Borrowed(&payload),
            },
            Frame::Multicast {
                targets: vec![second.actor_ref().actor_id()],
                payload: Cow::Borrowed(&payload),
            },
        ]
        .map(|frame| frame.encode_into(Vec::new()).expect("frame encodes"));

        let end = read_frames(
            FakeReceiver::new(frames),
            peer,
            endpoint.track_liveness(peer),
            endpoint,
        )
        .await;

        assert!(matches!(end, ReadEnd::Closed));
        let received = |mailbox: Mailbox<u64>| {
            mailbox
                .split()
                .0
                .drain()
                .filter_map(|queued| match queued {
                    Queued::Incoming(Incoming::Message(message)) => Some(message),
                    Queued::Incoming(Incoming::Terminated(_)) | Queued::Timer(_) => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(received(first_mailbox), [42]);
        assert_eq!(received(second_mailbox), [42, 42]);
    }

    /// A multicast which never reaches the transport is a dead letter for every target it names.
    #[test]
    fn a_dead_lettered_multicast_names_every_target() {
        let targets = [ActorId::new(), ActorId::new()];

        dead_letter(
            Frame::Multicast {
                targets: targets.to_vec(),
                payload: Cow::Borrowed(&[]),
            },
            "127.0.0.1:1234".parse().expect("valid address"),
            "node unreachable",
        );

        for target in targets {
            assert!(crate::sync::lock(&super::DEAD_LETTERS).remove(&target));
        }
    }

    /// Receives nothing, ever: the wire shape of a peer which connects and stays silent.
    struct SilentReceiver;

    impl FrameReceiver for SilentReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            std::future::pending().await
        }
    }

    struct RecordingSender(mpsc::Sender<Vec<u8>>);

    impl FrameSender for RecordingSender {
        async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            let _ = self.0.send(frame.to_vec());
            Ok(())
        }

        async fn send_batch(&mut self, frames: &[Vec<u8>]) -> Result<(), TransportError> {
            for frame in frames {
                self.send(frame).await?;
            }
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), TransportError> {
            let _ = self.0.send(Vec::new());
            Ok(())
        }
    }

    /// Records what it sends, an empty frame for a finished stream, and finishes only if told so,
    /// else fails as a stream the peer stopped.
    struct DrainingSender {
        sent_tx: mpsc::Sender<Vec<u8>>,
        finishes: bool,
    }

    impl DrainingSender {
        fn new(sent_tx: &mpsc::Sender<Vec<u8>>, finishes: bool) -> Self {
            Self {
                sent_tx: sent_tx.clone(),
                finishes,
            }
        }
    }

    impl FrameSender for DrainingSender {
        async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            let _ = self.sent_tx.send(frame.to_vec());
            Ok(())
        }

        async fn send_batch(&mut self, frames: &[Vec<u8>]) -> Result<(), TransportError> {
            for frame in frames {
                self.send(frame).await?;
            }
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), TransportError> {
            if !self.finishes {
                return Err(TransportError::other("peer stopped the stream"));
            }
            let _ = self.sent_tx.send(Vec::new());
            Ok(())
        }
    }

    enum CoordinatedSender {
        Block(Arc<Notify>),
        FailAfter(Arc<Notify>),
    }

    impl FrameSender for CoordinatedSender {
        async fn send(&mut self, _: &[u8]) -> Result<(), TransportError> {
            match self {
                CoordinatedSender::Block(started) => {
                    started.notify_one();
                    pending().await
                }
                CoordinatedSender::FailAfter(started) => {
                    started.notified().await;
                    Err(TransportError::other("connection lost"))
                }
            }
        }

        async fn send_batch(&mut self, frames: &[Vec<u8>]) -> Result<(), TransportError> {
            for frame in frames {
                self.send(frame).await?;
            }
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    fn peer() -> NodeId {
        NodeId::new("127.0.0.1:1234".parse().expect("valid address"))
    }

    fn leave_signals(leaving: &'static AtomicBool) -> (oneshot::Sender<()>, LeaveSignals) {
        let (drain_ack_tx, drain_ack_rx) = oneshot::channel();
        (
            drain_ack_tx,
            LeaveSignals {
                leaving,
                drain_ack_rx,
            },
        )
    }

    fn member_handshake(node: NodeId) -> HandshakeV1 {
        HandshakeV1 {
            node,
            intent: HandshakeIntent::Member,
            cluster: None,
        }
    }

    fn identity(ip_addresses: impl IntoIterator<Item = IpAddr>) -> PeerIdentity {
        uri_identity(ip_addresses, [])
    }

    fn uri_identity<'a>(
        ip_addresses: impl IntoIterator<Item = IpAddr>,
        uris: impl IntoIterator<Item = &'a str>,
    ) -> PeerIdentity {
        PeerIdentity {
            dns_names: Vec::new(),
            ip_addresses: ip_addresses.into_iter().collect(),
            uris: uris.into_iter().map(str::to_string).collect(),
        }
    }

    /// A transport which proves no identity binds nothing: without mutual TLS there is no
    /// certificate to bind the advertised address against, and admission falls to the other
    /// checks.
    #[test]
    fn an_unproven_identity_binds_every_address() {
        assert!(identity_binds(peer(), None));
    }

    /// Without a `tellus://` URI the advertised IP must appear among the certificate's IP
    /// addresses, which stops a peer holding a valid certificate from claiming another host.
    #[test]
    fn an_identity_binds_only_its_own_addresses() {
        let peer = peer();
        let own = peer.addr().ip();
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        assert!(identity_binds(peer, Some(identity([own]))));
        assert!(identity_binds(peer, Some(identity([other, own]))));

        assert!(!identity_binds(peer, Some(identity([other]))));
        assert!(!identity_binds(peer, Some(identity([]))));
    }

    /// DNS names never bind: a certificate naming the peer in DNS alone proves nothing about the
    /// address it advertises, since that is what the lane connects back to.
    #[test]
    fn dns_names_do_not_bind_an_address() {
        let identity = PeerIdentity {
            dns_names: vec!["node.example.com".to_string()],
            ip_addresses: Vec::new(),
            uris: Vec::new(),
        };

        assert!(!identity_binds(peer(), Some(identity)));
    }

    /// A `tellus://` URI binds the exact advertised address, IP and port, so it suffices without
    /// any IP address SAN.
    #[test]
    fn a_tellus_uri_binds_its_exact_address() {
        let peer = peer();

        assert!(identity_binds(
            peer,
            Some(uri_identity([], ["tellus://127.0.0.1:1234"]))
        ));
        assert!(identity_binds(
            peer,
            Some(uri_identity(
                [],
                ["tellus://127.0.0.1:4321", "tellus://127.0.0.1:1234"]
            ))
        ));
    }

    /// A `tellus://` URI for another port refuses the peer even if an IP address SAN matches its
    /// IP: the URI binds one address, so a second node on the same host cannot claim it.
    #[test]
    fn a_tellus_uri_for_another_port_does_not_bind() {
        let peer = peer();
        let own = peer.addr().ip();

        assert!(!identity_binds(
            peer,
            Some(uri_identity([own], ["tellus://127.0.0.1:4321"]))
        ));
        assert!(!identity_binds(
            peer,
            Some(uri_identity([], ["tellus://10.0.0.1:1234"]))
        ));
    }

    /// The scheme is matched case-insensitively and an IPv6 address is written in brackets.
    #[test]
    fn a_tellus_uri_binds_an_ipv6_address() {
        let peer = NodeId::new("[::1]:1234".parse().expect("valid address"));

        assert!(identity_binds(
            peer,
            Some(uri_identity([], ["TELLUS://[::1]:1234"]))
        ));
        assert!(!identity_binds(
            peer,
            Some(uri_identity([], ["tellus://[::1]:4321"]))
        ));
    }

    /// URIs of other schemes, e.g. SPIFFE IDs, neither bind nor disable the IP address SANs.
    #[test]
    fn a_non_tellus_uri_is_ignored() {
        let peer = peer();
        let own = peer.addr().ip();
        let spiffe = "spiffe://example.org/ns/default/sa/node";

        assert!(identity_binds(peer, Some(uri_identity([own], [spiffe]))));
        assert!(!identity_binds(peer, Some(uri_identity([], [spiffe]))));
    }

    /// A `tellus://` URI which is not an IP address and port binds nothing and leaves the IP
    /// address SANs in charge.
    #[test]
    fn an_unparsable_tellus_uri_is_ignored() {
        let peer = peer();
        let own = peer.addr().ip();

        assert!(identity_binds(
            peer,
            Some(uri_identity([own], ["tellus://node.example.com:1234"]))
        ));
        assert!(!identity_binds(
            peer,
            Some(uri_identity([], ["tellus://127.0.0.1"]))
        ));
    }

    /// A transport failure, a peer closing early, a peer staying silent, a refusal as an unknown
    /// member (gossip may not have converged yet), a held join (the unreachable member may be
    /// downed or come back) or a dead incarnation (its successor may come up at that address) is
    /// worth another attempt, while a peer
    /// which does not speak this protocol never becomes one and a Down refusal is final.
    #[test]
    fn only_transient_failures_are_retried() {
        assert!(ConnectError::Transport(TransportError::other("boom")).is_retryable());
        assert!(ConnectError::Closed.is_retryable());
        assert!(ConnectError::DataStreams(1).is_retryable());
        assert!(ConnectError::HandshakeTimeout.is_retryable());
        assert!(ConnectError::Refused(RefusalReason::UnknownMember).is_retryable());
        assert!(ConnectError::Refused(RefusalReason::NoCluster).is_retryable());
        assert!(ConnectError::Refused(RefusalReason::JoinsHeld).is_retryable());
        assert!(ConnectError::Dead.is_retryable());

        assert!(
            !ConnectError::Transport(TransportError::FrameTooLarge { len: 2, max: 1 })
                .is_retryable()
        );

        assert!(!ConnectError::NotAHandshake.is_retryable());
        assert!(!ConnectError::Decode(postcard::Error::DeserializeUnexpectedEnd).is_retryable());
        assert!(!ConnectError::Handshake(HandshakeError::Magic(0)).is_retryable());
        assert!(
            !ConnectError::Handshake(HandshakeError::Incompatible {
                ours: 1..=1,
                theirs: 2..=2,
            })
            .is_retryable()
        );
        assert!(!ConnectError::Refused(RefusalReason::Down).is_retryable());
        assert!(!ConnectError::Refused(RefusalReason::ForeignCluster).is_retryable());
        assert!(!ConnectError::ForeignCluster(ClusterId::new()).is_retryable());
        assert!(!ConnectError::NoClusterId.is_retryable());
    }

    #[test]
    fn a_member_address_is_never_refused() {
        for error in [
            ConnectError::NotAHandshake,
            ConnectError::Identity,
            ConnectError::AddressMismatch {
                advertised: "127.0.0.1:1".parse().expect("valid address"),
                dialed: "127.0.0.1:2".parse().expect("valid address"),
            },
            ConnectError::Handshake(HandshakeError::Magic(0)),
            ConnectError::Transport(TransportError::FrameTooLarge { len: 2, max: 1 }),
        ] {
            assert!(error.refuses_address(false), "{error}");
            assert!(!error.refuses_address(true), "{error}");
        }

        assert!(!ConnectError::Dead.refuses_address(false));
        assert!(!ConnectError::Closed.refuses_address(false));
    }

    /// The dial side's handshake wait is bounded like the accept side's: a peer which connects
    /// and never speaks times out into the retry path instead of pinning the lane's dial forever.
    #[tokio::test(start_paused = true)]
    async fn a_silent_peer_times_out_the_handshake() {
        let (sent_tx, _sent_rx) = mpsc::channel();
        let mut sender = RecordingSender(sent_tx);
        let mut receiver = SilentReceiver;

        let exchanged = exchange_handshakes(
            &mut sender,
            &mut receiver,
            member_handshake(peer()),
            Duration::from_secs(1),
        )
        .await;

        assert!(matches!(exchanged, Err(ConnectError::HandshakeTimeout)));
    }

    /// A peer answering within the deadline is handshaked normally, so the bound costs the happy
    /// path nothing but the timer.
    #[tokio::test]
    async fn handshakes_exchange_within_the_deadline() {
        let announced = peer();
        let bytes = Frame::Handshake(Handshake::new(member_handshake(announced)))
            .encode_into(Vec::new())
            .expect("handshake encodes");
        let (sent_tx, sent_rx) = mpsc::channel();
        let mut sender = RecordingSender(sent_tx);
        let mut receiver = FakeReceiver::new([bytes]);

        let greeting = exchange_handshakes(
            &mut sender,
            &mut receiver,
            member_handshake(peer()),
            Duration::from_secs(1),
        )
        .await
        .expect("handshakes are exchanged");

        assert_eq!(greeting.version, 1);
        assert_eq!(greeting.node, announced);
        assert_eq!(greeting.intent, HandshakeIntent::Member);
        assert_eq!(greeting.cluster, None);
        assert_eq!(sent_rx.try_iter().count(), 1);
    }

    /// An oversize frame slipping past the send-time check dies in the writer, the last check on
    /// the path: it never reaches the transport, whose receiver's refusal would kill the
    /// connection, and the frames behind it still ride the stream.
    #[tokio::test]
    async fn an_oversize_frame_dies_in_the_writer() {
        let (outbound_tx, outbound_rx) = flume::unbounded();
        outbound_tx
            .send(Frame::Message {
                target: ActorId::new(),
                reply_tags: Vec::new(),
                payload: Cow::Owned(vec![0; 64]),
            })
            .expect("the queue is open");
        outbound_tx
            .send(Frame::Gossip {
                members: Vec::new(),
                more: false,
            })
            .expect("the queue is open");
        drop(outbound_tx);

        let (sent_tx, sent_rx) = mpsc::channel();
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let end = write_frames(
            RecordingSender(sent_tx),
            outbound_rx,
            Quota::unbounded(),
            addr,
            32,
            None,
        )
        .await;

        assert!(end.is_ok());
        let sent = sent_rx.try_iter().collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0],
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .encode_into(Vec::new())
            .expect("gossip encodes")
        );
    }

    /// Leaving, every data stream is finished before the control stream carries the drain
    /// marker, its last frame, which names the number of data streams. Only the peer's answer to
    /// the marker makes the lane acknowledged: without one it is not, and a data stream which
    /// cannot be finished means no marker is sent at all.
    #[tokio::test]
    async fn a_lane_drained_for_a_leave_is_acknowledged_only_by_the_peer() {
        let gossip = || Frame::Gossip {
            members: Vec::new(),
            more: false,
        };
        let drained = Frame::Drained { data_streams: 2 }
            .encode_into(Vec::new())
            .expect("frame encodes");

        for (answered, finishes, acknowledged) in [
            (true, true, true),
            (false, true, false),
            (true, false, false),
        ] {
            let (control_tx, control_rx) = flume::unbounded();
            control_tx.send(gossip()).expect("the queue is open");
            let (data_tx, data_rx) = flume::unbounded::<Frame<'static>>();
            let (other_tx, other_rx) = flume::unbounded::<Frame<'static>>();
            drop((control_tx, data_tx, other_tx));
            let receivers = [control_rx, data_rx, other_rx];
            let (drain_ack_tx, leave) = leave_signals(&LEAVING);
            if answered {
                drain_ack_tx.send(()).expect("the writers await the answer");
            } else {
                drop(drain_ack_tx);
            }

            let (sent_tx, sent_rx) = mpsc::channel();
            let end = timeout(
                Duration::from_secs(5),
                write_streams(
                    vec![
                        DrainingSender::new(&sent_tx, true),
                        DrainingSender::new(&sent_tx, true),
                        DrainingSender::new(&sent_tx, finishes),
                    ],
                    &receivers,
                    &Quota::unbounded(),
                    "127.0.0.1:1234".parse().expect("valid address"),
                    1024,
                    tokio::sync::watch::channel(None).1,
                    leave,
                ),
            )
            .await
            .expect("the writers end");

            if acknowledged {
                assert!(matches!(end, WriterEnd::Acknowledged));
            } else {
                assert!(matches!(end, WriterEnd::Unacknowledged));
            }
            let sent = sent_rx.try_iter().collect::<Vec<_>>();
            let finished = sent.iter().filter(|frame| frame.is_empty()).count();
            if finishes {
                assert_eq!(finished, 2, "only the data streams are finished: {sent:?}");
                assert_eq!(sent.last(), Some(&drained), "the marker comes last");
                assert_eq!(sent.len(), 4);
            } else {
                assert_eq!(finished, 1);
                assert!(!sent.contains(&drained), "no marker after a failed stream");
            }
        }
    }

    /// The first failed stream aborts its siblings. A sibling already suspended in transport send
    /// has dequeued its frame, so its in-flight guard must still turn that frame into a dead
    /// letter.
    #[tokio::test]
    async fn aborting_a_sibling_writer_dead_letters_its_in_flight_frame() {
        let target = ActorId::new();
        crate::sync::lock(&super::DEAD_LETTERS).remove(&target);
        let (blocked_tx, blocked_rx) = flume::unbounded();
        blocked_tx
            .send(Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[]),
            })
            .expect("blocked queue is open");
        let (failed_tx, failed_rx) = flume::unbounded();
        failed_tx
            .send(Frame::Gossip {
                members: Vec::new(),
                more: false,
            })
            .expect("failed queue is open");
        let started = Arc::new(Notify::new());
        let receivers = [blocked_rx, failed_rx];

        let end = write_streams(
            vec![
                CoordinatedSender::Block(started.clone()),
                CoordinatedSender::FailAfter(started),
            ],
            &receivers,
            &Quota::unbounded(),
            "127.0.0.1:1234".parse().expect("valid address"),
            1024,
            tokio::sync::watch::channel(None).1,
            leave_signals(&NOT_LEAVING).1,
        )
        .await;
        assert!(matches!(end, WriterEnd::ConnectionLost));
        assert!(crate::sync::lock(&super::DEAD_LETTERS).remove(&target));
    }

    /// The slot holds the newest snapshot only: three offered to a stalled writer are written as
    /// one, contiguously and before the queue, and a written snapshot is not written again.
    #[tokio::test]
    async fn a_slot_coalesces_snapshots_for_a_slow_writer() {
        let (control_tx, control_rx) = flume::unbounded();
        control_tx
            .send(Frame::Gossip {
                members: Vec::new(),
                more: false,
            })
            .expect("queue is open");
        let snapshot_of = |version: u64| {
            let groups = (0..3)
                .map(|index| WireGroup {
                    key: WireKey::new::<u64>(KeyKind::Discovery, format!("name-{version}-{index}")),
                    ids: vec![ActorId::new()],
                })
                .collect();
            Arc::new(receptionist::snapshot(version, groups, 96))
        };
        let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(None);
        for version in 1..=3 {
            snapshot_tx.send_replace(Some(snapshot_of(version)));
        }
        let (recorded_tx, mut recorded_rx) = unbounded_channel();

        let writer = task::spawn(write_frames(
            ChannelSender(recorded_tx),
            control_rx,
            Quota::unbounded(),
            "127.0.0.1:1234".parse().expect("valid address"),
            1024,
            Some(snapshot_rx),
        ));
        let latest = snapshot_of(3);
        assert!(latest.frames.len() > 1, "the snapshot must be chunked");
        for expected in &latest.frames {
            let bytes = recorded_rx.recv().await.expect("a chunk is written");
            let written = Frame::from_bytes(&bytes).expect("frame decodes");
            let Frame::Registrations {
                chunk: expected, ..
            } = expected
            else {
                panic!("not a registrations frame");
            };
            assert!(matches!(
                written,
                Frame::Registrations { version: 3, chunk, .. } if chunk == *expected
            ));
        }
        let bytes = recorded_rx.recv().await.expect("the queued frame follows");
        assert!(matches!(
            Frame::from_bytes(&bytes).expect("frame decodes"),
            Frame::Gossip { .. }
        ));

        snapshot_tx.send_replace(Some(snapshot_of(4)));
        let bytes = recorded_rx
            .recv()
            .await
            .expect("the next snapshot is written");
        assert!(matches!(
            Frame::from_bytes(&bytes).expect("frame decodes"),
            Frame::Registrations {
                version: 4,
                chunk: 0,
                ..
            }
        ));
        for _ in 1..snapshot_of(4).frames.len() {
            recorded_rx
                .recv()
                .await
                .expect("the remaining chunks follow");
        }

        drop(control_tx);
        assert!(
            timeout(Duration::from_secs(5), writer)
                .await
                .expect("writer ends")
                .expect("writer does not panic")
                .is_ok()
        );
        assert!(recorded_rx.try_recv().is_err(), "nothing is written twice");
    }

    #[tokio::test]
    async fn a_closed_connection_is_reported_as_such() {
        let mut receiver = FakeReceiver::new([]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Closed)
        ));
    }

    #[tokio::test]
    async fn a_first_frame_other_than_a_handshake_is_rejected() {
        let bytes = Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
        .encode_into(Vec::new())
        .expect("gossip encodes");
        let mut receiver = FakeReceiver::new([bytes]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::NotAHandshake)
        ));
    }

    /// A refusal in place of the reply handshake surfaces as its reason, so the dialer can tell
    /// a not-yet-converged member (retry) from its own death (final).
    #[tokio::test]
    async fn a_refused_handshake_surfaces_its_reason() {
        let bytes = Frame::Refused {
            reason: RefusalReason::Down,
        }
        .encode_into(Vec::new())
        .expect("refusal encodes");
        let mut receiver = FakeReceiver::new([bytes]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Refused(RefusalReason::Down))
        ));
    }

    #[tokio::test]
    async fn an_undecodable_first_frame_is_rejected() {
        let mut receiver = FakeReceiver::new([vec![0xff; 8]]);

        assert!(matches!(
            recv_handshake(&mut receiver).await,
            Err(ConnectError::Decode(_))
        ));
    }
    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    /// A transport handing every dial to the test, which answers it with a connection it scripts
    /// or never answers at all.
    struct ScriptedTransport(UnboundedSender<Dial>);

    struct Dial {
        addr: SocketAddr,
        answer: oneshot::Sender<ConnectedControl<ScriptedConnection>>,
    }

    impl Transport for ScriptedTransport {
        type Connection = ScriptedConnection;

        fn data_streams(&self) -> Option<NonZeroUsize> {
            None
        }

        async fn connect(
            &self,
            addr: SocketAddr,
            _: usize,
        ) -> Result<ConnectedControl<ScriptedConnection>, TransportError> {
            let (answer, answer_rx) = oneshot::channel();
            self.0
                .send(Dial { addr, answer })
                .expect("the test holds the dials");
            answer_rx
                .await
                .map_err(|_| TransportError::other(io::Error::other("dial dropped")))
        }

        async fn accept(&self, _: usize) -> Result<ScriptedConnection, TransportError> {
            pending().await
        }
    }

    /// Hands every data stream it opens to the test.
    struct ScriptedConnection(UnboundedSender<ChannelReceiver>);

    impl Connection for ScriptedConnection {
        type Sender = ChannelSender;
        type Receiver = ChannelReceiver;

        async fn accept_control(&self) -> Result<(ChannelSender, ChannelReceiver), TransportError> {
            unreachable!("the dialing side opens the control stream")
        }

        async fn open_data(&self) -> Result<ChannelSender, TransportError> {
            let (frames_tx, frames) = unbounded_channel();
            self.0
                .send(ChannelReceiver {
                    frames,
                    current: Vec::new(),
                })
                .map_err(|_| TransportError::other(io::Error::other("connection closed")))?;
            Ok(ChannelSender(frames_tx))
        }

        async fn accept_data(&self) -> Result<Option<ChannelReceiver>, TransportError> {
            pending().await
        }
    }

    struct ChannelSender(UnboundedSender<Vec<u8>>);

    impl FrameSender for ChannelSender {
        async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
            self.0
                .send(frame.to_vec())
                .map_err(|_| TransportError::other(io::Error::other("stream closed")))
        }

        async fn send_batch(&mut self, frames: &[Vec<u8>]) -> Result<(), TransportError> {
            for frame in frames {
                self.send(frame).await?;
            }
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), TransportError> {
            Ok(())
        }
    }

    struct ChannelReceiver {
        frames: UnboundedReceiver<Vec<u8>>,
        current: Vec<u8>,
    }

    impl FrameReceiver for ChannelReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            match self.frames.recv().await {
                Some(frame) => {
                    self.current = frame;
                    Ok(Some(&self.current))
                }

                None => Ok(None),
            }
        }
    }

    /// The test's end of a scripted connection: what the joiner sent, on the control stream and
    /// on the data streams it opened, and the frames to answer with. Dropping it cuts the
    /// connection.
    struct Acceptor {
        from_joiner: UnboundedReceiver<Vec<u8>>,
        to_joiner: UnboundedSender<Vec<u8>>,
        data_streams: UnboundedReceiver<ChannelReceiver>,
        cluster: ClusterId,
    }

    impl Acceptor {
        fn connected() -> (ConnectedControl<ScriptedConnection>, Acceptor) {
            let (to_joiner, joiner_rx) = unbounded_channel();
            let (joiner_tx, from_joiner) = unbounded_channel();
            let (data_streams_tx, data_streams) = unbounded_channel();
            let connected = ConnectedControl {
                connection: ScriptedConnection(data_streams_tx),
                control_tx: ChannelSender(joiner_tx),
                control_rx: ChannelReceiver {
                    frames: joiner_rx,
                    current: Vec::new(),
                },
            };

            (
                connected,
                Acceptor {
                    from_joiner,
                    to_joiner,
                    data_streams,
                    cluster: ClusterId::new(),
                },
            )
        }

        async fn joiner_bytes(&mut self) -> Vec<u8> {
            self.from_joiner
                .recv()
                .await
                .expect("the joiner sends a handshake")
        }

        async fn joiner_handshake(&mut self) -> HandshakeV1 {
            let bytes = self.joiner_bytes().await;
            let envelope = HandshakeEnvelope::decode(&bytes)
                .expect("an envelope")
                .expect("a handshake");
            HandshakeEnvelope::this_build()
                .negotiate(envelope)
                .expect("a version in common");
            HandshakeV1::decode(&bytes).expect("a version 1 body")
        }

        fn admit(&self, addr: SocketAddr) {
            self.send(&Frame::Handshake(Handshake::new(HandshakeV1 {
                node: NodeId::new(addr),
                intent: HandshakeIntent::Member,
                cluster: Some(self.cluster),
            })));
        }

        fn send_bytes(&self, bytes: Vec<u8>) {
            self.to_joiner.send(bytes).expect("the joiner reads");
        }

        fn gossip(&self, members: Vec<Member>, more: bool) {
            self.send(&Frame::Gossip { members, more });
        }

        fn refuse(&self, reason: RefusalReason) {
            self.send(&Frame::Refused { reason });
        }

        fn send(&self, frame: &Frame<'_>) {
            self.send_bytes(frame.encode_into(Vec::new()).expect("frame encodes"));
        }
    }

    /// A leaked endpoint with its join loop running against a scripted transport. The lane dial
    /// requests must stay receivable, since applying a snapshot opens lanes towards the members.
    struct Joining {
        endpoint: &'static EndpointInner,
        dials: UnboundedReceiver<Dial>,
        _lane_dials: flume::Receiver<DialRequest>,
    }

    fn joining(port: u16) -> Joining {
        joining_with(EndpointConfig::new(addr(port)))
    }

    fn joining_with(config: EndpointConfig) -> Joining {
        let (endpoint, lane_dials, join_requests) = EndpointInner::for_tests(config);
        let (dial_tx, dials) = unbounded_channel();
        task::spawn(join_loop(
            Arc::new(ScriptedTransport(dial_tx)),
            join_requests,
            endpoint,
        ));

        Joining {
            endpoint,
            dials,
            _lane_dials: lane_dials,
        }
    }

    /// The first verdict of forming once no join attempt is in flight anymore.
    async fn form_once_settled(endpoint: &'static EndpointInner) -> Result<(), FormError> {
        timeout(Duration::from_secs(5), async {
            loop {
                match endpoint.form_cluster() {
                    Err(FormError::JoinInFlight) => sleep(Duration::from_millis(1)).await,
                    verdict => return verdict,
                }
            }
        })
        .await
        .expect("the join attempt finishes")
    }

    /// A node which is no cluster admits nobody, so nothing can join it before it has formed or
    /// joined; once formed, it admits.
    #[tokio::test]
    async fn an_unformed_endpoint_refuses_joins_as_no_cluster() {
        let (endpoint, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let joiner = NodeId::new(addr(2));

        assert!(matches!(
            admit_inbound(joiner, HandshakeIntent::Join, None, None, endpoint),
            Admission::Refuse(RefusalReason::NoCluster)
        ));

        endpoint.form_cluster().expect("an unformed endpoint forms");
        assert!(matches!(
            admit_inbound(joiner, HandshakeIntent::Join, None, None, endpoint),
            Admission::Admit(..)
        ));
    }

    /// An admitted join proves a tellus node is at its address, so a refusal of that address is
    /// lifted at once rather than after its backoff.
    #[tokio::test(start_paused = true)]
    async fn an_admitted_join_lifts_a_refusal() {
        let (endpoint, lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let joiner = NodeId::new(addr(2));
        endpoint.refuse(joiner.addr());

        assert!(matches!(
            admit_inbound(joiner, HandshakeIntent::Join, None, None, endpoint),
            Admission::Admit(..)
        ));

        endpoint
            .send(
                joiner,
                Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                },
            )
            .expect("an admitted joiner is dialed at once");
        assert!(!lane_dials.is_empty());
    }

    #[tokio::test]
    async fn a_superseding_join_evicts_the_predecessor_before_showing_the_successor() {
        let (endpoint, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let predecessor = NodeId::new(addr(2));
        let successor = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(predecessor));
        let key = WireKey::new::<u64>(KeyKind::Discovery, "pool");
        let (old_actor, new_actor) = (ActorId::new(), ActorId::new());
        assert!(endpoint.receptionist().apply(
            predecessor,
            1,
            vec![WireGroup {
                key: key.clone(),
                ids: vec![old_actor],
            }],
            true,
        ));
        assert!(endpoint.receptionist().apply(
            successor,
            1,
            vec![WireGroup {
                key: key.clone(),
                ids: vec![new_actor],
            }],
            false,
        ));
        let _listing = endpoint.receptionist().subscribe(key);
        let republished = endpoint.receptionist().republished().len();

        assert!(matches!(
            admit_inbound(successor, HandshakeIntent::Join, None, None, endpoint),
            Admission::Admit(..)
        ));

        assert_eq!(
            endpoint.receptionist().republished()[republished..],
            [vec![], vec![new_actor]]
        );
    }

    /// A snapshot cut before its last chunk is applied not at all, and the address stays pinned:
    /// the cluster admitted this node, so it may count it already.
    #[tokio::test]
    async fn a_truncated_snapshot_is_cluster_seen_and_applies_nothing() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        assert_eq!(dial.addr, seed);
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        assert_eq!(
            acceptor.joiner_handshake().await.intent,
            HandshakeIntent::Join
        );
        acceptor.admit(seed);
        acceptor.gossip(
            vec![Member::new(NodeId::new(addr(9)), MemberState::Up)],
            true,
        );
        drop(acceptor);

        let round = round.await.expect("the round completes");
        assert!(matches!(round, JoinRound::ClusterSeen(pinned, _) if pinned == seed));
        assert_eq!(endpoint.pinned_cluster(), Some(seed));
        assert!(!endpoint.formed());
        let members = endpoint.membership().members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].addr(), addr(1));
        assert_eq!(members[0].state(), MemberState::Up);
        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::ClusterPinned(pinned)) if pinned == seed
        ));
    }

    /// The caller going away after the cluster answered cannot discard what the endpoint learned:
    /// the attempt runs on, and the pin it earned stands once it has finished.
    #[tokio::test]
    async fn a_cancelled_caller_cannot_discard_earned_evidence() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: seed,
            result_tx,
        });

        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        drop(result_rx);
        drop(acceptor);

        assert!(matches!(
            form_once_settled(endpoint).await,
            Err(FormError::ClusterPinned(pinned)) if pinned == seed
        ));
        assert!(!endpoint.formed());
    }

    /// A connect which never resolves is abandoned with its caller, since nothing has been sent
    /// yet, and releases the permit; a connect which is still awaited holds it.
    #[tokio::test]
    async fn a_hung_connect_is_released_with_its_caller() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: addr(2),
            result_tx,
        });
        let dial = joining.dials.recv().await.expect("a dial");

        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::JoinInFlight)
        ));

        drop(result_rx);
        assert!(form_once_settled(endpoint).await.is_ok());
        drop(dial);
    }

    /// Attempts are exclusive: the second is not even dialed until the first has resolved.
    #[tokio::test]
    async fn a_second_join_waits_for_the_first() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, _second_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: addr(2),
            result_tx: first_tx,
        });
        endpoint.request_join(JoinRequest {
            addr: addr(3),
            result_tx: second_tx,
        });

        let first = joining.dials.recv().await.expect("the first dial");
        assert_eq!(first.addr, addr(2));
        assert!(
            timeout(Duration::from_millis(100), joining.dials.recv())
                .await
                .is_err(),
            "the second attempt dialed while the first was in flight"
        );

        let (connected, mut acceptor) = Acceptor::connected();
        assert!(first.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.refuse(RefusalReason::NoCluster);
        assert!(matches!(
            first_rx.await,
            Ok(Err(ConnectError::Refused(RefusalReason::NoCluster)))
        ));

        let second = joining.dials.recv().await.expect("the second dial");
        assert_eq!(second.addr, addr(3));
    }

    struct PanickingTransport;

    impl Transport for PanickingTransport {
        type Connection = ScriptedConnection;

        fn data_streams(&self) -> Option<NonZeroUsize> {
            None
        }

        async fn connect(
            &self,
            _: SocketAddr,
            _: usize,
        ) -> Result<ConnectedControl<ScriptedConnection>, TransportError> {
            panic!("transport bug")
        }

        async fn accept(&self, _: usize) -> Result<ScriptedConnection, TransportError> {
            pending().await
        }
    }

    /// A panic before the `Join` handshake was sent proves the seed did not admit this node, so
    /// the attempt ends without blocking forming.
    #[tokio::test]
    async fn a_panicking_join_attempt_is_finished() {
        let (endpoint, _lane_dials, join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        task::spawn(join_loop(
            Arc::new(PanickingTransport),
            join_requests,
            endpoint,
        ));

        for seed in [addr(2), addr(3)] {
            let (result_tx, result_rx) = oneshot::channel();
            endpoint.request_join(JoinRequest {
                addr: seed,
                result_tx,
            });
            assert!(
                timeout(Duration::from_secs(5), result_rx)
                    .await
                    .expect("the attempt ends")
                    .is_err()
            );
        }

        assert!(form_once_settled(endpoint).await.is_ok());
    }

    /// A transport whose control stream reader panics on the first frame the seed sends, and
    /// which hands the seed's ends of the control stream to the test.
    struct PanickingReaderTransport(UnboundedSender<SeedEnds>);

    struct SeedEnds {
        from_joiner: UnboundedReceiver<Vec<u8>>,
        to_joiner: UnboundedSender<Vec<u8>>,
    }

    impl Transport for PanickingReaderTransport {
        type Connection = PanickingReaderConnection;

        fn data_streams(&self) -> Option<NonZeroUsize> {
            None
        }

        async fn connect(
            &self,
            _: SocketAddr,
            _: usize,
        ) -> Result<ConnectedControl<PanickingReaderConnection>, TransportError> {
            let (joiner_tx, from_joiner) = unbounded_channel();
            let (to_joiner, joiner_rx) = unbounded_channel();
            self.0
                .send(SeedEnds {
                    from_joiner,
                    to_joiner,
                })
                .expect("the test holds the seed's ends");
            Ok(ConnectedControl {
                connection: PanickingReaderConnection,
                control_tx: ChannelSender(joiner_tx),
                control_rx: PanickingReceiver(joiner_rx),
            })
        }

        async fn accept(&self, _: usize) -> Result<PanickingReaderConnection, TransportError> {
            pending().await
        }
    }

    struct PanickingReaderConnection;

    impl Connection for PanickingReaderConnection {
        type Sender = ChannelSender;
        type Receiver = PanickingReceiver;

        async fn accept_control(
            &self,
        ) -> Result<(ChannelSender, PanickingReceiver), TransportError> {
            unreachable!("the dialing side opens the control stream")
        }

        async fn open_data(&self) -> Result<ChannelSender, TransportError> {
            unreachable!("a join opens no data streams")
        }

        async fn accept_data(&self) -> Result<Option<PanickingReceiver>, TransportError> {
            pending().await
        }
    }

    struct PanickingReceiver(UnboundedReceiver<Vec<u8>>);

    impl FrameReceiver for PanickingReceiver {
        async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
            self.0.recv().await;
            panic!("reader bug")
        }
    }

    /// A panic after the seed received the `Join` handshake proves nothing about admission, so
    /// forming is blocked until `ambiguous_join_bound` has passed.
    #[tokio::test(start_paused = true)]
    async fn a_join_attempt_panicking_after_its_handshake_is_ambiguous() {
        let mut config = EndpointConfig::new(addr(1));
        config.ambiguous_join_bound = Duration::from_secs(30);
        let (endpoint, _lane_dials, join_requests) = EndpointInner::for_tests(config);
        let (seed_ends_tx, mut seed_ends) = unbounded_channel();
        task::spawn(join_loop(
            Arc::new(PanickingReaderTransport(seed_ends_tx)),
            join_requests,
            endpoint,
        ));
        let seed = addr(2);
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: seed,
            result_tx,
        });

        let SeedEnds {
            mut from_joiner,
            to_joiner,
        } = seed_ends.recv().await.expect("a dial");
        from_joiner
            .recv()
            .await
            .expect("the joiner sends a handshake");
        to_joiner
            .send(Vec::new())
            .expect("the joiner reads its control stream");
        assert!(result_rx.await.is_err(), "the attempt panics");

        assert!(matches!(
            form_once_settled(endpoint).await,
            Err(FormError::JoinAmbiguous(ambiguous)) if ambiguous == seed
        ));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(endpoint.form_cluster().is_ok());
    }

    #[tokio::test]
    async fn an_oversize_join_snapshot_ends_the_attempt() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: seed,
            result_tx,
        });

        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        let max = MAX_SNAPSHOT_MEMBERS + membership::max_fences(endpoint.config().down_watermarks);
        let page = (0..=max)
            .map(|index| {
                Member::new(
                    NodeId::new(SocketAddr::from((
                        Ipv4Addr::LOCALHOST,
                        (index % 60_000) as u16,
                    ))),
                    MemberState::Up,
                )
            })
            .collect();
        acceptor.gossip(page, true);

        assert!(matches!(
            timeout(Duration::from_secs(5), result_rx).await,
            Ok(Ok(Err(ConnectError::SnapshotTooLarge(limit)))) if limit == max
        ));
    }

    /// A seed may hold fences above its cap up to the ceiling, and a joiner with the same cap
    /// accepts such a snapshot and keeps every fence in it.
    #[tokio::test]
    async fn a_join_snapshot_above_the_fence_cap_is_kept_whole() {
        let mut config = EndpointConfig::new(addr(1));
        config.down_watermarks = NonZeroUsize::new(2).expect("2 is not zero");
        let mut joining = joining_with(config);
        let endpoint = joining.endpoint;
        let seed = addr(2);
        let fences = (3..7)
            .map(|port| NodeId::new(addr(port)))
            .collect::<Vec<_>>();
        let snapshot = iter::once(Member::new(NodeId::new(seed), MemberState::Up))
            .chain(
                fences
                    .iter()
                    .map(|node| Member::new(*node, MemberState::Down)),
            )
            .collect();

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        acceptor.gossip(snapshot, false);

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::Joined
        ));
        assert_eq!(
            endpoint.membership().fences(),
            fences.into_iter().collect::<HashSet<_>>()
        );
    }

    /// A complete snapshot which lists this incarnation as Down downs this node, and the join
    /// reports that rather than success.
    #[tokio::test]
    async fn a_snapshot_downing_this_incarnation_returns_downed() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        acceptor.gossip(
            vec![
                Member::new(NodeId::new(seed), MemberState::Up),
                Member::new(endpoint.node(), MemberState::Down),
            ],
            false,
        );

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::Downed
        ));
        assert!(endpoint.downed());
        assert!(matches!(endpoint.form_cluster(), Err(FormError::Downed)));
    }

    /// An accepted connection whose control stream is handed out once.
    struct InboundConnection(Mutex<Option<(ChannelSender, ChannelReceiver)>>);

    impl Connection for InboundConnection {
        type Sender = ChannelSender;
        type Receiver = ChannelReceiver;

        async fn accept_control(&self) -> Result<(ChannelSender, ChannelReceiver), TransportError> {
            Ok(lock(&self.0)
                .take()
                .expect("one control stream per connection"))
        }

        async fn open_data(&self) -> Result<ChannelSender, TransportError> {
            unreachable!("the accepting side opens no data stream")
        }

        async fn accept_data(&self) -> Result<Option<ChannelReceiver>, TransportError> {
            pending().await
        }
    }

    /// The test's end of a connection the endpoint accepted: the frames to send it and what it
    /// answered. Dropping it closes the connection.
    struct Dialer {
        to_acceptor: UnboundedSender<Vec<u8>>,
        from_acceptor: UnboundedReceiver<Vec<u8>>,
        accepted: JoinHandle<()>,
    }

    impl Dialer {
        fn accepted_by(endpoint: &'static EndpointInner) -> Dialer {
            let (to_acceptor, acceptor_rx) = unbounded_channel();
            let (acceptor_tx, from_acceptor) = unbounded_channel();
            let connection = InboundConnection(Mutex::new(Some((
                ChannelSender(acceptor_tx),
                ChannelReceiver {
                    frames: acceptor_rx,
                    current: Vec::new(),
                },
            ))));

            Dialer {
                to_acceptor,
                from_acceptor,
                accepted: task::spawn(run_accepted(connection, endpoint)),
            }
        }

        fn send(&self, bytes: Vec<u8>) {
            self.to_acceptor.send(bytes).expect("the acceptor reads");
        }

        fn send_handshake(&self, handshake: HandshakeV1) {
            self.send(
                Frame::Handshake(Handshake::new(handshake))
                    .encode_into(Vec::new())
                    .expect("handshake encodes"),
            );
        }

        async fn recv(&mut self) -> Vec<u8> {
            timeout(Duration::from_secs(5), self.from_acceptor.recv())
                .await
                .expect("the acceptor answers in time")
                .expect("the acceptor answers")
        }

        async fn reply_handshake(&mut self) -> HandshakeV1 {
            let bytes = self.recv().await;
            let envelope = HandshakeEnvelope::decode(&bytes)
                .expect("an envelope")
                .expect("a handshake");
            assert_eq!(envelope, HandshakeEnvelope::this_build());
            HandshakeV1::decode(&bytes).expect("a version 1 body")
        }

        async fn refusal(&mut self) -> RefusalReason {
            match Frame::from_bytes(&self.recv().await).expect("a frame") {
                Frame::Refused { reason } => reason,
                _ => panic!("not a refusal"),
            }
        }

        /// The acceptor answers and closes only once this side closed, proving it read the answer.
        async fn close(self) {
            drop(self.to_acceptor);
            timeout(Duration::from_secs(5), self.accepted)
                .await
                .expect("the acceptor closes in time")
                .expect("the acceptor does not panic");
        }
    }

    fn formed(port: u16) -> &'static EndpointInner {
        let (endpoint, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(port)));
        endpoint.form_cluster().expect("an unformed endpoint forms");
        endpoint
    }

    /// A version 2 dialer sends its newest body; this build reads its version 1 prefix, admits
    /// the joiner and answers with the body of the version negotiated, which the dialer decodes
    /// with that version's schema.
    #[tokio::test]
    async fn a_newer_dialer_is_answered_in_the_negotiated_version() {
        let endpoint = formed(1);
        let joiner = NodeId::new(addr(2));
        let mut dialer = Dialer::accepted_by(endpoint);

        dialer.send(synthetic::encode(
            synthetic::envelope(1, 2),
            Body::V2(HandshakeV2 {
                node: joiner,
                intent: HandshakeIntent::Join,
                cluster: None,
                appended: 7,
            }),
        ));

        let reply = dialer.recv().await;
        let envelope = HandshakeEnvelope::decode(&reply)
            .expect("an envelope")
            .expect("a handshake");
        assert_eq!(envelope, HandshakeEnvelope::this_build());
        let version = synthetic::envelope(1, 2)
            .negotiate(envelope)
            .expect("a version in common");
        assert_eq!(version, 1);
        let Body::V1(body) = synthetic::decode(&reply, version) else {
            panic!("not a version 1 body");
        };
        assert_eq!(body.node, endpoint.node());
        assert_eq!(body.cluster, endpoint.committed_cluster());
        assert!(endpoint.membership().is_up(joiner));
    }

    /// This build sends its newest body to a version 2 acceptor, which reads it as version 1
    /// and answers in version 1, so the join completes and adopts the acceptor's cluster.
    #[tokio::test]
    async fn an_older_dialer_is_answered_in_the_negotiated_version() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());

        let bytes = acceptor.joiner_bytes().await;
        let envelope = HandshakeEnvelope::decode(&bytes)
            .expect("an envelope")
            .expect("a handshake");
        let version = synthetic::envelope(1, 2)
            .negotiate(envelope)
            .expect("a version in common");
        assert_eq!(version, 1);
        let Body::V1(handshake) = synthetic::decode(&bytes, version) else {
            panic!("not a version 1 body");
        };
        assert_eq!(handshake.node, endpoint.node());
        assert_eq!(handshake.intent, HandshakeIntent::Join);

        acceptor.send_bytes(synthetic::encode(
            synthetic::envelope(1, 2),
            Body::V1(HandshakeV1 {
                node: NodeId::new(seed),
                intent: HandshakeIntent::Member,
                cluster: Some(acceptor.cluster),
            }),
        ));
        acceptor.gossip(vec![Member::new(NodeId::new(seed), MemberState::Up)], false);

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::Joined
        ));
        assert_eq!(endpoint.committed_cluster(), Some(acceptor.cluster));
    }

    /// Without a version in common the acceptor answers with its bare envelope and closes, and
    /// the dialer fails the attempt as incompatible, naming both ranges.
    #[tokio::test]
    async fn disjoint_versions_are_incompatible_in_both_directions() {
        let endpoint = formed(1);
        let joiner = NodeId::new(addr(2));
        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send(synthetic::encode(
            synthetic::envelope(2, 3),
            Body::V2(HandshakeV2 {
                node: joiner,
                intent: HandshakeIntent::Join,
                cluster: None,
                appended: 7,
            }),
        ));

        let reply = dialer.recv().await;
        assert_eq!(
            reply,
            HandshakeEnvelope::this_build()
                .encode()
                .expect("envelope encodes")
        );
        let envelope = HandshakeEnvelope::decode(&reply)
            .expect("an envelope")
            .expect("a handshake");
        assert!(matches!(
            synthetic::envelope(2, 3).negotiate(envelope),
            Err(HandshakeError::Incompatible { .. })
        ));
        dialer.close().await;
        assert!(!endpoint.membership().is_up(joiner));

        let mut joining = joining(3);
        let seed = addr(4);
        let (result_tx, result_rx) = oneshot::channel();
        joining.endpoint.request_join(JoinRequest {
            addr: seed,
            result_tx,
        });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_bytes().await;
        acceptor.send_bytes(
            synthetic::envelope(2, 3)
                .encode()
                .expect("envelope encodes"),
        );

        let result = timeout(Duration::from_secs(5), result_rx)
            .await
            .expect("the attempt ends")
            .expect("the attempt reports");
        assert!(matches!(
            result,
            Err(ConnectError::Handshake(HandshakeError::Incompatible { ours, theirs }))
                if ours == (1..=1) && theirs == (2..=3)
        ));
        assert_eq!(joining.endpoint.pinned_cluster(), None);
    }

    /// A member handshake naming another cluster is refused as foreign, in both directions,
    /// while one naming this cluster, or none, is admitted as before.
    #[tokio::test]
    async fn a_member_of_another_cluster_is_refused() {
        let endpoint = formed(1);
        let member = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(member));
        let this_cluster = endpoint.committed_cluster();
        let other = ClusterId::new();

        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: member,
            intent: HandshakeIntent::Member,
            cluster: Some(other),
        });
        assert_eq!(dialer.refusal().await, RefusalReason::ForeignCluster);
        dialer.close().await;

        assert!(matches!(
            admit_inbound(
                member,
                HandshakeIntent::Member,
                this_cluster,
                None,
                endpoint
            ),
            Admission::Admit(..)
        ));
        assert!(matches!(
            admit_inbound(member, HandshakeIntent::Member, None, None, endpoint),
            Admission::Admit(..)
        ));

        assert!(matches!(
            check_dialed(member, Some(other), addr(2), None, endpoint),
            Err(ConnectError::ForeignCluster(cluster)) if cluster == other
        ));
        assert!(check_dialed(member, this_cluster, addr(2), None, endpoint).is_ok());
        assert!(check_dialed(member, None, addr(2), None, endpoint).is_ok());
    }

    /// A join naming another cluster is refused as foreign and admits nothing; a formed node's
    /// own join names its cluster, so a seed of another cluster refuses it, and a seed answering
    /// with another cluster fails the attempt on the joiner's side too. Both sides record the
    /// other cluster as foreign.
    #[tokio::test]
    async fn a_joiner_of_another_cluster_is_refused() {
        let endpoint = formed(1);
        let joiner = NodeId::new(addr(2));
        let joiner_cluster = ClusterId::new();
        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: joiner,
            intent: HandshakeIntent::Join,
            cluster: Some(joiner_cluster),
        });
        assert_eq!(dialer.refusal().await, RefusalReason::ForeignCluster);
        dialer.close().await;
        assert!(!endpoint.membership().is_up(joiner));
        assert_eq!(
            endpoint.state_rx().borrow().foreign_clusters(),
            [ForeignCluster::new(addr(2), joiner_cluster)]
        );

        let mut joining = joining(3);
        let endpoint = joining.endpoint;
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let seed = addr(4);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        assert_eq!(
            acceptor.joiner_handshake().await.cluster,
            endpoint.committed_cluster()
        );
        acceptor.refuse(RefusalReason::ForeignCluster);
        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::ForeignCluster(_)
        ));

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::ForeignCluster(_)
        ));
        assert_ne!(endpoint.committed_cluster(), Some(acceptor.cluster));
        assert_eq!(
            endpoint.state_rx().borrow().foreign_clusters(),
            [ForeignCluster::new(seed, acceptor.cluster)]
        );
    }

    /// A node of another cluster at the address of a dead incarnation is refused as foreign, not
    /// as down; one naming this cluster, or none, still meets the Down fence.
    #[test]
    fn a_foreign_cluster_refusal_precedes_an_old_down_fence() {
        let old_cluster = formed(1);
        let new_cluster = formed(2);
        let peer = new_cluster.node();
        old_cluster.update(|membership, _| membership.add_up(peer));
        old_cluster.node_death(peer);

        for intent in [HandshakeIntent::Join, HandshakeIntent::Member] {
            match admit_inbound(
                peer,
                intent,
                new_cluster.effective_cluster(),
                None,
                old_cluster,
            ) {
                Admission::Refuse(RefusalReason::ForeignCluster) => {}
                Admission::Refuse(reason) => panic!("expected ForeignCluster, got {reason:?}"),
                _ => panic!("expected ForeignCluster refusal"),
            }
            for cluster in [old_cluster.effective_cluster(), None] {
                assert!(matches!(
                    admit_inbound(peer, intent, cluster, None, old_cluster),
                    Admission::Refuse(RefusalReason::Down)
                ));
            }
        }
    }

    /// A dialed node of another cluster at the address of a dead incarnation fails as foreign,
    /// not as dead; one naming this cluster, or none, still meets the Down fence.
    #[test]
    fn a_dialed_foreign_cluster_precedes_an_old_down_fence() {
        let old_cluster = formed(1);
        let new_cluster = formed(2);
        let peer = new_cluster.node();
        old_cluster.update(|membership, _| membership.add_up(peer));
        old_cluster.node_death(peer);
        let foreign = new_cluster.effective_cluster();

        assert!(matches!(
            check_dialed(peer, foreign, addr(2), None, old_cluster),
            Err(ConnectError::ForeignCluster(cluster)) if Some(cluster) == foreign
        ));
        for cluster in [old_cluster.effective_cluster(), None] {
            assert!(matches!(
                check_dialed(peer, cluster, addr(2), None, old_cluster),
                Err(ConnectError::Dead)
            ));
        }
    }

    /// A joiner which is no cluster names none, adopts its seed's cluster once the snapshot is
    /// complete and publishes it.
    #[tokio::test]
    async fn a_joiner_adopts_the_cluster_of_its_seed() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        assert_eq!(acceptor.joiner_handshake().await.cluster, None);
        acceptor.admit(seed);
        acceptor.gossip(vec![Member::new(NodeId::new(seed), MemberState::Up)], false);

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::Joined
        ));
        assert_eq!(endpoint.committed_cluster(), Some(acceptor.cluster));
        assert_eq!(endpoint.effective_cluster(), Some(acceptor.cluster));
        assert_eq!(
            endpoint.state_rx().borrow().cluster(),
            Some(acceptor.cluster)
        );
        assert_eq!(endpoint.pinned_cluster(), None);
    }

    /// While a member is outside this node's component, a join from a new address is answered
    /// `JoinsHeld` and admits nothing, while a younger incarnation at the unreachable member's
    /// address supersedes it; with every member reachable again, the held joiner is admitted.
    #[tokio::test]
    async fn joins_are_held_while_a_member_is_unreachable() {
        let endpoint = formed(1);
        let unreachable = NodeId::new(addr(2));
        endpoint.update(|membership, reachability| {
            membership.add_up(unreachable);
            reachability.observe(unreachable, false);
        });
        let joiner = NodeId::new(addr(3));

        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: joiner,
            intent: HandshakeIntent::Join,
            cluster: None,
        });
        assert_eq!(dialer.refusal().await, RefusalReason::JoinsHeld);
        dialer.close().await;
        assert!(!endpoint.membership().is_up(joiner));

        let successor = NodeId::new(addr(2));
        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: successor,
            intent: HandshakeIntent::Join,
            cluster: None,
        });
        assert_eq!(dialer.reply_handshake().await.node, endpoint.node());
        assert!(endpoint.membership().is_up(successor));
        assert!(endpoint.membership().is_down(unreachable));

        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: joiner,
            intent: HandshakeIntent::Join,
            cluster: None,
        });
        assert_eq!(dialer.reply_handshake().await.node, endpoint.node());
        assert!(endpoint.membership().is_up(joiner));
    }

    /// A held join ends the round at once as `Held`, without trying further seeds, and pins
    /// nothing, since the seed admitted nobody.
    #[tokio::test]
    async fn a_held_join_ends_the_round_as_held() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let (seed, other) = (addr(2), addr(3));

        let round =
            task::spawn(async move { membership::join_round(endpoint, &[seed, other]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        assert_eq!(dial.addr, seed);
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.refuse(RefusalReason::JoinsHeld);

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::Held(held, _) if held == seed
        ));
        assert!(
            joining.dials.try_recv().is_err(),
            "no further seed is dialed"
        );
        assert_eq!(endpoint.pinned_cluster(), None);
        assert_eq!(endpoint.committed_cluster(), None);
    }

    /// A probe gets the reply handshake naming this node's cluster, from another cluster too,
    /// and nothing else: no snapshot follows and nobody is admitted. An unformed node answers it
    /// naming no cluster.
    #[tokio::test]
    async fn a_probe_is_answered_and_never_admitted() {
        let endpoint = formed(1);
        let prober = NodeId::new(addr(2));
        let mut dialer = Dialer::accepted_by(endpoint);
        dialer.send_handshake(HandshakeV1 {
            node: prober,
            intent: HandshakeIntent::Probe,
            cluster: Some(ClusterId::new()),
        });

        let reply = dialer.reply_handshake().await;
        assert_eq!(reply.node, endpoint.node());
        assert_eq!(reply.cluster, endpoint.committed_cluster());
        assert!(
            timeout(Duration::from_millis(100), dialer.from_acceptor.recv())
                .await
                .is_err(),
            "a probe is answered with the handshake alone"
        );
        dialer.close().await;
        assert!(!endpoint.membership().is_up(prober));

        let (unformed, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(3)));
        let mut dialer = Dialer::accepted_by(unformed);
        dialer.send_handshake(HandshakeV1 {
            node: prober,
            intent: HandshakeIntent::Probe,
            cluster: None,
        });
        assert_eq!(dialer.reply_handshake().await.cluster, None);
        dialer.close().await;
    }

    /// How a scripted seed fails a join attempt after the `Join` handshake was sent.
    #[derive(Debug, Clone, Copy)]
    enum PostSendFailure {
        Unwritable,
        Closed,
        Undecodable,
        NotAHandshake,
        OtherAddress,
        DeadIncarnation,
        NoClusterId,
    }

    /// Every failure after the `Join` handshake was sent which proves nothing about admission
    /// leaves the attempt ambiguous, which blocks forming at once.
    #[tokio::test]
    async fn a_join_failing_after_its_handshake_is_ambiguous() {
        for failure in [
            PostSendFailure::Unwritable,
            PostSendFailure::Closed,
            PostSendFailure::Undecodable,
            PostSendFailure::NotAHandshake,
            PostSendFailure::OtherAddress,
            PostSendFailure::DeadIncarnation,
            PostSendFailure::NoClusterId,
        ] {
            let mut joining = joining(1);
            let endpoint = joining.endpoint;
            let seed = addr(2);
            let dead = NodeId::new(seed);
            if matches!(failure, PostSendFailure::DeadIncarnation) {
                endpoint.update(|membership, _| membership.add_up(dead));
                endpoint.node_death(dead);
            }

            let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
            let dial = joining.dials.recv().await.expect("a dial");
            let (connected, mut acceptor) = Acceptor::connected();
            if matches!(failure, PostSendFailure::Unwritable) {
                drop(acceptor);
                assert!(dial.answer.send(connected).is_ok());
            } else {
                assert!(dial.answer.send(connected).is_ok());
                acceptor.joiner_handshake().await;
                let reply = |node, cluster| {
                    Frame::Handshake(Handshake::new(HandshakeV1 {
                        node,
                        intent: HandshakeIntent::Member,
                        cluster,
                    }))
                };
                match failure {
                    PostSendFailure::Closed => drop(acceptor),
                    PostSendFailure::Undecodable => acceptor.send_bytes(vec![0xff; 8]),
                    PostSendFailure::NotAHandshake => acceptor.gossip(Vec::new(), false),
                    PostSendFailure::OtherAddress => acceptor.admit(addr(9)),
                    PostSendFailure::DeadIncarnation => {
                        acceptor.send(&reply(dead, Some(acceptor.cluster)));
                    }
                    PostSendFailure::NoClusterId => acceptor.send(&reply(NodeId::new(seed), None)),
                    PostSendFailure::Unwritable => unreachable!("handled above"),
                }
            }

            assert!(
                matches!(
                    round.await.expect("the round completes"),
                    JoinRound::NoAdmission(_)
                ),
                "{failure:?}"
            );
            assert!(
                matches!(
                    endpoint.form_cluster(),
                    Err(FormError::JoinAmbiguous(ambiguous)) if ambiguous == seed
                ),
                "{failure:?}"
            );
            assert_eq!(endpoint.pinned_cluster(), None, "{failure:?}");
        }
    }

    /// A seed which never answers the `Join` handshake leaves the attempt ambiguous; forming is
    /// blocked until `ambiguous_join_bound` has passed.
    #[tokio::test(start_paused = true)]
    async fn a_silent_seed_blocks_forming_for_the_ambiguous_join_bound() {
        let mut config = EndpointConfig::new(addr(1));
        config.ambiguous_join_bound = Duration::from_secs(30);
        let mut joining = joining_with(config);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::NoAdmission(_)
        ));

        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::JoinAmbiguous(ambiguous)) if ambiguous == seed
        ));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(endpoint.form_cluster().is_ok());
    }

    /// A definitive refusal from the address of an ambiguous attempt proves that it did not admit
    /// this node, so forming proceeds.
    #[tokio::test]
    async fn a_definitive_refusal_lifts_an_ambiguous_join() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        for refuse in [false, true] {
            let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
            let dial = joining.dials.recv().await.expect("a dial");
            let (connected, mut acceptor) = Acceptor::connected();
            assert!(dial.answer.send(connected).is_ok());
            acceptor.joiner_handshake().await;
            if refuse {
                acceptor.refuse(RefusalReason::NoCluster);
            } else {
                drop(acceptor);
            }
            round.await.expect("the round completes");
        }

        assert!(endpoint.form_cluster().is_ok());
    }

    /// Which failures leave a join attempt ambiguous once its handshake was sent: everything but
    /// a refusal, a reply of another cluster, an incompatible answer and an end on this side.
    #[test]
    fn only_failures_short_of_an_answer_leave_a_join_ambiguous() {
        let ambiguous = [
            ConnectError::Transport(TransportError::other(io::Error::other("lost"))),
            ConnectError::Closed,
            ConnectError::DataStreams(1),
            ConnectError::HandshakeTimeout,
            ConnectError::NotAHandshake,
            ConnectError::Identity,
            ConnectError::AddressMismatch {
                advertised: addr(1),
                dialed: addr(2),
            },
            ConnectError::Dead,
            ConnectError::SnapshotTooLarge(1),
            ConnectError::NoClusterId,
        ];
        for error in ambiguous {
            assert!(error.leaves_join_ambiguous(), "{error:?}");
            assert!(!error.proves_no_admission(), "{error:?}");
        }

        let definitive = [
            ConnectError::Refused(RefusalReason::NoCluster),
            ConnectError::Refused(RefusalReason::UnknownMember),
            ConnectError::Refused(RefusalReason::Down),
            ConnectError::Refused(RefusalReason::JoinsHeld),
            ConnectError::Refused(RefusalReason::ForeignCluster),
            ConnectError::ForeignCluster(ClusterId::new()),
        ];
        for error in definitive {
            assert!(!error.leaves_join_ambiguous(), "{error:?}");
            assert!(error.proves_no_admission(), "{error:?}");
        }

        for error in [
            ConnectError::Handshake(HandshakeError::Magic(0)),
            ConnectError::SelfDowned,
            ConnectError::Abandoned,
        ] {
            assert!(!error.leaves_join_ambiguous(), "{error:?}");
        }
    }

    /// A probe sends a `Probe` handshake, answers with the cluster the reply names and closes the
    /// connection right after.
    #[tokio::test]
    async fn a_probe_reads_the_reply_handshake_and_closes() {
        let (endpoint, _lane_dials, _join_requests, probe_requests) =
            EndpointInner::for_tests_with_probes(EndpointConfig::new(addr(1)), 0);
        let (dial_tx, mut dials) = unbounded_channel();
        task::spawn(probe_loop(
            Arc::new(ScriptedTransport(dial_tx)),
            probe_requests,
            endpoint,
        ));
        let seed = addr(2);

        let probe = endpoint.request_probe(seed);
        let dial = dials.recv().await.expect("a dial");
        assert_eq!(dial.addr, seed);
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        assert_eq!(
            acceptor.joiner_handshake().await.intent,
            HandshakeIntent::Probe
        );
        acceptor.admit(seed);

        assert_eq!(
            probe
                .await
                .expect("the probe is answered")
                .expect("the probe succeeds"),
            Some(acceptor.cluster)
        );
        assert!(
            acceptor.from_joiner.recv().await.is_none(),
            "the prober closes after the reply"
        );
    }

    /// A snapshot cut short leaves the seed's cluster provisional: it is the effective one while
    /// the pin lasts, but neither committed nor published, and once discovery drops the seed the
    /// pin goes with it, so forming mints a cluster of its own.
    #[tokio::test]
    async fn a_truncated_snapshot_keeps_the_cluster_provisional() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed);
        acceptor.gossip(vec![Member::new(NodeId::new(seed), MemberState::Up)], true);
        let provisional = acceptor.cluster;
        drop(acceptor);

        assert!(matches!(
            round.await.expect("the round completes"),
            JoinRound::ClusterSeen(pinned, _) if pinned == seed
        ));
        assert_eq!(endpoint.committed_cluster(), None);
        assert_eq!(endpoint.effective_cluster(), Some(provisional));
        assert_eq!(endpoint.state_rx().borrow().cluster(), None);

        assert_eq!(
            endpoint.release_pin_if_gone(&BTreeSet::from([addr(1)])),
            Some(seed)
        );
        assert_eq!(endpoint.effective_cluster(), None);
        endpoint.form_cluster().expect("an unpinned endpoint forms");
        let formed = endpoint.committed_cluster().expect("forming commits an ID");
        assert_ne!(formed, provisional);
        assert_eq!(endpoint.state_rx().borrow().cluster(), Some(formed));
    }

    /// The provisional cluster rides every further join: an independently formed cluster refuses
    /// it as foreign, and the pin stays on the cluster which admitted this node first.
    #[tokio::test]
    async fn a_provisional_cluster_is_refused_by_another_cluster() {
        let mut joining = joining(1);
        let endpoint = joining.endpoint;
        let seed_a = addr(2);

        let round = task::spawn(async move { membership::join_round(endpoint, &[seed_a]).await });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        acceptor.joiner_handshake().await;
        acceptor.admit(seed_a);
        acceptor.gossip(
            vec![Member::new(NodeId::new(seed_a), MemberState::Up)],
            true,
        );
        let cluster_a = acceptor.cluster;
        drop(acceptor);
        round.await.expect("the round completes");

        let seed_b = formed(3);
        let (result_tx, result_rx) = oneshot::channel();
        endpoint.request_join(JoinRequest {
            addr: seed_b.node().addr(),
            result_tx,
        });
        let dial = joining.dials.recv().await.expect("a dial");
        let (connected, mut acceptor) = Acceptor::connected();
        assert!(dial.answer.send(connected).is_ok());
        let handshake = acceptor.joiner_handshake().await;
        assert_eq!(handshake.cluster, Some(cluster_a));
        let Admission::Refuse(reason) = admit_inbound(
            handshake.node,
            handshake.intent,
            handshake.cluster,
            None,
            seed_b,
        ) else {
            panic!("the other cluster admits the joiner");
        };
        assert_eq!(reason, RefusalReason::ForeignCluster);
        acceptor.refuse(reason);

        assert!(matches!(
            timeout(Duration::from_secs(5), result_rx).await,
            Ok(Ok(Err(ConnectError::Refused(
                RefusalReason::ForeignCluster
            ))))
        ));
        assert!(!seed_b.membership().is_up(endpoint.node()));
        assert_eq!(endpoint.pinned_cluster(), Some(seed_a));
        assert_eq!(endpoint.effective_cluster(), Some(cluster_a));
        assert!(matches!(
            endpoint.form_cluster(),
            Err(FormError::ClusterPinned(pinned)) if pinned == seed_a
        ));
    }

    /// An accepted connection whose control stream is handed out once and whose data streams
    /// arrive as the test opens them, until it closes them all.
    struct StreamingConnection {
        control: Mutex<Option<(ChannelSender, ChannelReceiver)>>,
        data: tokio::sync::Mutex<UnboundedReceiver<ChannelReceiver>>,
    }

    impl Connection for StreamingConnection {
        type Sender = ChannelSender;
        type Receiver = ChannelReceiver;

        async fn accept_control(&self) -> Result<(ChannelSender, ChannelReceiver), TransportError> {
            Ok(lock(&self.control)
                .take()
                .expect("one control stream per connection"))
        }

        async fn open_data(&self) -> Result<ChannelSender, TransportError> {
            unreachable!("the accepting side opens no data stream")
        }

        async fn accept_data(&self) -> Result<Option<ChannelReceiver>, TransportError> {
            Ok(self.data.lock().await.recv().await)
        }
    }

    /// The test's end of a member's connection the endpoint accepted, with data streams. Dropping
    /// it, and every stream it opened, closes the connection.
    struct InboundPeer {
        node: NodeId,
        to_acceptor: UnboundedSender<Vec<u8>>,
        streams: UnboundedSender<ChannelReceiver>,
        from_acceptor: UnboundedReceiver<Vec<u8>>,
    }

    impl InboundPeer {
        /// Admitted as a member, once the endpoint reads its connection.
        async fn admitted_by(endpoint: &'static EndpointInner, port: u16) -> InboundPeer {
            let node = NodeId::new(addr(port));
            endpoint.update(|membership, _| membership.add_up(node));
            let peer = InboundPeer::handshaking(endpoint, node, HandshakeIntent::Member)
                .await
                .expect("a member is admitted");
            eventually(|| endpoint.inbound_open(node)).await;
            peer
        }

        /// The connection of a node the endpoint already knows, once it answered the handshake:
        /// admitted, or refused with the reason it gives.
        async fn handshaking(
            endpoint: &'static EndpointInner,
            node: NodeId,
            intent: HandshakeIntent,
        ) -> Result<InboundPeer, RefusalReason> {
            let (to_acceptor, acceptor_rx) = unbounded_channel();
            let (acceptor_tx, mut from_acceptor) = unbounded_channel();
            let (streams, streams_rx) = unbounded_channel();
            let connection = StreamingConnection {
                control: Mutex::new(Some((
                    ChannelSender(acceptor_tx),
                    ChannelReceiver {
                        frames: acceptor_rx,
                        current: Vec::new(),
                    },
                ))),
                data: tokio::sync::Mutex::new(streams_rx),
            };
            task::spawn(run_accepted(connection, endpoint));

            to_acceptor
                .send(
                    Frame::Handshake(Handshake::new(HandshakeV1 {
                        node,
                        intent,
                        cluster: None,
                    }))
                    .encode_into(Vec::new())
                    .expect("handshake encodes"),
                )
                .expect("the acceptor reads");
            let answer = timeout(Duration::from_secs(5), from_acceptor.recv())
                .await
                .expect("the acceptor answers in time")
                .expect("the acceptor answers");
            if let Frame::Refused { reason } = Frame::from_bytes(&answer).expect("frame decodes") {
                return Err(reason);
            }
            Ok(InboundPeer {
                node,
                to_acceptor,
                streams,
                from_acceptor,
            })
        }

        /// A member handshake the dialer gives up right after sending it, so the acceptor's
        /// answer finds the connection gone.
        fn abort_handshake(endpoint: &'static EndpointInner, node: NodeId) {
            let (to_acceptor, acceptor_rx) = unbounded_channel();
            let (acceptor_tx, from_acceptor) = unbounded_channel::<Vec<u8>>();
            let (_streams, streams_rx) = unbounded_channel();
            let connection = StreamingConnection {
                control: Mutex::new(Some((
                    ChannelSender(acceptor_tx),
                    ChannelReceiver {
                        frames: acceptor_rx,
                        current: Vec::new(),
                    },
                ))),
                data: tokio::sync::Mutex::new(streams_rx),
            };
            drop(from_acceptor);
            to_acceptor
                .send(
                    Frame::Handshake(Handshake::new(member_handshake(node)))
                        .encode_into(Vec::new())
                        .expect("handshake encodes"),
                )
                .expect("the acceptor reads");
            task::spawn(run_accepted(connection, endpoint));
        }

        fn send(&self, frame: &Frame<'_>) {
            self.to_acceptor
                .send(frame.encode_into(Vec::new()).expect("frame encodes"))
                .expect("the acceptor reads");
        }

        /// Waits for the acceptor's answer to the drain marker, skipping the setup frames.
        async fn drain_ack(&mut self) {
            timeout(Duration::from_secs(5), async {
                loop {
                    let bytes = self
                        .from_acceptor
                        .recv()
                        .await
                        .expect("the acceptor answers the drain");
                    if Frame::from_bytes(&bytes).expect("frame decodes") == Frame::DrainAck {
                        return;
                    }
                }
            })
            .await
            .expect("the drain is acknowledged in time");
        }

        /// Whether the acceptor has answered the drain marker by now.
        fn drain_acked(&mut self) -> bool {
            drain_acked(&mut self.from_acceptor)
        }

        /// Closes the connection, every stream included, keeping what the acceptor sends.
        fn close(self) -> UnboundedReceiver<Vec<u8>> {
            self.from_acceptor
        }

        fn open_stream(&self) -> DataStream {
            let (frames_tx, frames) = unbounded_channel();
            self.streams
                .send(ChannelReceiver {
                    frames,
                    current: Vec::new(),
                })
                .expect("the acceptor accepts streams");
            DataStream(frames_tx)
        }
    }

    struct DataStream(UnboundedSender<Vec<u8>>);

    impl DataStream {
        fn send(&self, frame: &Frame<'_>) {
            self.0
                .send(frame.encode_into(Vec::new()).expect("frame encodes"))
                .expect("the acceptor reads");
        }
    }

    /// A local actor which a peer's frames are delivered to, watching one of the peer's actors.
    struct Recipient {
        self_ref: SelfRef<u64>,
        incoming_rx: flume::Receiver<Queued<u64>>,
        _watcher_registration: WatcherRegistration,
        received: Vec<Queued<u64>>,
    }

    impl Recipient {
        fn watching(endpoint: &'static EndpointInner, peer: NodeId, target: ActorId) -> Self {
            let (self_ref, mailbox) =
                SelfRef::<u64>::new(ActorId::new(), MailboxCapacity::Unbounded);
            endpoint.registry().bind(self_ref.actor_ref());
            endpoint
                .watchers()
                .add(peer, target, self_ref.make_watcher());
            let (incoming_rx, watcher_registration) = mailbox.split();
            Recipient {
                self_ref,
                incoming_rx,
                _watcher_registration: watcher_registration,
                received: Vec::new(),
            }
        }

        fn id(&self) -> ActorId {
            self.self_ref.actor_ref().actor_id()
        }

        fn poll(&mut self) -> usize {
            self.received.extend(self.incoming_rx.try_iter());
            self.received.len()
        }

        async fn receive(&mut self, count: usize) {
            timeout(Duration::from_secs(5), async {
                while self.poll() < count {
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the frames arrive in time");
        }

        /// What arrived, a message as its value and a terminated signal as `None`.
        fn received(&mut self) -> Vec<Option<u64>> {
            self.poll();
            self.received
                .iter()
                .map(|queued| match queued {
                    Queued::Incoming(Incoming::Message(message)) => Some(*message),
                    Queued::Incoming(Incoming::Terminated(_)) | Queued::Timer(_) => None,
                })
                .collect()
        }
    }

    async fn eventually<F>(condition: F)
    where
        F: Fn() -> bool,
    {
        timeout(Duration::from_secs(5), async {
            while !condition() {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the condition holds in time");
    }

    fn message(endpoint: &EndpointInner, target: ActorId, value: u64) -> Frame<'static> {
        Frame::Message {
            target,
            reply_tags: Vec::new(),
            payload: Cow::Owned(endpoint.codec().encode(&value).expect("encodes")),
        }
    }

    fn drain_acked(from_acceptor: &mut UnboundedReceiver<Vec<u8>>) -> bool {
        iter::from_fn(|| from_acceptor.try_recv().ok())
            .any(|bytes| Frame::from_bytes(&bytes).expect("frame decodes") == Frame::DrainAck)
    }

    fn departure(node: NodeId) -> Frame<'static> {
        Frame::Gossip {
            members: vec![Member::new(node, MemberState::Left)],
            more: false,
        }
    }

    /// A departure learned from the leaver itself defers its death while its connection is open:
    /// a message and a real terminated signal arriving on a data stream after it are delivered,
    /// and the synthesized signals only follow once the connection is closed.
    #[tokio::test]
    async fn a_departure_delivers_what_its_connection_still_carries_before_the_death() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let (stopped, survivor) = (ActorId::new(), ActorId::new());
        let mut recipient = Recipient::watching(endpoint, leaver.node, stopped);
        let mut bystander = Recipient::watching(endpoint, leaver.node, survivor);
        let stream = leaver.open_stream();

        leaver.send(&departure(leaver.node));
        eventually(|| endpoint.membership().state(leaver.node) == Some(MemberState::Left)).await;
        assert!(endpoint.departure_deferred(leaver.node));

        stream.send(&message(endpoint, recipient.id(), 7));
        stream.send(&Frame::Terminated {
            target: stopped,
            watcher: recipient.id(),
        });
        recipient.receive(2).await;
        assert_eq!(recipient.received(), [Some(7), None]);
        assert!(bystander.received().is_empty());
        assert!(endpoint.departure_deferred(leaver.node));

        let node = leaver.node;
        drop((stream, leaver));
        bystander.receive(1).await;
        assert_eq!(bystander.received(), [None]);
        assert!(!endpoint.departure_deferred(node));
        assert_eq!(recipient.received(), [Some(7), None]);
        assert_eq!(endpoint.membership().state(node), Some(MemberState::Left));
    }

    /// A departure forwarded by a third node can arrive before the leaver's own announcement; it
    /// defers the death all the same, so the leaver's remaining frames are still delivered.
    #[tokio::test]
    async fn a_forwarded_departure_defers_the_death_like_the_leavers_own() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let forwarder = InboundPeer::admitted_by(endpoint, 3).await;
        let survivor = ActorId::new();
        let mut recipient = Recipient::watching(endpoint, leaver.node, survivor);
        let stream = leaver.open_stream();

        forwarder.send(&departure(leaver.node));
        eventually(|| endpoint.departure_deferred(leaver.node)).await;

        stream.send(&message(endpoint, recipient.id(), 1));
        leaver.send(&departure(leaver.node));
        stream.send(&message(endpoint, recipient.id(), 2));
        recipient.receive(2).await;
        assert_eq!(recipient.received(), [Some(1), Some(2)]);
        assert!(endpoint.departure_deferred(leaver.node));

        let node = leaver.node;
        drop((stream, leaver));
        recipient.receive(3).await;
        assert_eq!(recipient.received(), [Some(1), Some(2), None]);
        assert!(!endpoint.departure_deferred(node));
        assert!(endpoint.membership().is_up(forwarder.node));
    }

    /// A stream which never finishes cannot hold the death beyond `leave_timeout`: it runs at the
    /// bound, the drain marker is never acknowledged, and nothing the connection carries
    /// afterwards is delivered.
    #[tokio::test(start_paused = true)]
    async fn a_departure_which_never_drains_dies_at_the_leave_timeout() {
        let endpoint = formed(1);
        let mut leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let survivor = ActorId::new();
        let mut recipient = Recipient::watching(endpoint, leaver.node, survivor);
        let stream = leaver.open_stream();
        leaver.send(&departure(leaver.node));
        leaver.send(&Frame::Drained { data_streams: 1 });
        eventually(|| endpoint.departure_deferred(leaver.node)).await;

        endpoint.end_overdue_departures(tokio::time::Instant::now());
        assert!(endpoint.departure_deferred(leaver.node));
        tokio::time::advance(endpoint.config().leave_timeout).await;
        endpoint.end_overdue_departures(tokio::time::Instant::now());

        assert!(!endpoint.departure_deferred(leaver.node));
        assert_eq!(recipient.received(), [None]);
        stream.send(&message(endpoint, recipient.id(), 1));
        eventually(|| !endpoint.inbound_open(leaver.node)).await;
        assert_eq!(recipient.received(), [None]);
        assert!(!leaver.drain_acked());
    }

    /// A downing of the leaver ends the deferral at once: the death runs, the member is Down, and
    /// nothing the connection carries afterwards is delivered.
    #[tokio::test]
    async fn a_downing_during_a_deferred_departure_ends_it() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let survivor = ActorId::new();
        let mut recipient = Recipient::watching(endpoint, leaver.node, survivor);
        let stream = leaver.open_stream();
        leaver.send(&departure(leaver.node));
        eventually(|| endpoint.departure_deferred(leaver.node)).await;

        membership::on_gossip(endpoint, &[Member::new(leaver.node, MemberState::Down)]);

        assert!(!endpoint.departure_deferred(leaver.node));
        assert_eq!(
            endpoint.membership().state(leaver.node),
            Some(MemberState::Down)
        );
        assert_eq!(recipient.received(), [None]);
        stream.send(&message(endpoint, recipient.id(), 1));
        eventually(|| !endpoint.inbound_open(leaver.node)).await;
        assert_eq!(recipient.received(), [None]);
    }

    /// A node whose detailed entry of the leaver is swept hands out only its fence, and that
    /// fence keeps the departure's state: arriving in a setup snapshot while the drain is pending,
    /// it neither ends the deferral nor turns the departure into a downing, and the drain's
    /// completion leaves the leaver Left.
    #[tokio::test]
    async fn a_swept_departure_in_a_setup_snapshot_keeps_the_deferral_running() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let node = leaver.node;
        let stream = leaver.open_stream();
        leaver.send(&departure(node));
        eventually(|| endpoint.departure_deferred(node)).await;

        let swept = Membership::new(NodeId::new(addr(3)), NonZeroUsize::MAX, Duration::ZERO);
        swept.add_up(node);
        swept.leave(node, std::time::Instant::now());
        swept.sweep(std::time::Instant::now());
        let snapshot = swept.handshake_snapshot();
        assert!(snapshot.contains(&Member::new(node, MemberState::Left)));
        assert!(!swept.members().iter().any(|member| member.node() == node));

        membership::on_gossip(endpoint, &snapshot);

        assert!(endpoint.departure_deferred(node));
        drop((stream, leaver));
        eventually(|| !endpoint.departure_deferred(node)).await;
        assert_eq!(endpoint.membership().state(node), Some(MemberState::Left));
        assert!(
            endpoint
                .state_rx()
                .borrow()
                .members()
                .contains(&Member::new(node, MemberState::Left))
        );
    }

    /// A departure gossiped by another member, of a node without a connection here, waits for
    /// one: the leaver's handshake is admitted to drain, its last frames are delivered, and the
    /// death follows its drained connection.
    #[tokio::test]
    async fn a_gossiped_departure_waits_for_the_leavers_connection() {
        let endpoint = formed(1);
        let leaver = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(leaver));
        let mut recipient = Recipient::watching(endpoint, leaver, ActorId::new());

        membership::on_gossip(endpoint, &[Member::new(leaver, MemberState::Left)]);
        assert!(endpoint.departure_deferred(leaver));
        assert!(endpoint.delivers_from(leaver));
        assert!(recipient.received().is_empty());

        let mut connection = InboundPeer::handshaking(endpoint, leaver, HandshakeIntent::Member)
            .await
            .expect("the leaver is admitted to drain");
        let stream = connection.open_stream();
        stream.send(&message(endpoint, recipient.id(), 1));
        connection.send(&Frame::Drained { data_streams: 1 });
        recipient.receive(1).await;
        drop(stream);
        connection.drain_ack().await;
        assert!(endpoint.departure_deferred(leaver));

        drop(connection.close());
        recipient.receive(2).await;
        assert_eq!(recipient.received(), [Some(1), None]);
        assert!(!endpoint.departure_deferred(leaver));
    }

    /// A departed node which never connects dies at the leave timeout, no sooner.
    #[tokio::test(start_paused = true)]
    async fn a_departure_without_a_connection_dies_at_the_leave_timeout() {
        let endpoint = formed(1);
        let leaver = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(leaver));
        let mut recipient = Recipient::watching(endpoint, leaver, ActorId::new());
        membership::on_gossip(endpoint, &[Member::new(leaver, MemberState::Left)]);

        endpoint.end_overdue_departures(tokio::time::Instant::now());
        assert!(endpoint.departure_deferred(leaver));
        assert!(recipient.received().is_empty());
        tokio::time::advance(endpoint.config().leave_timeout).await;
        endpoint.end_overdue_departures(tokio::time::Instant::now());

        assert!(!endpoint.departure_deferred(leaver));
        assert!(!endpoint.delivers_from(leaver));
        assert_eq!(recipient.received(), [None]);
    }

    /// A downing learned while a departure waits for its connection ends the wait.
    #[tokio::test]
    async fn a_downing_while_a_departure_waits_for_its_connection_ends_it() {
        let endpoint = formed(1);
        let leaver = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(leaver));
        let mut recipient = Recipient::watching(endpoint, leaver, ActorId::new());
        membership::on_gossip(endpoint, &[Member::new(leaver, MemberState::Left)]);
        assert!(endpoint.departure_deferred(leaver));

        membership::on_gossip(endpoint, &[Member::new(leaver, MemberState::Down)]);

        assert!(!endpoint.departure_deferred(leaver));
        assert_eq!(recipient.received(), [None]);
    }

    /// A reconnect during a deferred departure supersedes the reader it replaces without ending
    /// the deferral: the replacement delivers the last frames and answers the drain, and the
    /// death runs only once the replacement has ended.
    #[tokio::test]
    async fn a_reconnect_during_a_departure_takes_over_its_drain() {
        let endpoint = formed(1);
        let first = InboundPeer::admitted_by(endpoint, 2).await;
        let node = first.node;
        let mut recipient = Recipient::watching(endpoint, node, ActorId::new());
        first.send(&departure(node));
        eventually(|| endpoint.departure_deferred(node)).await;

        let mut second = InboundPeer::handshaking(endpoint, node, HandshakeIntent::Member)
            .await
            .expect("the reconnect is admitted to drain");
        let stream = second.open_stream();
        stream.send(&message(endpoint, recipient.id(), 1));
        second.send(&Frame::Drained { data_streams: 1 });
        recipient.receive(1).await;
        drop(stream);
        second.drain_ack().await;
        assert!(endpoint.departure_deferred(node));

        drop(second.close());
        recipient.receive(2).await;
        assert_eq!(recipient.received(), [Some(1), None]);
        assert!(!endpoint.departure_deferred(node));
        drop(first);
    }

    /// A handshake admitted to drain but abandoned before its reader starts has drained
    /// nothing: the departure keeps waiting, and a later connection still drains it.
    #[tokio::test]
    async fn an_abandoned_drain_handshake_leaves_the_departure_waiting() {
        let endpoint = formed(1);
        let leaver = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(leaver));
        let mut recipient = Recipient::watching(endpoint, leaver, ActorId::new());
        membership::on_gossip(endpoint, &[Member::new(leaver, MemberState::Left)]);

        InboundPeer::abort_handshake(endpoint, leaver);
        sleep(Duration::from_millis(100)).await;
        assert!(!endpoint.inbound_open(leaver));
        assert!(endpoint.departure_deferred(leaver));
        assert!(recipient.received().is_empty());

        let connection = InboundPeer::handshaking(endpoint, leaver, HandshakeIntent::Member)
            .await
            .expect("a later connection is still admitted to drain");
        drop(connection.close());
        recipient.receive(1).await;
        assert_eq!(recipient.received(), [None]);
    }

    /// A departed incarnation is admitted with a Member handshake only while its departure is
    /// deferred; a join is refused, and so is a Member handshake once the deferral has ended or
    /// the incarnation is Down.
    #[tokio::test(start_paused = true)]
    async fn only_a_deferred_departure_admits_its_member_handshake() {
        let endpoint = formed(1);
        let leaver = NodeId::new(addr(2));
        let downed = NodeId::new(addr(3));
        endpoint.update(|membership, _| {
            membership.add_up(leaver);
            membership.add_up(downed);
        });
        membership::on_gossip(
            endpoint,
            &[
                Member::new(leaver, MemberState::Left),
                Member::new(downed, MemberState::Down),
            ],
        );

        assert_eq!(
            InboundPeer::handshaking(endpoint, leaver, HandshakeIntent::Join)
                .await
                .err(),
            Some(RefusalReason::Down)
        );
        assert_eq!(
            InboundPeer::handshaking(endpoint, downed, HandshakeIntent::Member)
                .await
                .err(),
            Some(RefusalReason::Down)
        );
        assert!(matches!(
            endpoint.admit_member(leaver),
            MemberAdmission::Draining(_)
        ));

        tokio::time::advance(endpoint.config().leave_timeout).await;
        endpoint.end_overdue_departures(tokio::time::Instant::now());
        assert_eq!(
            InboundPeer::handshaking(endpoint, leaver, HandshakeIntent::Member)
                .await
                .err(),
            Some(RefusalReason::Down)
        );
    }

    /// A Member admission and a gossiped departure are serialized by the departures lock, so
    /// either order ends with the connection counted and the death waiting for it: admitted Up
    /// first, the departure is deferred for the counted connection; departed first, the
    /// admission is one to drain.
    #[tokio::test]
    async fn a_member_admission_and_a_departure_serialize_either_way() {
        let endpoint = formed(1);
        let admitted_first = NodeId::new(addr(2));
        let departed_first = NodeId::new(addr(3));
        endpoint.update(|membership, _| {
            membership.add_up(admitted_first);
            membership.add_up(departed_first);
        });

        let MemberAdmission::Member(mut slot) = endpoint.admit_member(admitted_first) else {
            panic!("an Up member is admitted as one");
        };
        membership::on_gossip(endpoint, &[Member::new(admitted_first, MemberState::Left)]);
        assert!(endpoint.departure_deferred(admitted_first));
        assert!(endpoint.inbound_open(admitted_first));
        slot.reading();
        drop(slot);
        assert!(!endpoint.departure_deferred(admitted_first));

        membership::on_gossip(endpoint, &[Member::new(departed_first, MemberState::Left)]);
        let MemberAdmission::Draining(mut slot) = endpoint.admit_member(departed_first) else {
            panic!("a deferred departure is admitted to drain");
        };
        assert!(endpoint.departure_deferred(departed_first));
        slot.reading();
        drop(slot);
        assert!(!endpoint.departure_deferred(departed_first));
    }

    /// Frames still buffered on a data stream when the departed peer closes its connection are
    /// delivered before the synthesized terminated signal: the close ends no reader early.
    #[tokio::test]
    async fn a_closed_departure_delivers_its_buffered_frames_before_the_death() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let node = leaver.node;
        let mut recipient = Recipient::watching(endpoint, node, ActorId::new());
        leaver.send(&departure(node));
        eventually(|| endpoint.departure_deferred(node)).await;

        let stream = leaver.open_stream();
        stream.send(&message(endpoint, recipient.id(), 7));
        stream.send(&message(endpoint, recipient.id(), 8));
        drop((stream, leaver));

        eventually(|| !endpoint.inbound_open(node)).await;
        assert_eq!(recipient.received(), [Some(7), Some(8), None]);
    }

    /// The drain marker is acknowledged once exactly as many data streams as it announces have
    /// ended cleanly: one which ended before the marker counts, one still open holds the answer
    /// back, and so does one accepted only after the marker. The acknowledgement keeps the
    /// connection open, and the death follows its close.
    #[tokio::test]
    async fn a_drain_is_acknowledged_once_every_announced_stream_has_ended() {
        let endpoint = formed(1);
        let mut leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let node = leaver.node;
        let mut recipient = Recipient::watching(endpoint, node, ActorId::new());
        leaver.send(&departure(node));
        eventually(|| endpoint.departure_deferred(node)).await;

        let early = leaver.open_stream();
        early.send(&message(endpoint, recipient.id(), 1));
        drop(early);
        let open = leaver.open_stream();
        open.send(&message(endpoint, recipient.id(), 2));
        recipient.receive(2).await;
        leaver.send(&Frame::Drained { data_streams: 3 });
        let late = leaver.open_stream();
        late.send(&message(endpoint, recipient.id(), 3));
        recipient.receive(3).await;
        drop(open);

        sleep(Duration::from_millis(50)).await;
        assert!(!leaver.drain_acked(), "the late stream is still open");
        drop(late);
        leaver.drain_ack().await;
        assert!(endpoint.departure_deferred(node));
        assert!(endpoint.inbound_open(node));
        assert_eq!(recipient.received(), [Some(1), Some(2), Some(3)]);

        drop(leaver.close());
        eventually(|| !endpoint.departure_deferred(node)).await;
        assert_eq!(recipient.received(), [Some(1), Some(2), Some(3), None]);
    }

    /// A departed peer without data streams announces none, and its marker is acknowledged at
    /// once.
    #[tokio::test]
    async fn a_drain_without_data_streams_is_acknowledged_at_once() {
        let endpoint = formed(1);
        let mut leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let node = leaver.node;
        let mut recipient = Recipient::watching(endpoint, node, ActorId::new());
        leaver.send(&departure(node));
        leaver.send(&Frame::Drained { data_streams: 0 });

        leaver.drain_ack().await;
        assert!(endpoint.departure_deferred(node));
        assert!(recipient.received().is_empty());

        drop(leaver.close());
        eventually(|| !endpoint.departure_deferred(node)).await;
        assert_eq!(recipient.received(), [None]);
    }

    /// A departed peer closing its connection before all the data streams its marker announces
    /// have ended is never acknowledged; its death still follows the close, behind what its
    /// streams carried.
    #[tokio::test]
    async fn a_departure_closed_before_its_drain_completes_is_never_acknowledged() {
        let endpoint = formed(1);
        let leaver = InboundPeer::admitted_by(endpoint, 2).await;
        let node = leaver.node;
        let mut recipient = Recipient::watching(endpoint, node, ActorId::new());
        leaver.send(&departure(node));
        let stream = leaver.open_stream();
        stream.send(&message(endpoint, recipient.id(), 7));
        leaver.send(&Frame::Drained { data_streams: 2 });
        drop(stream);

        let mut from_acceptor = leaver.close();
        eventually(|| !endpoint.inbound_open(node)).await;
        assert!(!endpoint.departure_deferred(node));
        assert_eq!(recipient.received(), [Some(7), None]);
        assert!(!drain_acked(&mut from_acceptor));
    }

    /// A drain frame outside a deferred departure breaks the protocol: a marker from a member
    /// which has not departed, or an acknowledgement on an accepted connection, closes the
    /// connection unanswered and leaves the member Up.
    #[tokio::test]
    async fn a_drain_frame_outside_a_departure_closes_the_connection() {
        let endpoint = formed(1);

        for (port, frame) in [
            (2, Frame::Drained { data_streams: 0 }),
            (3, Frame::DrainAck),
        ] {
            let mut member = InboundPeer::admitted_by(endpoint, port).await;
            member.send(&frame);

            eventually(|| !endpoint.inbound_open(member.node)).await;
            assert!(endpoint.membership().is_up(member.node));
            assert!(!member.drain_acked());
        }
    }

    /// A reconnect resets the member list version recorded for the peer, so the next tick sends
    /// the full list even though the lost connection had carried it.
    #[tokio::test]
    async fn a_reconnect_resets_the_gossiped_version() {
        let (endpoint, lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let member = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(member));
        let (dial_tx, mut dials) = unbounded_channel();
        task::spawn(dial_loop(
            Arc::new(ScriptedTransport(dial_tx)),
            lane_dials,
            endpoint,
        ));
        endpoint
            .send(
                member,
                Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                },
            )
            .expect("the lane opens");

        let mut acceptor = accepted_lane(endpoint, &mut dials, member).await;
        endpoint.gossip_tick();
        assert!(!next_gossip(&mut acceptor).await.is_empty());
        endpoint.gossip_tick();
        assert!(next_gossip(&mut acceptor).await.is_empty());

        drop(acceptor);
        endpoint.gossip_tick();
        let mut acceptor = accepted_lane(endpoint, &mut dials, member).await;
        endpoint.gossip_tick();
        assert!(!next_gossip(&mut acceptor).await.is_empty());
    }

    /// Answers the lane's next dial as `member` and returns once the setup snapshot arrived.
    async fn accepted_lane(
        endpoint: &EndpointInner,
        dials: &mut UnboundedReceiver<Dial>,
        member: NodeId,
    ) -> Acceptor {
        let dial = timeout(Duration::from_secs(5), dials.recv())
            .await
            .expect("the lane dials in time")
            .expect("the lane dials");
        let (connected, mut acceptor) = Acceptor::connected();
        acceptor.cluster = endpoint.committed_cluster().expect("a formed cluster");
        assert!(
            dial.answer.send(connected).is_ok(),
            "the lane awaits the dial"
        );
        acceptor.joiner_handshake().await;
        acceptor.send(&Frame::Handshake(Handshake::new(HandshakeV1 {
            node: member,
            intent: HandshakeIntent::Member,
            cluster: Some(acceptor.cluster),
        })));
        while next_gossip(&mut acceptor).await.is_empty() {}
        acceptor
    }

    /// The members of the next Gossip frame the lane writes, skipping every other frame.
    async fn next_gossip(acceptor: &mut Acceptor) -> Vec<Member> {
        timeout(Duration::from_secs(5), async {
            loop {
                let bytes = acceptor.joiner_bytes().await;
                if let Frame::Gossip { members, .. } =
                    Frame::from_bytes(&bytes).expect("frame decodes")
                {
                    return members;
                }
            }
        })
        .await
        .expect("a gossip frame in time")
    }

    /// The test's end of a leaving endpoint's lane to one Up member, connected and admitted.
    struct LeavingLane {
        endpoint: &'static EndpointInner,
        acceptor: Acceptor,
        data_streams: Vec<ChannelReceiver>,
    }

    impl LeavingLane {
        async fn connected(data_streams: usize) -> LeavingLane {
            let (endpoint, lane_dials, _join_requests) = EndpointInner::for_tests_with_data_streams(
                EndpointConfig::new(addr(1)),
                data_streams,
            );
            endpoint.form_cluster().expect("an unformed endpoint forms");
            let member = NodeId::new(addr(2));
            endpoint.update(|membership, _| membership.add_up(member));
            let (dial_tx, mut dials) = unbounded_channel();
            task::spawn(dial_loop(
                Arc::new(ScriptedTransport(dial_tx)),
                lane_dials,
                endpoint,
            ));

            endpoint
                .send(
                    member,
                    Frame::Gossip {
                        members: Vec::new(),
                        more: false,
                    },
                )
                .expect("the lane opens");
            let dial = timeout(Duration::from_secs(5), dials.recv())
                .await
                .expect("the lane dials in time")
                .expect("the lane dials");
            let (connected, mut acceptor) = Acceptor::connected();
            acceptor.cluster = endpoint.committed_cluster().expect("a formed cluster");
            assert!(
                dial.answer.send(connected).is_ok(),
                "the lane awaits the dial"
            );
            acceptor.joiner_handshake().await;
            acceptor.send(&Frame::Handshake(Handshake::new(HandshakeV1 {
                node: member,
                intent: HandshakeIntent::Member,
                cluster: Some(acceptor.cluster),
            })));

            let mut opened = Vec::new();
            for _ in 0..data_streams {
                opened.push(
                    timeout(Duration::from_secs(5), acceptor.data_streams.recv())
                        .await
                        .expect("the data streams open in time")
                        .expect("the data streams open"),
                );
            }
            eventually(|| endpoint.lane_connected(addr(2)) == Some(true)).await;
            LeavingLane {
                endpoint,
                acceptor,
                data_streams: opened,
            }
        }

        /// The number of data streams the drain marker announces, skipping the frames before it.
        async fn drained(&mut self) -> u32 {
            timeout(Duration::from_secs(5), async {
                loop {
                    let bytes = self.acceptor.joiner_bytes().await;
                    if let Frame::Drained { data_streams } =
                        Frame::from_bytes(&bytes).expect("frame decodes")
                    {
                        return data_streams;
                    }
                }
            })
            .await
            .expect("the drain marker arrives in time")
        }
    }

    /// A leaving endpoint finishes every data stream, announces their number in the drain marker
    /// and reports the member acknowledged once it answers the marker.
    #[tokio::test]
    async fn a_departure_is_acknowledged_by_the_answer_to_its_drain_marker() {
        let mut lane = LeavingLane::connected(2).await;
        let endpoint = lane.endpoint;
        let departure = task::spawn(endpoint.leave());

        assert_eq!(lane.drained().await, 2);
        for data_stream in &mut lane.data_streams {
            let finished = timeout(Duration::from_secs(5), data_stream.recv())
                .await
                .expect("the data stream ends in time");
            assert!(
                matches!(finished, Ok(None)),
                "every data stream is finished"
            );
        }
        lane.acceptor.send(&Frame::DrainAck);

        let departure = timeout(Duration::from_secs(5), departure)
            .await
            .expect("the departure ends in time")
            .expect("the departure does not panic")
            .expect("a departure");
        assert_eq!(departure.acknowledged(), [addr(2)]);
        assert!(departure.is_acknowledged());
    }

    /// A member closing the connection after the drain marker without answering it is reported
    /// unacknowledged, and so is a lane without data streams, whose marker announces none.
    #[tokio::test]
    async fn a_departure_closed_before_the_answer_is_unacknowledged() {
        let mut lane = LeavingLane::connected(0).await;
        let endpoint = lane.endpoint;
        let departure = task::spawn(endpoint.leave());

        assert_eq!(lane.drained().await, 0);
        drop(lane.acceptor);

        let departure = timeout(Duration::from_secs(5), departure)
            .await
            .expect("the departure ends in time")
            .expect("the departure does not panic")
            .expect("a departure");
        assert_eq!(departure.unacknowledged(), [addr(2)]);
        assert!(departure.acknowledged().is_empty());
        assert!(!departure.is_acknowledged());
    }

    /// A lane waiting out a reconnect backoff when its endpoint begins to leave dials again at
    /// once, so a backoff longer than the leave timeout cannot leave a healthy member
    /// unacknowledged.
    #[tokio::test(start_paused = true)]
    async fn a_lane_in_backoff_dials_again_when_its_endpoint_leaves() {
        let mut config = EndpointConfig::new(addr(1));
        config.reconnect_backoff =
            Backoff::new(Duration::from_secs(10), Duration::from_secs(10)).expect("ordered bounds");
        let (endpoint, lane_dials, _join_requests) =
            EndpointInner::for_tests_with_data_streams(config, 0);
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let member = NodeId::new(addr(2));
        endpoint.update(|membership, _| membership.add_up(member));
        let (dial_tx, mut dials) = unbounded_channel();
        task::spawn(dial_loop(
            Arc::new(ScriptedTransport(dial_tx)),
            lane_dials,
            endpoint,
        ));
        endpoint
            .send(
                member,
                Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                },
            )
            .expect("the lane opens");
        let failed = dials.recv().await.expect("the lane dials");
        drop(failed.answer);
        sleep(Duration::from_millis(1)).await;

        let departure = task::spawn(endpoint.leave());
        let dial = timeout(endpoint.config().leave_timeout, dials.recv())
            .await
            .expect("the lane dials again within the leave timeout")
            .expect("the lane dials");
        let (connected, mut acceptor) = Acceptor::connected();
        acceptor.cluster = endpoint.committed_cluster().expect("a formed cluster");
        assert!(
            dial.answer.send(connected).is_ok(),
            "the lane awaits the dial"
        );
        acceptor.joiner_handshake().await;
        acceptor.send(&Frame::Handshake(Handshake::new(HandshakeV1 {
            node: member,
            intent: HandshakeIntent::Member,
            cluster: Some(acceptor.cluster),
        })));
        timeout(Duration::from_secs(5), async {
            loop {
                let bytes = acceptor.joiner_bytes().await;
                if matches!(
                    Frame::from_bytes(&bytes).expect("frame decodes"),
                    Frame::Drained { .. }
                ) {
                    return;
                }
            }
        })
        .await
        .expect("the drain marker arrives in time");
        acceptor.send(&Frame::DrainAck);

        let departure = timeout(endpoint.config().leave_timeout, departure)
            .await
            .expect("the departure ends in time")
            .expect("the departure does not panic")
            .expect("a departure");
        assert_eq!(departure.acknowledged(), [addr(2)]);
    }
}
