use crate::{
    ActorId, ActorRef, ReplyTo, SchemaVersion,
    cluster::{
        codec::{Codec, CodecError},
        discovery::Nonce,
        endpoint::{self, EndpointInner},
        frame::{Frame, StreamKey},
        node::NodeId,
        schema::{self, Schema},
        sink::{RemoteSendError, encode_and_send},
    },
    request_response::SendReply,
    sync::lock,
    watch::{ActorTerminated, TerminatedHandler, Watcher, WatcherRegistry},
};
use derive_more::Debug;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser};
use std::{
    any::{Any, type_name},
    borrow::Cow,
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak, atomic::AtomicU64},
};
use thiserror::Error;
use tracing::{debug, warn};

type DeliverReply = fn(Box<dyn Any + Send>, SchemaVersion, &[u8], Codec) -> Result<(), CodecError>;

thread_local! {
    static MINTED_TAGS: RefCell<Option<Vec<ReplyTag>>> = const { RefCell::new(None) };
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<R> Serialize for ReplyTo<R>
where
    R: Schema + Send + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(endpoint) = endpoint::get() else {
            return Err(ser::Error::custom(ReplyToError::EndpointNotStarted));
        };

        let Some(send_reply) = self.take_send_reply() else {
            return Err(ser::Error::custom(ReplyToError::AlreadySerialized));
        };

        let wire = self.wire();
        let nonce = endpoint.pending_replies().add(
            Box::new(send_reply),
            deliver_reply::<R>,
            wire.recipient,
            wire.recipient_watchers.as_ref(),
            wire.proxy_origin,
            wire.interest.clone(),
        );
        note_minted(ReplyTag {
            nonce,
            recipient: wire.recipient,
        });

        WireReplyTo {
            node: endpoint.node(),
            nonce,
            recipient: wire.recipient,
        }
        .serialize(serializer)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<'de, R> Deserialize<'de> for ReplyTo<R>
where
    R: Schema + Send + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let WireReplyTo {
            node,
            nonce,
            recipient,
        } = WireReplyTo::deserialize(deserializer)?;

        let Some(endpoint) = endpoint::get() else {
            return Err(de::Error::custom(ReplyToError::EndpointNotStarted));
        };

        if node == endpoint.node() {
            let Some(pending) = endpoint.pending_replies().take(nonce) else {
                return Err(de::Error::custom(ReplyToError::NoPendingReply));
            };
            let send_reply = pending
                .send_reply
                .downcast::<SendReply<R>>()
                .map_err(|_| de::Error::custom(ReplyToError::ReplyType))?;
            let recipient_watchers =
                recipient.and_then(|recipient| endpoint.registry().watcher_registry(recipient));
            Ok(ReplyTo::new(*send_reply).with_wire(ReplyToWire {
                recipient,
                recipient_watchers,
                proxy_origin: pending.forwards,
                interest: pending.interest,
            }))
        } else {
            Ok(remote_proxy(
                endpoint.pending_replies(),
                node,
                nonce,
                recipient,
            ))
        }
    }
}

/// A reply destination which cannot be serialized or resolved; rendered into the serde error.
#[derive(Debug, Error)]
pub(crate) enum ReplyToError {
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    #[error("reply destination already serialized")]
    AlreadySerialized,

    #[error("no pending reply destination under this nonce")]
    NoPendingReply,

    #[error("reply destination of another reply type")]
    ReplyType,
}

/// Dropping an entry resolves its ask as `NoReply`; otherwise only the ask timeout resolves it.
pub(crate) struct PendingReplies {
    next_nonce: AtomicU64,
    evictor_id: ActorId,
    pending: Mutex<HashMap<Nonce, PendingReply>>,
    proxies: Mutex<HashMap<ProxyOrigin, Arc<()>>>,
}

impl PendingReplies {
    pub(crate) fn new() -> Self {
        Self {
            next_nonce: AtomicU64::new(0),
            evictor_id: ActorId::new(),
            pending: Mutex::new(HashMap::new()),
            proxies: Mutex::new(HashMap::new()),
        }
    }

    /// Stamping arms an entry's eviction on that node's death or its lane's give-up, and holds back
    /// its cancellation until [PendingReplies::confirm]; a second stamp replaces the destination.
    /// The returned cancellations must be sent after the request, on its stream.
    pub(crate) fn stamp(
        &self,
        tags: &[ReplyTag],
        peer: NodeId,
        stream: Option<StreamKey>,
    ) -> Vec<Cancellation> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut cancellations = Vec::new();
        let mut released = Vec::new();
        {
            let mut pending = lock(&self.pending);
            for tag in tags {
                if let Some(entry) = pending.get_mut(&tag.nonce)
                    && !entry.is_abandoned()
                {
                    entry.dispatch = Dispatch::InFlight(Destination { peer, stream });
                    continue;
                }
                released.extend(pending.remove(&tag.nonce));
                cancellations.push(Cancellation {
                    peer,
                    nonce: tag.nonce,
                    stream,
                });
            }
        }
        drop(released);

        cancellations
    }

    // Only once the request is queued; the returned cancellations must be sent after it.
    pub(crate) fn confirm(&self, tags: &[ReplyTag]) -> Vec<Cancellation> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut cancellations = Vec::new();
        let mut released = Vec::new();
        {
            let mut pending = lock(&self.pending);
            for tag in tags {
                let Entry::Occupied(mut occupied) = pending.entry(tag.nonce) else {
                    continue;
                };
                match occupied.get().dispatch {
                    Dispatch::InFlight(destination) => {
                        occupied.get_mut().dispatch = Dispatch::Sent(destination)
                    }

                    Dispatch::CanceledInFlight(_) => {
                        let entry = occupied.remove();
                        cancellations.extend(entry.cancellation(tag.nonce));
                        released.push(entry);
                    }

                    Dispatch::Unsent | Dispatch::Sent(_) => {}
                }
            }
        }
        drop(released);

        cancellations
    }

    pub(crate) fn discard(&self, tags: &[ReplyTag]) {
        if tags.is_empty() {
            return;
        }

        let mut pending = lock(&self.pending);
        let released = tags
            .iter()
            .filter_map(|tag| pending.remove(&tag.nonce))
            .collect::<Vec<_>>();
        drop(pending);
        drop(released);
    }

    pub(crate) fn discard_ask(&self, tags: &[ReplyTag]) -> Vec<Cancellation> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut pending = lock(&self.pending);
        let (released, cancellations) = release(
            &mut pending,
            tags.iter()
                .filter(|tag| tag.recipient.is_none())
                .map(|tag| tag.nonce),
        );
        drop(pending);
        drop(released);

        cancellations
    }

    /// An actor-origin request has no timeout, so its entry must not outlive its recipient.
    pub(crate) fn fail_recipient(&self, recipient: ActorId) -> Vec<Cancellation> {
        self.release_where(|entry| entry.recipient == Some(recipient))
    }

    pub(crate) fn fail_addr(&self, addr: SocketAddr) {
        drop(self.take_where(|entry| {
            entry
                .dispatch
                .peer()
                .is_some_and(|peer| peer.addr() == addr)
                || entry
                    .forwards
                    .is_some_and(|origin| origin.node.addr() == addr)
        }));
    }

    pub(crate) fn fail_fenced(&self, fence: NodeId) {
        drop(self.take_where(|entry| {
            entry.dispatch.peer().is_some_and(|peer| fence.covers(peer))
                || entry
                    .forwards
                    .is_some_and(|origin| fence.covers(origin.node))
        }));
    }

    pub(crate) fn release_abandoned(&self) -> Vec<Cancellation> {
        self.release_where(PendingReply::is_abandoned)
    }

    pub(crate) fn cancel_proxy(&self, origin: ProxyOrigin) -> Vec<Cancellation> {
        self.forget_proxy(origin);
        self.release_abandoned()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        lock(&self.pending).len()
    }

    /// The eviction is registered after the insert and never under the pending lock the sink takes.
    fn add(
        &self,
        send_reply: Box<dyn Any + Send>,
        deliver: DeliverReply,
        recipient: Option<ActorId>,
        recipient_watchers: Option<&WatcherRegistry>,
        forwards: Option<ProxyOrigin>,
        interest: Option<Weak<()>>,
    ) -> Nonce {
        let nonce = Nonce::mint(&self.next_nonce);

        lock(&self.pending).insert(
            nonce,
            PendingReply {
                dispatch: Dispatch::Unsent,
                recipient,
                forwards,
                interest,
                send_reply,
                deliver,
            },
        );

        if let Some(watchers) = recipient_watchers
            && watchers
                .add(Watcher::new(self.evictor_id, Arc::new(RecipientEvictor)))
                .is_err()
        {
            let reverted = lock(&self.pending).remove(&nonce);
            drop(reverted);
        }

        nonce
    }

    fn take(&self, nonce: Nonce) -> Option<PendingReply> {
        lock(&self.pending).remove(&nonce)
    }

    fn proxy_interest(&self, origin: ProxyOrigin) -> Weak<()> {
        Arc::downgrade(
            lock(&self.proxies)
                .entry(origin)
                .or_insert_with(|| Arc::new(())),
        )
    }

    fn forget_proxy(&self, origin: ProxyOrigin) {
        let interest = lock(&self.proxies).remove(&origin);
        drop(interest);
    }

    fn take_where<F>(&self, mut matches: F) -> Vec<(Nonce, PendingReply)>
    where
        F: FnMut(&PendingReply) -> bool,
    {
        let mut pending = lock(&self.pending);
        let nonces = pending
            .iter()
            .filter(|(_, entry)| matches(entry))
            .map(|(nonce, _)| *nonce)
            .collect::<Vec<_>>();
        nonces
            .into_iter()
            .filter_map(|nonce| pending.remove(&nonce).map(|entry| (nonce, entry)))
            .collect()
    }

    fn release_where<F>(&self, mut matches: F) -> Vec<Cancellation>
    where
        F: FnMut(&PendingReply) -> bool,
    {
        let mut pending = lock(&self.pending);
        let nonces = pending
            .iter()
            .filter(|(_, entry)| {
                !matches!(entry.dispatch, Dispatch::CanceledInFlight(_)) && matches(entry)
            })
            .map(|(nonce, _)| *nonce)
            .collect::<Vec<_>>();
        let (released, cancellations) = release(&mut pending, nonces);
        drop(pending);
        drop(released);

        cancellations
    }

    fn take_from(&self, nonce: Nonce, peer: NodeId) -> Option<PendingReply> {
        let mut pending = lock(&self.pending);

        pending
            .get(&nonce)
            .is_some_and(|entry| entry.dispatch.peer() == Some(peer))
            .then(|| pending.remove(&nonce))
            .flatten()
    }
}

/// Repeated in the frame, so a node dead-lettering the message undecoded can still answer each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplyTag {
    pub(crate) nonce: Nonce,
    pub(crate) recipient: Option<ActorId>,
}

#[derive(Debug, Default)]
pub(crate) struct ReplyToWire {
    pub(crate) recipient: Option<ActorId>,

    #[debug(skip)]
    pub(crate) recipient_watchers: Option<WatcherRegistry>,

    pub(crate) proxy_origin: Option<ProxyOrigin>,

    #[debug(skip)]
    pub(crate) interest: Option<Weak<()>>,
}

impl ReplyToWire {
    pub(crate) fn for_recipient<M>(recipient: &ActorRef<M>) -> Self {
        Self {
            recipient: Some(recipient.actor_id()),
            recipient_watchers: recipient.watcher_registry().cloned(),
            ..Self::default()
        }
    }
}

pub(crate) struct AskGuard {
    reply_tags: Option<AskReplyTags<'static>>,
    _interest: Arc<()>,
}

impl AskGuard {
    pub(crate) fn attach<R>(reply_to: ReplyTo<R>) -> (Self, ReplyTo<R>) {
        let interest = Arc::new(());
        let reply_to = reply_to.with_wire(ReplyToWire {
            interest: Some(Arc::downgrade(&interest)),
            ..ReplyToWire::default()
        });
        let guard = Self {
            reply_tags: None,
            _interest: interest,
        };
        (guard, reply_to)
    }

    pub(crate) fn sent(&mut self, tags: Vec<ReplyTag>) {
        self.reply_tags =
            endpoint::get().map(|endpoint| AskReplyTags::new(endpoint.pending_replies(), tags));
    }

    pub(crate) fn disarm(&mut self) {
        if let Some(reply_tags) = &mut self.reply_tags {
            reply_tags.disarm();
        }
    }
}

/// Releases the ask's own reply destinations on drop, however the ask ends.
pub(crate) struct AskReplyTags<'a> {
    pending: &'a PendingReplies,
    tags: Vec<ReplyTag>,
}

impl<'a> AskReplyTags<'a> {
    pub(crate) fn new(pending: &'a PendingReplies, tags: Vec<ReplyTag>) -> Self {
        Self { pending, tags }
    }

    /// Only once the ask resolved: its entry was taken or dropped with it, nothing is left.
    pub(crate) fn disarm(&mut self) {
        self.tags.clear();
    }
}

impl Drop for AskReplyTags<'_> {
    fn drop(&mut self) {
        let cancellations = self.pending.discard_ask(&self.tags);
        if let Some(endpoint) = endpoint::get() {
            send_cancellations(endpoint, cancellations);
        }
    }
}

/// Runs `f` recording every reply destination minted on this thread, so a send site learns them.
pub(crate) fn record_minted<T, F>(f: F) -> (T, Vec<ReplyTag>)
where
    F: FnOnce() -> T,
{
    /// Must restore the outer recording even if `f` unwinds!
    struct Restore {
        outer: Option<Vec<ReplyTag>>,
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            MINTED_TAGS.set(self.outer.take());
        }
    }

    let restore = Restore {
        outer: MINTED_TAGS.replace(Some(Vec::new())),
    };
    let value = f();
    let tags = MINTED_TAGS.replace(None).unwrap_or_default();
    drop(restore);

    (value, tags)
}

/// Called once a request's frame is known never to reach its peer; each entry holds a sink.
pub(crate) fn discard_replies(tags: &[ReplyTag]) {
    if let Some(endpoint) = endpoint::get() {
        endpoint.pending_replies().discard(tags);
    }
}

/// Only the peer a request was sent to may answer it: a nonce is unique to this node only.
pub(crate) fn on_reply(
    endpoint: &EndpointInner,
    peer: NodeId,
    nonce: Nonce,
    schema_version: SchemaVersion,
    payload: &[u8],
) {
    match endpoint.pending_replies().take_from(nonce, peer) {
        Some(pending) => {
            let delivered = (pending.deliver)(
                pending.send_reply,
                schema_version,
                payload,
                endpoint.codec(),
            );
            if let Err(error) = delivered {
                warn!(%peer, %nonce, %error, source = error.source(), "dead letter");
            }
        }

        None => warn!(
            %peer,
            %nonce,
            error = "no pending reply destination this peer was asked under this nonce",
            "dead letter"
        ),
    }
}

/// Dropping the entry resolves its ask as `NoReply`, hence only its peer may drop it.
pub(crate) fn on_reply_dropped(endpoint: &EndpointInner, peer: NodeId, nonce: Nonce) {
    endpoint.pending_replies().take_from(nonce, peer);
}

pub(crate) fn on_reply_canceled(endpoint: &EndpointInner, origin: NodeId, nonce: Nonce) {
    send_cancellations(
        endpoint,
        endpoint.pending_replies().cancel_proxy(ProxyOrigin {
            node: origin,
            nonce,
        }),
    );
}

pub(crate) fn send_cancellations(endpoint: &EndpointInner, cancellations: Vec<Cancellation>) {
    for Cancellation {
        peer,
        nonce,
        stream,
    } in cancellations
    {
        if let Err(error) = endpoint.send(peer, Frame::ReplyCanceled { nonce, stream }) {
            debug!(%peer, %nonce, %error, "cannot send reply cancellation");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Cancellation {
    pub(crate) peer: NodeId,
    pub(crate) nonce: Nonce,
    pub(crate) stream: Option<StreamKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ProxyOrigin {
    node: NodeId,
    nonce: Nonce,
}

/// For a message dead-lettered undecoded only, and tolerant of a repeated notification.
pub(crate) fn on_undeliverable(endpoint: &EndpointInner, peer: NodeId, reply_tags: &[ReplyTag]) {
    for tag in reply_tags {
        let frame = Frame::ReplyDropped {
            nonce: tag.nonce,
            recipient: tag.recipient,
        };
        if let Err(error) = endpoint.send(peer, frame) {
            debug!(%peer, nonce = %tag.nonce, %error, "cannot send reply-dropped notification");
        }
    }
}

struct PendingReply {
    dispatch: Dispatch,
    recipient: Option<ActorId>,
    forwards: Option<ProxyOrigin>,
    interest: Option<Weak<()>>,
    send_reply: Box<dyn Any + Send>,
    deliver: DeliverReply,
}

impl PendingReply {
    fn is_abandoned(&self) -> bool {
        self.interest
            .as_ref()
            .is_some_and(|interest| interest.strong_count() == 0)
    }

    fn cancellation(&self, nonce: Nonce) -> Option<Cancellation> {
        self.dispatch
            .destination()
            .map(|Destination { peer, stream }| Cancellation {
                peer,
                nonce,
                stream,
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dispatch {
    Unsent,
    InFlight(Destination),
    CanceledInFlight(Destination),
    Sent(Destination),
}

impl Dispatch {
    fn destination(self) -> Option<Destination> {
        match self {
            Dispatch::Unsent => None,

            Dispatch::InFlight(destination)
            | Dispatch::CanceledInFlight(destination)
            | Dispatch::Sent(destination) => Some(destination),
        }
    }

    fn peer(self) -> Option<NodeId> {
        self.destination().map(|destination| destination.peer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Destination {
    peer: NodeId,
    stream: Option<StreamKey>,
}

/// Registered on the actor a reply is delivered to, hence the signaled ID is the one to evict.
struct RecipientEvictor;

impl TerminatedHandler for RecipientEvictor {
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        if let Some(endpoint) = endpoint::get() {
            send_cancellations(
                endpoint,
                endpoint.pending_replies().fail_recipient(actor_id),
            );
        }

        Ok(())
    }
}

/// Notifies from `drop` unless the proxy sent its reply.
struct ReplyGuard {
    origin: ProxyOrigin,
    recipient: Option<ActorId>,
    replied: bool,
}

impl Drop for ReplyGuard {
    fn drop(&mut self) {
        let Some(endpoint) = endpoint::get() else {
            return;
        };

        endpoint.pending_replies().forget_proxy(self.origin);
        if self.replied {
            return;
        }

        let frame = Frame::ReplyDropped {
            nonce: self.origin.nonce,
            recipient: self.recipient,
        };
        if let Err(error) = endpoint.send(self.origin.node, frame) {
            debug!(
                origin = %self.origin.node,
                nonce = %self.origin.nonce,
                %error,
                "cannot send reply-dropped notification"
            );
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireReplyTo {
    node: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
}

fn deliver_reply<R>(
    send_reply: Box<dyn Any + Send>,
    schema_version: SchemaVersion,
    payload: &[u8],
    codec: Codec,
) -> Result<(), CodecError>
where
    R: Schema + Send + 'static,
{
    let send_reply = send_reply
        .downcast::<SendReply<R>>()
        .expect("the entry was added with this reply type's sink");
    let reply = schema::catch_panic_and_log::<R, _, _>(
        "reply decode panicked",
        CodecError::decoding,
        || R::decode(codec, schema_version, payload),
    )?;
    send_reply(reply);
    Ok(())
}

// An entry in flight is only marked, since its cancellation must not overtake its request.
fn release<I>(
    pending: &mut HashMap<Nonce, PendingReply>,
    nonces: I,
) -> (Vec<PendingReply>, Vec<Cancellation>)
where
    I: IntoIterator<Item = Nonce>,
{
    let mut released = Vec::new();
    let mut cancellations = Vec::new();
    for nonce in nonces {
        let Entry::Occupied(mut occupied) = pending.entry(nonce) else {
            continue;
        };
        match occupied.get().dispatch {
            Dispatch::InFlight(destination) => {
                occupied.get_mut().dispatch = Dispatch::CanceledInFlight(destination)
            }

            Dispatch::CanceledInFlight(_) => {}

            Dispatch::Unsent | Dispatch::Sent(_) => {
                let entry = occupied.remove();
                cancellations.extend(entry.cancellation(nonce));
                released.push(entry);
            }
        }
    }

    (released, cancellations)
}

fn note_minted(tag: ReplyTag) {
    MINTED_TAGS.with_borrow_mut(|tags| {
        if let Some(tags) = tags {
            tags.push(tag);
        }
    });
}

fn remote_proxy<R>(
    pending: &PendingReplies,
    origin: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
) -> ReplyTo<R>
where
    R: Schema + Send + 'static,
{
    let proxy_origin = ProxyOrigin {
        node: origin,
        nonce,
    };
    let interest = pending.proxy_interest(proxy_origin);
    let guard = ReplyGuard {
        origin: proxy_origin,
        recipient,
        replied: false,
    };

    ReplyTo::new(move |reply| {
        let mut guard = guard;
        match send_reply_frame(&reply, origin, nonce, recipient) {
            Ok(()) => guard.replied = true,

            Err(error) => warn!(
                origin = %origin,
                recipient = ?recipient,
                reply_type = type_name::<R>(),
                %error,
                source = error.source(),
                "dead letter"
            ),
        }
    })
    .with_wire(ReplyToWire {
        recipient,
        recipient_watchers: None,
        proxy_origin: Some(proxy_origin),
        interest: Some(interest),
    })
}

fn send_reply_frame<R>(
    reply: &R,
    origin: NodeId,
    nonce: Nonce,
    recipient: Option<ActorId>,
) -> Result<(), RemoteSendError>
where
    R: Schema,
{
    let endpoint = endpoint::get().ok_or(RemoteSendError::EndpointNotStarted)?;
    let peer = endpoint.peer(origin);

    encode_and_send(
        endpoint,
        origin,
        || {
            schema::catch_panic_and_log::<R, _, _>(
                "reply encode panicked",
                CodecError::encoding,
                || reply.encode(endpoint.codec(), &peer),
            )
        },
        |schema_version, payload, _| Frame::Reply {
            nonce,
            recipient,
            schema_version,
            payload: Cow::Owned(payload),
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, MailboxCapacity, ReplyTo, SchemaVersion,
        cluster::{
            codec::{Codec, CodecError},
            discovery::Nonce,
            endpoint::{self, EndpointConfig, EndpointInner},
            frame::{Frame, StreamKey},
            node::NodeId,
            reply::{
                AskReplyTags, Cancellation, PendingReplies, ProxyOrigin, ReplyTag, WireReplyTo,
                note_minted, on_reply, on_reply_canceled, record_minted, remote_proxy,
            },
            schema::{Peer, Schema},
        },
        mailbox::make_mailbox,
    };
    use serde::{Deserialize, Serialize};
    use std::{
        any::Any,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc::{self, TryRecvError},
        },
    };

    /// The bytes the trait object codec this one replaced encoded a [WireReplyTo] with a recipient
    /// into, and one without.
    const REPLY_TO_PAYLOADS: [&[u8]; 2] = [
        &[
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 249, 19, 16, 1, 161, 219, 7, 101,
            218, 119, 87, 150, 37, 188, 242, 134, 204, 219, 104, 41, 1, 16, 1, 161, 219, 7, 101,
            218, 119, 87, 150, 37, 188, 243, 77, 39, 244, 205,
        ],
        &[
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 249, 19, 16, 1, 161, 219, 7, 101,
            218, 119, 87, 150, 37, 188, 242, 134, 204, 219, 104, 42, 0,
        ],
    ];

    fn add_entry(pending: &PendingReplies, recipient: Option<ActorId>) -> Nonce {
        pending.add(Box::new(()), deliver_nothing, recipient, None, None, None)
    }

    fn send_stamped(
        pending: &PendingReplies,
        tags: &[ReplyTag],
        peer: NodeId,
        stream: Option<StreamKey>,
    ) -> Vec<Cancellation> {
        let mut cancellations = pending.stamp(tags, peer, stream);
        cancellations.extend(pending.confirm(tags));
        cancellations
    }

    fn tag(nonce: Nonce) -> ReplyTag {
        ReplyTag {
            nonce,
            recipient: Some(ActorId::new()),
        }
    }

    /// An entry is taken exactly once; a second take under the same nonce finds nothing, which is
    /// also what makes a repeated reply-dropped notification for one entry a no-op.
    #[test]
    fn entries_are_taken_at_most_once() {
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);

        assert!(pending.take(nonce).is_some());
        assert!(pending.take(nonce).is_none());
    }

    fn add_forwarded(pending: &PendingReplies, origin: NodeId, nonce: Nonce) -> Nonce {
        let origin = ProxyOrigin {
            node: origin,
            nonce,
        };
        pending.add(
            Box::new(()),
            deliver_nothing,
            None,
            None,
            Some(origin),
            Some(pending.proxy_interest(origin)),
        )
    }

    #[test]
    fn a_released_ask_cancels_what_it_sent() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let pending = PendingReplies::new();
        let sent = add_entry(&pending, None);
        let unsent = add_entry(&pending, None);
        let actors = add_entry(&pending, Some(ActorId::new()));
        let ask_tag = |nonce| ReplyTag {
            nonce,
            recipient: None,
        };
        send_stamped(&pending, &[ask_tag(sent)], peer, None);

        let cancellations = pending.discard_ask(&[ask_tag(sent), ask_tag(unsent), tag(actors)]);

        assert_eq!(
            cancellations,
            vec![Cancellation {
                peer,
                nonce: sent,
                stream: None
            }]
        );
        assert!(pending.take(sent).is_none());
        assert!(pending.take(unsent).is_none());
        assert!(pending.take(actors).is_some());
    }

    #[test]
    fn a_terminated_recipient_cancels_what_it_sent() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let recipient = ActorId::new();
        let pending = PendingReplies::new();
        let sent = add_entry(&pending, Some(recipient));
        let other = add_entry(&pending, Some(ActorId::new()));
        send_stamped(&pending, &[tag(sent), tag(other)], peer, None);

        assert_eq!(
            pending.fail_recipient(recipient),
            vec![Cancellation {
                peer,
                nonce: sent,
                stream: None
            }]
        );
        assert!(pending.take(other).is_some());
    }

    #[test]
    fn a_cancellation_releases_what_its_proxy_forwarded() {
        let origin = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let other_origin = NodeId::new("127.0.0.1:5678".parse().expect("valid address"));
        let next = NodeId::new("127.0.0.1:9012".parse().expect("valid address"));
        let pending = PendingReplies::new();
        let origin_nonces = AtomicU64::new(0);
        let (nonce, other_nonce) = (Nonce::mint(&origin_nonces), Nonce::mint(&origin_nonces));
        let stream = Some(StreamKey::Actor(ActorId::new()));

        let forwarded = add_forwarded(&pending, origin, nonce);
        let unsent = add_forwarded(&pending, origin, nonce);
        let for_other_nonce = add_forwarded(&pending, origin, other_nonce);
        let for_other_origin = add_forwarded(&pending, other_origin, nonce);
        assert!(send_stamped(&pending, &[tag(forwarded)], next, stream).is_empty());

        assert_eq!(
            pending.cancel_proxy(ProxyOrigin {
                node: origin,
                nonce
            }),
            vec![Cancellation {
                peer: next,
                nonce: forwarded,
                stream
            }]
        );
        assert!(pending.take(forwarded).is_none());
        assert!(pending.take(unsent).is_none());
        assert!(pending.take(for_other_nonce).is_some());
        assert!(pending.take(for_other_origin).is_some());
    }

    #[test]
    fn a_proxy_forwarded_after_its_cancellation_cancels_the_forward() {
        let origin = ProxyOrigin {
            node: NodeId::new("127.0.0.1:1234".parse().expect("valid address")),
            nonce: Nonce::first(),
        };
        let next = NodeId::new("127.0.0.1:9012".parse().expect("valid address"));
        let pending = PendingReplies::new();
        let interest = pending.proxy_interest(origin);
        let stream = Some(StreamKey::Actor(ActorId::new()));

        assert!(pending.cancel_proxy(origin).is_empty());
        let forwarded = pending.add(
            Box::new(()),
            deliver_nothing,
            None,
            None,
            Some(origin),
            Some(interest),
        );

        assert_eq!(
            send_stamped(&pending, &[tag(forwarded)], next, stream),
            vec![Cancellation {
                peer: next,
                nonce: forwarded,
                stream
            }]
        );
        assert!(pending.take(forwarded).is_none());
    }

    #[test]
    fn a_cancellation_in_flight_waits_for_its_request() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let recipient = ActorId::new();
        let pending = PendingReplies::new();
        let interest = Arc::new(());
        let ask = pending.add(
            Box::new(()),
            deliver_nothing,
            None,
            None,
            None,
            Some(Arc::downgrade(&interest)),
        );
        let actors = add_entry(&pending, Some(recipient));
        let tags = [
            ReplyTag {
                nonce: ask,
                recipient: None,
            },
            ReplyTag {
                nonce: actors,
                recipient: Some(recipient),
            },
        ];
        let stream = Some(StreamKey::Actor(ActorId::new()));
        assert!(pending.stamp(&tags, peer, stream).is_empty());

        drop(interest);
        assert!(pending.release_abandoned().is_empty());
        assert!(pending.discard_ask(&tags).is_empty());
        assert!(pending.fail_recipient(recipient).is_empty());

        assert_eq!(
            pending.confirm(&tags),
            vec![
                Cancellation {
                    peer,
                    nonce: ask,
                    stream
                },
                Cancellation {
                    peer,
                    nonce: actors,
                    stream
                }
            ]
        );
        assert!(pending.take(ask).is_none());
        assert!(pending.take(actors).is_none());
    }

    #[test]
    fn an_abandoned_ask_is_released() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let pending = PendingReplies::new();
        let (ended, waiting) = (Arc::new(()), Arc::new(()));
        let add = |interest: &Arc<()>| {
            pending.add(
                Box::new(()),
                deliver_nothing,
                None,
                None,
                None,
                Some(Arc::downgrade(interest)),
            )
        };
        let sent = add(&ended);
        let unsent = add(&ended);
        let still_awaited = add(&waiting);
        let actors = add_entry(&pending, Some(ActorId::new()));
        send_stamped(&pending, &[tag(sent)], peer, None);
        drop(ended);

        assert_eq!(
            pending.release_abandoned(),
            vec![Cancellation {
                peer,
                nonce: sent,
                stream: None
            }]
        );
        assert!(pending.take(unsent).is_none());
        assert!(pending.take(still_awaited).is_some());
        assert!(pending.take(actors).is_some());
    }

    #[test]
    fn a_received_cancellation_is_passed_down_the_chain() {
        let (endpoint, dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let origin = NodeId::new("127.0.0.1:2".parse().expect("valid address"));
        let next = NodeId::new("127.0.0.1:3".parse().expect("valid address"));
        let origin_nonce = Nonce::first();
        let forwarded = add_forwarded(endpoint.pending_replies(), origin, origin_nonce);
        let stream = Some(StreamKey::Actor(ActorId::new()));
        send_stamped(endpoint.pending_replies(), &[tag(forwarded)], next, stream);

        on_reply_canceled(endpoint, next, origin_nonce);
        assert!(dial_requests.is_empty(), "only the origin may cancel");

        on_reply_canceled(endpoint, origin, origin_nonce);

        assert!(endpoint.pending_replies().take(forwarded).is_none());
        let lane = dial_requests.try_recv().expect("the next node is dialed");
        assert_eq!(lane.addr, next.addr());
        assert!(matches!(
            lane.control_rx.try_recv(),
            Ok(frame @ Frame::ReplyCanceled { nonce, .. })
                if nonce == forwarded && frame.stream_key() == stream
        ));
    }

    #[test]
    fn fail_fenced_evicts_what_was_forwarded_for_the_fenced_node() {
        let origin = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let pending = PendingReplies::new();
        let forwarded = add_forwarded(&pending, origin, Nonce::first());
        let unrelated = add_entry(&pending, None);

        pending.fail_fenced(origin);

        assert!(pending.take(forwarded).is_none());
        assert!(pending.take(unrelated).is_some());
    }

    /// An unstamped entry and a newer incarnation at the address survive the fence.
    #[test]
    fn fail_fenced_evicts_only_covered_stamps() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let peer = NodeId::new(addr);
        let other = NodeId::new(addr);
        let pending = PendingReplies::new();

        assert!(peer.incarnation() < other.incarnation());
        let stamped = add_entry(&pending, None);
        let other_stamped = add_entry(&pending, None);
        let unstamped = add_entry(&pending, None);
        send_stamped(&pending, &[tag(stamped)], peer, None);
        send_stamped(&pending, &[tag(other_stamped)], other, None);

        pending.fail_fenced(peer);

        assert!(pending.take(stamped).is_none());
        assert!(pending.take(other_stamped).is_some());
        assert!(pending.take(unstamped).is_some());
    }

    /// Giving up a lane evicts the entries stamped with any incarnation at its address, so their
    /// asks resolve as `NoReply` instead of waiting out their timeout; an unstamped entry and one
    /// stamped with another address survive.
    #[test]
    fn fail_addr_evicts_every_incarnation_at_the_address() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let other_addr = "127.0.0.1:5678".parse().expect("valid address");
        let pending = PendingReplies::new();

        let stamped = add_entry(&pending, None);
        let successor_stamped = add_entry(&pending, None);
        let other_stamped = add_entry(&pending, None);
        let unstamped = add_entry(&pending, None);
        send_stamped(&pending, &[tag(stamped)], NodeId::new(addr), None);
        send_stamped(&pending, &[tag(successor_stamped)], NodeId::new(addr), None);
        send_stamped(
            &pending,
            &[tag(other_stamped)],
            NodeId::new(other_addr),
            None,
        );

        pending.fail_addr(addr);

        assert!(pending.take(stamped).is_none());
        assert!(pending.take(successor_stamped).is_none());
        assert!(pending.take(other_stamped).is_some());
        assert!(pending.take(unstamped).is_some());
    }

    /// Only the peer a request was sent to settles its entry: a nonce is unique to this node, not
    /// to the member naming it, so another member's reply or reply-dropped frame finds nothing.
    #[test]
    fn only_the_asked_peer_settles_an_entry() {
        let asked = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let other = NodeId::new("127.0.0.1:5678".parse().expect("valid address"));
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);
        send_stamped(&pending, &[tag(nonce)], asked, None);

        assert!(pending.take_from(nonce, other).is_none());
        assert!(pending.take_from(nonce, asked).is_some());
    }

    /// An entry is stamped before its frame is sent, so an unstamped one was never asked of
    /// anybody and no peer can settle it.
    #[test]
    fn an_unstamped_entry_is_settled_by_nobody() {
        let peer = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let pending = PendingReplies::new();

        let nonce = add_entry(&pending, None);

        assert!(pending.take_from(nonce, peer).is_none());
        assert!(pending.take(nonce).is_some());
    }

    /// An actor-origin request has no timeout, so the entry is bounded by the life of the actor
    /// awaiting the reply; entries of another recipient and recipientless ones survive.
    #[test]
    fn fail_recipient_evicts_only_that_recipients_entries() {
        let (recipient, other) = (ActorId::new(), ActorId::new());
        let pending = PendingReplies::new();

        let of_recipient = add_entry(&pending, Some(recipient));
        let of_other = add_entry(&pending, Some(other));
        let of_nobody = add_entry(&pending, None);

        pending.fail_recipient(recipient);

        assert!(pending.take(of_recipient).is_none());
        assert!(pending.take(of_other).is_some());
        assert!(pending.take(of_nobody).is_some());
    }

    /// One eviction registration serves all of a recipient's entries: registering is idempotent
    /// per watcher ID, so a busy asker does not accumulate watchers on itself.
    #[test]
    fn entries_of_one_recipient_register_one_eviction() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let recipient = ActorId::new();
        let pending = PendingReplies::new();

        for _ in 0..3 {
            pending.add(
                Box::new(()),
                deliver_nothing,
                Some(recipient),
                Some(mailbox_handle.watcher_registry()),
                None,
                None,
            );
        }

        let (_incoming_rx, watchers) = mailbox.split();
        assert_eq!(watchers.close().len(), 1);
    }

    /// A recipient which terminated before the entry was added can never receive the reply, so the
    /// entry is reverted rather than left for the peer's death to collect.
    #[test]
    fn an_entry_for_a_terminated_recipient_is_reverted() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (_incoming_rx, watchers) = mailbox.split();
        watchers.close();
        let pending = PendingReplies::new();

        let nonce = pending.add(
            Box::new(()),
            deliver_nothing,
            Some(ActorId::new()),
            Some(mailbox_handle.watcher_registry()),
            None,
            None,
        );

        assert!(pending.take(nonce).is_none());
    }

    /// Discarding removes exactly the given entries, e.g. after a failed send.
    #[test]
    fn discard_removes_the_given_entries() {
        let pending = PendingReplies::new();

        let discarded = add_entry(&pending, None);
        let kept = add_entry(&pending, None);

        pending.discard(&[tag(discarded)]);

        assert!(pending.take(discarded).is_none());
        assert!(pending.take(kept).is_some());
    }

    /// An entry's sink is the application's, so its destructor may reach the table again. Hence a
    /// discarded entry is dropped only once the pending lock is released.
    #[test]
    fn a_discarded_entry_is_dropped_outside_the_pending_lock() {
        let pending = Arc::new(PendingReplies::new());
        let unlocked = Arc::new(AtomicBool::new(false));
        let nonce = pending.add(
            Box::new(LockProbe {
                pending: pending.clone(),
                unlocked: unlocked.clone(),
            }),
            deliver_nothing,
            None,
            None,
            None,
            None,
        );

        pending.discard(&[tag(nonce)]);

        assert!(unlocked.load(Ordering::Relaxed));
    }

    /// The same holds for an entry reverted because its recipient has terminated.
    #[test]
    fn a_reverted_entry_is_dropped_outside_the_pending_lock() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (_incoming_rx, watchers) = mailbox.split();
        watchers.close();
        let pending = Arc::new(PendingReplies::new());
        let unlocked = Arc::new(AtomicBool::new(false));

        pending.add(
            Box::new(LockProbe {
                pending: pending.clone(),
                unlocked: unlocked.clone(),
            }),
            deliver_nothing,
            Some(ActorId::new()),
            Some(mailbox_handle.watcher_registry()),
            None,
            None,
        );

        assert!(unlocked.load(Ordering::Relaxed));
    }

    /// Dropping the guard is what releases an ask's own destination, so a canceled ask costs
    /// its entry no longer than a timed out one.
    #[test]
    fn dropping_the_guard_releases_the_asks_own_destinations() {
        let pending = PendingReplies::new();

        let own = add_entry(&pending, None);
        let actor = ActorId::new();
        let actors = add_entry(&pending, Some(actor));

        drop(AskReplyTags::new(
            &pending,
            vec![
                ReplyTag {
                    nonce: own,
                    recipient: None,
                },
                ReplyTag {
                    nonce: actors,
                    recipient: Some(actor),
                },
            ],
        ));

        assert!(pending.take(own).is_none());
        assert!(pending.take(actors).is_some());
    }

    /// A resolved ask disarms its guard; the entry is gone already, so nothing is released.
    #[test]
    fn a_disarmed_guard_releases_nothing() {
        let pending = PendingReplies::new();

        let own = add_entry(&pending, None);
        let mut guard = AskReplyTags::new(
            &pending,
            vec![ReplyTag {
                nonce: own,
                recipient: None,
            }],
        );

        guard.disarm();
        drop(guard);

        assert!(pending.take(own).is_some());
    }

    /// A nested recording keeps its tags to itself and restores the outer one, so a send during
    /// an encode cannot corrupt the outer send's bookkeeping.
    #[test]
    fn record_minted_scopes_nest() {
        let ((), outer) = record_minted(|| {
            note_minted(tag(Nonce::first()));

            let ((), inner) = record_minted(|| note_minted(tag(Nonce::first())));
            assert_eq!(inner.len(), 1);

            note_minted(tag(Nonce::first()));
        });

        assert_eq!(outer.len(), 2);
    }

    /// A reply whose encode panics is dropped like one which cannot be encoded. The panic does not
    /// reach the caller. The destination minted before it is released, and the origin is told.
    #[test]
    fn a_panicking_reply_encode_drops_the_reply() {
        let (endpoint, dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let origin = NodeId::new("127.0.0.1:2".parse().expect("valid address"));
        let nonce = Nonce::first();
        let reply_to =
            remote_proxy::<MintsThenPanics>(endpoint.pending_replies(), origin, nonce, None);

        endpoint::with_current(endpoint, || {
            reply_to.reply(MintsThenPanics(ReplyTo::new(|_| ())));
        });

        assert_eq!(endpoint.pending_replies().len(), 0);
        let lane = dial_requests.try_recv().expect("the origin is dialed");
        assert_eq!(lane.addr, origin.addr());
        assert!(matches!(
            lane.control_rx.try_recv(),
            Ok(Frame::ReplyDropped { nonce: dropped, .. }) if dropped == nonce
        ));
        assert!(lane.control_rx.is_empty());
    }

    /// A node which forwards a reply encodes it again. A panic of that encode drops the reply too:
    /// it does not reach the caller of `on_reply`, and the origin is told.
    #[test]
    fn a_panicking_encode_of_a_forwarded_reply_drops_the_reply() {
        let (endpoint, dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let origin = NodeId::new("127.0.0.1:2".parse().expect("valid address"));
        let next = NodeId::new("127.0.0.1:3".parse().expect("valid address"));
        let origin_nonce = Nonce::first();
        let pending = endpoint.pending_replies();
        let reply_to = remote_proxy::<PanicsWhenEncoded>(pending, origin, origin_nonce, None);
        let (forwarded, tags) = endpoint::with_current(endpoint, || {
            record_minted(|| endpoint.codec().encode(&reply_to))
        });
        forwarded.expect("the proxy is serialized");
        send_stamped(pending, &tags, next, None);
        let payload = endpoint
            .codec()
            .encode(&PanicsWhenEncoded)
            .expect("the reply is serialized");

        endpoint::with_current(endpoint, || {
            on_reply(
                endpoint,
                next,
                tags[0].nonce,
                PanicsWhenEncoded::VERSION,
                &payload,
            );
        });

        assert_eq!(pending.len(), 0);
        let lane = dial_requests.try_recv().expect("the origin is dialed");
        assert_eq!(lane.addr, origin.addr());
        assert!(matches!(
            lane.control_rx.try_recv(),
            Ok(Frame::ReplyDropped { nonce, .. }) if nonce == origin_nonce
        ));
        assert!(lane.control_rx.is_empty());
    }

    /// A reply whose decode panics is a dead letter like one which does not decode: the panic does
    /// not reach the caller of `on_reply`, and the reply's destination is dropped unanswered.
    #[test]
    fn a_panicking_reply_decode_drops_the_destination() {
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(
            EndpointConfig::new("127.0.0.1:1".parse().expect("valid address")),
        );
        let responder = NodeId::new("127.0.0.1:2".parse().expect("valid address"));
        let pending = endpoint.pending_replies();
        let (reply_tx, reply_rx) = mpsc::channel();
        let reply_to = ReplyTo::<PanicsWhenDecoded>::new(move |reply| {
            reply_tx.send(reply).expect("the test awaits the reply");
        });
        let (request, tags) = endpoint::with_current(endpoint, || {
            record_minted(|| endpoint.codec().encode(&reply_to))
        });
        request.expect("the destination is serialized");
        send_stamped(pending, &tags, responder, None);
        let payload = endpoint
            .codec()
            .encode(&PanicsWhenDecoded)
            .expect("the reply is serialized");

        on_reply(
            endpoint,
            responder,
            tags[0].nonce,
            PanicsWhenDecoded::VERSION,
            &payload,
        );

        assert_eq!(pending.len(), 0);
        assert!(matches!(
            reply_rx.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }

    fn deliver_nothing(
        _send_reply: Box<dyn Any + Send>,
        _schema_version: SchemaVersion,
        _payload: &[u8],
        _codec: Codec,
    ) -> Result<(), CodecError> {
        Ok(())
    }

    struct LockProbe {
        pending: Arc<PendingReplies>,
        unlocked: Arc<AtomicBool>,
    }

    impl Drop for LockProbe {
        fn drop(&mut self) {
            let unlocked = self.pending.pending.try_lock().is_ok();
            self.unlocked.store(unlocked, Ordering::Relaxed);
        }
    }

    #[derive(Serialize, Deserialize)]
    struct MintsThenPanics(ReplyTo<u64>);

    impl Schema for MintsThenPanics {
        fn encode(&self, codec: Codec, _: &Peer) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
            codec.encode(self)?;
            panic!("reply encode bug")
        }
    }

    #[derive(Serialize, Deserialize)]
    struct PanicsWhenEncoded;

    impl Schema for PanicsWhenEncoded {
        fn encode(&self, _: Codec, _: &Peer) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
            panic!("reply encode bug")
        }
    }

    #[derive(Serialize, Deserialize)]
    struct PanicsWhenDecoded;

    impl Schema for PanicsWhenDecoded {
        fn decode(_: Codec, _: SchemaVersion, _: &[u8]) -> Result<Self, CodecError> {
            panic!("reply decode bug")
        }
    }

    /// A reply-to serialized by the trait object codec decodes here, with and without a
    /// recipient, and encodes back into the same bytes.
    #[test]
    fn a_wire_reply_to_matches_the_trait_object_codec() {
        for (payload, nonce) in REPLY_TO_PAYLOADS.into_iter().zip([41, 42]) {
            let reply_to = Codec::Postcard
                .decode::<WireReplyTo>(payload)
                .expect("decodes");

            assert_eq!(
                reply_to.node.addr(),
                "[::1]:2553".parse().expect("an address")
            );
            assert_eq!(reply_to.nonce.to_string(), nonce.to_string());
            assert_eq!(reply_to.recipient.is_some(), nonce == 41);
            assert_eq!(Codec::Postcard.encode(&reply_to).expect("encodes"), payload);
        }
    }
}
