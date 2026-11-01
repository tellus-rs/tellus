#[cfg(feature = "cluster")]
use crate::cluster;
use crate::{ActorContext, ActorRef, TimerMode, actor_ref::Sink, mailbox::SendError};
use derive_more::Debug;
use std::{any::type_name, cell::Cell, time::Duration};
use thiserror::Error;
use tokio::{sync::oneshot, time::timeout};
use tracing::warn;

pub(crate) type SendReply<R> = Box<dyn FnOnce(R) + Send>;

impl<M> ActorRef<M> {
    /// Send a request to the actor represented by this reference and await the reply for at most
    /// `within`. The given function builds the request message around a [ReplyTo] for the actor
    /// to [ReplyTo::reply] to.
    ///
    /// For code outside of any actor, e.g. alongside [ActorSystem::terminated]; inside an actor
    /// use [ActorContext::reply_to] instead of awaiting.
    ///
    /// Unlike [ActorRef::tell], failures are returned instead of only logged, since the caller is
    /// awaiting. The `NoReply` detection is best-effort, which is why the wait is bounded. Against
    /// e.g. a responder which keeps its [ReplyTo] alive without replying, the ask resolves as
    /// `Timeout`. A late reply is dropped and logged as a dead letter.
    ///
    /// With the `cluster` feature the reference can point to an actor on another node; the same
    /// contract holds, with the `NoReply` detection weakening as spelled out in the `cluster`
    /// module documentation. Dropping the future before it resolves, e.g. through an outer timeout,
    /// drops its reply receiver; for a remote request it also releases its pending-reply entry.
    ///
    /// # Errors
    /// Fails if the request cannot be sent: the actor's bounded mailbox is full or the actor has
    /// terminated. With the `cluster` feature it also fails if the endpoint is not started, if the
    /// outbound queue toward a remote actor's node is full or if that node is unreachable. It
    /// fails as well if the request cannot be encoded or exceeds the maximum frame size. Once the
    /// request is sent, it fails if no reply can arrive anymore, or if `within` elapses without
    /// one.
    ///
    /// [ActorSystem::terminated]: crate::ActorSystem::terminated
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn ask<R, F>(&self, within: Duration, make_message: F) -> Result<R, AskError>
    where
        F: FnOnce(ReplyTo<R>) -> M,
        R: Send + 'static,
    {
        let actor_id = self.actor_id();
        let (reply_tx, reply_rx) = oneshot::channel();

        let reply_to = ReplyTo::new(move |reply| {
            if reply_tx.send(reply).is_err() {
                warn!(
                    %actor_id,
                    reply_type = type_name::<R>(),
                    error = "asker no longer awaits the reply",
                    "dead letter"
                );
            }
        });
        // Must live until the ask returns: a serialized destination is released once it is gone.
        #[cfg(feature = "cluster")]
        let (mut ask_guard, reply_to) = cluster::AskGuard::attach(reply_to);

        match self.sink() {
            Sink::Local(mailbox_handle) => {
                mailbox_handle.try_send_message(make_message(reply_to))?
            }

            #[cfg(feature = "cluster")]
            Sink::Remote(remote_sink) => {
                let tags = remote_sink.try_send_message(self.actor_id(), make_message(reply_to))?;
                ask_guard.sent(tags);
            }
        }

        let reply = timeout(within, reply_rx)
            .await
            .map_err(|_| AskError::Timeout(within))?;
        #[cfg(feature = "cluster")]
        ask_guard.disarm();

        reply.map_err(|_| AskError::NoReply)
    }
}

impl<M, T> ActorContext<M, T>
where
    T: TimerMode,
{
    /// Create a [ReplyTo] which delivers the reply to this actor as an ordinary message,
    /// converted by the given function, typically an enum variant constructor. This is the actor
    /// side of request-response: no future is created or awaited, the reply arrives via
    /// [Actor::receive](crate::Actor::receive) like any other message.
    ///
    /// The reply is enqueued even if this actor's bounded mailbox is full. A [ReplyTo] replies at
    /// most once, so the mailbox exceeds its capacity by at most one reply per open [ReplyTo].
    /// Nothing limits how many are open. The reply still counts against the capacity, so ordinary
    /// senders are refused until the mailbox has drained back under it. On this node, the reply is
    /// dropped and logged as a dead letter only if this actor has terminated. A reply from another
    /// node can also be lost before it arrives, like a remote tell; see docs/cluster.md.
    ///
    /// Unlike [ActorRef::ask], this side has no `NoReply`: a reply the responder never sends, or
    /// one lost on its way, is never reported. An actor which must detect a missing reply declares
    /// `type Timers = Timed`, starts a timer per request with [ActorContext::start_timer] and
    /// cancels it with [ActorContext::cancel_timer] once the reply has arrived.
    pub fn reply_to<R, F>(&self, into_message: F) -> ReplyTo<R>
    where
        F: FnOnce(R) -> M + Send + 'static,
        M: Send + 'static,
        R: 'static,
    {
        let actor_ref = self.self_ref().clone();
        #[cfg(feature = "cluster")]
        let wire = cluster::ReplyToWire::for_recipient(&actor_ref);

        let reply_to = ReplyTo::new(move |reply| actor_ref.tell_forced(into_message(reply)));
        #[cfg(feature = "cluster")]
        let reply_to = reply_to.with_wire(wire);

        reply_to
    }
}

/// A single-shot destination for the reply to a request, carried inside the request message.
///
/// Created by [ActorRef::ask] or [ActorContext::reply_to] and consumed by [ReplyTo::reply], hence
/// at most one reply can be sent, enforced at compile time. The responder cannot tell how a
/// [ReplyTo] was created; both origins are handled the same way.
///
/// With the `cluster` feature a [ReplyTo] is serializable and can travel inside a message to
/// another node.
#[derive(Debug)]
pub struct ReplyTo<R> {
    #[debug(skip)]
    send_reply: Cell<Option<SendReply<R>>>,

    #[cfg(feature = "cluster")]
    wire: cluster::ReplyToWire,
}

impl<R> ReplyTo<R> {
    /// Send the reply without blocking. If it cannot be delivered, e.g. because the asker has
    /// terminated or is no longer awaiting it, the reply is dropped and logged as a dead letter.
    pub fn reply(self, reply: R) {
        match self.send_reply.take() {
            Some(send_reply) => send_reply(reply),

            None => warn!(
                reply_to = ?self,
                reply_type = type_name::<R>(),
                error = "reply destination already serialized",
                "dead letter"
            ),
        }
    }

    pub(crate) fn new<F>(send_reply: F) -> Self
    where
        F: FnOnce(R) + Send + 'static,
    {
        Self {
            send_reply: Cell::new(Some(Box::new(send_reply))),

            #[cfg(feature = "cluster")]
            wire: cluster::ReplyToWire::default(),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn with_wire(self, wire: cluster::ReplyToWire) -> Self {
        Self { wire, ..self }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn wire(&self) -> &cluster::ReplyToWire {
        &self.wire
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn take_send_reply(&self) -> Option<SendReply<R>> {
        self.send_reply.take()
    }
}

/// The possible failures of [ActorRef::ask].
#[derive(Debug, Error)]
pub enum AskError {
    /// The request was not sent: the actor's bounded mailbox was full. For a remote actor, the
    /// outbound queue toward its node was full.
    #[error("mailbox full")]
    MailboxFull,

    /// The request was not sent: the actor has terminated. For a remote actor, its node was
    /// unreachable.
    #[error("actor terminated")]
    ActorTerminated,

    /// The request was not sent: it could not be encoded. Unlike [AskError::MailboxFull], this
    /// failure is permanent: the same request fails the same way again.
    #[cfg(feature = "cluster")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
    #[error("request not encodable")]
    NotEncodable(#[source] cluster::codec::CodecError),

    /// The request was not sent: its encoding exceeds the maximum frame size. Unlike
    /// [AskError::MailboxFull], this failure is permanent: the same request fails the same way
    /// again.
    #[cfg(feature = "cluster")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
    #[error("encoded request of {len} bytes exceeds the maximum frame size of {max} bytes")]
    TooLarge {
        /// The size of the encoded request.
        len: usize,

        /// The maximum frame size.
        max: usize,
    },

    /// The request was not sent: the remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[cfg(feature = "cluster")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// The request was sent, but no reply will ever arrive: the [ReplyTo] was dropped without a
    /// reply or the actor stopped with the request still queued.
    #[error("no reply")]
    NoReply,

    /// The request was sent, but no reply arrived within the given duration; a late reply is
    /// dropped and logged as a dead letter.
    #[error("no reply within {0:?}")]
    Timeout(Duration),
}

impl From<SendError> for AskError {
    fn from(error: SendError) -> Self {
        match error {
            SendError::MailboxFull(_) => Self::MailboxFull,
            SendError::ActorTerminated(_) => Self::ActorTerminated,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ReplyTo;
    #[cfg(feature = "cluster")]
    use std::sync::mpsc;

    /// The sink is skipped in the `Debug` output and adds no `Debug` bound on the reply type, so
    /// message enums holding a `ReplyTo` can derive `Debug`.
    #[test]
    fn debug_skips_the_sink() {
        struct NotDebug;

        let reply_to = ReplyTo::new(|NotDebug| ());

        assert!(format!("{reply_to:?}").starts_with("ReplyTo"));
    }

    /// A reply after the sink was taken, as remote serialization does, is a dead letter rather
    /// than a panic.
    #[cfg(feature = "cluster")]
    #[test]
    fn reply_after_take_is_a_dead_letter() {
        let (reply_tx, reply_rx) = mpsc::channel();
        let reply_to = ReplyTo::new(move |reply| reply_tx.send(reply).expect("reply is received"));

        let send_reply = reply_to.take_send_reply();
        assert!(send_reply.is_some());
        assert!(reply_to.take_send_reply().is_none());

        reply_to.reply(42);

        assert!(reply_rx.try_recv().is_err());
    }
}
