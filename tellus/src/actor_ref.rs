#[cfg(feature = "cluster")]
use crate::cluster;
use crate::{
    ActorId, MailboxCapacity,
    mailbox::{Mailbox, MailboxHandle, SendError, make_mailbox},
    watch::{TerminatedHandler, Watcher, WatcherRegistry},
};
use derive_more::Debug;
use std::{
    any::type_name,
    error::Error,
    hash::{Hash, Hasher},
    sync::Arc,
};
use tracing::warn;

/// A shareable reference to an actor, used to send it messages and read its ID.
///
/// Equality and hashing are by [ActorRef::actor_id] alone: any two references naming the same actor
/// compare equal and hash equally, however they were obtained.
#[derive(Debug)]
pub struct ActorRef<M> {
    actor_id: AlignedId,

    #[debug(skip)]
    sink: Sink<M>,
}

impl<M> ActorRef<M> {
    /// The ID of the actor represented by this reference.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id.0
    }

    /// Send a message to the actor represented by this reference without blocking. If the actor
    /// has terminated, or if its mailbox is full for [MailboxCapacity::Bounded], the message
    /// is dropped and logged as a dead letter. Also, even if the message is delivered to the
    /// actor, it might stop before processing it. With the `cluster` feature a reference can point
    /// to an actor on another node; then an unreachable node or a full outbound queue equally
    /// makes the message a dead letter.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn tell(&self, message: M) {
        match &self.sink {
            Sink::Local(mailbox_handle) => {
                if let Err(error) = mailbox_handle.try_send_message(message) {
                    self.dead_letter(&error);
                }
            }

            #[cfg(feature = "cluster")]
            Sink::Remote(remote_sink) => {
                if let Err(error) = remote_sink.try_send_message(self.actor_id.0, message) {
                    self.dead_letter(&error);
                }
            }
        }
    }

    /// Like [ActorRef::tell], but reporting whether the message was enqueued.
    pub(crate) fn try_tell(&self, message: M) -> Result<(), SendError> {
        self.local_mailbox_handle().try_send_message(message)
    }

    /// Enqueue past a full mailbox, for messages the framework has already accepted on the
    /// sender's behalf, e.g. what a [Host] buffered while an entity stopped.
    ///
    /// [Host]: crate::Host
    pub(crate) fn try_tell_forced(&self, message: M) -> Result<(), SendError> {
        self.local_mailbox_handle().try_send_forced_message(message)
    }

    pub(crate) fn sink(&self) -> &Sink<M> {
        &self.sink
    }

    pub(crate) fn watcher_registry(&self) -> Option<&WatcherRegistry> {
        match &self.sink {
            Sink::Local(mailbox_handle) => Some(mailbox_handle.watcher_registry()),

            #[cfg(feature = "cluster")]
            Sink::Remote(_) => None,
        }
    }

    /// Crate-private on purpose: for a remote reference the depth would need a round trip, so
    /// other actors ask through the actor's own protocol; only the actor's own, local reference
    /// is asked.
    pub(crate) fn queued(&self) -> usize {
        self.local_mailbox_handle().queued()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn remote(actor_id: ActorId, remote_sink: cluster::RemoteSink<M>) -> Self {
        Self {
            actor_id: AlignedId(actor_id),
            sink: Sink::Remote(remote_sink),
        }
    }

    pub(crate) fn dead_letter(&self, error: &dyn Error) {
        warn!(
            actor_id = %self.actor_id.0,
            message_type = type_name::<M>(),
            %error,
            source = error.source(),
            "dead letter"
        );
    }

    fn local_mailbox_handle(&self) -> &MailboxHandle<M> {
        match &self.sink {
            Sink::Local(mailbox_handle) => mailbox_handle,

            #[cfg(feature = "cluster")]
            Sink::Remote(_) => unreachable!("a host only tells entities it spawned itself"),
        }
    }

    fn new(actor_id: ActorId, mailbox_handle: MailboxHandle<M>) -> Self {
        Self {
            actor_id: AlignedId(actor_id),
            sink: Sink::Local(mailbox_handle),
        }
    }
}

// Derived impls would needlessly require `M: PartialEq`/`M: Hash`.
impl<M> PartialEq for ActorRef<M> {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id.0 == other.actor_id.0
    }
}

impl<M> Eq for ActorRef<M> {}

impl<M> Hash for ActorRef<M> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.actor_id.0.hash(state);
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id,
            sink: self.sink.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(align(16))]
struct AlignedId(ActorId);

pub(crate) enum Sink<M> {
    Local(MailboxHandle<M>),

    #[cfg(feature = "cluster")]
    Remote(cluster::RemoteSink<M>),
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for Sink<M> {
    fn clone(&self) -> Self {
        match self {
            Sink::Local(mailbox_handle) => Sink::Local(mailbox_handle.clone()),

            #[cfg(feature = "cluster")]
            Sink::Remote(remote_sink) => Sink::Remote(remote_sink.clone()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SelfRef<M> {
    actor_ref: ActorRef<M>,

    #[debug(skip)]
    terminated_handler: Arc<dyn TerminatedHandler>,
}

impl<M> SelfRef<M> {
    pub(crate) fn new(actor_id: ActorId, mailbox_capacity: MailboxCapacity) -> (Self, Mailbox<M>)
    where
        M: Send + 'static,
    {
        let (mailbox_handle, mailbox) = make_mailbox(mailbox_capacity);
        let terminated_handler = mailbox_handle.terminated_handler();
        let actor_ref = ActorRef::new(actor_id, mailbox_handle);

        (
            Self {
                actor_ref,
                terminated_handler,
            },
            mailbox,
        )
    }

    pub(crate) fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    pub(crate) fn make_watcher(&self) -> Watcher {
        Watcher::new(self.actor_ref.actor_id.0, self.terminated_handler.clone())
    }

    pub(crate) fn send_terminated(&self, actor_id: ActorId) {
        self.terminated_handler
            .handle_terminated(actor_id)
            .expect("the actor's own mailbox outlives its context");
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for SelfRef<M> {
    fn clone(&self) -> Self {
        Self {
            actor_ref: self.actor_ref.clone(),
            terminated_handler: self.terminated_handler.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ActorId, ActorRef, MailboxCapacity, actor_ref::SelfRef};
    use std::hash::{DefaultHasher, Hash, Hasher};

    /// References compare and hash by ID alone, hence a clone stands in for the original wherever
    /// references are used as keys, while a reference to another actor does not.
    #[test]
    fn references_are_equal_and_hash_by_id() {
        let (self_ref, _mailbox) = SelfRef::<()>::new(ActorId::new(), MailboxCapacity::Unbounded);
        let (other_ref, _other_mailbox) =
            SelfRef::<()>::new(ActorId::new(), MailboxCapacity::Unbounded);
        let actor_ref = self_ref.actor_ref().clone();

        assert_eq!(&actor_ref, self_ref.actor_ref());
        assert_eq!(hash_of(&actor_ref), hash_of(self_ref.actor_ref()));

        assert_ne!(&actor_ref, other_ref.actor_ref());
    }

    fn hash_of<M>(actor_ref: &ActorRef<M>) -> u64 {
        let mut hasher = DefaultHasher::new();
        actor_ref.hash(&mut hasher);
        hasher.finish()
    }
}
