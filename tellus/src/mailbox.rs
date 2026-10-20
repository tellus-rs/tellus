use crate::{
    ActorId, Incoming, MailboxCapacity,
    quota::{CountedSendError, CountedSender, Full, Quota},
    sync::lock,
};
use flume::Receiver;
#[cfg(feature = "test-util")]
use flume::TryRecvError;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

pub(crate) struct MailboxHandle<M> {
    incoming_tx: CountedSender<Incoming<M>>,
    watcher_registry: WatcherRegistry,
}

impl<M> MailboxHandle<M> {
    pub(crate) fn try_send_message(&self, message: M) -> Result<(), SendError> {
        self.incoming_tx
            .try_send_counted(Incoming::Message(message))?;

        Ok(())
    }

    pub(crate) fn try_send_forced_message(&self, message: M) -> Result<(), SendError> {
        self.incoming_tx
            .try_send_forced(Incoming::Message(message))
            .map_err(|_| ActorTerminated)?;

        Ok(())
    }

    pub(crate) fn watcher_registry(&self) -> &WatcherRegistry {
        &self.watcher_registry
    }

    /// The number of items not received yet, signals included.
    pub(crate) fn queued(&self) -> usize {
        self.incoming_tx.len()
    }

    /// The same underlying sender as for messages, hence a signal is ordered behind previously
    /// delivered messages while bypassing the quota.
    pub(crate) fn terminated_sink(&self) -> Arc<dyn TerminatedSink>
    where
        M: Send + 'static,
    {
        Arc::new(self.incoming_tx.clone())
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for MailboxHandle<M> {
    fn clone(&self) -> Self {
        Self {
            incoming_tx: self.incoming_tx.clone(),
            watcher_registry: self.watcher_registry.clone(),
        }
    }
}

pub(crate) struct Mailbox<M> {
    incoming_rx: Receiver<Incoming<M>>,
    watcher_registry: WatcherRegistry,
    quota: Quota,
}

impl<M> Mailbox<M> {
    /// One consumer per mailbox: `&mut self` enforces it, though the body would allow `&self`.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) async fn recv(&mut self) -> Option<Incoming<M>> {
        let incoming = self.incoming_rx.recv_async().await.ok()?;
        if matches!(incoming, Incoming::Message(_)) {
            self.quota.unreserve();
        }
        Some(incoming)
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn try_recv(&mut self) -> Option<Incoming<M>> {
        match self.incoming_rx.try_recv() {
            Ok(incoming) => {
                if matches!(incoming, Incoming::Message(_)) {
                    self.quota.unreserve();
                }
                Some(incoming)
            }

            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    /// Dropping the returned receiver makes every send fail as terminated, while the
    /// [ClosedMailbox] keeps registration open until [ClosedMailbox::take_watchers].
    pub(crate) fn split(self) -> (Receiver<Incoming<M>>, ClosedMailbox) {
        (self.incoming_rx, ClosedMailbox(self.watcher_registry))
    }
}

/// The watcher half of a split [Mailbox]: only its owner reaches [WatcherRegistry::take], so a
/// sender side registry clone can never close registration.
pub(crate) struct ClosedMailbox(WatcherRegistry);

impl ClosedMailbox {
    /// Consumes the mailbox: an empty result always means an unwatched actor, never a repeated
    /// take.
    pub(crate) fn take_watchers(self) -> Vec<Watcher> {
        self.0.take()
    }
}

/// Shared between both mailbox halves and the watching actors' contexts; `None` once
/// [WatcherRegistry::take] has closed registration.
#[derive(Clone)]
pub(crate) struct WatcherRegistry(Arc<Mutex<Option<HashMap<ActorId, Watcher>>>>);

impl WatcherRegistry {
    /// Registering is idempotent.
    pub(crate) fn add(&self, watcher: Watcher) -> Result<(), ActorTerminated> {
        let mut registry = lock(&self.0);
        let watchers = registry.as_mut().ok_or(ActorTerminated)?;
        watchers.entry(watcher.watcher_id()).or_insert(watcher);

        Ok(())
    }

    pub(crate) fn remove(&self, watcher_id: ActorId) {
        if let Some(watchers) = lock(&self.0).as_mut() {
            watchers.remove(&watcher_id);
        }
    }

    /// Close registration atomically, so a racing [WatcherRegistry::add] either is returned here
    /// or fails. Private: closing is the run loop's privilege, else a sender side caller could
    /// drop a live actor's watchers without ever signaling them.
    fn take(&self) -> Vec<Watcher> {
        lock(&self.0)
            .take()
            .map(|watchers| watchers.into_values().collect())
            .unwrap_or_default()
    }
}

impl Default for WatcherRegistry {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Some(HashMap::new()))))
    }
}

#[derive(Debug, Error)]
pub(crate) enum SendError {
    #[error("mailbox full")]
    MailboxFull(#[from] Full),

    #[error(transparent)]
    ActorTerminated(#[from] ActorTerminated),
}

impl From<CountedSendError> for SendError {
    fn from(error: CountedSendError) -> Self {
        match error {
            CountedSendError::Full(full) => Self::MailboxFull(full),
            CountedSendError::Disconnected(_) => Self::ActorTerminated(ActorTerminated),
        }
    }
}

#[derive(Debug, Error)]
#[error("actor terminated")]
pub(crate) struct ActorTerminated;

pub(crate) struct Watcher {
    watcher_id: ActorId,
    terminated_sink: Arc<dyn TerminatedSink>,
}

impl Watcher {
    pub(crate) fn new(watcher_id: ActorId, terminated_sink: Arc<dyn TerminatedSink>) -> Self {
        Self {
            watcher_id,
            terminated_sink,
        }
    }

    pub(crate) fn watcher_id(&self) -> ActorId {
        self.watcher_id
    }

    pub(crate) fn send_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.terminated_sink.send_terminated(actor_id)
    }
}

/// Type-erases the watching actor's sender, so a [Watcher] does not name its message type.
pub(crate) trait TerminatedSink
where
    Self: Send + Sync,
{
    fn send_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated>;
}

impl<M> TerminatedSink for CountedSender<Incoming<M>>
where
    M: Send + 'static,
{
    fn send_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.try_send_uncounted(Incoming::Terminated(actor_id))
            .map_err(|_| ActorTerminated)
    }
}

/// Both halves must share one quota count and one watcher registration, hence clone them, never
/// rebuild them.
pub(crate) fn make_mailbox<M>(mailbox_capacity: MailboxCapacity) -> (MailboxHandle<M>, Mailbox<M>) {
    let (incoming_tx, incoming_rx) = flume::unbounded();

    let quota = match mailbox_capacity {
        MailboxCapacity::Unbounded => Quota::unbounded(),
        MailboxCapacity::Bounded(capacity) => Quota::bounded(capacity),
    };
    let watcher_registry = WatcherRegistry::default();

    let mailbox_handle = MailboxHandle {
        incoming_tx: CountedSender::new(incoming_tx, quota.clone()),
        watcher_registry: watcher_registry.clone(),
    };
    let mailbox = Mailbox {
        incoming_rx,
        watcher_registry,
        quota,
    };

    (mailbox_handle, mailbox)
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity,
        mailbox::{SendError, Watcher, make_mailbox},
    };
    use std::{num::NonZeroUsize, time::Duration};
    use tokio::time::timeout;

    #[test]
    fn unbounded_never_fills() {
        let (mailbox_handle, _mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        for _ in 0..1_000 {
            assert!(mailbox_handle.try_send_message(()).is_ok());
        }
    }

    #[test]
    fn bounded_rejects_beyond_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    /// A bounded mailbox which is full when the actor terminates reports the termination, not the
    /// full mailbox, as the reason for a rejected send.
    #[test]
    fn terminated_overrides_full() {
        let (mailbox_handle, mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        drop(mailbox);

        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::ActorTerminated(_))
        ));
    }

    /// Splitting the mailbox already fails sends as terminated while registration stays open, so
    /// termination can reject senders early yet signal its watchers last.
    #[test]
    fn splitting_disconnects_senders_but_keeps_registration_open() {
        let (mailbox_handle, mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        assert!(mailbox_handle.try_send_message(()).is_ok());

        let (incoming_rx, closed_mailbox) = mailbox.split();
        drop(incoming_rx);

        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::ActorTerminated(_))
        ));

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        assert_eq!(closed_mailbox.take_watchers().len(), 1);
    }

    #[tokio::test]
    async fn receiving_a_message_frees_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox.recv().await.is_some());
        assert!(mailbox_handle.try_send_message(()).is_ok());
    }

    #[tokio::test]
    async fn recv_drains_queued_messages_before_ending() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<u32>(MailboxCapacity::Unbounded);

        assert!(mailbox_handle.try_send_message(1).is_ok());
        assert!(mailbox_handle.try_send_message(2).is_ok());
        drop(mailbox_handle);

        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(1))));
        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(2))));
        assert!(mailbox.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn recv_ends_only_once_every_handle_is_dropped() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<u32>(MailboxCapacity::Unbounded);
        let clone = mailbox_handle.clone();
        drop(mailbox_handle);

        assert!(
            timeout(Duration::from_secs(5), mailbox.recv())
                .await
                .is_err()
        );

        drop(clone);
        assert!(mailbox.recv().await.is_none());
    }

    #[test]
    fn clones_share_one_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        let clone = mailbox_handle.clone();

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            clone.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    /// Cloning a handle shares the watcher registration as well as the capacity: a watcher
    /// registered through a clone is taken by the receiving half, hence signaled at termination.
    #[test]
    fn clones_share_one_watcher_registry() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let clone = mailbox_handle.clone();

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());
        assert!(clone.watcher_registry().add(watcher).is_ok());

        assert_eq!(mailbox.split().1.take_watchers().len(), 1);
    }

    /// A send to a terminated actor reports the termination rather than a full mailbox, also when
    /// capacity is still available: that is the reserve-then-send path, whereas
    /// `terminated_overrides_full` covers the one where the quota is already exhausted.
    #[test]
    fn terminated_with_spare_capacity() {
        let capacity = NonZeroUsize::new(2).expect("2 is not zero");
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Bounded(capacity));

        drop(mailbox);

        for _ in 0..2 * capacity.get() {
            assert!(matches!(
                mailbox_handle.try_send_message(()),
                Err(SendError::ActorTerminated(_))
            ));
        }
    }

    #[tokio::test]
    async fn terminated_signals_ignore_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        let terminated_sink = mailbox_handle.terminated_sink();

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(terminated_sink.send_terminated(ActorId::new()).is_ok());

        assert!(matches!(mailbox.recv().await, Some(Incoming::Message(_))));
        assert!(matches!(
            mailbox.recv().await,
            Some(Incoming::Terminated(_))
        ));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    #[test]
    fn watching_ignores_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
    }

    /// Registering the same watcher twice signals once: a terminated signal only names the
    /// terminated actor, hence a second one would carry nothing.
    #[test]
    fn adding_a_watcher_twice_registers_once() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let (watcher_handle, _watcher_mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        let watcher_id = ActorId::new();
        for _ in 0..3 {
            assert!(
                mailbox_handle
                    .watcher_registry()
                    .add(Watcher::new(watcher_id, watcher_handle.terminated_sink()))
                    .is_ok()
            );
        }

        assert_eq!(mailbox.split().1.take_watchers().len(), 1);
    }

    /// Removing a watcher deregisters it, so no terminated signal is sent to it and no reference
    /// to it is held anymore.
    #[test]
    fn removing_a_watcher_deregisters_it() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        let watcher_id = ActorId::new();
        let watcher = Watcher::new(watcher_id, mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        mailbox_handle.watcher_registry().remove(watcher_id);

        assert!(mailbox.split().1.take_watchers().is_empty());
    }

    /// Removing after registration has been closed has no effect, in particular it must not
    /// reopen registration.
    #[test]
    fn removing_after_take_is_a_noop() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        let watcher_id = ActorId::new();
        let watcher = Watcher::new(watcher_id, mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        assert_eq!(mailbox.split().1.take_watchers().len(), 1);

        mailbox_handle.watcher_registry().remove(watcher_id);

        let watcher = Watcher::new(watcher_id, mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_err());
    }

    /// A watcher delivers the terminated signal into the watching actor's mailbox and reports an
    /// error once that mailbox is gone, i.e. the watching actor itself has terminated.
    #[tokio::test]
    async fn watcher_sends_terminated_into_watching_mailbox() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());

        let actor_id = ActorId::new();
        assert!(watcher.send_terminated(actor_id).is_ok());
        assert!(matches!(
            mailbox.recv().await,
            Some(Incoming::Terminated(other)) if other == actor_id
        ));

        drop(mailbox);
        assert!(watcher.send_terminated(actor_id).is_err());
    }

    /// The depth counts every item still in the queue, a forced message and a terminated signal
    /// like a counted message, and goes down as items are received.
    #[tokio::test]
    async fn queued_is_the_number_of_items_in_the_queue() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        assert_eq!(mailbox_handle.queued(), 0);

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox_handle.try_send_forced_message(()).is_ok());
        assert!(
            mailbox_handle
                .terminated_sink()
                .send_terminated(ActorId::new())
                .is_ok()
        );
        assert_eq!(mailbox_handle.queued(), 3);

        assert!(mailbox.recv().await.is_some());
        assert_eq!(mailbox_handle.queued(), 2);

        assert!(mailbox.recv().await.is_some());
        assert!(mailbox.recv().await.is_some());
        assert_eq!(mailbox_handle.queued(), 0);
    }

    /// Taking the watchers closes registration, hence a watcher racing with termination either is
    /// taken or learns that the actor has terminated, but is never lost.
    #[test]
    fn taking_watchers_closes_registration() {
        let (mailbox_handle, mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);

        let (_incoming_rx, closed_mailbox) = mailbox.split();

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        assert_eq!(closed_mailbox.take_watchers().len(), 1);

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_sink());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_err());
    }
}
