use crate::{
    ActorId, Incoming, MailboxCapacity,
    quota::{CountedSendError, CountedSender, Full, Quota},
    timers::TimerId,
    watch::{ActorTerminated, TerminatedHandler, WatcherRegistration, WatcherRegistry},
};
use flume::Receiver;
#[cfg(feature = "test-util")]
use flume::TryRecvError;
use std::sync::Arc;
use thiserror::Error;

/// Sending mailbox half.
pub(crate) struct MailboxHandle<M> {
    incoming_tx: CountedSender<Queued<M>>,
    watcher_registry: WatcherRegistry,
}

impl<M> MailboxHandle<M> {
    pub(crate) fn try_send_message(&self, message: M) -> Result<(), SendError> {
        self.incoming_tx
            .try_send_counted(Queued::Incoming(Incoming::Message(message)))?;

        Ok(())
    }

    pub(crate) fn try_send_forced_message(&self, message: M) -> Result<(), SendError> {
        self.incoming_tx
            .try_send_forced(Queued::Incoming(Incoming::Message(message)))
            .map_err(|_| ActorTerminated)?;

        Ok(())
    }

    pub(crate) fn try_send_timer(&self, id: TimerId) -> Result<(), ActorTerminated> {
        self.incoming_tx
            .try_send_uncounted(Queued::Timer(id))
            .map_err(|_| ActorTerminated)
    }

    pub(crate) fn watcher_registry(&self) -> &WatcherRegistry {
        &self.watcher_registry
    }

    pub(crate) fn queued(&self) -> usize {
        self.incoming_tx.len()
    }

    pub(crate) fn terminated_handler(&self) -> Arc<dyn TerminatedHandler>
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

/// Receiving mailbox half.
pub(crate) struct Mailbox<M> {
    incoming_rx: Receiver<Queued<M>>,
    watcher_registration: WatcherRegistration,
    quota: Quota,
}

impl<M> Mailbox<M> {
    /// `&mut self` to enforce one consumer per mailbox.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) async fn recv(&mut self) -> Option<Queued<M>> {
        let queued = self.incoming_rx.recv_async().await.ok()?;
        if matches!(queued, Queued::Incoming(Incoming::Message(_))) {
            self.quota.unreserve();
        }
        Some(queued)
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn try_recv(&mut self) -> Option<Queued<M>> {
        match self.incoming_rx.try_recv() {
            Ok(queued) => {
                if matches!(queued, Queued::Incoming(Incoming::Message(_))) {
                    self.quota.unreserve();
                }
                Some(queued)
            }

            Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
        }
    }

    pub(crate) fn split(self) -> (Receiver<Queued<M>>, WatcherRegistration) {
        (self.incoming_rx, self.watcher_registration)
    }
}

/// An item in a mailbox: an incoming for the actor, or the slot and generation of a due timer.
pub(crate) enum Queued<M> {
    Incoming(Incoming<M>),

    Timer(TimerId),
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

impl<M> TerminatedHandler for CountedSender<Queued<M>>
where
    M: Send + 'static,
{
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.try_send_uncounted(Queued::Incoming(Incoming::Terminated(actor_id)))
            .map_err(|_| ActorTerminated)
    }
}

pub(crate) fn make_mailbox<M>(mailbox_capacity: MailboxCapacity) -> (MailboxHandle<M>, Mailbox<M>) {
    let (incoming_tx, incoming_rx) = flume::unbounded();

    let quota = match mailbox_capacity {
        MailboxCapacity::Unbounded => Quota::unbounded(),
        MailboxCapacity::Bounded(capacity) => Quota::bounded(capacity),
    };
    let (watcher_registry, watcher_registration) = WatcherRegistry::new();

    let mailbox_handle = MailboxHandle {
        incoming_tx: CountedSender::new(incoming_tx, quota.clone()),
        watcher_registry,
    };
    let mailbox = Mailbox {
        incoming_rx,
        watcher_registration,
        quota,
    };

    (mailbox_handle, mailbox)
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity,
        mailbox::{Queued, SendError, make_mailbox},
        timers::TimerId,
        watch::Watcher,
    };
    use std::{num::NonZeroUsize, time::Duration};
    use tokio::time::timeout;

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

        let (incoming_rx, watcher_registration) = mailbox.split();
        drop(incoming_rx);

        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::ActorTerminated(_))
        ));

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
        assert_eq!(watcher_registration.close().len(), 1);
    }

    #[tokio::test]
    async fn receiving_a_message_frees_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox.recv().await.is_some());
        assert!(mailbox_handle.try_send_message(()).is_ok());
    }

    #[test]
    fn watching_ignores_capacity() {
        let (mailbox_handle, _mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(mailbox_handle.watcher_registry().add(watcher).is_ok());
    }

    #[tokio::test]
    async fn recv_drains_queued_messages_before_ending() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<u32>(MailboxCapacity::Unbounded);

        assert!(mailbox_handle.try_send_message(1).is_ok());
        assert!(mailbox_handle.try_send_message(2).is_ok());
        drop(mailbox_handle);

        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Message(1)))
        ));
        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Message(2)))
        ));
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

        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());
        assert!(clone.watcher_registry().add(watcher).is_ok());

        assert_eq!(mailbox.split().1.close().len(), 1);
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
        let terminated_handler = mailbox_handle.terminated_handler();

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(terminated_handler.handle_terminated(ActorId::new()).is_ok());

        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Message(_)))
        ));
        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Terminated(_)))
        ));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    #[tokio::test]
    async fn timer_items_ignore_capacity() {
        let (mailbox_handle, mut mailbox) =
            make_mailbox::<()>(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox_handle.try_send_timer(TimerId::new(0, 1)).is_ok());

        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Message(_)))
        ));
        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Timer(id)) if id == TimerId::new(0, 1)
        ));

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(matches!(
            mailbox_handle.try_send_message(()),
            Err(SendError::MailboxFull(_))
        ));
    }

    #[test]
    fn a_queued_item_is_no_larger_than_an_incoming() {
        assert_eq!(size_of::<Queued<()>>(), size_of::<Incoming<()>>());
        assert_eq!(size_of::<Queued<u32>>(), size_of::<Incoming<u32>>());
        assert_eq!(size_of::<Queued<u64>>(), size_of::<Incoming<u64>>());
        assert_eq!(size_of::<Queued<String>>(), size_of::<Incoming<String>>());
    }

    /// A watcher delivers the terminated signal into the watching actor's mailbox and reports an
    /// error once that mailbox is gone, i.e. the watching actor itself has terminated.
    #[tokio::test]
    async fn watcher_sends_terminated_into_watching_mailbox() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        let watcher = Watcher::new(ActorId::new(), mailbox_handle.terminated_handler());

        let actor_id = ActorId::new();
        assert!(watcher.handle_terminated(actor_id).is_ok());
        assert!(matches!(
            mailbox.recv().await,
            Some(Queued::Incoming(Incoming::Terminated(other))) if other == actor_id
        ));

        drop(mailbox);
        assert!(watcher.handle_terminated(actor_id).is_err());
    }

    /// The depth counts every item still in the queue, a forced message, a terminated signal and a
    /// timer item like a counted message, and goes down as items are received.
    #[tokio::test]
    async fn queued_is_the_number_of_items_in_the_queue() {
        let (mailbox_handle, mut mailbox) = make_mailbox::<()>(MailboxCapacity::Unbounded);
        assert_eq!(mailbox_handle.queued(), 0);

        assert!(mailbox_handle.try_send_message(()).is_ok());
        assert!(mailbox_handle.try_send_forced_message(()).is_ok());
        assert!(
            mailbox_handle
                .terminated_handler()
                .handle_terminated(ActorId::new())
                .is_ok()
        );
        assert!(mailbox_handle.try_send_timer(TimerId::new(0, 1)).is_ok());
        assert_eq!(mailbox_handle.queued(), 4);

        assert!(mailbox.recv().await.is_some());
        assert_eq!(mailbox_handle.queued(), 3);

        assert!(mailbox.recv().await.is_some());
        assert!(mailbox.recv().await.is_some());
        assert!(mailbox.recv().await.is_some());
        assert_eq!(mailbox_handle.queued(), 0);
    }
}
