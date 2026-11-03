mod private {
    use crate::{Actor, ActorConfig, ActorId, ActorRef, MailboxCapacity, Timed, Untimed};
    #[cfg(feature = "persistence")]
    use crate::{Codec, EventSourced, EventStore, Persistence, SnapshotStore};
    use tokio::sync::watch;

    pub trait Sealed
    where
        Self: Sized + 'static,
    {
        type Table<M>;

        fn make_table<M>(token: Token, capacity: MailboxCapacity) -> Self::Table<M>;

        fn settle<M>(token: Token, table: &mut Self::Table<M>);

        fn settle_at_start<M>(token: Token, table: &mut Self::Table<M>);

        fn note_timer_item<M>(token: Token, table: &mut Self::Table<M>);

        fn drop_signals_of<M>(token: Token, table: &Self::Table<M>, actor_id: ActorId);

        fn hand_back_stash<M>(token: Token, actor_id: ActorId, table: &mut Self::Table<M>);

        fn drop_stashed_messages<M>(token: Token, actor_id: ActorId, table: &mut Self::Table<M>);

        fn spawn_untimed<A>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            config: ActorConfig,
        ) -> ActorRef<A::Message>
        where
            A: Actor<Timers = Untimed, Stash = Self> + Send + 'static,
            A::Message: Send + 'static,
            A::State: Send + 'static;

        fn spawn_timed<A>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            config: ActorConfig,
        ) -> ActorRef<A::Message>
        where
            A: Actor<Timers = Timed, Stash = Self> + Send + 'static,
            A::Message: Send + 'static,
            A::State: Send + 'static;

        #[cfg(feature = "persistence")]
        fn spawn_event_sourced_untimed<A, E, S, C>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            persistence: Persistence<E, S, C>,
            config: ActorConfig,
        ) -> ActorRef<A::Command>
        where
            A: EventSourced<Timers = Untimed, Stash = Self> + Send + Sync + 'static,
            A::Command: Send + 'static,
            A::State: Send + 'static,
            A::Event: Send + 'static,
            A::Snapshot: Send + 'static,
            E: EventStore,
            S: SnapshotStore,
            C: Codec + Send + Sync + 'static;

        #[cfg(feature = "persistence")]
        fn spawn_event_sourced_timed<A, E, S, C>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            persistence: Persistence<E, S, C>,
            config: ActorConfig,
        ) -> ActorRef<A::Command>
        where
            A: EventSourced<Timers = Timed, Stash = Self> + Send + Sync + 'static,
            A::Command: Send + 'static,
            A::State: Send + 'static,
            A::Event: Send + 'static,
            A::Snapshot: Send + 'static,
            E: EventStore,
            S: SnapshotStore,
            C: Codec + Send + Sync + 'static;
    }

    pub struct Token(pub(crate) ());
}

pub(crate) use private::{Sealed, Token};

use crate::{
    Actor, ActorConfig, ActorId, ActorRef, Incoming, MailboxCapacity, Timed, Untimed,
    actor_context::{self, drop_containing_panic},
};
#[cfg(feature = "persistence")]
use crate::{Codec, EventSourced, EventStore, Persistence, SnapshotStore, persistence::spawn};
use std::{cell::RefCell, cmp, collections::VecDeque, mem, num::NonZeroUsize};
use tokio::sync::watch;

pub(crate) const STASHED_FAILED_TO_DROP: &str = "stashed message failed to drop";

/// Whether an actor stashes, chosen per actor type via [Actor::Stash] and [EventSourced::Stash]:
/// [Unstashed] or [Stashed]. Sealed: these two are the only modes.
///
/// An [Unstashed] actor cannot stash; its context has no `stash`:
///
/// ```compile_fail,E0599
/// use std::convert::Infallible;
/// use tellus::{Actor, ActorContext, Control, Incoming, Unstashed, Untimed};
///
/// struct Eager;
///
/// impl Actor for Eager {
///     type Message = ();
///     type Timers = Untimed;
///     type Stash = Unstashed;
///     type State = ();
///     type Error = Infallible;
///
///     fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
///         Ok(())
///     }
///
///     fn receive(
///         &self,
///         context: &ActorContext<Self::Message>,
///         incoming: Incoming<Self::Message>,
///         _: Self::State,
///     ) -> Result<Control<Self::State>, Self::Error> {
///         let _ = context.stash(incoming);
///         Ok(Control::Stop)
///     }
/// }
/// ```
///
/// The run loop an actor gets is chosen by its modes when it is spawned; nothing outside tellus can
/// spawn one another way:
///
/// ```compile_fail,E0277
/// use tellus::{Actor, ActorConfig, ActorRef, StashMode, Unstashed, Untimed};
/// use tokio::sync::watch;
///
/// fn spawn_anyway<A, S>(actor: A, stopped: watch::Receiver<()>) -> ActorRef<A::Message>
/// where
///     A: Actor<Timers = Untimed, Stash = S> + Send + 'static,
///     A::Message: Send + 'static,
///     A::State: Send + 'static,
///     S: StashMode,
/// {
///     S::spawn_untimed(Default::default(), stopped, actor, ActorConfig::default())
/// }
/// ```
///
/// [Actor::Stash]: crate::Actor::Stash
/// [EventSourced::Stash]: crate::EventSourced::Stash
pub trait StashMode
where
    Self: Sealed,
{
}

/// The mode of an actor without a stash: its [ActorContext](crate::ActorContext) has neither
/// `stash` nor `unstash_all`, and its run loop takes no items from a stash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unstashed;

impl StashMode for Unstashed {}

impl Sealed for Unstashed {
    type Table<M> = ();

    fn make_table<M>(_: Token, _: MailboxCapacity) -> Self::Table<M> {}

    fn settle<M>(_: Token, _: &mut Self::Table<M>) {}

    fn settle_at_start<M>(_: Token, _: &mut Self::Table<M>) {}

    fn note_timer_item<M>(_: Token, _: &mut Self::Table<M>) {}

    fn drop_signals_of<M>(_: Token, _: &Self::Table<M>, _: ActorId) {}

    fn hand_back_stash<M>(_: Token, _: ActorId, _: &mut Self::Table<M>) {}

    fn drop_stashed_messages<M>(_: Token, _: ActorId, _: &mut Self::Table<M>) {}

    fn spawn_untimed<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Untimed, Stash = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_untimed_unstashed(stopped_by_parent_rx, actor, config)
    }

    fn spawn_timed<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Timed, Stash = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_timed_unstashed(stopped_by_parent_rx, actor, config)
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced_untimed<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Untimed, Stash = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_untimed_unstashed(
            stopped_by_parent_rx,
            actor,
            persistence,
            config,
        )
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced_timed<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Timed, Stash = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_timed_unstashed(stopped_by_parent_rx, actor, persistence, config)
    }
}

/// The mode of an actor with a stash: its [ActorContext](crate::ActorContext) has `stash` and
/// `unstash_all`, and its run loop receives the unstashed items ahead of a due timer and of the
/// mailbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stashed;

impl StashMode for Stashed {}

impl Sealed for Stashed {
    type Table<M> = RefCell<Stash<M>>;

    fn make_table<M>(_: Token, capacity: MailboxCapacity) -> Self::Table<M> {
        RefCell::new(Stash::new(capacity))
    }

    fn settle<M>(_: Token, table: &mut Self::Table<M>) {
        table.get_mut().settle();
    }

    fn settle_at_start<M>(_: Token, table: &mut Self::Table<M>) {
        table.get_mut().settle_at_start();
    }

    fn note_timer_item<M>(_: Token, table: &mut Self::Table<M>) {
        table.get_mut().note_timer_item();
    }

    fn drop_signals_of<M>(_: Token, table: &Self::Table<M>, actor_id: ActorId) {
        table.borrow_mut().drop_signals_of(actor_id);
    }

    fn hand_back_stash<M>(_: Token, actor_id: ActorId, table: &mut Self::Table<M>) {
        for incoming in table.get_mut().hand_back_all() {
            drop_containing_panic(actor_id, STASHED_FAILED_TO_DROP, incoming);
        }
    }

    fn drop_stashed_messages<M>(_: Token, actor_id: ActorId, table: &mut Self::Table<M>) {
        for incoming in table.get_mut().take_all() {
            drop_containing_panic(actor_id, STASHED_FAILED_TO_DROP, incoming);
        }
    }

    fn spawn_untimed<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Untimed, Stash = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_untimed_stashed(stopped_by_parent_rx, actor, config)
    }

    fn spawn_timed<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Timed, Stash = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_timed_stashed(stopped_by_parent_rx, actor, config)
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced_untimed<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Untimed, Stash = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_untimed_stashed(stopped_by_parent_rx, actor, persistence, config)
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced_timed<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Timed, Stash = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_timed_stashed(stopped_by_parent_rx, actor, persistence, config)
    }
}

// Named by `Sealed::Table` of a `pub` trait, so `pub`; the module is private.
/// The items an actor has deferred, those it has handed back to its run loop, and the requests of
/// the item it is handling, which take effect once that item is settled.
pub struct Stash<M> {
    capacity: Option<NonZeroUsize>,

    // The unstashed items first, `unstashed` of them, then the stashed ones.
    items: VecDeque<Incoming<M>>,
    unstashed: usize,

    // The messages in `items` and `requested`: terminated signals skip the capacity.
    messages: usize,

    requested: Vec<Incoming<M>>,

    // How many of `requested` precede the last `unstash_all` request.
    release: Option<usize>,

    // How many leading stashed items an `unstash_all` made during the drain hands back.
    held: usize,

    // The length of `items` when the drain began.
    drain_len: usize,

    // Where the item in hand came from.
    source: Source,

    // Whether `settle` has anything to do.
    pending: bool,
}

impl<M> Stash<M> {
    pub(crate) fn new(capacity: MailboxCapacity) -> Self {
        let capacity = match capacity {
            MailboxCapacity::Unbounded => None,
            MailboxCapacity::Bounded(capacity) => Some(capacity),
        };

        Self {
            capacity,
            items: VecDeque::new(),
            unstashed: 0,
            messages: 0,
            requested: Vec::new(),
            release: None,
            held: 0,
            drain_len: 0,
            source: Source::Mailbox,
            pending: false,
        }
    }

    pub(crate) fn request_stash(&mut self, incoming: Incoming<M>) -> Result<(), Incoming<M>> {
        if matches!(incoming, Incoming::Message(_)) {
            if self
                .capacity
                .is_some_and(|capacity| self.messages >= capacity.get())
            {
                return Err(incoming);
            }
            self.messages += 1;
        }

        self.requested.push(incoming);
        self.pending = true;
        Ok(())
    }

    pub(crate) fn request_unstash_all(&mut self) {
        self.release = Some(self.requested.len());
        self.pending = true;
    }

    pub(crate) fn has_unstashed(&self) -> bool {
        self.unstashed > 0
    }

    pub(crate) fn pop_unstashed(&mut self) -> Option<Incoming<M>> {
        if self.unstashed == 0 {
            return None;
        }

        self.unstashed -= 1;
        self.source = Source::Stash;
        self.pending = true;
        let incoming = self
            .items
            .pop_front()
            .expect("every unstashed item is in the stash");
        if matches!(incoming, Incoming::Message(_)) {
            self.messages -= 1;
        }
        Some(incoming)
    }

    pub(crate) fn settle(&mut self) {
        if self.pending {
            self.apply_requests();
        }
    }

    pub(crate) fn settle_at_start(&mut self) {
        self.source = Source::Start;
        self.apply_requests();
    }

    pub(crate) fn note_timer_item(&mut self) {
        self.source = Source::Timer;
        self.pending = true;
    }

    #[cold]
    fn apply_requests(&mut self) {
        let source = mem::replace(&mut self.source, Source::Mailbox);
        let release = self
            .release
            .take()
            .map(|requested| self.items.len() - self.unstashed + requested);
        self.items.extend(self.requested.drain(..));

        match source {
            Source::Stash => {
                if let Some(release) = release {
                    self.held = cmp::max(self.held, release);
                }

                if self.unstashed == 0 && self.held > 0 && self.items.len() < self.drain_len {
                    let held = mem::take(&mut self.held);
                    self.hand_back(held);
                }
            }

            Source::Mailbox => {
                let held = mem::take(&mut self.held);
                self.hand_back(cmp::max(held, release.unwrap_or_default()));
            }

            Source::Timer | Source::Start => {
                if let Some(release) = release {
                    self.hand_back(release);
                }
            }
        }
        self.pending = self.held > 0;
    }

    pub(crate) fn drop_signals_of(&mut self, actor_id: ActorId) {
        if self.items.len() + self.requested.len() == self.messages {
            return;
        }

        let is_signal = |incoming: &Incoming<M>| matches!(incoming, Incoming::Terminated(id) if *id == actor_id);

        let (unstashed, held) = (self.unstashed, self.held);
        let (mut dropped_unstashed, mut dropped_held) = (0, 0);
        let mut position = 0;
        self.items.retain(|incoming| {
            let index = position;
            position += 1;
            if !is_signal(incoming) {
                return true;
            }

            if index < unstashed {
                dropped_unstashed += 1;
            } else if index - unstashed < held {
                dropped_held += 1;
            }
            false
        });
        self.unstashed -= dropped_unstashed;
        self.held -= dropped_held;

        let release = self.release;
        let mut dropped_released = 0;
        let mut position = 0;
        self.requested.retain(|incoming| {
            let index = position;
            position += 1;
            if !is_signal(incoming) {
                return true;
            }

            if release.is_some_and(|release| index < release) {
                dropped_released += 1;
            }
            false
        });
        if let Some(release) = &mut self.release {
            *release -= dropped_released;
        }
    }

    pub(crate) fn hand_back_all(&mut self) -> Vec<Incoming<M>> {
        let discarded = mem::take(&mut self.requested);
        self.messages -= message_count(&discarded);
        self.release = None;
        self.held = 0;
        self.source = Source::Mailbox;
        self.pending = false;
        self.unstashed = self.items.len();
        self.drain_len = self.items.len();
        discarded
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn take_stashed(&mut self) -> Vec<Incoming<M>> {
        let stashed = self.items.drain(self.unstashed..).collect::<Vec<_>>();
        self.messages -= message_count(&stashed);
        self.held = 0;
        stashed
    }

    pub(crate) fn take_all(&mut self) -> Vec<Incoming<M>> {
        self.unstashed = 0;
        self.messages = 0;
        self.release = None;
        self.held = 0;
        self.source = Source::Mailbox;
        self.pending = false;
        self.items
            .drain(..)
            .chain(self.requested.drain(..))
            .collect()
    }

    fn hand_back(&mut self, stashed: usize) {
        if stashed == 0 {
            return;
        }

        self.unstashed += stashed;
        self.held = self.held.saturating_sub(stashed);
        self.drain_len = self.items.len();
    }
}

/// Where the item an actor has just handled came from, `Start` standing for `init` and
/// `recovered`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Stash,
    Mailbox,
    Timer,
    Start,
}

fn message_count<M>(incomings: &[Incoming<M>]) -> usize {
    incomings
        .iter()
        .filter(|incoming| matches!(incoming, Incoming::Message(_)))
        .count()
}

#[cfg(test)]
mod tests {
    use crate::{ActorId, Incoming, MailboxCapacity, stash::Stash};
    use std::num::NonZeroUsize;

    /// A full stash hands a message back, counting the unstashed items and the requests, while a
    /// terminated signal skips the capacity.
    #[test]
    fn a_full_stash_hands_a_message_back() {
        let mut stash = Stash::new(MailboxCapacity::Bounded(
            NonZeroUsize::new(2).expect("two is non-zero"),
        ));

        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        assert_eq!(
            stash.request_stash(Incoming::Message(3)),
            Err(Incoming::Message(3))
        );
        assert!(
            stash
                .request_stash(Incoming::Terminated(ActorId::new()))
                .is_ok()
        );

        stash.request_unstash_all();
        stash.settle();
        assert_eq!(
            stash.request_stash(Incoming::Message(3)),
            Err(Incoming::Message(3))
        );
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
    }

    /// The requests of one item take effect in the order they were made: an `unstash_all` hands
    /// back what was stashed before it, and an item stashed after it stays stashed.
    #[test]
    fn requests_take_effect_in_call_order() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.settle();
        assert_eq!(stash.pop_unstashed(), None);

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        assert_eq!(stash.pop_unstashed(), None);

        stash.settle();
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        assert_eq!(stash.pop_unstashed(), None);
        assert_eq!(stash.take_stashed(), [Incoming::Message(3)]);
    }

    /// A release made during a drain which stashes every item again waits for an item from the
    /// mailbox; a due timer does not count.
    #[test]
    fn a_release_during_a_drain_waits_for_the_mailbox() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        stash.settle();

        for n in [1, 2] {
            assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(n)));
            assert!(stash.request_stash(Incoming::Message(n)).is_ok());
            stash.request_unstash_all();
            stash.settle();
        }
        assert_eq!(stash.pop_unstashed(), None);

        stash.note_timer_item();
        stash.settle();
        assert_eq!(stash.pop_unstashed(), None);

        stash.settle();
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
    }

    /// A release made while receiving a due timer takes effect at once, a held one included, since
    /// a timer is no part of a drain.
    #[test]
    fn a_release_during_a_timer_takes_effect_at_once() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle();

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle();
        assert_eq!(stash.pop_unstashed(), None);

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.request_unstash_all();
        stash.note_timer_item();
        stash.settle();
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
    }

    /// A release made during a drain which has shrunk the stash takes effect once the drain is
    /// over: of 3 waiting for 2 and 2 waiting for 1, 3 is handed back once 2 is handled.
    #[test]
    fn a_release_during_a_shrinking_drain_takes_effect_at_its_end() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.settle();
        stash.request_unstash_all();
        stash.settle();

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
        stash.request_unstash_all();
        stash.settle();

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        stash.request_unstash_all();
        stash.settle();

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert_eq!(stash.pop_unstashed(), None);
    }

    /// A restart hands every item back and discards the requests of the failed item.
    #[test]
    fn a_restart_hands_everything_back() {
        let mut stash = Stash::new(MailboxCapacity::Bounded(NonZeroUsize::MIN));
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.settle();
        assert!(stash.request_stash(Incoming::Message(2)).is_err());

        stash.request_unstash_all();
        assert!(stash.hand_back_all().is_empty());
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));

        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        assert_eq!(stash.hand_back_all(), [Incoming::Message(2)]);
        assert_eq!(stash.pop_unstashed(), None);
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());
    }

    /// Dropping the signals of an actor removes them from the unstashed and stashed items and from
    /// the requests, and a pending release still hands back the rest.
    #[test]
    fn dropping_the_signals_of_an_actor_keeps_everything_else() {
        let (dropped, kept) = (ActorId::new(), ActorId::new());
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        for incoming in [
            Incoming::Terminated(dropped),
            Incoming::Message(1),
            Incoming::Terminated(kept),
        ] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.request_unstash_all();
        stash.settle();
        for incoming in [Incoming::Terminated(dropped), Incoming::Message(2)] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.settle();
        for incoming in [Incoming::Message(3), Incoming::Terminated(dropped)] {
            assert!(stash.request_stash(incoming).is_ok());
        }
        stash.request_unstash_all();

        stash.drop_signals_of(dropped);
        stash.settle();

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Terminated(kept)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(2)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
        assert_eq!(stash.pop_unstashed(), None);
    }

    /// Taking everything yields the unstashed items, then the stashed ones, then the requests.
    #[test]
    fn take_all_yields_unstashed_then_stashed_then_requested() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.request_stash(Incoming::Message(1)).is_ok());
        stash.request_unstash_all();
        stash.settle();
        assert!(stash.request_stash(Incoming::Message(2)).is_ok());
        stash.settle();
        assert!(stash.request_stash(Incoming::Message(3)).is_ok());

        assert_eq!(
            stash.take_all(),
            [
                Incoming::Message(1),
                Incoming::Message(2),
                Incoming::Message(3)
            ]
        );
        assert_eq!(stash.pop_unstashed(), None);
    }
}
