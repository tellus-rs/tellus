mod private {
    use crate::{Actor, ActorConfig, ActorId, ActorRef};
    #[cfg(feature = "persistence")]
    use crate::{Codec, EventSourced, EventStore, Persistence, SnapshotStore};
    use tokio::sync::watch;

    pub trait Sealed
    where
        Self: Sized + 'static,
    {
        type Table<M>: Default;

        fn drop_timer_messages<M>(token: Token, actor_id: ActorId, table: &mut Self::Table<M>);

        fn spawn<A>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            config: ActorConfig,
        ) -> ActorRef<A::Message>
        where
            A: Actor<Timers = Self> + Send + 'static,
            A::Message: Send + 'static,
            A::State: Send + 'static;

        #[cfg(feature = "persistence")]
        fn spawn_event_sourced<A, E, S, C>(
            token: Token,
            stopped_by_parent_rx: watch::Receiver<()>,
            actor: A,
            persistence: Persistence<E, S, C>,
            config: ActorConfig,
        ) -> ActorRef<A::Command>
        where
            A: EventSourced<Timers = Self> + Send + Sync + 'static,
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
    Actor, ActorConfig, ActorId, ActorRef,
    actor_context::{self, drop_containing_panic},
};
#[cfg(feature = "persistence")]
use crate::{Codec, EventSourced, EventStore, Persistence, SnapshotStore, persistence::spawn};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    pin::Pin,
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::watch,
    time::{Instant, Sleep, sleep_until},
};

// A delay the clock cannot represent is capped at about thirty years, as `tokio::time::sleep` does.
const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

pub(crate) const TIMER_FAILED_TO_DROP: &str = "timer message failed to drop";

/// Whether an actor uses timers, chosen per actor type via [Actor::Timers] and
/// [EventSourced::Timers]: [Untimed] or [Timed]. Sealed: these two are the only modes.
///
/// An [Untimed] actor cannot start a timer; its context has no `start_timer`:
///
/// ```compile_fail,E0599
/// use std::{convert::Infallible, time::Duration};
/// use tellus::{Actor, ActorContext, Control, Incoming, Unstashed, Untimed};
///
/// struct Quiet;
///
/// impl Actor for Quiet {
///     type Message = ();
///     type Timers = Untimed;
///     type Stash = Unstashed;
///     type State = ();
///     type Error = Infallible;
///
///     fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
///         context.start_timer("t", (), Duration::from_secs(1));
///         Ok(())
///     }
///
///     fn receive(
///         &self,
///         _: &ActorContext<Self::Message>,
///         _: Incoming<Self::Message>,
///         _: Self::State,
///     ) -> Result<Control<Self::State>, Self::Error> {
///         Ok(Control::Stop)
///     }
/// }
/// ```
///
/// The run loop an actor gets is chosen by its mode when it is spawned; nothing outside tellus can
/// spawn one another way:
///
/// ```compile_fail,E0277
/// use tellus::{Actor, ActorConfig, ActorRef, TimerMode};
/// use tokio::sync::watch;
///
/// fn spawn_anyway<A, T>(actor: A, stopped: watch::Receiver<()>) -> ActorRef<A::Message>
/// where
///     A: Actor<Timers = T> + Send + 'static,
///     A::Message: Send + 'static,
///     A::State: Send + 'static,
///     T: TimerMode,
/// {
///     T::spawn(Default::default(), stopped, actor, ActorConfig::default())
/// }
/// ```
///
/// [Actor::Timers]: crate::Actor::Timers
/// [EventSourced::Timers]: crate::EventSourced::Timers
pub trait TimerMode
where
    Self: Sealed,
{
}

/// The mode of an actor without timers: its [ActorContext](crate::ActorContext) has no
/// `start_timer`, and its run loop waits for the stop signal and the mailbox alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Untimed;

impl TimerMode for Untimed {}

impl Sealed for Untimed {
    type Table<M> = ();

    fn drop_timer_messages<M>(_: Token, _: ActorId, _: &mut Self::Table<M>) {}

    fn spawn<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_untimed(stopped_by_parent_rx, actor, config)
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_untimed(stopped_by_parent_rx, actor, persistence, config)
    }
}

/// The mode of an actor with timers: its [ActorContext](crate::ActorContext) has `start_timer` and
/// `cancel_timer`, and its run loop also waits for its timers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Timed;

impl TimerMode for Timed {}

impl Sealed for Timed {
    type Table<M> = RefCell<TimerTable<M>>;

    fn drop_timer_messages<M>(_: Token, actor_id: ActorId, table: &mut Self::Table<M>) {
        for message in table.get_mut().take_all() {
            drop_containing_panic(actor_id, TIMER_FAILED_TO_DROP, message);
        }
    }

    fn spawn<A>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        config: ActorConfig,
    ) -> ActorRef<A::Message>
    where
        A: Actor<Timers = Self> + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        actor_context::spawn_timed(stopped_by_parent_rx, actor, config)
    }

    #[cfg(feature = "persistence")]
    fn spawn_event_sourced<A, E, S, C>(
        _: Token,
        stopped_by_parent_rx: watch::Receiver<()>,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced<Timers = Self> + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn::spawn_event_sourced_timed(stopped_by_parent_rx, actor, persistence, config)
    }
}

// Named by `Sealed::Table` of a `pub` trait, so `pub`; the module is private.
pub struct TimerTable<M> {
    next_generation: u64,
    slots: Vec<Option<Timer<M>>>,
    free_slots: Vec<usize>,
    slots_by_key: HashMap<&'static str, usize>,
    sleep: Option<Pin<Box<Sleep>>>,
    received: u64,
    due_timers: VecDeque<DueTimer>,
}

impl<M> TimerTable<M> {
    pub(crate) fn start(
        &mut self,
        key: &'static str,
        message: M,
        delay: Duration,
        now: Instant,
    ) -> Option<M> {
        let deadline = now.checked_add(delay).unwrap_or_else(|| now + FAR_FUTURE);
        // Before the table changes: without the time driver, creating the sleep panics.
        self.arm(deadline);

        let generation = self.next_generation;
        self.next_generation += 1;
        let timer = Timer {
            key,
            generation,
            deadline: Some(deadline),
            message,
        };
        match self.slots_by_key.get(key) {
            Some(&slot) => self.slots[slot].replace(timer).map(|timer| timer.message),

            None => {
                let slot = match self.free_slots.pop() {
                    Some(slot) => {
                        self.slots[slot] = Some(timer);
                        slot
                    }

                    None => {
                        self.slots.push(Some(timer));
                        self.slots.len() - 1
                    }
                };
                self.slots_by_key.insert(key, slot);
                None
            }
        }
    }

    pub(crate) fn cancel(&mut self, key: &'static str) -> Option<M> {
        let slot = self.slots_by_key.remove(key)?;
        let canceled = self.slots[slot].take().map(|timer| timer.message);
        self.free_slots.push(slot);
        if self.slots_by_key.is_empty() {
            self.sleep = None;
        }
        canceled
    }

    pub(crate) fn armed(&self) -> bool {
        self.sleep.is_some()
    }

    pub(crate) fn sleep(&mut self) -> Option<Pin<&mut Sleep>> {
        self.sleep.as_mut().map(Pin::as_mut)
    }

    // The clock before the depth, else a deadline passing in between overtakes the items queued.
    pub(crate) fn fire_due_timers<N, Q>(&mut self, now: N, queued: Q)
    where
        N: FnOnce() -> Instant,
        Q: FnOnce() -> usize,
    {
        let now = now();
        let deliver_at = self.position_after(queued());
        while let Some(id) = self.fire_due(now) {
            self.due_timers.push_back(DueTimer { deliver_at, id });
        }
        self.rearm();
    }

    pub(crate) fn count_received(&mut self) {
        self.received += 1;
    }

    pub(crate) fn deliverable(&self) -> bool {
        self.due_timers
            .front()
            .is_some_and(|due_timer| due_timer.deliver_at <= self.received)
    }

    // One entry per call, stale or not: the run loop yields between calls!
    pub(crate) fn take_deliverable(&mut self) -> Option<Delivery<M>> {
        if !self.deliverable() {
            return None;
        }
        let DueTimer { id, .. } = self.due_timers.pop_front()?;

        Some(match self.take(id) {
            Some(message) => Delivery::Message(message),
            None => Delivery::Stale(id),
        })
    }

    pub(crate) fn take_all(&mut self) -> Vec<M> {
        self.slots_by_key.clear();
        self.free_slots.clear();
        self.sleep = None;
        self.due_timers.clear();

        self.slots
            .drain(..)
            .flatten()
            .map(|timer| timer.message)
            .collect()
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn pending(&self) -> Vec<(&'static str, Instant)> {
        let mut pending = self
            .slots
            .iter()
            .flatten()
            .filter_map(|timer| Some((timer.deadline?, timer.generation, timer.key)))
            .collect::<Vec<_>>();
        pending.sort_unstable();
        pending
            .into_iter()
            .map(|(deadline, _, key)| (key, deadline))
            .collect()
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn fire(&mut self, key: &'static str, queued: usize) -> bool {
        let deliver_at = self.position_after(queued);
        let Some(&slot) = self.slots_by_key.get(key) else {
            return false;
        };
        let Some(timer) = self.slots[slot].as_mut() else {
            return false;
        };
        if timer.deadline.take().is_none() {
            return false;
        }
        let id = TimerId::new(slot, timer.generation);
        self.due_timers.push_back(DueTimer { deliver_at, id });
        true
    }

    fn arm(&mut self, deadline: Instant) {
        match &mut self.sleep {
            Some(sleep) => {
                if deadline < sleep.deadline() {
                    sleep.as_mut().reset(deadline);
                }
            }

            None => self.sleep = new_sleep(deadline),
        }
    }

    fn position_after(&self, queued: usize) -> u64 {
        let queued = u64::try_from(queued).expect("a usize fits a u64");
        let deliver_at = self.received + queued;
        debug_assert!(
            self.due_timers
                .back()
                .is_none_or(|due_timer| due_timer.deliver_at <= deliver_at),
            "due timers are ordered by position"
        );
        deliver_at
    }

    fn fire_due(&mut self, now: Instant) -> Option<TimerId> {
        self.slots
            .iter_mut()
            .enumerate()
            .filter_map(|(slot, timer)| Some((slot, timer.as_mut()?)))
            .filter(|(_, timer)| timer.deadline.is_some_and(|deadline| deadline <= now))
            .min_by_key(|(_, timer)| (timer.deadline, timer.generation))
            .map(|(slot, timer)| {
                // Fired, not removed: a cancel must still reach it.
                timer.deadline = None;
                TimerId::new(slot, timer.generation)
            })
    }

    fn rearm(&mut self) {
        let Some(deadline) = self.next_deadline() else {
            self.sleep = None;
            return;
        };

        match &mut self.sleep {
            Some(sleep) => sleep.as_mut().reset(deadline),
            None => self.sleep = new_sleep(deadline),
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.slots
            .iter()
            .flatten()
            .filter_map(|timer| timer.deadline)
            .min()
    }

    fn take(&mut self, id: TimerId) -> Option<M> {
        let slot = id.slot();
        let timer = self
            .slots
            .get_mut(slot)?
            .take_if(|timer| timer.generation == id.generation())?;
        self.slots_by_key.remove(timer.key);
        self.free_slots.push(slot);
        Some(timer.message)
    }
}

// A derived `Default` would needlessly require `M: Default`.
impl<M> Default for TimerTable<M> {
    fn default() -> Self {
        Self {
            next_generation: 1,
            slots: Vec::new(),
            free_slots: Vec::new(),
            slots_by_key: HashMap::new(),
            sleep: None,
            received: 0,
            due_timers: VecDeque::new(),
        }
    }
}

pub(crate) enum Delivery<M> {
    Message(M),

    Stale(TimerId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerId {
    slot: u32,
    generation: u64,
}

impl TimerId {
    pub(crate) fn generation(self) -> u64 {
        self.generation
    }

    fn new(slot: usize, generation: u64) -> Self {
        let slot = u32::try_from(slot).expect("an actor has fewer than 2^32 timers");

        Self { slot, generation }
    }

    fn slot(self) -> usize {
        usize::try_from(self.slot).expect("a u32 fits a usize")
    }
}

// Deliverable once `deliver_at` items have been received since the table was created.
struct DueTimer {
    deliver_at: u64,
    id: TimerId,
}

struct Timer<M> {
    key: &'static str,
    generation: u64,

    // None once fired.
    deadline: Option<Instant>,

    message: M,
}

// Outside a runtime, as for a TestContext in a plain test, no run loop polls a sleep.
fn new_sleep(deadline: Instant) -> Option<Pin<Box<Sleep>>> {
    Handle::try_current()
        .is_ok()
        .then(|| Box::pin(sleep_until(deadline)))
}

#[cfg(test)]
mod tests {
    use crate::timers::{Delivery, FAR_FUTURE, TimerTable};
    use std::{cell::Cell, time::Duration};
    use tokio::time::Instant;

    // Key and delay in seconds, started out of deadline order, equal delays in reverse key order.
    const STARTED: [(&str, u64); 8] = [
        ("a", 3),
        ("h", 1),
        ("g", 2),
        ("f", 1),
        ("e", 2),
        ("d", 1),
        ("c", 2),
        ("b", 1),
    ];

    /// A started timer is pending at its deadline and gone after it is taken.
    #[test]
    fn starting_then_taking_delivers_the_message_once() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::from_secs(1), now);
        assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(1)));
        assert_eq!(timers.fire_due(now), None);
        let id = timers
            .fire_due(now + Duration::from_secs(1))
            .expect("the timer is due");
        assert_eq!(timers.next_deadline(), None);
        assert_eq!(timers.take(id), Some(1));
        assert_eq!(timers.take(id), None);
    }

    /// Starting a key in use replaces the timer: the old message is handed back, the old generation
    /// is stale and the new deadline counts from the second start.
    #[test]
    fn restarting_a_key_replaces_the_timer() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        assert_eq!(timers.start("t", 1, Duration::from_secs(1), now), None);
        let first = timers
            .fire_due(now + Duration::from_secs(1))
            .expect("the first timer is due");
        let replaced = timers.start("t", 2, Duration::from_secs(1), now + Duration::from_secs(1));
        assert_eq!(replaced, Some(1));
        assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(2)));
        assert_eq!(timers.take(first), None);
        let second = timers
            .fire_due(now + Duration::from_secs(2))
            .expect("the second timer is due");
        assert_eq!(timers.take(second), Some(2));
    }

    /// A canceled timer is handed back, neither due nor taken, also when it had fired already.
    #[test]
    fn canceling_drops_a_pending_and_a_fired_timer() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("pending", 1, Duration::from_secs(1), now);
        timers.start("fired", 2, Duration::ZERO, now);
        let fired = timers.fire_due(now).expect("the zero delay timer is due");
        assert_eq!(timers.cancel("pending"), Some(1));
        assert_eq!(timers.cancel("fired"), Some(2));
        assert_eq!(timers.next_deadline(), None);
        assert_eq!(timers.take(fired), None);
    }

    /// A stale id does not take the timer which reuses its slot, since the generation differs.
    #[test]
    fn a_stale_id_does_not_take_the_timer_reusing_its_slot() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("a", 1, Duration::ZERO, now);
        let stale = timers.fire_due(now).expect("the first timer is due");
        timers.cancel("a");
        timers.start("b", 2, Duration::ZERO, now);
        let fresh = timers.fire_due(now).expect("the second timer is due");
        assert_eq!(timers.take(stale), None);
        assert_eq!(timers.take(fresh), Some(2));
    }

    #[test]
    fn canceling_an_unknown_key_is_a_noop() {
        let mut timers = TimerTable::<u32>::default();
        assert_eq!(timers.cancel("never started"), None);
        assert_eq!(timers.next_deadline(), None);
    }

    /// The earliest pending deadline is armed, and a fired entry no longer counts.
    #[test]
    fn the_earliest_pending_deadline_is_next() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("late", 1, Duration::from_secs(3), now);
        timers.start("early", 2, Duration::from_secs(1), now);
        assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(1)));
        timers
            .fire_due(now + Duration::from_secs(1))
            .expect("the early timer is due");
        assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(3)));
    }

    /// Timers fire by deadline, and those with equal deadlines in the order they were started.
    #[test]
    fn due_timers_fire_by_deadline_then_in_start_order() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        for (key, seconds) in STARTED {
            timers.start(key, key, Duration::from_secs(seconds), now);
        }
        let mut fired = Vec::new();
        while let Some(id) = timers.fire_due(now + Duration::from_secs(4)) {
            fired.push(timers.take(id).expect("the timer is in the table"));
        }
        assert_eq!(fired, ["h", "f", "d", "b", "g", "e", "c", "a"]);
    }

    /// Taking everything keeps the generation counter, so a stale id from before cannot match a
    /// timer started after.
    #[test]
    fn taking_all_keeps_the_generation_counter() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::ZERO, now);
        let stale = timers.fire_due(now).expect("the timer is due");
        assert_eq!(timers.take_all(), vec![1]);
        timers.start("t", 2, Duration::ZERO, now);
        assert_eq!(timers.take(stale), None);
    }

    /// A fired timer is deliverable only once the items queued when it fired have been received.
    #[test]
    fn a_fired_timer_waits_for_the_items_queued_before_it() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::ZERO, now);
        timers.fire_due_timers(|| now, || 2);
        assert!(timers.take_deliverable().is_none());
        timers.count_received();
        assert!(!timers.deliverable());
        timers.count_received();
        assert!(timers.deliverable());
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Message(1))
        ));
        assert!(!timers.deliverable());
    }

    /// The clock is read before the mailbox depth. Whichever is read second sees a message queued
    /// meanwhile and a second deadline passed meanwhile; that second timer is left for the next
    /// firing and lands behind the message, which arrived before its deadline.
    #[test]
    fn a_deadline_passing_while_sampling_waits_for_the_next_firing() {
        let before = Instant::now();
        let after = before + Duration::from_millis(1);
        let mut timers = TimerTable::default();
        timers.start("a", 1, Duration::ZERO, before);
        timers.start("b", 2, Duration::from_millis(1), before);

        let sampled = Cell::new(false);
        timers.fire_due_timers(
            || if sampled.replace(true) { after } else { before },
            || if sampled.replace(true) { 1 } else { 0 },
        );
        assert!(!timers.deliverable());

        timers.fire_due_timers(|| after, || 1);
        assert!(!timers.deliverable());

        timers.count_received();
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Message(1))
        ));
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Message(2))
        ));
    }

    /// Each call pops exactly one entry, also when several stale entries are deliverable at once,
    /// so a caller looping over calls can yield between them.
    #[test]
    fn taking_a_deliverable_timer_pops_one_entry_per_call() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        for message in 1..=3 {
            timers.start("t", message, Duration::ZERO, now);
            timers.fire_due_timers(|| now, || 1);
        }
        timers.count_received();
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Stale(_))
        ));
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Stale(_))
        ));
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Message(3))
        ));
        assert!(timers.take_deliverable().is_none());
    }

    /// Timers due at one wake-up share their position and are delivered by deadline, those with
    /// equal deadlines in the order they were started.
    #[test]
    fn timers_due_at_one_wake_up_are_delivered_in_firing_order() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        for (key, seconds) in STARTED {
            timers.start(key, key, Duration::from_secs(seconds), now);
        }
        timers.fire_due_timers(|| now + Duration::from_secs(4), || 0);
        let mut delivered = Vec::new();
        while let Some(delivery) = timers.take_deliverable() {
            let Delivery::Message(key) = delivery else {
                panic!("no timer was canceled or replaced");
            };
            delivered.push(key);
        }
        assert_eq!(delivered, ["h", "f", "d", "b", "g", "e", "c", "a"]);
    }

    /// Taking everything also drops the fired timers waiting for their position.
    #[test]
    fn taking_all_drops_the_fired_timers() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::ZERO, now);
        timers.fire_due_timers(|| now, || 1);
        assert_eq!(timers.take_all(), vec![1]);
        timers.count_received();
        assert!(timers.take_deliverable().is_none());
    }

    #[test]
    fn an_unrepresentable_delay_is_capped() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::MAX, now);
        assert_eq!(timers.next_deadline(), Some(now + FAR_FUTURE));
    }

    /// The first timer arms the sleep at its deadline.
    #[tokio::test(start_paused = true)]
    async fn starting_a_timer_arms_the_sleep() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        assert!(!timers.armed());
        timers.start("t", 1, Duration::from_secs(1), now);
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(1)));
    }

    /// A later deadline, as an idle timeout restarted on every message sets, leaves the sleep
    /// alone, while an earlier one moves it.
    #[tokio::test(start_paused = true)]
    async fn only_an_earlier_deadline_moves_the_sleep() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::from_secs(2), now);
        timers.start("t", 2, Duration::from_secs(3), now);
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(2)));
        timers.start("u", 3, Duration::from_secs(1), now);
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(1)));
    }

    /// Rearming after an early wake-up sets the sleep to the earliest pending deadline.
    #[tokio::test(start_paused = true)]
    async fn rearming_sets_the_sleep_to_the_next_deadline() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::from_secs(1), now);
        timers.start("t", 2, Duration::from_secs(3), now);
        assert_eq!(timers.fire_due(now + Duration::from_secs(1)), None);
        timers.rearm();
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(3)));
    }

    /// Canceling leaves the sleep alone, even for the earliest timer, until the table is empty.
    #[tokio::test(start_paused = true)]
    async fn canceling_leaves_the_sleep_until_the_table_is_empty() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("a", 1, Duration::from_secs(1), now);
        timers.start("b", 2, Duration::from_secs(2), now);
        timers.cancel("a");
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(1)));
        timers.cancel("b");
        assert!(!timers.armed());
    }

    /// Without a pending timer the sleep is dropped, whether the last one fired or was taken with
    /// all others.
    #[tokio::test(start_paused = true)]
    async fn the_sleep_is_dropped_once_no_timer_is_pending() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::from_secs(1), now);
        timers
            .fire_due(now + Duration::from_secs(1))
            .expect("the timer is due");
        timers.rearm();
        assert!(!timers.armed());

        timers.start("u", 2, Duration::from_secs(1), now);
        timers.take_all();
        assert!(!timers.armed());
    }

    /// Outside a runtime the table records timers without a sleep.
    #[test]
    fn outside_a_runtime_no_sleep_is_armed() {
        let mut timers = TimerTable::default();
        timers.start("t", 1, Duration::from_secs(1), Instant::now());
        assert!(!timers.armed());
        assert!(timers.next_deadline().is_some());
    }

    #[cfg(feature = "test-util")]
    #[test]
    fn pending_lists_by_deadline_and_fire_marks_once() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        timers.start("late", 1, Duration::from_secs(2), now);
        timers.start("early", 2, Duration::from_secs(1), now);
        assert_eq!(
            timers.pending(),
            vec![
                ("early", now + Duration::from_secs(1)),
                ("late", now + Duration::from_secs(2))
            ]
        );
        assert!(timers.fire("late", 0));
        assert!(!timers.fire("late", 0));
        assert_eq!(
            timers.pending(),
            vec![("early", now + Duration::from_secs(1))]
        );
        assert!(matches!(
            timers.take_deliverable(),
            Some(Delivery::Message(1))
        ));
    }

    /// Pending timers are listed by deadline, and those with equal deadlines in the order they were
    /// started.
    #[cfg(feature = "test-util")]
    #[test]
    fn pending_lists_by_deadline_then_in_start_order() {
        let now = Instant::now();
        let mut timers = TimerTable::default();
        for (key, seconds) in STARTED {
            timers.start(key, (), Duration::from_secs(seconds), now);
        }
        let keys = timers
            .pending()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(keys, ["h", "f", "d", "b", "g", "e", "c", "a"]);
    }

    fn sleep_deadline<M>(timers: &TimerTable<M>) -> Option<Instant> {
        timers.sleep.as_ref().map(|sleep| sleep.deadline())
    }
}
