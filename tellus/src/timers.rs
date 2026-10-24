use std::{collections::HashMap, pin::Pin, time::Duration};
use tokio::{
    runtime::Handle,
    time::{Instant, Sleep, sleep_until},
};

// A delay the clock cannot represent is capped at about thirty years, as `tokio::time::sleep` does.
const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

pub(crate) struct Timers<M> {
    next_generation: u64,
    slots: Vec<Option<Timer<M>>>,
    free_slots: Vec<usize>,
    slots_by_key: HashMap<&'static str, usize>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<M> Timers<M> {
    pub(crate) fn start(&mut self, key: &'static str, message: M, delay: Duration, now: Instant) {
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
            Some(&slot) => self.slots[slot] = Some(timer),

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
            }
        }
    }

    pub(crate) fn cancel(&mut self, key: &'static str) {
        if let Some(slot) = self.slots_by_key.remove(key) {
            self.slots[slot] = None;
            self.free_slots.push(slot);
            if self.slots_by_key.is_empty() {
                self.sleep = None;
            }
        }
    }

    pub(crate) fn armed(&self) -> bool {
        self.sleep.is_some()
    }

    pub(crate) fn sleep(&mut self) -> Option<Pin<&mut Sleep>> {
        self.sleep.as_mut().map(Pin::as_mut)
    }

    pub(crate) fn fire_due(&mut self, now: Instant) -> Option<TimerId> {
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

    pub(crate) fn rearm(&mut self) {
        let Some(deadline) = self.next_deadline() else {
            self.sleep = None;
            return;
        };

        match &mut self.sleep {
            Some(sleep) => sleep.as_mut().reset(deadline),
            None => self.sleep = new_sleep(deadline),
        }
    }

    pub(crate) fn take(&mut self, id: TimerId) -> Option<M> {
        let slot = id.slot();
        let timer = self
            .slots
            .get_mut(slot)?
            .take_if(|timer| timer.generation == id.generation())?;
        self.slots_by_key.remove(timer.key);
        self.free_slots.push(slot);
        Some(timer.message)
    }

    pub(crate) fn take_all(&mut self) -> Vec<M> {
        self.slots_by_key.clear();
        self.free_slots.clear();
        self.sleep = None;

        // next_generation is kept, so a stale item cannot match a later timer.
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
    pub(crate) fn fire(&mut self, key: &'static str) -> Option<TimerId> {
        let slot = *self.slots_by_key.get(key)?;
        let timer = self.slots[slot].as_mut()?;
        timer.deadline.take()?;
        Some(TimerId::new(slot, timer.generation))
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

    fn next_deadline(&self) -> Option<Instant> {
        self.slots
            .iter()
            .flatten()
            .filter_map(|timer| timer.deadline)
            .min()
    }
}

// A derived `Default` would needlessly require `M: Default`.
impl<M> Default for Timers<M> {
    fn default() -> Self {
        Self {
            next_generation: 1,
            slots: Vec::new(),
            free_slots: Vec::new(),
            slots_by_key: HashMap::new(),
            sleep: None,
        }
    }
}

// Bytes, not integers: an 8-byte alignment would grow the mailbox item of a small message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerId {
    slot: [u8; 4],
    generation: [u8; 8],
}

impl TimerId {
    pub(crate) fn new(slot: usize, generation: u64) -> Self {
        let slot = u32::try_from(slot).expect("an actor has fewer than 2^32 timers");

        Self {
            slot: slot.to_ne_bytes(),
            generation: generation.to_ne_bytes(),
        }
    }

    pub(crate) fn generation(self) -> u64 {
        u64::from_ne_bytes(self.generation)
    }

    fn slot(self) -> usize {
        usize::try_from(u32::from_ne_bytes(self.slot)).expect("a u32 fits a usize")
    }
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
    use crate::timers::{FAR_FUTURE, Timers};
    use std::time::Duration;
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
        let mut timers = Timers::default();
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

    /// Starting a key in use replaces the timer: the old generation is stale and the new deadline
    /// counts from the second start.
    #[test]
    fn restarting_a_key_replaces_the_timer() {
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.start("t", 1, Duration::from_secs(1), now);
        let first = timers
            .fire_due(now + Duration::from_secs(1))
            .expect("the first timer is due");
        timers.start("t", 2, Duration::from_secs(1), now + Duration::from_secs(1));
        assert_eq!(timers.next_deadline(), Some(now + Duration::from_secs(2)));
        assert_eq!(timers.take(first), None);
        let second = timers
            .fire_due(now + Duration::from_secs(2))
            .expect("the second timer is due");
        assert_eq!(timers.take(second), Some(2));
    }

    /// A canceled timer is neither due nor taken, also when it had fired already.
    #[test]
    fn canceling_drops_a_pending_and_a_fired_timer() {
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.start("pending", 1, Duration::from_secs(1), now);
        timers.start("fired", 2, Duration::ZERO, now);
        let fired = timers.fire_due(now).expect("the zero delay timer is due");
        timers.cancel("pending");
        timers.cancel("fired");
        assert_eq!(timers.next_deadline(), None);
        assert_eq!(timers.take(fired), None);
    }

    /// A stale id does not take the timer which reuses its slot, since the generation differs.
    #[test]
    fn a_stale_id_does_not_take_the_timer_reusing_its_slot() {
        let now = Instant::now();
        let mut timers = Timers::default();
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
        let mut timers = Timers::<u32>::default();
        timers.cancel("never started");
        assert_eq!(timers.next_deadline(), None);
    }

    /// The earliest pending deadline is armed, and a fired entry no longer counts.
    #[test]
    fn the_earliest_pending_deadline_is_next() {
        let now = Instant::now();
        let mut timers = Timers::default();
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
        let mut timers = Timers::default();
        for (key, seconds) in STARTED {
            timers.start(key, key, Duration::from_secs(seconds), now);
        }
        let mut fired = Vec::new();
        while let Some(id) = timers.fire_due(now + Duration::from_secs(4)) {
            fired.push(timers.take(id).expect("the timer is in the table"));
        }
        assert_eq!(fired, ["h", "f", "d", "b", "g", "e", "c", "a"]);
    }

    /// Taking everything keeps the generation counter, so a stale item from before cannot match a
    /// timer started after.
    #[test]
    fn taking_all_keeps_the_generation_counter() {
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.start("t", 1, Duration::ZERO, now);
        let stale = timers.fire_due(now).expect("the timer is due");
        assert_eq!(timers.take_all(), vec![1]);
        timers.start("t", 2, Duration::ZERO, now);
        assert_eq!(timers.take(stale), None);
    }

    #[test]
    fn an_unrepresentable_delay_is_capped() {
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.start("t", 1, Duration::MAX, now);
        assert_eq!(timers.next_deadline(), Some(now + FAR_FUTURE));
    }

    /// The first timer arms the sleep at its deadline.
    #[tokio::test(start_paused = true)]
    async fn starting_a_timer_arms_the_sleep() {
        let now = Instant::now();
        let mut timers = Timers::default();
        assert!(!timers.armed());
        timers.start("t", 1, Duration::from_secs(1), now);
        assert_eq!(sleep_deadline(&timers), Some(now + Duration::from_secs(1)));
    }

    /// A later deadline, as an idle timeout restarted on every message sets, leaves the sleep
    /// alone, while an earlier one moves it.
    #[tokio::test(start_paused = true)]
    async fn only_an_earlier_deadline_moves_the_sleep() {
        let now = Instant::now();
        let mut timers = Timers::default();
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
        let mut timers = Timers::default();
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
        let mut timers = Timers::default();
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
        let mut timers = Timers::default();
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
        let mut timers = Timers::default();
        timers.start("t", 1, Duration::from_secs(1), Instant::now());
        assert!(!timers.armed());
        assert!(timers.next_deadline().is_some());
    }

    #[cfg(feature = "test-util")]
    #[test]
    fn pending_lists_by_deadline_and_fire_marks_once() {
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.start("late", 1, Duration::from_secs(2), now);
        timers.start("early", 2, Duration::from_secs(1), now);
        assert_eq!(
            timers.pending(),
            vec![
                ("early", now + Duration::from_secs(1)),
                ("late", now + Duration::from_secs(2))
            ]
        );
        let id = timers.fire("late").expect("late is pending");
        assert_eq!(timers.fire("late"), None);
        assert_eq!(
            timers.pending(),
            vec![("early", now + Duration::from_secs(1))]
        );
        assert_eq!(timers.take(id), Some(1));
    }

    /// Pending timers are listed by deadline, and those with equal deadlines in the order they were
    /// started.
    #[cfg(feature = "test-util")]
    #[test]
    fn pending_lists_by_deadline_then_in_start_order() {
        let now = Instant::now();
        let mut timers = Timers::default();
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

    fn sleep_deadline<M>(timers: &Timers<M>) -> Option<Instant> {
        timers.sleep.as_ref().map(|sleep| sleep.deadline())
    }
}
