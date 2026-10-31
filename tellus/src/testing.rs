//! Unit testing of actor logic without a running actor system: [TestContext] provides the
//! [ActorContext] which [Actor::init] and [Actor::receive] require, as well as `handle` and
//! `recovered` of `EventSourced`, and collects what the actor sends to itself. With the
//! `persistence` feature, `settle` additionally runs the `Effect` returned by `handle` against
//! the actor's state, without any store.
//!
//! [Actor::init]: crate::Actor::init
//! [Actor::receive]: crate::Actor::receive

use crate::{
    ActorContext, ActorId, Incoming, MailboxCapacity, Timed, TimerMode, Untimed, actor_context,
    actor_ref::SelfRef, mailbox::Mailbox,
};
#[cfg(feature = "persistence")]
use crate::{
    EventSourced,
    persistence::{effect::Effect, spawn},
};
use std::{iter, time::Duration};
use tokio::time::Instant;

/// An [ActorContext] together with the mailbox behind it, for calling an actor's methods directly.
///
/// Everything the actor sends to itself arrives in the mailbox: [ActorRef::tell] on
/// [ActorContext::self_ref], the replies of a [ReplyTo] from [ActorContext::reply_to], the
/// [Incoming::Terminated] signal of [ActorContext::watch] on an actor which has already
/// terminated. [TestContext::take_incoming] returns it in FIFO order, with the message of a timer
/// fired by [TestContext::fire_timer] behind the items which were queued when it fired.
///
/// Calling an actor directly bypasses the run loop. Notably, a terminated signal passed to
/// [Actor::receive] is not dropped when its sender is no longer watched, and nothing restarts the
/// actor on failure. [ActorContext::spawn] still spawns a real child actor, hence needs a Tokio
/// runtime; the child is asked to stop when the [TestContext] is dropped, and
/// [TestContext::terminate] waits for it.
///
/// `T` is the actor's [TimerMode]; [TestContext::new] creates an [Untimed] context and
/// [TestContext::timed] a [Timed] one. No timer fires when its delay elapses; see
/// [TestContext::fire_timer]. [TestContext::take_incoming] drops canceled or replaced timers for
/// its whole batch at once, not item by item between calls to [Actor::receive].
///
/// # Testing
///
/// ```
/// use std::convert::Infallible;
/// use tellus::{Actor, ActorContext, Control, Incoming, Untimed, testing::TestContext};
///
/// struct Countdown;
///
/// impl Actor for Countdown {
///     type Message = u32;
///     type Timers = Untimed;
///     type State = u32;
///     type Error = Infallible;
///
///     fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
///         Ok(0)
///     }
///
///     fn receive(
///         &self,
///         context: &ActorContext<Self::Message>,
///         incoming: Incoming<Self::Message>,
///         sum: Self::State,
///     ) -> Result<Control<Self::State>, Self::Error> {
///         let Incoming::Message(n) = incoming else {
///             return Ok(Control::Stop);
///         };
///
///         if n > 0 {
///             context.self_ref().tell(n - 1);
///         }
///         Ok(Control::Continue(sum + n))
///     }
/// }
///
/// let mut test_context = TestContext::new();
///
/// let control = Countdown
///     .receive(test_context.context(), Incoming::Message(3), 0)
///     .unwrap();
///
/// assert_eq!(control, Control::Continue(3));
/// assert_eq!(test_context.take_incoming(), [Incoming::Message(2)]);
/// ```
///
/// [Actor::receive]: crate::Actor::receive
/// [ActorRef::tell]: crate::ActorRef::tell
/// [ReplyTo]: crate::ReplyTo
pub struct TestContext<M, T = Untimed>
where
    T: TimerMode,
{
    context: ActorContext<M, T>,
    mailbox: Mailbox<M>,
}

impl<M, T> TestContext<M, T>
where
    T: TimerMode,
{
    /// The context to pass to the actor's methods.
    pub fn context(&self) -> &ActorContext<M, T> {
        &self.context
    }

    /// Everything the actor has stashed and not unstashed yet, in the order it was stashed.
    pub fn take_stashed(&mut self) -> Vec<Incoming<M>> {
        self.context.take_stashed()
    }

    /// Everything the actor has unstashed, in the order the run loop would receive it, ahead of
    /// the mailbox.
    pub fn take_unstashed(&mut self) -> Vec<Incoming<M>> {
        iter::from_fn(|| self.context.take_unstashed()).collect()
    }

    /// Terminate this context's actor as the run loop would: stop its children and wait until all
    /// of them have terminated, and only then send [Incoming::Terminated] to its watchers.
    ///
    /// The actor value and its state are not owned by the [TestContext], hence a test of their
    /// destructors has to drop them itself.
    pub async fn terminate(self) {
        actor_context::terminate((), self.context, self.mailbox).await;
    }

    fn with_fresh_id() -> Self
    where
        M: Send + 'static,
    {
        let (self_ref, mailbox) = SelfRef::new(ActorId::new(), MailboxCapacity::Unbounded);

        Self {
            context: ActorContext::new(self_ref, MailboxCapacity::Unbounded),
            mailbox,
        }
    }
}

impl<M> TestContext<M, Untimed> {
    /// Create an [Untimed] context with a fresh [ActorId] and an unbounded mailbox. Needs no Tokio
    /// runtime.
    pub fn new() -> Self
    where
        M: Send + 'static,
    {
        Self::with_fresh_id()
    }

    /// Create a [Timed] context with a fresh [ActorId] and an unbounded mailbox. Needs no Tokio
    /// runtime; without one, no timer is armed, and [TestContext::fire_timer] still fires it.
    pub fn timed() -> TestContext<M, Timed>
    where
        M: Send + 'static,
    {
        TestContext::with_fresh_id()
    }

    /// Everything enqueued in the mailbox since the last call, in the order it arrived. Does not
    /// wait for more.
    pub fn take_incoming(&mut self) -> Vec<Incoming<M>> {
        iter::from_fn(|| self.mailbox.try_recv()).collect()
    }
}

impl<M> TestContext<M, Timed> {
    /// Everything enqueued in the mailbox since the last call, in the order it arrived. Does not
    /// wait for more. Like the run loop, it drops a fired timer which the actor has canceled or
    /// replaced since. Unlike the run loop, it does so for the whole batch at once rather than item
    /// by item between calls to [Actor::receive]. A cancel made while handling an earlier item
    /// hence cannot drop a timer behind it. To test that, pass the earlier item to
    /// [Actor::receive] directly and take the incoming afterwards.
    ///
    /// [Actor::receive]: crate::Actor::receive
    pub fn take_incoming(&mut self) -> Vec<Incoming<M>> {
        iter::from_fn(|| {
            loop {
                if self.context.timer_deliverable() {
                    if let Some(message) = self.context.take_due_timer() {
                        return Some(Incoming::Message(message));
                    }
                } else {
                    let incoming = self.mailbox.try_recv()?;
                    self.context.count_received();
                    return Some(incoming);
                }
            }
        })
        .collect()
    }

    /// The timers the actor has started which have not fired yet, earliest deadline first, those
    /// with equal deadlines in the order they were started. However much time passes, a timer stays
    /// listed until [TestContext::fire_timer] fires it or the actor cancels or replaces it.
    ///
    /// The time remaining is measured on Tokio's clock. A test which asserts it pauses that clock,
    /// e.g. with `#[tokio::test(start_paused = true)]`.
    pub fn timers(&self) -> Vec<PendingTimer> {
        let now = Instant::now();

        self.context
            .pending_timers()
            .into_iter()
            .map(|(key, deadline)| PendingTimer {
                key,
                remaining: deadline.saturating_duration_since(now),
            })
            .collect()
    }

    /// Fire the timer started under this key as the run loop does once its delay has elapsed,
    /// whatever time is left. Its message is placed behind the items already in the mailbox, and
    /// [TestContext::take_incoming] returns it unless the actor cancels or replaces the timer
    /// first. Returns whether there was a timer to fire, that is `false` for an unknown key and for
    /// a timer which has fired already.
    pub fn fire_timer(&mut self, key: &'static str) -> bool {
        self.context.fire_timer(key)
    }
}

impl<M> Default for TestContext<M, Untimed>
where
    M: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A timer which an actor has started and which has not fired yet, as listed by
/// [TestContext::timers].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingTimer {
    /// The key the timer was started under.
    pub key: &'static str,

    /// The time left until the timer is due, zero once its deadline has passed.
    pub remaining: Duration,
}

/// The outcome of [settle].
#[cfg(feature = "persistence")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settlement<S> {
    /// The state after the events of the settled [Effect] have been applied.
    pub state: S,

    /// Whether the settled [Effect] stops the actor.
    pub stop: bool,
}

/// Settle the given [Effect] as an event-sourced actor would once its events are durable: apply
/// the events to the state in order via [EventSourced::apply], then run the [then] continuations
/// in order on the resulting state. Replies and messages sent by a continuation reach the
/// [TestContext] they were addressed to.
///
/// Encoding, appending, snapshots and sequence numbers are not simulated, and a panic in `apply`
/// or in a continuation is not caught but fails the test.
///
/// # Testing
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use std::convert::Infallible;
/// use tellus::{
///     ActorContext, Effect, EventSourced, Incoming, Nothing, PersistenceId, ReplyTo,
///     SchemaVersion, Untimed, Versioned,
///     testing::{TestContext, settle},
/// };
///
/// struct Counter;
///
/// enum Command {
///     Increase(ReplyTo<u64>),
///     Total(u64),
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct Increased;
///
/// impl Versioned for Increased {
///     const MANIFEST: &'static str = "increased";
///     const VERSION: SchemaVersion = SchemaVersion::new(1);
/// }
///
/// impl EventSourced for Counter {
///     type Command = Command;
///     type Timers = Untimed;
///     type Event = Increased;
///     type State = u64;
///     type Snapshot = Nothing;
///     type Error = Infallible;
///
///     fn persistence_id(&self) -> PersistenceId {
///         PersistenceId::new("counter", "1").expect("the segments are valid")
///     }
///
///     fn init(&self) -> Result<Self::State, Self::Error> {
///         Ok(0)
///     }
///
///     fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
///         match snapshot {}
///     }
///
///     fn handle(
///         &self,
///         _: &ActorContext<Self::Command>,
///         incoming: Incoming<Self::Command>,
///         _: &Self::State,
///     ) -> Result<Effect<Self>, Self::Error> {
///         match incoming {
///             Incoming::Message(Command::Increase(reply_to)) => {
///                 Ok(Effect::persist(Increased).then(move |count| reply_to.reply(*count)))
///             }
///
///             _ => Ok(Effect::none()),
///         }
///     }
///
///     fn apply(&self, count: Self::State, _: Self::Event) -> Self::State {
///         count + 1
///     }
/// }
///
/// let mut test_context = TestContext::new();
/// let reply_to = test_context.context().reply_to(Command::Total);
///
/// let effect = Counter
///     .handle(
///         test_context.context(),
///         Incoming::Message(Command::Increase(reply_to)),
///         &0,
///     )
///     .unwrap();
/// assert_eq!(effect.events().len(), 1);
/// assert!(!effect.stop_requested());
///
/// let settlement = settle(&Counter, 0, effect);
/// assert_eq!(settlement.state, 1);
/// assert!(!settlement.stop);
/// assert!(matches!(
///     test_context.take_incoming().as_slice(),
///     [Incoming::Message(Command::Total(1))]
/// ));
/// ```
///
/// [then]: crate::Effect::then
#[cfg(feature = "persistence")]
pub fn settle<A>(actor: &A, state: A::State, effect: Effect<A>) -> Settlement<A::State>
where
    A: EventSourced,
{
    let Effect {
        events,
        stop,
        thens,
    } = effect;

    let state = spawn::apply_events(actor, state, events);
    for then in thens {
        then(&state);
    }

    Settlement { state, stop }
}
