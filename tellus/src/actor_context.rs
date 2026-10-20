use crate::{
    Actor, ActorConfig, ActorId, ActorRef, Control, Incoming, ReplyTo, SupervisionStrategy,
    actor_ref::SelfRef,
    mailbox::{Mailbox, WatcherRegistry},
};
use derive_more::Debug;
use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    future::{Future, poll_fn},
    mem,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    task::Poll,
    time::Duration,
};
use tokio::{
    select,
    sync::watch,
    task::{self, consume_budget},
    time::{Instant, sleep},
};
use tracing::{debug, error};

pub(crate) const STATE_FAILED_TO_DROP: &str = "actor state failed to drop";

/// Contextual methods for a given actor, provided to [Actor::init] and [Actor::receive].
///
/// A context belongs to its actor's task, hence it is deliberately not [Sync].
#[derive(Debug)]
pub struct ActorContext<M> {
    self_ref: SelfRef<M>,

    #[debug(skip)]
    stopping_tx: watch::Sender<()>,

    #[debug(skip)]
    stopping_rx: watch::Receiver<()>,

    #[debug(skip)]
    watched: RefCell<HashMap<ActorId, WatcherRegistry>>,
}

impl<M> ActorContext<M> {
    /// The reference for the actor itself.
    pub fn self_ref(&self) -> &ActorRef<M> {
        self.self_ref.actor_ref()
    }

    /// Spawn a child actor with the given [Actor], using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn<A>(&self, actor: A) -> ActorRef<A::Message>
    where
        A: Actor + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        self.spawn_with_config(actor, ActorConfig::default())
    }

    /// Spawn a child actor with the given [Actor] and [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_with_config<A>(&self, actor: A, config: ActorConfig) -> ActorRef<A::Message>
    where
        A: Actor + Send + 'static,
        A::Message: Send + 'static,
        A::State: Send + 'static,
    {
        spawn(self.stopping_rx.clone(), actor, config)
    }

    /// Create a [ReplyTo] which delivers the reply to this actor as an ordinary message,
    /// converted by the given function, typically an enum variant constructor. This is the actor
    /// side of request-response: no future is created or awaited, the reply arrives via
    /// [Actor::receive] like any other message.
    ///
    /// The reply takes the same path as an [ActorRef::tell] to this actor: it counts against a
    /// bounded mailbox capacity and is dropped and logged as a dead letter if this actor has
    /// terminated or its mailbox is full.
    pub fn reply_to<R, F>(&self, into_message: F) -> ReplyTo<R>
    where
        F: FnOnce(R) -> M + Send + 'static,
        M: Send + 'static,
        R: 'static,
    {
        let actor_ref = self.self_ref().clone();
        ReplyTo::new(move |reply| actor_ref.tell(into_message(reply)))
    }

    /// Run the given future on a new Tokio task and deliver its output to this actor as an
    /// ordinary message, converted by the given function, typically an enum variant constructor.
    /// This is how an actor uses async code without awaiting: it keeps handling messages while
    /// the future runs, and the output arrives via [Actor::receive] like any other message.
    ///
    /// The message takes the same path as an [ActorRef::tell] to this actor: it counts against a
    /// bounded mailbox capacity and is dropped and logged as a dead letter if this actor has
    /// terminated or its mailbox is full. The future is not cancelled when this actor stops or
    /// restarts; after a restart, its message is received by the new state. A panic in the future
    /// or in the conversion is logged at error level, and no message is delivered.
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn pipe_to_self<F, G>(&self, future: F, into_message: G)
    where
        F: Future + Send + 'static,
        G: FnOnce(F::Output) -> M + Send + 'static,
        M: Send + 'static,
    {
        let actor_ref = self.self_ref().clone();

        task::spawn(async move {
            let actor_id = actor_ref.actor_id();
            let piped = async move { into_message(future.await) };
            if let Some(message) =
                catch_panic_and_log_async(actor_id, "piped future failed", piped).await
            {
                actor_ref.tell(message);
            }
        });
    }

    /// Watch another actor, i.e. receive an [Incoming::Terminated] signal once that actor has
    /// terminated. If it has already terminated, the signal is received right away. Watching an
    /// already watched actor again has no effect: the signal is delivered once.
    ///
    /// The signal is ordered behind all messages the other actor has delivered to this actor,
    /// hence receiving it proves that this actor has seen every message from the other one it will
    /// ever see: each arrived before the signal or was dropped as a dead letter.
    ///
    /// [Incoming::Terminated]: crate::Incoming::Terminated
    pub fn watch<N>(&self, other: &ActorRef<N>) {
        let registry = other.watcher_registry().clone();
        let registration = registry.add(self.self_ref.make_watcher());
        self.watched.borrow_mut().insert(other.actor_id(), registry);

        if registration.is_err() {
            self.self_ref.send_terminated(other.actor_id());
        }
    }

    /// Stop watching another actor: no terminated signal for it will be received anymore, even if
    /// it has already terminated and the signal is already enqueued. Unwatching an actor which is
    /// not watched, e.g. because it was never watched or its signal has already been received, has
    /// no effect.
    pub fn unwatch<N>(&self, other: &ActorRef<N>) {
        if let Some(registry) = self.watched.borrow_mut().remove(&other.actor_id()) {
            registry.remove(self.self_ref().actor_id());
        }
    }

    /// The number of incoming items queued for this actor, messages and terminated signals alike,
    /// which it has not received yet. For the actor itself the value is a lower bound: only the
    /// actor removes items and senders can only add to them, so at least this many are waiting.
    /// The run loop drops the terminated signals of unwatched actors before [Actor::receive] sees
    /// them, so the count can exceed what `receive` will get by that many.
    ///
    /// This is for an actor's own behavior, e.g. flushing a batch once its mailbox has run empty
    /// or shedding load above a threshold, as part of its protocol. It is no substitute for flow
    /// control, since senders do not see it; for that, see [MailboxCapacity::Bounded].
    ///
    /// [MailboxCapacity::Bounded]: crate::MailboxCapacity::Bounded
    pub fn queued(&self) -> usize {
        self.self_ref().queued()
    }

    #[cfg(feature = "persistence")]
    pub(crate) fn stopping_rx(&self) -> watch::Receiver<()> {
        self.stopping_rx.clone()
    }

    pub(crate) fn new(self_ref: SelfRef<M>) -> Self {
        let (stopping_tx, stopping_rx) = watch::channel(());

        Self {
            self_ref,
            stopping_tx,
            stopping_rx,
            watched: RefCell::new(HashMap::new()),
        }
    }

    pub(crate) fn take_watched_for(&mut self, other_id: ActorId) -> bool {
        self.watched.get_mut().remove(&other_id).is_some()
    }

    /// Install the next generation's receiver before awaiting the current sender, else this
    /// context's own receiver keeps the channel open and `closed` never resolves.
    async fn stop_children(&mut self) {
        let (next_stopping_tx, next_stopping_rx) = watch::channel(());

        let stopping_tx = mem::replace(&mut self.stopping_tx, next_stopping_tx);
        let _ = stopping_tx.send(());
        self.stopping_rx = next_stopping_rx;

        stopping_tx.closed().await;
    }

    fn take_watched(&mut self) -> HashMap<ActorId, WatcherRegistry> {
        mem::take(self.watched.get_mut())
    }
}

/// `dyn Any` formats as "Any { .. }", hence the payload has to be downcast.
pub(crate) struct PanicPayload<'a>(pub(crate) &'a (dyn Any + Send));

impl Display for PanicPayload<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let payload = self
            .0
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");

        f.write_str(payload)
    }
}

// A macro, not an async fn: the run loops' message hot path must not pay for a nested state
// machine.
macro_rules! next_incoming {
    ($actor_id:expr, $mailbox:expr, $context:expr, $stopped_by_parent:expr) => {
        'next_incoming: loop {
            consume_budget().await;

            let incoming = tokio::select! {
                biased;

                _ = &mut $stopped_by_parent => {
                    tracing::debug!(
                        actor_id = %$actor_id,
                        "stopping, because parent stopped this actor"
                    );
                    break 'next_incoming None;
                }

                incoming = $mailbox.recv() => {
                    incoming.expect("self_ref keeps a mailbox handle alive")
                }
            };

            if let crate::Incoming::Terminated(other) = &incoming
                && !$context.take_watched_for(*other)
            {
                tracing::debug!(
                    actor_id = %$actor_id,
                    other_id = %*other,
                    "dropping terminated signal for an unwatched actor"
                );
                continue;
            }

            break 'next_incoming Some(incoming);
        }
    };
}

#[cfg(feature = "persistence")]
pub(crate) use next_incoming;

pub(crate) fn spawn<A>(
    parent_stopping_rx: watch::Receiver<()>,
    actor: A,
    config: ActorConfig,
) -> ActorRef<A::Message>
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
{
    let actor_id = ActorId::new();
    let (self_ref, mut mailbox) = SelfRef::new(actor_id, config.mailbox_capacity);
    let actor_ref = self_ref.actor_ref().clone();

    task::spawn(async move {
        let mut context = ActorContext::new(self_ref);

        let mut rx = parent_stopping_rx.clone();
        let mut stopped_by_parent = pin!(rx.changed());

        let mut restarts = 0;

        'run: loop {
            let state = catch_and_log(actor_id, "actor failed to initialize", || {
                actor.init(&context)
            });
            let mut up_since = None;

            if let Some(mut state) = state {
                up_since = Some(Instant::now());

                loop {
                    let incoming = next_incoming!(actor_id, mailbox, context, stopped_by_parent);
                    let Some(incoming) = incoming else {
                        drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
                        break 'run;
                    };

                    match receive_incoming(actor_id, &actor, &context, incoming, state) {
                        Some(Control::Continue(next_state)) => state = next_state,

                        Some(Control::Stop) => {
                            debug!(%actor_id, "stopping as decided by actor");
                            break 'run;
                        }

                        None => break,
                    }
                }
            }

            let restart = await_restart(
                actor_id,
                config.supervision_strategy,
                up_since,
                &mut restarts,
                &parent_stopping_rx,
                &mut stopped_by_parent,
                &mut context,
            )
            .await;
            if !restart {
                break;
            }
        }

        terminate(actor, context, mailbox).await;
    });

    actor_ref
}

/// The failure is consumed here, before the run loop's awaits, so `A::Error` need not be [Send].
pub(crate) fn catch_and_log<T, E, F>(actor_id: ActorId, failure: &str, f: F) -> Option<T>
where
    E: Error,
    F: FnOnce() -> Result<T, E>,
{
    match catch_panic_and_log(actor_id, failure, f)? {
        Ok(value) => Some(value),

        Err(error) => {
            dispose_error(actor_id, error, |error| {
                error!(%actor_id, %error, source = error.source(), "{failure}");
            });
            None
        }
    }
}

// Separate unwind boundaries: dropping while unwinding a formatting panic would abort.
pub(crate) fn dispose_error<E, L>(actor_id: ActorId, error: E, log: L)
where
    L: FnOnce(&E),
{
    catch_panic_and_log(actor_id, "error failed to display", || log(&error));
    drop_containing_panic(actor_id, "error failed to drop", error);
}

pub(crate) fn catch_panic_and_log<T, F>(actor_id: ActorId, failure: &str, f: F) -> Option<T>
where
    F: FnOnce() -> T,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => Some(value),

        Err(panic) => {
            error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
            None
        }
    }
}

pub(crate) async fn catch_panic_and_log_async<T, Fut>(
    actor_id: ActorId,
    failure: &str,
    fut: Fut,
) -> Option<T>
where
    Fut: Future<Output = T>,
{
    match catch_unwind_async(fut).await {
        Ok(value) => Some(value),

        Err(panic) => {
            error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
            None
        }
    }
}

// Only polls are caught: `fut` must be a local async fn or async block, whose construction cannot
// run the code it guards.
pub(crate) async fn catch_unwind_async<T, Fut>(fut: Fut) -> Result<T, Box<dyn Any + Send>>
where
    Fut: Future<Output = T>,
{
    let mut fut = pin!(fut);

    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

/// A panic escaping the destructor must not unwind the task.
pub(crate) fn drop_containing_panic<T>(actor_id: ActorId, failure: &str, value: T) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        error!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{failure}");
    }
}

pub(crate) async fn await_restart<F, M>(
    actor_id: ActorId,
    supervision_strategy: SupervisionStrategy,
    up_since: Option<Instant>,
    restarts: &mut u32,
    parent_stopping_rx: &watch::Receiver<()>,
    stopped_by_parent: &mut Pin<&mut F>,
    context: &mut ActorContext<M>,
) -> bool
where
    F: Future,
{
    let delay = match next_restart(supervision_strategy, up_since, restarts) {
        Restart::After(delay) => delay,

        Restart::LimitExceeded => {
            error!(%actor_id, "stopping, because the restart limit is exceeded");
            return false;
        }

        Restart::NotConfigured => return false,
    };

    if parent_stopping_rx.has_changed().unwrap_or(true) {
        debug!(%actor_id, "stopping, because parent stopped this actor");
        return false;
    }
    debug!(%actor_id, restarts = *restarts, ?delay, "restarting");

    context.stop_children().await;

    match await_backoff(delay, parent_stopping_rx, stopped_by_parent.as_mut()).await {
        Interrupted::No => true,

        Interrupted::StoppedByParent => {
            debug!(%actor_id, "stopping, because parent stopped this actor");
            false
        }
    }
}

/// The state must already have been dropped by the caller. The queued messages are moved out and
/// the channel is dropped before their destructors run: senders observe the termination while the
/// children still stop, and none of these destructors runs between drain and drop. A concurrent
/// sender can still enqueue in that window; flume retains such a message until its last sender
/// drops, which this sequence does not prevent. The watchers are signaled last, since a terminated
/// signal must prove that the actor's destructors have run.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn terminate<A, M>(actor: A, mut context: ActorContext<M>, mailbox: Mailbox<M>) {
    let actor_id = context.self_ref().actor_id();

    let (incoming_rx, closed_mailbox) = mailbox.split();
    let drained = incoming_rx.drain().collect::<Vec<_>>();
    drop_containing_panic(actor_id, "mailbox failed to drop", incoming_rx);
    for incoming in drained {
        drop_containing_panic(actor_id, "queued message failed to drop", incoming);
    }

    for registry in context.take_watched().into_values() {
        registry.remove(actor_id);
    }

    context.stop_children().await;
    debug!(%actor_id, "all child actors terminated");
    drop(context);

    drop_containing_panic(actor_id, "actor failed to drop", actor);

    for watcher in closed_mailbox.take_watchers() {
        if let Err(error) = watcher.send_terminated(actor_id) {
            debug!(
                %actor_id,
                watcher_id = %watcher.watcher_id(),
                %error,
                source = error.source(),
                "cannot send terminated signal"
            );
        }
    }

    debug!(%actor_id, "terminated");
}

#[derive(Debug, PartialEq, Eq)]
enum Restart {
    After(Duration),
    LimitExceeded,
    NotConfigured,
}

#[derive(Debug, PartialEq, Eq)]
enum Interrupted {
    No,
    StoppedByParent,
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn receive_incoming<A>(
    actor_id: ActorId,
    actor: &A,
    context: &ActorContext<A::Message>,
    incoming: Incoming<A::Message>,
    state: A::State,
) -> Option<Control<A::State>>
where
    A: Actor,
{
    catch_and_log(actor_id, "actor failed", || {
        actor.receive(context, incoming, state)
    })
}

fn next_restart(
    supervision_strategy: SupervisionStrategy,
    up_since: Option<Instant>,
    restarts: &mut u32,
) -> Restart {
    let SupervisionStrategy::Restart(policy) = supervision_strategy else {
        return Restart::NotConfigured;
    };

    if up_since.is_some_and(|up_since| up_since.elapsed() >= policy.reset_after) {
        *restarts = 0;
    }
    if *restarts >= policy.max_restarts.get() {
        return Restart::LimitExceeded;
    }

    let delay = policy.backoff.duration(*restarts);
    *restarts += 1;

    Restart::After(delay)
}

async fn await_backoff<F>(
    delay: Duration,
    parent_stopping_rx: &watch::Receiver<()>,
    stopped_by_parent: Pin<&mut F>,
) -> Interrupted
where
    F: Future,
{
    // A zero delay skips the select!, so the parent stop must be probed here.
    let stopped = if delay.is_zero() {
        parent_stopping_rx.has_changed().unwrap_or(true)
    } else {
        select! {
            biased;
            _ = stopped_by_parent => true,
            _ = sleep(delay) => false,
        }
    };

    if stopped {
        Interrupted::StoppedByParent
    } else {
        Interrupted::No
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Backoff, RestartPolicy, SupervisionStrategy,
        actor_context::{Interrupted, PanicPayload, Restart, await_backoff, next_restart},
    };
    use std::{future, num::NonZeroU32, pin::pin, time::Duration};
    use tokio::{sync::watch, time::Instant};

    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(1);

    #[tokio::test(start_paused = true)]
    async fn a_backoff_elapses_uninterrupted() {
        let (_parent_stopping_tx, parent_stopping_rx) = watch::channel(());

        let interrupted = await_backoff(
            Duration::from_secs(1),
            &parent_stopping_rx,
            pin!(future::pending::<()>()),
        )
        .await;

        assert_eq!(interrupted, Interrupted::No);
    }

    #[tokio::test(start_paused = true)]
    async fn a_parent_stop_beats_the_backoff() {
        let (_parent_stopping_tx, parent_stopping_rx) = watch::channel(());

        let interrupted = await_backoff(
            Duration::from_secs(1),
            &parent_stopping_rx,
            pin!(future::ready(())),
        )
        .await;

        assert_eq!(interrupted, Interrupted::StoppedByParent);
    }

    /// A zero backoff never awaits the stop future, so the parent stop is caught by the second
    /// check rather than by the race.
    #[tokio::test(start_paused = true)]
    async fn a_zero_backoff_still_checks_the_parent() {
        let (parent_stopping_tx, parent_stopping_rx) = watch::channel(());

        let interrupted = await_backoff(
            Duration::ZERO,
            &parent_stopping_rx,
            pin!(future::pending::<()>()),
        )
        .await;
        assert_eq!(interrupted, Interrupted::No);

        parent_stopping_tx.send(()).expect("receiver is alive");

        let interrupted = await_backoff(
            Duration::ZERO,
            &parent_stopping_rx,
            pin!(future::pending::<()>()),
        )
        .await;
        assert_eq!(interrupted, Interrupted::StoppedByParent);
    }

    /// A panic payload is a `&'static str` for a literal panic and a `String` for a formatted one,
    /// so both must format as the message itself; anything else is named as such rather than
    /// silently swallowed.
    #[test]
    fn panic_payload_displays_both_string_shapes() {
        assert_eq!(PanicPayload(&"boom").to_string(), "boom");
        assert_eq!(PanicPayload(&"boom".to_string()).to_string(), "boom");
        assert_eq!(PanicPayload(&42).to_string(), "<non-string panic payload>");
    }

    /// Under `Stop` a failure is never retried, whatever the restart count.
    #[test]
    fn stop_never_restarts() {
        let mut restarts = 0;

        assert_eq!(
            next_restart(SupervisionStrategy::Stop, None, &mut restarts),
            Restart::NotConfigured
        );
        assert_eq!(restarts, 0);
    }

    /// The n-th counted restart is delayed by the backoff's `min * 2^(n-1)`, capped at its `max`,
    /// and each one increments the restart count by exactly one.
    #[test]
    fn the_delay_doubles_and_increments_the_count() {
        let strategy = restart(NonZeroU32::MAX);
        let mut restarts = 0;

        for expected in [MIN, MIN * 2, MIN * 4, MAX, MAX] {
            assert_eq!(
                next_restart(strategy, None, &mut restarts),
                Restart::After(expected)
            );
        }
        assert_eq!(restarts, 5);
    }

    /// A failure after `max_restarts` counted restarts stops the actor rather than restarting it
    /// again.
    #[test]
    fn exceeding_the_limit_stops() {
        let strategy = restart(NonZeroU32::new(2).expect("2 is not zero"));
        let mut restarts = 0;

        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::After(MIN)
        );
        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::After(MIN * 2)
        );
        assert_eq!(
            next_restart(strategy, None, &mut restarts),
            Restart::LimitExceeded
        );
    }

    /// Running for at least `reset_after` resets the restart count, so an actor which keeps
    /// recovering is restarted indefinitely instead of exhausting its limit.
    #[tokio::test]
    async fn running_long_enough_resets_the_count() {
        let strategy = restart(NonZeroU32::MIN);
        let mut restarts = 7;

        assert_eq!(
            next_restart(strategy, Some(Instant::now()), &mut restarts),
            Restart::After(MIN)
        );
        assert_eq!(restarts, 1);
    }

    /// A policy which resets on any uptime at all, so the restart count is governed by the call
    /// sequence rather than by wall clock time.
    fn restart(max_restarts: NonZeroU32) -> SupervisionStrategy {
        SupervisionStrategy::Restart(RestartPolicy {
            max_restarts,
            backoff: Backoff::new(MIN, MAX).expect("the bounds are ordered"),
            reset_after: Duration::ZERO,
        })
    }
}
