#[cfg(feature = "cluster")]
use crate::cluster;
use crate::{
    Actor, ActorConfig, ActorId, ActorRef, Control, Incoming, SupervisionStrategy,
    actor_ref::{SelfRef, Sink},
    mailbox::{Mailbox, Queued},
    timers::Timers,
    watch::WatcherRegistry,
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
    time::{Instant, Sleep, sleep},
};
use tracing::{debug, error};

pub(crate) const STATE_FAILED_TO_DROP: &str = "actor state failed to drop";

const TIMER_FAILED_TO_DROP: &str = "timer message failed to drop";

/// Contextual methods for a given actor, provided to [Actor::init] and [Actor::receive].
#[derive(Debug)]
pub struct ActorContext<M> {
    self_ref: SelfRef<M>,

    #[debug(skip)]
    stop_children_tx: watch::Sender<()>,

    #[debug(skip)]
    stop_children_rx: watch::Receiver<()>,

    #[debug(skip)]
    watched: RefCell<HashMap<ActorId, Watched>>,

    #[debug(skip)]
    timers: RefCell<Option<Box<Timers<M>>>>,
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
        spawn(self.stop_children_rx.clone(), actor, config)
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

    /// Start a timer: once the delay has elapsed, the message is delivered to this actor through
    /// its mailbox, like an [ActorRef::tell] to itself. Hence a message which arrived before the
    /// deadline is received before the timer's message. Unlike a told message, a timer bypasses the
    /// capacity of a bounded mailbox, as a terminated signal does.
    ///
    /// Starting a timer under a key which is in use replaces that timer. A timer which has been
    /// replaced, or canceled via [ActorContext::cancel_timer], is never received, even if it is
    /// due and its message is queued already. A restart of this actor cancels all its timers, and
    /// termination drops them.
    ///
    /// A delay the clock cannot represent, e.g. `Duration::MAX`, is capped at about thirty years,
    /// as `tokio::time::sleep` does. The run loop enqueues a due timer between two items, so a
    /// deadline which passes during a long [Actor::receive] is enqueued after it.
    ///
    /// Timers need Tokio's time driver. On a runtime built without it, this panics, so the actor
    /// fails under its supervision as for any other panic in its own code. Timers add no such
    /// requirement for an actor which never starts one.
    pub fn start_timer(&self, key: &'static str, message: M, delay: Duration) {
        self.timers
            .borrow_mut()
            .get_or_insert_default()
            .start(key, message, delay, Instant::now());
    }

    /// Cancel the timer under this key: its message is dropped and never received, even if the
    /// timer is due and its message is queued already. Canceling a key which is unknown, or whose
    /// timer has been received already, has no effect.
    pub fn cancel_timer(&self, key: &'static str) {
        if let Some(timers) = self.timers.borrow_mut().as_deref_mut() {
            timers.cancel(key);
        }
    }

    /// Watch another actor, i.e. receive an [Incoming::Terminated] signal once that actor has
    /// terminated. If it has already terminated, the signal is received right away. Watching an
    /// already watched actor again has no effect: the signal is delivered once.
    ///
    /// The signal is ordered behind all messages the other actor has delivered to this actor.
    /// Receiving it hence proves that this actor has seen every message from the other one it will
    /// ever see: each arrived before the signal or was dropped as a dead letter.
    ///
    /// With the `cluster` feature the other actor may live on another node. The ordering guarantee
    /// is kept there, too. Yet only a signal coming from that node proves that the other actor has
    /// terminated. A signal synthesized here, after that node was declared dead, does not: the
    /// other actor may still be alive. See docs/cluster.md for the exact contract.
    pub fn watch<N>(&self, other: &ActorRef<N>) {
        match other.sink() {
            Sink::Local(mailbox_handle) => {
                let registry = mailbox_handle.watcher_registry().clone();
                let registration = registry.add(self.self_ref.make_watcher());
                self.watched
                    .borrow_mut()
                    .insert(other.actor_id(), Watched::Local(registry));

                if registration.is_err() {
                    self.self_ref.send_terminated(other.actor_id());
                }
            }

            #[cfg(feature = "cluster")]
            Sink::Remote(remote_sink) => {
                let node = remote_sink.node();
                self.watched
                    .borrow_mut()
                    .insert(other.actor_id(), Watched::Remote(node));
                cluster::watch_remote(node, other.actor_id(), self.self_ref.make_watcher());
            }
        }
    }

    /// Stop watching another actor: no terminated signal for it will be received anymore, even if
    /// it has already terminated and the signal is already enqueued. Unwatching an actor which is
    /// not watched, has no effect.
    pub fn unwatch<N>(&self, other: &ActorRef<N>) {
        if let Some(watched) = self.watched.borrow_mut().remove(&other.actor_id()) {
            unwatch(self.self_ref().actor_id(), other.actor_id(), watched);
        }
    }

    /// The number of incoming items queued for this actor, messages, terminated signals and timer
    /// items alike, which it has not received yet. For the actor itself the value is a lower bound:
    /// only the actor removes items and senders can only add to them, so at least this many are
    /// waiting. The run loop drops the terminated signals of unwatched actors and the items of
    /// canceled or replaced timers before [Actor::receive] sees them, so the count can exceed what
    /// `receive` will get by that many.
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
    pub(crate) fn stop_children_rx(&self) -> watch::Receiver<()> {
        self.stop_children_rx.clone()
    }

    pub(crate) fn new(self_ref: SelfRef<M>) -> Self {
        let (stop_children_tx, stop_children_rx) = watch::channel(());

        Self {
            self_ref,
            stop_children_tx,
            stop_children_rx,
            watched: RefCell::new(HashMap::new()),
            timers: RefCell::new(None),
        }
    }

    pub(crate) fn timer_armed(&mut self) -> bool {
        self.timers.get_mut().as_deref().is_some_and(Timers::armed)
    }

    pub(crate) fn timer_sleep(&mut self) -> Option<Pin<&mut Sleep>> {
        self.timers.get_mut().as_deref_mut()?.sleep()
    }

    pub(crate) fn fire_due_timers(&mut self) {
        let now = Instant::now();
        while let Some(id) = self
            .timers
            .get_mut()
            .as_deref_mut()
            .and_then(|timers| timers.fire_due(now))
        {
            self.self_ref().enqueue_timer(id);
        }

        if let Some(timers) = self.timers.get_mut() {
            timers.rearm();
        }
    }

    pub(crate) fn gate(&self, queued: Queued<M>) -> Option<Incoming<M>> {
        match queued {
            Queued::Incoming(incoming) => Some(incoming),

            Queued::Timer(id) => {
                let message = self
                    .timers
                    .borrow_mut()
                    .as_deref_mut()
                    .and_then(|timers| timers.take(id));
                if message.is_none() {
                    debug!(
                        actor_id = %self.self_ref().actor_id(),
                        generation = id.generation(),
                        "dropping stale timer"
                    );
                }

                message.map(Incoming::Message)
            }
        }
    }

    pub(crate) fn take_watched_for(&mut self, other_id: ActorId) -> bool {
        self.watched.get_mut().remove(&other_id).is_some()
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn pending_timers(&self) -> Vec<(&'static str, Instant)> {
        self.timers
            .borrow()
            .as_deref()
            .map(Timers::pending)
            .unwrap_or_default()
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn fire_timer(&self, key: &'static str) -> bool {
        let id = self
            .timers
            .borrow_mut()
            .as_deref_mut()
            .and_then(|timers| timers.fire(key));
        if let Some(id) = id {
            self.self_ref().enqueue_timer(id);
        }

        id.is_some()
    }

    fn take_timer_messages(&mut self) -> Vec<M> {
        self.timers
            .get_mut()
            .as_deref_mut()
            .map(Timers::take_all)
            .unwrap_or_default()
    }

    async fn stop_children(&mut self) {
        let (next_stop_children_tx, next_stop_children_rx) = watch::channel(());

        let stop_children_tx = mem::replace(&mut self.stop_children_tx, next_stop_children_tx);
        let _ = stop_children_tx.send(());

        // The assignment also drops the old `stop_children_rx` without which `closed` never
        // resolves.
        self.stop_children_rx = next_stop_children_rx;
        stop_children_tx.closed().await;
    }

    fn take_watched(&mut self) -> HashMap<ActorId, Watched> {
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

/// A macro, not an async fn: the run loops' message hot path must not pay for a nested state
/// machine. Expanded, the awaits below are states of the run loop itself, not of a future it has
/// to poll into once per message.
macro_rules! next_incoming {
    ($actor_id:expr, $mailbox:expr, $context:expr, $stopped_by_parent:expr) => {
        'next_incoming: loop {
            // Flume's receive future does not participate in Tokio's cooperative budget. Charge
            // one unit explicitly, so an actor whose mailbox never runs empty still yields to the
            // other tasks on its worker.
            consume_budget().await;

            let armed = $context.timer_armed();

            let queued = tokio::select! {
                biased;

                _ = &mut $stopped_by_parent => {
                    tracing::debug!(
                        actor_id = %$actor_id,
                        "stopping, because parent stopped this actor"
                    );
                    break 'next_incoming None;
                }

                // Before the mailbox, else a mailbox which never runs empty starves a due timer.
                _ = async {
                    $context
                        .timer_sleep()
                        .expect("an armed timer has its sleep")
                        .await
                }, if armed => {
                    $context.fire_due_timers();
                    continue 'next_incoming;
                }

                queued = $mailbox.recv() => {
                    queued.expect("self_ref keeps a mailbox handle alive")
                }
            };

            let Some(incoming) = $context.gate(queued) else {
                continue 'next_incoming;
            };

            if let crate::Incoming::Terminated(other) = &incoming
                && !$context.take_watched_for(*other)
            {
                tracing::debug!(
                    actor_id = %$actor_id,
                    other_id = %*other,
                    "dropping terminated signal for an unwatched actor"
                );
                continue 'next_incoming;
            }

            break 'next_incoming Some(incoming);
        }
    };
}

#[cfg(feature = "persistence")]
pub(crate) use next_incoming;

pub(crate) fn spawn<A>(
    stopped_by_parent_rx: watch::Receiver<()>,
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

        let mut rx = stopped_by_parent_rx.clone();
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

            let restart = should_restart(
                actor_id,
                config.supervision_strategy,
                up_since,
                &mut restarts,
                &stopped_by_parent_rx,
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

pub(crate) fn catch_and_log<T, E, F>(actor_id: ActorId, failure: &str, f: F) -> Option<T>
where
    E: Error,
    F: FnOnce() -> Result<T, E>,
{
    log_failure(actor_id, failure, catch_panic_and_log(actor_id, failure, f))
}

pub(crate) fn log_failure<T, E>(
    actor_id: ActorId,
    failure: &str,
    result: Option<Result<T, E>>,
) -> Option<T>
where
    E: Error,
{
    match result? {
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

pub(crate) async fn should_restart<F, M>(
    actor_id: ActorId,
    supervision_strategy: SupervisionStrategy,
    up_since: Option<Instant>,
    restarts: &mut u32,
    stopped_by_parent_rx: &watch::Receiver<()>,
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

    if stopped_by_parent_rx.has_changed().unwrap_or(true) {
        debug!(%actor_id, "stopping, because parent stopped this actor");
        return false;
    }
    debug!(%actor_id, restarts = *restarts, ?delay, "restarting");

    for message in context.take_timer_messages() {
        drop_containing_panic(actor_id, TIMER_FAILED_TO_DROP, message);
    }

    context.stop_children().await;

    match await_backoff(delay, stopped_by_parent.as_mut()).await {
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
/// drops, which this sequence does not prevent. The messages of the timers are dropped right after
/// the queued ones. The watchers are signaled last, since a terminated signal must prove that the
/// actor's destructors have run.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) async fn terminate<A, M>(actor: A, mut context: ActorContext<M>, mailbox: Mailbox<M>) {
    let actor_id = context.self_ref().actor_id();

    let (incoming_rx, watcher_registration) = mailbox.split();
    let drained = incoming_rx.drain().collect::<Vec<_>>();
    drop_containing_panic(actor_id, "mailbox failed to drop", incoming_rx);
    for queued in drained {
        drop_containing_panic(actor_id, "queued message failed to drop", queued);
    }

    for message in context.take_timer_messages() {
        drop_containing_panic(actor_id, TIMER_FAILED_TO_DROP, message);
    }

    for (other_id, watched) in context.take_watched() {
        unwatch(actor_id, other_id, watched);
    }

    context.stop_children().await;
    debug!(%actor_id, "all child actors terminated");
    drop(context);

    drop_containing_panic(actor_id, "actor failed to drop", actor);

    for watcher in watcher_registration.close() {
        if let Err(error) = watcher.handle_terminated(actor_id) {
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

enum Watched {
    Local(WatcherRegistry),

    #[cfg(feature = "cluster")]
    Remote(cluster::NodeId),
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

fn unwatch(watcher_id: ActorId, other_id: ActorId, watched: Watched) {
    match (watched, other_id) {
        (Watched::Local(registry), _) => registry.remove(watcher_id),

        #[cfg(feature = "cluster")]
        (Watched::Remote(node), other_id) => cluster::unwatch_remote(node, other_id, watcher_id),
    }
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

async fn await_backoff<F>(delay: Duration, stopped_by_parent: Pin<&mut F>) -> Interrupted
where
    F: Future,
{
    select! {
        biased;
        _ = stopped_by_parent => Interrupted::StoppedByParent,
        _ = sleep(delay) => Interrupted::No,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Backoff, RestartPolicy, SupervisionStrategy,
        actor_context::{Interrupted, PanicPayload, Restart, await_backoff, next_restart},
    };
    use std::{future, num::NonZeroU32, pin::pin, time::Duration};
    use tokio::time::Instant;

    const MIN: Duration = Duration::from_millis(250);
    const MAX: Duration = Duration::from_secs(1);
    const NO_RESET: Duration = Duration::from_secs(3600);

    #[tokio::test(start_paused = true)]
    async fn a_backoff_elapses_uninterrupted() {
        let interrupted =
            await_backoff(Duration::from_secs(1), pin!(future::pending::<()>())).await;

        assert_eq!(interrupted, Interrupted::No);
    }

    #[tokio::test(start_paused = true)]
    async fn a_parent_stop_beats_the_backoff() {
        let interrupted = await_backoff(Duration::from_secs(1), pin!(future::ready(()))).await;

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
        let strategy = restart(NonZeroU32::MAX, Duration::ZERO);
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
        let strategy = restart(NonZeroU32::new(2).expect("2 is not zero"), Duration::ZERO);
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
        let strategy = restart(NonZeroU32::MIN, Duration::ZERO);
        let mut restarts = 7;

        assert_eq!(
            next_restart(strategy, Some(Instant::now()), &mut restarts),
            Restart::After(MIN)
        );
        assert_eq!(restarts, 1);
    }

    /// Running for less than `reset_after` keeps the restart count, so an actor which fails again
    /// right after coming up still escalates to a stop instead of restarting forever.
    #[tokio::test]
    async fn running_briefly_keeps_the_count() {
        let strategy = restart(NonZeroU32::MIN, NO_RESET);
        let mut restarts = 1;

        assert_eq!(
            next_restart(strategy, Some(Instant::now()), &mut restarts),
            Restart::LimitExceeded
        );
        assert_eq!(restarts, 1);
    }

    fn restart(max_restarts: NonZeroU32, reset_after: Duration) -> SupervisionStrategy {
        SupervisionStrategy::Restart(RestartPolicy {
            max_restarts,
            backoff: Backoff::new(MIN, MAX).expect("the bounds are valid"),
            reset_after,
        })
    }
}
