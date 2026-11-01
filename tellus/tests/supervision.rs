use std::{
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Backoff, Control, Incoming, Nothing,
    RestartPolicy, SupervisionStrategy, Untimed,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{Instant, sleep, timeout},
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// `Restart` replaces the state of the failed actor with a newly initialized one. The count of one
/// proves both halves: two of them would mean the state survived, none that it was never counted.
#[tokio::test]
async fn restart_reinitializes_state() {
    let count =
        count_after_failure(restart_strategy(1, Duration::ZERO), FailureTrigger::Panic).await;
    assert_eq!(count, 1);
}

/// Returning `Err` from `receive` must drive supervision exactly like a panic does: `?` is the
/// let-it-crash operator, and no panic is involved.
#[tokio::test]
async fn err_from_receive_restarts_like_panic() {
    let count = count_after_failure(restart_strategy(1, Duration::ZERO), FailureTrigger::Err).await;
    assert_eq!(count, 1);
}

/// `Stop` stops the failed actor, which terminates the actor system if it is the root actor.
#[tokio::test]
async fn panic_under_stop_terminates_system() {
    terminates_after_failure(FailureTrigger::Panic).await;
}

/// The `Err` counterpart: no panic, same supervision.
#[tokio::test]
async fn err_under_stop_terminates_system() {
    terminates_after_failure(FailureTrigger::Err).await;
}

/// A panic in `init` on the restart path counts like any other failure; once the limit is
/// exceeded the actor terminates, so that watchers (and hence the actor system) learn about it
/// instead of waiting forever.
#[tokio::test]
async fn panic_in_init_under_restart_terminates_system() {
    terminates_after_failing_reinitialization(FailureTrigger::Panic).await;
}

/// The `Err` counterpart of the above.
#[tokio::test]
async fn err_in_init_under_restart_terminates_system() {
    terminates_after_failing_reinitialization(FailureTrigger::Err).await;
}

/// An `Err` from the very first `init` terminates the actor and signals its watchers.
#[tokio::test]
async fn err_from_first_init_terminates_system() {
    let system = ActorSystem::new(FailInit::default());

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate after `init` returned an error")
        .expect("watching the root actor failed");
}

/// Restarting must stop the failed actor's children before creating the new state, else every
/// restart accumulates another generation of children.
#[tokio::test]
async fn restart_stops_children_before_reinitializing() {
    let alive = Arc::new(AtomicUsize::new(0));
    let (child_started_tx, mut child_started_rx) = mpsc::unbounded_channel();
    let parent = Parent {
        alive: alive.clone(),
        child_started_tx,
    };
    let system = ActorSystem::with_config(parent, config(restart_strategy(1, Duration::ZERO)));

    timeout(TIMEOUT, child_started_rx.recv())
        .await
        .expect("first child did not start")
        .expect("child_started channel closed");
    assert_eq!(alive.load(SeqCst), 1);

    system.root().tell(());

    timeout(TIMEOUT, child_started_rx.recv())
        .await
        .expect("child of the restarted actor did not start")
        .expect("child_started channel closed");
    assert_eq!(
        alive.load(SeqCst),
        1,
        "restart left the previous generation of children running"
    );
}

/// Watches survive a restart: the restarted state receives the terminated signal for a child the
/// previous incarnation watched. The restart itself stops that child, so the signal is enqueued
/// while the actor has no state at all and must still be delivered afterward.
#[tokio::test]
async fn watches_survive_a_restart() {
    let (signaled_tx, mut signaled_rx) = mpsc::channel(1);
    let parent = WatchingParent {
        inits: AtomicUsize::new(0),
        signaled_tx,
    };
    let system = ActorSystem::with_config(parent, config(restart_strategy(1, Duration::ZERO)));

    system.root().tell(());

    timeout(TIMEOUT, signaled_rx.recv())
        .await
        .expect("restarted actor did not get the terminated signal for the child it watched")
        .expect("signaled channel closed");

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}

/// Failing more often than `max_restarts` stops the actor; `reset_after` is far away here, so every
/// failure counts.
#[tokio::test]
async fn exceeding_the_restart_limit_terminates_system() {
    let (counter, _reported_rx) = counter(FailureTrigger::Err);
    let system = ActorSystem::with_config(
        counter,
        config(restart_strategy(2, Duration::from_secs(3600))),
    );

    for _ in 0..3 {
        system.root().tell(CounterMessage::Fail);
    }

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate after exceeding the restart limit")
        .expect("watching the root actor failed");
}

/// Running without failure for at least `reset_after` resets the restart count: with a zero
/// `reset_after` every successful `init` resets the count, so the actor survives more failures
/// than `max_restarts`.
#[tokio::test]
async fn running_resets_the_restart_count() {
    let (counter, mut reported_rx) = counter(FailureTrigger::Err);
    let system = ActorSystem::with_config(counter, config(restart_strategy(1, Duration::ZERO)));

    for _ in 0..5 {
        system.root().tell(CounterMessage::Fail);
    }
    system.root().tell(CounterMessage::Bump);
    system.root().tell(CounterMessage::Report);

    let count = timeout(TIMEOUT, reported_rx.recv())
        .await
        .expect("actor did not report its count")
        .expect("reported channel closed");
    assert_eq!(count, 1);
}

/// Restart delays grow exponentially from `min_backoff` and are capped at `max_backoff`: three
/// retries of a failing first `init` must advance the paused clock by exactly 1s + 2s + 2s.
#[tokio::test(start_paused = true)]
async fn restarts_back_off_exponentially() {
    let inits = Arc::new(AtomicUsize::new(0));
    let policy = backoff_strategy(
        NonZeroU32::new(3).expect("max_restarts is not zero"),
        Duration::from_secs(1),
        Duration::from_secs(2),
    );
    let system = ActorSystem::with_config(
        FailInit {
            inits: inits.clone(),
            failed_tx: None,
        },
        config(policy),
    );

    let started = Instant::now();
    timeout(Duration::from_secs(60), system.terminated())
        .await
        .expect("actor system did not terminate after exceeding the restart limit")
        .expect("watching the root actor failed");

    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(inits.load(SeqCst), 4);
}

/// The parent stopping must interrupt a backoff delay, else termination waits for the delay. The
/// child's `init` must have failed before the parent is told to stop, else the child is stopped
/// while still starting up and no backoff is ever in flight.
#[tokio::test]
async fn parent_stop_cancels_backoff() {
    let (failed_tx, mut failed_rx) = mpsc::channel(1);
    let system = ActorSystem::new(BackoffParent { failed_tx });

    timeout(TIMEOUT, failed_rx.recv())
        .await
        .expect("child actor did not fail its first `init`")
        .expect("child actor dropped its failure channel");

    system.root().tell(());

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate while a child was backing off")
        .expect("watching the root actor failed");
}

/// A parent stopped while a zero-backoff restart is stopping the failed actor's children must be
/// honored once they are stopped, without another `init` cycle. Here the restarting actor is held
/// in `stop_children` by a child blocked in its `init`, the parent is stopped meanwhile, and once
/// the child is unblocked the actor must terminate without reinitializing. A stop landing before
/// the restart's first probe passes vacuously, but the actor enters `stop_children` without an
/// await point after acking its failure, so the raced window is hit reliably.
#[tokio::test(flavor = "multi_thread")]
async fn parent_stop_during_restart_stop_children_skips_reinit() {
    let inits = Arc::new(AtomicUsize::new(0));
    let (middle_tx, mut middle_rx) = mpsc::channel(1);
    let (failed_tx, mut failed_rx) = mpsc::channel(1);
    let (blocked_tx, mut blocked_rx) = mpsc::channel(1);
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();

    let middle = RestartingMiddle {
        inits: inits.clone(),
        failed_tx,
        blocked_tx,
        unblock_rx: Mutex::new(Some(unblock_rx)),
    };
    let system = ActorSystem::new(StoppingRoot {
        middle: Mutex::new(Some(middle)),
        middle_tx,
    });

    let middle_ref = timeout(TIMEOUT, middle_rx.recv())
        .await
        .expect("root did not spawn the restarting actor")
        .expect("middle channel closed");
    timeout(TIMEOUT, blocked_rx.recv())
        .await
        .expect("child did not start blocking in `init`")
        .expect("blocked channel closed");

    middle_ref.tell(());
    timeout(TIMEOUT, failed_rx.recv())
        .await
        .expect("restarting actor did not fail")
        .expect("failed channel closed");

    system.root().tell(());
    sleep(Duration::from_millis(100)).await;
    unblock_tx
        .send(())
        .expect("child dropped the unblock channel");

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
    assert_eq!(
        inits.load(SeqCst),
        1,
        "the restart reinitialized under a parent stop"
    );
}

async fn count_after_failure(
    supervision_strategy: SupervisionStrategy,
    fail: FailureTrigger,
) -> u32 {
    let (counter, mut reported_rx) = counter(fail);
    let system = ActorSystem::with_config(counter, config(supervision_strategy));

    system.root().tell(CounterMessage::Bump);
    system.root().tell(CounterMessage::Fail);
    system.root().tell(CounterMessage::Bump);
    system.root().tell(CounterMessage::Report);

    timeout(TIMEOUT, reported_rx.recv())
        .await
        .expect("actor did not report its count")
        .expect("reported channel closed")
}

async fn terminates_after_failure(fail: FailureTrigger) {
    let (counter, _reported_rx) = counter(fail);
    let system = ActorSystem::with_config(counter, config(SupervisionStrategy::Stop));

    system.root().tell(CounterMessage::Fail);

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate after the root actor failed")
        .expect("watching the root actor failed");
}

async fn terminates_after_failing_reinitialization(fail: FailureTrigger) {
    let inits = Arc::new(AtomicUsize::new(0));
    let actor = FailOnReinitialization {
        inits: inits.clone(),
        fail,
    };
    let system = ActorSystem::with_config(actor, config(restart_strategy(3, Duration::ZERO)));

    system.root().tell(());

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate after `init` failed on the restart path")
        .expect("watching the root actor failed");

    assert_eq!(
        inits.load(SeqCst),
        4,
        "expected the initial init plus max_restarts failing retries"
    );
}

/// A counter reporting through a fresh channel, the pair every failure test starts from.
fn counter(fail: FailureTrigger) -> (Counter, mpsc::Receiver<u32>) {
    let (reported_tx, reported_rx) = mpsc::channel(1);
    (Counter { fail, reported_tx }, reported_rx)
}

fn config(supervision_strategy: SupervisionStrategy) -> ActorConfig {
    ActorConfig::default().with_supervision_strategy(supervision_strategy)
}

/// A restart policy pacing restarts, for the tests which observe the backoff itself.
fn backoff_strategy(
    max_restarts: NonZeroU32,
    min_backoff: Duration,
    max_backoff: Duration,
) -> SupervisionStrategy {
    SupervisionStrategy::Restart(RestartPolicy {
        max_restarts,
        backoff: Backoff::new(min_backoff, max_backoff).expect("the bounds are valid"),
        reset_after: Duration::ZERO,
    })
}

fn restart_strategy(max_restarts: u32, reset_after: Duration) -> SupervisionStrategy {
    SupervisionStrategy::Restart(RestartPolicy {
        max_restarts: NonZeroU32::new(max_restarts).expect("max_restarts is not zero"),
        backoff: Backoff::new(Duration::from_nanos(1), Duration::from_nanos(1))
            .expect("the bounds are valid"),
        reset_after,
    })
}

#[derive(Debug, Clone, Copy)]
enum FailureTrigger {
    Panic,
    Err,
}

impl FailureTrigger {
    fn trigger<T>(self) -> Result<T, Boom> {
        match self {
            Self::Panic => panic!("boom"),
            Self::Err => Err(Boom),
        }
    }
}

#[derive(Debug, Error)]
#[error("boom")]
struct Boom;

struct Counter {
    fail: FailureTrigger,
    reported_tx: mpsc::Sender<u32>,
}

impl Actor for Counter {
    type Message = CounterMessage;
    type Timers = Untimed;
    type State = u32;
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(CounterMessage::Bump) => Ok(Control::Continue(state + 1)),

            Incoming::Message(CounterMessage::Fail) => self.fail.trigger(),

            Incoming::Message(CounterMessage::Report) => {
                let _ = self.reported_tx.try_send(state);
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

enum CounterMessage {
    Bump,
    Fail,
    Report,
}

struct FailOnReinitialization {
    inits: Arc<AtomicUsize>,
    fail: FailureTrigger,
}

impl Actor for FailOnReinitialization {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        if self.inits.fetch_add(1, SeqCst) > 0 {
            self.fail.trigger()
        } else {
            Ok(())
        }
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        self.fail.trigger()
    }
}

#[derive(Default)]
struct FailInit {
    inits: Arc<AtomicUsize>,
    failed_tx: Option<mpsc::Sender<()>>,
}

impl Actor for FailInit {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.inits.fetch_add(1, SeqCst);
        if let Some(failed_tx) = &self.failed_tx {
            let _ = failed_tx.try_send(());
        }
        Err(Boom)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

struct BackoffParent {
    failed_tx: mpsc::Sender<()>,
}

impl Actor for BackoffParent {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let policy = backoff_strategy(
            NonZeroU32::MAX,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        let child = FailInit {
            failed_tx: Some(self.failed_tx.clone()),
            ..Default::default()
        };
        context.spawn_with_config(child, config(policy));
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

/// Spawn the restarting actor with a zero-backoff restart, hand its reference to the test and stop
/// on the first message.
struct StoppingRoot {
    middle: Mutex<Option<RestartingMiddle>>,
    middle_tx: mpsc::Sender<ActorRef<()>>,
}

impl Actor for StoppingRoot {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        if let Some(middle) = self
            .middle
            .lock()
            .expect("middle mutex is not poisoned")
            .take()
        {
            let middle_ref =
                context.spawn_with_config(middle, config(restart_strategy(1, Duration::ZERO)));
            let _ = self.middle_tx.try_send(middle_ref);
        }

        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

/// Spawn a blocking child on the first `init` only, count `init` calls and fail on the first
/// message, acking right before returning the error.
struct RestartingMiddle {
    inits: Arc<AtomicUsize>,
    failed_tx: mpsc::Sender<()>,
    blocked_tx: mpsc::Sender<()>,
    unblock_rx: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl Actor for RestartingMiddle {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.inits.fetch_add(1, SeqCst);
        if let Some(unblock_rx) = self
            .unblock_rx
            .lock()
            .expect("unblock mutex is not poisoned")
            .take()
        {
            context.spawn(BlockedInInit {
                blocked_tx: self.blocked_tx.clone(),
                unblock_rx,
            });
        }

        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let _ = self.failed_tx.try_send(());
        Err(Boom)
    }
}

/// Ack and block in `init` until unblocked, so the parent's `stop_children` cannot complete before
/// the test releases it; requires a multi thread runtime, since the blocked `init` occupies a
/// worker.
struct BlockedInInit {
    blocked_tx: mpsc::Sender<()>,
    unblock_rx: std::sync::mpsc::Receiver<()>,
}

impl Actor for BlockedInInit {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let _ = self.blocked_tx.try_send(());
        let _ = self.unblock_rx.recv();
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

struct Parent {
    alive: Arc<AtomicUsize>,
    child_started_tx: mpsc::UnboundedSender<()>,
}

impl Actor for Parent {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let child = Child {
            alive: self.alive.clone(),
            started_tx: self.child_started_tx.clone(),
        };
        context.spawn(child);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Err(Boom)
    }
}

struct Child {
    alive: Arc<AtomicUsize>,
    started_tx: mpsc::UnboundedSender<()>,
}

impl Actor for Child {
    type Message = Nothing;
    type Timers = Untimed;
    type State = Alive;
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let alive = Alive::new(self.alive.clone());
        let _ = self.started_tx.send(());
        Ok(alive)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

struct Alive(Arc<AtomicUsize>);

impl Alive {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, SeqCst);
        Self(counter)
    }
}

impl Drop for Alive {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

/// Spawn and watch a child on the first `init` only, fail on the first message and report the
/// terminated signal the restart's stopping of that child produces.
struct WatchingParent {
    inits: AtomicUsize,
    signaled_tx: mpsc::Sender<()>,
}

impl Actor for WatchingParent {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        if self.inits.fetch_add(1, SeqCst) == 0 {
            let child = context.spawn(Idle);
            context.watch(&child);
        }

        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => Err(Boom),

            Incoming::Terminated(_) => {
                let _ = self.signaled_tx.try_send(());
                Ok(Control::Stop)
            }
        }
    }
}

struct Idle;

impl Actor for Idle {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}
