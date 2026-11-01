use std::{
    convert::Infallible,
    hint,
    num::NonZeroUsize,
    thread,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, Control, Incoming,
    MailboxCapacity, Nothing, Untimed,
};
use tokio::{sync::mpsc, task::block_in_place, time::timeout};

const TIMEOUT: Duration = Duration::from_secs(5);
const BOUNDED_TO_ONE: MailboxCapacity = MailboxCapacity::Bounded(NonZeroUsize::MIN);

const FLOOD_MESSAGES: u32 = 1_000;
const REPORTS: u32 = 1_000;
const ROUNDS: u32 = 100;
const WATCHERS: u32 = 20;
const PRE_WATCHERS: u32 = 50;
const DELAY_STEP: Duration = Duration::from_nanos(250);

/// Watching an actor which has already terminated must still deliver the terminated signal. The
/// termination is observed through another watcher's signal, which the child sends only after
/// closing its registration, so the root provably watches after the child fully terminated.
#[tokio::test]
async fn watch_after_terminated_still_signals() {
    let (terminated_tx, mut terminated_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Root(terminated_tx));

    system.root().tell(RootMessage::StopChild);
    recv(&mut terminated_rx, "child actor did not terminate").await;

    system.root().tell(RootMessage::WatchChild);

    assert_terminates(
        system,
        "root actor did not get a terminated signal for the already terminated child",
    )
    .await;
}

/// A terminated signal is never subject to `MailboxCapacity::Bounded`, so it is delivered even when
/// the watcher's message mailbox is full and dropping messages. The watcher is blocked in `receive`
/// while the flood fills its single slot, so the mailbox is provably full rather than racing the
/// drain. The target is told to stop only after the flood, so the watcher stays blocked until its
/// mailbox holds the target's signal behind the one flood message. The count of flood messages the
/// root did receive is what proves the mailbox was full and dropping throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminated_signal_survives_a_full_mailbox() {
    let (target_ref_tx, mut target_ref_rx) = mpsc::channel(1);
    let (blocked_tx, mut blocked_rx) = mpsc::channel(1);
    let (outcome_tx, mut outcome_rx) = mpsc::channel(1);

    let root = BlockedFloodWatcher {
        target_ref_tx,
        blocked_tx,
        outcome_tx,
    };
    let config = ActorConfig::default().with_mailbox_capacity(BOUNDED_TO_ONE);
    let system = ActorSystem::with_config(root, config);

    let target = recv(
        &mut target_ref_rx,
        "root did not hand out the target reference",
    )
    .await;

    system.root().tell(FloodMessage::Block);
    recv(&mut blocked_rx, "root did not block").await;

    // The root is blocked, so the first of these fills its single slot and the rest are dropped.
    for _ in 0..FLOOD_MESSAGES {
        system.root().tell(FloodMessage::Flood);
    }

    target.tell(Stop);

    let received = recv(
        &mut outcome_rx,
        "root was not signaled about the terminated target",
    )
    .await;
    assert_eq!(
        received, 1,
        "the bounded mailbox was not full during the flood"
    );

    assert_terminates(system, "actor system did not terminate").await;
}

/// A terminated signal must be ordered behind all messages the terminated actor has sent, i.e.
/// receiving it proves those messages have already been delivered. `REPORTS` is chosen large enough
/// that the counter cannot drain them all before the reporter terminates, so a signal jumping the
/// queue is caught.
#[tokio::test]
async fn terminated_is_ordered_behind_messages() {
    let (count_tx, mut count_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Counter(count_tx));

    let count = recv(
        &mut count_rx,
        "counter did not get a terminated signal for the reporter",
    )
    .await;
    assert_eq!(count, REPORTS);

    assert_terminates(system, "actor system did not terminate").await;
}

/// Every watcher of a terminated actor receives the terminated signal, not just one of them. The
/// root only stops, and hence the system only terminates, once all watchers have reported their
/// signal.
#[tokio::test]
async fn every_watcher_receives_the_terminated_signal() {
    let (outcome_tx, mut outcome_rx) = mpsc::channel(1);
    let system = ActorSystem::new(MultiWatchRoot(outcome_tx));

    let outcome = recv(&mut outcome_rx, "the root reported no outcome").await;
    assert_eq!(outcome, MultiWatchOutcome::AllSignaled);

    assert_terminates(system, "not every watcher received the terminated signal").await;
}

/// Watching the same actor twice delivers exactly one terminated signal. A duplicate signal would
/// be enqueued while the target terminates, hence before the marker the watcher sends itself after
/// the first signal: the marker arriving next proves there is no duplicate.
#[tokio::test]
async fn watching_twice_signals_once() {
    let (outcome_tx, mut outcome_rx) = mpsc::channel(1);
    let system = ActorSystem::new(DoubleWatcher(outcome_tx));

    let outcome = recv(
        &mut outcome_rx,
        "watcher observed neither a marker nor a duplicate signal",
    )
    .await;
    assert_eq!(outcome, DoubleWatchOutcome::Single);

    assert_terminates(system, "actor system did not terminate").await;
}

/// After `unwatch` no terminated signal for the target is delivered. The target is a child of the
/// guardian, which terminates only once the target's task has ended, i.e. after the target has
/// signaled its watchers. Hence the guardian's signal arrives at the root behind any (hypothetical)
/// signal for the target: "clean" proves there was none.
#[tokio::test]
async fn unwatch_prevents_the_signal() {
    let (outcome_tx, mut outcome_rx) = mpsc::channel(1);
    let system = ActorSystem::new(UnwatchingRoot(outcome_tx));

    let outcome = recv(
        &mut outcome_rx,
        "root observed neither the guardian nor a target signal",
    )
    .await;
    assert_eq!(outcome, UnwatchOutcome::Clean);

    assert_terminates(system, "actor system did not terminate").await;
}

/// Unwatching suppresses even a terminated signal which is already enqueued: the root, blocked in
/// `receive`, unwatches only once its mailbox holds the target's signal, and then sends itself a
/// probe, which is queued behind that signal. The probe must find the root without a signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unwatch_suppresses_the_enqueued_signal() {
    let (target_ref_tx, mut target_ref_rx) = mpsc::channel(1);
    let (outcome_tx, mut outcome_rx) = mpsc::channel(1);

    let root = BlockedUnwatcher {
        target_ref_tx,
        outcome_tx,
    };
    let system = ActorSystem::new(root);

    let target = recv(
        &mut target_ref_rx,
        "root did not hand out the target reference",
    )
    .await;

    system.root().tell(BlockedMessage::Block);
    target.tell(Stop);

    let outcome = recv(&mut outcome_rx, "root reported no outcome").await;
    assert_eq!(outcome, UnwatchOutcome::Clean);

    assert_terminates(system, "actor system did not terminate").await;
}

/// Unwatching does not poison the pair: watching the target again afterward delivers its
/// terminated signal, which stops the root and hence terminates the system.
#[tokio::test]
async fn rewatching_after_unwatch_signals() {
    let system = ActorSystem::new(RewatchingRoot);

    assert_terminates(
        system,
        "terminated signal was not delivered after rewatching",
    )
    .await;
}

/// Watching an actor which is terminating right now must still deliver the terminated signal. The
/// late watcher registers at a delay swept across rounds, so that some rounds hit the moment
/// between the terminating actor taking its watchers and its mailbox being disconnected;
/// `PRE_WATCHERS` widens that moment, as their signals are sent within it.
#[tokio::test(flavor = "multi_thread")]
async fn watch_racing_termination_still_signals() {
    for round in 0..ROUNDS {
        let delay = DELAY_STEP * round;

        let (signaled_tx, mut signaled_rx) = mpsc::channel(1);
        let (outcome_tx, mut outcome_rx) = mpsc::channel(1);
        let coordinator = Coordinator {
            signaled_tx,
            outcome_tx,
            delay,
        };
        let system = ActorSystem::new(coordinator);

        recv(
            &mut signaled_rx,
            &format!(
                "late watcher registering {delay:?} after the target was told to stop never got \
                 a terminated signal"
            ),
        )
        .await;

        system.root().tell(CoordinatorMessage::Finish);

        let outcome = recv(&mut outcome_rx, "the coordinator reported no outcome").await;
        assert_eq!(outcome, RoundOutcome::Finished);

        assert_terminates(system, "actor system did not terminate").await;
    }
}

async fn recv<T>(rx: &mut mpsc::Receiver<T>, not_received: &str) -> T {
    timeout(TIMEOUT, rx.recv())
        .await
        .expect(not_received)
        .expect("channel closed")
}

async fn assert_terminates<M>(system: ActorSystem<M>, not_terminated: &str)
where
    M: Send + 'static,
{
    timeout(TIMEOUT, system.terminated())
        .await
        .expect(not_terminated)
        .expect("watching the root actor failed");
}

/// Spawn the child and an [Observer] watching it: the observer's report proves the child has
/// signaled its watchers, i.e. closed its registration, before the root is told to watch it.
struct Root(mpsc::Sender<()>);

impl Actor for Root {
    type Message = RootMessage;
    type Timers = Untimed;
    type State = ActorRef<Stop>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let child = context.spawn(Target);
        context.spawn(Observer {
            target: child.clone(),
            terminated_tx: self.0.clone(),
        });
        Ok(child)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(RootMessage::StopChild) => {
                state.tell(Stop);
                Ok(Control::Continue(state))
            }

            Incoming::Message(RootMessage::WatchChild) => {
                context.watch(&state);
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => Ok(Control::Stop),
        }
    }
}

enum RootMessage {
    StopChild,
    WatchChild,
}

/// Count the reports it receives and, once the reporter it watches has terminated, report that
/// count back to the test.
struct Counter(mpsc::Sender<u32>);

impl Actor for Counter {
    type Message = Report;
    type Timers = Untimed;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let reporter = context.spawn(Reporter(context.self_ref().clone()));
        context.watch(&reporter);
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Report) => Ok(Control::Continue(state + 1)),

            Incoming::Terminated(_) => {
                let _ = self.0.try_send(state);
                Ok(Control::Stop)
            }
        }
    }
}

struct Report;

/// Send all its reports and only then stop, so a correctly ordered terminated signal
/// arrives at the counter behind all of them.
struct Reporter(ActorRef<Report>);

impl Actor for Reporter {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for _ in 0..REPORTS {
            self.0.tell(Report);
        }
        context.self_ref().tell(());
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

/// Spawn the target and `WATCHERS` watchers for it; stop the target once all watchers have
/// registered and report the outcome once all of them have reported the terminated signal.
///
/// This actor watches nobody, so a terminated signal for it would be a bug. It is reported rather
/// than asserted, because a panic in `receive` is contained by supervision and would surface as a
/// termination just like success.
struct MultiWatchRoot(mpsc::Sender<MultiWatchOutcome>);

impl Actor for MultiWatchRoot {
    type Message = MultiWatchMessage;
    type Timers = Untimed;
    type State = MultiWatch;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);

        for _ in 0..WATCHERS {
            let watcher = ReportingWatcher {
                target: target.clone(),
                root: context.self_ref().clone(),
            };
            context.spawn(watcher);
        }

        Ok(MultiWatch {
            target,
            registered: 0,
            signaled: 0,
        })
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(MultiWatchMessage::Registered) => {
                let registered = state.registered + 1;
                if registered == WATCHERS {
                    state.target.tell(Stop);
                }
                Ok(Control::Continue(MultiWatch {
                    registered,
                    ..state
                }))
            }

            Incoming::Message(MultiWatchMessage::Signaled) => {
                let signaled = state.signaled + 1;
                if signaled == WATCHERS {
                    let _ = self.0.try_send(MultiWatchOutcome::AllSignaled);
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(MultiWatch { signaled, ..state }))
                }
            }

            Incoming::Terminated(_) => {
                let _ = self.0.try_send(MultiWatchOutcome::Spurious);
                Ok(Control::Stop)
            }
        }
    }
}

enum MultiWatchMessage {
    Registered,
    Signaled,
}

struct MultiWatch {
    target: ActorRef<Stop>,
    registered: u32,
    signaled: u32,
}

/// What the multi watch root observed: every watcher signaled, or a terminated signal for an actor
/// it never watched.
#[derive(Debug, PartialEq, Eq)]
enum MultiWatchOutcome {
    AllSignaled,
    Spurious,
}

/// Watch the target and report registration; report the terminated signal once it arrives.
struct ReportingWatcher {
    target: ActorRef<Stop>,
    root: ActorRef<MultiWatchMessage>,
}

impl Actor for ReportingWatcher {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.target);
        self.root.tell(MultiWatchMessage::Registered);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        self.root.tell(MultiWatchMessage::Signaled);
        Ok(Control::Stop)
    }
}

/// Watch the target twice, tell it to stop and send itself a marker after the first terminated
/// signal: a duplicate signal would already be enqueued and hence arrive before the marker.
struct DoubleWatcher(mpsc::Sender<DoubleWatchOutcome>);

impl Actor for DoubleWatcher {
    type Message = Marker;
    type Timers = Untimed;
    type State = DoubleWatch;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);
        context.watch(&target);
        context.watch(&target);
        target.tell(Stop);
        Ok(DoubleWatch::AwaitingSignal)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Terminated(_), DoubleWatch::AwaitingSignal) => {
                context.self_ref().tell(Marker);
                Ok(Control::Continue(DoubleWatch::AwaitingMarker))
            }

            (Incoming::Terminated(_), DoubleWatch::AwaitingMarker) => {
                let _ = self.0.try_send(DoubleWatchOutcome::Duplicate);
                Ok(Control::Stop)
            }

            (Incoming::Message(Marker), DoubleWatch::AwaitingMarker) => {
                let _ = self.0.try_send(DoubleWatchOutcome::Single);
                Ok(Control::Stop)
            }

            (Incoming::Message(Marker), DoubleWatch::AwaitingSignal) => {
                let _ = self.0.try_send(DoubleWatchOutcome::EarlyMarker);
                Ok(Control::Stop)
            }
        }
    }
}

/// What the double watcher observed: one signal followed by its own marker, a second signal, or
/// a marker before any signal.
#[derive(Debug, PartialEq, Eq)]
enum DoubleWatchOutcome {
    Single,
    Duplicate,
    EarlyMarker,
}

struct Marker;

enum DoubleWatch {
    AwaitingSignal,
    AwaitingMarker,
}

/// Spawn the guardian, which spawns the target; watch and unwatch the target, then stop the
/// guardian. Report whether the first arriving signal names the target (a leak through the
/// unwatch) or the guardian (proving the target has signaled all its watchers).
struct UnwatchingRoot(mpsc::Sender<UnwatchOutcome>);

impl Actor for UnwatchingRoot {
    type Message = ActorRef<Stop>;
    type Timers = Untimed;
    type State = (ActorRef<Stop>, Option<ActorId>);
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let guardian = context.spawn(Guardian(context.self_ref().clone()));
        context.watch(&guardian);
        Ok((guardian, None))
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        (guardian, target_id): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(target) => {
                context.watch(&target);
                context.unwatch(&target);
                guardian.tell(Stop);
                Ok(Control::Continue((guardian, Some(target.actor_id()))))
            }

            Incoming::Terminated(actor_id) => {
                let outcome = if Some(actor_id) == target_id {
                    UnwatchOutcome::Signal
                } else {
                    UnwatchOutcome::Clean
                };
                let _ = self.0.try_send(outcome);
                Ok(Control::Stop)
            }
        }
    }
}

/// What a root observed after unwatching its target: only the guardian's signal, or a signal for
/// the target itself, which would mean the unwatch leaked.
#[derive(Debug, PartialEq, Eq)]
enum UnwatchOutcome {
    Clean,
    Signal,
}

/// Spawn the target as its child and hand it to the root; stop on `Stop`, which terminates the
/// target first: this actor's terminated signal proves the target's task has ended.
struct Guardian(ActorRef<ActorRef<Stop>>);

impl Actor for Guardian {
    type Message = Stop;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.0.tell(context.spawn(Target));
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

/// Block in `receive` on `Block` until the target's terminated signal is enqueued, then unwatch the
/// target and send itself a probe. Report "signal" if the terminated signal is delivered
/// nevertheless, "clean" if the probe arrives first.
struct BlockedUnwatcher {
    target_ref_tx: mpsc::Sender<ActorRef<Stop>>,
    outcome_tx: mpsc::Sender<UnwatchOutcome>,
}

impl Actor for BlockedUnwatcher {
    type Message = BlockedMessage;
    type Timers = Untimed;
    type State = ActorRef<Stop>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);
        context.watch(&target);
        let _ = self.target_ref_tx.try_send(target.clone());
        Ok(target)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        target: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(BlockedMessage::Block) => {
                wait_until_queued(context, 1);
                context.unwatch(&target);
                context.self_ref().tell(BlockedMessage::Probe);
                Ok(Control::Continue(target))
            }

            Incoming::Message(BlockedMessage::Probe) => {
                let _ = self.outcome_tx.try_send(UnwatchOutcome::Clean);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => {
                let _ = self.outcome_tx.try_send(UnwatchOutcome::Signal);
                Ok(Control::Stop)
            }
        }
    }
}

enum BlockedMessage {
    Block,
    Probe,
}

/// Block in `receive` on `Block` until its mailbox holds two items, so the flood it is sent
/// meanwhile fills the bounded mailbox and stays there: the target's terminated signal, the second
/// item, is enqueued into a full mailbox. Count the flood messages actually received and report
/// that count once the signal arrives, which is what proves the mailbox was full and dropping.
struct BlockedFloodWatcher {
    target_ref_tx: mpsc::Sender<ActorRef<Stop>>,
    blocked_tx: mpsc::Sender<()>,
    outcome_tx: mpsc::Sender<u32>,
}

impl Actor for BlockedFloodWatcher {
    type Message = FloodMessage;
    type Timers = Untimed;
    type State = (ActorRef<Stop>, u32);
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);
        context.watch(&target);
        let _ = self.target_ref_tx.try_send(target.clone());
        Ok((target, 0))
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        (target, received): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(FloodMessage::Block) => {
                let _ = self.blocked_tx.try_send(());
                wait_until_queued(context, 2);
                Ok(Control::Continue((target, received)))
            }

            Incoming::Message(FloodMessage::Flood) => Ok(Control::Continue((target, received + 1))),

            Incoming::Terminated(_) => {
                let _ = self.outcome_tx.try_send(received);
                Ok(Control::Stop)
            }
        }
    }
}

enum FloodMessage {
    Block,
    Flood,
}

/// Watch the target and report its terminated signal to the test, proving that the target has
/// closed its watcher registration.
struct Observer {
    target: ActorRef<Stop>,
    terminated_tx: mpsc::Sender<()>,
}

impl Actor for Observer {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.target);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let _ = self.terminated_tx.try_send(());
        Ok(Control::Stop)
    }
}

/// Watch, unwatch and rewatch the target, then stop it; stop itself on the terminated signal.
struct RewatchingRoot;

impl Actor for RewatchingRoot {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);
        context.watch(&target);
        context.unwatch(&target);
        context.watch(&target);
        target.tell(Stop);
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

/// Set up one round: spawn the target, `PRE_WATCHERS` watchers for it and the late watcher, then,
/// once all pre watchers have registered, tell the target to stop and the late watcher to go.
struct Coordinator {
    signaled_tx: mpsc::Sender<()>,
    outcome_tx: mpsc::Sender<RoundOutcome>,
    delay: Duration,
}

impl Actor for Coordinator {
    type Message = CoordinatorMessage;
    type Timers = Untimed;
    type State = Round;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let target = context.spawn(Target);

        for _ in 0..PRE_WATCHERS {
            let pre_watcher = PreWatcher {
                target: target.clone(),
                registered: context.self_ref().clone(),
            };
            context.spawn(pre_watcher);
        }

        let late_watcher = LateWatcher {
            target: target.clone(),
            delay: self.delay,
            signaled_tx: self.signaled_tx.clone(),
        };
        let late_watcher = context.spawn(late_watcher);

        Ok(Round {
            target,
            late_watcher,
            registered: 0,
        })
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(CoordinatorMessage::Registered) => {
                let registered = state.registered + 1;
                if registered == PRE_WATCHERS {
                    state.target.tell(Stop);
                    state.late_watcher.tell(Go);
                }
                Ok(Control::Continue(Round {
                    registered,
                    ..state
                }))
            }

            Incoming::Message(CoordinatorMessage::Finish) => {
                let _ = self.outcome_tx.try_send(RoundOutcome::Finished);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => {
                let _ = self.outcome_tx.try_send(RoundOutcome::Spurious);
                Ok(Control::Stop)
            }
        }
    }
}

enum CoordinatorMessage {
    Registered,
    Finish,
}

struct Round {
    target: ActorRef<Stop>,
    late_watcher: ActorRef<Go>,
    registered: u32,
}

/// What the coordinator observed: the round finished, or a terminated signal for an actor it never
/// watched.
#[derive(Debug, PartialEq, Eq)]
enum RoundOutcome {
    Finished,
    Spurious,
}

struct Target;

impl Actor for Target {
    type Message = Stop;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
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

struct Stop;

/// Watch the target up front and report back, so the round only starts once the terminating target
/// has this many signals to send after draining its mailbox.
struct PreWatcher {
    target: ActorRef<Stop>,
    registered: ActorRef<CoordinatorMessage>,
}

impl Actor for PreWatcher {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.target);
        self.registered.tell(CoordinatorMessage::Registered);
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

/// Watch the target only after the given delay, i.e. while it is already terminating.
struct LateWatcher {
    target: ActorRef<Stop>,
    delay: Duration,
    signaled_tx: mpsc::Sender<()>,
}

impl Actor for LateWatcher {
    type Message = Go;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Go) => {
                spin(self.delay);
                context.watch(&self.target);
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => {
                let _ = self.signaled_tx.try_send(());
                Ok(Control::Stop)
            }
        }
    }
}

struct Go;

fn wait_until_queued<M>(context: &ActorContext<M>, queued: usize) {
    let deadline = Instant::now() + TIMEOUT;
    // Blocking must not strand the task in this worker's LIFO slot!
    block_in_place(|| {
        while context.queued() < queued {
            assert!(
                Instant::now() < deadline,
                "mailbox did not reach {queued} queued items"
            );
            thread::yield_now();
        }
    });
}

fn spin(delay: Duration) {
    let start = Instant::now();
    while start.elapsed() < delay {
        hint::spin_loop();
    }
}
