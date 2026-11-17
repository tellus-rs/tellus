use std::{
    convert::Infallible,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc as std_mpsc,
    },
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorId, ActorRef, ActorSystem, Control, Incoming, Nothing, StashMode,
    Stashed, Timed, TimerMode, Unstashed, Untimed,
};
use thiserror::Error;
use tokio::{sync::mpsc, task::block_in_place, time::timeout};

const TIMEOUT: Duration = Duration::from_secs(5);
const SELF_SENDS: usize = 1_000;
const TERMINATION_ORDER: &[&str] = &["grandchild", "child", "root"];

static TRACING: Once = Once::new();

/// An actor system terminates once its root actor has stopped.
#[tokio::test]
async fn stopping_root_terminates_system() {
    let (terminated_tx, _terminated_rx) = mpsc::channel(TERMINATION_ORDER.len());
    let (started_tx, _started_rx) = mpsc::channel(1);
    let root = Root(Terminated("root", terminated_tx), started_tx);
    let system = ActorSystem::new(root);

    system.root().tell(());

    assert_terminates(system).await;
}

/// A stopping actor stops its child actors first and only terminates once all descendants have
/// terminated, i.e. the actor tree terminates bottom-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descendants_terminate_bottom_up() {
    let (terminated_tx, mut terminated_rx) = mpsc::channel(TERMINATION_ORDER.len());
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let root = Root(Terminated("root", terminated_tx), started_tx);
    let system = ActorSystem::new(root);
    recv(&mut started_rx, "the tree did not start").await;

    system.root().tell(());

    let mut terminated = Vec::new();
    for _ in 0..TERMINATION_ORDER.len() {
        let actor = recv(&mut terminated_rx, "not all actors terminated").await;
        terminated.push(actor);
    }
    assert_eq!(terminated, TERMINATION_ORDER);

    assert_terminates(system).await;
}

/// An actor system can be stopped from the outside, which stops the root actor and with it the
/// whole tree, bottom-up like any parent stopping its children.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_a_system_terminates_the_tree_bottom_up() {
    let (terminated_tx, mut terminated_rx) = mpsc::channel(TERMINATION_ORDER.len());
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Enduring(Terminated("root", terminated_tx), started_tx));
    recv(&mut started_rx, "the tree did not start").await;

    system.stop();

    let mut terminated = Vec::new();
    for _ in 0..TERMINATION_ORDER.len() {
        let actor = recv(&mut terminated_rx, "not all actors terminated").await;
        terminated.push(actor);
    }
    assert_eq!(terminated, TERMINATION_ORDER);

    assert_terminates(system).await;
}

/// An outside stop takes effect between messages, never inside one: a message being handled when
/// it arrives is finished first, and the tree terminates afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_a_system_finishes_the_message_being_handled() {
    let (probe_tx, mut probe_rx) = mpsc::channel(2);
    let (release_tx, release_rx) = std_mpsc::channel();
    let system = ActorSystem::new(Blocking {
        probe: probe_tx,
        release: release_rx,
    });

    system.root().tell(());
    assert_eq!(
        recv(&mut probe_rx, "the root never started").await,
        "started"
    );

    system.stop();
    release_tx.send(()).expect("the root awaits the release");

    assert_eq!(
        recv(&mut probe_rx, "the root never finished").await,
        "finished"
    );
    assert_terminates(system).await;
}

/// A child whose parent stops it before it ran never runs `init`, but it still terminates: its
/// actor value is dropped before its watcher gets the terminated signal, and before its parent's.
#[tokio::test]
async fn a_child_stopped_before_it_ran_never_initializes() {
    assert_child_never_initializes::<Untimed, Unstashed>().await;
}

/// [a_child_stopped_before_it_ran_never_initializes] for a timed child.
#[tokio::test]
async fn a_timed_child_stopped_before_it_ran_never_initializes() {
    assert_child_never_initializes::<Timed, Unstashed>().await;
}

/// [a_child_stopped_before_it_ran_never_initializes] for a stashed child.
#[tokio::test]
async fn a_stashed_child_stopped_before_it_ran_never_initializes() {
    assert_child_never_initializes::<Untimed, Stashed>().await;
}

/// [a_child_stopped_before_it_ran_never_initializes] for a timed and stashed child.
#[tokio::test]
async fn a_timed_stashed_child_stopped_before_it_ran_never_initializes() {
    assert_child_never_initializes::<Timed, Stashed>().await;
}

/// A root whose system is stopped before it ran never runs `init`, and the system terminates.
#[tokio::test]
async fn a_root_stopped_before_it_ran_never_initializes() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Unborn::<Untimed, Unstashed>::new(events_tx));

    system.stop();
    assert_terminates(system).await;

    let mut events = Vec::new();
    while let Ok(event) = events_rx.try_recv() {
        events.push(event);
    }
    assert_eq!(events, [Event::UnbornDropped]);
}

/// Dropping an actor system does not stop its actors: the root actor keeps running and processing
/// messages, it only forfeits `ActorSystem::terminated`.
#[tokio::test]
async fn dropping_the_system_does_not_stop_the_actors() {
    let (received_tx, mut received_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Echo(received_tx));
    let root = system.root().clone();

    drop(system);

    root.tell(());
    recv(
        &mut received_rx,
        "root actor did not receive the message after its actor system was dropped",
    )
    .await;

    root.tell(());
    recv(
        &mut received_rx,
        "root actor stopped processing messages after its actor system was dropped",
    )
    .await;
}

/// `tell` never blocks, so an actor may send itself any number of messages from `init` even though
/// its mailbox is not drained yet. With the default `Unbounded` mailbox none of them are dropped.
#[tokio::test]
async fn init_may_self_send_without_blocking() {
    let (received_tx, mut received_rx) = mpsc::channel(1);
    let actor = SelfSender { received_tx };
    let system = ActorSystem::new(actor);

    let received = recv(
        &mut received_rx,
        "actor did not receive all messages sent by `init`",
    )
    .await;
    assert_eq!(received, SELF_SENDS);

    assert_terminates(system).await;
}

/// A terminated signal must prove that the actor's destructors have run, but a panic escaping one
/// of them must not skip the signal. The actor value is dropped on the termination path. A panic
/// there would otherwise unwind the actor's task before its watchers are signaled, and the actor
/// system would never terminate.
#[tokio::test]
async fn panicking_actor_destructor_still_terminates_system() {
    let (dropped_tx, mut dropped_rx) = mpsc::channel(1);
    let system = ActorSystem::new(PanickingActor(dropped_tx));

    system.root().tell(());

    recv(&mut dropped_rx, "actor destructor did not run").await;
    assert_terminates(system).await;
}

/// The same for a panic escaping the destructor of an actor's state, which is dropped when the
/// parent stops the actor: its watchers must still be signaled. The watch is registered while the
/// actor is still alive, so that the signal comes from the termination path rather than from
/// watching an already terminated actor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_state_destructor_still_signals_watchers() {
    let (child_tx, mut child_rx) = mpsc::channel(1);
    let system = ActorSystem::new(PanickingStateRoot(child_tx));

    let child = recv(&mut child_rx, "root actor did not spawn its child actor").await;

    let (observed_tx, mut observed_rx) = mpsc::channel(2);
    let observer = ActorSystem::new(Observer(observed_tx));
    observer.root().tell(child);
    assert_eq!(
        recv(&mut observed_rx, "watcher did not register its watch").await,
        Observed::Watching
    );

    system.root().tell(());

    assert_eq!(
        recv(
            &mut observed_rx,
            "watcher was not signaled about the terminated actor"
        )
        .await,
        Observed::Terminated
    );
    assert_terminates(system).await;
}

/// A state destructor panicking while `receive` returns an error is contained. The state is
/// dropped on receive's normal return path, so the panic starts a fresh unwind. It is caught like
/// any other panic and fed to supervision, here the default `Stop`. Only a destructor
/// panicking during the unwind of a panicking `receive` or `init` aborts the process, as anywhere
/// in Rust.
#[tokio::test]
async fn panicking_state_destructor_on_error_is_supervised() {
    let system = ActorSystem::new(FailingWithPanickingState);

    system.root().tell(());

    assert_terminates(system).await;
}

/// A failure is logged after `init` or `receive` has returned, so a panic in the error's `Display`
/// happens outside of them; it must be contained, and the actor still terminates.
#[tokio::test]
async fn panicking_error_display_is_contained() {
    for failing in [Failing::Init, Failing::Receive] {
        let hooks = assert_failure_terminates(failing, Panics::display()).await;
        assert!(hooks.display.load(SeqCst), "display was not called");
        assert!(hooks.drop.load(SeqCst), "the error was not dropped");
    }
}

/// The same for a panic in the error's `source`, which the failure log reads too.
#[tokio::test]
async fn panicking_error_source_is_contained() {
    for failing in [Failing::Init, Failing::Receive] {
        let hooks = assert_failure_terminates(failing, Panics::source()).await;
        assert!(hooks.source.load(SeqCst), "source was not called");
        assert!(hooks.drop.load(SeqCst), "the error was not dropped");
    }
}

/// The same for a panic in the error's destructor, which runs after the failure is logged.
#[tokio::test]
async fn panicking_error_drop_is_contained() {
    for failing in [Failing::Init, Failing::Receive] {
        let hooks = assert_failure_terminates(failing, Panics::drop()).await;
        assert!(hooks.drop.load(SeqCst), "the error was not dropped");
    }
}

/// Formatting and dropping the error are contained separately: a destructor panicking after a
/// `Display` panic does not run during that panic's unwind, which would abort the process.
#[tokio::test]
async fn panicking_error_display_and_drop_are_contained_separately() {
    for failing in [Failing::Init, Failing::Receive] {
        let hooks = assert_failure_terminates(failing, Panics::display_and_drop()).await;
        assert!(hooks.display.load(SeqCst), "display was not called");
        assert!(hooks.drop.load(SeqCst), "the error was not dropped");
    }
}

/// The actor value is dropped before the watchers are signaled, since a terminated signal must
/// prove that the actor's destructors have run. The subject's destructor tells its watcher, and
/// that message shares the FIFO with the signal, so it must arrive first.
#[tokio::test]
async fn the_actor_value_is_dropped_before_the_watchers_are_signaled() {
    let (witnessed_tx, mut witnessed_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Witness(witnessed_tx));

    let mut witnessed = Vec::new();
    for _ in 0..2 {
        witnessed.push(recv_unbounded(&mut witnessed_rx, "the witness saw too little").await);
    }
    assert_eq!(witnessed, [Witnessed::Dropped, Witnessed::Terminated]);
    assert_terminates(system).await;
}

/// A stopped actor drops its state before it stops its children: the root's state reports its
/// drop ahead of the child's actor value, which the child drops only on its way out.
#[tokio::test]
async fn the_state_is_dropped_before_the_children_are_stopped() {
    let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(StatefulParent(lifecycle_tx));
    assert_eq!(
        recv_unbounded(&mut lifecycle_rx, "the parent did not start").await,
        Lifecycle::Started
    );

    system.stop();
    assert_terminates(system).await;

    let mut lifecycle = Vec::new();
    while let Ok(event) = lifecycle_rx.try_recv() {
        lifecycle.push(event);
    }
    assert_eq!(
        lifecycle,
        [Lifecycle::StateDropped, Lifecycle::ChildDropped]
    );
}

/// Queued messages whose destructors panic are dropped under containment at termination: every
/// destructor runs, and the system still terminates.
#[tokio::test]
async fn panicking_queued_message_destructors_are_contained() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let system = ActorSystem::new(Hoarder);

    system.root().tell(Hoarded::Stop);
    for _ in 0..2 {
        system.root().tell(Hoarded::Bomb {
            _bomb: Bomb(dropped.clone()),
        });
    }

    assert_terminates(system).await;
    assert_eq!(dropped.load(SeqCst), 2);
}

async fn assert_failure_terminates(failing: Failing, panics: Panics) -> Arc<Hooks> {
    // A logged error is formatted only under a subscriber which formats fields!
    init_tracing();

    let hooks = Arc::new(Hooks::new(panics));
    let system = ActorSystem::new(HostileFailure {
        failing,
        hooks: hooks.clone(),
    });
    if failing == Failing::Receive {
        system.root().tell(());
    }

    assert_terminates(system).await;
    hooks
}

/// Global, as a thread-local subscriber races with the callsite interest cache of parallel tests.
fn init_tracing() {
    TRACING.call_once(|| {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt().with_test_writer().finish(),
        )
        .expect("no other global subscriber is set");
    });
}

/// Runs a [Grandparent] whose [HastyParent] spawns an [Unborn] child and stops in the same
/// `receive`.
async fn assert_child_never_initializes<T, S>()
where
    T: TimerMode + 'static,
    S: StashMode + 'static,
{
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Grandparent::<T, S>::new(events_tx));

    let mut events = Vec::new();
    while events
        .iter()
        .filter(|event| matches!(event, Event::Terminated(_)))
        .count()
        < 2
    {
        events.push(recv_unbounded(&mut events_rx, "not all terminated signals arrived").await);
    }
    system.stop();
    assert_terminates(system).await;
    while let Ok(event) = events_rx.try_recv() {
        events.push(event);
    }

    assert!(
        !events.contains(&Event::UnbornInitialized),
        "the child ran init: {events:?}"
    );
    let parent_id = events
        .iter()
        .find_map(|event| match event {
            Event::ParentSpawned(id) => Some(*id),
            _ => None,
        })
        .expect("the parent was spawned");
    let child_id = events
        .iter()
        .find_map(|event| match event {
            Event::ChildSpawned(id) => Some(*id),
            _ => None,
        })
        .expect("the child was spawned");
    let position = |expected| {
        events
            .iter()
            .position(|event| *event == expected)
            .unwrap_or_else(|| panic!("{expected:?} is missing: {events:?}"))
    };
    assert!(position(Event::UnbornDropped) < position(Event::Terminated(child_id)));
    assert!(position(Event::UnbornDropped) < position(Event::ParentDropped));
    assert!(position(Event::ParentDropped) < position(Event::Terminated(parent_id)));
}

async fn recv<T>(rx: &mut mpsc::Receiver<T>, not_received: &str) -> T {
    timeout(TIMEOUT, rx.recv())
        .await
        .expect(not_received)
        .expect("channel closed")
}

async fn recv_unbounded<T>(rx: &mut mpsc::UnboundedReceiver<T>, not_received: &str) -> T {
    timeout(TIMEOUT, rx.recv())
        .await
        .expect(not_received)
        .expect("channel closed")
}

async fn assert_terminates<M>(system: ActorSystem<M>)
where
    M: Send + 'static,
{
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}

struct Root(Terminated, mpsc::Sender<()>);

impl Actor for Root {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.spawn(Child(self.0.child("child"), self.1.clone()));
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

/// A root which never stops on its own, so only an outside stop ends it.
struct Enduring(Terminated, mpsc::Sender<()>);

impl Actor for Enduring {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.spawn(Child(self.0.child("child"), self.1.clone()));
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

/// Spawns a [HastyParent] and watches it and the child it reports.
struct Grandparent<T, S> {
    events: mpsc::UnboundedSender<Event>,
    _modes: PhantomData<fn() -> (T, S)>,
}

impl<T, S> Grandparent<T, S> {
    fn new(events: mpsc::UnboundedSender<Event>) -> Self {
        Self {
            events,
            _modes: PhantomData,
        }
    }
}

impl<T, S> Actor for Grandparent<T, S>
where
    T: TimerMode + 'static,
    S: StashMode + 'static,
{
    type Message = ActorRef<()>;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let parent = context.spawn(HastyParent::<T, S> {
            grandparent: context.self_ref().clone(),
            events: self.events.clone(),
            _modes: PhantomData,
        });
        context.watch(&parent);
        let _ = self.events.send(Event::ParentSpawned(parent.actor_id()));
        parent.tell(());
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(child) => context.watch(&child),
            Incoming::Terminated(id) => {
                let _ = self.events.send(Event::Terminated(id));
            }
        }
        Ok(Control::Continue(state))
    }
}

/// Spawns an [Unborn] child, reports it to its [Grandparent] and stops, all in one `receive`.
struct HastyParent<T, S> {
    grandparent: ActorRef<ActorRef<()>>,
    events: mpsc::UnboundedSender<Event>,
    _modes: PhantomData<fn() -> (T, S)>,
}

impl<T, S> Actor for HastyParent<T, S>
where
    T: TimerMode + 'static,
    S: StashMode + 'static,
{
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let child = context.spawn(Unborn::<T, S>::new(self.events.clone()));
        let _ = self.events.send(Event::ChildSpawned(child.actor_id()));
        self.grandparent.tell(child);
        Ok(Control::Stop)
    }
}

impl<T, S> Drop for HastyParent<T, S> {
    fn drop(&mut self) {
        let _ = self.events.send(Event::ParentDropped);
    }
}

/// Reports its `init` and the drop of its actor value.
struct Unborn<T, S> {
    events: mpsc::UnboundedSender<Event>,
    _modes: PhantomData<fn() -> (T, S)>,
}

impl<T, S> Unborn<T, S> {
    fn new(events: mpsc::UnboundedSender<Event>) -> Self {
        Self {
            events,
            _modes: PhantomData,
        }
    }
}

impl<T, S> Actor for Unborn<T, S>
where
    T: TimerMode + 'static,
    S: StashMode + 'static,
{
    type Message = ();
    type Timers = T;
    type Stash = S;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message, T, S>) -> Result<Self::State, Self::Error> {
        let _ = self.events.send(Event::UnbornInitialized);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message, T, S>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

impl<T, S> Drop for Unborn<T, S> {
    fn drop(&mut self) {
        let _ = self.events.send(Event::UnbornDropped);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    ParentSpawned(ActorId),
    ChildSpawned(ActorId),
    UnbornInitialized,
    UnbornDropped,
    ParentDropped,
    Terminated(ActorId),
}

/// Spawns a [Subject], watches it and tells it to stop, then reports what it receives.
struct Witness(mpsc::UnboundedSender<Witnessed>);

impl Actor for Witness {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let subject = context.spawn(Subject(context.self_ref().clone()));
        context.watch(&subject);
        subject.tell(());
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => {
                let _ = self.0.send(Witnessed::Dropped);
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => {
                let _ = self.0.send(Witnessed::Terminated);
                Ok(Control::Stop)
            }
        }
    }
}

/// Stops on its first message and tells its [Witness] when its actor value is dropped.
struct Subject(ActorRef<()>);

impl Actor for Subject {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
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

impl Drop for Subject {
    fn drop(&mut self) {
        self.0.tell(());
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Witnessed {
    Dropped,
    Terminated,
}

/// Holds a state which reports its drop and spawns a [ReportingChild]; never stops on its own.
struct StatefulParent(mpsc::UnboundedSender<Lifecycle>);

impl Actor for StatefulParent {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ReportingState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.spawn(ReportingChild(self.0.clone()));
        let _ = self.0.send(Lifecycle::Started);
        Ok(ReportingState(self.0.clone()))
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

struct ReportingState(mpsc::UnboundedSender<Lifecycle>);

impl Drop for ReportingState {
    fn drop(&mut self) {
        let _ = self.0.send(Lifecycle::StateDropped);
    }
}

/// Reports the drop of its actor value.
struct ReportingChild(mpsc::UnboundedSender<Lifecycle>);

impl Actor for ReportingChild {
    type Message = Nothing;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

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

impl Drop for ReportingChild {
    fn drop(&mut self) {
        let _ = self.0.send(Lifecycle::ChildDropped);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Lifecycle {
    Started,
    StateDropped,
    ChildDropped,
}

/// Stops on [Hoarded::Stop], leaving whatever is queued behind it to the termination.
struct Hoarder;

impl Actor for Hoarder {
    type Message = Hoarded;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Hoarded::Stop) => Ok(Control::Stop),
            _ => Ok(Control::Continue(state)),
        }
    }
}

enum Hoarded {
    Stop,
    Bomb { _bomb: Bomb },
}

/// Counts its drop, then panics.
struct Bomb(Arc<AtomicUsize>);

impl Drop for Bomb {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
        panic!("queued message destructor panics on purpose");
    }
}

/// Blocks inside `receive` until released, so a test can stop the system while a message is being
/// handled; `receive` is synchronous, hence this blocks the thread.
struct Blocking {
    probe: mpsc::Sender<&'static str>,
    release: std_mpsc::Receiver<()>,
}

impl Actor for Blocking {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let _ = self.probe.try_send("started");
        let _ = block_in_place(|| self.release.recv());
        let _ = self.probe.try_send("finished");

        Ok(Control::Continue(state))
    }
}

struct Child(Terminated, mpsc::Sender<()>);

impl Actor for Child {
    type Message = Nothing;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let grand_child = GrandChild {
            _terminated: self.0.child("grandchild"),
            started: self.1.clone(),
        };
        context.spawn(grand_child);
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

struct GrandChild {
    _terminated: Terminated,
    started: mpsc::Sender<()>,
}

impl Actor for GrandChild {
    type Message = Nothing;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let _ = self.started.try_send(());
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

/// Report every message it receives, so that its sender can tell that this actor keeps running.
struct Echo(mpsc::Sender<()>);

impl Actor for Echo {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let _ = self.0.try_send(());
        Ok(Control::Continue(state))
    }
}

struct SelfSender {
    received_tx: mpsc::Sender<usize>,
}

impl Actor for SelfSender {
    type Message = SelfSend;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = usize;
    type Error = Infallible;

    /// The mailbox is FIFO, hence `Done` arrives behind every `Tick` and the count it reports is
    /// the number of ticks which survived.
    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for _ in 0..SELF_SENDS {
            context.self_ref().tell(SelfSend::Tick);
        }
        context.self_ref().tell(SelfSend::Done);
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(SelfSend::Tick) => Ok(Control::Continue(state + 1)),

            Incoming::Message(SelfSend::Done) => {
                let _ = self.received_tx.try_send(state);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

enum SelfSend {
    Tick,
    Done,
}

struct PanickingActor(mpsc::Sender<()>);

impl Drop for PanickingActor {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
        panic!("panicking actor destructor");
    }
}

impl Actor for PanickingActor {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
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

struct PanickingStateRoot(mpsc::Sender<ActorRef<Nothing>>);

impl Actor for PanickingStateRoot {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let _ = self.0.try_send(context.spawn(PanickingStateChild));
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

struct PanickingStateChild;

impl Actor for PanickingStateChild {
    type Message = Nothing;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = PanickingState;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(PanickingState)
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

struct PanickingState;

impl Drop for PanickingState {
    fn drop(&mut self) {
        panic!("panicking state destructor");
    }
}

struct FailingWithPanickingState;

impl Actor for FailingWithPanickingState {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = PanickingState;
    type Error = Boom;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(PanickingState)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        // A bare `_` would drop, hence panic, at binding instead of on the `Err` return.
        _state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Err(Boom)
    }
}

#[derive(Debug, Error)]
#[error("boom")]
struct Boom;

struct Observer(mpsc::Sender<Observed>);

impl Actor for Observer {
    type Message = ActorRef<Nothing>;
    type Timers = Untimed;
    type Stash = Unstashed;
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
            Incoming::Message(target) => {
                context.watch(&target);
                let _ = self.0.try_send(Observed::Watching);
            }

            Incoming::Terminated(_) => {
                let _ = self.0.try_send(Observed::Terminated);
            }
        }
        Ok(Control::Continue(state))
    }
}

/// What the observer saw: that it registered its watch, or the terminated signal itself.
#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Watching,
    Terminated,
}

struct Terminated(&'static str, mpsc::Sender<&'static str>);

impl Terminated {
    fn child(&self, name: &'static str) -> Self {
        Self(name, self.1.clone())
    }
}

impl Drop for Terminated {
    fn drop(&mut self) {
        let _ = self.1.try_send(self.0);
    }
}

struct HostileFailure {
    failing: Failing,
    hooks: Arc<Hooks>,
}

impl Actor for HostileFailure {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = HostileError;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        match self.failing {
            Failing::Init => Err(HostileError(self.hooks.clone())),
            Failing::Receive => Ok(()),
        }
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Err(HostileError(self.hooks.clone()))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Failing {
    Init,
    Receive,
}

struct HostileError(Arc<Hooks>);

impl Debug for HostileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("HostileError")
    }
}

impl Display for HostileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.display.store(true, SeqCst);
        if self.0.panics.display {
            panic!("error display panics on purpose");
        }
        f.write_str("hostile error")
    }
}

impl Error for HostileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source.store(true, SeqCst);
        if self.0.panics.source {
            panic!("error source panics on purpose");
        }
        None
    }
}

impl Drop for HostileError {
    fn drop(&mut self) {
        self.0.drop.store(true, SeqCst);
        if self.0.panics.drop {
            panic!("error destructor panics on purpose");
        }
    }
}

struct Hooks {
    panics: Panics,
    display: AtomicBool,
    source: AtomicBool,
    drop: AtomicBool,
}

impl Hooks {
    fn new(panics: Panics) -> Self {
        Self {
            panics,
            display: AtomicBool::new(false),
            source: AtomicBool::new(false),
            drop: AtomicBool::new(false),
        }
    }
}

#[derive(Clone, Copy)]
struct Panics {
    display: bool,
    source: bool,
    drop: bool,
}

impl Panics {
    fn display() -> Self {
        Self {
            display: true,
            source: false,
            drop: false,
        }
    }

    fn source() -> Self {
        Self {
            display: false,
            source: true,
            drop: false,
        }
    }

    fn drop() -> Self {
        Self {
            display: false,
            source: false,
            drop: true,
        }
    }

    fn display_and_drop() -> Self {
        Self {
            display: true,
            source: false,
            drop: true,
        }
    }
}
