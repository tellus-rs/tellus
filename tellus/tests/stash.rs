use std::{
    convert::Infallible,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::SeqCst},
        mpsc as std_mpsc,
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AskError, Backoff, Control, Incoming,
    MailboxCapacity, ReplyTo, RestartPolicy, Stashed, SupervisionStrategy, Timed, Unstashed,
    Untimed,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{block_in_place, spawn},
    time::timeout,
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// Stashed messages are handled when the actor unstashes, ahead of messages which arrived later:
/// the read stashed while closed runs before the open and the write queued behind it, and the
/// open stashed while open runs before the read queued behind it.
#[tokio::test]
async fn stashed_messages_are_handled_before_later_ones() {
    let (system, mut events) = resource_system();

    for message in [
        Message::Read,
        Message::Open,
        Message::Open,
        Message::Write("I love stash"),
        Message::Close,
        Message::Read,
    ] {
        system.root().tell(message);
    }

    for expected in [
        Event::Stashed(Message::Read),
        Event::Opened,
        Event::Read(""),
        Event::Stashed(Message::Open),
        Event::Wrote("I love stash"),
        Event::Closed,
        Event::Opened,
        Event::Read("I love stash"),
    ] {
        assert_eq!(next_event(&mut events).await, expected);
    }
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A full stash hands the incoming back: with a mailbox bounded to one, the second of two items
/// stashed in one `receive` is refused, and the first is received after unstashing.
#[tokio::test]
async fn a_full_stash_hands_the_incoming_back() {
    let config =
        ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
    let (system, mut events) = probe_system(config);

    system.root().tell(ProbeMessage::Fill(2));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(0));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Refused(1));

    system.root().tell(ProbeMessage::Release);
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(0, 1));
    stop(system).await;
}

/// A terminated signal skips the stash capacity, as it skips the mailbox's: with a mailbox bounded
/// to one, the signal is stashed behind a stashed message, and both are received in order after
/// unstashing.
#[tokio::test]
async fn a_terminated_signal_skips_the_stash_capacity() {
    let config =
        ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
    let (system, mut events) = probe_system(config);

    system.root().tell(ProbeMessage::Fill(1));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(0));
    system.root().tell(ProbeMessage::Watch);
    assert_eq!(next_event(&mut events).await, ProbeEvent::StashedTerminated);

    system.root().tell(ProbeMessage::Release);
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(0, 1));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Terminated);
    stop(system).await;
}

/// A restart hands the stash back to the new state, without the stash requests of the failed
/// `receive`: the item stashed before the failure is received by the second incarnation ahead of
/// the mailbox, the one stashed by the failing call is not received at all.
#[tokio::test]
async fn a_restart_hands_the_stash_back_without_the_failed_requests() {
    let (system, mut events) = probe_system(restart_config());

    system.root().tell(ProbeMessage::Hold);
    system.root().tell(ProbeMessage::Keep(1));
    system.root().tell(ProbeMessage::StashAndFail(2));
    system.root().tell(ProbeMessage::Keep(3));

    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(1));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(1, 2));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(3, 2));
    stop(system).await;
}

/// Termination drops the stash like the mailbox: a request stashed inside resolves its `ask` as
/// `NoReply`.
#[tokio::test]
async fn termination_drops_the_stash() {
    let (system, mut events) = probe_system(ActorConfig::default());

    system.root().tell(ProbeMessage::Hold);
    let root = system.root().clone();
    let asked = spawn(async move { root.ask(TIMEOUT, ProbeMessage::Ask).await });
    assert_eq!(next_event(&mut events).await, ProbeEvent::StashedAsk);

    stop(system).await;
    let asked = asked.await.expect("the ask task panicked");
    assert!(matches!(asked, Err(AskError::NoReply)));
}

/// A stashed terminated signal is received after unstashing: a watcher defers it like a message.
#[tokio::test]
async fn a_stashed_terminated_signal_is_received_after_unstashing() {
    let (system, mut events) = probe_system(ActorConfig::default());

    system.root().tell(ProbeMessage::Hold);
    system.root().tell(ProbeMessage::Watch);
    assert_eq!(next_event(&mut events).await, ProbeEvent::StashedTerminated);

    system.root().tell(ProbeMessage::Release);
    assert_eq!(next_event(&mut events).await, ProbeEvent::Terminated);
    stop(system).await;
}

/// `unwatch` drops a stashed terminated signal: once the watcher has unwatched the stopped actor,
/// unstashing hands back only the message stashed after the signal.
#[tokio::test]
async fn unwatch_drops_a_stashed_terminated_signal() {
    let (system, mut events) = probe_system(ActorConfig::default());

    system.root().tell(ProbeMessage::Hold);
    system.root().tell(ProbeMessage::Watch);
    assert_eq!(next_event(&mut events).await, ProbeEvent::StashedTerminated);
    system.root().tell(ProbeMessage::Keep(4));
    system.root().tell(ProbeMessage::Unwatch);
    system.root().tell(ProbeMessage::Release);

    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(4));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(4, 1));
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// Unstashed items are received before a due timer: they arrived before its deadline.
#[tokio::test]
async fn unstashed_items_are_received_before_a_due_timer() {
    let (system, mut events) = probe_system(ActorConfig::default());

    system.root().tell(ProbeMessage::Hold);
    system.root().tell(ProbeMessage::Keep(3));
    system.root().tell(ProbeMessage::ReleaseWithTimer);

    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(3));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(3, 1));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Fired);
    stop(system).await;
}

/// A stop by the parent is received ahead of the unstashed items: the first of two unstashed items
/// blocks until the system has been asked to stop, and the second is never received.
#[tokio::test(flavor = "multi_thread")]
async fn a_stop_is_received_ahead_of_unstashed_items() {
    let (system, mut kept, gate) = drainer_system();

    system.root().tell(DrainerMessage::Hold);
    system.root().tell(DrainerMessage::Keep(1));
    system.root().tell(DrainerMessage::Keep(2));
    system.root().tell(DrainerMessage::Release);
    assert_eq!(next_event(&mut kept).await, 1);

    system.stop();
    gate.send(()).expect("the drainer dropped the gate");
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
    assert_eq!(kept.recv().await, None);
}

/// A drain cannot starve the mailbox: an actor which stashes every item again and unstashes on
/// every receive drains once per message instead of forever. The ask queued behind two waiting
/// steps is answered after five receptions of them, and the actor stops when asked.
#[tokio::test]
async fn a_drain_cannot_starve_the_mailbox() {
    let (system, _handled) = chain_system();

    system.root().tell(ChainMessage::Step(3));
    system.root().tell(ChainMessage::Step(2));
    let received = system
        .root()
        .ask(TIMEOUT, ChainMessage::Received)
        .await
        .expect("the ask behind the drains was not answered");

    assert_eq!(received, 5);
    stop(system).await;
}

/// A drain which handles an item takes a held `unstash_all` into effect once it is over, without
/// waiting for another message: of 3 waiting for 2 and 2 waiting for 1, 3 is handled right after 2.
#[tokio::test]
async fn a_drain_which_handles_an_item_unstashes_again_at_once() {
    let (system, mut handled) = chain_system();

    for n in [3, 2, 1] {
        system.root().tell(ChainMessage::Step(n));
    }

    for n in [1, 2, 3] {
        assert_eq!(next_event(&mut handled).await, n);
    }
    stop(system).await;
}

/// Stash what arrives while holding, messages and terminated signals alike, and report every step.
struct Probe {
    events: mpsc::UnboundedSender<ProbeEvent>,
    generations: Arc<AtomicU32>,
}

impl Actor for Probe {
    type Message = ProbeMessage;
    type Timers = Timed;
    type Stash = Stashed;
    type State = Holding;
    type Error = Failed;

    fn init(
        &self,
        _: &ActorContext<Self::Message, Timed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        Ok(Holding {
            generation: self.generations.fetch_add(1, SeqCst) + 1,
            holding: false,
            watched: None,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed, Stashed>,
        incoming: Incoming<Self::Message>,
        mut holding: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ProbeMessage::Hold) => holding.holding = true,

            Incoming::Message(ProbeMessage::Release) => {
                holding.holding = false;
                context.unstash_all();
            }

            Incoming::Message(ProbeMessage::ReleaseWithTimer) => {
                holding.holding = false;
                context.start_timer("fired", ProbeMessage::Fired, Duration::ZERO);
                context.unstash_all();
            }

            Incoming::Message(ProbeMessage::Fill(count)) => {
                holding.holding = true;
                for n in 0..count {
                    match context.stash(Incoming::Message(ProbeMessage::Keep(n))) {
                        Ok(()) => self.emit(ProbeEvent::Stashed(n)),
                        Err(_) => self.emit(ProbeEvent::Refused(n)),
                    }
                }
            }

            Incoming::Message(ProbeMessage::Watch) => {
                let stopper = context.spawn(Stopper);
                context.watch(&stopper);
                stopper.tell(());
                holding.watched = Some(stopper);
            }

            Incoming::Message(ProbeMessage::Unwatch) => {
                if let Some(stopper) = &holding.watched {
                    context.unwatch(stopper);
                }
            }

            Incoming::Message(ProbeMessage::StashAndFail(n)) => {
                self.stash(context, Incoming::Message(ProbeMessage::Keep(n)));
                return Err(Failed);
            }

            Incoming::Message(ProbeMessage::Fired) => self.emit(ProbeEvent::Fired),

            Incoming::Message(ProbeMessage::Keep(n)) if holding.holding => {
                self.stash(context, Incoming::Message(ProbeMessage::Keep(n)));
                self.emit(ProbeEvent::Stashed(n));
            }

            Incoming::Message(ProbeMessage::Keep(n)) => {
                self.emit(ProbeEvent::Kept(n, holding.generation));
            }

            Incoming::Message(ProbeMessage::Ask(reply_to)) if holding.holding => {
                self.stash(context, Incoming::Message(ProbeMessage::Ask(reply_to)));
                self.emit(ProbeEvent::StashedAsk);
            }

            Incoming::Message(ProbeMessage::Ask(reply_to)) => reply_to.reply(holding.generation),

            Incoming::Terminated(other) if holding.holding => {
                self.stash(context, Incoming::Terminated(other));
                self.emit(ProbeEvent::StashedTerminated);
            }

            Incoming::Terminated(_) => self.emit(ProbeEvent::Terminated),
        }
        Ok(Control::Continue(holding))
    }
}

impl Probe {
    fn stash(
        &self,
        context: &ActorContext<ProbeMessage, Timed, Stashed>,
        incoming: Incoming<ProbeMessage>,
    ) {
        context.stash(incoming).expect("the stash has room");
    }

    fn emit(&self, event: ProbeEvent) {
        self.events.send(event).expect("test dropped the receiver");
    }
}

struct Holding {
    generation: u32,
    holding: bool,
    watched: Option<ActorRef<()>>,
}

#[derive(Debug)]
enum ProbeMessage {
    Hold,
    Release,
    ReleaseWithTimer,
    Fill(u32),
    Watch,
    Unwatch,
    StashAndFail(u32),
    Fired,
    Keep(u32),
    Ask(ReplyTo<u32>),
}

#[derive(Debug, PartialEq, Eq)]
enum ProbeEvent {
    Stashed(u32),
    Refused(u32),
    Kept(u32, u32),
    StashedAsk,
    StashedTerminated,
    Terminated,
    Fired,
}

#[derive(Debug, Error)]
#[error("probe failed on purpose")]
struct Failed;

/// Stop on the first message.
struct Stopper;

impl Actor for Stopper {
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
        (): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

/// Handle `Step(n)` only once `Step(n - 1)` has been handled, stashing it until then, and unstash
/// on every receive, as a retry loop does. Report each handled step.
struct Chain {
    handled: mpsc::UnboundedSender<u32>,
}

impl Actor for Chain {
    type Message = ChainMessage;
    type Timers = Untimed;
    type Stash = Stashed;
    type State = Steps;
    type Error = Infallible;

    fn init(
        &self,
        _: &ActorContext<Self::Message, Untimed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        Ok(Steps {
            next: 1,
            received: 0,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
        incoming: Incoming<Self::Message>,
        mut steps: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the chain watches no actor, hence never gets a terminated signal")
        };

        match message {
            ChainMessage::Step(n) => {
                steps.received += 1;
                if n == steps.next {
                    steps.next += 1;
                    self.handled.send(n).expect("test dropped the receiver");
                } else {
                    context
                        .stash(Incoming::Message(ChainMessage::Step(n)))
                        .expect("the stash is unbounded");
                }
            }

            ChainMessage::Received(reply_to) => reply_to.reply(steps.received),
        }
        context.unstash_all();
        Ok(Control::Continue(steps))
    }
}

struct Steps {
    next: u32,
    received: u32,
}

#[derive(Debug)]
enum ChainMessage {
    Step(u32),
    Received(ReplyTo<u32>),
}

/// Stash `Keep` while holding, report each `Keep` received, and block on the gate while receiving
/// `Keep(1)`.
struct Drainer {
    kept: mpsc::UnboundedSender<u32>,
    gate: std_mpsc::Receiver<()>,
}

impl Actor for Drainer {
    type Message = DrainerMessage;
    type Timers = Untimed;
    type Stash = Stashed;
    type State = bool;
    type Error = Infallible;

    fn init(
        &self,
        _: &ActorContext<Self::Message, Untimed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        Ok(false)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
        incoming: Incoming<Self::Message>,
        holding: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the drainer watches no actor, hence never gets a terminated signal")
        };

        match message {
            DrainerMessage::Hold => return Ok(Control::Continue(true)),

            DrainerMessage::Keep(n) if holding => {
                context
                    .stash(Incoming::Message(DrainerMessage::Keep(n)))
                    .expect("the stash is unbounded");
            }

            DrainerMessage::Keep(n) => {
                self.kept.send(n).expect("test dropped the receiver");
                if n == 1 {
                    block_in_place(|| self.gate.recv()).expect("test dropped the gate");
                }
            }

            DrainerMessage::Release => {
                context.unstash_all();
                return Ok(Control::Continue(false));
            }
        }
        Ok(Control::Continue(holding))
    }
}

#[derive(Debug)]
enum DrainerMessage {
    Hold,
    Keep(u32),
    Release,
}

fn probe_system(
    config: ActorConfig,
) -> (
    ActorSystem<ProbeMessage>,
    mpsc::UnboundedReceiver<ProbeEvent>,
) {
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let probe = Probe {
        events: events_tx,
        generations: Arc::new(AtomicU32::new(0)),
    };
    (ActorSystem::with_config(probe, config), events_rx)
}

fn drainer_system() -> (
    ActorSystem<DrainerMessage>,
    mpsc::UnboundedReceiver<u32>,
    std_mpsc::Sender<()>,
) {
    let (kept_tx, kept_rx) = mpsc::unbounded_channel();
    let (gate_tx, gate_rx) = std_mpsc::channel();
    let drainer = Drainer {
        kept: kept_tx,
        gate: gate_rx,
    };
    (ActorSystem::new(drainer), kept_rx, gate_tx)
}

fn chain_system() -> (ActorSystem<ChainMessage>, mpsc::UnboundedReceiver<u32>) {
    let (handled_tx, handled_rx) = mpsc::unbounded_channel();
    (
        ActorSystem::new(Chain {
            handled: handled_tx,
        }),
        handled_rx,
    )
}

fn restart_config() -> ActorConfig {
    let backoff = Backoff::new(Duration::from_millis(1), Duration::from_millis(1))
        .expect("the bounds are valid");
    ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(NonZeroU32::MIN).with_backoff(backoff),
    ))
}

/// A resource which is closed or open and holds what was last written to it. Closed, it handles
/// `Open` only; open, it handles everything but `Open`. What it cannot handle in its state it
/// stashes, and every state change unstashes.
struct Resource {
    events: mpsc::UnboundedSender<Event>,
}

impl Actor for Resource {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Stashed;
    type State = State;
    type Error = Infallible;

    fn init(
        &self,
        _: &ActorContext<Self::Message, Untimed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        Ok(State::Closed { data: "" })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the resource watches no actor, hence never gets a terminated signal")
        };

        let state = match (state, message) {
            (State::Closed { data }, Message::Open) => {
                self.emit(Event::Opened);
                context.unstash_all();
                State::Open { data }
            }

            (State::Open { data }, Message::Close) => {
                self.emit(Event::Closed);
                context.unstash_all();
                State::Closed { data }
            }

            (State::Open { data }, Message::Read) => {
                self.emit(Event::Read(data));
                State::Open { data }
            }

            (State::Open { .. }, Message::Write(data)) => {
                self.emit(Event::Wrote(data));
                State::Open { data }
            }

            (state, message) => {
                self.emit(Event::Stashed(message));
                context
                    .stash(Incoming::Message(message))
                    .expect("the stash is unbounded");
                state
            }
        };
        Ok(Control::Continue(state))
    }
}

impl Resource {
    fn emit(&self, event: Event) {
        self.events.send(event).expect("test dropped the receiver");
    }
}

enum State {
    Closed { data: &'static str },
    Open { data: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Message {
    Open,
    Close,
    Read,
    Write(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Stashed(Message),
    Opened,
    Closed,
    Read(&'static str),
    Wrote(&'static str),
}

fn resource_system() -> (ActorSystem<Message>, mpsc::UnboundedReceiver<Event>) {
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    (ActorSystem::new(Resource { events: events_tx }), events_rx)
}

async fn next_event<E>(events: &mut mpsc::UnboundedReceiver<E>) -> E
where
    E: Debug,
{
    timeout(TIMEOUT, events.recv())
        .await
        .expect("no event in time")
        .expect("actor dropped the sender")
}

async fn stop<M>(system: ActorSystem<M>)
where
    M: Send + 'static,
{
    system.stop();
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}

#[cfg(feature = "persistence-in-memory")]
mod event_sourced {
    use crate::Failed;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    use tellus::{
        ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Nothing,
        Persistence, PersistenceId, Stashed, Untimed,
    };
    use tokio::sync::mpsc;

    /// An event-sourced actor stashes through `handle` the same way: the command stashed while
    /// holding is received after unstashing.
    #[tokio::test]
    async fn an_event_sourced_actor_stashes_through_handle() {
        let (journal, mut events) = journal();
        let system =
            ActorSystem::event_sourced(journal, Persistence::new(InMemoryStore::default()));

        system.root().tell(Command::Keep(5));
        system.root().tell(Command::Release);

        for expected in [Event::Recovered, Event::Stashed(5), Event::Kept(5)] {
            assert_eq!(super::next_event(&mut events).await, expected);
        }
        super::stop(system).await;
    }

    /// A restart hands the stash of an event-sourced actor back once it has recovered: the command
    /// stashed before the failure is received again after `recovered`, ahead of the mailbox.
    #[tokio::test]
    async fn a_restart_hands_the_stash_back_after_recovered() {
        let (journal, mut events) = journal();
        let system = ActorSystem::event_sourced_with_config(
            journal,
            Persistence::new(InMemoryStore::default()),
            super::restart_config(),
        );

        system.root().tell(Command::Keep(6));
        system.root().tell(Command::Fail);
        system.root().tell(Command::Release);

        for expected in [
            Event::Recovered,
            Event::Stashed(6),
            Event::Recovered,
            Event::Stashed(6),
            Event::Kept(6),
        ] {
            assert_eq!(super::next_event(&mut events).await, expected);
        }
        super::stop(system).await;
    }

    /// Stash `Keep` while holding; `Release` stops holding and unstashes, `Fail` fails.
    struct Journal {
        holding: AtomicBool,
        events: mpsc::UnboundedSender<Event>,
    }

    impl EventSourced for Journal {
        type Command = Command;
        type Timers = Untimed;
        type Stash = Stashed;
        type Event = Nothing;
        type State = ();
        type Snapshot = Nothing;
        type Error = Failed;

        fn persistence_id(&self) -> PersistenceId {
            PersistenceId::new("journal", "1").expect("the segments are valid")
        }

        fn init(&self) -> Result<Self::State, Self::Error> {
            Ok(())
        }

        fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
            match snapshot {}
        }

        fn recovered(
            &self,
            _: &ActorContext<Self::Command, Untimed, Stashed>,
            (): Self::State,
        ) -> Result<Self::State, Self::Error> {
            self.emit(Event::Recovered);
            Ok(())
        }

        fn handle(
            &self,
            context: &ActorContext<Self::Command, Untimed, Stashed>,
            incoming: Incoming<Self::Command>,
            (): &Self::State,
        ) -> Result<Effect<Self>, Self::Error> {
            match incoming {
                Incoming::Message(Command::Keep(n)) if self.holding.load(SeqCst) => {
                    context
                        .stash(Incoming::Message(Command::Keep(n)))
                        .expect("the stash is unbounded");
                    self.emit(Event::Stashed(n));
                }

                Incoming::Message(Command::Keep(n)) => self.emit(Event::Kept(n)),

                Incoming::Message(Command::Release) => {
                    self.holding.store(false, SeqCst);
                    context.unstash_all();
                }

                Incoming::Message(Command::Fail) => return Err(Failed),

                Incoming::Terminated(_) => {
                    unreachable!(
                        "the journal watches no actor, hence never gets a terminated signal"
                    )
                }
            }
            Ok(Effect::none())
        }

        fn apply(&self, (): Self::State, event: Self::Event) -> Self::State {
            match event {}
        }
    }

    impl Journal {
        fn emit(&self, event: Event) {
            self.events.send(event).expect("test dropped the receiver");
        }
    }

    #[derive(Debug)]
    enum Command {
        Keep(u32),
        Release,
        Fail,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Recovered,
        Stashed(u32),
        Kept(u32),
    }

    fn journal() -> (Journal, mpsc::UnboundedReceiver<Event>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let journal = Journal {
            holding: AtomicBool::new(true),
            events: events_tx,
        };
        (journal, events_rx)
    }
}
