use std::{
    convert::Infallible,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::SeqCst},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorSystem, AskError, Backoff, Control, Incoming,
    MailboxCapacity, ReplyTo, RestartPolicy, SupervisionStrategy, Timed, Untimed,
};
use thiserror::Error;
use tokio::{sync::mpsc, task::spawn, time::timeout};

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

/// The stash survives a restart: an item stashed by the first incarnation is received by the
/// second.
#[tokio::test]
async fn the_stash_survives_a_restart() {
    let backoff = Backoff::new(Duration::from_millis(1), Duration::from_millis(1))
        .expect("the bounds are valid");
    let config = ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(NonZeroU32::MIN).with_backoff(backoff),
    ));
    let (system, mut events) = probe_system(config);

    system.root().tell(ProbeMessage::Hold);
    system.root().tell(ProbeMessage::Keep(7));
    system.root().tell(ProbeMessage::Fail);
    system.root().tell(ProbeMessage::Release);

    assert_eq!(next_event(&mut events).await, ProbeEvent::Stashed(7));
    assert_eq!(next_event(&mut events).await, ProbeEvent::Kept(7, 2));
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

/// Stash what arrives while holding, messages and terminated signals alike, and report every step.
struct Probe {
    events: mpsc::UnboundedSender<ProbeEvent>,
    generations: Arc<AtomicU32>,
}

impl Actor for Probe {
    type Message = ProbeMessage;
    type Timers = Timed;
    type State = Holding;
    type Error = Failed;

    fn init(&self, _: &ActorContext<Self::Message, Timed>) -> Result<Self::State, Self::Error> {
        Ok(Holding {
            generation: self.generations.fetch_add(1, SeqCst) + 1,
            holding: false,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
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
            }

            Incoming::Message(ProbeMessage::Fail) => return Err(Failed),

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
    fn stash(&self, context: &ActorContext<ProbeMessage, Timed>, incoming: Incoming<ProbeMessage>) {
        context.stash(incoming).expect("the stash is unbounded");
    }

    fn emit(&self, event: ProbeEvent) {
        self.events.send(event).expect("test dropped the receiver");
    }
}

struct Holding {
    generation: u32,
    holding: bool,
}

#[derive(Debug)]
enum ProbeMessage {
    Hold,
    Release,
    ReleaseWithTimer,
    Fill(u32),
    Watch,
    Fail,
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

/// A resource which is closed or open and holds what was last written to it. Closed, it handles
/// `Open` only; open, it handles everything but `Open`. What it cannot handle in its state it
/// stashes, and every state change unstashes.
struct Resource {
    events: mpsc::UnboundedSender<Event>,
}

impl Actor for Resource {
    type Message = Message;
    type Timers = Untimed;
    type State = State;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(State::Closed { data: "" })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
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
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicBool, Ordering::SeqCst},
    };
    use tellus::{
        ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Nothing,
        Persistence, PersistenceId, Untimed,
    };
    use tokio::sync::mpsc;

    /// An event-sourced actor stashes through `handle` the same way: the command stashed while
    /// holding is received after unstashing.
    #[tokio::test]
    async fn an_event_sourced_actor_stashes_through_handle() {
        let (events_tx, mut events) = mpsc::unbounded_channel();
        let journal = Journal {
            holding: AtomicBool::new(true),
            events: events_tx,
        };
        let system =
            ActorSystem::event_sourced(journal, Persistence::new(InMemoryStore::default()));

        system.root().tell(Command::Keep(5));
        system.root().tell(Command::Release);

        assert_eq!(super::next_event(&mut events).await, Event::Stashed(5));
        assert_eq!(super::next_event(&mut events).await, Event::Kept(5));
        super::stop(system).await;
    }

    /// Stash `Keep` while holding; `Release` stops holding and unstashes.
    struct Journal {
        holding: AtomicBool,
        events: mpsc::UnboundedSender<Event>,
    }

    impl EventSourced for Journal {
        type Command = Command;
        type Timers = Untimed;
        type Event = Nothing;
        type State = ();
        type Snapshot = Nothing;
        type Error = Infallible;

        fn persistence_id(&self) -> PersistenceId {
            PersistenceId::new("journal", "1").expect("the segments are valid")
        }

        fn init(&self) -> Result<Self::State, Self::Error> {
            Ok(())
        }

        fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
            match snapshot {}
        }

        fn handle(
            &self,
            context: &ActorContext<Self::Command>,
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
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Stashed(u32),
        Kept(u32),
    }
}
