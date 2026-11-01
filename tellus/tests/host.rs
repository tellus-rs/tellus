use std::{
    convert::Infallible,
    num::NonZeroUsize,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, Control, Host, HostConfig,
    HostEnvelope, Incoming, InvalidHostConfig, MailboxCapacity, ReplyTo, Untimed,
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc},
    time::{sleep, timeout},
};

const TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_millis(50);
const BUFFER: NonZeroUsize = NonZeroUsize::new(4).expect("4 is not zero");

/// An envelope spawns the entity of its key, and a key of its own gets an entity of its own.
#[tokio::test]
async fn an_envelope_spawns_the_entity_of_its_key() {
    let entities = Entities::new();
    let host = host(HostConfig::new(BUFFER), entities.clone());

    host.tell(note(1, "eins"));
    host.tell(note(2, "zwei"));

    let (first, _) = entities.note().await;
    let (second, _) = entities.note().await;

    assert_ne!(first, second);
    assert_eq!(entities.spawned(), 2);
}

/// A second envelope for one key reaches the same incarnation rather than spawning another.
#[tokio::test]
async fn a_second_envelope_reaches_the_same_entity() {
    let entities = Entities::new();
    let host = host(HostConfig::new(BUFFER), entities.clone());

    host.tell(note(1, "eins"));
    host.tell(note(1, "zwei"));

    let (first, _) = entities.note().await;
    let (second, _) = entities.note().await;

    assert_eq!(first, second);
    assert_eq!(entities.spawned(), 1);
}

/// An entity which stopped on its own is forgotten, and the next envelope spawns it afresh.
#[tokio::test]
async fn an_entity_which_stopped_is_spawned_afresh() {
    let entities = Entities::new();
    let host = host(HostConfig::new(BUFFER), entities.clone());

    host.tell(note(1, "eins"));
    let (first, _) = entities.note().await;

    host.tell(HostEnvelope {
        key: 1,
        message: Message::Stop,
    });
    entities.stopped().await;

    host.tell(note(1, "zwei"));
    let (second, _) = entities.note().await;

    assert_ne!(first, second);
    assert_eq!(entities.spawned(), 2);
}

/// An entity nobody messages is passivated once the idle timeout has elapsed; the paused clock
/// auto-advances, so the test does not actually wait.
#[tokio::test(start_paused = true)]
async fn an_idle_entity_is_passivated() {
    let entities = Entities::new();
    let host = host(idle_config(), entities.clone());

    host.tell(note(1, "eins"));
    entities.note().await;

    entities.stopped().await;
}

/// An entity which keeps being messaged is not passivated, since every envelope stamps it as
/// used; this one runs on the real clock, so the stamps and the sweeps interleave as they would.
#[tokio::test]
async fn a_busy_entity_is_not_passivated() {
    let entities = Entities::new();
    let host = host(
        HostConfig::new(BUFFER)
            .with_idle_timeout(Duration::from_millis(200))
            .expect("200ms is not zero"),
        entities.clone(),
    );

    host.tell(note(1, "eins"));
    let (first, _) = entities.note().await;

    for _ in 0..5 {
        sleep(Duration::from_millis(60)).await;
        host.tell(note(1, "weiter"));
        let (again, _) = entities.note().await;
        assert_eq!(again, first);
    }

    assert_eq!(entities.spawned(), 1);
}

/// A message arriving while an entity stops is buffered and delivered to its next incarnation,
/// never to the one on its way out.
#[tokio::test(start_paused = true)]
async fn a_message_during_the_stop_reaches_the_next_entity() {
    let entities = Entities::new().holding();
    let host = host(idle_config(), entities.clone());

    host.tell(note(1, "eins"));
    let (first, _) = entities.note().await;
    entities.passivating().await;

    host.tell(note(1, "zwei"));
    entities.release();

    let (second, note) = entities.note().await;
    assert_ne!(second, first);
    assert_eq!(note, "zwei");
}

/// Buffered messages keep their order and are delivered ahead of whatever is sent after the
/// respawn, so one sender's order holds across a passivation.
#[tokio::test(start_paused = true)]
async fn buffered_messages_keep_their_order() {
    let entities = Entities::new().holding();
    let host = host(idle_config(), entities.clone());

    host.tell(note(1, "eins"));
    entities.note().await;
    entities.passivating().await;

    for note in ["zwei", "drei", "vier"] {
        host.tell(self::note(1, note));
    }
    entities.release();

    assert_eq!(entities.note().await.1, "zwei");
    assert_eq!(entities.note().await.1, "drei");
    assert_eq!(entities.note().await.1, "vier");

    host.tell(note(1, "fünf"));
    assert_eq!(entities.note().await.1, "fünf");
}

/// A mailbox smaller than the host's buffer loses none of what the host accepted: the drain
/// reserves past the capacity. Until the entity has caught up the count stays over it, so
/// ordinary sends are refused the way a full mailbox refuses them, and work again afterward.
///
/// Blocking the replacement blocks a worker, so this one runs on real time and several threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bounded_entity_keeps_every_buffered_message() {
    // Only the replacement is gated, so it holds the drained messages while the test sends one.
    let entities = Entities::new().holding().bounded().gated_from_spawn(1);
    // A timeout with slack, so the sweep which passivates the first entity is the only one to
    // land while the assertions below run.
    let host = host(
        HostConfig::new(BUFFER)
            .with_idle_timeout(Duration::from_millis(200))
            .expect("200ms is not zero"),
        entities.clone(),
    );

    host.tell(note(1, "eins"));
    entities.note().await;
    entities.passivating().await;

    for note in ["zwei", "drei", "vier", "fünf"] {
        host.tell(self::note(1, note));
    }
    entities.release();

    // The replacement takes the first of the four and blocks, leaving three over its capacity.
    assert_eq!(entities.note().await.1, "zwei");
    host.tell(note(1, "sechs"));

    // The host routes in order, so its answer for another key proves it has routed "sechs" too,
    // while the replacement is still blocked and hence still over its capacity.
    host.tell(note(2, "merk"));
    assert_eq!(entities.note().await.1, "merk");

    entities.open();
    for note in ["drei", "vier", "fünf"] {
        assert_eq!(entities.note().await.1, note);
    }

    // "sechs" was refused rather than queued, so the next note is the one sent after the drain.
    host.tell(note(1, "sieben"));
    assert_eq!(entities.note().await.1, "sieben");
}

/// A full entity mailbox refuses the passivate message, so the entity stays live and reachable
/// instead of being counted as stopping and buried under a filling buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_entity_mailbox_is_not_passivated() {
    let entities = Entities::new().gated().bounded();
    let host = host(
        HostConfig::new(BUFFER)
            .with_idle_timeout(Duration::from_millis(100))
            .expect("100ms is not zero"),
        entities.clone(),
    );

    host.tell(note(1, "eins"));
    assert_eq!(entities.note().await.1, "eins");

    // The entity is at the gate with an empty mailbox, so this one fills it.
    host.tell(note(1, "zwei"));
    sleep(Duration::from_millis(300)).await;

    entities.open();
    assert_eq!(entities.note().await.1, "zwei");

    host.tell(note(1, "drei"));
    assert_eq!(entities.note().await.1, "drei");
}

/// An entity which does not stop on its passivate message keeps its messages buffered rather
/// than losing them, which is what makes the limitation a delay and not a loss.
#[tokio::test(start_paused = true)]
async fn an_entity_which_does_not_stop_keeps_its_messages() {
    let entities = Entities::new().holding();
    let host = host(idle_config(), entities.clone());

    host.tell(note(1, "eins"));
    entities.note().await;
    entities.passivating().await;

    host.tell(note(1, "zwei"));
    assert!(timeout(IDLE_TIMEOUT * 4, entities.notes()).await.is_err());

    entities.release();
    assert_eq!(entities.note().await.1, "zwei");
}

/// Beyond the buffer the newest message is refused, the way a full mailbox refuses one, and the
/// entity keeps working.
#[tokio::test(start_paused = true)]
async fn a_full_buffer_refuses_the_newest_message() {
    let entities = Entities::new().holding();
    let host = host(
        HostConfig::new(NonZeroUsize::MIN)
            .with_idle_timeout(IDLE_TIMEOUT)
            .expect("the idle timeout is not zero"),
        entities.clone(),
    );

    host.tell(note(1, "eins"));
    entities.note().await;
    entities.passivating().await;

    host.tell(note(1, "zwei"));
    host.tell(note(1, "drei"));
    entities.release();

    assert_eq!(entities.note().await.1, "zwei");

    host.tell(note(1, "vier"));
    assert_eq!(entities.note().await.1, "vier");
}

/// A passivation nobody followed spawns nothing: the entity is gone until it is messaged again,
/// which is what frees the memory.
#[tokio::test(start_paused = true)]
async fn nothing_is_spawned_after_a_passivation() {
    let entities = Entities::new();
    let host = host(idle_config(), entities.clone());

    host.tell(note(1, "eins"));
    entities.note().await;
    entities.stopped().await;

    sleep(IDLE_TIMEOUT * 4).await;
    assert_eq!(entities.spawned(), 1);

    host.tell(note(1, "zwei"));
    entities.note().await;
    assert_eq!(entities.spawned(), 2);
}

/// Stopping the host stops its entities and its ticker first, hence the whole tree terminates.
#[tokio::test]
async fn stopping_the_host_stops_its_entities() {
    let entities = Entities::new();
    let system = ActorSystem::new(Root {
        config: idle_config(),
        entities: entities.clone(),
    });

    let host = system
        .root()
        .ask(TIMEOUT, RootMessage::Host)
        .await
        .expect("the root answers with its host");
    host.tell(note(1, "eins"));
    host.tell(note(2, "zwei"));
    entities.note().await;
    entities.note().await;

    system.root().tell(RootMessage::Stop);

    timeout(TIMEOUT, system.terminated())
        .await
        .expect("the tree does not terminate")
        .expect("watching the root actor failed");
    entities.stopped().await;
    entities.stopped().await;
}

/// An ask through the host is answered by the entity of its key.
#[tokio::test]
async fn an_ask_reaches_the_entity() {
    let entities = Entities::new();
    let host = host(HostConfig::new(BUFFER), entities.clone());

    let entity = host
        .ask(TIMEOUT, |reply_to| HostEnvelope {
            key: 1,
            message: Message::Ping(reply_to),
        })
        .await
        .expect("the entity answers");

    host.tell(note(1, "eins"));
    assert_eq!(entities.note().await.0, entity);
}

/// A zero idle timeout is refused: it would have the host sweeping without pause, and leaving the
/// timeout unset is how passivation is switched off.
#[test]
fn a_zero_idle_timeout_is_refused() {
    assert_eq!(
        HostConfig::default()
            .with_idle_timeout(Duration::ZERO)
            .expect_err("a zero idle timeout is refused"),
        InvalidHostConfig::ZeroIdleTimeout
    );
    assert!(
        HostConfig::default()
            .with_idle_timeout(IDLE_TIMEOUT)
            .is_ok()
    );
}

fn host(config: HostConfig, entities: Entities) -> ActorRef<HostEnvelope<u8, Message>> {
    let system = ActorSystem::new(Host::new(
        config,
        move |key, context| entities.spawn(*key, context),
        || Message::Stop,
    ));
    let host = system.root().clone();
    // Dropping the system stops no actor, and a root host has no message which would stop it.
    drop(system);

    host
}

fn idle_config() -> HostConfig {
    HostConfig::new(BUFFER)
        .with_idle_timeout(IDLE_TIMEOUT)
        .expect("the idle timeout is not zero")
}

fn note(key: u8, note: &'static str) -> HostEnvelope<u8, Message> {
    HostEnvelope {
        key,
        message: Message::Note(note),
    }
}

struct Root {
    config: HostConfig,
    entities: Entities,
}

impl Actor for Root {
    type Message = RootMessage;
    type Timers = Untimed;
    type State = ActorRef<HostEnvelope<u8, Message>>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let entities = self.entities.clone();

        Ok(context.spawn(Host::new(
            self.config,
            move |key, context| entities.spawn(*key, context),
            || Message::Stop,
        )))
    }

    fn receive(
        &self,
        _context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(RootMessage::Host(reply_to)) => {
                reply_to.reply(state.clone());
                Ok(Control::Continue(state))
            }

            Incoming::Message(RootMessage::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

enum RootMessage {
    Host(ReplyTo<ActorRef<HostEnvelope<u8, Message>>>),
    Stop,
}

enum Message {
    Note(&'static str),
    Ping(ReplyTo<ActorId>),
    Stop,
    Die,
}

struct Entity {
    key: u8,
    gate: Option<Gate>,
    entities: Entities,
}

impl Actor for Entity {
    type Message = Message;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let actor_id = context.self_ref().actor_id();

        match incoming {
            Incoming::Message(Message::Note(note)) => {
                let _ = self.entities.notes.send((actor_id, note));

                // Blocking after the note, so a test knows the entity is about to stop draining!
                if let Some(gate) = &self.gate {
                    gate.wait();
                }
            }

            Incoming::Message(Message::Ping(reply_to)) => reply_to.reply(actor_id),

            Incoming::Message(Message::Stop) => {
                // Must be created before the report, which a release may follow at once!
                let released = self.entities.hold.clone().map(Notify::notified_owned);
                let _ = self.entities.passivating.send(actor_id);

                match released {
                    Some(released) => {
                        let entity = context.self_ref().clone();
                        tokio::spawn(async move {
                            released.await;
                            entity.tell(Message::Die);
                        });
                    }

                    None => return Ok(Control::Stop),
                }
            }

            Incoming::Message(Message::Die) => return Ok(Control::Stop),

            Incoming::Terminated(_) => {}
        }

        Ok(Control::Continue(state))
    }
}

impl Drop for Entity {
    fn drop(&mut self) {
        let _ = self.entities.stopped.send(self.key);
    }
}

/// The entities of one test: their observations, and the knobs deciding how they behave.
#[derive(Clone)]
struct Entities {
    notes: mpsc::UnboundedSender<(ActorId, &'static str)>,
    received: Arc<AsyncMutex<mpsc::UnboundedReceiver<(ActorId, &'static str)>>>,
    passivating: mpsc::UnboundedSender<ActorId>,
    passivated: Arc<AsyncMutex<mpsc::UnboundedReceiver<ActorId>>>,
    stopped: mpsc::UnboundedSender<u8>,
    gone: Arc<AsyncMutex<mpsc::UnboundedReceiver<u8>>>,
    spawns: Arc<AtomicUsize>,
    hold: Option<Arc<Notify>>,
    gate: Option<Gate>,
    gated_from_spawn: usize,
    bounded: bool,
}

impl Entities {
    fn new() -> Self {
        let (notes, received) = mpsc::unbounded_channel();
        let (passivating, passivated) = mpsc::unbounded_channel();
        let (stopped, gone) = mpsc::unbounded_channel();

        Self {
            notes,
            received: Arc::new(AsyncMutex::new(received)),
            passivating,
            passivated: Arc::new(AsyncMutex::new(passivated)),
            stopped,
            gone: Arc::new(AsyncMutex::new(gone)),
            spawns: Arc::default(),
            hold: None,
            gate: None,
            gated_from_spawn: 0,
            bounded: false,
        }
    }

    /// Entities which do not stop on their passivate message until [Entities::release].
    fn holding(self) -> Self {
        Self {
            hold: Some(Arc::new(Notify::new())),
            ..self
        }
    }

    /// Entities which block in `receive` until [Entities::open], so their mailbox fills up.
    fn gated(self) -> Self {
        Self {
            gate: Some(Gate::new()),
            ..self
        }
    }

    /// Gated, but only from the given spawn on, so a test can block a replacement while the
    /// entity it replaces runs freely.
    fn gated_from_spawn(self, gated_from_spawn: usize) -> Self {
        Self {
            gated_from_spawn,
            ..self.gated()
        }
    }

    /// Entities whose mailbox holds one message.
    fn bounded(self) -> Self {
        Self {
            bounded: true,
            ..self
        }
    }

    fn spawn(
        &self,
        key: u8,
        context: &ActorContext<HostEnvelope<u8, Message>>,
    ) -> ActorRef<Message> {
        let spawned = self.spawns.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gate
            .clone()
            .filter(|_| spawned >= self.gated_from_spawn);

        let entity = Entity {
            key,
            gate,
            entities: self.clone(),
        };

        if self.bounded {
            let config = ActorConfig::default()
                .with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
            context.spawn_with_config(entity, config)
        } else {
            context.spawn(entity)
        }
    }

    fn spawned(&self) -> usize {
        self.spawns.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.hold
            .as_ref()
            .expect("the entities hold their stop")
            .notify_waiters();
    }

    fn open(&self) {
        self.gate.as_ref().expect("the entities are gated").open();
    }

    async fn note(&self) -> (ActorId, &'static str) {
        timeout(TIMEOUT, self.notes())
            .await
            .expect("no note arrives in time")
    }

    async fn notes(&self) -> (ActorId, &'static str) {
        let mut received = self.received.lock().await;
        received.recv().await.expect("the notes are not closed")
    }

    async fn passivating(&self) -> ActorId {
        let passivating = async {
            let mut passivated = self.passivated.lock().await;
            passivated
                .recv()
                .await
                .expect("the passivations are not closed")
        };

        timeout(TIMEOUT, passivating)
            .await
            .expect("no entity is asked to stop in time")
    }

    async fn stopped(&self) -> u8 {
        let stopped = async {
            let mut gone = self.gone.lock().await;
            gone.recv().await.expect("the stops are not closed")
        };

        timeout(TIMEOUT, stopped)
            .await
            .expect("no entity stops in time")
    }
}

/// Blocks whoever waits on it until it is opened, so an entity can be kept from draining its
/// mailbox; `receive` is synchronous, hence this blocks the thread rather than awaiting.
#[derive(Clone)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait(&self) {
        let (open, opened) = &*self.0;
        let mut open = open.lock().expect("the gate is not poisoned");
        while !*open {
            open = opened.wait(open).expect("the gate is not poisoned");
        }
    }

    fn open(&self) {
        let (open, opened) = &*self.0;
        *open.lock().expect("the gate is not poisoned") = true;
        opened.notify_all();
    }
}
