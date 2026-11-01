#![cfg(feature = "persistence-in-memory")]

use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, Host, HostConfig, HostEnvelope, InMemoryStore,
    Incoming, Nothing, Persistence, PersistenceId, ReplyTo, SchemaVersion, Unstashed, Untimed,
    Versioned,
};

const TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_millis(50);

/// An event-sourced entity recovers its state when the next message spawns it afresh, so
/// passivating it costs the replay and nothing else; the paused clock auto-advances, so the test
/// does not actually wait.
#[tokio::test(start_paused = true)]
async fn an_event_sourced_entity_recovers_after_passivation() {
    let store = InMemoryStore::default();
    let spawns = Arc::new(AtomicUsize::new(0));

    let host = {
        let persistence = Persistence::new(store);
        let spawns = spawns.clone();

        let system = ActorSystem::new(Host::new(
            HostConfig::default()
                .with_idle_timeout(IDLE_TIMEOUT)
                .expect("the idle timeout is not zero"),
            move |key: &u8, context: &ActorContext<HostEnvelope<u8, Command>>| {
                spawns.fetch_add(1, Ordering::SeqCst);
                context.spawn_event_sourced(Counter { key: *key }, persistence.clone())
            },
            || Command::Stop,
        ));
        let host = system.root().clone();
        // Dropping the system stops no actor, and a root host has no message which would stop it.
        drop(system);

        host
    };

    host.tell(HostEnvelope {
        key: 1,
        message: Command::Increase,
    });
    host.tell(HostEnvelope {
        key: 1,
        message: Command::Increase,
    });
    let count = host
        .ask(TIMEOUT, |reply_to| HostEnvelope {
            key: 1,
            message: Command::Count(reply_to),
        })
        .await
        .expect("the counter answers");
    assert_eq!(count, 2);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);

    tokio::time::sleep(IDLE_TIMEOUT * 4).await;

    let count = host
        .ask(TIMEOUT, |reply_to| HostEnvelope {
            key: 1,
            message: Command::Count(reply_to),
        })
        .await
        .expect("the recovered counter answers");
    assert_eq!(count, 2);
    assert_eq!(spawns.load(Ordering::SeqCst), 2);
}

struct Counter {
    key: u8,
}

impl EventSourced for Counter {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", self.key.to_string()).expect("a number is a valid ID segment")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn handle(
        &self,
        _context: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _state: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let effect = match incoming {
            Incoming::Message(Command::Increase) => Effect::persist(Increased),

            Incoming::Message(Command::Count(reply_to)) => {
                Effect::<Self>::none().then(move |count| reply_to.reply(*count))
            }

            Incoming::Message(Command::Stop) => Effect::stop(),

            Incoming::Terminated(_) => Effect::none(),
        };

        Ok(effect)
    }

    fn apply(&self, count: Self::State, _event: Self::Event) -> Self::State {
        count + 1
    }
}

enum Command {
    Increase,
    Count(ReplyTo<u64>),
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
