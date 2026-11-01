//! Contract and end-to-end tests against a real PostgreSQL server via testcontainers. Each test
//! starts its own container: a shared one would die with the first finishing test's runtime,
//! since every `#[tokio::test]` runs its own and the pool's background tasks live on it.

use composed::{Compose, compose};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    convert::Infallible,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, Incoming, Nothing, Persistence, PersistenceId,
    ReplyTo, SchemaVersion, Unstashed, Untimed, Versioned,
};
use tellus_persistence_postgres::PostgresStore;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Image tag and credentials come from the repository's compose.yaml, so the tests always
/// run against the version and settings developers run locally, never against the testcontainers
/// defaults.
static COMPOSE: LazyLock<Compose> = LazyLock::new(|| compose!("../compose.yaml"));

#[tokio::test]
async fn event_store_contract() {
    let (store, _container) = store().await;
    tellus::persistence_tests::event_store_contract(store).await;
}

#[tokio::test]
async fn snapshot_store_contract() {
    let (store, _container) = store().await;
    tellus::persistence_tests::snapshot_store_contract(store).await;
}

#[tokio::test]
async fn snapshot_with_event_tail() {
    let (store, _container) = store().await;
    tellus::persistence_tests::snapshot_with_event_tail(store.clone(), store).await;
}

/// Applying the embedded migrations to an already migrated schema succeeds, so an application can
/// run them at every start.
#[tokio::test]
async fn migrations_are_idempotent() {
    let (store, _container) = store().await;
    store
        .migrate()
        .await
        .expect("repeated migration must succeed");
}

/// End to end through tellus: a counter increments against PostgreSQL, terminates, and a second
/// incarnation recovers the count by replay.
#[tokio::test]
async fn a_counter_survives_incarnations() {
    let (store, _container) = store().await;
    let entity_id = unique_entity_id();

    let system = ActorSystem::event_sourced(
        Counter {
            entity_id: entity_id.clone(),
        },
        Persistence::new(store.clone()),
    );
    for expected in 1..=3 {
        let count = system
            .root()
            .ask(TIMEOUT, Command::Increment)
            .await
            .expect("no reply to increment");
        assert_eq!(count, expected);
    }
    system.root().tell(Command::Stop);
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("first incarnation did not terminate")
        .expect("watching the root actor failed");

    let system = ActorSystem::event_sourced(Counter { entity_id }, Persistence::new(store));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Increment)
        .await
        .expect("no reply to increment after recovery");
    assert_eq!(count, 4);

    system.root().tell(Command::Stop);
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("second incarnation did not terminate")
        .expect("watching the root actor failed");
}

struct Counter {
    entity_id: String,
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
        PersistenceId::new("counter", &self.entity_id).expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        match incoming {
            Incoming::Message(Command::Increment(reply_to)) => {
                Ok(Effect::persist(Increased).then(move |count| reply_to.reply(*count)))
            }

            Incoming::Message(Command::Stop) => Ok(Effect::stop()),

            Incoming::Terminated(_) => Ok(Effect::none()),
        }
    }

    fn apply(&self, state: Self::State, Increased: Self::Event) -> Self::State {
        state + 1
    }
}

#[derive(Debug)]
enum Command {
    Increment(ReplyTo<u64>),
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

async fn store() -> (PostgresStore, ContainerAsync<Postgres>) {
    let postgres = COMPOSE.service("postgres");

    let (user, password, dbname) = (
        postgres.env("POSTGRES_USER"),
        postgres.env("POSTGRES_PASSWORD"),
        postgres.env("POSTGRES_DB"),
    );

    let container = Postgres::default()
        .with_db_name(dbname)
        .with_user(user)
        .with_password(password)
        .with_tag(postgres.image().tag_with_digest())
        .start()
        .await
        .expect("the PostgreSQL container starts");

    let host = container.get_host().await.expect("the host is known");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("the mapped port is known");
    let url = format!("postgres://{user}:{password}@{host}:{port}/{dbname}");
    let pool = PgPool::connect(&url).await.expect("the pool connects");

    let store = PostgresStore::new(pool);
    store.migrate().await.expect("the migrations run");

    (store, container)
}

fn unique_entity_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the current time is after the Unix epoch")
        .as_nanos();

    format!("counter-{nanos}")
}
