//! Contract and end-to-end tests against a real PostgreSQL server via testcontainers. Each test
//! starts its own container: a shared one would die with the first finishing test's runtime,
//! since every `#[tokio::test]` runs its own and the pool's background tasks live on it.

use composed::{Compose, compose};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgPool, SqlSafeStr,
    migrate::{MigrateError, Migration, MigrationType, Migrator},
};
use std::{
    convert::Infallible,
    num::NonZeroUsize,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tellus::{
    ActorContext, ActorSystem, AppendError, Effect, EncodedEvent, EventSourced, EventStore,
    Incoming, Nothing, Persistence, PersistenceId, ReplyTo, SchemaVersion, SeqNo, Unstashed,
    Untimed, Versioned,
};
use tellus_persistence_postgres::PostgresStore;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);
const APPLICATION_VERSION: i64 = 20_260_101_000_000;

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

/// A position which no stream can reach is a mismatch like any other: a conflict for an append,
/// with or without events, and an empty page for a read.
#[tokio::test]
async fn a_position_beyond_bigint_is_a_conflict_and_an_empty_page() {
    let (store, _container) = store().await;
    let id = PersistenceId::new("edges", unique_entity_id()).expect("the segments are valid");
    let event = EncodedEvent {
        manifest: "edge".into(),
        schema_version: SchemaVersion::new(1),
        payload: vec![1],
    };

    for beyond in [i64::MAX as u64 + 1, u64::MAX].map(SeqNo::new) {
        for events in [Vec::new(), vec![event.clone()]] {
            let len = events.len();
            let appended = store.append(&id, beyond, events).await;
            assert!(
                matches!(appended, Err(AppendError::Conflict)),
                "an append of {len} events at {beyond} must fail with Conflict, got {appended:?}"
            );
        }
        let read = store.read(&id, beyond, NonZeroUsize::MIN).await;
        assert!(
            matches!(read.as_deref(), Ok([])),
            "a read from {beyond} must be an empty page, got {read:?}"
        );
    }
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

/// The store's migrations and an application's own sqlx migrations share a database, whichever
/// runs first.
#[tokio::test]
async fn migrations_coexist_with_an_applications_own() {
    let migrator = application_migrator();

    let (pool, _container) = database().await;
    PostgresStore::new(pool.clone())
        .migrate()
        .await
        .expect("the store's migrations run");
    migrator
        .run(&pool)
        .await
        .expect("the application's migrations run after the store's");

    let (pool, _container) = database().await;
    migrator
        .run(&pool)
        .await
        .expect("the application's migrations run");
    PostgresStore::new(pool)
        .migrate()
        .await
        .expect("the store's migrations run after the application's");
}

/// A database which an earlier release has migrated, recording that in sqlx's shared table:
/// `migrate` succeeds, records in the store's own table, keeps the events and leaves the old row
/// alone. That row still stops an application's own migrations, unless they ignore missing
/// versions. The statement of the README removes that row and no other, and then they run.
#[tokio::test]
async fn a_database_of_an_earlier_release_keeps_its_events_and_its_old_row() {
    let (pool, _container) = database().await;
    sqlx::migrate!()
        .run_to(1, &pool)
        .await
        .expect("the first migration runs as an earlier release ran it");
    sqlx::query("INSERT INTO events VALUES ('counter', 'kept', 0, 'increased', 1, ''::bytea)")
        .execute(&pool)
        .await
        .expect("an event is stored");

    let store = PostgresStore::new(pool.clone());
    store
        .migrate()
        .await
        .expect("the store's migrations run on the migrated database");
    let recorded_versions =
        sqlx::query_scalar::<_, i64>("SELECT version FROM _tellus_persistence_migrations")
            .fetch_all(&pool)
            .await
            .expect("the store's own table is read");
    assert_eq!(recorded_versions, [1]);
    let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .expect("the events are counted");
    assert_eq!(event_count, 1);

    let migrator = application_migrator();
    // sqlx keeps its advisory lock on a connection whose migrator has failed!
    let mut connection = pool.acquire().await.expect("a connection is acquired");
    let migrated = migrator.run(&mut *connection).await;
    connection
        .close()
        .await
        .expect("the connection which holds the lock closes");
    assert!(
        matches!(migrated, Err(MigrateError::VersionMissing(1))),
        "the old row did not stop the application's migrations: {migrated:?}"
    );
    let mut tolerant_migrator = application_migrator();
    tolerant_migrator.set_ignore_missing(true);
    tolerant_migrator
        .run(&pool)
        .await
        .expect("migrations which ignore missing versions run beside the old row");

    let (_, statement) = include_str!("../README.md")
        .split_once("```sql\n")
        .expect("the README opens a statement");
    let (statement, _) = statement
        .split_once("```")
        .expect("the README closes the statement");
    let checksum = sqlx::migrate!()
        .iter()
        .next()
        .expect("the first migration is embedded")
        .checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert!(
        statement.contains(&checksum),
        "the statement of the README does not name the checksum of the first migration, which \
         must never change"
    );
    let removed = sqlx::query(statement)
        .execute(&pool)
        .await
        .expect("the statement of the README runs");
    assert_eq!(removed.rows_affected(), 1);
    let remaining_versions = sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations")
        .fetch_all(&pool)
        .await
        .expect("the shared table is read");
    assert_eq!(
        remaining_versions,
        [APPLICATION_VERSION],
        "the statement of the README removed another row"
    );
    migrator
        .run(&pool)
        .await
        .expect("the application's migrations run once the old row is gone");
    store
        .migrate()
        .await
        .expect("the store's migrations still run");
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

fn application_migrator() -> Migrator {
    Migrator::with_migrations(vec![Migration::new(
        APPLICATION_VERSION,
        "read model".into(),
        MigrationType::Simple,
        "CREATE TABLE read_model (id BIGINT PRIMARY KEY)".into_sql_str(),
        false,
    )])
}

async fn store() -> (PostgresStore, ContainerAsync<Postgres>) {
    let (pool, container) = database().await;

    let store = PostgresStore::new(pool);
    store.migrate().await.expect("the migrations run");

    (store, container)
}

async fn database() -> (PgPool, ContainerAsync<Postgres>) {
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

    (pool, container)
}

fn unique_entity_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the current time is after the Unix epoch")
        .as_nanos();

    format!("counter-{nanos}")
}
