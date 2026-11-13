//! Contract and end-to-end tests against a real PostgreSQL server via testcontainers. Each test
//! which needs a server starts its own container: a shared one would die with the first finishing
//! test's runtime, since every `#[tokio::test]` runs its own and the pool's background tasks live
//! on it.

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
    ActorContext, ActorSystem, AppendError, Effect, EncodedEvent, EncodedSnapshot, EventSourced,
    EventStore, Incoming, Nothing, Persistence, PersistenceId, ReplyTo, SchemaVersion, SeqNo,
    SnapshotStore, StoredEvent, StoredSnapshot, Unstashed, Untimed, Versioned,
};
use tellus_persistence_postgres::{Error, PostgresStore};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);
const APPLICATION_VERSION: i64 = 20_260_101_000_000;

/// The checksum of the first migration as release 0.2.14 and earlier embedded and recorded it.
const EARLIER_CHECKSUM: &str = concat!(
    "f10c204f698541a4885fb4a78dbbc33ff5dc728a9f7d324c",
    "2f2ee2d0d63f6863fa349fd623b0dda74481e618e15a466a",
);

/// The checksum of the embedded first migration.
const CHECKSUM: &str = concat!(
    "4c993d1dc5f8bb453f303ef640f7f8232113bf21a2dc48c5",
    "9da9a2cb376fc8c743e02761a44e1252fa7028fdeb467ca5",
);

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
/// `migrate` succeeds, records in the store's own table, creates the store's tables and leaves the
/// old tables and the old row alone. That row still stops an application's own migrations, unless
/// they ignore missing versions. The statement of the README removes that row and no other, and
/// then they run.
#[tokio::test]
async fn a_database_of_an_earlier_release_keeps_its_old_tables_and_its_old_row() {
    let (pool, _container) = database_of_an_earlier_release().await;

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
    assert_eq!(
        table_names(&pool).await,
        [
            "_sqlx_migrations",
            "_tellus_persistence_migrations",
            "events",
            "snapshots",
            "tellus_events",
            "tellus_snapshots"
        ]
    );
    let earlier_counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM snapshots)",
    )
    .fetch_one(&pool)
    .await
    .expect("the events and snapshots of the earlier release are counted");
    assert_eq!(earlier_counts, (1, 1));

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
    assert!(
        statement.contains(EARLIER_CHECKSUM),
        "the statement of the README does not name the checksum which earlier releases recorded"
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

/// The statements of the release note move the events and snapshots of a database which an
/// earlier release has migrated into the store's tables, once `migrate` has created them. Until
/// then the store finds neither.
#[tokio::test]
async fn the_statements_of_the_release_note_move_what_an_earlier_release_stored() {
    let (pool, _container) = database_of_an_earlier_release().await;
    let id = PersistenceId::new("counter", "kept").expect("the segments are valid");
    let limit = NonZeroUsize::new(10).expect("10 is not zero");
    let store = PostgresStore::new(pool.clone());
    store
        .migrate()
        .await
        .expect("the store's migrations run on the migrated database");
    let read = store.read(&id, SeqNo::ZERO, limit).await;
    assert_eq!(read.expect("the events are read"), []);
    let loaded = store.load(&id).await;
    assert_eq!(loaded.expect("the snapshot is loaded"), None);

    sqlx::raw_sql(release_note_statements())
        .execute(&pool)
        .await
        .expect("the statements of the release note run");

    let read = store.read(&id, SeqNo::ZERO, limit).await;
    assert_eq!(
        read.expect("the events are read"),
        [StoredEvent {
            seq_no: SeqNo::ZERO,
            event: earlier_event(),
        }]
    );
    let loaded = store.load(&id).await;
    assert_eq!(
        loaded.expect("the snapshot is loaded"),
        Some(StoredSnapshot {
            next_seq_no: SeqNo::new(1),
            snapshot: EncodedSnapshot {
                manifest: "count".into(),
                schema_version: SchemaVersion::new(1),
                payload: vec![2],
            },
        })
    );
    assert_eq!(
        table_names(&pool).await,
        [
            "_sqlx_migrations",
            "_tellus_persistence_migrations",
            "tellus_events",
            "tellus_snapshots"
        ]
    );
}

/// Once the store has appended for an entity of the earlier release, the statements of the release
/// note fail on that entity and leave the old tables as they are.
#[tokio::test]
async fn the_statements_of_the_release_note_change_nothing_after_an_append() {
    let (pool, _container) = database_of_an_earlier_release().await;
    let id = PersistenceId::new("counter", "kept").expect("the segments are valid");
    let store = PostgresStore::new(pool.clone());
    store
        .migrate()
        .await
        .expect("the store's migrations run on the migrated database");
    store
        .append(&id, SeqNo::ZERO, vec![earlier_event()])
        .await
        .expect("the store appends to its empty table");

    // A batch which fails leaves its transaction open on its connection!
    let mut connection = pool.acquire().await.expect("a connection is acquired");
    let moved = sqlx::raw_sql(release_note_statements())
        .execute(&mut *connection)
        .await;
    connection
        .close()
        .await
        .expect("the connection with the open transaction closes");

    assert!(
        moved.is_err(),
        "the statements of the release note ran over an entity which the store had appended to"
    );
    let earlier_counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM snapshots)",
    )
    .fetch_one(&pool)
    .await
    .expect("the events and snapshots of the earlier release are counted");
    assert_eq!(earlier_counts, (1, 1));
}

/// A migration which a release has embedded never changes: a database which has recorded it
/// fails `migrate` on any other checksum. The first migration of release 0.2.14 and earlier is
/// kept under `tests/release-0.2.14`, for the tests which start from such a database.
#[test]
fn the_first_migration_keeps_its_checksums() {
    let checksum = |migrator: Migrator| {
        migrator
            .iter()
            .flat_map(|migration| migration.checksum.iter())
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };

    assert_eq!(checksum(sqlx::migrate!()), CHECKSUM);
    assert_eq!(checksum(earlier_release_migrator()), EARLIER_CHECKSUM);
}

/// An application's own tables called `events` and `snapshots` stay as they are: `migrate`
/// creates the store's tables next to them, and the store passes its contract.
#[tokio::test]
async fn an_applications_own_events_and_snapshots_tables_are_left_alone() {
    let (pool, _container) = database().await;
    sqlx::raw_sql(
        "CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
         CREATE TABLE snapshots (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
         INSERT INTO events VALUES (1, 'signed up'); \
         INSERT INTO snapshots VALUES (1, 'nightly')",
    )
    .execute(&pool)
    .await
    .expect("the application's tables are created");

    let store = PostgresStore::new(pool.clone());
    store
        .migrate()
        .await
        .expect("the store's migrations run next to the application's tables");
    tellus::persistence_tests::event_store_contract(store.clone()).await;
    tellus::persistence_tests::snapshot_store_contract(store).await;

    let names = sqlx::query_scalar::<_, String>(
        "SELECT name FROM events UNION ALL SELECT name FROM snapshots ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .expect("the application's tables are read");
    assert_eq!(names, ["nightly", "signed up"]);
}

/// `migrate` never adopts a table. Where a table has one of the store's names and `migrate` has
/// not created it, whether from the definition of the README or of any other shape, it fails
/// and creates neither of its two tables.
#[tokio::test]
async fn migrate_fails_where_a_table_has_one_of_the_stores_names() {
    let other_shape = "CREATE TABLE tellus_snapshots (id BIGINT PRIMARY KEY)";
    for (definition, tables_afterward) in [
        (
            readme_schema(),
            vec![
                "_tellus_persistence_migrations",
                "tellus_events",
                "tellus_snapshots",
            ],
        ),
        (
            other_shape,
            vec!["_tellus_persistence_migrations", "tellus_snapshots"],
        ),
    ] {
        let (pool, _container) = database().await;
        sqlx::raw_sql(definition)
            .execute(&pool)
            .await
            .expect("the table is created");

        let migrated = PostgresStore::new(pool.clone()).migrate().await;

        assert!(
            matches!(
                migrated,
                Err(Error::Migrate(MigrateError::ExecuteMigration(_, 1)))
            ),
            "migrate did not fail on a table which exists: {migrated:?}"
        );
        assert_eq!(table_names(&pool).await, tables_afterward);
    }
}

/// The definition of the schema in the README is the one embedded migration, byte for byte.
#[test]
fn the_schema_of_the_readme_is_the_embedded_migration() {
    let migrator = sqlx::migrate!();
    let embedded_migrations = migrator
        .iter()
        .map(|migration| migration.sql.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        embedded_migrations,
        [readme_schema()],
        "the README's definition of the schema is not the embedded migration"
    );
}

/// A database whose tables were created from the definition in the README, without `migrate`:
/// the store passes its contract on it and creates no table of its own.
#[tokio::test]
async fn the_store_passes_its_contract_on_the_schema_of_the_readme() {
    let (pool, _container) = database().await;
    sqlx::raw_sql(readme_schema())
        .execute(&pool)
        .await
        .expect("the definition of the README runs");

    let store = PostgresStore::new(pool.clone());
    tellus::persistence_tests::event_store_contract(store.clone()).await;
    tellus::persistence_tests::snapshot_store_contract(store.clone()).await;
    tellus::persistence_tests::snapshot_with_event_tail(store.clone(), store).await;

    assert_eq!(
        table_names(&pool).await,
        ["tellus_events", "tellus_snapshots"]
    );
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

/// A database as release 0.2.14 left it: the first migration of that release recorded in sqlx's
/// shared table, and one event and one snapshot in its tables.
async fn database_of_an_earlier_release() -> (PgPool, ContainerAsync<Postgres>) {
    let (pool, container) = database().await;

    earlier_release_migrator()
        .run(&pool)
        .await
        .expect("the first migration runs as an earlier release ran it");
    sqlx::raw_sql(
        "INSERT INTO events VALUES ('counter', 'kept', 0, 'increased', 1, '\\x01'::bytea); \
         INSERT INTO snapshots VALUES ('counter', 'kept', 1, 'count', 1, '\\x02'::bytea)",
    )
    .execute(&pool)
    .await
    .expect("an event and a snapshot are stored");

    (pool, container)
}

fn earlier_release_migrator() -> Migrator {
    Migrator::with_migrations(vec![Migration::new(
        1,
        "events and snapshots".into(),
        MigrationType::Simple,
        include_str!("release-0.2.14/0001_events_and_snapshots.sql").into_sql_str(),
        false,
    )])
}

/// The event which [database_of_an_earlier_release] stores, as the store reads it.
fn earlier_event() -> EncodedEvent {
    EncodedEvent {
        manifest: "increased".into(),
        schema_version: SchemaVersion::new(1),
        payload: vec![1],
    }
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

fn release_note_statements() -> &'static str {
    let (_, statements) = include_str!("../release-notes/311-table-rename.md")
        .split_once("```sql\n")
        .expect("the release note opens the statements");
    let (statements, _) = statements
        .split_once("```")
        .expect("the release note closes the statements");

    statements
}

fn readme_schema() -> &'static str {
    let (_, schema) = include_str!("../README.md")
        .split_once("## Schema")
        .expect("the README has a section on the schema");
    let (_, schema) = schema
        .split_once("```sql\n")
        .expect("the README opens the definition of the schema");
    let (schema, _) = schema
        .split_once("```")
        .expect("the README closes the definition of the schema");

    schema
}

async fn table_names(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables WHERE table_schema = 'public' \
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .expect("the tables are listed")
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
