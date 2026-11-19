//! What becomes of an append whose statement waits in the server while the caller gives up on
//! it, as tellus's store timeout gives up on a call by dropping it, and what the pool settings of
//! the README change about it. Another transaction's uncommitted row at the append's position
//! holds the statement up; a read does not wait for it. Each test starts its own container, as in
//! `store.rs`.

use composed::{Compose, compose};
use sqlx::{
    PgPool, Postgres as Database, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{num::NonZeroUsize, sync::LazyLock, time::Duration};
use tellus::{AppendError, EncodedEvent, EventStore, PersistenceId, SchemaVersion, SeqNo};
use tellus_persistence_postgres::{Error, PostgresStore};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const HELD_UP: &str = "INSERT INTO tellus_events \
                       (entity_type, entity_id, seq_no, manifest, schema_version, payload) \
                       VALUES ('counter', $1, 0, 'blocker', 1, ''::bytea)";

const PAGE: NonZeroUsize = NonZeroUsize::new(10).unwrap();

/// Stands in for the `Persistence` timeout, above the acquire timeout plus twice the statement
/// timeout.
const BACKSTOP: Duration = Duration::from_secs(1);

const BOUNDS: Bounds = Bounds {
    statement_timeout: Duration::from_millis(300),
    acquire_timeout: Duration::from_millis(300),
};

static COMPOSE: LazyLock<Compose> = LazyLock::new(|| compose!("../compose.yaml"));

/// Without pool settings, a dropped append keeps running in the server: a read right after the
/// drop sees an empty stream, as a restart's replay would, and once nothing holds the statement
/// up any more its event lands, so the next append at the same position conflicts.
#[tokio::test]
async fn without_bounds_a_dropped_append_lands_after_a_later_read() {
    let (url, _container) = database().await;
    let (store, pool) = store(&url, 10, None).await;
    let id = persistence_id("late");

    let blocker = hold_up(&pool, &id).await;
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()])).await;
    assert!(appended.is_err(), "the held up append must be dropped");

    let read = timeout(BACKSTOP, store.read(&id, SeqNo::ZERO, PAGE))
        .await
        .expect("a read does not wait for the held up row")
        .expect("the read succeeds");
    assert!(
        read.is_empty(),
        "the dropped append must not have landed yet"
    );

    blocker.rollback().await.expect("the blocker rolls back");
    assert_eq!(
        landed(&store, &id).await,
        1,
        "the dropped append must land once nothing holds it up"
    );
    let next = store.append(&id, SeqNo::ZERO, vec![event()]).await;
    assert!(
        matches!(next, Err(AppendError::Conflict)),
        "the next append at the same position must conflict, got {next:?}"
    );
}

/// Without pool settings, a dropped append also keeps its connection until its statement ends: in
/// a pool of two, one held by the blocker, the next read gets no connection, and once the
/// statement has ended the pool serves calls again.
#[tokio::test]
async fn without_bounds_a_dropped_append_keeps_its_connection() {
    let (url, _container) = database().await;
    let (store, pool) = store(&url, 2, None).await;
    let id = persistence_id("stuck");

    let blocker = hold_up(&pool, &id).await;
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()])).await;
    assert!(appended.is_err(), "the held up append must be dropped");

    let read = timeout(BACKSTOP, store.read(&id, SeqNo::ZERO, PAGE)).await;
    assert!(
        read.is_err(),
        "the next read must find no connection, got {read:?}"
    );

    blocker.rollback().await.expect("the blocker rolls back");
    let read = timeout(BACKSTOP, store.read(&id, SeqNo::ZERO, PAGE))
        .await
        .expect("the pool must serve calls once the statement has ended")
        .expect("the read succeeds");
    assert_eq!(read.len(), 1, "the dropped append must have landed");
}

/// With a statement timeout on the pool's connections, a held up append fails by it before the
/// backstop: the failure is definite, the connection serves the next read at once, nothing lands
/// later, and the next append at the same position succeeds.
#[tokio::test]
async fn with_bounds_a_held_up_append_fails_definitely() {
    let (url, _container) = database().await;
    let (store, pool) = store(&url, 2, Some(BOUNDS)).await;
    let id = persistence_id("bounded");

    let blocker = hold_up(&pool, &id).await;
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()]))
        .await
        .expect("the statement timeout must end the append before the backstop");
    assert!(
        is_statement_timeout(&appended),
        "the append must fail by its statement timeout, got {appended:?}"
    );

    let read = timeout(BACKSTOP, store.read(&id, SeqNo::ZERO, PAGE))
        .await
        .expect("the append's connection must serve the next read")
        .expect("the read succeeds");
    assert!(read.is_empty());

    blocker.rollback().await.expect("the blocker rolls back");
    assert_nothing_lands_and_the_position_is_free(&store, &id).await;
}

/// The statement timeout does not count the wait for a connection. With both other connections of a
/// pool of three busy, the append starts late. The bounds still end it definitely, since their
/// acquire timeout plus twice the statement timeout stays below the backstop.
#[tokio::test]
async fn with_bounds_an_append_which_starts_late_fails_definitely() {
    let (url, _container) = database().await;
    let (store, pool) = store(&url, 3, Some(BOUNDS)).await;
    let monitor = monitor(&url).await;
    let id = persistence_id("late-start");

    let blocker = hold_up(&pool, &id).await;
    let sleepers = keep_busy(&pool, &monitor, 2, Duration::from_millis(150)).await;
    let started = Instant::now();
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()]))
        .await
        .expect("the pool's bounds must end the append before the backstop");
    assert!(
        is_statement_timeout(&appended),
        "the append must fail by its statement timeout, got {appended:?}"
    );
    assert!(
        started.elapsed() > BOUNDS.statement_timeout,
        "the append must have waited for a connection before its statement started"
    );
    assert_eq!(
        running_appends(&monitor).await,
        0,
        "no statement of the append may outlive the call"
    );

    blocker.rollback().await.expect("the blocker rolls back");
    finish(sleepers).await;
    assert_nothing_lands_and_the_position_is_free(&store, &id).await;
}

/// The statement timeout can count twice. An append which is the first use of its statement on
/// the pool's only connection waits about two thirds of the acquire timeout for that connection,
/// then about two thirds of the statement timeout in Parse behind a SHARE lock. sqlx ends that
/// round trip with a Sync, which cancels the statement timeout, and the Bind which follows starts
/// it again. Behind a second SHARE lock, the append then waits until its statement timeout ends
/// it: after more than the acquire timeout plus one statement timeout, but before the backstop.
#[tokio::test]
async fn with_bounds_an_append_which_prepares_its_statement_counts_the_statement_timeout_twice() {
    const LOCK: &str = "LOCK TABLE tellus_events IN SHARE MODE";

    let (url, _container) = database().await;
    let (store, pool) = store(&url, 1, Some(BOUNDS)).await;
    let monitor = monitor(&url).await;
    let lockers = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("the lockers connect");
    let id = persistence_id("first-use");

    let mut first = lockers.begin().await.expect("the transaction begins");
    sqlx::query(LOCK)
        .execute(&mut *first)
        .await
        .expect("the first lock is taken");
    let mut second = lockers.begin().await.expect("the transaction begins");
    let sleepers = keep_busy(&pool, &monitor, 1, BOUNDS.acquire_timeout * 2 / 3).await;
    let started = Instant::now();
    let appending = tokio::spawn({
        let store = store.clone();
        let id = id.clone();
        async move { timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()])).await }
    });
    finish(sleepers).await;
    while running_appends(&monitor).await < 1 {
        sleep(Duration::from_millis(5)).await;
    }
    let second = tokio::spawn(async move {
        sqlx::query(LOCK)
            .execute(&mut *second)
            .await
            .expect("the second lock is taken");
        second
    });
    sleep(BOUNDS.statement_timeout * 2 / 3).await;
    first.commit().await.expect("the first lock is released");

    let appended = appending
        .await
        .expect("the append task ends")
        .expect("the statement timeout must end the append before the backstop");
    let elapsed = started.elapsed();
    assert!(
        is_statement_timeout(&appended),
        "the append must fail by its statement timeout, got {appended:?}"
    );
    assert!(
        elapsed > BOUNDS.acquire_timeout + BOUNDS.statement_timeout,
        "the statement timeout must have counted twice, the append took {elapsed:?}"
    );

    second
        .await
        .expect("the second locker ends")
        .commit()
        .await
        .expect("the second lock is released");
    assert_nothing_lands_and_the_position_is_free(&store, &id).await;
}

/// With every connection of the pool held for longer than the acquire timeout, the append fails
/// waiting for a connection, before it has sent anything.
#[tokio::test]
async fn with_bounds_an_append_which_gets_no_connection_fails_definitely() {
    let (url, _container) = database().await;
    let (store, pool) = store(&url, 2, Some(BOUNDS)).await;
    let id = persistence_id("no-connection");

    let held = [
        pool.acquire().await.expect("a connection is acquired"),
        pool.acquire().await.expect("a connection is acquired"),
    ];
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()]))
        .await
        .expect("the acquire timeout must end the append before the backstop");
    assert!(
        matches!(
            appended,
            Err(AppendError::Store(Error::Sqlx(sqlx::Error::PoolTimedOut)))
        ),
        "the append must fail waiting for a connection, got {appended:?}"
    );

    drop(held);
    assert_nothing_lands_and_the_position_is_free(&store, &id).await;
}

/// The statement timeout does not cover the commit which ends an append's statement. A commit
/// waiting for a synchronous standby which never answers outlives the statement timeout, the
/// backstop gives up on it, and once the wait ends the event has landed, so the next append at
/// the same position conflicts. The pool serves calls again afterwards.
#[tokio::test]
async fn with_bounds_a_stalled_commit_stays_ambiguous() {
    let (url, _container) = database().await;
    let (store, _) = store(&url, 4, Some(BOUNDS)).await;
    let monitor = monitor(&url).await;
    let id = persistence_id("stalled-commit");

    stall_commits(&monitor, true).await;
    let appended = timeout(BACKSTOP, store.append(&id, SeqNo::ZERO, vec![event()])).await;
    assert!(
        appended.is_err(),
        "the stalled commit must outlive the statement timeout, got {appended:?}"
    );

    let waiting = sqlx::query_scalar::<_, i32>(
        "SELECT pid FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
    )
    .fetch_all(&monitor)
    .await
    .expect("the activity is read");
    assert_eq!(
        waiting.len(),
        1,
        "the dropped append must wait for the standby"
    );
    stall_commits(&monitor, false).await;
    for pid in waiting {
        sqlx::query("SELECT pg_cancel_backend($1)")
            .bind(pid)
            .execute(&monitor)
            .await
            .expect("the backend is canceled");
    }

    assert_eq!(
        landed(&store, &id).await,
        1,
        "the given up append must have landed"
    );
    let next = store.append(&id, SeqNo::ZERO, vec![event()]).await;
    assert!(
        matches!(next, Err(AppendError::Conflict)),
        "the next append at the same position must conflict, got {next:?}"
    );
    store
        .append(&id, SeqNo::new(1), vec![event()])
        .await
        .expect("the pool serves calls again");
}

#[derive(Clone, Copy)]
struct Bounds {
    statement_timeout: Duration,
    acquire_timeout: Duration,
}

async fn store(url: &str, max_connections: u32, bounds: Option<Bounds>) -> (PostgresStore, PgPool) {
    let mut options = url.parse::<PgConnectOptions>().expect("the URL is valid");
    let mut pool_options = PgPoolOptions::new().max_connections(max_connections);
    if let Some(bounds) = bounds {
        options = options.options([(
            "statement_timeout",
            format!("{}ms", bounds.statement_timeout.as_millis()),
        )]);
        pool_options = pool_options.acquire_timeout(bounds.acquire_timeout);
    }
    let pool = pool_options
        .connect_with(options)
        .await
        .expect("the pool connects");

    let store = PostgresStore::new(pool.clone());
    store.migrate().await.expect("the migrations run");
    (store, pool)
}

/// A connection of its own, outside the store's pool, for watching the server while that pool is
/// busy.
async fn monitor(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("the monitor connects")
}

async fn hold_up(pool: &PgPool, id: &PersistenceId) -> Transaction<'static, Database> {
    let mut blocker = pool.begin().await.expect("the transaction begins");
    sqlx::query(HELD_UP)
        .bind(id.entity_id())
        .execute(&mut *blocker)
        .await
        .expect("the blocker inserts");
    blocker
}

/// Keeps `connections` connections of the pool busy for about `busy`, from when this returns.
async fn keep_busy(
    pool: &PgPool,
    monitor: &PgPool,
    connections: usize,
    busy: Duration,
) -> Vec<JoinHandle<()>> {
    let sleepers = (0..connections)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                sqlx::query("SELECT pg_sleep($1)")
                    .bind(busy.as_secs_f64())
                    .execute(&pool)
                    .await
                    .expect("the sleep runs");
            })
        })
        .collect::<Vec<_>>();
    while busy_connections(monitor).await < connections {
        sleep(Duration::from_millis(5)).await;
    }
    sleepers
}

async fn busy_connections(monitor: &PgPool) -> usize {
    let busy = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'",
    )
    .fetch_one(monitor)
    .await
    .expect("the activity is read");
    usize::try_from(busy).expect("a count is not negative")
}

async fn finish(sleepers: Vec<JoinHandle<()>>) {
    for sleeper in sleepers {
        sleeper.await.expect("the sleeper finishes");
    }
}

async fn running_appends(monitor: &PgPool) -> usize {
    let running = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_stat_activity \
         WHERE query LIKE 'INSERT INTO tellus_events%' AND state = 'active'",
    )
    .fetch_one(monitor)
    .await
    .expect("the activity is read");
    usize::try_from(running).expect("a count is not negative")
}

/// Makes every commit wait for a synchronous standby which does not exist, or stop waiting.
async fn stall_commits(monitor: &PgPool, stall: bool) {
    let (statement, shown) = if stall {
        (
            "ALTER SYSTEM SET synchronous_standby_names = 'nobody'",
            "nobody",
        )
    } else {
        ("ALTER SYSTEM RESET synchronous_standby_names", "")
    };
    sqlx::query(statement)
        .execute(monitor)
        .await
        .expect("the setting is altered");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(monitor)
        .await
        .expect("the configuration is reloaded");
    while sqlx::query_scalar::<_, String>("SHOW synchronous_standby_names")
        .fetch_one(monitor)
        .await
        .expect("the setting is shown")
        != shown
    {
        sleep(Duration::from_millis(10)).await;
    }
}

/// The number of events the stream holds once the held up statement had time to end.
async fn landed(store: &PostgresStore, id: &PersistenceId) -> usize {
    let deadline = Instant::now() + BACKSTOP;
    loop {
        let stored = store
            .read(id, SeqNo::ZERO, PAGE)
            .await
            .expect("the read succeeds")
            .len();
        if stored > 0 || Instant::now() >= deadline {
            return stored;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn assert_nothing_lands_and_the_position_is_free(store: &PostgresStore, id: &PersistenceId) {
    assert_eq!(
        landed(store, id).await,
        0,
        "the failed append must not land"
    );
    store
        .append(id, SeqNo::ZERO, vec![event()])
        .await
        .expect("the next append at the same position succeeds");
}

fn is_statement_timeout(appended: &Result<(), AppendError<Error>>) -> bool {
    matches!(
        appended,
        Err(AppendError::Store(Error::Sqlx(sqlx::Error::Database(error))))
            if error.code().as_deref() == Some("57014")
    )
}

fn persistence_id(entity_id: &str) -> PersistenceId {
    PersistenceId::new("counter", entity_id).expect("the segments are valid")
}

fn event() -> EncodedEvent {
    EncodedEvent {
        manifest: "increased".into(),
        schema_version: SchemaVersion::new(1),
        payload: vec![1],
    }
}

async fn database() -> (String, ContainerAsync<Postgres>) {
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

    (
        format!("postgres://{user}:{password}@{host}:{port}/{dbname}"),
        container,
    )
}
