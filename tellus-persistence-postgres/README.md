# tellus-persistence-postgres

PostgreSQL-backed stores for [tellus](../tellus) persistence: `PostgresStore` implements both
`EventStore` and `SnapshotStore` over one shared connection pool, so a single value, cheap to
clone, wires a whole `Persistence`.

For the contract these stores implement, from replay equals live execution to fencing and schema
evolution, see [docs/persistence.md](../docs/persistence.md).

tellus-persistence-postgres is available on
[crates.io](https://crates.io/crates/tellus-persistence-postgres):

```sh
cargo add tellus --features persistence
cargo add tellus-persistence-postgres
```

## Usage

`PostgresStore::new` takes a `PgPool` of the sqlx version this crate depends on. The crate
re-exports sqlx, so an application builds the pool through it and needs no sqlx dependency of its
own. One store serves as both event store and snapshot store:

```rust
use tellus::Persistence;
use tellus_persistence_postgres::{PostgresStore, sqlx::PgPool};

let pool = PgPool::connect("postgres://tellus:tellus@localhost:5432/tellus").await?;
let store = PostgresStore::new(pool);
store.migrate().await?;

let persistence = Persistence::new(store.clone()).with_snapshot_store(store);
```

The re-export also gives access to `sqlx::postgres::PgPoolOptions`, which configures the pool,
for example its size.

`migrate` applies the embedded schema, idempotently. Calling it is optional: `PostgresStore::new`
does not touch the schema. An application which manages its schema with a tool of its own can
create the tables itself instead, from the definition under [Schema](#schema). It then does not call
`migrate`. `migrate` never adopts a table: it fails if `tellus_events` or `tellus_snapshots` exists
and it has not created it.

`migrate` records what it has applied in a table of its own, `_tellus_persistence_migrations`, so
the store's schema and an application's own sqlx migrations can share a database. Release 0.2.14 and
earlier recorded it in sqlx's shared table `_sqlx_migrations`. In a database which such a release
has migrated, `migrate` leaves the old row alone. An application which adds its own sqlx migrator to
such a database removes that row first. Otherwise its migrator fails with `VersionMissing(1)`,
unless it sets `ignore_missing`. If one of its own migrations has version 1, it fails with
`VersionMismatch(1)` either way. This statement removes the row:

```sql
DELETE FROM _sqlx_migrations
WHERE version = 1
  AND checksum = '\xf10c204f698541a4885fb4a78dbbc33ff5dc728a9f7d324c2f2ee2d0d63f6863fa349fd623b0dda74481e618e15a466a';
```

The checksum is that of the first migration as those releases embedded it, so the statement removes
no other row. Remove the row only once no process of release 0.2.14 or earlier calls `migrate` in
that database. Such a release records the row again. Once the application's migrator has recorded a
migration, `migrate` of such a release fails instead.

Such a release also kept its events and snapshots in tables of other names. The
[release note](release-notes/311-table-rename.md) on the rename says how to move them.

## Timeouts

tellus bounds every store call by the timeout of its `Persistence`, 10 s by default, but only as a
backstop: it gives up on a call by dropping it. A dropped call does not end a statement which has
reached the server. sqlx sends no cancel request, so the statement runs on, and its connection
returns to the pool only once the statement has ended. A dropped append can hence still commit, even
after the replay of the actor's next incarnation has read the stream. Its events are then in the
stream but not in the state, and the next append at the same position fails with
`AppendError::Conflict`.

The pool therefore bounds the calls itself, with a `statement_timeout` on its connections and an
acquire timeout. The server cancels a statement which exceeds `statement_timeout`, and the call
then fails definitely: nothing it sent commits later. The statement timeout counts from the moment
the statement reaches the server, not the wait for a pool connection, which the acquire timeout
bounds. A call can hence take as long as both together, and their sum stays below the
`Persistence` timeout, with room for the network round trips. sqlx's default acquire timeout, 30 s,
is above the default `Persistence` timeout:

```rust
use std::time::Duration;
use tellus::Persistence;
use tellus_persistence_postgres::{
    PostgresStore,
    sqlx::postgres::{PgConnectOptions, PgPoolOptions},
};

let options = "postgres://tellus:tellus@localhost:5432/tellus"
    .parse::<PgConnectOptions>()?
    .options([("statement_timeout", "4s")]);
let pool = PgPoolOptions::new()
    .acquire_timeout(Duration::from_secs(4))
    .connect_with(options)
    .await?;
let store = PostgresStore::new(pool);
store.migrate().await?;

let persistence = Persistence::new(store.clone())
    .with_snapshot_store(store)
    .with_timeout(Duration::from_secs(10))?;
```

`migrate` runs under the same statement timeout. While another process migrates the same database,
`migrate` hence waits for it at most the statement timeout and then fails.

One case stays ambiguous: a call which fails after its statement has begun to commit and before the
answer has arrived. Neither `statement_timeout` nor `transaction_timeout` covers the commit which
ends a statement, since PostgreSQL disarms both before it commits. A commit which stalls in the
server, e.g. waiting for a synchronous standby, outlives them, and only the backstop ends the call.
A lost connection or a crash of the process at that point leaves the outcome open in the same way.
Such an append can land after the replay, as above.
[docs/persistence.md](../docs/persistence.md#store-timeout) describes what follows for the actor.

## Schema

Events live in the `tellus_events` table with the primary key `(entity_type, entity_id, seq_no)`,
snapshots in `tellus_snapshots` keyed by `(entity_type, entity_id)`, only the latest one retained.
`PostgresStore::migrate` applies
[`migrations/0001_events_and_snapshots.sql`](migrations/0001_events_and_snapshots.sql), which is
embedded in the crate. Its two statements are the supported definition of the schema, also for an
application which creates the tables itself:

```sql
CREATE TABLE tellus_events (
    entity_type    TEXT   NOT NULL,
    entity_id      TEXT   NOT NULL,
    seq_no         BIGINT NOT NULL,
    manifest       TEXT   NOT NULL,
    schema_version INT    NOT NULL,
    payload        BYTEA  NOT NULL,
    PRIMARY KEY (entity_type, entity_id, seq_no)
);

CREATE TABLE tellus_snapshots (
    entity_type    TEXT   NOT NULL,
    entity_id      TEXT   NOT NULL,
    next_seq_no    BIGINT NOT NULL,
    manifest       TEXT   NOT NULL,
    schema_version INT    NOT NULL,
    payload        BYTEA  NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
);
```

The store uses these two tables only. A database whose tables an application created itself hence
needs no `_tellus_persistence_migrations`. Every change to the schema is listed in the
[changelog](CHANGELOG.md).

Appends are a single atomic, conditional statement: an expected next sequence number other than the
actual one fails with `AppendError::Conflict` and leaves the stream untouched, which
is tellus's fencing guarantee. An empty append inserts nothing but is still fenced.

## Examples

All three need the `postgres` service of the repository's
[compose.yaml](../compose.yaml); start it with `docker compose up -d --wait postgres`.
The database is addressed via `DATABASE_URL`, defaulting to that service. `docker compose down`
stops it, `down -v` resets the data.

- [`event_sourced_counter`](examples/event_sourced_counter.rs): a counter surviving process
  restarts; every run recovers the count by replay, increments it once and prints the new count,
  so repeated runs count on:

  ```shell
  cargo run --quiet -p tellus-persistence-postgres --example event_sourced_counter
  ```

- [`event_sourced_supervision`](examples/event_sourced_supervision.rs): a flaky loader under the
  `Restart` supervision strategy, event-sourced, so the restart after a toxic value recovers the
  count by replay instead of starting over at 0:

  ```shell
  RUST_LOG=tellus=debug cargo run --quiet -p tellus-persistence-postgres \
    --example event_sourced_supervision
  ```

- [`event_sourced_04_postgres`](examples/tutorials/event_sourced/04_postgres.rs): step 4 of the
  event-sourced actors tutorial, the device of step 3 with events and snapshots in PostgreSQL;
  every run records one reading and recovers the earlier ones:

  ```shell
  cargo run --quiet -p tellus-persistence-postgres --example event_sourced_04_postgres
  ```

## Tests

[`tests/store.rs`](tests/store.rs) runs the contract test suite which ships with `tellus` behind
its `persistence-tests` feature against a real server, started via testcontainers, hence Docker is
required. [`tests/timeouts.rs`](tests/timeouts.rs) gives up on calls whose statements wait in the
server, with and without the pool settings of [Timeouts](#timeouts), and uses the pool again
afterwards. Image tag and credentials are read out of the repository's
[compose.yaml](../compose.yaml) at compile time via
[`composed`](https://crates.io/crates/composed), so there is one place to change either.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
