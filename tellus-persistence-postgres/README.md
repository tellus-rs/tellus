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

`migrate` applies the embedded schema, idempotently; skip it if the schema is managed externally.
The re-export also gives access to `sqlx::postgres::PgPoolOptions`, which configures the pool,
for example its size.

## Schema

Events live in the `events` table with the primary key `(entity_type, entity_id, seq_no)`,
snapshots in `snapshots` keyed by `(entity_type, entity_id)`, only the latest one retained. The
schema is [`migrations/0001_events_and_snapshots.sql`](migrations/0001_events_and_snapshots.sql),
embedded in the crate and applied via `PostgresStore::migrate`.

Appends are a single atomic, conditional statement: a stale expected next sequence number, below
or above the actual one, fails with `AppendError::Conflict` and leaves the stream untouched, which
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
required. Image tag and credentials are read out of the repository's
[compose.yaml](../compose.yaml) at compile time via
[`composed`](https://crates.io/crates/composed), so there is one place to change either.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
