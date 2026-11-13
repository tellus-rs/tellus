//! PostgreSQL-backed stores for tellus persistence: [PostgresStore] implements both [EventStore]
//! and [SnapshotStore] over one shared connection pool, so a single value, cheap to clone, wires
//! a whole [Persistence](tellus::Persistence).
//!
//! Events live in the `events` table with the primary key `(entity_type, entity_id, seq_no)`,
//! snapshots in `snapshots` keyed by `(entity_type, entity_id)`, only the latest one retained. The
//! schema is embedded and applied via [PostgresStore::migrate], which records it in a table of its
//! own, `_tellus_persistence_migrations`. Appends are a single atomic, conditional statement: an
//! expected next sequence number not matching the actual one, below or above, fails with
//! [AppendError::Conflict] and leaves the stream untouched, which is tellus's fencing guarantee. An
//! empty append inserts nothing but is still fenced.
//!
//! [PostgresStore::new] takes a [PgPool] of the [sqlx] version this crate depends on. The crate
//! re-exports sqlx, so an application builds the pool through it and needs no sqlx dependency of
//! its own:
//!
//! ```rust,no_run
//! use tellus::Persistence;
//! use tellus_persistence_postgres::{PostgresStore, sqlx::PgPool};
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let pool = PgPool::connect("postgres://tellus:tellus@localhost:5432/tellus").await?;
//! let store = PostgresStore::new(pool);
//! store.migrate().await?;
//!
//! let persistence = Persistence::new(store.clone()).with_snapshot_store(store);
//! # Ok(())
//! # }
//! ```

#![doc(
    html_logo_url = "https://tellus.rs/brand/logo-tile.svg",
    html_favicon_url = "https://tellus.rs/favicon.svg"
)]
#![warn(missing_docs, clippy::missing_errors_doc)]

pub use sqlx;

use sqlx::{PgPool, Row, postgres::PgRow};
use std::num::NonZeroUsize;
use tellus::{
    AppendError, EncodedEvent, EncodedSnapshot, EventStore, PersistenceId, SchemaVersion, SeqNo,
    SnapshotStore, StoredEvent, StoredSnapshot,
};
use thiserror::Error;

/// A persistence store backed by PostgreSQL, implementing both [EventStore] and [SnapshotStore]
/// over a shared connection pool; cheap to clone.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Create a store over the given connection pool. The schema is not touched; apply it via
    /// [PostgresStore::migrate] or create it from the definition in the README.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Apply the embedded schema migrations, idempotently. They are recorded in the table
    /// `_tellus_persistence_migrations` rather than in sqlx's shared `_sqlx_migrations`, so an
    /// application's own sqlx migrations can run in the same database. Release 0.2.14 and earlier
    /// recorded them in the shared table, in a row which `migrate` leaves alone; see the README
    /// for removing it.
    ///
    /// # Errors
    /// Fails if the database cannot be reached, if a migration cannot be applied, or if the
    /// migrations recorded in its own table do not match the embedded ones (one was changed, is
    /// missing or was left partially applied).
    pub async fn migrate(&self) -> Result<(), Error> {
        let mut migrator = sqlx::migrate!();
        // The default table is shared with every other sqlx migrator of the database!
        migrator.dangerous_set_table_name("_tellus_persistence_migrations");
        migrator.run(&self.pool).await?;
        Ok(())
    }
}

impl EventStore for PostgresStore {
    type Error = Error;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        // No stream of this schema reaches a position beyond BIGINT!
        let Ok(next_seq_no) = into_bigint(next_seq_no) else {
            return Err(AppendError::Conflict);
        };

        // An empty append inserts no rows, so the row count cannot reveal a conflict.
        if events.is_empty() {
            let query = "SELECT COALESCE(MAX(seq_no) + 1, 0) \
                         FROM events \
                         WHERE entity_type = $1 \
                         AND entity_id = $2";
            let actual = sqlx::query_scalar::<_, i64>(query)
                .bind(id.entity_type())
                .bind(id.entity_id())
                .fetch_one(&self.pool)
                .await
                .map_err(Error::Sqlx)?;

            return if actual == next_seq_no {
                Ok(())
            } else {
                Err(AppendError::Conflict)
            };
        }

        let mut seq_nos = Vec::with_capacity(events.len());
        let mut manifests = Vec::with_capacity(events.len());
        let mut schema_versions = Vec::with_capacity(events.len());
        let mut payloads = Vec::with_capacity(events.len());
        for (n, event) in events.into_iter().enumerate() {
            let seq_no = i64::try_from(n)
                .ok()
                .and_then(|n| next_seq_no.checked_add(n))
                .ok_or(Error::SeqNoOutOfRange)?;

            seq_nos.push(seq_no);
            manifests.push(event.manifest);
            schema_versions.push(i32::from(event.schema_version.as_u16()));
            payloads.push(event.payload);
        }

        let query = "INSERT INTO events (entity_type, entity_id, seq_no, manifest, schema_version, payload) \
                     SELECT $1, $2, seq_no, manifest, schema_version, payload \
                     FROM UNNEST($3::BIGINT[], $4::TEXT[], $5::INT[], $6::BYTEA[]) \
                     AS unnested(seq_no, manifest, schema_version, payload) \
                     WHERE (SELECT COALESCE(MAX(seq_no) + 1, 0) \
                            FROM events \
                            WHERE entity_type = $1 \
                            AND entity_id = $2) = $7";
        let inserted = sqlx::query(query)
            .bind(id.entity_type())
            .bind(id.entity_id())
            .bind(&seq_nos)
            .bind(&manifests)
            .bind(&schema_versions)
            .bind(&payloads)
            .bind(next_seq_no)
            .execute(&self.pool)
            .await;

        match inserted {
            Ok(done) if done.rows_affected() == seq_nos.len() as u64 => Ok(()),
            Ok(_) => Err(AppendError::Conflict),
            Err(error) if is_unique_violation(&error) => Err(AppendError::Conflict),
            Err(error) => Err(AppendError::Store(Error::Sqlx(error))),
        }
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        // No stream of this schema reaches a position beyond BIGINT!
        let Ok(from_seq_no) = into_bigint(from_seq_no) else {
            return Ok(Vec::new());
        };
        let limit = i64::try_from(limit.get()).unwrap_or(i64::MAX);

        let query = "SELECT seq_no, manifest, schema_version, payload \
                     FROM events \
                     WHERE entity_type = $1 \
                     AND entity_id = $2 \
                     AND seq_no >= $3 \
                     ORDER BY seq_no \
                     LIMIT $4";
        let rows = sqlx::query(query)
            .bind(id.entity_type())
            .bind(id.entity_id())
            .bind(from_seq_no)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(stored_event).collect()
    }
}

impl SnapshotStore for PostgresStore {
    type Error = Error;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        let next_seq_no = into_bigint(next_seq_no)?;

        let query = "INSERT INTO snapshots (entity_type, entity_id, next_seq_no, manifest, schema_version, payload) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (entity_type, entity_id) DO UPDATE SET \
                     next_seq_no = EXCLUDED.next_seq_no, \
                     manifest = EXCLUDED.manifest, \
                     schema_version = EXCLUDED.schema_version, \
                     payload = EXCLUDED.payload";
        sqlx::query(query)
            .bind(id.entity_type())
            .bind(id.entity_id())
            .bind(next_seq_no)
            .bind(snapshot.manifest)
            .bind(i32::from(snapshot.schema_version.as_u16()))
            .bind(snapshot.payload)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        let query = "SELECT next_seq_no, manifest, schema_version, payload \
                     FROM snapshots \
                     WHERE entity_type = $1 \
                     AND entity_id = $2";
        let row = sqlx::query(query)
            .bind(id.entity_type())
            .bind(id.entity_id())
            .fetch_optional(&self.pool)
            .await?;

        row.map(stored_snapshot).transpose()
    }
}

/// Errors possibly returned by [PostgresStore].
#[derive(Debug, Error)]
pub enum Error {
    /// The underlying database operation failed.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// The schema migration failed.
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// A sequence number outside the BIGINT range of the schema.
    #[error("sequence number outside BIGINT range")]
    SeqNoOutOfRange,

    /// A schema version outside the range of [Versioned::VERSION](tellus::Versioned::VERSION).
    #[error("schema version outside u16 range")]
    SchemaVersionOutOfRange,
}

fn into_bigint(seq_no: SeqNo) -> Result<i64, Error> {
    i64::try_from(seq_no.as_u64()).map_err(|_| Error::SeqNoOutOfRange)
}

fn stored_event(row: PgRow) -> Result<StoredEvent, Error> {
    Ok(StoredEvent {
        seq_no: seq_no(&row, "seq_no")?,
        event: EncodedEvent {
            manifest: row.try_get::<String, _>("manifest")?.into(),
            schema_version: schema_version(&row)?,
            payload: row.try_get("payload")?,
        },
    })
}

fn stored_snapshot(row: PgRow) -> Result<StoredSnapshot, Error> {
    Ok(StoredSnapshot {
        next_seq_no: seq_no(&row, "next_seq_no")?,
        snapshot: EncodedSnapshot {
            manifest: row.try_get::<String, _>("manifest")?.into(),
            schema_version: schema_version(&row)?,
            payload: row.try_get("payload")?,
        },
    })
}

fn seq_no(row: &PgRow, column: &str) -> Result<SeqNo, Error> {
    let seq_no = row.try_get::<i64, _>(column)?;

    u64::try_from(seq_no)
        .map(SeqNo::new)
        .map_err(|_| Error::SeqNoOutOfRange)
}

fn schema_version(row: &PgRow) -> Result<SchemaVersion, Error> {
    let schema_version = row.try_get::<i32, _>("schema_version")?;

    u16::try_from(schema_version)
        .map(SchemaVersion::new)
        .map_err(|_| Error::SchemaVersionOutOfRange)
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.code().as_deref() == Some("23505"))
}
