use crate::persistence::{
    persistence_id::PersistenceId, schema_version::SchemaVersion, seq_no::SeqNo,
};
use std::{borrow::Cow, convert::Infallible, error::Error, num::NonZeroUsize};
use thiserror::Error;
use tracing::warn;

/// A store of event streams, one per [PersistenceId], operating on encoded payloads: the durable
/// source of truth. Sequence numbers are per stream, gapless and start at 0. Implementations must
/// be cheap to clone, e.g. by wrapping a connection pool.
#[trait_variant::make(Send)]
pub trait EventStore
where
    Self: Clone + Send + Sync + 'static,
{
    /// The type of the store's failures.
    type Error: Error + Send + Sync + 'static;

    /// Append the given events at sequence number `next_seq_no`, the first one taking that number,
    /// atomically: after a crash the stream contains all of them or none. The append is
    /// conditional on `next_seq_no`, which fences concurrent incarnations. `next_seq_no` is
    /// [SeqNo::ZERO] for an empty stream. An empty `events` appends nothing but is still
    /// conditional.
    ///
    /// # Errors
    /// Implementations fail with a conflict if the stream's actual next sequence number differs
    /// from `next_seq_no`, below or above, leaving the stream untouched, or with their own error
    /// if the events cannot be written; either way the append is all or nothing.
    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>>;

    /// Read up to `limit` events from the given sequence number on, inclusive, in ascending
    /// order. The events are consecutive, the first one at `from_seq_no`. Replay pages through
    /// this until a page is short.
    ///
    /// # Errors
    /// Implementations fail if the events cannot be read; a stream with nothing from
    /// `from_seq_no` on is an empty page, not a failure.
    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error>;
}

/// A store of snapshots, at most the latest one per [PersistenceId]: a discardable derivative of
/// the events, never a source of truth. Implementations must be cheap to clone, e.g. by wrapping
/// a connection pool.
#[trait_variant::make(Send)]
pub trait SnapshotStore
where
    Self: Clone + Send + Sync + 'static,
{
    /// The type of the store's failures.
    type Error: Error + Send + Sync + 'static;

    /// Save the given snapshot together with `next_seq_no`, the sequence number at which replay
    /// resumes, replacing any earlier snapshot: only the latest one is ever loaded.
    ///
    /// # Errors
    /// Implementations fail if the snapshot cannot be written.
    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error>;

    /// Load the latest snapshot, if any.
    ///
    /// # Errors
    /// Implementations fail if the store cannot be read; no snapshot is `None`, not a failure.
    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error>;
}

/// The [SnapshotStore] of actors without snapshots: it loads nothing, and a snapshot offered
/// without a configured snapshot store is dropped and logged.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSnapshots;

impl SnapshotStore for NoSnapshots {
    type Error = Infallible;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        _snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        warn!(%id, %next_seq_no, "snapshot dropped, no snapshot store configured");

        Ok(())
    }

    async fn load(&self, _id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        Ok(None)
    }
}

/// An encoded event as handed to [EventStore::append].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedEvent {
    /// The stable name of the event type, from [Versioned::MANIFEST](crate::Versioned::MANIFEST).
    pub manifest: Cow<'static, str>,

    /// The schema version of the payload, from [Versioned::VERSION](crate::Versioned::VERSION) at
    /// the time of writing.
    pub schema_version: SchemaVersion,

    /// The encoded event itself.
    pub payload: Vec<u8>,
}

/// A stored event as returned by [EventStore::read].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// The position in the stream, gapless and starting at 0.
    pub seq_no: SeqNo,

    /// The encoded event.
    pub event: EncodedEvent,
}

/// An encoded snapshot as handed to [SnapshotStore::save].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedSnapshot {
    /// The stable name of the snapshot type, from
    /// [Versioned::MANIFEST](crate::Versioned::MANIFEST).
    pub manifest: Cow<'static, str>,

    /// The schema version of the payload, from [Versioned::VERSION](crate::Versioned::VERSION) at
    /// the time of writing.
    pub schema_version: SchemaVersion,

    /// The encoded snapshot itself.
    pub payload: Vec<u8>,
}

/// A stored snapshot as returned by [SnapshotStore::load].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    /// The sequence number at which replay resumes: the snapshot covers every event before it.
    pub next_seq_no: SeqNo,

    /// The encoded snapshot.
    pub snapshot: EncodedSnapshot,
}

/// Errors possibly returned by [EventStore::append].
#[derive(Debug, Error)]
pub enum AppendError<E>
where
    E: Error,
{
    /// The given next sequence number does not match the stream head, below or above: another
    /// writer has extended the stream, or the caller expected a stream it never wrote.
    #[error("append at a next sequence number not matching the stream head")]
    Conflict,

    /// The store itself failed.
    #[error(transparent)]
    Store(#[from] E),
}
