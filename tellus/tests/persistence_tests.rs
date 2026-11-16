//! Validates the contract suite itself: the checks must be runnable and must pass against
//! [InMemoryStore], which follows the contract by construction, and a check must fail against a
//! store which breaks the property it names.

use std::{convert::Infallible, num::NonZeroUsize};
use tellus::{
    AppendError, EncodedEvent, EncodedSnapshot, EventStore, InMemoryStore, PersistenceId, SeqNo,
    SnapshotStore, StoredEvent, StoredSnapshot,
};

#[tokio::test]
async fn event_store_contract() {
    tellus::persistence_tests::event_store_contract(InMemoryStore::default()).await;
}

#[tokio::test]
async fn snapshot_store_contract() {
    tellus::persistence_tests::snapshot_store_contract(InMemoryStore::default()).await;
}

#[tokio::test]
async fn snapshot_with_event_tail() {
    let store = InMemoryStore::default();
    tellus::persistence_tests::snapshot_with_event_tail(store.clone(), store).await;
}

/// The fencing check fails against a store which appends at the stream's end whatever the
/// expected next sequence number.
#[tokio::test]
#[should_panic(expected = "must fail with Conflict")]
async fn the_fencing_check_fails_against_an_unfenced_store() {
    tellus::persistence_tests::conflicting_append_is_rejected_and_leaves_the_stream_untouched(
        Unfenced::default(),
    )
    .await;
}

/// The paging check fails against a store which returns one event less than a full page.
#[tokio::test]
#[should_panic(expected = "must return exactly")]
async fn the_paging_check_fails_against_a_store_with_short_pages() {
    tellus::persistence_tests::reads_return_full_pages(ShortPages::default()).await;
}

/// The snapshot check fails against a store which keeps the first snapshot rather than the latest.
#[tokio::test]
#[should_panic(expected = "saving again must replace the earlier snapshot")]
async fn the_snapshot_check_fails_against_a_store_keeping_the_first_snapshot() {
    tellus::persistence_tests::snapshots_round_trip_and_the_latest_wins(
        FirstSnapshotWins::default(),
    )
    .await;
}

/// Appends at the end of the stream, ignoring the expected next sequence number.
#[derive(Debug, Clone, Default)]
struct Unfenced(InMemoryStore);

impl EventStore for Unfenced {
    type Error = <InMemoryStore as EventStore>::Error;

    async fn append(
        &self,
        id: &PersistenceId,
        _: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        let len = self
            .0
            .read(id, SeqNo::ZERO, NonZeroUsize::MAX)
            .await
            .map_err(AppendError::Store)?
            .len();
        let head = SeqNo::new(u64::try_from(len).expect("the stream length fits a u64"));
        self.0.append(id, head, events).await
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        self.0.read(id, from_seq_no, limit).await
    }
}

/// Drops the last event of every full page.
#[derive(Debug, Clone, Default)]
struct ShortPages(InMemoryStore);

impl EventStore for ShortPages {
    type Error = <InMemoryStore as EventStore>::Error;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        self.0.append(id, next_seq_no, events).await
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let mut page = self.0.read(id, from_seq_no, limit).await?;
        if page.len() == limit.get() {
            page.pop();
        }
        Ok(page)
    }
}

/// Saves a snapshot only where none is stored yet.
#[derive(Debug, Clone, Default)]
struct FirstSnapshotWins(InMemoryStore);

impl SnapshotStore for FirstSnapshotWins {
    type Error = Infallible;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        if self.0.load(id).await?.is_none() {
            self.0.save(id, next_seq_no, snapshot).await?;
        }
        Ok(())
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        self.0.load(id).await
    }
}
