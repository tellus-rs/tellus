use crate::persistence::{
    persistence_id::PersistenceId,
    seq_no::{SeqNo, SeqNoOverflow},
    store::{
        AppendError, EncodedEvent, EncodedSnapshot, EventStore, SnapshotStore, StoredEvent,
        StoredSnapshot,
    },
};
use std::{
    collections::HashMap,
    convert::Infallible,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// An in-memory [EventStore] and [SnapshotStore] for testing event-sourced actors: clones share
/// the same streams and snapshots, which are lost with the process. An append fails only with
/// [SeqNoOverflow], the snapshot store never fails, hence [Infallible], and a poisoned lock
/// panics.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    streams: Arc<Mutex<HashMap<PersistenceId, Vec<StoredEvent>>>>,
    snapshots: Arc<Mutex<HashMap<PersistenceId, StoredSnapshot>>>,
}

impl InMemoryStore {
    /// The stored events of the given ID, oldest first.
    pub fn events(&self, id: &PersistenceId) -> Vec<StoredEvent> {
        self.streams
            .lock()
            .expect("streams lock poisoned")
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// The stored snapshot of the given ID, if there is one.
    pub fn snapshot(&self, id: &PersistenceId) -> Option<StoredSnapshot> {
        self.snapshots
            .lock()
            .expect("snapshots lock poisoned")
            .get(id)
            .cloned()
    }
}

impl EventStore for InMemoryStore {
    type Error = SeqNoOverflow;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        let mut streams = self.streams.lock().expect("streams lock poisoned");
        let stream = streams.entry(id.clone()).or_default();
        if SeqNo::new(stream.len() as u64) != next_seq_no {
            return Err(AppendError::Conflict);
        }

        let count = events.len();
        let Some(head) = next_seq_no.advanced_by(count) else {
            return Err(AppendError::Store(SeqNoOverflow {
                seq_no: next_seq_no,
                count,
            }));
        };

        let seq_nos = (next_seq_no.as_u64()..head.as_u64()).map(SeqNo::new);
        for (seq_no, event) in seq_nos.zip(events) {
            stream.push(StoredEvent { seq_no, event });
        }
        Ok(())
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let streams = self.streams.lock().expect("streams lock poisoned");
        let events = streams
            .get(id)
            .map(|stream| {
                // The index is the sequence number: append keeps every stream gapless from 0!
                let start = (from_seq_no.as_u64() as usize).min(stream.len());
                let end = start.saturating_add(limit.get()).min(stream.len());
                stream[start..end].to_vec()
            })
            .unwrap_or_default();

        Ok(events)
    }
}

impl SnapshotStore for InMemoryStore {
    type Error = Infallible;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        self.snapshots
            .lock()
            .expect("snapshots lock poisoned")
            .insert(
                id.clone(),
                StoredSnapshot {
                    next_seq_no,
                    snapshot,
                },
            );

        Ok(())
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        let snapshot = self
            .snapshots
            .lock()
            .expect("snapshots lock poisoned")
            .get(id)
            .cloned();

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AppendError, EncodedEvent, EncodedSnapshot, EventStore, PersistenceId, SchemaVersion,
        SeqNo, SnapshotStore, persistence::in_memory_store::InMemoryStore,
    };

    #[tokio::test]
    async fn appended_events_are_returned_in_order() {
        let store = InMemoryStore::default();
        let id = persistence_id("events");

        store
            .append(&id, SeqNo::ZERO, vec![event(1), event(2)])
            .await
            .expect("the append succeeds");

        let events = store.events(&id);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].seq_no, SeqNo::ZERO);
        assert_eq!(events[0].event, event(1));
        assert_eq!(events[1].seq_no, SeqNo::new(1));
        assert_eq!(events[1].event, event(2));
    }

    #[tokio::test]
    async fn an_append_at_the_last_position_of_an_empty_stream_is_a_conflict() {
        let store = InMemoryStore::default();
        let id = persistence_id("overflow");

        let appended = store
            .append(&id, SeqNo::new(u64::MAX), vec![event(1)])
            .await;

        assert!(matches!(appended, Err(AppendError::Conflict)));
        assert!(store.events(&id).is_empty());
    }

    #[tokio::test]
    async fn a_saved_snapshot_is_returned() {
        let store = InMemoryStore::default();
        let id = persistence_id("snapshot");

        assert!(store.snapshot(&id).is_none());

        store
            .save(&id, SeqNo::new(2), snapshot())
            .await
            .expect("the save succeeds");
        let stored = store.snapshot(&id).expect("the snapshot is stored");

        assert_eq!(stored.next_seq_no, SeqNo::new(2));
        assert_eq!(stored.snapshot, snapshot());
    }

    #[tokio::test]
    async fn an_unknown_id_holds_nothing() {
        let store = InMemoryStore::default();

        assert!(store.events(&persistence_id("unknown")).is_empty());
        assert!(store.snapshot(&persistence_id("unknown")).is_none());
    }

    #[tokio::test]
    async fn a_clone_shares_events_and_snapshots() {
        let store = InMemoryStore::default();
        let id = persistence_id("clone");
        let clone = store.clone();

        store
            .append(&id, SeqNo::ZERO, vec![event(1)])
            .await
            .expect("the append succeeds");
        store
            .save(&id, SeqNo::new(1), snapshot())
            .await
            .expect("the save succeeds");

        assert_eq!(clone.events(&id).len(), 1);
        assert!(clone.snapshot(&id).is_some());
    }

    fn persistence_id(name: &str) -> PersistenceId {
        PersistenceId::new("in-memory", name).expect("the segments are valid")
    }

    fn event(n: u8) -> EncodedEvent {
        EncodedEvent {
            manifest: "in-memory-event".into(),
            schema_version: SchemaVersion::new(1),
            payload: vec![n],
        }
    }

    fn snapshot() -> EncodedSnapshot {
        EncodedSnapshot {
            manifest: "in-memory-snapshot".into(),
            schema_version: SchemaVersion::new(1),
            payload: vec![0xFF],
        }
    }
}
