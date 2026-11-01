//! Contract tests for tellus persistence stores: the properties any [EventStore] and
//! [SnapshotStore] implementation must satisfy, callable from a backend crate's integration
//! tests against a real store. Run [event_store_contract] and [snapshot_store_contract], or the
//! granular checks for sharper failure locations, and [snapshot_with_event_tail] where both
//! stores are implemented. Every check uses fresh persistence IDs, so all checks can share one
//! long-lived store and server.
//!
//! A check panics with a description of the violated property, like an assertion in a test. The
//! checks need a Tokio runtime, e.g. under `#[tokio::test]`.

use crate::persistence::{
    persistence_id::PersistenceId,
    schema_version::SchemaVersion,
    seq_no::SeqNo,
    spawn::REPLAY_PAGE,
    store::{
        AppendError, EncodedEvent, EncodedSnapshot, EventStore, SnapshotStore, StoredEvent,
        StoredSnapshot,
    },
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinSet;

const LIMIT: NonZeroUsize = NonZeroUsize::new(1_024).unwrap();

/// Run every [EventStore] check against the given store.
pub async fn event_store_contract<E>(store: E)
where
    E: EventStore,
{
    append_then_read_round_trips(store.clone()).await;
    conflicting_append_is_rejected_and_leaves_the_stream_untouched(store.clone()).await;
    an_empty_append_is_fenced(store.clone()).await;
    concurrent_appends_are_fenced(store.clone()).await;
    streams_are_isolated(store.clone()).await;
    reads_are_paged_and_inclusive(store.clone()).await;
    reads_return_full_pages(store).await;
}

/// Appended events read back complete, in order, gapless from sequence number 0, with manifest,
/// schema version and payload untouched.
pub async fn append_then_read_round_trips<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();
    let events = sample_events(3);

    store
        .append(&id, SeqNo::ZERO, events.clone())
        .await
        .expect("the first append must succeed");
    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");

    assert_eq!(
        stored.len(),
        events.len(),
        "replay must return every appended event"
    );
    for (n, stored) in stored.iter().enumerate() {
        assert_eq!(
            stored.seq_no,
            SeqNo::new(n as u64),
            "sequence numbers must be gapless and start at 0"
        );
        assert_eq!(
            stored.event, events[n],
            "manifest, schema version and payload must round-trip untouched"
        );
    }
}

/// An append whose expected next sequence number does not match the stream's actual one, below or
/// above, fails with [AppendError::Conflict] and leaves the stream untouched, all events of the
/// failed append included: this is the fencing guarantee.
pub async fn conflicting_append_is_rejected_and_leaves_the_stream_untouched<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();
    let events = sample_events(2);

    store
        .append(&id, SeqNo::ZERO, events.clone())
        .await
        .expect("the first append must succeed");

    for mismatched in [0, 1, 7].map(SeqNo::new) {
        let conflict = store.append(&id, mismatched, sample_events(3)).await;
        assert!(
            matches!(conflict, Err(AppendError::Conflict)),
            "an append expecting next sequence number {mismatched} on a stream with 2 events must \
             fail with Conflict, got {conflict:?}"
        );
    }

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    assert_eq!(
        stored.len(),
        events.len(),
        "a conflicting append must leave the stream untouched"
    );
    for (n, stored) in stored.iter().enumerate() {
        assert_eq!(
            stored.event, events[n],
            "a conflicting append must not alter stored events"
        );
    }
}

/// An empty append inserts nothing but is still conditional: it succeeds against the stream's
/// actual next sequence number, empty streams included, fails with [AppendError::Conflict]
/// against a mismatched one, below or above, and leaves the stream untouched either way.
pub async fn an_empty_append_is_fenced<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();

    store
        .append(&id, SeqNo::ZERO, Vec::new())
        .await
        .expect("an empty append at the head of an empty stream must succeed");

    let events = sample_events(2);
    store
        .append(&id, SeqNo::ZERO, events.clone())
        .await
        .expect("the first append must succeed");

    store
        .append(&id, SeqNo::new(2), Vec::new())
        .await
        .expect("an empty append at the stream head must succeed");

    for mismatched in [0, 1, 7].map(SeqNo::new) {
        let conflict = store.append(&id, mismatched, Vec::new()).await;
        assert!(
            matches!(conflict, Err(AppendError::Conflict)),
            "an empty append expecting next sequence number {mismatched} on a stream with 2 \
             events must fail with Conflict, got {conflict:?}"
        );
    }

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    assert_eq!(
        stored.len(),
        events.len(),
        "an empty append must leave the stream untouched"
    );
}

/// Concurrent appends racing on one expected next sequence number are fenced: exactly one writer
/// wins, every loser fails with [AppendError::Conflict], and the stream holds exactly the
/// winner's events, gapless from sequence number 0. This is the race fencing exists for; a
/// non-atomic check-then-insert implementation passes the sequential checks but fails here.
pub async fn concurrent_appends_are_fenced<E>(store: E)
where
    E: EventStore,
{
    const WRITERS: u8 = 8;

    let id = unique_persistence_id();

    let mut appends = JoinSet::new();
    for writer in 0..WRITERS {
        let store = store.clone();
        let id = id.clone();
        appends.spawn(async move {
            (
                writer,
                store.append(&id, SeqNo::ZERO, writer_events(writer)).await,
            )
        });
    }

    let mut winners = Vec::new();
    let mut conflicts = 0;
    while let Some(joined) = appends.join_next().await {
        let (writer, appended) = joined.expect("a writer task must not panic");
        match appended {
            Ok(()) => winners.push(writer),

            Err(AppendError::Conflict) => conflicts += 1,

            Err(AppendError::Store(error)) => {
                panic!("a racing append must fail with Conflict, not a store error: {error}")
            }
        }
    }

    assert_eq!(
        winners.len(),
        1,
        "exactly one of the racing appends must win, got winners {winners:?}"
    );
    assert_eq!(
        conflicts,
        u32::from(WRITERS) - 1,
        "every losing append must fail with Conflict"
    );

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    assert_eq!(
        stored
            .iter()
            .map(|stored| stored.seq_no)
            .collect::<Vec<_>>(),
        (0..writer_events(winners[0]).len() as u64)
            .map(SeqNo::new)
            .collect::<Vec<_>>(),
        "the stream must be gapless from sequence number 0"
    );
    assert_eq!(
        stored
            .into_iter()
            .map(|stored| stored.event)
            .collect::<Vec<_>>(),
        writer_events(winners[0]),
        "the stream must hold exactly the winner's events, never an interleaving"
    );
}

/// Streams are isolated per persistence ID: interleaved appends to two IDs never leak across,
/// and each stream numbers its events independently.
pub async fn streams_are_isolated<E>(store: E)
where
    E: EventStore,
{
    let id_a = unique_persistence_id();
    let id_b = unique_persistence_id();
    let events_a = sample_events(2);
    let events_b = sample_events(1);

    store
        .append(&id_a, SeqNo::ZERO, vec![events_a[0].clone()])
        .await
        .expect("the append to the first stream must succeed");
    store
        .append(&id_b, SeqNo::ZERO, events_b.clone())
        .await
        .expect("the append to the second stream must succeed");
    store
        .append(&id_a, SeqNo::new(1), vec![events_a[1].clone()])
        .await
        .expect("the second append to the first stream must succeed");

    let stored_a = store
        .read(&id_a, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the first stream must succeed");
    let stored_b = store
        .read(&id_b, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the second stream must succeed");

    assert_eq!(
        stored_a
            .iter()
            .map(|stored| stored.seq_no)
            .collect::<Vec<_>>(),
        [SeqNo::ZERO, SeqNo::new(1)],
        "the first stream must number its events independently"
    );
    assert_eq!(
        stored_b
            .iter()
            .map(|stored| stored.seq_no)
            .collect::<Vec<_>>(),
        [SeqNo::ZERO],
        "the second stream must number its events independently"
    );
    assert_eq!(
        stored_a
            .into_iter()
            .map(|stored| stored.event)
            .collect::<Vec<_>>(),
        events_a,
        "the first stream must contain exactly its own events"
    );
    assert_eq!(
        stored_b
            .into_iter()
            .map(|stored| stored.event)
            .collect::<Vec<_>>(),
        events_b,
        "the second stream must contain exactly its own events"
    );
}

/// Reads start at the given sequence number inclusively, honor the limit, and return nothing
/// beyond the stream or for an unknown ID.
pub async fn reads_are_paged_and_inclusive<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();

    store
        .append(&id, SeqNo::ZERO, sample_events(5))
        .await
        .expect("the append must succeed");

    let page = store
        .read(
            &id,
            SeqNo::new(1),
            NonZeroUsize::new(2).expect("2 is not zero"),
        )
        .await
        .expect("the paged read must succeed");
    assert_eq!(
        seq_nos(page),
        [SeqNo::new(1), SeqNo::new(2)],
        "a read must start at the given sequence number inclusively and honor the limit"
    );

    let tail = store
        .read(&id, SeqNo::new(3), LIMIT)
        .await
        .expect("the tail read must succeed");
    assert_eq!(
        seq_nos(tail),
        [SeqNo::new(3), SeqNo::new(4)],
        "a read must return the whole tail"
    );

    let beyond = store
        .read(&id, SeqNo::new(5), LIMIT)
        .await
        .expect("the read beyond the stream must succeed");
    assert!(
        beyond.is_empty(),
        "a read beyond the stream must return nothing"
    );

    let unknown = store
        .read(&unique_persistence_id(), SeqNo::ZERO, LIMIT)
        .await
        .expect("the read of an unknown ID must succeed");
    assert!(
        unknown.is_empty(),
        "a read of an unknown ID must return nothing"
    );
}

/// A read returns exactly `limit` events while the stream holds that many from the given sequence
/// number on, and fewer only where the stream ends, over a stream longer than two replay pages.
/// Replay stops at the first short page, so a store capping its pages truncates recovery.
pub async fn reads_return_full_pages<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();
    let len = 2 * REPLAY_PAGE.get() as u64 + 1;

    store
        .append(&id, SeqNo::ZERO, sample_events(len as usize))
        .await
        .expect("the append must succeed");

    let page_len = REPLAY_PAGE.get() as u64;
    for from in [0, page_len, 2 * page_len] {
        let page = store
            .read(&id, SeqNo::new(from), REPLAY_PAGE)
            .await
            .expect("the paged read must succeed");
        assert_eq!(
            seq_nos(page),
            (from..len.min(from + page_len))
                .map(SeqNo::new)
                .collect::<Vec<_>>(),
            "a read from sequence number {from} of a stream with {len} events must return exactly \
             {page_len} events while the stream holds that many, and only then fewer"
        );
    }

    let page = store
        .read(&id, SeqNo::new(1), LIMIT)
        .await
        .expect("the read must succeed");
    assert_eq!(
        seq_nos(page),
        (1..1 + LIMIT.get() as u64)
            .map(SeqNo::new)
            .collect::<Vec<_>>(),
        "a read from sequence number 1 of a stream with {len} events must return exactly {LIMIT} \
         events"
    );
}

/// Run every [SnapshotStore] check against the given store.
pub async fn snapshot_store_contract<S>(store: S)
where
    S: SnapshotStore,
{
    snapshots_round_trip_and_the_latest_wins(store).await;
}

/// An unknown ID loads no snapshot; a saved snapshot loads back untouched together with the
/// sequence number at which replay resumes; saving again replaces it, so only the latest snapshot
/// is loaded.
pub async fn snapshots_round_trip_and_the_latest_wins<S>(store: S)
where
    S: SnapshotStore,
{
    let id = unique_persistence_id();

    let unknown = store
        .load(&id)
        .await
        .expect("loading an unknown ID must succeed");
    assert_eq!(unknown, None, "an unknown ID must load no snapshot");

    let first = sample_snapshot(1);
    store
        .save(&id, SeqNo::new(3), first.clone())
        .await
        .expect("the first save must succeed");
    let loaded = store.load(&id).await.expect("the first load must succeed");
    assert_eq!(
        loaded,
        Some(StoredSnapshot {
            next_seq_no: SeqNo::new(3),
            snapshot: first,
        }),
        "a saved snapshot must load back untouched with its sequence number"
    );

    let second = sample_snapshot(2);
    store
        .save(&id, SeqNo::new(6), second.clone())
        .await
        .expect("the second save must succeed");
    let loaded = store.load(&id).await.expect("the second load must succeed");
    assert_eq!(
        loaded,
        Some(StoredSnapshot {
            next_seq_no: SeqNo::new(6),
            snapshot: second,
        }),
        "saving again must replace the earlier snapshot"
    );
}

/// The recovery read path across both stores: with 5 events and a snapshot covering the first 3,
/// loading the snapshot and reading from its sequence number yields exactly the tail, which is
/// what makes a snapshot plus its tail equal to full replay.
pub async fn snapshot_with_event_tail<E, S>(event_store: E, snapshot_store: S)
where
    E: EventStore,
    S: SnapshotStore,
{
    let id = unique_persistence_id();

    event_store
        .append(&id, SeqNo::ZERO, sample_events(5))
        .await
        .expect("the append must succeed");
    snapshot_store
        .save(&id, SeqNo::new(3), sample_snapshot(1))
        .await
        .expect("the save must succeed");

    let snapshot = snapshot_store
        .load(&id)
        .await
        .expect("the load must succeed")
        .expect("the snapshot must be loaded");
    assert_eq!(
        snapshot.next_seq_no,
        SeqNo::new(3),
        "the snapshot must resume replay at sequence number 3"
    );

    let tail = event_store
        .read(&id, snapshot.next_seq_no, LIMIT)
        .await
        .expect("the tail read must succeed");
    assert_eq!(
        tail.iter().map(|stored| stored.seq_no).collect::<Vec<_>>(),
        [SeqNo::new(3), SeqNo::new(4)],
        "reading after the snapshot must yield exactly the event tail"
    );
}

fn unique_persistence_id() -> PersistenceId {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the current time is after the Unix epoch")
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);

    PersistenceId::new("contract", format!("{nanos}-{count}")).expect("the segments are valid")
}

fn sample_events(count: usize) -> Vec<EncodedEvent> {
    (0..count)
        .map(|n| EncodedEvent {
            manifest: "contract-event".into(),
            schema_version: SchemaVersion::new(n as u16 + 1),
            payload: vec![0xC0, n as u8, 0xFF],
        })
        .collect()
}

fn writer_events(writer: u8) -> Vec<EncodedEvent> {
    (0..3)
        .map(|n| EncodedEvent {
            manifest: "contract-event".into(),
            schema_version: SchemaVersion::new(1),
            payload: vec![0xC2, writer, n],
        })
        .collect()
}

fn seq_nos(stored: Vec<StoredEvent>) -> Vec<SeqNo> {
    stored.into_iter().map(|stored| stored.seq_no).collect()
}

fn sample_snapshot(tag: u8) -> EncodedSnapshot {
    EncodedSnapshot {
        manifest: "contract-snapshot".into(),
        schema_version: SchemaVersion::new(1),
        payload: vec![0xC1, tag],
    }
}
