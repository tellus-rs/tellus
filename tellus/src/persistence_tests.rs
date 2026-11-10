//! Contract tests for tellus persistence stores: the properties any [EventStore] and
//! [SnapshotStore] implementation must satisfy, callable from a backend crate's integration
//! tests against a real store. Run [event_store_contract] and [snapshot_store_contract], or the
//! granular checks for sharper failure locations, and [snapshot_with_event_tail] where both
//! stores are implemented. Every check uses fresh persistence IDs, so all checks can share one
//! long-lived store and server.
//!
//! A check panics with a description of the violated property, like an assertion in a test. The
//! checks need a Tokio runtime, e.g. under `#[tokio::test]`.

use crate::{
    persistence::{
        persistence_id::PersistenceId,
        seq_no::SeqNo,
        spawn::REPLAY_PAGE,
        store::{
            AppendError, EncodedEvent, EncodedSnapshot, EventStore, SnapshotStore, StoredEvent,
            StoredSnapshot,
        },
    },
    schema_version::SchemaVersion,
};
use std::{
    borrow::Cow,
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
    events_round_trip_at_their_extremes(store.clone()).await;
    conflicting_append_is_rejected_and_leaves_the_stream_untouched(store.clone()).await;
    an_append_to_an_empty_stream_is_fenced(store.clone()).await;
    an_empty_append_is_fenced(store.clone()).await;
    concurrent_appends_are_fenced(store.clone()).await;
    concurrent_appends_to_a_non_empty_stream_are_fenced(store.clone()).await;
    streams_are_isolated(store.clone()).await;
    streams_are_isolated_by_both_segments(store.clone()).await;
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

/// The extremes of an event read back exactly as written: the lowest and the highest schema
/// version, an empty payload, a payload holding every byte value, and a manifest beyond ASCII
/// with quotes and a backslash in it.
pub async fn events_round_trip_at_their_extremes<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();
    let events = Vec::from(
        extremes().map(|(manifest, schema_version, payload)| EncodedEvent {
            manifest,
            schema_version,
            payload,
        }),
    );

    store
        .append(&id, SeqNo::ZERO, events.clone())
        .await
        .expect("the append must succeed");
    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");

    assert_eq!(
        stored.len(),
        events.len(),
        "replay must return every appended event"
    );
    for (stored, event) in stored.into_iter().zip(events) {
        assert_eq!(
            stored.event.manifest, event.manifest,
            "a manifest must round-trip untouched, beyond ASCII and with quotes in it too"
        );
        assert_eq!(
            stored.event.schema_version, event.schema_version,
            "a schema version must round-trip untouched, the lowest and the highest too"
        );
        assert_eq!(
            stored.event.payload, event.payload,
            "a payload must round-trip untouched, empty or holding every byte value"
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

/// An append to an empty stream is conditional like any other: only next sequence number 0
/// matches it. An append expecting another one fails with [AppendError::Conflict], an empty
/// append included, and leaves the stream empty.
pub async fn an_append_to_an_empty_stream_is_fenced<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();

    for mismatched in [1, 7].map(SeqNo::new) {
        for events in [sample_events(2), Vec::new()] {
            let len = events.len();
            let conflict = store.append(&id, mismatched, events).await;
            assert!(
                matches!(conflict, Err(AppendError::Conflict)),
                "an append of {len} events expecting next sequence number {mismatched} on an \
                 empty stream must fail with Conflict, got {conflict:?}"
            );
        }
    }

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    assert!(
        stored.is_empty(),
        "a conflicting append must leave an empty stream empty"
    );
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
/// winner's events, gapless from sequence number 0. This is the race fencing exists for. A
/// non-atomic check-then-insert implementation passes the sequential checks but can fail here.
pub async fn concurrent_appends_are_fenced<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();

    let winner = winner_of_racing_appends(&store, &id, SeqNo::ZERO).await;

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    assert_eq!(
        stored
            .iter()
            .map(|stored| stored.seq_no)
            .collect::<Vec<_>>(),
        (0..writer_events(winner).len() as u64)
            .map(SeqNo::new)
            .collect::<Vec<_>>(),
        "the stream must be gapless from sequence number 0"
    );
    assert_eq!(
        stored
            .into_iter()
            .map(|stored| stored.event)
            .collect::<Vec<_>>(),
        writer_events(winner),
        "the stream must hold exactly the winner's events, never an interleaving"
    );
}

/// Concurrent appends racing at the head of a stream which holds events already are fenced:
/// exactly one writer wins, every loser fails with [AppendError::Conflict], and the stream holds
/// its earlier events followed by exactly the winner's, gapless.
pub async fn concurrent_appends_to_a_non_empty_stream_are_fenced<E>(store: E)
where
    E: EventStore,
{
    let id = unique_persistence_id();
    let events = sample_events(2);

    store
        .append(&id, SeqNo::ZERO, events.clone())
        .await
        .expect("the first append must succeed");

    let winner = winner_of_racing_appends(&store, &id, SeqNo::new(events.len() as u64)).await;

    let stored = store
        .read(&id, SeqNo::ZERO, LIMIT)
        .await
        .expect("reading the stream must succeed");
    let expected = events
        .into_iter()
        .chain(writer_events(winner))
        .collect::<Vec<_>>();
    assert_eq!(
        stored
            .iter()
            .map(|stored| stored.seq_no)
            .collect::<Vec<_>>(),
        (0..expected.len() as u64)
            .map(SeqNo::new)
            .collect::<Vec<_>>(),
        "the stream must stay gapless, with no sequence number twice"
    );
    assert_eq!(
        stored
            .into_iter()
            .map(|stored| stored.event)
            .collect::<Vec<_>>(),
        expected,
        "the stream must hold its earlier events followed by exactly the winner's"
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

/// Both segments of a persistence ID identify the stream: one entity ID under two entity types
/// names two streams, and so do two IDs whose segments concatenate to the same string.
pub async fn streams_are_isolated_by_both_segments<E>(store: E)
where
    E: EventStore,
{
    let events = sample_events(2);

    for (id_a, id_b) in confusable_id_pairs() {
        store
            .append(&id_a, SeqNo::ZERO, vec![events[0].clone()])
            .await
            .expect("the append to the first stream must succeed");
        let appended = store
            .append(&id_b, SeqNo::ZERO, vec![events[1].clone()])
            .await;
        assert!(
            appended.is_ok(),
            "{id_b} must be a stream of its own, empty whatever {id_a} holds, got {appended:?}"
        );

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
                .into_iter()
                .map(|stored| stored.event)
                .collect::<Vec<_>>(),
            [events[0].clone()],
            "{id_a} must contain exactly its own events, none of {id_b}"
        );
        assert_eq!(
            stored_b
                .into_iter()
                .map(|stored| stored.event)
                .collect::<Vec<_>>(),
            [events[1].clone()],
            "{id_b} must contain exactly its own events, none of {id_a}"
        );
    }
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
    snapshots_round_trip_and_the_latest_wins(store.clone()).await;
    snapshots_round_trip_at_their_extremes(store.clone()).await;
    snapshots_are_isolated(store.clone()).await;
    snapshots_are_isolated_by_both_segments(store).await;
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

/// The extremes of a snapshot load back exactly as written: the lowest and the highest schema
/// version, an empty payload, a payload holding every byte value, and a manifest beyond ASCII
/// with quotes and a backslash in it.
pub async fn snapshots_round_trip_at_their_extremes<S>(store: S)
where
    S: SnapshotStore,
{
    for (manifest, schema_version, payload) in extremes() {
        let id = unique_persistence_id();
        let snapshot = EncodedSnapshot {
            manifest,
            schema_version,
            payload,
        };

        store
            .save(&id, SeqNo::new(3), snapshot.clone())
            .await
            .expect("the save must succeed");
        let loaded = store
            .load(&id)
            .await
            .expect("the load must succeed")
            .expect("the snapshot must be loaded");

        assert_eq!(
            loaded.snapshot.manifest, snapshot.manifest,
            "a manifest must load back untouched, beyond ASCII and with quotes in it too"
        );
        assert_eq!(
            loaded.snapshot.schema_version, snapshot.schema_version,
            "a schema version must load back untouched, the lowest and the highest too"
        );
        assert_eq!(
            loaded.snapshot.payload, snapshot.payload,
            "a payload must load back untouched, empty or holding every byte value"
        );
    }
}

/// Snapshots are isolated per persistence ID: snapshots saved under two IDs of the same entity
/// type each load back as their own, with their own sequence number.
pub async fn snapshots_are_isolated<S>(store: S)
where
    S: SnapshotStore,
{
    let id_a = unique_persistence_id();
    let id_b = unique_persistence_id();
    let snapshot_a = sample_snapshot(1);
    let snapshot_b = sample_snapshot(2);

    store
        .save(&id_a, SeqNo::new(3), snapshot_a.clone())
        .await
        .expect("the save under the first ID must succeed");
    store
        .save(&id_b, SeqNo::new(5), snapshot_b.clone())
        .await
        .expect("the save under the second ID must succeed");

    let loaded_a = store
        .load(&id_a)
        .await
        .expect("loading the first ID must succeed");
    let loaded_b = store
        .load(&id_b)
        .await
        .expect("loading the second ID must succeed");

    assert_eq!(
        loaded_a,
        Some(StoredSnapshot {
            next_seq_no: SeqNo::new(3),
            snapshot: snapshot_a,
        }),
        "the first ID must load exactly its own snapshot"
    );
    assert_eq!(
        loaded_b,
        Some(StoredSnapshot {
            next_seq_no: SeqNo::new(5),
            snapshot: snapshot_b,
        }),
        "the second ID must load exactly its own snapshot"
    );
}

/// Both segments of a persistence ID identify a snapshot: a snapshot saved under one ID loads
/// under no other one, be it the same entity ID under another entity type or an ID whose
/// segments concatenate to the same string.
pub async fn snapshots_are_isolated_by_both_segments<S>(store: S)
where
    S: SnapshotStore,
{
    for (id_a, id_b) in confusable_id_pairs() {
        let snapshot_a = sample_snapshot(1);
        let snapshot_b = sample_snapshot(2);

        store
            .save(&id_a, SeqNo::new(3), snapshot_a.clone())
            .await
            .expect("the save under the first ID must succeed");
        let unknown = store
            .load(&id_b)
            .await
            .expect("loading the second ID must succeed");
        assert_eq!(
            unknown, None,
            "a snapshot saved under {id_a} must not load under {id_b}"
        );

        store
            .save(&id_b, SeqNo::new(5), snapshot_b.clone())
            .await
            .expect("the save under the second ID must succeed");
        let loaded_a = store
            .load(&id_a)
            .await
            .expect("loading the first ID must succeed");
        let loaded_b = store
            .load(&id_b)
            .await
            .expect("loading the second ID must succeed");
        assert_eq!(
            loaded_a,
            Some(StoredSnapshot {
                next_seq_no: SeqNo::new(3),
                snapshot: snapshot_a,
            }),
            "{id_a} must load exactly its own snapshot, not the one of {id_b}"
        );
        assert_eq!(
            loaded_b,
            Some(StoredSnapshot {
                next_seq_no: SeqNo::new(5),
                snapshot: snapshot_b,
            }),
            "{id_b} must load exactly its own snapshot, not the one of {id_a}"
        );
    }
}

/// The recovery read path across both stores: with 5 events and a snapshot covering the first 3,
/// loading the snapshot and reading from its sequence number yields exactly the tail. That makes a
/// snapshot plus its tail equal to full replay.
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
    persistence_id("contract", unique_entity_id())
}

fn persistence_id<T, I>(entity_type: T, entity_id: I) -> PersistenceId
where
    T: AsRef<str>,
    I: AsRef<str>,
{
    PersistenceId::new(entity_type, entity_id).expect("the segments are valid")
}

fn unique_entity_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the current time is after the Unix epoch")
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);

    format!("{nanos}-{count}")
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

fn extremes() -> [(Cow<'static, str>, SchemaVersion, Vec<u8>); 2] {
    [
        ("contract".into(), SchemaVersion::new(0), Vec::new()),
        (
            "contract-événement \"quoted\" 'single' back\\slash 100%".into(),
            SchemaVersion::new(u16::MAX),
            (0..=u8::MAX).collect(),
        ),
    ]
}

async fn winner_of_racing_appends<E>(store: &E, id: &PersistenceId, next_seq_no: SeqNo) -> u8
where
    E: EventStore,
{
    const WRITERS: u8 = 8;

    let mut appends = JoinSet::new();
    for writer in 0..WRITERS {
        let store = store.clone();
        let id = id.clone();
        appends.spawn(async move {
            (
                writer,
                store.append(&id, next_seq_no, writer_events(writer)).await,
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
        "exactly one of the appends racing at next sequence number {next_seq_no} must win, got \
         winners {winners:?}"
    );
    assert_eq!(
        conflicts,
        u32::from(WRITERS) - 1,
        "every append losing the race at next sequence number {next_seq_no} must fail with \
         Conflict"
    );

    winners[0]
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

fn confusable_id_pairs() -> [(PersistenceId, PersistenceId); 2] {
    let unique = unique_entity_id();

    [
        (
            persistence_id("contract-a", &unique),
            persistence_id("contract-b", &unique),
        ),
        (
            persistence_id("contract-c", format!("d{unique}")),
            persistence_id("contract-cd", &unique),
        ),
    ]
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
