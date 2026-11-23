use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    error::{self, Error as _},
    fmt::{self, Display, Formatter},
    future::pending,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicBool, Ordering::SeqCst},
        mpsc as std_mpsc,
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AppendError, AskError, Backoff, Cbor,
    Codec, Control, Effect, EncodeError, EncodedEvent, EncodedSnapshot, EventSourced, EventStore,
    InMemoryStore, Incoming, Json, Nothing, PayloadError, Persistence, PersistenceId, ReplyTo,
    RestartPolicy, SchemaVersion, SeqNo, SnapshotStore, Stashed, StoredEvent, StoredSnapshot,
    SupervisionStrategy, Timed, Unstashed, Untimed, Versioned,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{block_in_place, spawn},
    time::{Instant, advance, sleep, timeout},
};

const TIMEOUT: Duration = Duration::from_secs(5);
const STORE_TIMEOUT: Duration = Duration::from_secs(1);

static TRACING: Once = Once::new();

/// Replay reconstructs the live state: a counter increments across single and atomic multi-event
/// effects and persists a final event while stopping. A second incarnation over the same store
/// answers with the same count, seeded by `init` and folded by `apply` alone.
#[tokio::test]
async fn replay_reconstructs_the_live_state() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("1", probe_tx.clone()),
        Persistence::new(store.clone()),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    system.root().tell(Command::IncrementTwice(2, 3));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(count, 6);

    system.root().tell(Command::StopAfter(4));
    assert_terminates(system, "first incarnation did not terminate").await;

    let id = persistence_id("1");
    let events = store.inner().events(&id);
    assert_eq!(events.len(), 4);
    assert_eq!(
        events.last().map(|stored| stored.seq_no),
        Some(SeqNo::new(3)),
        "the final event must be persisted before stopping"
    );

    let system =
        ActorSystem::event_sourced(Counter::new("1", probe_tx), Persistence::new(store.clone()));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 10);

    system.root().tell(Command::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;
}

/// Without a snapshot every recovery seeds via `init`; with a snapshot `init` is skipped and
/// `init_from_snapshot` seeds instead; `recovered` runs on every recovery either way. A snapshot
/// also shortens replay: the second incarnation reads only the events after it.
#[tokio::test]
async fn snapshots_skip_init_and_shorten_replay() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("2", probe_tx.clone()).with_snapshot_every(3),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    for _ in 0..3 {
        system.root().tell(Command::Increment(1));
    }
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Counter::new("2", probe_tx).with_snapshot_every(3),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    let second_recovery = probes
        .iter()
        .skip_while(|probe| !matches!(probe, Probe::InitFromSnapshot))
        .take(2)
        .collect::<Vec<_>>();
    assert_eq!(
        second_recovery,
        [&Probe::InitFromSnapshot, &Probe::Recovered],
        "the second recovery must seed from the snapshot and still run recovered"
    );
    assert!(
        !probes.contains(&Probe::Init),
        "init must be skipped once a snapshot exists"
    );
    assert_eq!(
        store.reads(),
        [SeqNo::ZERO, SeqNo::new(3)],
        "the second recovery must replay only the events after the snapshot"
    );
}

/// Recovery pages through the stream across the replay page of 512 events. A stream of exactly
/// one page, of one event more and of exactly two pages each recover in full, and every full page
/// is followed by a read of the next one.
#[tokio::test]
async fn recovery_reads_across_the_replay_page_boundary() {
    for (entity_id, len, reads) in [
        ("32", 512, vec![0, 512]),
        ("33", 513, vec![0, 512]),
        ("34", 1_024, vec![0, 512, 1_024]),
    ] {
        let store = FaultyStore::default();
        let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

        store
            .inner()
            .append(
                &persistence_id(entity_id),
                SeqNo::ZERO,
                vec![encoded(&Increased(1)); len],
            )
            .await
            .expect("the seeded events are appended");

        let system = ActorSystem::event_sourced(
            Counter::new(entity_id, probe_tx),
            Persistence::new(store.clone()),
        );
        assert_eq!(
            ask_count(&system).await,
            len as u64,
            "a stream of {len} events must recover in full"
        );

        system.root().tell(Command::Stop);
        assert_terminates(system, "system did not terminate").await;

        assert_eq!(
            store.reads(),
            reads.into_iter().map(SeqNo::new).collect::<Vec<_>>(),
            "replay of {len} events must read every page and stop at the first short one"
        );
    }
}

/// A failure to save a snapshot never fails the actor: commands keep settling, and with no
/// snapshot stored the next recovery seeds via `init` and replays in full.
#[tokio::test]
async fn a_failed_snapshot_save_never_fails_the_actor() {
    let store = FaultyStore::default().with_failing_saves();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("8", probe_tx.clone()).with_snapshot_every(1),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    system.root().tell(Command::Increment(1));
    system.root().tell(Command::Increment(2));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Counter::new("8", probe_tx).with_snapshot_every(1),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert!(
        !probes.contains(&Probe::InitFromSnapshot),
        "with every save failing, no snapshot must exist to recover from"
    );
    assert_eq!(
        store.reads(),
        [SeqNo::ZERO, SeqNo::ZERO],
        "the second recovery must replay in full"
    );
}

/// A snapshot offered without a configured snapshot store is dropped, not a failure: commands
/// keep settling and every recovery replays in full.
#[tokio::test]
async fn an_offered_snapshot_without_a_store_is_dropped() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("9", probe_tx.clone()).with_snapshot_every(1),
        Persistence::new(store.clone()),
    );
    system.root().tell(Command::Increment(1));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(count, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Counter::new("9", probe_tx).with_snapshot_every(1),
        Persistence::new(store.clone()),
    );
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert!(
        !probes.contains(&Probe::InitFromSnapshot),
        "without a snapshot store, no snapshot must exist to recover from"
    );
    assert_eq!(
        store.reads(),
        [SeqNo::ZERO, SeqNo::ZERO],
        "the second recovery must replay in full"
    );
}

/// An effect without events saves no snapshot, even for an actor which offers one of every state:
/// two `Get`s leave the snapshot store empty. A `Get` replies from a `then` continuation, which
/// runs before `snapshot` would be called. Only the second reply hence shows that the first `Get`
/// has settled. The `Increment` after them has an event and saves one.
#[tokio::test]
async fn an_effect_without_events_saves_no_snapshot() {
    let store = FaultyStore::default();
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();
    let id = persistence_id("no-events");

    let system = ActorSystem::event_sourced(
        Counter::new("no-events", probe_tx).with_snapshot_every(1),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    ask_count(&system).await;
    ask_count(&system).await;
    assert_eq!(
        store.inner().snapshot(&id),
        None,
        "an effect without events must save no snapshot"
    );

    system.root().tell(Command::Increment(1));
    ask_count(&system).await;
    assert_eq!(
        store.inner().snapshot(&id).map(|stored| stored.next_seq_no),
        Some(SeqNo::new(1)),
        "the effect with an event must save a snapshot"
    );

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A `then` continuation runs once its events are settled and never on replay: across a failure
/// and restart, replaying the events emits no continuation probes.
#[tokio::test]
async fn continuations_never_run_on_replay() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("3", probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no settled probe").await,
        Probe::Settled(1)
    );

    system.root().tell(Command::Fail);
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no init probe after restart").await,
        Probe::Init
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe after restart").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(2));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no settled probe").await,
        Probe::Settled(3),
        "replay must restore the count without emitting settled probes"
    );

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A stale append is fenced: an event appended by another writer makes the actor's next append
/// conflict, the conflict goes through supervision, and the restarted actor replays onto the
/// winner's events instead of overwriting them; the conflicting command is consumed.
#[tokio::test]
async fn append_conflict_restarts_onto_the_winners_events() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("4", probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );

    system.root().tell(Command::Increment(1));
    loop {
        if recv(&mut probe_rx, "no settled probe").await == Probe::Settled(1) {
            break;
        }
    }

    let id = persistence_id("4");
    store
        .append(&id, SeqNo::new(1), vec![encoded(&Increased(10))])
        .await
        .expect("the interloping append must succeed");

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the conflict").await,
        Probe::Init
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe after the conflict").await,
        Probe::Recovered
    );

    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get");
    assert_eq!(count, 11);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A store failure on append, unlike a conflict, has no second writer, but takes the same path:
/// supervision restarts the actor, replay reconciles with the store, and the command whose
/// append failed is consumed, its event never appended.
#[tokio::test]
async fn an_append_store_failure_restarts_and_consumes_the_command() {
    let store = FaultyStore::default().with_append_failures(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("10", probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the failed append").await,
        Probe::Init
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe after the failed append").await,
        Probe::Recovered
    );

    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(count, 0, "the failed command's event must not be appended");

    system.root().tell(Command::Increment(2));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 2);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A continuation failing after a durable append keeps the stash requests of its command: the
/// `Get` stashed by the command is handed back at the restart and answered from the recovered
/// state, which replays the durable event.
#[tokio::test]
async fn a_failure_after_a_durable_append_keeps_the_stash_requests_of_its_command() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("deferring-then-failing", probe_tx),
        Persistence::new(FaultyStore::default()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    let deferred = system
        .root()
        .ask(TIMEOUT, |reply_to| {
            Command::IncrementAndDeferThenPanic(2, reply_to)
        })
        .await;
    assert_eq!(deferred.expect("the deferred get was dropped"), 2);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A failed append discards the stash requests of its command along with its events: the `Get`
/// stashed by the failed command is dropped, so its `ask` resolves as `NoReply`, while the one
/// stashed by a durable command is handed back by `unstash_all`.
#[tokio::test]
async fn a_failed_append_discards_the_stash_requests_of_its_command() {
    let store = FaultyStore::default().with_append_failures(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("deferring", probe_tx),
        Persistence::new(store),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    let deferred = system
        .root()
        .ask(TIMEOUT, |reply_to| Command::IncrementAndDefer(1, reply_to))
        .await;
    assert!(matches!(deferred, Err(AskError::NoReply)));
    for expected in [Probe::Handled, Probe::Init, Probe::Recovered] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the failed append").await,
            expected
        );
    }

    let root = system.root().clone();
    let deferred = spawn(async move {
        root.ask(TIMEOUT, |reply_to| Command::IncrementAndDefer(2, reply_to))
            .await
    });
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    system.root().tell(Command::Undefer);

    let deferred = deferred.await.expect("the ask task panicked");
    assert_eq!(deferred.expect("the deferred get was not answered"), 2);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A store panic on append, like a store error, is an actor failure: supervision restarts the
/// actor instead of the panic unwinding its task, replay reconciles with the store, and the
/// command whose append panicked is consumed.
#[tokio::test]
async fn an_append_store_panic_restarts_and_consumes_the_command() {
    let store = FaultyStore::default().with_append_panics(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("12", probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the panicked append").await,
        Probe::Init
    );
    assert_eq!(
        recv(
            &mut probe_rx,
            "no recovered probe after the panicked append"
        )
        .await,
        Probe::Recovered
    );

    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to first get");
    assert_eq!(
        count, 0,
        "the panicked command's event must not be appended"
    );

    system.root().tell(Command::Increment(2));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to second get");
    assert_eq!(count, 2);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A parent's stop is honored during recovery: stopping the parent of an event-sourced child
/// whose store read hangs forever aborts the replay instead of stalling termination.
#[tokio::test]
async fn a_parent_stop_aborts_a_hung_recovery() {
    let store = FaultyStore::default().with_hanging_reads(u32::MAX);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::new(Parent {
        store,
        probe_tx: probe_tx.clone(),
    });
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);

    sleep(Duration::from_millis(50)).await;
    assert!(
        probe_rx.try_recv().is_err(),
        "recovery must still be hung: no probe may follow init"
    );

    system.root().tell(());
    assert_terminates(system, "termination must not wait for the hung recovery").await;
}

/// An event-sourced child whose parent stops it before it ran never recovers: the store sees no
/// read, neither `init` nor `recovered` runs, and the parent still terminates.
#[tokio::test]
async fn an_event_sourced_child_stopped_before_it_ran_never_recovers() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::new(HastyParent {
        store: store.clone(),
        probe_tx,
    });
    system.root().tell(());
    assert_terminates(system, "the parent did not terminate").await;

    assert_eq!(store.reads(), []);
    assert_eq!(probe_rx.recv().await, None);
}

/// An event-sourced root whose system is stopped before it ran never recovers: the store sees no
/// read and neither `init` nor `recovered` runs.
#[tokio::test]
async fn an_event_sourced_root_stopped_before_it_ran_never_recovers() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("43", probe_tx),
        Persistence::new(store.clone()),
    );
    system.stop();
    assert_terminates(system, "the root did not terminate").await;

    assert_eq!(store.reads(), []);
    assert_eq!(probe_rx.recv().await, None);
}

/// A stop which arrives while recovery runs without awaiting, here a snapshot load blocking its
/// worker, is honored before `recovered`: once the load returns, the actor stops without running
/// `recovered`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_during_the_last_stretch_of_recovery_skips_recovered() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (release_tx, release_rx) = std_mpsc::channel();
    let snapshots = BlockingLoads {
        inner: InMemoryStore::default(),
        started_tx,
        release_rx: Arc::new(Mutex::new(release_rx)),
    };
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("stopped-in-recovery", probe_tx),
        Persistence::new(FaultyStore::default()).with_snapshot_store(snapshots),
    );
    recv(&mut started_rx, "the snapshot load did not start").await;
    system.stop();
    release_tx
        .send(())
        .expect("the snapshot load waits for its release");
    assert_terminates(system, "the root did not terminate").await;

    assert_eq!(probe_rx.recv().await, Some(Probe::Init));
    assert_eq!(probe_rx.recv().await, None);
}

/// A command whose events would advance the stream beyond the last position fails the actor
/// before anything is appended, even with a store which would accept the append: a saturating
/// position would have stored the event at `u64::MAX` and carried on.
#[tokio::test]
async fn a_command_beyond_the_last_position_fails_without_appending() {
    let store = LastPositionStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("last-position", probe_tx),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    assert_eq!(
        recv(&mut probe_rx, "no init from snapshot probe").await,
        Probe::InitFromSnapshot
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_terminates(system, "the overflowing command must fail the actor").await;
    assert_eq!(*store.appended.lock().expect("appended lock poisoned"), 0);
}

/// Recovery fails at an event stored at `u64::MAX`, since replay cannot advance past it, rather
/// than resuming at a position it has already applied.
#[tokio::test]
async fn recovery_fails_at_an_event_at_the_last_position() {
    let store = LastPositionStore {
        event_at_last_position: true,
        ..LastPositionStore::default()
    };
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("last-position", probe_tx),
        Persistence::new(store.clone()).with_snapshot_store(store),
    );
    assert_eq!(
        recv(&mut probe_rx, "no init from snapshot probe").await,
        Probe::InitFromSnapshot
    );
    assert_terminates(system, "the overflowing replay must fail recovery").await;
    assert!(
        probe_rx.try_recv().is_err(),
        "recovery must fail before `recovered` runs"
    );
}

/// A failure of `recovered` is an ordinary startup failure: under `Restart` the next attempt
/// runs recovery again, and once `recovered` succeeds the actor handles commands normally.
#[tokio::test]
async fn a_recovered_failure_restarts_into_a_working_actor() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("11", probe_tx).with_recovered_failures(1),
        Persistence::new(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no init probe after restart").await,
        Probe::Init
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get");
    assert_eq!(count, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// An outside stop takes effect between commands, like a parent's, hence an append already under
/// way, which answers within the store timeout, settles before the tree terminates: such a
/// shutdown can lose the acknowledgment of an event, never the event.
#[tokio::test]
async fn stopping_a_system_lets_an_append_settle() {
    let store = FaultyStore::default().with_append_delay(Duration::from_millis(50));
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("13", probe_tx),
        Persistence::new(store.clone()),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );

    // The append is under way, so this stop lands in the middle of a settlement.
    system.stop();

    assert_eq!(
        recv(&mut probe_rx, "no settled probe").await,
        Probe::Settled(1)
    );
    assert_terminates(system, "system did not terminate").await;
}

/// A stop waits for an append which never answers at most the store timeout: the append fails
/// once the timeout has passed, its command never settles, and the tree terminates.
#[tokio::test(start_paused = true)]
async fn a_stop_waits_for_a_hung_append_at_most_the_timeout() {
    let store = FaultyStore::default().with_hanging_appends(u32::MAX);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(Counter::new("hung-stop", probe_tx), bounded(&store));
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    let stopped_at = Instant::now();
    system.stop();

    assert_terminates(system, "termination must not wait beyond the store timeout").await;
    assert!(
        stopped_at.elapsed() >= STORE_TIMEOUT,
        "the stop must wait for the append until the store timeout has passed"
    );
    assert!(
        probe_rx.try_recv().is_err(),
        "the command whose append timed out must not settle"
    );
}

/// A read which exceeds the store timeout fails recovery into supervision: under `Restart` the
/// next attempt recovers, and the actor handles commands.
#[tokio::test(start_paused = true)]
async fn a_hung_read_fails_recovery_into_supervision() {
    let store = FaultyStore::default().with_hanging_reads(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();
    let started = Instant::now();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("hung-read", probe_tx),
        bounded(&store),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the timed out read").await,
        Probe::Init
    );
    assert!(
        started.elapsed() >= STORE_TIMEOUT,
        "the read must fail only once the store timeout has passed"
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(ask_count(&system).await, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A snapshot load which exceeds the store timeout fails recovery into supervision like a failed
/// load, rather than falling back to full replay: under `Restart` the next attempt loads the
/// snapshot.
#[tokio::test(start_paused = true)]
async fn a_hung_snapshot_load_fails_recovery_into_supervision() {
    let store = FaultyStore::default();
    let id = persistence_id("hung-load");
    store
        .inner()
        .save(
            &id,
            SeqNo::ZERO,
            EncodedSnapshot {
                manifest: Count::MANIFEST.into(),
                schema_version: Count::VERSION,
                payload: Cbor.encode(&Count(7)).expect("the snapshot is encodable"),
            },
        )
        .await
        .expect("the seeded snapshot is saved");
    let store = store.with_hanging_loads(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();
    let started = Instant::now();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("hung-load", probe_tx),
        bounded(&store).with_snapshot_store(store.clone()),
        restart_config(),
    );
    assert_eq!(
        recv(&mut probe_rx, "no init from snapshot probe").await,
        Probe::InitFromSnapshot
    );
    assert!(
        started.elapsed() >= STORE_TIMEOUT,
        "the load must fail only once the store timeout has passed"
    );
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );
    assert_eq!(ask_count(&system).await, 7);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// An append which exceeds the store timeout is an actor failure: under `Stop` the actor stops,
/// and its command never settles.
#[tokio::test(start_paused = true)]
async fn a_hung_append_stops_the_actor_under_stop() {
    let store = FaultyStore::default().with_hanging_appends(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system =
        ActorSystem::event_sourced(Counter::new("hung-append-stop", probe_tx), bounded(&store));
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    let handled_at = Instant::now();

    assert_terminates(system, "the timed out append must stop the actor").await;
    assert!(
        handled_at.elapsed() >= STORE_TIMEOUT,
        "the append must fail only once the store timeout has passed"
    );
    assert!(
        probe_rx.try_recv().is_err(),
        "the command whose append timed out must not settle"
    );
    assert!(
        store
            .inner()
            .events(&persistence_id("hung-append-stop"))
            .is_empty()
    );
}

/// An append which exceeds the store timeout takes the path of a failed append under `Restart`:
/// the actor restarts, replays and handles the next command, and the timed out command is
/// consumed.
#[tokio::test(start_paused = true)]
async fn a_hung_append_restarts_and_consumes_the_command_under_restart() {
    let store = FaultyStore::default().with_hanging_appends(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("hung-append-restart", probe_tx),
        bounded(&store),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    let handled_at = Instant::now();
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the timed out append").await,
        Probe::Init
    );
    assert!(
        handled_at.elapsed() >= STORE_TIMEOUT,
        "the append must fail only once the store timeout has passed"
    );
    assert_eq!(
        recv(
            &mut probe_rx,
            "no recovered probe after the timed out append"
        )
        .await,
        Probe::Recovered
    );
    assert_eq!(
        ask_count(&system).await,
        0,
        "the timed out command's event must not be appended"
    );

    system.root().tell(Command::Increment(2));
    assert_eq!(ask_count(&system).await, 2);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A timed out append is ambiguous: one whose events were stored before it stopped answering
/// fails the actor all the same, and the replay of the restarted actor picks its events up.
#[tokio::test(start_paused = true)]
async fn the_replay_picks_up_a_timed_out_append_which_was_stored() {
    let store = FaultyStore::default().with_hanging_stored_appends(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("hung-stored-append", probe_tx),
        bounded(&store),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    for expected in [Probe::Handled, Probe::Init, Probe::Recovered] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the timed out append").await,
            expected
        );
    }
    assert_eq!(
        ask_count(&system).await,
        1,
        "the replay must pick up the stored events of the timed out append"
    );

    system.root().tell(Command::Increment(2));
    assert_eq!(ask_count(&system).await, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A snapshot save which exceeds the store timeout is logged like a failed save and never fails
/// the actor: its command settles once the timeout has passed, and the next one settles, too.
#[tokio::test(start_paused = true)]
async fn a_hung_snapshot_save_never_fails_the_actor() {
    let store = FaultyStore::default().with_hanging_saves(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("hung-save", probe_tx).with_snapshot_every(1),
        bounded(&store).with_snapshot_store(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    let handled_at = Instant::now();
    system.root().tell(Command::Increment(2));
    assert_eq!(
        recv(&mut probe_rx, "no settled probe").await,
        Probe::Settled(1)
    );
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled,
        "the next command must be handled only once the save has timed out"
    );
    assert!(
        handled_at.elapsed() >= STORE_TIMEOUT,
        "the save must be given up only once the store timeout has passed"
    );
    assert_eq!(
        recv(&mut probe_rx, "no settled probe").await,
        Probe::Settled(3)
    );
    assert_eq!(ask_count(&system).await, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert!(
        !probes.contains(&Probe::Init),
        "the timed out save must not restart the actor"
    );
}

/// Writes are strict: a command is fully settled, its continuations included, before the next one
/// is handled, even while the append itself is slow.
#[tokio::test]
async fn a_command_settles_before_the_next_is_handled() {
    let store = FaultyStore::default().with_append_delay(Duration::from_millis(20));
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system =
        ActorSystem::event_sourced(Counter::new("5", probe_tx), Persistence::new(store.clone()));
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    system.root().tell(Command::Increment(2));

    let probes = [
        recv(&mut probe_rx, "no first probe").await,
        recv(&mut probe_rx, "no second probe").await,
        recv(&mut probe_rx, "no third probe").await,
        recv(&mut probe_rx, "no fourth probe").await,
    ];
    assert_eq!(
        probes,
        [
            Probe::Handled,
            Probe::Settled(1),
            Probe::Handled,
            Probe::Settled(3)
        ]
    );

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A watched actor's termination is delivered to `handle` as an ordinary incoming signal.
#[tokio::test]
async fn terminated_signals_reach_handle() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Watcher {
            probe_tx,
            bye_before_stopping: false,
        },
        Persistence::new(FaultyStore::default()),
    );

    system.root().tell(WatcherCommand::StopChild);
    assert_eq!(
        recv(&mut probe_rx, "no terminated probe").await,
        Probe::Terminated
    );

    assert_terminates(system, "system did not terminate").await;
}

/// After `unwatch` no terminated signal is received, even if it is already enqueued. The child
/// says bye through the shared FIFO right before stopping, and the watcher unwatches on the bye.
/// The signal queued behind it is dropped before `handle` ever sees it.
#[tokio::test]
async fn unwatch_drops_an_enqueued_terminated_signal() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Watcher {
            probe_tx,
            bye_before_stopping: true,
        },
        Persistence::new(FaultyStore::default()),
    );

    system.root().tell(WatcherCommand::StopChild);
    assert_eq!(recv(&mut probe_rx, "no bye probe").await, Probe::Bye);

    sleep(Duration::from_millis(100)).await;
    system.root().tell(WatcherCommand::Stop);
    assert_terminates(system, "system did not terminate").await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert!(
        !probes.contains(&Probe::Terminated),
        "no terminated signal must be received after unwatch"
    );
}

/// An undecodable snapshot is not a failure: it is discarded, recovery falls back to full
/// replay seeded by `init`, and the state still comes out right.
#[tokio::test]
async fn undecodable_snapshot_falls_back_to_full_replay() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let id = persistence_id("7");
    store
        .inner()
        .append(
            &id,
            SeqNo::ZERO,
            vec![encoded(&Increased(1)), encoded(&Increased(2))],
        )
        .await
        .expect("the seeded events are appended");
    store
        .inner()
        .save(
            &id,
            SeqNo::new(2),
            EncodedSnapshot {
                manifest: Count::MANIFEST.into(),
                schema_version: SchemaVersion::new(99),
                payload: Vec::new(),
            },
        )
        .await
        .expect("the seeded snapshot is saved");

    let system = ActorSystem::event_sourced(
        Counter::new("7", probe_tx),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A history the current code cannot decode is a recovery failure: under the default `Stop`
/// strategy the actor stops without ever running `recovered`, and its watchers learn about it.
#[tokio::test]
async fn undecodable_history_stops_the_actor() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let id = persistence_id("6");
    store
        .inner()
        .append(
            &id,
            SeqNo::ZERO,
            vec![EncodedEvent {
                manifest: Increased::MANIFEST.into(),
                schema_version: SchemaVersion::new(99),
                payload: Vec::new(),
            }],
        )
        .await
        .expect("the seeded event is appended");

    let system =
        ActorSystem::event_sourced(Counter::new("6", probe_tx), Persistence::new(store.clone()));
    assert_terminates(system, "the actor must stop on an undecodable history").await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert_eq!(
        probes,
        [Probe::Init],
        "recovery must fail after init and before recovered"
    );
}

/// Decoding is handed the stored schema version, not the type's own: a snapshot of the current
/// shape stored under a version the code does not know is discarded, and recovery falls back to
/// full replay.
#[tokio::test]
async fn a_snapshot_at_an_unknown_version_is_discarded_although_its_payload_decodes() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let id = persistence_id("unknown-snapshot-version");
    store
        .inner()
        .append(
            &id,
            SeqNo::ZERO,
            vec![encoded(&Increased(1)), encoded(&Increased(2))],
        )
        .await
        .expect("the seeded events are appended");
    store
        .inner()
        .save(
            &id,
            SeqNo::new(2),
            EncodedSnapshot {
                manifest: Count::MANIFEST.into(),
                schema_version: SchemaVersion::new(2),
                payload: Cbor.encode(&Count(100)).expect("the snapshot is encodable"),
            },
        )
        .await
        .expect("the seeded snapshot is saved");

    let system = ActorSystem::event_sourced(
        Counter::new("unknown-snapshot-version", probe_tx),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get");
    assert_eq!(count, 3);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// Decoding is handed the stored schema version, not the type's own: an event of the current shape
/// stored under a version the code does not know fails recovery.
#[tokio::test]
async fn an_event_at_an_unknown_version_stops_the_actor_although_its_payload_decodes() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let id = persistence_id("unknown-event-version");
    store
        .inner()
        .append(
            &id,
            SeqNo::ZERO,
            vec![EncodedEvent {
                manifest: Increased::MANIFEST.into(),
                schema_version: SchemaVersion::new(2),
                payload: Cbor.encode(&Increased(1)).expect("the event is encodable"),
            }],
        )
        .await
        .expect("the seeded event is appended");

    let system = ActorSystem::event_sourced(
        Counter::new("unknown-event-version", probe_tx),
        Persistence::new(store.clone()),
    );
    assert_terminates(
        system,
        "the actor must stop on an event at an unknown version",
    )
    .await;

    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    assert_eq!(
        probes,
        [Probe::Init],
        "recovery must fail after init and before recovered"
    );
}

/// A codec outside this crate wraps its serializer's error through the public constructors, which
/// is what makes the `Codec` trait implementable at all; the wrapped error is the source.
#[test]
fn a_custom_codec_wraps_its_own_errors() {
    let encode_error = Failing
        .encode(&Increased(1))
        .expect_err("the failing codec never encodes");
    assert!(
        encode_error
            .source()
            .is_some_and(|source| source.is::<Boom>()),
        "got {encode_error:?}"
    );

    let payload_error = Failing
        .decode::<Increased>(&[])
        .expect_err("the failing codec never decodes");
    assert!(
        payload_error
            .source()
            .is_some_and(|source| source.is::<Boom>()),
        "got {payload_error:?}"
    );
}

fn persistence_id(entity_id: &str) -> PersistenceId {
    PersistenceId::new("counter", entity_id).expect("the segments are valid")
}

fn encoded(event: &Increased) -> EncodedEvent {
    EncodedEvent {
        manifest: Increased::MANIFEST.into(),
        schema_version: Increased::VERSION,
        payload: Cbor.encode(event).expect("the event is encodable"),
    }
}

/// A store error whose `Display` and destructor panic, failing an append, is contained like any
/// other failure: the actor restarts, and formatting and dropping the error each panic in their own
/// unwind.
#[tokio::test]
async fn a_panicking_append_error_is_contained() {
    init_tracing();
    let store = FaultyStore::default()
        .with_append_failures(1)
        .with_panicking_errors();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("19", probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    assert_eq!(recv(&mut probe_rx, "no init probe").await, Probe::Init);
    assert_eq!(
        recv(&mut probe_rx, "no recovered probe").await,
        Probe::Recovered
    );

    system.root().tell(Command::Increment(1));
    assert_eq!(
        recv(&mut probe_rx, "no handled probe").await,
        Probe::Handled
    );
    assert_eq!(
        recv(&mut probe_rx, "no init probe after the failed append").await,
        Probe::Init
    );
    assert!(
        store.error_hooks().displayed.load(SeqCst),
        "the error was not displayed"
    );
    assert!(
        store.error_hooks().dropped.load(SeqCst),
        "the error was not dropped"
    );

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A failed snapshot save is only a warning, also when the store error's `Display` and destructor
/// panic: the actor keeps handling commands without restarting.
#[tokio::test]
async fn a_panicking_snapshot_save_error_keeps_the_actor_running() {
    init_tracing();
    let store = FaultyStore::default()
        .with_failing_saves()
        .with_panicking_errors();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("20", probe_tx).with_snapshot_every(1),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
    );
    system.root().tell(Command::Increment(1));
    system.root().tell(Command::Increment(2));
    let count = system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get");
    assert_eq!(count, 3);
    assert!(
        store.error_hooks().displayed.load(SeqCst),
        "the error was not displayed"
    );
    assert!(
        store.error_hooks().dropped.load(SeqCst),
        "the error was not dropped"
    );

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;

    let mut inits = 0;
    while let Ok(probe) = probe_rx.try_recv() {
        if probe == Probe::Init {
            inits += 1;
        }
    }
    assert_eq!(inits, 1, "the actor must not restart");
}

/// A timer started in `handle` is received by an event-sourced actor like a command once its delay
/// has elapsed: here its message persists an event.
#[tokio::test(start_paused = true)]
async fn an_event_sourced_actor_receives_its_timer() {
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        TimedCounter::new("21", probe_tx),
        Persistence::new(FaultyStore::default()),
    );

    system.root().tell(Command::StartTimer);
    assert_eq!(ask_count(&system).await, 0);

    advance(Duration::from_secs(1)).await;
    // Only the second get is sure to follow the due timer.
    ask_count(&system).await;
    assert_eq!(ask_count(&system).await, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// The modes of parent and child are independent: a timed event-sourced child of an untimed
/// parent receives its timer.
#[tokio::test(start_paused = true)]
async fn an_untimed_parent_spawns_a_timed_event_sourced_child() {
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();
    let (child_tx, mut children) = mpsc::unbounded_channel();
    let system = ActorSystem::new(UntimedParent {
        store: FaultyStore::default(),
        probe_tx,
        child_tx,
    });

    let child = recv(&mut children, "child not spawned").await;
    assert_eq!(ask_child_count(&child).await, 0);

    advance(Duration::from_secs(1)).await;
    // Only the second get is sure to follow the due timer.
    ask_child_count(&child).await;
    assert_eq!(ask_child_count(&child).await, 1);

    system.root().tell(());
    assert_terminates(system, "system did not terminate").await;
}

/// The modes of parent and child are independent: an untimed event-sourced child of a timed
/// parent handles its commands.
#[tokio::test]
async fn a_timed_parent_spawns_an_untimed_event_sourced_child() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();
    let (child_tx, mut children) = mpsc::unbounded_channel();
    let system = ActorSystem::new(TimedParent {
        store: FaultyStore::default(),
        probe_tx,
        child_tx,
    });

    let child = recv(&mut children, "child not spawned").await;
    child.tell(WatcherCommand::StopChild);
    assert_eq!(
        recv(&mut probe_rx, "child did not see its child stop").await,
        Probe::Terminated
    );

    system.root().tell(());
    assert_terminates(system, "system did not terminate").await;
}

/// A timer started in `recovered`, before the first command, is received like one started in
/// `handle`: here its message persists an event.
#[tokio::test(start_paused = true)]
async fn an_event_sourced_actor_receives_a_timer_started_in_recovered() {
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        TimedCounter::new("23", probe_tx).with_timer_on_recovery(),
        Persistence::new(FaultyStore::default()),
    );

    assert_eq!(ask_count(&system).await, 0);

    advance(Duration::from_secs(1)).await;
    // Only the second get is sure to follow the due timer.
    ask_count(&system).await;
    assert_eq!(ask_count(&system).await, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A restart drops an event-sourced actor's timers: they are runtime state, neither persisted nor
/// recreated by replay. Here `recovered` does not start "t" again, so the next incarnation never
/// receives it.
#[tokio::test(start_paused = true)]
async fn a_restart_drops_an_event_sourced_actors_timer() {
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        TimedCounter::new("22", probe_tx),
        Persistence::new(FaultyStore::default()),
        restart_config(),
    );

    system.root().tell(Command::StartTimer);
    system.root().tell(Command::Fail);
    assert_eq!(ask_count(&system).await, 0);

    advance(Duration::from_secs(1)).await;
    // Only the second get is sure to come behind a due timer which the restart kept.
    ask_count(&system).await;
    assert_eq!(ask_count(&system).await, 0);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// All events of one effect go into one append, which a conforming store makes atomic: the two
/// events of `IncrementTwice` are one call, the single event of `Increment` another.
#[tokio::test]
async fn all_events_of_an_effect_go_into_one_append() {
    let store = FaultyStore::default();
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("one-append", probe_tx),
        Persistence::new(store.clone()),
    );
    system.root().tell(Command::IncrementTwice(2, 3));
    system.root().tell(Command::Increment(1));
    assert_eq!(ask_count(&system).await, 6);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
    assert_eq!(store.appends(), [2, 1]);
}

/// A panic in `handle` is an actor failure: supervision restarts the actor instead of the panic
/// unwinding its task, and the command is consumed without an event.
#[tokio::test]
async fn a_panic_in_handle_restarts_and_consumes_the_command() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Fragile::new("handle-panics", Fault::HandlePanics, probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    system.root().tell(Command::Increment(1));
    for expected in [
        Probe::Init,
        Probe::Recovered,
        Probe::Handled,
        Probe::Init,
        Probe::Recovered,
    ] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the panic").await,
            expected
        );
    }
    assert_eq!(ask_count(&system).await, 0);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
    assert!(
        store
            .inner()
            .events(&persistence_id("handle-panics"))
            .is_empty()
    );
}

/// A panic in `apply` after the append is an actor failure, here under `Stop`, and the event stays
/// durable: the actor terminates without settling the command, and the store holds the event.
#[tokio::test]
async fn a_panic_in_apply_fails_the_actor_with_its_event_durable() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Fragile::new("apply-panics", Fault::ApplyPanics, probe_tx),
        Persistence::new(store.clone()),
    );
    system.root().tell(Command::Increment(1));
    assert_terminates(system, "the panic in apply must stop the actor").await;

    assert_eq!(
        received(&mut probe_rx),
        [Probe::Init, Probe::Recovered, Probe::Handled]
    );
    assert_eq!(
        store.inner().events(&persistence_id("apply-panics")).len(),
        1
    );
}

/// A panicking continuation fails the actor. Its event stays durable, and the continuation after
/// it is lost: the restarted actor replays the event, while `Settled` is never probed.
#[tokio::test]
async fn a_panicking_continuation_fails_the_actor_and_loses_the_later_ones() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Fragile::new("then-panics", Fault::ThenPanics, probe_tx),
        Persistence::new(store.clone()),
        restart_config(),
    );
    system.root().tell(Command::Increment(1));
    for expected in [
        Probe::Init,
        Probe::Recovered,
        Probe::Handled,
        Probe::Init,
        Probe::Recovered,
    ] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the panic").await,
            expected
        );
    }
    assert_eq!(ask_count(&system).await, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
    assert!(!received(&mut probe_rx).contains(&Probe::Settled(1)));
}

/// A failure to encode an event is an actor failure: supervision restarts the actor, and nothing
/// is appended.
#[tokio::test]
async fn an_event_encode_failure_restarts_and_appends_nothing() {
    let store = FaultyStore::default();
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("unencodable", probe_tx),
        Persistence::new(store.clone()).with_codec(Failing),
        restart_config(),
    );
    system.root().tell(Command::Increment(1));
    for expected in [
        Probe::Init,
        Probe::Recovered,
        Probe::Handled,
        Probe::Init,
        Probe::Recovered,
    ] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the encode failure").await,
            expected
        );
    }

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
    assert!(store.appends().is_empty());
}

/// A failing or panicking `snapshot` is a failed snapshot save, which never fails the actor:
/// commands keep settling, and the actor never restarts.
#[tokio::test]
async fn a_failing_snapshot_never_fails_the_actor() {
    for (entity_id, fault) in [
        ("snapshot-fails", Fault::SnapshotFails),
        ("snapshot-panics", Fault::SnapshotPanics),
    ] {
        let store = FaultyStore::default();
        let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

        let system = ActorSystem::event_sourced_with_config(
            Fragile::new(entity_id, fault, probe_tx),
            Persistence::new(store.clone()).with_snapshot_store(store.clone()),
            restart_config(),
        );
        system.root().tell(Command::Increment(1));
        system.root().tell(Command::Increment(2));
        assert_eq!(ask_count(&system).await, 3, "{fault:?}");

        system.root().tell(Command::Stop);
        assert_terminates(system, "system did not terminate").await;
        assert_eq!(
            received(&mut probe_rx)
                .iter()
                .filter(|probe| **probe == Probe::Init)
                .count(),
            1,
            "{fault:?} must not restart the actor"
        );
    }
}

/// A failing `init` is a deterministic recovery failure: every restart fails identically, so it
/// escalates through the restart limit of five to a stop.
#[tokio::test]
async fn a_failing_init_escalates_through_the_restart_limit() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Fragile::new("init-fails", Fault::InitFails, probe_tx),
        Persistence::new(FaultyStore::default()),
        restart_config(),
    );
    assert_terminates(system, "the failing init must escalate to a stop").await;
    assert_eq!(received(&mut probe_rx), [Probe::Init; 6]);
}

/// A failing `init_from_snapshot` escalates through the restart limit like a failing `init`,
/// rather than falling back to `init` and a full replay as an undecodable snapshot does.
#[tokio::test]
async fn a_failing_init_from_snapshot_escalates_without_replaying_in_full() {
    let store = FaultyStore::default();
    seed_snapshot(&store, "init-from-snapshot-fails", Count(7)).await;
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Fragile::new(
            "init-from-snapshot-fails",
            Fault::InitFromSnapshotFails,
            probe_tx,
        ),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
        restart_config(),
    );
    assert_terminates(
        system,
        "the failing init_from_snapshot must escalate to a stop",
    )
    .await;
    assert_eq!(received(&mut probe_rx), [Probe::InitFromSnapshot; 6]);
    assert_eq!(store.reads(), []);
}

/// A panic in `apply` during replay is a deterministic recovery failure: it escalates through the
/// restart limit to a stop instead of unwinding the actor's task, and `recovered` never runs.
#[tokio::test]
async fn a_panic_in_apply_during_replay_escalates_through_the_restart_limit() {
    let store = FaultyStore::default();
    store
        .inner()
        .append(
            &persistence_id("replay-panics"),
            SeqNo::ZERO,
            vec![encoded(&Increased(1))],
        )
        .await
        .expect("the seeded event is appended");
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Fragile::new("replay-panics", Fault::ApplyPanics, probe_tx),
        Persistence::new(store),
        restart_config(),
    );
    assert_terminates(system, "the panic in replay must escalate to a stop").await;
    assert_eq!(received(&mut probe_rx), [Probe::Init; 6]);
}

/// A stored event whose sequence number is not the next one expected is a deterministic recovery
/// failure: it escalates through the restart limit to a stop, and `recovered` never runs.
#[tokio::test]
async fn a_sequence_gap_escalates_through_the_restart_limit() {
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("gap", probe_tx),
        Persistence::new(GapStore),
        restart_config(),
    );
    assert_terminates(system, "the gap must escalate to a stop").await;
    assert_eq!(received(&mut probe_rx), [Probe::Init; 6]);
}

/// A failing read is an ordinary startup failure: under `Restart` the next attempt recovers, and
/// the actor handles commands.
#[tokio::test]
async fn a_failing_read_fails_recovery_into_supervision() {
    let store = FaultyStore::default().with_failing_reads(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("failing-read", probe_tx),
        Persistence::new(store),
        restart_config(),
    );
    for expected in [Probe::Init, Probe::Init, Probe::Recovered] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the failed read").await,
            expected
        );
    }
    system.root().tell(Command::Increment(1));
    assert_eq!(ask_count(&system).await, 1);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// A failing snapshot load fails recovery into supervision rather than being traded for full
/// replay: the failed attempt neither seeds via `init` nor reads, and the next attempt loads the
/// snapshot.
#[tokio::test]
async fn a_failing_snapshot_load_fails_recovery_instead_of_replaying_in_full() {
    let store = FaultyStore::default();
    seed_snapshot(&store, "failing-load", Count(7)).await;
    let store = store.with_failing_loads(1);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("failing-load", probe_tx),
        Persistence::new(store.clone()).with_snapshot_store(store.clone()),
        restart_config(),
    );
    for expected in [Probe::InitFromSnapshot, Probe::Recovered] {
        assert_eq!(
            recv(&mut probe_rx, "no probe around the failed load").await,
            expected
        );
    }
    assert_eq!(ask_count(&system).await, 7);
    assert_eq!(store.reads(), [SeqNo::ZERO]);

    system.root().tell(Command::Stop);
    assert_terminates(system, "system did not terminate").await;
}

/// With `Persistence::with_codec(Json)` the stored payloads are readable JSON, and the next
/// incarnation decodes them back into the live state.
#[tokio::test]
async fn json_payloads_are_readable_and_replay_into_the_live_state() {
    let store = FaultyStore::default();
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Counter::new("json", probe_tx.clone()),
        Persistence::new(store.clone()).with_codec(Json),
    );
    system.root().tell(Command::Increment(1));
    system.root().tell(Command::IncrementTwice(2, 3));
    assert_eq!(ask_count(&system).await, 6);
    system.root().tell(Command::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let payloads = store
        .inner()
        .events(&persistence_id("json"))
        .into_iter()
        .map(|stored| String::from_utf8(stored.event.payload).expect("a JSON payload is text"))
        .collect::<Vec<_>>();
    assert_eq!(payloads, ["1", "2", "3"]);

    let system = ActorSystem::event_sourced(
        Counter::new("json", probe_tx),
        Persistence::new(store).with_codec(Json),
    );
    assert_eq!(ask_count(&system).await, 6);
    system.root().tell(Command::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;
}

/// An event nested 127 levels deep reads back with `Json`, one nested 128 levels deep is stored
/// all the same, and then every recovery fails on it.
#[tokio::test]
async fn an_event_nested_beyond_the_json_limit_is_stored_and_fails_every_recovery() {
    let store = FaultyStore::default();
    let persistence = Persistence::new(store.clone()).with_codec(Json);
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Holder::<Value>::new("deep-event", probe_tx.clone()),
        persistence.clone(),
    );
    assert_eq!(ask_held(&system, Some(nested(127))).await, nested(127));
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Holder::<Value>::new("deep-event", probe_tx.clone()),
        persistence.clone(),
    );
    assert_eq!(ask_held(&system, None).await, nested(127));
    assert_eq!(ask_held(&system, Some(nested(128))).await, nested(128));
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;
    assert_eq!(
        store.inner().events(&persistence_id("deep-event")).len(),
        2,
        "the deeper event must be stored"
    );

    received(&mut probe_rx);
    for _ in 0..2 {
        let system = ActorSystem::event_sourced(
            Holder::<Value>::new("deep-event", probe_tx.clone()),
            persistence.clone(),
        );
        assert_terminates(system, "the deeper event must fail recovery").await;
        assert_eq!(received(&mut probe_rx), [Probe::Init]);
    }
}

/// A snapshot nested 127 levels deep reads back with `Json`, while one nested 128 levels deep is
/// discarded at every recovery, which then replays in full.
#[tokio::test]
async fn a_snapshot_nested_beyond_the_json_limit_is_discarded_at_every_recovery() {
    for (depth, entity_id, seed) in [
        (127, "snapshot-127", Probe::InitFromSnapshot),
        (128, "snapshot-128", Probe::Init),
    ] {
        let store = FaultyStore::default();
        let persistence = Persistence::new(store.clone())
            .with_snapshot_store(store.clone())
            .with_codec(Json);
        let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

        let system = ActorSystem::event_sourced(
            Holder::<Value>::new(entity_id, probe_tx.clone()).with_snapshot(nested(depth)),
            persistence.clone(),
        );
        assert_eq!(ask_held(&system, Some(nested(1))).await, nested(1));
        system.root().tell(HolderCommand::Stop);
        assert_terminates(system, "first incarnation did not terminate").await;

        received(&mut probe_rx);
        for _ in 0..2 {
            let system = ActorSystem::event_sourced(
                Holder::<Value>::new(entity_id, probe_tx.clone()),
                persistence.clone(),
            );
            let held = ask_held(&system, None).await;
            system.root().tell(HolderCommand::Stop);
            assert_terminates(system, "a later incarnation did not terminate").await;

            assert_eq!(received(&mut probe_rx), [seed, Probe::Recovered]);
            let expected = if seed == Probe::Init {
                nested(1)
            } else {
                nested(depth)
            };
            assert_eq!(held, expected, "at depth {depth}");
        }
    }
}

/// An event nested 256 levels deep reads back with the default `Cbor`, one nested 257 levels deep
/// is stored all the same, and then every recovery fails on it.
#[tokio::test]
async fn an_event_nested_beyond_the_cbor_limit_is_stored_and_fails_every_recovery() {
    let store = FaultyStore::default();
    let persistence = Persistence::new(store.clone());
    let (probe_tx, mut probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Holder::<Value>::new("cbor-limit", probe_tx.clone()),
        persistence.clone(),
    );
    assert_eq!(ask_held(&system, Some(nested(256))).await, nested(256));
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Holder::<Value>::new("cbor-limit", probe_tx.clone()),
        persistence.clone(),
    );
    assert_eq!(ask_held(&system, None).await, nested(256));
    assert_eq!(ask_held(&system, Some(nested(257))).await, nested(257));
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;
    assert_eq!(
        store.inner().events(&persistence_id("cbor-limit")).len(),
        2,
        "the deeper event must be stored"
    );

    received(&mut probe_rx);
    for _ in 0..2 {
        let system = ActorSystem::event_sourced(
            Holder::<Value>::new("cbor-limit", probe_tx.clone()),
            persistence.clone(),
        );
        assert_terminates(system, "the deeper event must fail recovery").await;
        assert_eq!(received(&mut probe_rx), [Probe::Init]);
    }
}

/// Neither codec preserves `Some(None)`: the live state holds it, while the state recovered from
/// the very event holds `None`, without any error.
#[tokio::test]
async fn some_none_reads_back_as_none() {
    assert_some_none_reads_back_as_none(Cbor, "some-none-cbor").await;
    assert_some_none_reads_back_as_none(Json, "some-none-json").await;
}

/// Global, as a thread-local subscriber races with the callsite interest cache of parallel tests.
fn init_tracing() {
    TRACING.call_once(|| {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt().with_test_writer().finish(),
        )
        .expect("no other global subscriber is set");
    });
}

async fn recv<T>(probe_rx: &mut mpsc::UnboundedReceiver<T>, not_received: &str) -> T {
    timeout(TIMEOUT, probe_rx.recv())
        .await
        .expect(not_received)
        .expect("probe channel closed")
}

async fn assert_terminates<M>(system: ActorSystem<M>, not_terminated: &str)
where
    M: Send + 'static,
{
    timeout(TIMEOUT, system.terminated())
        .await
        .expect(not_terminated)
        .expect("watching the root actor failed");
}

/// Round-trip a `Get`, so the actor has handled everything told before.
async fn ask_count(system: &ActorSystem<Command>) -> u64 {
    system
        .root()
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get")
}

async fn ask_child_count(child: &ActorRef<Command>) -> u64 {
    child
        .ask(TIMEOUT, Command::Get)
        .await
        .expect("no reply to get")
}

/// Every probe received so far.
fn received(probe_rx: &mut mpsc::UnboundedReceiver<Probe>) -> Vec<Probe> {
    let mut probes = Vec::new();
    while let Ok(probe) = probe_rx.try_recv() {
        probes.push(probe);
    }
    probes
}

/// Save a snapshot of the given count for the given entity, from which replay resumes at 0.
async fn seed_snapshot(store: &FaultyStore, entity_id: &str, count: Count) {
    store
        .inner()
        .save(
            &persistence_id(entity_id),
            SeqNo::ZERO,
            EncodedSnapshot {
                manifest: Count::MANIFEST.into(),
                schema_version: Count::VERSION,
                payload: Cbor.encode(&count).expect("the snapshot is encodable"),
            },
        )
        .await
        .expect("the seeded snapshot is saved");
}

/// Set the held value, if one is given, and round-trip a `Get` for it.
async fn ask_held<T>(system: &ActorSystem<HolderCommand<T>>, value: Option<T>) -> T
where
    T: Send + 'static,
{
    if let Some(value) = value {
        system.root().tell(HolderCommand::Set(value));
    }
    system
        .root()
        .ask(TIMEOUT, HolderCommand::Get)
        .await
        .expect("no reply to get")
}

/// A JSON value of nested arrays, `depth` levels deep.
fn nested(depth: usize) -> Value {
    (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]))
}

async fn assert_some_none_reads_back_as_none<C>(codec: C, entity_id: &'static str)
where
    C: Codec + Clone + Send + Sync + 'static,
{
    let persistence = Persistence::new(FaultyStore::default()).with_codec(codec);
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced(
        Holder::<Option<Option<u32>>>::new(entity_id, probe_tx.clone()),
        persistence.clone(),
    );
    assert_eq!(ask_held(&system, Some(Some(None))).await, Some(None));
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "first incarnation did not terminate").await;

    let system = ActorSystem::event_sourced(
        Holder::<Option<Option<u32>>>::new(entity_id, probe_tx),
        persistence,
    );
    assert_eq!(ask_held(&system, None).await, None);
    system.root().tell(HolderCommand::Stop);
    assert_terminates(system, "second incarnation did not terminate").await;
}

/// The wiring over the given store, its calls bounded by [STORE_TIMEOUT].
fn bounded(store: &FaultyStore) -> Persistence<FaultyStore> {
    Persistence::new(store.clone())
        .with_timeout(STORE_TIMEOUT)
        .expect("the store timeout is not zero")
}

fn restart_config() -> ActorConfig {
    ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(NonZeroU32::new(5).expect("5 is not zero")).with_backoff(
            Backoff::new(Duration::from_millis(1), Duration::from_millis(10))
                .expect("the bounds are valid"),
        ),
    ))
}

struct Counter {
    entity_id: &'static str,
    probe_tx: mpsc::UnboundedSender<Probe>,
    snapshot_every: Option<u64>,
    recovered_failures: Mutex<u32>,
}

impl Counter {
    fn new(entity_id: &'static str, probe_tx: mpsc::UnboundedSender<Probe>) -> Self {
        Self {
            entity_id,
            probe_tx,
            snapshot_every: None,
            recovered_failures: Mutex::new(0),
        }
    }

    fn with_snapshot_every(mut self, snapshot_every: u64) -> Self {
        self.snapshot_every = Some(snapshot_every);
        self
    }

    fn with_recovered_failures(mut self, failures: u32) -> Self {
        self.recovered_failures = Mutex::new(failures);
        self
    }

    /// Fails as often as configured via [Counter::with_recovered_failures], then succeeds.
    fn recovered_failure(&self) -> Result<(), Boom> {
        let mut failures = self
            .recovered_failures
            .lock()
            .expect("recovered failures lock poisoned");
        if *failures > 0 {
            *failures -= 1;
            return Err(Boom);
        }
        Ok(())
    }
}

impl EventSourced for Counter {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Stashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Boom;

    fn persistence_id(&self) -> PersistenceId {
        persistence_id(self.entity_id)
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::Init);
        Ok(0)
    }

    fn init_from_snapshot(&self, Count(count): Self::Snapshot) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::InitFromSnapshot);
        Ok(count)
    }

    fn recovered(
        &self,
        _: &ActorContext<Self::Command, Untimed, Stashed>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        self.recovered_failure()?;
        let _ = self.probe_tx.send(Probe::Recovered);
        Ok(state)
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command, Untimed, Stashed>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            return Ok(Effect::none());
        };
        let _ = self.probe_tx.send(Probe::Handled);

        match command {
            Command::IncrementAndDefer(n, reply_to) => {
                context
                    .stash(Incoming::Message(Command::Get(reply_to)))
                    .expect("the stash is unbounded");
                Ok(Effect::persist(Increased(n)))
            }

            Command::IncrementAndDeferThenPanic(n, reply_to) => {
                context
                    .stash(Incoming::Message(Command::Get(reply_to)))
                    .expect("the stash is unbounded");
                Ok(
                    Effect::persist(Increased(n))
                        .then(|_| panic!("continuation panics on purpose")),
                )
            }

            Command::Undefer => {
                context.unstash_all();
                Ok(Effect::none())
            }

            command => counter_effect(&self.probe_tx, command),
        }
    }

    fn apply(&self, state: Self::State, Increased(n): Self::Event) -> Self::State {
        state + n
    }

    fn snapshot(&self, state: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        match self.snapshot_every {
            Some(every) if state % every == 0 => Ok(Some(Count(*state))),
            _ => Ok(None),
        }
    }
}

/// A [Counter] declaring timers: it starts the timer "t" on [Command::StartTimer] and, if
/// configured, in `recovered`.
struct TimedCounter {
    counter: Counter,
    timer_on_recovery: bool,
}

impl TimedCounter {
    fn new(entity_id: &'static str, probe_tx: mpsc::UnboundedSender<Probe>) -> Self {
        Self {
            counter: Counter::new(entity_id, probe_tx),
            timer_on_recovery: false,
        }
    }

    fn with_timer_on_recovery(mut self) -> Self {
        self.timer_on_recovery = true;
        self
    }
}

impl EventSourced for TimedCounter {
    type Command = Command;
    type Timers = Timed;
    type Stash = Unstashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Boom;

    fn persistence_id(&self) -> PersistenceId {
        self.counter.persistence_id()
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        self.counter.init()
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        self.counter.init_from_snapshot(snapshot)
    }

    fn recovered(
        &self,
        context: &ActorContext<Self::Command, Timed>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        self.counter.recovered_failure()?;
        if self.timer_on_recovery {
            context.start_timer("t", Command::Increment(1), Duration::from_secs(1));
        }
        let _ = self.counter.probe_tx.send(Probe::Recovered);
        Ok(state)
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command, Timed>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            return Ok(Effect::none());
        };
        let _ = self.counter.probe_tx.send(Probe::Handled);

        if let Command::StartTimer = command {
            context.start_timer("t", Command::Increment(1), Duration::from_secs(1));
            return Ok(Effect::none());
        }
        counter_effect(&self.counter.probe_tx, command)
    }

    fn apply(&self, state: Self::State, event: Self::Event) -> Self::State {
        self.counter.apply(state, event)
    }

    fn snapshot(&self, state: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        self.counter.snapshot(state)
    }
}

/// A counter which fails at the step its [Fault] names, every time it gets there.
struct Fragile {
    entity_id: &'static str,
    fault: Fault,
    probe_tx: mpsc::UnboundedSender<Probe>,
}

impl Fragile {
    fn new(entity_id: &'static str, fault: Fault, probe_tx: mpsc::UnboundedSender<Probe>) -> Self {
        Self {
            entity_id,
            fault,
            probe_tx,
        }
    }
}

impl EventSourced for Fragile {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Boom;

    fn persistence_id(&self) -> PersistenceId {
        persistence_id(self.entity_id)
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::Init);
        if self.fault == Fault::InitFails {
            return Err(Boom);
        }
        Ok(0)
    }

    fn init_from_snapshot(&self, Count(count): Self::Snapshot) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::InitFromSnapshot);
        if self.fault == Fault::InitFromSnapshotFails {
            return Err(Boom);
        }
        Ok(count)
    }

    fn recovered(
        &self,
        _: &ActorContext<Self::Command>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::Recovered);
        Ok(state)
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            return Ok(Effect::none());
        };

        match command {
            Command::Increment(n) => {
                let _ = self.probe_tx.send(Probe::Handled);
                if self.fault == Fault::HandlePanics {
                    panic!("handle panics on purpose");
                }

                let mut effect = Effect::persist(Increased(n));
                if self.fault == Fault::ThenPanics {
                    effect = effect.then(|_| panic!("a continuation panics on purpose"));
                }
                let probe_tx = self.probe_tx.clone();
                Ok(effect.then(move |count| {
                    let _ = probe_tx.send(Probe::Settled(*count));
                }))
            }

            Command::Get(reply_to) => Ok(Effect::none().then(move |count| reply_to.reply(*count))),

            Command::Stop => Ok(Effect::stop()),

            _ => unreachable!("a fragile counter is told to increment, get and stop only"),
        }
    }

    fn apply(&self, state: Self::State, Increased(n): Self::Event) -> Self::State {
        if self.fault == Fault::ApplyPanics {
            panic!("apply panics on purpose");
        }
        state + n
    }

    fn snapshot(&self, _: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        match self.fault {
            Fault::SnapshotFails => Err(Boom),
            Fault::SnapshotPanics => panic!("snapshot panics on purpose"),
            _ => Ok(None),
        }
    }
}

/// The step at which a [Fragile] counter fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    InitFails,
    InitFromSnapshotFails,
    HandlePanics,
    ApplyPanics,
    ThenPanics,
    SnapshotFails,
    SnapshotPanics,
}

/// Holds the value of its last event, and offers a fixed snapshot after every event if configured.
struct Holder<T> {
    entity_id: &'static str,
    probe_tx: mpsc::UnboundedSender<Probe>,
    snapshot: Option<T>,
}

impl<T> Holder<T> {
    fn new(entity_id: &'static str, probe_tx: mpsc::UnboundedSender<Probe>) -> Self {
        Self {
            entity_id,
            probe_tx,
            snapshot: None,
        }
    }

    fn with_snapshot(mut self, snapshot: T) -> Self {
        self.snapshot = Some(snapshot);
        self
    }
}

impl<T> EventSourced for Holder<T>
where
    T: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Command = HolderCommand<T>;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Held<T>;
    type State = T;
    type Snapshot = Held<T>;
    type Error = !;

    fn persistence_id(&self) -> PersistenceId {
        persistence_id(self.entity_id)
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::Init);
        Ok(T::default())
    }

    fn init_from_snapshot(&self, Held(value): Self::Snapshot) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::InitFromSnapshot);
        Ok(value)
    }

    fn recovered(
        &self,
        _: &ActorContext<Self::Command>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        let _ = self.probe_tx.send(Probe::Recovered);
        Ok(state)
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        match incoming {
            Incoming::Message(HolderCommand::Set(value)) => Ok(Effect::persist(Held(value))),

            Incoming::Message(HolderCommand::Get(reply_to)) => {
                Ok(Effect::<Self>::none().then(move |value| reply_to.reply(value.clone())))
            }

            Incoming::Message(HolderCommand::Stop) => Ok(Effect::stop()),

            Incoming::Terminated(_) => Ok(Effect::none()),
        }
    }

    fn apply(&self, _: Self::State, Held(value): Self::Event) -> Self::State {
        value
    }

    fn snapshot(&self, _: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok(self.snapshot.clone().map(Held))
    }
}

enum HolderCommand<T> {
    Set(T),
    Get(ReplyTo<T>),
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Held<T>(T);

impl<T> Versioned for Held<T>
where
    T: Serialize + DeserializeOwned,
{
    const MANIFEST: &'static str = "held";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Debug)]
enum Command {
    Increment(u64),
    IncrementTwice(u64, u64),
    Get(ReplyTo<u64>),
    IncrementAndDefer(u64, ReplyTo<u64>),
    IncrementAndDeferThenPanic(u64, ReplyTo<u64>),
    Undefer,
    Fail,
    Stop,
    StopAfter(u64),
    StartTimer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Increased(u64);

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Count(u64);

impl Versioned for Count {
    const MANIFEST: &'static str = "count";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Debug, Error)]
#[error("boom")]
struct Boom;

#[derive(Debug, Clone, Copy)]
struct Failing;

impl Codec for Failing {
    fn encode<T>(&self, _value: &T) -> Result<Vec<u8>, EncodeError>
    where
        T: Serialize,
    {
        Err(EncodeError::new(Boom))
    }

    fn decode<T>(&self, _payload: &[u8]) -> Result<T, PayloadError>
    where
        T: DeserializeOwned,
    {
        Err(PayloadError::new(Boom))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probe {
    Init,
    InitFromSnapshot,
    Recovered,
    Handled,
    Settled(u64),
    Bye,
    Terminated,
}

struct Watcher {
    probe_tx: mpsc::UnboundedSender<Probe>,
    bye_before_stopping: bool,
}

impl EventSourced for Watcher {
    type Command = WatcherCommand;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Nothing;
    type State = Option<ActorRef<()>>;
    type Snapshot = Nothing;
    type Error = !;

    fn persistence_id(&self) -> PersistenceId {
        persistence_id("watcher")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn recovered(
        &self,
        context: &ActorContext<Self::Command>,
        _: Self::State,
    ) -> Result<Self::State, Self::Error> {
        let child = context.spawn(Child {
            parent: self.bye_before_stopping.then(|| context.self_ref().clone()),
        });
        context.watch(&child);

        Ok(Some(child))
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        state: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        match incoming {
            Incoming::Message(WatcherCommand::StopChild) => {
                if let Some(child) = state {
                    child.tell(());
                }
                Ok(Effect::none())
            }

            Incoming::Message(WatcherCommand::Bye) => {
                let _ = self.probe_tx.send(Probe::Bye);
                if let Some(child) = state {
                    context.unwatch(child);
                }
                Ok(Effect::none())
            }

            Incoming::Message(WatcherCommand::Stop) => Ok(Effect::stop()),

            Incoming::Terminated(_) => {
                let _ = self.probe_tx.send(Probe::Terminated);
                Ok(Effect::stop())
            }
        }
    }

    fn apply(&self, _: Self::State, event: Self::Event) -> Self::State {
        match event {}
    }
}

#[derive(Debug)]
enum WatcherCommand {
    StopChild,
    Bye,
    Stop,
}

struct Child {
    parent: Option<ActorRef<WatcherCommand>>,
}

impl Actor for Child {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => {
                if let Some(parent) = &self.parent {
                    parent.tell(WatcherCommand::Bye);
                }
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

struct Parent {
    store: FaultyStore,
    probe_tx: mpsc::UnboundedSender<Probe>,
}

impl Actor for Parent {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.spawn_event_sourced(
            Counter::new("13", self.probe_tx.clone()),
            Persistence::new(self.store.clone()),
        );

        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => Ok(Control::Stop),
            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

/// Spawns a [Counter] and stops, both in the `receive` of its one message.
struct HastyParent {
    store: FaultyStore,
    probe_tx: mpsc::UnboundedSender<Probe>,
}

impl Actor for HastyParent {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        context.spawn_event_sourced(
            Counter::new("42", self.probe_tx.clone()),
            Persistence::new(self.store.clone()),
        );
        Ok(Control::Stop)
    }
}

/// Spawns a [TimedCounter] with a timer started in `recovered` and hands its reference out.
struct UntimedParent {
    store: FaultyStore,
    probe_tx: mpsc::UnboundedSender<Probe>,
    child_tx: mpsc::UnboundedSender<ActorRef<Command>>,
}

impl Actor for UntimedParent {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let child = context.spawn_event_sourced(
            TimedCounter::new("31", self.probe_tx.clone()).with_timer_on_recovery(),
            Persistence::new(self.store.clone()),
        );
        self.child_tx
            .send(child)
            .expect("test dropped the receiver");
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => Ok(Control::Stop),
            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

/// Spawns an untimed [Watcher] and hands its reference out; timed itself, with a timer no test
/// waits for.
struct TimedParent {
    store: FaultyStore,
    probe_tx: mpsc::UnboundedSender<Probe>,
    child_tx: mpsc::UnboundedSender<ActorRef<WatcherCommand>>,
}

impl Actor for TimedParent {
    type Message = ();
    type Timers = Timed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        context.start_timer("t", (), Duration::from_secs(3600));
        let child = context.spawn_event_sourced(
            Watcher {
                probe_tx: self.probe_tx.clone(),
                bye_before_stopping: false,
            },
            Persistence::new(self.store.clone()),
        );
        self.child_tx
            .send(child)
            .expect("test dropped the receiver");
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(()) => Ok(Control::Stop),
            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

/// An [InMemoryStore] which can be made to misbehave: appends can be delayed, and fail, panic or
/// hang a given number of times, or hang once their events are stored; reads, snapshot loads and
/// snapshot saves can hang a given number of times, reads and snapshot loads can fail a given
/// number of times, and snapshot saves can fail. Successful reads and every append are logged, so
/// a test can assert which positions a replay asked for and how many events each append carried.
#[derive(Debug, Clone, Default)]
struct FaultyStore {
    inner: InMemoryStore,
    reads: Arc<Mutex<Vec<SeqNo>>>,
    appends: Arc<Mutex<Vec<usize>>>,
    append_delay: Option<Duration>,
    append_failures: Arc<Mutex<u32>>,
    append_panics: Arc<Mutex<u32>>,
    hanging_appends: Arc<Mutex<u32>>,
    hanging_stored_appends: Arc<Mutex<u32>>,
    hanging_reads: Arc<Mutex<u32>>,
    failing_reads: Arc<Mutex<u32>>,
    hanging_loads: Arc<Mutex<u32>>,
    failing_loads: Arc<Mutex<u32>>,
    hanging_saves: Arc<Mutex<u32>>,
    fail_saves: bool,
    error_hooks: Arc<ErrorHooks>,
}

impl FaultyStore {
    fn with_append_delay(mut self, append_delay: Duration) -> Self {
        self.append_delay = Some(append_delay);
        self
    }

    fn with_append_failures(mut self, failures: u32) -> Self {
        self.append_failures = Arc::new(Mutex::new(failures));
        self
    }

    fn with_append_panics(mut self, panics: u32) -> Self {
        self.append_panics = Arc::new(Mutex::new(panics));
        self
    }

    fn with_hanging_appends(mut self, hangs: u32) -> Self {
        self.hanging_appends = Arc::new(Mutex::new(hangs));
        self
    }

    fn with_hanging_stored_appends(mut self, hangs: u32) -> Self {
        self.hanging_stored_appends = Arc::new(Mutex::new(hangs));
        self
    }

    fn with_hanging_reads(mut self, hangs: u32) -> Self {
        self.hanging_reads = Arc::new(Mutex::new(hangs));
        self
    }

    fn with_failing_reads(mut self, failures: u32) -> Self {
        self.failing_reads = Arc::new(Mutex::new(failures));
        self
    }

    fn with_hanging_loads(mut self, hangs: u32) -> Self {
        self.hanging_loads = Arc::new(Mutex::new(hangs));
        self
    }

    fn with_failing_loads(mut self, failures: u32) -> Self {
        self.failing_loads = Arc::new(Mutex::new(failures));
        self
    }

    fn with_hanging_saves(mut self, hangs: u32) -> Self {
        self.hanging_saves = Arc::new(Mutex::new(hangs));
        self
    }

    fn with_failing_saves(mut self) -> Self {
        self.fail_saves = true;
        self
    }

    fn with_panicking_errors(mut self) -> Self {
        self.error_hooks = Arc::new(ErrorHooks {
            panicking: true,
            ..ErrorHooks::default()
        });
        self
    }

    fn error_hooks(&self) -> &ErrorHooks {
        &self.error_hooks
    }

    fn error(&self) -> FaultyStoreError {
        FaultyStoreError(self.error_hooks.clone())
    }

    fn inner(&self) -> &InMemoryStore {
        &self.inner
    }

    fn reads(&self) -> Vec<SeqNo> {
        self.reads.lock().expect("reads lock poisoned").clone()
    }

    fn appends(&self) -> Vec<usize> {
        self.appends.lock().expect("appends lock poisoned").clone()
    }
}

impl EventStore for FaultyStore {
    type Error = FaultyStoreError;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        self.appends
            .lock()
            .expect("appends lock poisoned")
            .push(events.len());

        if let Some(append_delay) = self.append_delay {
            sleep(append_delay).await;
        }

        if take_fault(&self.hanging_appends) {
            pending::<()>().await;
        }

        {
            let mut failures = self
                .append_failures
                .lock()
                .expect("append failures lock poisoned");
            if *failures > 0 {
                *failures -= 1;
                return Err(AppendError::Store(self.error()));
            }
        }

        // Panic outside the lock scope, else the poisoned lock fails every later append. The
        // scopes also keep the guards off the delegating await below, which must stay Send.
        let panic = {
            let mut panics = self
                .append_panics
                .lock()
                .expect("append panics lock poisoned");
            if *panics > 0 {
                *panics -= 1;
                true
            } else {
                false
            }
        };
        if panic {
            panic!("append panicked");
        }

        let appended =
            self.inner
                .append(id, next_seq_no, events)
                .await
                .map_err(|error| match error {
                    AppendError::Conflict => AppendError::Conflict,
                    AppendError::Store(error) => {
                        panic!("the in-memory store failed an append: {error}")
                    }
                });

        if appended.is_ok() && take_fault(&self.hanging_stored_appends) {
            pending::<()>().await;
        }

        appended
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        if take_fault(&self.hanging_reads) {
            pending::<()>().await;
        }

        if take_fault(&self.failing_reads) {
            return Err(self.error());
        }

        self.reads
            .lock()
            .expect("reads lock poisoned")
            .push(from_seq_no);

        self.inner
            .read(id, from_seq_no, limit)
            .await
            .map_err(|error| panic!("the in-memory store failed a read: {error}"))
    }
}

impl SnapshotStore for FaultyStore {
    type Error = FaultyStoreError;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        if self.fail_saves {
            return Err(self.error());
        }

        if take_fault(&self.hanging_saves) {
            pending::<()>().await;
        }

        self.inner
            .save(id, next_seq_no, snapshot)
            .await
            .map_err(|error| match error {})
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        if take_fault(&self.hanging_loads) {
            pending::<()>().await;
        }

        if take_fault(&self.failing_loads) {
            return Err(self.error());
        }

        self.inner.load(id).await.map_err(|error| match error {})
    }
}

/// A snapshot store whose `load` reports that it started and then blocks its worker until it is
/// released, so recovery runs on without awaiting while the test acts.
#[derive(Debug, Clone)]
struct BlockingLoads {
    inner: InMemoryStore,
    started_tx: mpsc::UnboundedSender<()>,
    release_rx: Arc<Mutex<std_mpsc::Receiver<()>>>,
}

impl SnapshotStore for BlockingLoads {
    type Error = !;

    async fn save(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        snapshot: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        self.inner.save(id, next_seq_no, snapshot).await
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        self.started_tx
            .send(())
            .expect("the test awaits the start of the load");
        block_in_place(|| {
            self.release_rx
                .lock()
                .expect("release lock poisoned")
                .recv()
                .expect("the test releases the load")
        });
        self.inner.load(id).await
    }
}

/// A store whose snapshot leaves replay to resume at `u64::MAX`, the last position, and which
/// accepts every append without fencing, so overflowing positions reach the run loop. With
/// `event_at_last_position` a replay from there returns one event at `u64::MAX`.
#[derive(Debug, Clone, Default)]
struct LastPositionStore {
    event_at_last_position: bool,
    appended: Arc<Mutex<usize>>,
}

impl EventStore for LastPositionStore {
    type Error = !;

    async fn append(
        &self,
        _: &PersistenceId,
        _: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        *self.appended.lock().expect("appended lock poisoned") += events.len();
        Ok(())
    }

    async fn read(
        &self,
        _: &PersistenceId,
        from_seq_no: SeqNo,
        _: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        let last = SeqNo::new(u64::MAX);
        if self.event_at_last_position && from_seq_no == last {
            return Ok(vec![StoredEvent {
                seq_no: last,
                event: encoded(&Increased(1)),
            }]);
        }
        Ok(Vec::new())
    }
}

impl SnapshotStore for LastPositionStore {
    type Error = !;

    async fn save(
        &self,
        _: &PersistenceId,
        _: SeqNo,
        _: EncodedSnapshot,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn load(&self, _: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        Ok(Some(StoredSnapshot {
            next_seq_no: SeqNo::new(u64::MAX),
            snapshot: EncodedSnapshot {
                manifest: Count::MANIFEST.into(),
                schema_version: Count::VERSION,
                payload: Cbor.encode(&Count(5)).expect("the snapshot is encodable"),
            },
        }))
    }
}

/// A store whose stream has a gap: a read from the start returns the events at 0 and 2.
#[derive(Debug, Clone, Copy)]
struct GapStore;

impl EventStore for GapStore {
    type Error = !;

    async fn append(
        &self,
        _: &PersistenceId,
        _: SeqNo,
        _: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        Ok(())
    }

    async fn read(
        &self,
        _: &PersistenceId,
        from_seq_no: SeqNo,
        _: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        if from_seq_no != SeqNo::ZERO {
            return Ok(Vec::new());
        }
        Ok([0, 2]
            .map(|seq_no| StoredEvent {
                seq_no: SeqNo::new(seq_no),
                event: encoded(&Increased(1)),
            })
            .into())
    }
}

/// An injected store failure; with panicking hooks, its `Display` and its destructor panic.
#[derive(Debug)]
struct FaultyStoreError(Arc<ErrorHooks>);

impl Display for FaultyStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.displayed.store(true, SeqCst);
        if self.0.panicking {
            panic!("store error display panics on purpose");
        }
        f.write_str("injected store failure")
    }
}

impl error::Error for FaultyStoreError {}

impl Drop for FaultyStoreError {
    fn drop(&mut self) {
        self.0.dropped.store(true, SeqCst);
        if self.0.panicking {
            panic!("store error destructor panics on purpose");
        }
    }
}

#[derive(Debug, Default)]
struct ErrorHooks {
    panicking: bool,
    displayed: AtomicBool,
    dropped: AtomicBool,
}

/// The effect of a counter command, shared by [Counter] and [TimedCounter].
fn counter_effect<A>(
    probe_tx: &mpsc::UnboundedSender<Probe>,
    command: Command,
) -> Result<Effect<A>, Boom>
where
    A: EventSourced<Command = Command, Event = Increased, State = u64, Error = Boom>,
{
    match command {
        Command::Increment(n) => {
            let probe_tx = probe_tx.clone();
            Ok(Effect::persist(Increased(n)).then(move |count| {
                let _ = probe_tx.send(Probe::Settled(*count));
            }))
        }

        Command::IncrementTwice(n, m) => Ok(Effect::persist_all([Increased(n), Increased(m)])),

        Command::Get(reply_to) => Ok(Effect::none().then(move |count| reply_to.reply(*count))),

        Command::Fail => Err(Boom),

        Command::Stop => Ok(Effect::stop()),

        Command::StopAfter(n) => Ok(Effect::persist(Increased(n)).and_stop()),

        Command::StartTimer => unreachable!("only a TimedCounter is told StartTimer"),

        Command::IncrementAndDefer(..)
        | Command::IncrementAndDeferThenPanic(..)
        | Command::Undefer => {
            unreachable!("only a Counter is told to defer")
        }
    }
}

/// Whether one of the given remaining faults is due, taking it if so.
fn take_fault(faults: &Mutex<u32>) -> bool {
    let mut faults = faults.lock().expect("faults lock poisoned");
    if *faults > 0 {
        *faults -= 1;
        true
    } else {
        false
    }
}
