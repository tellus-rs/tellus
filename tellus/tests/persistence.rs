#![cfg(feature = "persistence-in-memory")]

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    convert::Infallible,
    error::{self, Error as _},
    fmt::{self, Display, Formatter},
    future::pending,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc, Mutex, Once,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AppendError, Backoff, Cbor, Codec,
    Control, Effect, EncodeError, EncodedEvent, EncodedSnapshot, EventSourced, EventStore,
    InMemoryStore, Incoming, Nothing, PayloadError, Persistence, PersistenceId, ReplyTo,
    RestartPolicy, SchemaVersion, SeqNo, SnapshotStore, StoredEvent, StoredSnapshot,
    SupervisionStrategy, Versioned,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{advance, sleep, timeout},
};

const TIMEOUT: Duration = Duration::from_secs(5);

static TRACING: Once = Once::new();

/// Replay reconstructs the live state: a counter increments across single and atomic multi-event
/// effects, persists a final event while stopping, and a second incarnation over the same store
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
        !probes[1..].contains(&Probe::Init),
        "init must be skipped once a snapshot exists"
    );
    assert_eq!(
        store.reads(),
        [SeqNo::ZERO, SeqNo::new(3)],
        "the second recovery must replay only the events after the snapshot"
    );
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
    let store = FaultyStore::default().with_hanging_reads();
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
/// way settles before the tree terminates: a shutdown can lose the acknowledgement of an event,
/// never the event.
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

/// After `unwatch` no terminated signal is received, even if it is already enqueued: the child
/// says bye through the shared FIFO right before stopping, the watcher unwatches on the bye, and
/// the signal queued behind it is dropped before `handle` ever sees it.
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
        Counter::new("21", probe_tx),
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

/// A restart drops an event-sourced actor's timers: they are runtime state, neither persisted nor
/// recreated by replay. Here `recovered` does not start "t" again, so the next incarnation never
/// receives it.
#[tokio::test(start_paused = true)]
async fn a_restart_drops_an_event_sourced_actors_timer() {
    let (probe_tx, _probe_rx) = mpsc::unbounded_channel();

    let system = ActorSystem::event_sourced_with_config(
        Counter::new("22", probe_tx),
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
}

impl EventSourced for Counter {
    type Command = Command;
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
        _: &ActorContext<Self::Command>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        {
            let mut failures = self
                .recovered_failures
                .lock()
                .expect("recovered failures lock poisoned");
            if *failures > 0 {
                *failures -= 1;
                return Err(Boom);
            }
        }

        let _ = self.probe_tx.send(Probe::Recovered);
        Ok(state)
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            return Ok(Effect::none());
        };
        let _ = self.probe_tx.send(Probe::Handled);

        match command {
            Command::Increment(n) => {
                let probe_tx = self.probe_tx.clone();
                Ok(Effect::persist(Increased(n)).then(move |count| {
                    let _ = probe_tx.send(Probe::Settled(*count));
                }))
            }

            Command::IncrementTwice(n, m) => Ok(Effect::persist_all([Increased(n), Increased(m)])),

            Command::Get(reply_to) => Ok(Effect::none().then(move |count| reply_to.reply(*count))),

            Command::Fail => Err(Boom),

            Command::Stop => Ok(Effect::stop()),

            Command::StopAfter(n) => Ok(Effect::persist(Increased(n)).and_stop()),

            Command::StartTimer => {
                context.start_timer("t", Command::Increment(1), Duration::from_secs(1));
                Ok(Effect::none())
            }
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

#[derive(Debug)]
enum Command {
    Increment(u64),
    IncrementTwice(u64, u64),
    Get(ReplyTo<u64>),
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

#[derive(Debug, PartialEq, Eq)]
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
    type Event = Nothing;
    type State = Option<ActorRef<()>>;
    type Snapshot = Nothing;
    type Error = Infallible;

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
    type State = ();
    type Error = Infallible;

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
    type State = ();
    type Error = Infallible;

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

/// An [InMemoryStore] which can be made to misbehave: appends can be delayed, fail or panic a
/// given number of times, reads can hang forever and snapshot saves can fail. Reads are logged,
/// so a test can assert which positions a replay asked for.
#[derive(Debug, Clone, Default)]
struct FaultyStore {
    inner: InMemoryStore,
    reads: Arc<Mutex<Vec<SeqNo>>>,
    append_delay: Option<Duration>,
    append_failures: Arc<Mutex<u32>>,
    append_panics: Arc<Mutex<u32>>,
    hang_reads: bool,
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

    fn with_hanging_reads(mut self) -> Self {
        self.hang_reads = true;
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
}

impl EventStore for FaultyStore {
    type Error = FaultyStoreError;

    async fn append(
        &self,
        id: &PersistenceId,
        next_seq_no: SeqNo,
        events: Vec<EncodedEvent>,
    ) -> Result<(), AppendError<Self::Error>> {
        if let Some(append_delay) = self.append_delay {
            sleep(append_delay).await;
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

        self.inner
            .append(id, next_seq_no, events)
            .await
            .map_err(|error| match error {
                AppendError::Conflict => AppendError::Conflict,
                AppendError::Store(error) => match error {},
            })
    }

    async fn read(
        &self,
        id: &PersistenceId,
        from_seq_no: SeqNo,
        limit: NonZeroUsize,
    ) -> Result<Vec<StoredEvent>, Self::Error> {
        if self.hang_reads {
            pending::<()>().await;
        }

        self.reads
            .lock()
            .expect("reads lock poisoned")
            .push(from_seq_no);

        self.inner
            .read(id, from_seq_no, limit)
            .await
            .map_err(|error| match error {})
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

        self.inner
            .save(id, next_seq_no, snapshot)
            .await
            .map_err(|error| match error {})
    }

    async fn load(&self, id: &PersistenceId) -> Result<Option<StoredSnapshot>, Self::Error> {
        self.inner.load(id).await.map_err(|error| match error {})
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
