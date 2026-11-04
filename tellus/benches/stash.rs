//! Stashing benchmarks, reported as messages per second:
//!
//! - `actor`: concurrent sender tasks, one and four per worker thread, flood a single counting
//!   actor which stashes the first [STASH_BATCH] - 1 messages of every batch and unstashes them on
//!   the batch's last message, so those messages pass `receive` twice.
//! - `event_sourced`: the bench thread floods a single event-sourced counter, one persisted event
//!   per command, which stashes the first [STASH_BATCH] - 1 commands of every batch and unstashes
//!   them on the batch's last command, so those commands pass `handle` twice.
//!
//! All benchmarks run on a Tokio runtime with a fixed worker thread count, which also sizes the
//! workloads, so measurements do not depend on the host's core count.

use hotpath::criterion::{
    BenchmarkId, Config, Criterion, Throughput, criterion_group, criterion_main,
};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Control, Effect, EventSourced,
    InMemoryStore, Incoming, Persistence, PersistenceId, SchemaVersion, Stashed, Untimed,
    Versioned,
};
use tokio::runtime::{Builder, Runtime};

const WORKER_THREADS: usize = 4;
const STASH_BATCH: usize = 8;
const ACTOR_MESSAGES: usize = 100_000;
const EVENTS: u64 = 10_000;

// Every stashed item must be released: what is told in total is a multiple of the batch.
const _: () = assert!(
    ACTOR_MESSAGES.is_multiple_of(4 * WORKER_THREADS) && ACTOR_MESSAGES.is_multiple_of(STASH_BATCH)
);
const _: () = assert!(EVENTS.is_multiple_of(STASH_BATCH as u64));

fn actor(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("actor");

    for senders in [WORKER_THREADS, 4 * WORKER_THREADS] {
        let share = ACTOR_MESSAGES / senders;
        group.throughput(Throughput::Elements((share * senders) as u64));

        group.bench_function(
            BenchmarkId::new(format!("batch_{STASH_BATCH}"), senders),
            |b| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    measure(
                        iters,
                        || {
                            let actor = Stashing {
                                messages: share * senders,
                            };
                            (actor, ActorConfig::default())
                        },
                        |root| {
                            for _ in 0..senders {
                                let root = root.clone();
                                tokio::spawn(async move {
                                    for _ in 0..share {
                                        root.tell(Tick);
                                    }
                                });
                            }
                        },
                    )
                    .await
                });
            },
        );
    }

    group.finish();
}

fn event_sourced(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("event_sourced");
    group.throughput(Throughput::Elements(EVENTS));

    group.bench_function(format!("batch_{STASH_BATCH}"), |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iters {
                let store = InMemoryStore::default();

                let start = Instant::now();
                let system = spawn_event_sourced(&store);
                for n in 0..EVENTS {
                    system.root().tell(IncreaseAt(n));
                }
                system
                    .terminated()
                    .await
                    .expect("awaiting actor system termination");
                elapsed += start.elapsed();
            }

            elapsed
        });
    });

    group.finish();
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
        .expect("tokio runtime can be created")
}

async fn measure<A, F, D>(iters: u64, make: F, drive: D) -> Duration
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
    F: Fn() -> (A, ActorConfig),
    D: Fn(&ActorRef<A::Message>),
{
    let mut elapsed = Duration::ZERO;

    for _ in 0..iters {
        let (actor, config) = make();
        let system = ActorSystem::with_config(actor, config);

        let start = Instant::now();
        drive(system.root());
        system
            .terminated()
            .await
            .expect("awaiting actor system termination");
        elapsed += start.elapsed();
    }

    elapsed
}

fn spawn_event_sourced(store: &InMemoryStore) -> ActorSystem<IncreaseAt> {
    let counter = StashingCounter { events: EVENTS };
    let persistence = Persistence::new(store.clone()).with_snapshot_store(store.clone());

    ActorSystem::event_sourced(counter, persistence)
}

struct Tick;

/// A countdown which stashes the first `STASH_BATCH - 1` messages of every batch and unstashes them
/// on the batch's last message, so it receives those messages twice and counts every message once,
/// when it handles it.
struct Stashing {
    messages: usize,
}

struct StashingState {
    remaining: usize,
    stashed: usize,
    unstashed: usize,
}

impl Actor for Stashing {
    type Message = Tick;
    type Timers = Untimed;
    type Stash = Stashed;
    type State = StashingState;
    type Error = Infallible;

    fn init(
        &self,
        _: &ActorContext<Self::Message, Untimed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        Ok(StashingState {
            remaining: self.messages,
            stashed: 0,
            unstashed: 0,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if state.unstashed > 0 {
            state.unstashed -= 1;
        } else if state.stashed + 1 < STASH_BATCH {
            let Ok(()) = context.stash(incoming) else {
                unreachable!("an unbounded stash has room");
            };
            state.stashed += 1;
            return Ok(Control::Continue(state));
        } else {
            context.unstash_all();
            state.unstashed = state.stashed;
            state.stashed = 0;
        }

        state.remaining -= 1;
        let control = if state.remaining == 0 {
            Control::Stop
        } else {
            Control::Continue(state)
        };

        Ok(control)
    }
}

/// A command carrying its index, which tells a stashed command from a new one once it comes back.
struct IncreaseAt(u64);

#[derive(Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Serialize, Deserialize)]
struct Count(u64);

impl Versioned for Count {
    const MANIFEST: &'static str = "count";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

/// A counter which stashes the first `STASH_BATCH - 1` commands of every batch and unstashes them
/// on the batch's last command. A new command's index is at least the count, and a command coming
/// back from the stash has an index below it, since the batch's last command has been persisted by
/// then.
struct StashingCounter {
    events: u64,
}

impl EventSourced for StashingCounter {
    type Command = IncreaseAt;
    type Timers = Untimed;
    type Stash = Stashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", "stashing").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn init_from_snapshot(&self, Count(count): Self::Snapshot) -> Result<Self::State, Self::Error> {
        Ok(count)
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command, Untimed, Stashed>,
        incoming: Incoming<Self::Command>,
        count: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let index = match &incoming {
            Incoming::Message(IncreaseAt(index)) => *index,
            Incoming::Terminated(_) => return Ok(Effect::none()),
        };

        if index >= *count {
            if (index + 1).is_multiple_of(STASH_BATCH as u64) {
                context.unstash_all();
            } else {
                let Ok(()) = context.stash(incoming) else {
                    unreachable!("an unbounded stash has room");
                };
                return Ok(Effect::none());
            }
        }

        let effect = if *count + 1 == self.events {
            Effect::persist(Increased).and_stop()
        } else {
            Effect::persist(Increased)
        };

        Ok(effect)
    }

    fn apply(&self, count: Self::State, Increased: Self::Event) -> Self::State {
        count + 1
    }

    fn snapshot(&self, _: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok(None)
    }
}

criterion_group!(
    name = benches;
    config = Config::default()
        .sample_size(30)
        .noise_threshold(0.05)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10));
    targets = actor, event_sourced
);
criterion_main!(benches);
