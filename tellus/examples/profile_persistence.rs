//! A profiling workload for the `hotpath` feature on the `persistence` code: flood an event-sourced
//! counter through an in-memory store, one persisted event per command with a snapshot every
//! `SNAPSHOT_EACH` events, then recover it from the populated store. Together they exercise every
//! instrumented function, from command settlement (encode, append, apply, snapshot) to recovery
//! (snapshot load, paged read, replay).
//!
//! Run `just profile-persistence` for a timing report or `just profile-persistence-alloc` for
//! per-call allocations. The in-memory store isolates framework overhead (codec, settlement,
//! replay) from backend latency; against a real backend the store I/O wrappers (`read_page`,
//! `append_events`, `save_snapshot`) dominate instead. Unlike the messaging path, settlement
//! allocates by design (payload buffers, manifests), so read the alloc report as a budget, not a
//! zero check.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Persistence,
    PersistenceId, SchemaVersion, Unstashed, Untimed, Versioned,
};

const EVENTS: u64 = 50_000;
const SNAPSHOT_EACH: u64 = 3_000;

const _: () = assert!(
    !EVENTS.is_multiple_of(SNAPSHOT_EACH),
    "recovery needs a replay tail after the last snapshot"
);

#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() -> anyhow::Result<()> {
    let store = InMemoryStore::default();

    flood(store.clone()).await?;
    recover(store).await
}

async fn flood(store: InMemoryStore) -> anyhow::Result<()> {
    let system = ActorSystem::event_sourced(Counter { events: EVENTS }, persistence(store));

    for _ in 0..EVENTS {
        system.root().tell(Increase);
    }

    system
        .terminated()
        .await
        .context("awaiting flood termination")
}

async fn recover(store: InMemoryStore) -> anyhow::Result<()> {
    let system = ActorSystem::event_sourced(Counter { events: EVENTS }, persistence(store));

    system.root().tell(Increase);

    system
        .terminated()
        .await
        .context("awaiting recovery termination")
}

fn persistence(store: InMemoryStore) -> Persistence<InMemoryStore, InMemoryStore> {
    Persistence::new(store.clone()).with_snapshot_store(store)
}

struct Increase;

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

struct Counter {
    events: u64,
}

impl EventSourced for Counter {
    type Command = Increase;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Count;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", "profile").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn init_from_snapshot(&self, Count(count): Self::Snapshot) -> Result<Self::State, Self::Error> {
        Ok(count)
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        _: Incoming<Self::Command>,
        count: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let effect = if *count >= self.events {
            Effect::stop()
        } else if *count + 1 == self.events {
            Effect::persist(Increased).and_stop()
        } else {
            Effect::persist(Increased)
        };
        Ok(effect)
    }

    fn apply(&self, count: Self::State, Increased: Self::Event) -> Self::State {
        count + 1
    }

    fn snapshot(&self, count: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok(count.is_multiple_of(SNAPSHOT_EACH).then_some(Count(*count)))
    }
}
