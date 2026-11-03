//! Step 3 of the event-sourced actors tutorial: the device offers a snapshot after every third
//! reading, so recovery starts from the latest snapshot and only replays the events after it.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, time::Duration};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Persistence,
    PersistenceId, ReplyTo, SchemaVersion, Unstashed, Untimed, Versioned,
};

const SNAPSHOT_EVERY: u64 = 3;

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = InMemoryStore::default();
    let persistence = Persistence::new(store.clone()).with_snapshot_store(store.clone());

    let system = ActorSystem::event_sourced(Device, persistence.clone());
    for degrees in [21.5, 22.0, 22.4, 22.1, 21.8, 21.9, 22.3] {
        system.root().tell(Command::Record(degrees));
    }
    system
        .root()
        .ask(Duration::from_secs(1), Command::Read)
        .await
        .context("asking for the readings")?;
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the first actor system's termination")?;

    let id = Device.persistence_id();
    let snapshot = store.snapshot(&id).context("no snapshot was saved")?;
    println!(
        "Stored events: {}, the snapshot covers the first {}",
        store.events(&id).len(),
        snapshot.next_seq_no.as_u64()
    );

    let system = ActorSystem::event_sourced(Device, persistence);
    let readings = system
        .root()
        .ask(Duration::from_secs(1), Command::Read)
        .await
        .context("asking for the readings")?;
    if let Some(degrees) = readings.last {
        println!(
            "After recovery: {} readings, the last one {degrees:.1}",
            readings.count
        );
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the second actor system's termination")
}
// ANCHOR_END: main

struct Device;

impl EventSourced for Device {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Recorded;
    type State = Readings;
    type Snapshot = Readings;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", "left").expect("the segments are valid")
    }

    // ANCHOR: init
    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(Readings::default())
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        Ok(snapshot)
    }
    // ANCHOR_END: init

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        match command {
            Command::Record(degrees) => Ok(Effect::persist(Recorded(degrees))),

            Command::Read(reply_to) => {
                Ok(Effect::none().then(move |readings| reply_to.reply(*readings)))
            }
        }
    }

    // ANCHOR: apply
    fn apply(&self, readings: Self::State, Recorded(degrees): Self::Event) -> Self::State {
        Readings {
            last: Some(degrees),
            count: readings.count + 1,
        }
    }

    fn snapshot(&self, readings: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok((readings.count % SNAPSHOT_EVERY == 0).then_some(*readings))
    }
    // ANCHOR_END: apply
}

enum Command {
    Record(f64),
    Read(ReplyTo<Readings>),
}

#[derive(Serialize, Deserialize)]
struct Recorded(f64);

impl Versioned for Recorded {
    const MANIFEST: &'static str = "recorded";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

// ANCHOR: readings
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
struct Readings {
    last: Option<f64>,
    count: u64,
}

impl Versioned for Readings {
    const MANIFEST: &'static str = "readings";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
// ANCHOR_END: readings
