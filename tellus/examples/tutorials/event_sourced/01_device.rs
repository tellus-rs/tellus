//! Step 1 of the event-sourced actors tutorial: a device which persists every reading as an event,
//! so a new actor system over the same store recovers the last reading by replay.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, time::Duration};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Nothing, Persistence,
    PersistenceId, ReplyTo, SchemaVersion, Unstashed, Untimed, Versioned,
};

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = InMemoryStore::default();

    let system = ActorSystem::event_sourced(Device, Persistence::new(store.clone()));
    system.root().tell(Command::Record(21.5));
    system.root().tell(Command::Record(22.0));
    let last_reading = system
        .root()
        .ask(Duration::from_secs(1), Command::Read)
        .await
        .context("asking for the last reading")?;
    if let Some(degrees) = last_reading {
        println!("Last reading before stopping: {degrees:.1}");
    }
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the first actor system's termination")?;

    let system = ActorSystem::event_sourced(Device, Persistence::new(store));
    let last_reading = system
        .root()
        .ask(Duration::from_secs(1), Command::Read)
        .await
        .context("asking for the last reading")?;
    match last_reading {
        Some(degrees) => println!("Last reading after recovery: {degrees:.1}"),
        None => println!("No reading after recovery"),
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the second actor system's termination")
}
// ANCHOR_END: main

struct Device;

// ANCHOR: device
impl EventSourced for Device {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Recorded;
    type State = Option<f64>;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", "left").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

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
                Ok(Effect::none().then(move |last_reading| reply_to.reply(*last_reading)))
            }
        }
    }

    fn apply(&self, _: Self::State, Recorded(degrees): Self::Event) -> Self::State {
        Some(degrees)
    }
}
// ANCHOR_END: device

// ANCHOR: types
enum Command {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}

#[derive(Serialize, Deserialize)]
struct Recorded(f64);

impl Versioned for Recorded {
    const MANIFEST: &'static str = "recorded";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
// ANCHOR_END: types
