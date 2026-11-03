//! Step 2 of the event-sourced actors tutorial: an invalid reading fails the device, which restarts
//! under the `Restart` supervision strategy and recovers its last reading by replay, unlike the
//! plain device of the getting started tutorial, which loses it.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, time::Duration};
use tellus::{
    ActorConfig, ActorContext, ActorSystem, Effect, EventSourced, InMemoryStore, Incoming, Nothing,
    Persistence, PersistenceId, ReplyTo, RestartPolicy, SchemaVersion, SupervisionStrategy,
    Unstashed, Untimed, Versioned,
};
use thiserror::Error;

const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(MAX_RESTARTS),
    ));
    let system = ActorSystem::event_sourced_with_config(
        Device,
        Persistence::new(InMemoryStore::default()),
        config,
    );

    system.root().tell(Command::Record(21.5));
    system.root().tell(Command::Record(f64::NAN));

    let last_reading = system
        .root()
        .ask(Duration::from_secs(5), Command::Read)
        .await
        .context("asking for the last reading")?;
    match last_reading {
        Some(degrees) => println!("Last reading after the restart: {degrees:.1}"),
        None => println!("No reading after the restart"),
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

struct Device;

impl EventSourced for Device {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Recorded;
    type State = Option<f64>;
    type Snapshot = Nothing;
    type Error = InvalidReading;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", "left").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    // ANCHOR: handle-apply
    // ANCHOR: handle
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
            Command::Record(degrees) if degrees.is_nan() => Err(InvalidReading),

            Command::Record(degrees) => Ok(Effect::persist(Recorded(degrees))),

            Command::Read(reply_to) => {
                Ok(Effect::none().then(move |last_reading| reply_to.reply(*last_reading)))
            }
        }
    }
    // ANCHOR_END: handle

    fn apply(&self, _: Self::State, Recorded(degrees): Self::Event) -> Self::State {
        Some(degrees)
    }
    // ANCHOR_END: handle-apply
}

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

#[derive(Debug, Error)]
#[error("reading is not a number")]
struct InvalidReading;
