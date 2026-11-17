//! How-to example for evolving a stored event: release 1 of a thermometer stored its readings in
//! Fahrenheit, release 2 stores them in Celsius. Release 2 still reads the events of release 1,
//! because `Versioned::decode` upcasts schema version 1 to the current shape. Both releases store
//! their events with the `Json` codec, so the stored payloads are readable.
//!
//! Run it with `cargo run --quiet -p tellus --features persistence-in-memory --example
//! how_to_evolve_events`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, time::Duration};
use tellus::{
    ActorContext, ActorSystem, Codec, DecodeError, Effect, EventSourced, InMemoryStore, Incoming,
    Json, Nothing, Persistence, PersistenceId, ReplyTo, SchemaVersion, Unstashed, Untimed,
    Versioned,
};

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = InMemoryStore::default();
    let persistence = Persistence::new(store.clone()).with_codec(Json);

    let system = ActorSystem::event_sourced(ThermometerV1, persistence.clone());
    for fahrenheit in [70.7, 71.6] {
        system
            .root()
            .ask(Duration::from_secs(1), |reply_to| {
                CommandV1::Record(fahrenheit, reply_to)
            })
            .await
            .context("asking release 1 to record a reading")?;
    }
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the termination of release 1")?;
    print_stored_events(&store);

    let system = ActorSystem::event_sourced(Thermometer, persistence);
    system.root().tell(Command::Record(22.4));
    let readings = system
        .root()
        .ask(Duration::from_secs(1), Command::Read)
        .await
        .context("asking release 2 for the readings")?;
    let readings = readings
        .iter()
        .map(|celsius| format!("{celsius:.1}"))
        .collect::<Vec<_>>()
        .join(", ");
    println!("Readings in Celsius: {readings}");
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the termination of release 2")?;
    print_stored_events(&store);

    Ok(())
}
// ANCHOR_END: main

struct ThermometerV1;

impl EventSourced for ThermometerV1 {
    type Command = CommandV1;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = RecordedV1;
    type State = ();
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        thermometer_id()
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(())
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
        let Incoming::Message(CommandV1::Record(fahrenheit, reply_to)) = incoming else {
            unreachable!("the thermometer watches no actor, hence never gets a terminated signal")
        };
        Ok(Effect::persist(RecordedV1 { fahrenheit }).then(move |_| reply_to.reply(())))
    }

    fn apply(&self, state: Self::State, _: Self::Event) -> Self::State {
        state
    }
}

enum CommandV1 {
    Record(f64, ReplyTo<()>),
}

// ANCHOR: recorded-v1
#[derive(Serialize, Deserialize)]
struct RecordedV1 {
    fahrenheit: f64,
}

impl Versioned for RecordedV1 {
    const MANIFEST: &'static str = "recorded";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
// ANCHOR_END: recorded-v1

struct Thermometer;

impl EventSourced for Thermometer {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Recorded;
    type State = Vec<f64>;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        thermometer_id()
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(Vec::new())
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
            unreachable!("the thermometer watches no actor, hence never gets a terminated signal")
        };

        match command {
            Command::Record(celsius) => Ok(Effect::persist(Recorded { celsius })),

            Command::Read(reply_to) => {
                Ok(Effect::<Self>::none().then(move |readings| reply_to.reply(readings.clone())))
            }
        }
    }

    fn apply(&self, mut readings: Self::State, Recorded { celsius }: Self::Event) -> Self::State {
        readings.push(celsius);
        readings
    }
}

enum Command {
    Record(f64),
    Read(ReplyTo<Vec<f64>>),
}

// ANCHOR: recorded
#[derive(Serialize, Deserialize)]
struct Recorded {
    celsius: f64,
}

impl Versioned for Recorded {
    const MANIFEST: &'static str = "recorded";
    const VERSION: SchemaVersion = SchemaVersion::new(2);

    fn decode<C>(
        codec: &C,
        schema_version: SchemaVersion,
        payload: &[u8],
    ) -> Result<Self, DecodeError>
    where
        C: Codec,
    {
        if schema_version == Self::VERSION {
            Ok(codec.decode(payload)?)
        } else if schema_version == RecordedV1::VERSION {
            let RecordedV1 { fahrenheit } = codec.decode(payload)?;
            Ok(Self {
                celsius: (fahrenheit - 32.0) * 5.0 / 9.0,
            })
        } else {
            Err(DecodeError::UnsupportedSchemaVersion {
                manifest: Self::MANIFEST,
                schema_version,
            })
        }
    }
}
// ANCHOR_END: recorded

fn thermometer_id() -> PersistenceId {
    PersistenceId::new("thermometer", "attic").expect("the segments are valid")
}

fn print_stored_events(store: &InMemoryStore) {
    for stored in store.events(&thermometer_id()) {
        println!(
            "Stored event {}: {} version {}, {}",
            stored.seq_no.as_u64(),
            stored.event.manifest,
            stored.event.schema_version,
            String::from_utf8_lossy(&stored.event.payload)
        );
    }
}
