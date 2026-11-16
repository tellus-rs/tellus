//! How-to example for event-sourced child actors: a building spawns one event-sourced device per
//! name with `ActorContext::spawn_event_sourced`. A device persists a batch of readings atomically
//! with `Effect::persist_all` and forgets them once `EXPIRE_AFTER` has passed without a new batch.
//! The expiry is a timer, which replay does not recreate, so the device starts it again in
//! `EventSourced::recovered`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, time::Duration};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Effect, EventSourced, InMemoryStore,
    Incoming, Nothing, Persistence, PersistenceId, ReplyTo, SchemaVersion, Timed, Unstashed,
    Untimed, Versioned,
};
use tokio::time::sleep;

const DEVICE_NAMES: [&str; 2] = ["left", "right"];

// ANCHOR: expire-after
const EXPIRE_AFTER: Duration = Duration::from_millis(500);
// ANCHOR_END: expire-after

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let persistence = Persistence::new(InMemoryStore::default());

    let system = ActorSystem::new(Building {
        persistence: persistence.clone(),
    });
    let building = system.root();
    building.tell(Message::Record {
        device_name: "left",
        readings: vec![21.5, 22.0, 22.4],
    });
    building.tell(Message::Record {
        device_name: "right",
        readings: vec![19.1, 19.4],
    });
    print_readings(building).await?;
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the first actor system's termination")?;

    let system = ActorSystem::new(Building { persistence });
    let building = system.root();
    print_readings(building).await?;
    sleep(2 * EXPIRE_AFTER).await;
    print_readings(building).await?;
    system.stop();
    system
        .terminated()
        .await
        .context("awaiting the second actor system's termination")
}
// ANCHOR_END: main

async fn print_readings(building: &ActorRef<Message>) -> anyhow::Result<()> {
    for device_name in DEVICE_NAMES {
        let readings = building
            .ask(Duration::from_secs(1), |reply_to| Message::Read {
                device_name,
                reply_to,
            })
            .await
            .context("asking for the readings")?;
        if readings.is_empty() {
            println!("Device {device_name}: no readings");
        } else {
            let readings = readings
                .iter()
                .map(|degrees| format!("{degrees:.1}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!("Device {device_name}: {readings}");
        }
    }
    Ok(())
}

// ANCHOR: building
struct Building {
    persistence: Persistence<InMemoryStore>,
}

impl Actor for Building {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<&'static str, ActorRef<Command>>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(HashMap::new())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut devices: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the building watches no actor, hence never gets a terminated signal")
        };

        let (device_name, command) = match message {
            Message::Record {
                device_name,
                readings,
            } => (device_name, Command::Record(readings)),

            Message::Read {
                device_name,
                reply_to,
            } => (device_name, Command::Read(reply_to)),
        };
        devices
            .entry(device_name)
            .or_insert_with(|| {
                context.spawn_event_sourced(Device { name: device_name }, self.persistence.clone())
            })
            .tell(command);

        Ok(Control::Continue(devices))
    }
}
// ANCHOR_END: building

enum Message {
    Record {
        device_name: &'static str,
        readings: Vec<f64>,
    },

    Read {
        device_name: &'static str,
        reply_to: ReplyTo<Vec<f64>>,
    },
}

struct Device {
    name: &'static str,
}

impl EventSourced for Device {
    type Command = Command;
    type Timers = Timed;
    type Stash = Unstashed;
    type Event = Event;
    type State = Vec<f64>;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", self.name).expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(Vec::new())
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    // ANCHOR: recovered
    fn recovered(
        &self,
        context: &ActorContext<Self::Command, Timed>,
        readings: Self::State,
    ) -> Result<Self::State, Self::Error> {
        if !readings.is_empty() {
            println!("Device {} recovered {} readings", self.name, readings.len());
            context.start_timer("expire", Command::Expire, EXPIRE_AFTER);
        }
        Ok(readings)
    }
    // ANCHOR_END: recovered

    // ANCHOR: handle
    fn handle(
        &self,
        context: &ActorContext<Self::Command, Timed>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        match command {
            Command::Record(readings) => {
                context.start_timer("expire", Command::Expire, EXPIRE_AFTER);
                Ok(Effect::persist_all(
                    readings.into_iter().map(Event::Recorded),
                ))
            }

            Command::Read(reply_to) => {
                Ok(Effect::<Self>::none().then(move |readings| reply_to.reply(readings.clone())))
            }

            Command::Expire => Ok(Effect::persist(Event::Expired)),
        }
    }
    // ANCHOR_END: handle

    fn apply(&self, mut readings: Self::State, event: Self::Event) -> Self::State {
        match event {
            Event::Recorded(degrees) => readings.push(degrees),
            Event::Expired => readings.clear(),
        }
        readings
    }
}

enum Command {
    Record(Vec<f64>),
    Read(ReplyTo<Vec<f64>>),
    Expire,
}

#[derive(Serialize, Deserialize)]
enum Event {
    Recorded(f64),
    Expired,
}

impl Versioned for Event {
    const MANIFEST: &'static str = "device-event";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
