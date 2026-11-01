//! Step 4 of the event-sourced actors tutorial: the device of step 3 on PostgreSQL. Every run of
//! this program records one reading and recovers all earlier ones, so the count of readings grows
//! from run to run.
//!
//! It needs the `postgres` service of the repository's compose.yaml, started via
//! `docker compose up -d --wait postgres`. The database is addressed via `DATABASE_URL`, defaulting
//! to that service; `docker compose down -v postgres` resets the readings.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, env, time::Duration};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, Incoming, Persistence, PersistenceId, ReplyTo,
    SchemaVersion, Unstashed, Untimed, Versioned,
};
use tellus_persistence_postgres::{PostgresStore, sqlx::PgPool};

const DATABASE_URL: &str = "postgres://tellus:tellus@localhost:5432/tellus";
const SNAPSHOT_EVERY: u64 = 3;

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| DATABASE_URL.to_string());
    let pool = PgPool::connect(&url)
        .await
        .context("connection to PostgreSQL")?;
    let store = PostgresStore::new(pool);
    store.migrate().await.context("schema migration")?;

    let persistence = Persistence::new(store.clone()).with_snapshot_store(store);
    let system = ActorSystem::event_sourced(Device, persistence);

    system.root().tell(Command::Record(21.5));
    let readings = system
        .root()
        .ask(Duration::from_secs(5), Command::Read)
        .await
        .context("asking for the readings")?;
    println!("Readings so far: {}", readings.count);

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
    type State = Readings;
    type Snapshot = Readings;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", "tutorial").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(Readings::default())
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        Ok(snapshot)
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
                Ok(Effect::none().then(move |readings| reply_to.reply(*readings)))
            }
        }
    }

    fn apply(&self, readings: Self::State, Recorded(degrees): Self::Event) -> Self::State {
        Readings {
            last: Some(degrees),
            count: readings.count + 1,
        }
    }

    fn snapshot(&self, readings: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok((readings.count % SNAPSHOT_EVERY == 0).then_some(*readings))
    }
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

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
struct Readings {
    last: Option<f64>,
    count: u64,
}

impl Versioned for Readings {
    const MANIFEST: &'static str = "readings";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
