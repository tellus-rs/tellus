//! A flaky `Loader` under the `Restart` supervision strategy, event-sourced: the count is a fold of
//! persisted events. So the restart after a toxic value recovers it by replay from PostgreSQL
//! instead of starting over at 0, and a rerun of the whole process counts on where the last one
//! stopped. The mailbox survives a restart as before, so the values queued behind a toxic one
//! still get loaded.
//!
//! The output is printed to stdout and tellus logs the failures and restarts to stderr; the log
//! level is configured via `RUST_LOG`.
//!
//! It needs the `postgres` service of the repository's compose.yaml, started via
//! `docker compose up -d --wait postgres`. The database is addressed via `DATABASE_URL`, defaulting
//! to that service; `docker compose down -v postgres` resets the count.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, env, io, num::NonZeroU32, time::Duration};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorSystem, Backoff, Control, Effect, EventSourced,
    Incoming, Nothing, Persistence, PersistenceId, RestartPolicy, SchemaVersion,
    SupervisionStrategy, Untimed, Versioned,
};
use tellus_persistence_postgres::{PostgresStore, sqlx::PgPool};
use thiserror::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const DATABASE_URL: &str = "postgres://tellus:tellus@localhost:5432/tellus";

const TOXIC: u32 = 13;
const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let url = env::var("DATABASE_URL").unwrap_or_else(|_| DATABASE_URL.to_string());
    let pool = PgPool::connect(&url)
        .await
        .context("connection to PostgreSQL")?;
    let store = PostgresStore::new(pool);
    store.migrate().await.context("schema migration")?;

    let backoff = Backoff::new(Duration::from_millis(50), Duration::from_secs(1))
        .context("backoff bounds for the loader")?;
    let loader_config = ActorConfig::default().with_supervision_strategy(
        SupervisionStrategy::Restart(RestartPolicy::new(MAX_RESTARTS).with_backoff(backoff)),
    );

    let system = ActorSystem::new(Overseer {
        loader_config,
        persistence: Persistence::new(store),
    });

    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(io::stderr),
        )
        .init();
}

struct Overseer {
    loader_config: ActorConfig,
    persistence: Persistence<PostgresStore>,
}

impl Actor for Overseer {
    type Message = Nothing;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let loader = context.spawn_event_sourced_with_config(
            Loader,
            self.persistence.clone(),
            self.loader_config,
        );
        context.watch(&loader);

        // The two toxic values cause two restarts; everything queued behind them still loads.
        for value in [1, 2, TOXIC, 3, TOXIC, 4] {
            loader.tell(Command::Load(value));
        }
        loader.tell(Command::Finish);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        // With `Nothing` as message type the only possible incoming is the terminated signal.
        println!("## Loader finished");
        Ok(Control::Stop)
    }
}

struct Loader;

impl EventSourced for Loader {
    type Command = Command;
    type Timers = Untimed;
    type Event = Loaded;
    type State = u64;
    type Snapshot = Nothing;
    type Error = ToxicValue;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("loader", "example").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        println!("## Loader seeded with count 0");
        Ok(0)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn recovered(
        &self,
        _: &ActorContext<Self::Command>,
        count: Self::State,
    ) -> Result<Self::State, Self::Error> {
        println!("## Loader recovered, count is {count}");
        Ok(count)
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the loader watches no actor, hence never gets a terminated signal")
        };

        match message {
            // The error escalates to supervision; the restart recovers the count by replay.
            Command::Load(TOXIC) => Err(ToxicValue(TOXIC)),

            Command::Load(value) => Ok(Effect::persist(Loaded(value))
                .then(move |count| println!("## Loaded {value}, count is {count}"))),

            Command::Finish => Ok(Effect::stop()),
        }
    }

    fn apply(&self, count: Self::State, Loaded(_): Self::Event) -> Self::State {
        count + 1
    }
}

enum Command {
    Load(u32),
    Finish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Loaded(u32);

impl Versioned for Loaded {
    const MANIFEST: &'static str = "loaded";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Debug, Error)]
#[error("toxic value {0}")]
struct ToxicValue(u32);
