//! An event-sourced counter surviving process restarts: every run recovers the count from
//! PostgreSQL by replay, increments it once and prints the new count, so repeated runs count on.
//!
//! It needs the `postgres` service of the repository's docker-compose.yaml, started via
//! `docker compose up -d --wait postgres`. The database is addressed via `DATABASE_URL`, defaulting
//! to that service; `docker compose down -v postgres` resets the count.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, env, time::Duration};
use tellus::{
    ActorContext, ActorSystem, Effect, EventSourced, Incoming, Nothing, Persistence, PersistenceId,
    ReplyTo, SchemaVersion, Untimed, Versioned,
};
use tellus_persistence_postgres::{PostgresStore, sqlx::PgPool};

const DATABASE_URL: &str = "postgres://tellus:tellus@localhost:5432/tellus";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| DATABASE_URL.to_string());
    let pool = PgPool::connect(&url)
        .await
        .context("connection to PostgreSQL")?;
    let store = PostgresStore::new(pool);
    store.migrate().await.context("schema migration")?;

    let system = ActorSystem::event_sourced(Counter, Persistence::new(store));
    let count = system
        .root()
        .ask(Duration::from_secs(5), Command::Increment)
        .await
        .context("increment request")?;
    println!("The count is now {count}.");

    system.root().tell(Command::Stop);
    system.terminated().await?;
    Ok(())
}

struct Counter;

impl EventSourced for Counter {
    type Command = Command;
    type Timers = Untimed;
    type Event = Increased;
    type State = u64;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", "example").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
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
        match incoming {
            Incoming::Message(Command::Increment(reply_to)) => {
                Ok(Effect::persist(Increased).then(move |count| reply_to.reply(*count)))
            }

            Incoming::Message(Command::Stop) => Ok(Effect::stop()),

            Incoming::Terminated(_) => Ok(Effect::none()),
        }
    }

    fn apply(&self, state: Self::State, Increased: Self::Event) -> Self::State {
        state + 1
    }
}

#[derive(Debug)]
enum Command {
    Increment(ReplyTo<u64>),
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}
