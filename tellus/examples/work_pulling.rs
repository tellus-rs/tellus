//! Work pulling: the manager owns a queue of jobs and each worker requests the next job whenever
//! it is ready, carrying a `ReplyTo` which is single-shot, so one request gets at most one job.
//! A worker thus never has more than one job in flight, which makes a bounded mailbox of capacity
//! one provably sufficient: backpressure by design, without dropping work. Likewise, the manager
//! never holds more requests than there are workers, which bounds its mailbox.
//!
//! The word counts are printed to stdout and tellus logs to stderr; the log level is configured
//! via `RUST_LOG`, e.g. `RUST_LOG=tellus=debug cargo run --quiet -p tellus --example
//! work_pulling`.

use anyhow::Context;
use std::{
    convert::{Infallible, identity},
    io,
    num::NonZeroUsize,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Control, Incoming, MailboxCapacity,
    ReplyTo, Unstashed, Untimed,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const DOCUMENTS: [&str; 6] = [
    "the quick brown fox jumps over the lazy dog",
    "actors all the way down",
    "one message at a time",
    "backpressure by design",
    "workers pull instead of being pushed",
    "a terminated signal arrives behind all messages",
];

const WORKERS: NonZeroUsize = NonZeroUsize::new(3).unwrap();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // ANCHOR: system
    // Each worker has at most one request in flight, so capacity `WORKERS` never drops.
    let config = ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(WORKERS));
    let system = ActorSystem::with_config(Manager, config);
    // ANCHOR_END: system

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

struct Manager;

impl Actor for Manager {
    type Message = RequestJob;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Managing;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        // ANCHOR: spawn
        // A worker has at most one job in flight, so a mailbox of capacity one never drops.
        let config = ActorConfig::default()
            .with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));

        for _ in 0..WORKERS.get() {
            let worker = context.spawn_with_config(
                Worker {
                    manager: context.self_ref().clone(),
                },
                config,
            );
            context.watch(&worker);
        }
        // ANCHOR_END: spawn
        Ok(Managing {
            jobs: DOCUMENTS.to_vec(),
            workers: WORKERS.get(),
        })
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            // ANCHOR: hand-out
            Incoming::Message(RequestJob(reply_to)) => {
                let mut jobs = state.jobs;
                match jobs.pop() {
                    Some(document) => reply_to.reply(Next::Job(document)),
                    None => reply_to.reply(Next::Drained),
                }
                Ok(Control::Continue(Managing { jobs, ..state }))
            }

            // ANCHOR_END: hand-out
            Incoming::Terminated(_) => {
                let workers = state.workers - 1;

                if workers > 0 {
                    Ok(Control::Continue(Managing { workers, ..state }))
                } else {
                    println!("## All documents processed");
                    Ok(Control::Stop)
                }
            }
        }
    }
}

struct RequestJob(ReplyTo<Next>);

struct Managing {
    jobs: Vec<&'static str>,
    workers: usize,
}

struct Worker {
    manager: ActorRef<RequestJob>,
}

impl Actor for Worker {
    type Message = Next;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    // ANCHOR: init
    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.request_job(context);
        Ok(())
    }
    // ANCHOR_END: init

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(next) = incoming else {
            unreachable!("the worker watches no actor, hence never gets a terminated signal")
        };

        // ANCHOR: work
        match next {
            Next::Job(document) => {
                let words = document.split_whitespace().count();
                println!("## {words} words in: {document}");

                self.request_job(context);
                Ok(Control::Continue(state))
            }

            Next::Drained => Ok(Control::Stop),
        }
        // ANCHOR_END: work
    }
}

// ANCHOR: request
impl Worker {
    fn request_job(&self, context: &ActorContext<Next>) {
        // The reply is already this worker's message type, so it converts via `identity`.
        self.manager.tell(RequestJob(context.reply_to(identity)));
    }
}
// ANCHOR_END: request

enum Next {
    Job(&'static str),
    Drained,
}
