//! A profiling workload for the `hotpath` feature: flood a single actor through a bounded mailbox,
//! then drive sequential ask round trips; together they exercise every instrumented function, from
//! the send path through the mailbox to the run loop and termination.
//!
//! The flood runs on a single-threaded runtime, so it is sent in full before the actor receives any
//! of it. Every run therefore reaches the same mailbox backlog and allocates the same bytes. The
//! ask round trips run on a multi-threaded runtime, so each one wakes the actor on another thread.
//!
//! Run `just profile` for a timing report or `just profile-alloc` for per-call allocations. Read
//! the numbers as relative attribution, not absolute cost: instrumentation overhead is significant
//! for nanosecond-scale operations, and an async function like `recv` is measured over its
//! future's lifetime, hence includes waiting for messages.

use anyhow::Context;
use std::{convert::Infallible, num::NonZeroUsize, time::Duration};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorSystem, Control, Incoming, MailboxCapacity, ReplyTo,
    Unstashed, Untimed,
};
use tokio::runtime;

const FLOOD_MESSAGES: usize = 100_000;
const ASK_REQUESTS: usize = 10_000;
const ASK_TIMEOUT: Duration = Duration::from_secs(1);

#[cfg_attr(feature = "hotpath", hotpath::main)]
fn main() -> anyhow::Result<()> {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the flood runtime")?
        .block_on(flood())?;
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the ask runtime")?
        .block_on(ask_round_trips())
}

async fn flood() -> anyhow::Result<()> {
    let capacity = NonZeroUsize::new(FLOOD_MESSAGES).context("flood message count is zero")?;
    let config = ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(capacity));
    let system = ActorSystem::with_config(
        Countdown {
            messages: FLOOD_MESSAGES,
        },
        config,
    );

    for _ in 0..FLOOD_MESSAGES {
        system.root().tell(Tick);
    }

    system
        .terminated()
        .await
        .context("awaiting flood termination")
}

async fn ask_round_trips() -> anyhow::Result<()> {
    let system = ActorSystem::new(Echo {
        requests: ASK_REQUESTS,
    });

    for _ in 0..ASK_REQUESTS {
        system
            .root()
            .ask(ASK_TIMEOUT, Request)
            .await
            .context("asking echo actor")?;
    }

    system
        .terminated()
        .await
        .context("awaiting echo termination")
}

fn next_control(remaining: usize) -> Control<usize> {
    remaining
        .checked_sub(1)
        .filter(|n| *n > 0)
        .map_or(Control::Stop, Control::Continue)
}

struct Tick;

struct Countdown {
    messages: usize,
}

impl Actor for Countdown {
    type Message = Tick;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = usize;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.messages)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(next_control(remaining))
    }
}

struct Request(ReplyTo<usize>);

struct Echo {
    requests: usize,
}

impl Actor for Echo {
    type Message = Request;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = usize;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.requests)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(Request(reply_to)) = incoming {
            reply_to.reply(remaining);
        }
        Ok(next_control(remaining))
    }
}
