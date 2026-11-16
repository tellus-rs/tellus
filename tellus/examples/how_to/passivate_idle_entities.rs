//! How-to example for passivating idle entities: a `Host` spawns one session actor per visitor on
//! the first `HostEnvelope` for that visitor, routes every further visit to it and passivates it
//! with `Hosted::Passivate` once it has been idle, so the next visit starts a new session.

use anyhow::Context;
use std::{convert::Infallible, time::Duration};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Host, HostConfig, HostEnvelope, Hosted,
    Incoming, ReplyTo, Unstashed, Untimed,
};
use tokio::time::sleep;

const IDLE_TIMEOUT: Duration = Duration::from_millis(200);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = HostConfig::default()
        .with_idle_timeout(IDLE_TIMEOUT)
        .context("configuring the host")?;
    let system = ActorSystem::new(Host::new(config, |&visitor, context| {
        context.spawn(Session { visitor })
    }));
    let host = system.root();

    visit(host, "alice", "/").await?;
    visit(host, "bob", "/").await?;
    visit(host, "alice", "/pricing").await?;

    sleep(3 * IDLE_TIMEOUT).await;
    visit(host, "alice", "/signup").await?;

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

struct Session {
    visitor: &'static str,
}

impl Actor for Session {
    type Message = Hosted<Visit>;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = u64;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        println!("Session of {} started", self.visitor);
        Ok(0)
    }

    // ANCHOR: receive
    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        visits: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Hosted::Message(Visit { page, reply_to })) => {
                println!("{} visits {page}", self.visitor);
                reply_to.reply(visits + 1);
                Ok(Control::Continue(visits + 1))
            }

            Incoming::Message(Hosted::Passivate) => Ok(Control::Stop),

            Incoming::Terminated(_) => {
                unreachable!("a session watches no actor, hence never gets a terminated signal")
            }
        }
    }
    // ANCHOR_END: receive
}

// ANCHOR: visit
struct Visit {
    page: &'static str,
    reply_to: ReplyTo<u64>,
}
// ANCHOR_END: visit

async fn visit(
    host: &ActorRef<HostEnvelope<&'static str, Visit>>,
    visitor: &'static str,
    page: &'static str,
) -> anyhow::Result<()> {
    let visits = host
        .ask(Duration::from_secs(1), |reply_to| HostEnvelope {
            key: visitor,
            message: Visit { page, reply_to },
        })
        .await
        .with_context(|| format!("asking the session of {visitor}"))?;
    println!("Visits in the session of {visitor}: {visits}");
    Ok(())
}
