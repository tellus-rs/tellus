//! Step 1 of the getting started tutorial: the minimal tellus program, a root actor which greets
//! for the one message it receives and then stops, terminating the actor system.

use anyhow::Context;
use std::convert::Infallible;
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, Untimed};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Greeter);

    system.root().tell(Greet("Tellus".to_string()));

    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}

struct Greeter;

impl Actor for Greeter {
    type Message = Greet;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(Greet(name)) = incoming {
            println!("Hello, {name}!");
        }
        Ok(Control::Stop)
    }
}

struct Greet(String);
