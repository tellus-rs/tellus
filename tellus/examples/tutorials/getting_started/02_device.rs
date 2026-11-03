//! Step 2 of the getting started tutorial: a device actor which records temperature readings it is
//! told and answers when asked for the last one.

use anyhow::Context;
use std::{convert::Infallible, time::Duration};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed};

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Device);
    let device = system.root();

    device.tell(DeviceMessage::Record(21.5));
    device.tell(DeviceMessage::Record(22.0));

    let last_reading = device
        .ask(Duration::from_secs(1), DeviceMessage::Read)
        .await
        .context("asking for the last reading")?;
    match last_reading {
        Some(degrees) => println!("Last reading: {degrees:.1}"),
        None => println!("No reading yet"),
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

// ANCHOR: device
struct Device;

impl Actor for Device {
    type Message = DeviceMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Option<f64>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        last_reading: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        match message {
            DeviceMessage::Record(degrees) => Ok(Control::Continue(Some(degrees))),

            DeviceMessage::Read(reply_to) => {
                reply_to.reply(last_reading);
                Ok(Control::Continue(last_reading))
            }
        }
    }
}
// ANCHOR_END: device

// ANCHOR: message
enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}
// ANCHOR_END: message
