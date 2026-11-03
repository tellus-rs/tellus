//! Example for the explanation of actors as state machines: a device which ignores readings until
//! it is calibrated. Its state is an enum with one variant per state of the machine.

use anyhow::Context;
use std::{convert::Infallible, time::Duration};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Device);
    let device = system.root();

    device.tell(Message::Record(21.0));
    let last_reading = device
        .ask(Duration::from_secs(1), Message::Read)
        .await
        .context("asking for the last reading")?;
    println!("Before calibration: {last_reading:?}");

    device.tell(Message::Calibrate { offset: -0.5 });
    device.tell(Message::Record(22.0));
    let last_reading = device
        .ask(Duration::from_secs(1), Message::Read)
        .await
        .context("asking for the last reading")?;
    println!("After calibration: {last_reading:?}");

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}

struct Device;

impl Actor for Device {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = State;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(State::Uncalibrated)
    }

    // ANCHOR: receive
    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        let state = match (state, message) {
            (State::Uncalibrated, Message::Record(_)) => State::Uncalibrated,

            (State::Uncalibrated, Message::Read(reply_to)) => {
                reply_to.reply(None);
                State::Uncalibrated
            }

            (State::Calibrated { offset, .. }, Message::Record(degrees)) => State::Calibrated {
                offset,
                last_reading: Some(degrees + offset),
            },

            (calibrated @ State::Calibrated { last_reading, .. }, Message::Read(reply_to)) => {
                reply_to.reply(last_reading);
                calibrated
            }

            (_, Message::Calibrate { offset }) => State::Calibrated {
                offset,
                last_reading: None,
            },
        };
        Ok(Control::Continue(state))
    }
    // ANCHOR_END: receive
}

// ANCHOR: types
enum State {
    Uncalibrated,
    Calibrated {
        offset: f64,
        last_reading: Option<f64>,
    },
}

enum Message {
    Calibrate { offset: f64 },
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}
// ANCHOR_END: types
