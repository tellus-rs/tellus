//! Step 3 of the getting started tutorial: a group actor which spawns a child actor per device on
//! demand and routes readings and requests to it.

use anyhow::Context;
use std::{collections::HashMap, convert::Infallible, time::Duration};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed,
};

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Group);
    let group = system.root();

    group.tell(GroupMessage::Record {
        device_name: "left",
        degrees: 21.5,
    });
    group.tell(GroupMessage::Record {
        device_name: "right",
        degrees: 19.1,
    });
    group.tell(GroupMessage::Record {
        device_name: "left",
        degrees: 22.0,
    });

    for device_name in ["left", "right", "middle"] {
        let last_reading = group
            .ask(Duration::from_secs(1), |reply_to| GroupMessage::Read {
                device_name,
                reply_to,
            })
            .await
            .context("asking for the last reading")?;
        match last_reading {
            Some(degrees) => println!("Last reading of {device_name}: {degrees:.1}"),
            None => println!("No reading of {device_name}"),
        }
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

// ANCHOR: group
struct Group;

impl Actor for Group {
    type Message = GroupMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<&'static str, ActorRef<DeviceMessage>>;
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
            unreachable!("the group watches no actor, hence never gets a terminated signal")
        };

        match message {
            // ANCHOR: record
            GroupMessage::Record {
                device_name,
                degrees,
            } => {
                let device = devices
                    .entry(device_name)
                    .or_insert_with(|| context.spawn(Device));
                device.tell(DeviceMessage::Record(degrees));
                Ok(Control::Continue(devices))
            }

            // ANCHOR_END: record
            GroupMessage::Read {
                device_name,
                reply_to,
            } => {
                match devices.get(device_name) {
                    Some(device) => device.tell(DeviceMessage::Read(reply_to)),
                    None => reply_to.reply(None),
                }
                Ok(Control::Continue(devices))
            }
        }
    }
}

enum GroupMessage {
    Record {
        device_name: &'static str,
        degrees: f64,
    },

    Read {
        device_name: &'static str,
        reply_to: ReplyTo<Option<f64>>,
    },
}
// ANCHOR_END: group

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

enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}
