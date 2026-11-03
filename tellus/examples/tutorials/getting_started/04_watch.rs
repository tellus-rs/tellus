//! Step 4 of the getting started tutorial: the group watches its devices, so it learns when a
//! retired device has terminated, and thanks to the ordering guarantee it knows that the device's
//! handover has arrived before.

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

    let last_reading = group
        .ask(Duration::from_secs(1), |reply_to| GroupMessage::Read {
            device_name: "left",
            reply_to,
        })
        .await
        .context("asking for the last reading")?;
    match last_reading {
        Some(degrees) => println!("Last reading of left: {degrees:.1}"),
        None => println!("No reading of left"),
    }

    group.tell(GroupMessage::Retire {
        device_name: "left",
    });
    group.tell(GroupMessage::Retire {
        device_name: "right",
    });

    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

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

    // ANCHOR: receive
    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut devices: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(GroupMessage::Record {
                device_name,
                degrees,
            }) => {
                let device = devices.entry(device_name).or_insert_with(|| {
                    let device = context.spawn(Device);
                    context.watch(&device);
                    device
                });
                device.tell(DeviceMessage::Record(degrees));
                Ok(Control::Continue(devices))
            }

            Incoming::Message(GroupMessage::Read {
                device_name,
                reply_to,
            }) => {
                match devices.get(device_name) {
                    Some(device) => device.tell(DeviceMessage::Read(reply_to)),
                    None => reply_to.reply(None),
                }
                Ok(Control::Continue(devices))
            }

            Incoming::Message(GroupMessage::Retire { device_name }) => {
                if let Some(device) = devices.get(device_name) {
                    device.tell(DeviceMessage::Retire(context.reply_to(
                        move |last_reading| GroupMessage::Retired {
                            device_name,
                            last_reading,
                        },
                    )));
                }
                Ok(Control::Continue(devices))
            }

            Incoming::Message(GroupMessage::Retired {
                device_name,
                last_reading,
            }) => {
                match last_reading {
                    Some(degrees) => {
                        println!("{device_name} handed over its last reading {degrees:.1}")
                    }
                    None => println!("{device_name} handed over no reading"),
                }
                Ok(Control::Continue(devices))
            }

            Incoming::Terminated(device_id) => {
                devices.retain(|device_name, device| {
                    let gone = device.actor_id() == device_id;
                    if gone {
                        println!("{device_name} is gone");
                    }
                    !gone
                });

                if devices.is_empty() {
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(devices))
                }
            }
        }
    }
    // ANCHOR_END: receive
}

// ANCHOR: group-message
enum GroupMessage {
    Record {
        device_name: &'static str,
        degrees: f64,
    },

    Read {
        device_name: &'static str,
        reply_to: ReplyTo<Option<f64>>,
    },

    Retire {
        device_name: &'static str,
    },

    Retired {
        device_name: &'static str,
        last_reading: Option<f64>,
    },
}
// ANCHOR_END: group-message

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

    // ANCHOR: device-receive
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

            DeviceMessage::Retire(reply_to) => {
                reply_to.reply(last_reading);
                Ok(Control::Stop)
            }
        }
    }
    // ANCHOR_END: device-receive
}

enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
    Retire(ReplyTo<Option<f64>>),
}
