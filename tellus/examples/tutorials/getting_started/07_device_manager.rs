//! Step 7 of the getting started tutorial: a device manager routes commands to groups, which it
//! spawns on demand, one more level of the actor tree on top of the groups and devices of step 6.
//! It is tellus's take on the IoT device manager from the Apache Pekko documentation.
//!
//! The averages are printed to stdout and tellus logs the failures and restarts to stderr; the log
//! level is configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --example getting_started_07_device_manager`.

use anyhow::Context;
use std::{collections::HashMap, convert::Infallible, io, num::NonZeroU32, time::Duration};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, Control, Incoming, ReplyTo,
    RestartPolicy, SupervisionStrategy, Unstashed, Untimed,
};
use thiserror::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let system = ActorSystem::new(DeviceManager);
    let manager = system.root();

    let readings = [
        ("attic", "left", 21.5),
        ("attic", "right", 19.1),
        ("cellar", "boiler", 11.0),
        ("cellar", "boiler", f64::NAN),
    ];
    for (group_name, device_name, degrees) in readings {
        manager.tell(Command::Record {
            group_name,
            device_name,
            degrees,
        });
    }
    manager.tell(Command::Retire {
        group_name: "attic",
        device_name: "right",
    });

    for group_name in ["attic", "cellar", "garage"] {
        let average = manager
            .ask(Duration::from_secs(5), |reply_to| Command::Average {
                group_name,
                reply_to,
            })
            .await
            .context("asking for the average temperature")?;
        match average {
            Some(degrees) => println!("Average temperature in {group_name}: {degrees:.2}"),
            None => println!("No readings in {group_name}"),
        }
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

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

// ANCHOR: manager
struct DeviceManager;

impl Actor for DeviceManager {
    type Message = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<&'static str, ActorRef<GroupMessage>>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(HashMap::new())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut groups: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            unreachable!("the manager watches no actor, hence never gets a terminated signal")
        };

        match command {
            Command::Record {
                group_name,
                device_name,
                degrees,
            } => {
                let group = groups
                    .entry(group_name)
                    .or_insert_with(|| context.spawn(Group));
                group.tell(GroupMessage::Record {
                    device_name,
                    degrees,
                });
                Ok(Control::Continue(groups))
            }

            Command::Retire {
                group_name,
                device_name,
            } => {
                if let Some(group) = groups.get(group_name) {
                    group.tell(GroupMessage::Retire { device_name });
                }
                Ok(Control::Continue(groups))
            }

            Command::Average {
                group_name,
                reply_to,
            } => {
                match groups.get(group_name) {
                    Some(group) => group.tell(GroupMessage::Average(reply_to)),
                    None => reply_to.reply(None),
                }
                Ok(Control::Continue(groups))
            }
        }
    }
}

enum Command {
    Record {
        group_name: &'static str,
        device_name: &'static str,
        degrees: f64,
    },

    Retire {
        group_name: &'static str,
        device_name: &'static str,
    },

    Average {
        group_name: &'static str,
        reply_to: ReplyTo<Option<f64>>,
    },
}
// ANCHOR_END: manager

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
        match incoming {
            Incoming::Message(GroupMessage::Record {
                device_name,
                degrees,
            }) => {
                let device = devices.entry(device_name).or_insert_with(|| {
                    let config = ActorConfig::default().with_supervision_strategy(
                        SupervisionStrategy::Restart(RestartPolicy::new(MAX_RESTARTS)),
                    );
                    let device = context.spawn_with_config(Device, config);
                    context.watch(&device);
                    device
                });
                device.tell(DeviceMessage::Record(degrees));
                Ok(Control::Continue(devices))
            }

            Incoming::Message(GroupMessage::Retire { device_name }) => {
                if let Some(device) = devices.get(device_name) {
                    device.tell(DeviceMessage::Retire);
                }
                Ok(Control::Continue(devices))
            }

            Incoming::Message(GroupMessage::Average(reply_to)) => {
                let query = context.spawn(Query);
                query.tell(QueryMessage::Start {
                    devices: devices.values().cloned().collect(),
                    reply_to,
                });
                Ok(Control::Continue(devices))
            }

            Incoming::Terminated(device_id) => {
                devices.retain(|_, device| device.actor_id() != device_id);
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

    Retire {
        device_name: &'static str,
    },

    Average(ReplyTo<Option<f64>>),
}

struct Query;

impl Actor for Query {
    type Message = QueryMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = QueryState;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(QueryState::Starting)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Message(QueryMessage::Start { devices, reply_to }), _) => {
                let mut pending = HashMap::new();
                for device in devices {
                    context.watch(&device);
                    let device_id = device.actor_id();
                    device.tell(DeviceMessage::Read(context.reply_to(move |reading| {
                        QueryMessage::Reading { device_id, reading }
                    })));
                    pending.insert(device_id, device);
                }
                Query::conclude(pending, 0.0, 0, reply_to)
            }

            (
                Incoming::Message(QueryMessage::Reading { device_id, reading }),
                QueryState::Running {
                    mut pending,
                    mut sum,
                    mut count,
                    reply_to,
                },
            ) => {
                if let Some(device) = pending.remove(&device_id) {
                    context.unwatch(&device);
                }
                if let Some(degrees) = reading {
                    sum += degrees;
                    count += 1;
                }
                Query::conclude(pending, sum, count, reply_to)
            }

            (
                Incoming::Terminated(device_id),
                QueryState::Running {
                    mut pending,
                    sum,
                    count,
                    reply_to,
                },
            ) => {
                // The ordering guarantee proves that this device's reading can no longer arrive.
                pending.remove(&device_id);
                Query::conclude(pending, sum, count, reply_to)
            }

            (_, QueryState::Starting) => unreachable!("the start message comes first"),
        }
    }
}

impl Query {
    fn conclude(
        pending: HashMap<ActorId, ActorRef<DeviceMessage>>,
        sum: f64,
        count: u32,
        reply_to: ReplyTo<Option<f64>>,
    ) -> Result<Control<QueryState>, Infallible> {
        if pending.is_empty() {
            reply_to.reply((count > 0).then(|| sum / f64::from(count)));
            Ok(Control::Stop)
        } else {
            Ok(Control::Continue(QueryState::Running {
                pending,
                sum,
                count,
                reply_to,
            }))
        }
    }
}

enum QueryMessage {
    Start {
        devices: Vec<ActorRef<DeviceMessage>>,
        reply_to: ReplyTo<Option<f64>>,
    },

    Reading {
        device_id: ActorId,
        reading: Option<f64>,
    },
}

enum QueryState {
    Starting,

    Running {
        pending: HashMap<ActorId, ActorRef<DeviceMessage>>,
        sum: f64,
        count: u32,
        reply_to: ReplyTo<Option<f64>>,
    },
}

struct Device;

impl Actor for Device {
    type Message = DeviceMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Option<f64>;
    type Error = InvalidReading;

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
            DeviceMessage::Record(degrees) if degrees.is_nan() => Err(InvalidReading),

            DeviceMessage::Record(degrees) => Ok(Control::Continue(Some(degrees))),

            DeviceMessage::Read(reply_to) => {
                reply_to.reply(last_reading);
                Ok(Control::Continue(last_reading))
            }

            DeviceMessage::Retire => Ok(Control::Stop),
        }
    }
}

enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
    Retire,
}

#[derive(Debug, Error)]
#[error("reading is not a number")]
struct InvalidReading;
