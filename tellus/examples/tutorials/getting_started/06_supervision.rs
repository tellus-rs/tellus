//! Step 6 of the getting started tutorial: devices fail on invalid readings and restart under the
//! `Restart` supervision strategy, losing their last reading but keeping their mailbox. A device
//! failing more than `MAX_RESTARTS` times in a row stops, and the group's watch prunes it.
//!
//! The average is printed to stdout and tellus logs the failures and restarts to stderr; the log
//! level is configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --example getting_started_06_supervision`.

use anyhow::Context;
use std::{collections::HashMap, convert::Infallible, io, num::NonZeroU32, time::Duration};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, Control, Incoming, ReplyTo,
    RestartPolicy, SupervisionStrategy, Untimed,
};
use thiserror::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

// ANCHOR: max-restarts
const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");
// ANCHOR_END: max-restarts

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let system = ActorSystem::new(Group);
    let group = system.root();

    let readings = [
        ("left", 21.5),
        ("middle", 20.4),
        ("right", 19.1),
        ("left", f64::NAN),
        ("broken", f64::NAN),
        ("broken", f64::NAN),
        ("broken", f64::NAN),
        ("broken", f64::NAN),
    ];
    for (device_name, degrees) in readings {
        group.tell(GroupMessage::Record {
            device_name,
            degrees,
        });
    }
    group.tell(GroupMessage::Retire {
        device_name: "right",
    });

    let average = group
        .ask(Duration::from_secs(5), GroupMessage::Average)
        .await
        .context("asking for the average temperature")?;
    match average {
        Some(degrees) => println!("Average temperature: {degrees:.2}"),
        None => println!("No readings"),
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

struct Group;

impl Actor for Group {
    type Message = GroupMessage;
    type Timers = Untimed;
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
                // ANCHOR: spawn
                let device = devices.entry(device_name).or_insert_with(|| {
                    let config = ActorConfig::default().with_supervision_strategy(
                        SupervisionStrategy::Restart(RestartPolicy::new(MAX_RESTARTS)),
                    );
                    let device = context.spawn_with_config(Device, config);
                    context.watch(&device);
                    device
                });
                // ANCHOR_END: spawn
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

// ANCHOR: device
impl Actor for Device {
    type Message = DeviceMessage;
    type Timers = Untimed;
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
// ANCHOR_END: device

enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
    Retire,
}

#[derive(Debug, Error)]
#[error("reading is not a number")]
struct InvalidReading;
