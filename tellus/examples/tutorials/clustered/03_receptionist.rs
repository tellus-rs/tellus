//! Step 3 of the clustered actors tutorial: the first node finds the device manager by its key
//! alone, wherever it runs, by subscribing to the receptionist instead of looking up a known
//! address.
//!
//! The averages are printed to stdout by the first node; tellus logs to stderr, configured via
//! `RUST_LOG`, e.g. `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev
//! --example clustered_03_receptionist`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    num::NonZeroU32,
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, Control, Incoming, ReplyTo,
    RestartPolicy, SupervisionStrategy, Unstashed, Untimed,
    cluster::{
        self, BootstrapConfig, EndpointConfig, FixedSeeds, Key, Manifest, receptionist,
        transport::QuicTransport,
    },
};
use thiserror::Error;
use tokio::{
    process::{self, Child},
    time::timeout,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const MANAGER_NODE_ARG: &str = "manager-node";
const MANAGER_KEY: &str = "device-manager";
const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const FIND_TIMEOUT: Duration = Duration::from_secs(10);
const ASK_TIMEOUT: Duration = Duration::from_secs(5);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(MANAGER_NODE_ARG) => manager_node().await,
        _ => first_node().await,
    }
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

// ANCHOR: first-node
async fn first_node() -> anyhow::Result<()> {
    let seeds = reserved_addrs(2)?;
    let _manager_node = spawn_node(MANAGER_NODE_ARG, seeds[1], &seeds)?;
    join_cluster(seeds[0], seeds.clone()).await?;
    let manager = find_manager().await?;

    let readings = [
        ("attic", "left", 21.5),
        ("attic", "right", 19.1),
        ("cellar", "boiler", 11.0),
        ("cellar", "boiler", f64::NAN),
    ];
    for (group_name, device_name, degrees) in readings {
        manager.tell(Command::Record {
            group_name: group_name.to_string(),
            device_name: device_name.to_string(),
            degrees,
        });
    }
    manager.tell(Command::Retire {
        group_name: "attic".to_string(),
        device_name: "right".to_string(),
    });

    for group_name in ["attic", "cellar", "garage"] {
        let average = manager
            .ask(ASK_TIMEOUT, |reply_to| Command::Average {
                group_name: group_name.to_string(),
                reply_to,
            })
            .await
            .context("asking for the average temperature")?;
        match average {
            Some(degrees) => println!("Average temperature in {group_name}: {degrees:.2}"),
            None => println!("No readings in {group_name}"),
        }
    }
    Ok(())
}
// ANCHOR_END: first-node

// ANCHOR: manager-node
async fn manager_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    join_cluster(addr, seeds).await?;

    let system = ActorSystem::new(DeviceManager);
    cluster::register(&Key::new(MANAGER_KEY), system.root())
        .context("registering the device manager")?;
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: manager-node

// ANCHOR: find-manager
async fn find_manager() -> anyhow::Result<ActorRef<Command>> {
    let mut managers =
        receptionist::subscribe(&Key::new(MANAGER_KEY)).context("subscribing to the key")?;
    let find = async {
        loop {
            let current = managers
                .current()
                .context("resolving the device managers")?;
            match current.into_iter().next() {
                Some(manager) => return anyhow::Ok(manager),
                None => managers.changed().await,
            }
        }
    };

    timeout(FIND_TIMEOUT, find)
        .await
        .context("no device manager registered within the timeout")?
}
// ANCHOR_END: find-manager

async fn join_cluster(addr: SocketAddr, seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(FixedSeeds::new(seeds), BootstrapConfig::new()),
    )
    .await
    .context("no cluster within the timeout")?
    .context("bootstrapping the cluster")
}

fn node_args() -> anyhow::Result<(SocketAddr, Vec<SocketAddr>)> {
    let addr = env::args()
        .nth(2)
        .context("node address argument")?
        .parse()
        .context("node address")?;
    let seeds = env::args()
        .skip(3)
        .map(|seed| seed.parse().context("seed address"))
        .collect::<anyhow::Result<_>>()?;
    Ok((addr, seeds))
}

fn reserved_addrs(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let sockets = (0..count)
        .map(|_| UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
        .collect::<Result<Vec<_>, _>>()
        .context("reserving loopback ports")?;
    sockets
        .iter()
        .map(|socket| socket.local_addr().context("reserved local address"))
        .collect()
}

fn spawn_node(role: &str, addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(role)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning a node process")
}

// ANCHOR: manager
struct DeviceManager;

impl Actor for DeviceManager {
    type Message = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<String, ActorRef<GroupMessage>>;
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
                if let Some(group) = groups.get(&group_name) {
                    group.tell(GroupMessage::Retire { device_name });
                }
                Ok(Control::Continue(groups))
            }

            Command::Average {
                group_name,
                reply_to,
            } => {
                match groups.get(&group_name) {
                    Some(group) => group.tell(GroupMessage::Average(reply_to)),
                    None => reply_to.reply(None),
                }
                Ok(Control::Continue(groups))
            }
        }
    }
}

// ANCHOR: command
#[derive(Serialize, Deserialize)]
enum Command {
    Record {
        group_name: String,
        device_name: String,
        degrees: f64,
    },

    Retire {
        group_name: String,
        device_name: String,
    },

    Average {
        group_name: String,
        reply_to: ReplyTo<Option<f64>>,
    },
}

impl Manifest for Command {
    const MANIFEST: &'static str = "device-manager-command";
}
// ANCHOR_END: command
// ANCHOR_END: manager

struct Group;

impl Actor for Group {
    type Message = GroupMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<String, ActorRef<DeviceMessage>>;
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
                if let Some(device) = devices.get(&device_name) {
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
    Record { device_name: String, degrees: f64 },

    Retire { device_name: String },

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
