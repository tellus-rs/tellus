//! Step 5 of the clustered actors tutorial: the device manager the first node uses stops on
//! request, and its node leaves the cluster instead of being killed. The watch fires with a real
//! termination, the cluster lists the node as left without detecting a failure, and the first
//! node turns to the other device manager.
//!
//! The first node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! clustered_05_leave`.

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
        self, BootstrapConfig, ClusterState, EndpointConfig, FixedSeeds, Key, Manifest,
        MemberState, Schema, receptionist, transport::QuicTransport,
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
const NODES: usize = 3;
const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(3).expect("3 is not zero");
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const FIND_TIMEOUT: Duration = Duration::from_secs(10);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);
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
    let seeds = reserved_addrs(NODES)?;
    let mut manager_nodes = seeds[1..]
        .iter()
        .map(|addr| spawn_node(MANAGER_NODE_ARG, *addr, &seeds))
        .collect::<anyhow::Result<Vec<_>>>()?;
    join_cluster(seeds[0], seeds.clone()).await?;
    await_all_up().await?;
    find_managers(2).await?;

    let manager = receptionist::lookup_at(&Key::new(MANAGER_KEY), seeds[1])
        .context("looking up the device manager on the second node")?
        .into_iter()
        .next()
        .context("no device manager on the second node")?;
    let watcher = ActorSystem::new(Watcher {
        manager: manager.clone(),
    });
    for (device_name, degrees) in [("left", 21.5), ("right", 19.1)] {
        manager.tell(Command::Record {
            group_name: "attic".to_string(),
            device_name: device_name.to_string(),
            degrees,
        });
    }
    print_attic_average(&manager).await?;
    manager.tell(Command::Stop);
    timeout(LEAVE_TIMEOUT, watcher.terminated())
        .await
        .context("no terminated signal within the timeout")?
        .context("awaiting the watcher's termination")?;
    await_left(seeds[1]).await?;
    println!("State of the second node: {}", member_state(seeds[1])?);
    let status = manager_nodes[0]
        .wait()
        .await
        .context("awaiting the second node's exit")?;
    anyhow::ensure!(status.success(), "the second node failed: {status}");

    let manager = find_managers(1).await?.remove(0);
    print_attic_average(&manager).await
}
// ANCHOR_END: first-node

// ANCHOR: watcher
struct Watcher {
    manager: ActorRef<Command>,
}

impl Actor for Watcher {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.manager);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(_) = incoming else {
            unreachable!("nothing tells the watcher a message")
        };

        println!("The device manager has terminated");
        Ok(Control::Stop)
    }
}
// ANCHOR_END: watcher

// ANCHOR: manager-node
async fn manager_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    join_cluster(addr, seeds).await?;
    await_all_up().await?;

    let system = ActorSystem::new(DeviceManager);
    cluster::register(&Key::new(MANAGER_KEY), system.root())
        .context("registering the device manager")?;
    cluster::leave_on_terminated(system)
        .await
        .context("leaving the cluster")?;
    Ok(())
}
// ANCHOR_END: manager-node

// ANCHOR: await-left
async fn await_left(addr: SocketAddr) -> anyhow::Result<()> {
    let mut state = cluster::cluster_state().context("subscribing to the cluster state")?;
    let left = |state: &ClusterState| {
        state
            .members()
            .iter()
            .any(|member| member.addr() == addr && member.state() == MemberState::Left)
    };
    timeout(LEAVE_TIMEOUT, state.wait_for(left))
        .await
        .context("the node did not leave within the timeout")?
        .context("the cluster state stopped")?;
    Ok(())
}
// ANCHOR_END: await-left

// ANCHOR: await-all-up
async fn await_all_up() -> anyhow::Result<()> {
    let mut state = cluster::cluster_state().context("subscribing to the cluster state")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        state.wait_for(|state| state.up().count() == NODES),
    )
    .await
    .context("not all nodes up within the timeout")?
    .context("the cluster state stopped")?;
    Ok(())
}
// ANCHOR_END: await-all-up

// ANCHOR: find-managers
async fn find_managers(count: usize) -> anyhow::Result<Vec<ActorRef<Command>>> {
    let mut managers =
        receptionist::subscribe(&Key::new(MANAGER_KEY)).context("subscribing to the key")?;
    let find = async {
        loop {
            let current = managers
                .current()
                .context("resolving the device managers")?;
            if current.len() == count {
                return anyhow::Ok(current);
            }
            managers.changed().await;
        }
    };

    timeout(FIND_TIMEOUT, find)
        .await
        .context("not the expected device managers within the timeout")?
}
// ANCHOR_END: find-managers

async fn print_attic_average(manager: &ActorRef<Command>) -> anyhow::Result<()> {
    let average = manager
        .ask(ASK_TIMEOUT, |reply_to| Command::Average {
            group_name: "attic".to_string(),
            reply_to,
        })
        .await
        .context("asking for the average temperature")?;
    match average {
        Some(degrees) => println!("Average temperature in attic: {degrees:.2}"),
        None => println!("No readings in attic"),
    }
    Ok(())
}

fn member_state(addr: SocketAddr) -> anyhow::Result<MemberState> {
    cluster::members()
        .context("listing the members")?
        .iter()
        .find(|member| member.addr() == addr)
        .map(|member| member.state())
        .context("no such member")
}

// ANCHOR: join-cluster
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
// ANCHOR_END: join-cluster

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

            Command::Stop => Ok(Control::Stop),
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

    Stop,
}

impl Schema for Command {}

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
