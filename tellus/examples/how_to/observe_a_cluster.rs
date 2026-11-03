//! How-to example: observe a cluster. The first node prints every membership transition it sees
//! while a second node joins and then leaves.
//!
//! The transitions are printed to stdout by the first node; tellus logs to stderr, configured via
//! `RUST_LOG`, e.g. `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev
//! --example how_to_observe_a_cluster`.

use anyhow::Context;
use std::{
    convert::Infallible,
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorSystem, Control, Incoming, Unstashed, Untimed,
    cluster::{self, Change, EndpointConfig, Transition, transport::QuicTransport},
};
use tokio::{
    process::{self, Child},
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const SECOND_NODE_ARG: &str = "second-node";
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(20);
const MEMBERSHIP_TIME: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(SECOND_NODE_ARG) => second_node().await,
        _ => first_node().await,
    }
}

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
    let addrs = reserved_addrs(2)?;
    start_endpoint(addrs[0])?;
    let mut changes = cluster::changes().context("subscribing to the changes")?;
    let mut previous = cluster::cluster_state()
        .context("reading the cluster state")?
        .borrow()
        .clone();
    cluster::form().context("forming the cluster")?;
    let mut second_node = spawn_second_node(addrs[1], addrs[0])?;

    let name = |addr| {
        if addr == addrs[0] {
            "first node"
        } else {
            "second node"
        }
    };
    let observe = async {
        loop {
            let Change::State(state) = changes.next().await else {
                continue;
            };
            for transition in state.diff(&previous) {
                let member = transition.member().addr();
                println!("{}: {}", name(member), describe(&transition));
                if matches!(transition, Transition::Left(_)) && member == addrs[1] {
                    return;
                }
            }
            previous = state;
        }
    };
    timeout(OBSERVE_TIMEOUT, observe)
        .await
        .context("the second node did not leave within the timeout")?;

    second_node
        .wait()
        .await
        .context("awaiting the second node's exit")?;
    Ok(())
}
// ANCHOR_END: first-node

// ANCHOR: describe
fn describe(transition: &Transition) -> &'static str {
    match transition {
        Transition::Up(_) => "up",
        Transition::Left(_) => "left",
        Transition::Down(_) => "down",
        Transition::Forgotten(_) => "forgotten",
        Transition::Unreachable(_) => "unreachable",
        Transition::Reachable(_) => "reachable again",
    }
}
// ANCHOR_END: describe

async fn second_node() -> anyhow::Result<()> {
    let addr = env::args()
        .nth(2)
        .context("node address argument")?
        .parse()
        .context("node address")?;
    let seed = env::args()
        .nth(3)
        .context("seed address argument")?
        .parse::<SocketAddr>()
        .context("seed address")?;
    start_endpoint(addr)?;
    timeout(JOIN_TIMEOUT, cluster::join(&[seed]))
        .await
        .context("no cluster to join within the timeout")?
        .context("joining the cluster")?;

    let system = ActorSystem::new(Idle);
    sleep(MEMBERSHIP_TIME).await;
    system.stop();
    cluster::leave_on_terminated(system)
        .await
        .context("leaving the cluster")?;
    Ok(())
}

struct Idle;

impl Actor for Idle {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

fn start_endpoint(addr: SocketAddr) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")
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

fn spawn_second_node(addr: SocketAddr, seed: SocketAddr) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(SECOND_NODE_ARG)
        .arg(addr.to_string())
        .arg(seed.to_string())
        .kill_on_drop(true)
        .spawn()
        .context("spawning the second node process")
}
