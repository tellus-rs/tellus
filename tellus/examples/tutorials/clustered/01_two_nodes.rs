//! Step 1 of the clustered actors tutorial: two nodes find each other through a fixed list of
//! seeds and form a cluster.
//!
//! A process hosts one remoting endpoint, so each node is a process of its own. Running this
//! example starts the first node, which spawns itself again as the second node, so a single
//! command runs both. Only the first node prints to stdout; tellus logs to stderr, configured via
//! `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! clustered_01_two_nodes`.

use anyhow::Context;
use std::{
    env, future, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};
use tellus::cluster::{
    self, BootstrapConfig, EndpointConfig, FixedSeeds, transport::QuicTransport,
};
use tokio::{
    process::{Child, Command},
    time::timeout,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const NODE_ARG: &str = "node";
const NODES: usize = 2;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(NODE_ARG) => second_node().await,
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
    let _second_node = spawn_node(seeds[1], &seeds)?;
    join_cluster(seeds[0], seeds).await?;

    let mut state = cluster::cluster_state().context("subscribing to the cluster state")?;
    let state = timeout(
        BOOTSTRAP_TIMEOUT,
        state.wait_for(|state| state.up().count() == NODES),
    )
    .await
    .context("the second node did not come up within the timeout")?;
    println!("Members up: {}", state.up().count());
    Ok(())
}
// ANCHOR_END: first-node

// ANCHOR: second-node
async fn second_node() -> anyhow::Result<()> {
    let addr = env::args()
        .nth(2)
        .context("node address argument")?
        .parse()
        .context("node address")?;
    let seeds = env::args()
        .skip(3)
        .map(|seed| seed.parse().context("seed address"))
        .collect::<anyhow::Result<_>>()?;
    join_cluster(addr, seeds).await?;

    future::pending().await
}
// ANCHOR_END: second-node

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

fn spawn_node(addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    Command::new(env::current_exe()?)
        .arg(NODE_ARG)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning the second node process")
}
