//! How-to example: form a cluster although the lowest seed never starts. The default formation
//! provider, `Majority`, lets only the lowest address of the seeds form, so two of three seeds
//! would wait for the missing one forever. `MajorityWithFallback` passes over the lowest address
//! once it has been unavailable for a while, so the second seed forms and the third joins it,
//! then leaves again.
//!
//! The first node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! how_to_form_without_the_lowest_seed`.

use anyhow::Context;
use std::{
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};
use tellus::cluster::{
    self, BootstrapConfig, EndpointConfig, FixedSeeds, MemberState,
    formation::MajorityWithFallback, transport::QuicTransport,
};
use tokio::{
    process::{self, Child},
    time::timeout,
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const SECOND_NODE_ARG: &str = "second-node";
const SEEDS: usize = 3;
const FALLBACK_AFTER: Duration = Duration::from_secs(2);
const JOIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(SECOND_NODE_ARG) => second_node().await,
        _ => first_node().await,
    }
}
// ANCHOR_END: main

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(io::stderr),
        )
        .init();
}

// ANCHOR: first-node
async fn first_node() -> anyhow::Result<()> {
    let mut seeds = free_addrs(SEEDS)?;
    seeds.sort();
    let mut second_node = spawn_second_node(seeds[2], &seeds)?;
    join_cluster(seeds[1], seeds.clone()).await?;

    let mut states = cluster::cluster_state().context("subscribing to the cluster state")?;
    let left = states.wait_for(|state| {
        state
            .members()
            .iter()
            .any(|member| member.addr() == seeds[2] && member.state() == MemberState::Left)
    });
    timeout(BOOTSTRAP_TIMEOUT + LEAVE_TIMEOUT, left)
        .await
        .context("the second node did not leave within the timeout")?;
    let status = timeout(EXIT_TIMEOUT, second_node.wait())
        .await
        .context("the second node did not exit within the timeout")?
        .context("awaiting the second node's exit")?;
    anyhow::ensure!(status.success(), "the second node failed: {status}");

    let members = cluster::members().context("reading the members")?;
    for (index, seed) in seeds.iter().enumerate() {
        let state = members
            .iter()
            .find(|member| member.addr() == *seed)
            .map_or_else(
                || "never a member".to_string(),
                |member| member.state().to_string(),
            );
        println!("Seed {}: {state}", index + 1);
    }
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

    let mut states = cluster::cluster_state().context("subscribing to the cluster state")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        states.wait_for(|state| state.up().count() == SEEDS - 1),
    )
    .await
    .context("not both nodes up within the timeout")?;
    let departure = cluster::leave().await.context("leaving the cluster")?;
    anyhow::ensure!(
        departure.is_acknowledged(),
        "departure not acknowledged by {:?}, timed out for {:?}",
        departure.unacknowledged(),
        departure.timed_out()
    );
    Ok(())
}
// ANCHOR_END: second-node

// ANCHOR: join-cluster
async fn join_cluster(addr: SocketAddr, seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")?;
    let config = BootstrapConfig {
        join_attempt_timeout: JOIN_ATTEMPT_TIMEOUT,
        formation_provider: Arc::new(|| Box::new(MajorityWithFallback::new(FALLBACK_AFTER))),
        ..BootstrapConfig::new()
    };
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(FixedSeeds::new(seeds)?, config),
    )
    .await
    .context("no cluster within the timeout")?
    .context("bootstrapping the cluster")
}
// ANCHOR_END: join-cluster

fn free_addrs(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    // Every socket stays bound until the last address is read, else a port could repeat!
    let sockets = (0..count)
        .map(|_| UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
        .collect::<Result<Vec<_>, _>>()
        .context("binding free loopback ports")?;
    sockets
        .iter()
        .map(|socket| socket.local_addr().context("free local address"))
        .collect()
}

fn spawn_second_node(addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(SECOND_NODE_ARG)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning the second node process")
}
