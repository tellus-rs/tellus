//! How-to example: remove a member from a cluster of three. The second node removes itself by
//! leaving, which every peer acknowledges. The first node then downs the third, which learns of
//! it and exits, since only a restarted process may rejoin.
//!
//! The first node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! how_to_remove_a_member`.

use anyhow::Context;
use std::{
    env,
    io::{self, BufRead},
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    process::Stdio,
    time::Duration,
};
use tellus::cluster::{
    self, BootstrapConfig, EndpointConfig, FixedSeeds, Lifecycle, MemberState, receptionist,
    transport::QuicTransport,
};
use tokio::{
    process::{self, Child},
    select, task,
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const LEAVING_NODE_ARG: &str = "leaving-node";
const DOWNED_NODE_ARG: &str = "downed-node";
const NODES: usize = 3;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const REMOVE_TIMEOUT: Duration = Duration::from_secs(20);
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(LEAVING_NODE_ARG) => leaving_node().await,
        Some(DOWNED_NODE_ARG) => downed_node().await,
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
    let addrs = free_addrs(NODES)?;
    let mut leaving_node = spawn_node(LEAVING_NODE_ARG, addrs[1], &addrs)?;
    let mut downed_node = spawn_node(DOWNED_NODE_ARG, addrs[2], &addrs)?;
    join_cluster(addrs[0], addrs.clone()).await?;
    await_all_up().await?;
    drop(leaving_node.stdin.take());

    await_member_state(addrs[1], MemberState::Left).await?;
    println!("The second node has left");
    await_exit(&mut leaving_node, "second").await?;

    cluster::down(addrs[2]).context("downing the third node")?;
    await_member_state(addrs[2], MemberState::Down).await?;
    println!("The third node is down");
    await_exit(&mut downed_node, "third").await?;

    let up = cluster::members()
        .context("reading the members")?
        .iter()
        .filter(|member| member.state() == MemberState::Up)
        .count();
    println!("Up members: {up}");
    Ok(())
}
// ANCHOR_END: first-node

// ANCHOR: await-member-state
async fn await_member_state(addr: SocketAddr, member_state: MemberState) -> anyhow::Result<()> {
    let mut states = cluster::cluster_state().context("subscribing to the cluster state")?;
    let reached = states.wait_for(|state| {
        state
            .members()
            .iter()
            .any(|member| member.addr() == addr && member.state() == member_state)
    });
    timeout(REMOVE_TIMEOUT, reached)
        .await
        .with_context(|| format!("{addr} not {member_state} within the timeout"))?;
    Ok(())
}
// ANCHOR_END: await-member-state

// ANCHOR: leaving-node
async fn leaving_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    join_cluster(addr, seeds).await?;
    await_settled().await?;
    await_stdin_closed().await?;

    let departure = cluster::leave().await.context("leaving the cluster")?;
    anyhow::ensure!(
        departure.is_acknowledged(),
        "departure not acknowledged by {:?}, timed out for {:?}",
        departure.unacknowledged(),
        departure.timed_out()
    );
    Ok(())
}
// ANCHOR_END: leaving-node

// ANCHOR: downed-node
async fn downed_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    join_cluster(addr, seeds).await?;

    let downed = async {
        while cluster::lifecycle().context("reading the lifecycle")? != Lifecycle::Downed {
            sleep(LIFECYCLE_POLL_INTERVAL).await;
        }
        anyhow::Ok(())
    };
    timeout(REMOVE_TIMEOUT + EXIT_TIMEOUT, downed)
        .await
        .context("not downed within the timeout")?
}
// ANCHOR_END: downed-node

async fn await_all_up() -> anyhow::Result<()> {
    let mut states = cluster::cluster_state().context("subscribing to the cluster state")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        states.wait_for(|state| state.up().count() == NODES),
    )
    .await
    .context("not all nodes up within the timeout")?;
    Ok(())
}

async fn await_settled() -> anyhow::Result<()> {
    let mut states = cluster::cluster_state().context("subscribing to the cluster state")?;
    let settle = async {
        loop {
            let state = states.wait_for(|state| state.up().count() == NODES).await;
            if receptionist::settled(state.version()).context("asking for the settlement")? {
                return anyhow::Ok(());
            }
            select! {
                () = states.changed() => {}
                () = sleep(SETTLE_POLL_INTERVAL) => {}
            }
        }
    };
    timeout(SETTLE_TIMEOUT, settle)
        .await
        .context("the receptionist did not settle within the timeout")?
}

async fn await_stdin_closed() -> anyhow::Result<()> {
    task::spawn_blocking(|| {
        let mut stdin = io::stdin().lock();
        let mut line = String::new();
        while stdin.read_line(&mut line).context("reading stdin")? > 0 {
            line.clear();
        }
        anyhow::Ok(())
    })
    .await
    .context("awaiting the closed stdin")?
}

async fn await_exit(node: &mut Child, name: &str) -> anyhow::Result<()> {
    let status = timeout(EXIT_TIMEOUT, node.wait())
        .await
        .with_context(|| format!("the {name} node did not exit within the timeout"))?
        .with_context(|| format!("awaiting the {name} node's exit"))?;
    anyhow::ensure!(status.success(), "the {name} node failed: {status}");
    Ok(())
}

async fn join_cluster(addr: SocketAddr, seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(FixedSeeds::new(seeds)?, BootstrapConfig::new()),
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

fn spawn_node(role: &str, addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(role)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("spawning a node process")
}
