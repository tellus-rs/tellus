//! How-to example: hand over an actor reference out of band. The first node serializes the
//! reference of its greeter and passes it to the second node on the command line, in hex. The
//! second node deserializes it once it has joined the cluster and asks the greeter.
//!
//! The second node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! how_to_hand_over_a_reference`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed,
    cluster::{
        self, BootstrapConfig, EndpointConfig, FixedSeeds, Manifest, Schema,
        transport::QuicTransport,
    },
};
use tokio::{
    process::{self, Child},
    time::timeout,
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const SECOND_NODE_ARG: &str = "second-node";
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const ASK_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);

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
    let mut addrs = free_addrs(2)?;
    addrs.sort();
    start_endpoint(addrs[0])?;
    let greeter = ActorSystem::new(Greeter);
    let reference = cluster::serialize_ref(greeter.root()).context("serializing the greeter")?;
    let mut second_node = spawn_second_node(addrs[1], &to_hex(&reference), &addrs)?;
    bootstrap(addrs.clone()).await?;

    let status = timeout(EXIT_TIMEOUT, second_node.wait())
        .await
        .context("the second node did not exit within the timeout")?
        .context("awaiting the second node's exit")?;
    anyhow::ensure!(status.success(), "the second node failed: {status}");
    greeter.stop();
    greeter
        .terminated()
        .await
        .context("awaiting the greeter's termination")
}
// ANCHOR_END: first-node

// ANCHOR: second-node
async fn second_node() -> anyhow::Result<()> {
    let addr = env::args()
        .nth(2)
        .context("node address argument")?
        .parse()
        .context("node address")?;
    let reference = from_hex(&env::args().nth(3).context("reference argument")?)?;
    let seeds = env::args()
        .skip(4)
        .map(|seed| seed.parse().context("seed address"))
        .collect::<anyhow::Result<_>>()?;
    start_endpoint(addr)?;
    bootstrap(seeds).await?;

    let greeter =
        cluster::deserialize_ref::<Greet>(&reference).context("deserializing the greeter")?;
    let greeting = greeter
        .ask(ASK_TIMEOUT, |reply_to| Greet {
            name: "Bob".to_string(),
            reply_to,
        })
        .await
        .context("asking the greeter")?;
    println!("The greeter answered: {greeting}");

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

struct Greeter;

impl Actor for Greeter {
    type Message = Greet;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = !;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Greet { name, reply_to }) = incoming else {
            unreachable!("the greeter watches no actor, hence never gets a terminated signal")
        };

        reply_to.reply(format!("Hello, {name}"));
        Ok(Control::Continue(state))
    }
}

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
    reply_to: ReplyTo<String>,
}

impl Schema for Greet {}

impl Manifest for Greet {
    const MANIFEST: &'static str = "greet";
}

fn start_endpoint(addr: SocketAddr) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")
}

async fn bootstrap(seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(FixedSeeds::new(seeds)?, BootstrapConfig::new()),
    )
    .await
    .context("no cluster within the timeout")?
    .context("bootstrapping the cluster")
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(hex: &str) -> anyhow::Result<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            let digits = hex
                .get(index..index + 2)
                .context("an odd number of digits")?;
            u8::from_str_radix(digits, 16).context("not a hex digit")
        })
        .collect()
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

fn spawn_second_node(
    addr: SocketAddr,
    reference: &str,
    seeds: &[SocketAddr],
) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(SECOND_NODE_ARG)
        .arg(addr.to_string())
        .arg(reference)
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning the second node process")
}
