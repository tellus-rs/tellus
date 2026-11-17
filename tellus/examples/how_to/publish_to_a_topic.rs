//! How-to example: publish to a topic. Both nodes of a two-node cluster subscribe a listener to
//! the topic before they bootstrap, so once the receptionist has settled, a lookup on the first
//! node lists both. One publish then tells the local listener and the remote one, and each
//! answers with a receipt.
//!
//! The first node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example
//! how_to_publish_to_a_topic`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, Unstashed, Untimed,
    cluster::{
        self, BootstrapConfig, EndpointConfig, FixedSeeds, Manifest, Schema,
        pubsub::{self, Topic},
        receptionist,
        transport::QuicTransport,
    },
};
use tokio::{
    process::{self, Child},
    select,
    sync::mpsc::{self, UnboundedSender},
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const SUBSCRIBER_NODE_ARG: &str = "subscriber-node";
const TOPIC: &str = "notices";
const NODES: usize = 2;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
const SETTLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(5);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(SUBSCRIBER_NODE_ARG) => subscriber_node().await,
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
    let mut subscriber_node = spawn_node(addrs[1], &addrs)?;
    let topic = Topic::new(TOPIC);
    start_endpoint(addrs[0])?;
    let listener = ActorSystem::new(Listener { addr: addrs[0] });
    pubsub::subscribe(&topic, listener.root()).context("subscribing to the topic")?;
    bootstrap(addrs.clone()).await?;

    await_settled().await?;
    let subscribers = receptionist::lookup(topic.key()).context("looking up the subscribers")?;
    println!("Subscribers: {}", subscribers.len());

    let (receipts_tx, mut receipts_rx) = mpsc::unbounded_channel();
    let collector = ActorSystem::new(Collector {
        receipts: receipts_tx,
    });
    let notice = Notice {
        text: "maintenance at noon".to_string(),
        receipts: collector.root().clone(),
    };
    let told = pubsub::publish(&topic, notice).context("publishing the notice")?;
    println!("Told: {told}");

    let mut receivers = Vec::new();
    for _ in 0..told {
        let Receipt { addr, text } = timeout(RECEIPT_TIMEOUT, receipts_rx.recv())
            .await
            .context("no receipt within the timeout")?
            .context("the collector has stopped")?;
        let node = addrs
            .iter()
            .position(|node| *node == addr)
            .context("a receipt from an unknown node")?;
        receivers.push((node + 1, text));
    }
    receivers.sort();
    for (node, text) in receivers {
        println!("Node {node} received: {text}");
    }

    let status = timeout(EXIT_TIMEOUT, subscriber_node.wait())
        .await
        .context("the subscriber node did not exit within the timeout")?
        .context("awaiting the subscriber node's exit")?;
    anyhow::ensure!(status.success(), "the subscriber node failed: {status}");
    collector.stop();
    collector
        .terminated()
        .await
        .context("awaiting the collector's termination")?;
    listener
        .terminated()
        .await
        .context("awaiting the listener's termination")
}
// ANCHOR_END: first-node

// ANCHOR: await-settled
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
// ANCHOR_END: await-settled

// ANCHOR: subscriber-node
async fn subscriber_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    start_endpoint(addr)?;
    let listener = ActorSystem::new(Listener { addr });
    pubsub::subscribe(&Topic::new(TOPIC), listener.root()).context("subscribing to the topic")?;
    bootstrap(seeds).await?;

    let departure = cluster::leave_on_terminated(listener)
        .await
        .context("leaving the cluster")?;
    anyhow::ensure!(
        departure.is_acknowledged(),
        "departure not acknowledged by {:?}, timed out for {:?}",
        departure.unacknowledged(),
        departure.timed_out()
    );
    Ok(())
}
// ANCHOR_END: subscriber-node

// ANCHOR: listener
struct Listener {
    addr: SocketAddr,
}

impl Actor for Listener {
    type Message = Notice;
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
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Notice { text, receipts }) = incoming else {
            unreachable!("the listener watches no actor, hence never gets a terminated signal")
        };

        receipts.tell(Receipt {
            addr: self.addr,
            text,
        });
        Ok(Control::Stop)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Notice {
    text: String,
    receipts: ActorRef<Receipt>,
}

impl Schema for Notice {}

impl Manifest for Notice {
    const MANIFEST: &'static str = "notice";
}
// ANCHOR_END: listener

struct Collector {
    receipts: UnboundedSender<Receipt>,
}

impl Actor for Collector {
    type Message = Receipt;
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
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(receipt) = incoming else {
            unreachable!("the collector watches no actor, hence never gets a terminated signal")
        };

        match self.receipts.send(receipt) {
            Ok(()) => Ok(Control::Continue(state)),
            Err(_) => Ok(Control::Stop),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    addr: SocketAddr,
    text: String,
}

impl Schema for Receipt {}

impl Manifest for Receipt {
    const MANIFEST: &'static str = "receipt";
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

fn spawn_node(addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(SUBSCRIBER_NODE_ARG)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning the subscriber node process")
}
