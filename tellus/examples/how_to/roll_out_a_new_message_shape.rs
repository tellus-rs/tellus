//! How-to example: roll out a new message shape. The first node runs application version 2.0.0,
//! whose greeting carries a language, the legacy node version 1.0.0, whose greeting does not.
//! Each asks the other's greeter: the first node encodes its greeting in the old shape for the
//! legacy node, and decodes the legacy node's greeting by upcasting it.
//!
//! Both nodes print to stdout, the legacy node first; tellus logs to stderr, configured via
//! `RUST_LOG`, e.g. `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev
//! --example how_to_roll_out_a_new_message_shape`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, ReplyTo, SchemaVersion,
    Unstashed, Untimed,
    cluster::{
        self, AppVersion, BootstrapConfig, EndpointConfig, FixedSeeds, Key, Manifest, Peer, Schema,
        UnsupportedSchemaVersion,
        codec::{Codec, CodecError},
        receptionist,
        transport::QuicTransport,
    },
};
use tokio::{
    process::{self, Child},
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const LEGACY_NODE_ARG: &str = "legacy-node";
const GREETER_KEY: &str = "greeter";
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);
const FIND_TIMEOUT: Duration = Duration::from_secs(10);
const FIND_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ASK_TIMEOUT: Duration = Duration::from_secs(5);
const GREETED_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(LEGACY_NODE_ARG) => legacy_node().await,
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
    let addrs = free_addrs(2)?;
    let mut legacy_node = spawn_legacy_node(addrs[1], &addrs)?;
    start_endpoint(addrs[0], AppVersion::new(2, 0, 0))?;
    let greeter = ActorSystem::new(Greeter);
    cluster::register(&Key::new(GREETER_KEY), greeter.root()).context("registering the greeter")?;
    bootstrap(addrs.clone()).await?;

    let legacy_greeter = find_greeter(addrs[1]).await?;
    let answer = legacy_greeter
        .ask(ASK_TIMEOUT, |reply_to| Greeting {
            name: "Ann".to_string(),
            language: "de".to_string(),
            reply_to,
        })
        .await
        .context("asking the legacy greeter")?;

    let status = timeout(EXIT_TIMEOUT, legacy_node.wait())
        .await
        .context("the legacy node did not exit within the timeout")?
        .context("awaiting the legacy node's exit")?;
    anyhow::ensure!(status.success(), "the legacy node failed: {status}");
    println!("The legacy node answered: {answer}");

    greeter.stop();
    greeter
        .terminated()
        .await
        .context("awaiting the greeter's termination")
}
// ANCHOR_END: first-node

// ANCHOR: legacy-node
async fn legacy_node() -> anyhow::Result<()> {
    let (addr, seeds) = node_args()?;
    start_endpoint(addr, AppVersion::new(1, 0, 0))?;
    let greeter = ActorSystem::new(LegacyGreeter);
    cluster::register(&Key::new(GREETER_KEY), greeter.root())
        .context("registering the legacy greeter")?;
    bootstrap(seeds.clone()).await?;

    let current_greeter = find_greeter(seeds[0]).await?;
    let answer = current_greeter
        .ask(ASK_TIMEOUT, |reply_to| LegacyGreeting {
            name: "Bob".to_string(),
            reply_to,
        })
        .await
        .context("asking the current greeter")?;
    println!("The current node answered: {answer}");

    let leave = cluster::leave_on_terminated(greeter);
    let departure = timeout(GREETED_TIMEOUT, leave)
        .await
        .context("not greeted within the timeout")?
        .context("leaving the cluster")?;
    anyhow::ensure!(
        departure.is_acknowledged(),
        "departure not acknowledged by {:?}, timed out for {:?}",
        departure.unacknowledged(),
        departure.timed_out()
    );
    Ok(())
}
// ANCHOR_END: legacy-node

async fn find_greeter<M>(addr: SocketAddr) -> anyhow::Result<ActorRef<M>>
where
    M: Manifest + Send + 'static,
{
    let find = async {
        loop {
            let greeters = receptionist::lookup_at(&Key::new(GREETER_KEY), addr)
                .context("looking up the greeter")?;
            match greeters.into_iter().next() {
                Some(greeter) => return anyhow::Ok(greeter),
                None => sleep(FIND_POLL_INTERVAL).await,
            }
        }
    };
    timeout(FIND_TIMEOUT, find)
        .await
        .context("no greeter registered within the timeout")?
}

// ANCHOR: greeter
struct Greeter;

impl Actor for Greeter {
    type Message = Greeting;
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
        let Incoming::Message(Greeting {
            name,
            language,
            reply_to,
        }) = incoming
        else {
            unreachable!("the greeter watches no actor, hence never gets a terminated signal")
        };

        reply_to.reply(format!("Hello, {name} ({language})"));
        Ok(Control::Continue(state))
    }
}
// ANCHOR_END: greeter

// ANCHOR: greeting
#[derive(Serialize, Deserialize)]
struct Greeting {
    name: String,
    language: String,
    reply_to: ReplyTo<String>,
}

impl Schema for Greeting {
    const VERSION: SchemaVersion = SchemaVersion::new(1);

    fn encode(&self, codec: Codec, peer: &Peer) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
        if peer.app_version().is_none_or(|version| version.major < 2) {
            let view = GreetingV0View {
                name: &self.name,
                reply_to: &self.reply_to,
            };
            return Ok((SchemaVersion::new(0), codec.encode(&view)?));
        }
        Ok((Self::VERSION, codec.encode(self)?))
    }

    fn decode(
        codec: Codec,
        schema_version: SchemaVersion,
        payload: &[u8],
    ) -> Result<Self, CodecError> {
        if schema_version == SchemaVersion::new(0) {
            let LegacyGreeting { name, reply_to } = codec.decode(payload)?;
            return Ok(Greeting {
                name,
                language: "en".to_string(),
                reply_to,
            });
        }
        if schema_version == Self::VERSION {
            return codec.decode(payload);
        }
        Err(CodecError::make_decoding(UnsupportedSchemaVersion::new::<
            Self,
        >(schema_version)))
    }
}

impl Manifest for Greeting {
    const MANIFEST: &'static str = "greeting";
}

#[derive(Serialize)]
struct GreetingV0View<'a> {
    name: &'a str,
    reply_to: &'a ReplyTo<String>,
}
// ANCHOR_END: greeting

// ANCHOR: legacy-greeter
struct LegacyGreeter;

impl Actor for LegacyGreeter {
    type Message = LegacyGreeting;
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
        let Incoming::Message(LegacyGreeting { name, reply_to }) = incoming else {
            unreachable!("the greeter watches no actor, hence never gets a terminated signal")
        };

        reply_to.reply(format!("Hello, {name}"));
        Ok(Control::Stop)
    }
}

#[derive(Serialize, Deserialize)]
struct LegacyGreeting {
    name: String,
    reply_to: ReplyTo<String>,
}

impl Schema for LegacyGreeting {}

impl Manifest for LegacyGreeting {
    const MANIFEST: &'static str = "greeting";
}
// ANCHOR_END: legacy-greeter

// ANCHOR: start-endpoint
fn start_endpoint(addr: SocketAddr, app_version: AppVersion) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(addr).context("dev QUIC transport")?;
    let config = EndpointConfig {
        app_version: Some(app_version),
        ..EndpointConfig::new(addr)
    };
    cluster::start_endpoint(config, transport).context("remoting endpoint")
}
// ANCHOR_END: start-endpoint

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

fn spawn_legacy_node(addr: SocketAddr, seeds: &[SocketAddr]) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(LEGACY_NODE_ARG)
        .arg(addr.to_string())
        .args(seeds.iter().map(SocketAddr::to_string))
        .kill_on_drop(true)
        .spawn()
        .context("spawning the legacy node process")
}
