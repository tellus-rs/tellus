//! A four node cluster in four processes: an observer plus three member nodes, showing what
//! membership adds to remoting.
//!
//! Every member node is started with a single seed address, the observer's, and learns the rest
//! of the cluster from gossip. Once converged, every node's own member list names all four,
//! which the observer proves by asking each node what it sees rather than by trusting its own
//! view.
//!
//! Then one node is killed, and at the end the survivors are stopped, which shows the two ways
//! out of a cluster side by side. A node which vanishes is detected, while a node which shuts down
//! cleanly announces its own departure and is listed Left within a gossip round.
//!
//! What happens when a node is killed? Its silence is noticed by the other nodes' failure
//! detectors. Their downing providers turn that into a decision (three of four members are still
//! reachable, so the majority downs the fourth). The death watch the observer holds on an actor of
//! the killed node fires a synthesized terminated signal, the weaker of the two tiers of the watch
//! contract (see docs/cluster.md). It promises not that the actor's destructors ran, but that
//! nothing from it is ever delivered here again. Each survivor downs the node on its own clock,
//! which the observer again proves by asking them, and the cluster keeps working afterward: the
//! survivors are stopped through ordinary messages.
//!
//! All four nodes run as separate processes, because a process hosts one remoting endpoint.
//! Running this example starts the observer, which spawns the member nodes as children, so a
//! single command runs the whole cluster.
//!
//! The results are printed to stdout and tellus logs to stderr; the log level is configured via
//! `RUST_LOG`, e.g. `RUST_LOG=tellus=debug cargo run --quiet --features cluster-dev --example
//! cluster`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    env, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed,
    cluster::{
        self, EndpointConfig, Key, Manifest, MemberState, Schema,
        downing::KeepMajority,
        failure::{Deadline, DeadlineFailureDetector},
        receptionist,
        transport::QuicTransport,
    },
};
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const MEMBER_NODE_ARG: &str = "member-node";
const WORKER_KEY: &str = "worker";
const MEMBERS: usize = 3;
const FAILURE_DEADLINE: Duration = Duration::from_secs(3);
const DOWNING_DEADLINE: Duration = Duration::from_secs(2);
const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const LOOKUP_INTERVAL: Duration = Duration::from_millis(50);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const ASK_TIMEOUT: Duration = Duration::from_secs(5);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const DOWNING_TIMEOUT: Duration = Duration::from_secs(20);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(MEMBER_NODE_ARG) => member_node().await,
        _ => observer_node().await,
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

async fn member_node() -> anyhow::Result<()> {
    let bind_addr = env::args().nth(2).context("member node address argument")?;
    let seed_addr = env::args().nth(3).context("seed address argument")?;
    let addr = start_endpoint(bind_addr.parse().context("member node address")?)?;

    let seed = seed_addr.parse::<SocketAddr>().context("seed address")?;
    timeout(JOIN_TIMEOUT, cluster::join(&[seed]))
        .await
        .context("no cluster to join within the timeout")?
        .context("joining the cluster")?;

    let system = ActorSystem::new(Worker { addr });
    cluster::register(&Key::new(WORKER_KEY), system.root()).context("registering the worker")?;

    let departure = cluster::leave_on_terminated(system)
        .await
        .context("leaving the cluster once the worker has terminated")?;
    if departure.is_acknowledged() {
        println!(
            "## node {addr} left, its departure acknowledged by {} peers",
            departure.acknowledged().len()
        );
    } else {
        println!(
            "## node {addr} left, its departure not acknowledged by {:?} and timed out at {:?}",
            departure.unacknowledged(),
            departure.timed_out()
        );
    }
    Ok(())
}

/// The actor every member node registers: it answers what its node sees of the cluster, and it
/// is what the observer watches, so killing its node has an actor to synthesize a signal for.
struct Worker {
    addr: SocketAddr,
}

impl Actor for Worker {
    type Message = Job;
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
        let Incoming::Message(job) = incoming else {
            unreachable!("worker only receives Job")
        };

        match job {
            Job::View { reply_to } => {
                reply_to.reply(local_view(self.addr));
                Ok(Control::Continue(state))
            }

            Job::Stop => {
                println!("## node {} stopping", self.addr);
                Ok(Control::Stop)
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Job {
    View { reply_to: ReplyTo<View> },
    Stop,
}

impl Schema for Job {}

impl Manifest for Job {
    const MANIFEST: &'static str = "job";
}

/// What one node sees of the cluster, next to the address of the node seeing it.
#[derive(Serialize, Deserialize)]
struct View {
    addr: SocketAddr,
    members: Vec<(SocketAddr, MemberState)>,
}

impl Schema for View {}

async fn observer_node() -> anyhow::Result<()> {
    let observer = start_endpoint(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    cluster::form().context("forming the cluster")?;
    println!("## observer {observer} formed a cluster of one");

    let addrs = reserved_addrs(MEMBERS)?;
    let mut nodes = Vec::with_capacity(MEMBERS);
    for addr in &addrs {
        nodes.push(spawn_member_node(*addr, observer)?);
    }
    println!("## spawned {MEMBERS} member nodes, each seeded with {observer} and nothing else");

    let mut workers = Vec::with_capacity(MEMBERS);
    for addr in &addrs {
        workers.push(lookup_worker(*addr).await?);
    }

    let mut all = vec![observer];
    all.extend(&addrs);
    let converged = timeout(
        CONVERGENCE_TIMEOUT,
        await_views(observer, &workers, |view| sees_all_up(view, &all)),
    )
    .await
    .context("the cluster did not converge within the timeout")??;
    println!("## every node sees the whole cluster:");
    print_views(&converged);

    let victim = addrs[0];
    println!("## watching the worker on {victim}, then killing that node");
    let system = ActorSystem::new(Watcher {
        worker: workers[0].clone(),
        addr: victim,
    });
    nodes[0].kill().await.context("killing the victim node")?;

    timeout(DOWNING_TIMEOUT, system.terminated())
        .await
        .context("no terminated signal for the killed node within the timeout")?
        .context("awaiting the watcher's termination")?;

    let survivors = &workers[1..];
    let downed = timeout(
        DOWNING_TIMEOUT,
        await_views(observer, survivors, |view| sees_down(view, victim)),
    )
    .await
    .context("the cluster did not down the killed node within the timeout")??;
    println!("## every surviving node has downed it:");
    print_views(&downed);

    for worker in survivors {
        worker.tell(Job::Stop);
    }
    timeout(LEAVE_TIMEOUT, await_left(&addrs[1..]))
        .await
        .context("the stopped nodes did not leave within the timeout")??;
    println!("## the stopped nodes announced their departure and are listed Left at once");

    for node in &mut nodes[1..] {
        node.wait()
            .await
            .context("awaiting a member node process")?;
    }
    Ok(())
}

/// A node which stops announces its departure, so the observer lists it Left within a gossip
/// round instead of waiting out failure detection plus downing as it does for the killed node.
async fn await_left(addrs: &[SocketAddr]) -> anyhow::Result<()> {
    loop {
        let members = cluster::members().context("listing the members")?;
        let left = addrs.iter().all(|addr| {
            members
                .iter()
                .any(|member| member.addr() == *addr && member.state() == MemberState::Left)
        });
        if left {
            return Ok(());
        }

        sleep(POLL_INTERVAL).await;
    }
}

/// Watches an actor on the node about to be killed: its terminated signal is synthesized by the
/// downing, since no wire frame can arrive from a node which is gone.
struct Watcher {
    worker: ActorRef<Job>,
    addr: SocketAddr,
}

impl Actor for Watcher {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.worker);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(_) = incoming else {
            return Ok(Control::Continue(state));
        };

        println!(
            "## synthesized terminated signal for the worker on {}",
            self.addr
        );
        Ok(Control::Stop)
    }
}

/// The deadlines are deliberately impatient, so a killed node is downed within seconds and the
/// example finishes quickly; the defaults are the adaptive [PhiAccrualFailureDetector] and
/// [KeepMajority] with a ten second deadline.
///
/// [PhiAccrualFailureDetector]: tellus::cluster::failure::PhiAccrualFailureDetector
fn start_endpoint(bind_addr: SocketAddr) -> anyhow::Result<SocketAddr> {
    let transport = QuicTransport::dev(bind_addr).context("dev QUIC transport")?;
    let advertised_addr = transport.local_addr().context("local transport address")?;

    let failure_deadline = Deadline::new(FAILURE_DEADLINE).context("failure deadline")?;

    let mut config = EndpointConfig::new(advertised_addr);
    config.failure_detector =
        Arc::new(move || Box::new(DeadlineFailureDetector::new(failure_deadline)));
    config.downing_provider = Arc::new(|| Box::new(KeepMajority::new(DOWNING_DEADLINE)));

    cluster::start_endpoint(config, transport).context("remoting endpoint")?;
    Ok(advertised_addr)
}

/// Addresses the OS has just handed out and nothing holds anymore, so the nodes about to be
/// spawned can bind them. Every socket is held until the last address is read, else one port
/// could be handed out twice.
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

fn spawn_member_node(addr: SocketAddr, seed: SocketAddr) -> anyhow::Result<Child> {
    Command::new(env::current_exe()?)
        .arg(MEMBER_NODE_ARG)
        .arg(addr.to_string())
        .arg(seed.to_string())
        .kill_on_drop(true)
        .spawn()
        .context("spawning a member node process")
}

/// The directory answers empty until the node there has joined and its registrations have been
/// replicated here, so the lookup is retried until one deadline passes.
async fn lookup_worker(addr: SocketAddr) -> anyhow::Result<ActorRef<Job>> {
    let key = Key::new(WORKER_KEY);

    let lookup = async {
        loop {
            let workers = receptionist::lookup_at(&key, addr).context("resolving a worker")?;
            match workers.into_iter().next() {
                Some(worker) => return anyhow::Ok(worker),
                None => sleep(LOOKUP_INTERVAL).await,
            }
        }
    };

    timeout(LOOKUP_TIMEOUT, lookup)
        .await
        .with_context(|| format!("no worker registered at {addr} within {LOOKUP_TIMEOUT:?}"))?
}

fn sees_all_up(view: &View, all: &[SocketAddr]) -> bool {
    all.iter().all(|addr| {
        view.members
            .iter()
            .any(|(member, state)| member == addr && *state == MemberState::Up)
    })
}

fn sees_down(view: &View, addr: SocketAddr) -> bool {
    view.members
        .iter()
        .any(|(member, state)| *member == addr && *state == MemberState::Down)
}

/// Polls what every node sees, this one locally and the others by asking them, until all of them
/// have settled. Asking rather than gossiping a verdict is the point: each node decides on its
/// own, so only its own answer proves what it sees.
async fn await_views<F>(
    observer: SocketAddr,
    workers: &[ActorRef<Job>],
    settled: F,
) -> anyhow::Result<Vec<View>>
where
    F: Fn(&View) -> bool,
{
    loop {
        let mut views = vec![local_view(observer)];
        for worker in workers {
            let view = worker
                .ask(ASK_TIMEOUT, |reply_to| Job::View { reply_to })
                .await
                .context("asking a node what it sees")?;
            views.push(view);
        }

        if views.iter().all(&settled) {
            return Ok(views);
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// This node's own view; anything asking for it runs past [start_endpoint].
fn local_view(addr: SocketAddr) -> View {
    let members = cluster::members()
        .expect("remoting endpoint started")
        .iter()
        .map(|member| (member.addr(), member.state()))
        .collect();

    View { addr, members }
}

fn print_views(views: &[View]) {
    for view in views {
        let members = view
            .members
            .iter()
            .map(|(addr, state)| format!("{addr} {state}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("##   {} sees {members}", view.addr);
    }
}
