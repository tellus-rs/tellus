//! A node of the cluster demo: a tellus remoting endpoint which bootstraps via DNS or Kubernetes
//! seed discovery and registers a worker under a well known key. It serves an HTTP API answering
//! what it sees of the cluster and whether it can still message every other member.
//!
//! Configured by `config/default.yaml` plus one of the `config/dns.yaml` and `config/k8s.yaml`
//! overlays, which is what chooses the discovery. The per-node `CFG__` environment overrides
//! [configured](https://github.com/hseeberger/configured) layers on top, see `Config`. A node
//! downed by the cluster exits, e.g. on the minority side of a partition, which downs itself. The
//! orchestrator's restart then mints the fresh incarnation which alone can rejoin.

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use configured::{Configured, LoadOptions};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, Untimed,
    cluster::{
        self, BootstrapConfig, Change, ClusterState, EndpointConfig, Key, Manifest, Member,
        MemberState, SeedDiscovery, Transition,
        pubsub::{self, Topic},
        receptionist,
        transport::{ConnectedControl, QuicConnection, QuicTransport, Transport, TransportError},
    },
};
use tellus_bootstrap_dns::{DnsSeeds, Query};
use tellus_bootstrap_k8s::{K8sSeeds, Pods};
use tellus_cluster_demo::{ClusterView, NodeEvent, Phase, Probe, ProbeOutcome, ProbeReport};
use tellus_cluster_inspect::{
    InspectConfig,
    events::{self, EventLog, now_millis},
};
use tokio::{
    join,
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// The latest event's sequence number and arrival per publisher, shared by the listener writing
/// it and the HTTP API reading it.
type Heard = Arc<Mutex<BTreeMap<SocketAddr, (u64, Instant)>>>;

const HTTP_PORT: u16 = 8080;
const WORKER_KEY: &str = "worker";
const EVENT_TOPIC: &str = "events";
const PUBLISH_INTERVAL: Duration = Duration::from_secs(1);
const EVENT_FRESHNESS: Duration = Duration::from_secs(3);
const EVENTS_KEPT: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
// Must exceed the time a load balancer takes to drop a node that fails its health check.
const UNHEALTHY_LINGER: Duration = Duration::from_secs(5);
const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);
const ASK_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::load(LoadOptions::default()).context("load configuration")?;
    let addr = config.endpoint.advertised_addr;
    start_endpoint(config.endpoint)?;

    let state_rx = cluster::cluster_state().context("subscribing to the cluster state")?;
    let inspect =
        tellus_cluster_inspect::router(InspectConfig::default()).context("inspect router")?;
    let state = Arc::new(NodeState::new(config.node_name.clone(), addr, state_rx));
    tokio::spawn(serve_http(state.clone(), inspect));
    tokio::spawn(record_events(state.clone()));
    tokio::spawn(log_changes());
    info!(name = %config.node_name, %addr, "endpoint started, bootstrapping");

    let bootstrap = match config.seeds {
        Seeds::Dns(query) => {
            let seeds = DnsSeeds::new(query).context("DNS seed discovery")?;
            bootstrap_or_shutdown(seeds, config.bootstrap).await?
        }

        Seeds::K8s(pods) => {
            let seeds = K8sSeeds::new(pods)
                .await
                .context("Kubernetes seed discovery")?;
            bootstrap_or_shutdown(seeds, config.bootstrap).await?
        }
    };
    if matches!(bootstrap, Bootstrap::Shutdown) {
        info!("shutdown requested before having joined");
        return Ok(());
    }

    let system = ActorSystem::new(Worker {
        addr,
        heard: state.heard.clone(),
    });
    cluster::register(&Key::new(WORKER_KEY), system.root()).context("registering the worker")?;
    state.set_phase(Phase::Member);
    info!(name = %config.node_name, "member of the cluster");
    let publishing = tokio::spawn(publish_events(addr));

    tokio::select! {
        () = await_downed() => {
            state.set_phase(Phase::Downed);
            error!("downed by the cluster, exiting so a restart rejoins");
            sleep(UNHEALTHY_LINGER).await;
            Err(anyhow!("downed by the cluster"))
        }

        () = await_shutdown_signal() => {
            state.set_phase(Phase::Leaving);
            info!("shutdown requested, leaving the cluster");
            publishing.abort();
            system.root().tell(Job::Stop);
            let (departure, ()) = join!(
                timeout(LEAVE_TIMEOUT, cluster::leave_on_terminated(system)),
                sleep(UNHEALTHY_LINGER)
            );
            let departure = departure
                .context("the departure was not announced within the timeout")?
                .context("leaving the cluster")?;
            if departure.is_acknowledged() {
                info!(acknowledged = ?departure.acknowledged(), "left the cluster");
            } else {
                warn!(
                    acknowledged = ?departure.acknowledged(),
                    unacknowledged = ?departure.unacknowledged(),
                    timed_out = ?departure.timed_out(),
                    "left the cluster, not every peer acknowledged the departure"
                );
            }
            Ok(())
        }
    }
}

/// This node's configuration: `config/default.yaml` for everything the five nodes share, one of
/// the `config/dns.yaml` and `config/k8s.yaml` overlays for `seeds`, the `CFG__NODE_NAME` and
/// `CFG__ENDPOINT__ADVERTISED_ADDR` environment overrides for what differs per node.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_name: String,
    seeds: Seeds,
    bootstrap: BootstrapConfig,
    endpoint: EndpointConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Seeds {
    Dns(Query),
    K8s(Pods),
}

/// What the HTTP API needs beyond the process-wide remoting endpoint.
struct NodeState {
    name: String,
    addr: SocketAddr,
    started_at: u64,
    phase: watch::Sender<Phase>,
    state_rx: watch::Receiver<ClusterState>,
    heard: Heard,
    events: Arc<EventLog<NodeEvent>>,
}

impl NodeState {
    fn new(name: String, addr: SocketAddr, state_rx: watch::Receiver<ClusterState>) -> Self {
        let started_at = now_millis();
        Self {
            name,
            addr,
            started_at,
            phase: watch::Sender::new(Phase::Bootstrapping),
            state_rx,
            heard: Heard::default(),
            events: Arc::new(EventLog::new(started_at, EVENTS_KEPT)),
        }
    }

    fn cluster_state(&self) -> ClusterState {
        self.state_rx.borrow().clone()
    }

    fn phase(&self) -> Phase {
        *self.phase.borrow()
    }

    fn set_phase(&self, phase: Phase) {
        self.phase.send_replace(phase);
    }

    fn publishers(&self) -> BTreeSet<SocketAddr> {
        let now = Instant::now();
        self.heard
            .lock()
            .expect("not poisoned")
            .iter()
            .filter(|(_, (_, at))| now.duration_since(*at) < EVENT_FRESHNESS)
            .map(|(addr, _)| *addr)
            .collect()
    }
}

enum Bootstrap {
    Member,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceptionistCounts {
    workers: usize,
    subscribers: usize,
}

/// The actor every node registers: it answers a ping, so probing proves messaging works, not
/// only that the member lists agree.
struct Worker {
    addr: SocketAddr,
    heard: Heard,
}

impl Actor for Worker {
    type Message = Job;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let listener = context.spawn(Listener {
            heard: self.heard.clone(),
        });
        pubsub::subscribe(&Topic::new(EVENT_TOPIC), &listener).expect("remoting endpoint started");
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
            Job::Ping { reply_to } => {
                reply_to.reply(Pong {
                    addr: self.addr,
                    up_members: up_addrs().len(),
                });
                Ok(Control::Continue(state))
            }

            Job::Stop => Ok(Control::Stop),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Job {
    Ping { reply_to: ReplyTo<Pong> },
    Stop,
}

impl Manifest for Job {
    const MANIFEST: &'static str = "job";
}

#[derive(Serialize, Deserialize)]
struct Pong {
    addr: SocketAddr,
    up_members: usize,
}

/// The worker's child, subscribed to the event topic: it records the latest event of every
/// publisher, so a node's view shows whom pub-sub currently reaches it from.
struct Listener {
    heard: Heard,
}

impl Actor for Listener {
    type Message = Event;
    type Timers = Untimed;
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
        if let Incoming::Message(event) = incoming {
            self.heard
                .lock()
                .expect("not poisoned")
                .insert(event.from, (event.seq, Instant::now()));
        }
        Ok(Control::Continue(state))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    from: SocketAddr,
    seq: u64,
}

impl Manifest for Event {
    const MANIFEST: &'static str = "event";
}

/// A [QuicTransport] whose connect gives up after `CONNECT_TIMEOUT`.
struct BoundedConnect(QuicTransport);

impl Transport for BoundedConnect {
    type Connection = QuicConnection;

    fn data_streams(&self) -> Option<NonZeroUsize> {
        self.0.data_streams()
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        max_frame_size: usize,
    ) -> Result<ConnectedControl<QuicConnection>, TransportError> {
        match timeout(CONNECT_TIMEOUT, self.0.connect(addr, max_frame_size)).await {
            Ok(connected) => connected,
            Err(_) => Err(TransportError::other(anyhow!("connect timeout"))),
        }
    }

    async fn accept(&self, max_frame_size: usize) -> Result<QuicConnection, TransportError> {
        self.0.accept(max_frame_size).await
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// The dev transport, which does not verify certificates: this cluster runs on a private network,
/// a production one would use [QuicTransport::mutual_tls].
///
/// Must bind the advertised address, not the unspecified one: the dev certificate carries the bind
/// address's IP, which the dialer's identity check requires to match the advertised address.
fn start_endpoint(config: EndpointConfig) -> anyhow::Result<()> {
    let transport = QuicTransport::dev(config.advertised_addr).context("dev QUIC transport")?;
    let transport = BoundedConnect(transport);

    cluster::start_endpoint(config, transport).context("remoting endpoint")
}

async fn serve_http(state: Arc<NodeState>, inspect: Router) {
    let router = Router::new()
        .route("/health", get(health))
        .route("/cluster", get(cluster))
        .route("/probe", get(probe))
        .route("/events", get(events))
        .with_state(state)
        .nest("/inspect", inspect);

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, HTTP_PORT));
    match TcpListener::bind(addr).await {
        Ok(listener) => {
            if let Err(error) = axum::serve(listener, router).await {
                error!(%error, "HTTP API stopped");
            }
        }

        Err(error) => error!(%error, %addr, "cannot bind the HTTP API"),
    }
}

/// The load balancer's health check: a node downed by the cluster or leaving it is taken out of
/// rotation before its process exits.
async fn health(State(state): State<Arc<NodeState>>) -> StatusCode {
    if matches!(state.phase(), Phase::Downed | Phase::Leaving) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

async fn cluster(State(state): State<Arc<NodeState>>) -> Json<ClusterView> {
    let ReceptionistCounts {
        workers,
        subscribers,
    } = receptionist_counts();

    // The verdict first, then the state, so the verdict is never newer than the state.
    let settlement = receptionist::settlement().expect("remoting endpoint started");
    Json(ClusterView {
        name: state.name.clone(),
        phase: state.phase(),
        state: state.cluster_state(),
        settlement,
        workers,
        subscribers,
        publishers: state.publishers(),
    })
}

fn receptionist_counts() -> ReceptionistCounts {
    let workers = receptionist::lookup(&Key::<Job>::new(WORKER_KEY))
        .map(|workers| workers.len())
        .unwrap_or_else(|error| {
            error!(%error, "cannot resolve the workers");
            0
        });
    let subscribers = receptionist::lookup(Topic::<Event>::new(EVENT_TOPIC).key())
        .map(|subscribers| subscribers.len())
        .unwrap_or_else(|error| {
            error!(%error, "cannot resolve the subscribers");
            0
        });

    ReceptionistCounts {
        workers,
        subscribers,
    }
}

async fn probe(State(state): State<Arc<NodeState>>) -> Json<ProbeReport> {
    let mut tasks = JoinSet::new();
    for addr in up_addrs().into_iter().filter(|addr| *addr != state.addr) {
        tasks.spawn(async move {
            Probe {
                addr,
                outcome: probe_member(addr).await,
            }
        });
    }

    let mut probes = tasks.join_all().await;
    probes.sort_by_key(|probe| probe.addr);

    Json(ProbeReport {
        name: state.name.clone(),
        probes,
    })
}

/// The worker is resolved from the directory on every probe rather than cached, so a member whose
/// registrations never replicated here is reported as failed; the ask is what crosses the wire.
async fn probe_member(addr: SocketAddr) -> ProbeOutcome {
    let start = Instant::now();

    let worker = match receptionist::lookup_at(&Key::new(WORKER_KEY), addr) {
        Ok(workers) => match workers.into_iter().next() {
            Some(worker) => worker,

            None => {
                return ProbeOutcome::Failed {
                    error: format!("no worker replicated from {addr}"),
                };
            }
        },

        Err(error) => {
            return ProbeOutcome::Failed {
                error: error.to_string(),
            };
        }
    };

    match worker
        .ask(ASK_TIMEOUT, |reply_to| Job::Ping { reply_to })
        .await
    {
        Ok(pong) => ProbeOutcome::Ok {
            millis: start.elapsed().as_millis(),
            up_members: pong.up_members,
        },

        Err(error) => ProbeOutcome::Failed {
            error: error.to_string(),
        },
    }
}

async fn events(State(state): State<Arc<NodeState>>, headers: HeaderMap) -> impl IntoResponse {
    let replay = state.events.replay(events::last_event_id(&headers));
    let hello = NodeEvent::Hello {
        name: state.name.clone(),
        addr: state.addr,
        started_at: state.started_at,
        resumed: replay.resumed,
    };
    let anchor = replay.anchor;

    events::sse(
        &state.events,
        replay.into_items(state.started_at),
        anchor,
        hello,
    )
}

async fn bootstrap_or_shutdown<D>(seeds: D, config: BootstrapConfig) -> anyhow::Result<Bootstrap>
where
    D: SeedDiscovery,
{
    tokio::select! {
        result = cluster::bootstrap(seeds, config) => {
            result.context("bootstrapping the cluster")?;
            Ok(Bootstrap::Member)
        }

        () = await_shutdown_signal() => Ok(Bootstrap::Shutdown),
    }
}

/// Publishes one event per interval to the topic every node's listener subscribes to, so a quiet
/// window shows fan-out reaching every member.
async fn publish_events(addr: SocketAddr) {
    let topic = Topic::new(EVENT_TOPIC);
    let mut seq = 0;
    loop {
        sleep(PUBLISH_INTERVAL).await;
        seq += 1;
        match pubsub::publish(&topic, Event { from: addr, seq }) {
            Ok(told) => debug!(seq, told, "published an event"),
            Err(error) => error!(%error, "cannot publish an event"),
        }
    }
}

async fn record_events(state: Arc<NodeState>) {
    let mut phase_rx = state.phase.subscribe();
    let mut workers =
        receptionist::subscribe(&Key::<Job>::new(WORKER_KEY)).expect("remoting endpoint started");
    let mut subscribers = receptionist::subscribe(Topic::<Event>::new(EVENT_TOPIC).key())
        .expect("remoting endpoint started");

    state.events.push(NodeEvent::Phase {
        phase: *phase_rx.borrow_and_update(),
    });
    let mut counts = receptionist_counts();
    record_counts(&state, counts);

    loop {
        tokio::select! {
            changed = phase_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                state.events.push(NodeEvent::Phase {
                    phase: *phase_rx.borrow_and_update(),
                });
            }

            () = workers.changed() => {
                if let Err(error) = workers.current() {
                    error!(%error, "cannot resolve the workers");
                }
            }

            () = subscribers.changed() => {
                if let Err(error) = subscribers.current() {
                    error!(%error, "cannot resolve the subscribers");
                }
            }
        }
        let current = receptionist_counts();
        if current != counts {
            counts = current;
            record_counts(&state, counts);
        }
    }
}

fn record_counts(state: &NodeState, counts: ReceptionistCounts) {
    state.events.push(NodeEvent::Receptionist {
        workers: counts.workers,
        subscribers: counts.subscribers,
    });
}

async fn log_changes() {
    let mut changes = cluster::changes().expect("remoting endpoint started");
    let mut previous = None::<ClusterState>;
    loop {
        match changes.next().await {
            Change::State(current) => {
                let version = current.version();
                if let Some(previous) = &previous {
                    for transition in current.diff(previous) {
                        let member = transition.member();
                        let what = match transition {
                            Transition::Up(_) | Transition::Left(_) | Transition::Down(_) => {
                                "member changed"
                            }
                            Transition::Forgotten(_) => "member forgotten",
                            Transition::Unreachable(_) => "member unreachable",
                            Transition::Reachable(_) => "member reachable again",
                        };
                        info!(
                            version,
                            addr = %member.addr(),
                            incarnation = %member.incarnation(),
                            state = %member.state(),
                            what
                        );
                    }
                }
                previous = Some(current);
            }

            Change::Settled(settlement) => debug!(
                version = settlement.version(),
                settled = settlement.settled(),
                "receptionist verdict"
            ),

            Change::Gap { dropped } => warn!(dropped, "fell behind the cluster state"),
        }
    }
}

/// By incarnation: a predecessor's Down entry at the same address does not count.
async fn await_downed() {
    let mut changes = cluster::changes().expect("remoting endpoint started");
    loop {
        if let Change::State(state) = changes.next().await
            && state
                .this_member()
                .is_none_or(|member| member.state() == MemberState::Down)
        {
            return;
        }
    }
}

async fn await_shutdown_signal() {
    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut interrupt = signal(SignalKind::interrupt()).expect("SIGINT handler");

    tokio::select! {
        _ = terminate.recv() => (),
        _ = interrupt.recv() => (),
    }
}

fn up_addrs() -> BTreeSet<SocketAddr> {
    members()
        .into_iter()
        .filter(|member| member.state() == MemberState::Up)
        .map(|member| member.addr())
        .collect()
}

fn members() -> Vec<Member> {
    cluster::members().expect("remoting endpoint started")
}

#[cfg(test)]
mod tests {
    use crate::{Config, Seeds};
    use configured::{Configured, LoadOptions};
    use std::{env, net::SocketAddr, num::NonZeroU16};
    use tellus_bootstrap_dns::Query;
    use tellus_bootstrap_k8s::{Pods, Port};

    /// The shipped configuration must keep deserializing into tellus's own config types; the
    /// per-node overrides are what the compose file and the manifests set, and each overlay must
    /// contribute exactly its own discovery.
    /// Both overlays are asserted by one test, since the overrides are process-global environment
    /// variables which two tests would race on.
    #[test]
    fn the_shipped_config_loads() {
        let options = LoadOptions::default()
            .config_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/config"))
            .config_env_prefix("CFG");
        unsafe {
            env::set_var("CFG__NODE_NAME", "node1");
            env::set_var("CFG__ENDPOINT__ADVERTISED_ADDR", "172.28.0.11:7878");
        }

        let config =
            Config::load(options.clone().config_overlays(["dns"])).expect("the dns overlay loads");

        assert_eq!(config.node_name, "node1");
        assert_eq!(
            config.endpoint.advertised_addr,
            "172.28.0.11:7878".parse::<SocketAddr>().expect("valid")
        );
        assert_eq!(config.bootstrap.min_peers.get(), 5);
        let Seeds::Dns(query) = config.seeds else {
            panic!("the dns overlay yields DNS discovery")
        };
        assert_eq!(
            query,
            Query::Ip {
                name: "tellus".to_string(),
                port: NonZeroU16::new(7_878).expect("7878 is not zero"),
            }
        );

        let config =
            Config::load(options.clone().config_overlays(["k8s"])).expect("the k8s overlay loads");

        let Seeds::K8s(pods) = config.seeds else {
            panic!("the k8s overlay yields Kubernetes discovery")
        };
        assert_eq!(
            pods,
            Pods {
                namespace: None,
                label_selector: "app=tellus".to_string(),
                port: Port::Number(NonZeroU16::new(7_878).expect("7878 is not zero")),
            }
        );

        assert!(Config::load(options.config_overlays::<_, &str>([])).is_err());
    }
}
