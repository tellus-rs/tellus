//! A node of the cluster demo: a tellus remoting endpoint which bootstraps via DNS or Kubernetes
//! seed discovery and registers a worker under a well known key. It serves an HTTP API answering
//! what it sees of the cluster and whether it can still message every other member.
//!
//! Configured by `config/default.yaml` plus one of the `config/dns.yaml` and `config/k8s.yaml`
//! overlays, which is what chooses the discovery. The per-node `CFG__` environment overrides
//! [configured](https://github.com/hseeberger/configured) layers on top, see `Config`. A node
//! downed by the cluster exits, e.g. on the minority side of a partition, which downs itself. The
//! orchestrator's restart then mints the fresh incarnation which alone can rejoin.
//!
//! The binary holds two applications, chosen by the major of `endpoint.app_version`: the legacy
//! one (major 1) answers a probe in the old shape of the pong, the current one (major 2) in the
//! new shape, encoded as the old one for a legacy peer. A rolling upgrade restarts the nodes one
//! at a time with the other version, which is what the chaos agent does.

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
    Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, SchemaVersion, Unstashed,
    Untimed,
    cluster::{
        self, AppVersion, BootstrapConfig, Change, ClusterState, EndpointConfig, Key, Manifest,
        Member, MemberState, Peer, Schema, SeedDiscovery, Transition, UnsupportedSchemaVersion,
        codec::{Codec, CodecError},
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

    let Config {
        node_name,
        seeds,
        bootstrap,
        endpoint,
    } = Config::load(LoadOptions::default()).context("load configuration")?;
    let app_version = endpoint
        .app_version
        .clone()
        .context("endpoint.app_version is not configured")?;
    let generation = generation(&app_version)?;
    let addr = endpoint.advertised_addr;
    start_endpoint(endpoint)?;

    let inspect =
        tellus_cluster_inspect::router(InspectConfig::default()).context("inspect router")?;
    let state = Arc::new(NodeState::new(node_name, addr, app_version.clone()));
    tokio::spawn(log_changes());
    info!(name = %state.name, %addr, %app_version, "endpoint started, bootstrapping");

    let heard = state.heard.clone();
    match generation {
        Generation::Legacy => {
            let worker = LegacyWorker { addr, heard };
            run::<LegacyJob, _>(seeds, bootstrap, state, inspect, worker).await
        }

        Generation::Current => {
            let worker = Worker {
                addr,
                heard,
                app_version,
            };
            run::<Job, _>(seeds, bootstrap, state, inspect, worker).await
        }
    }
}

/// This node's configuration: `config/default.yaml` for everything the five nodes share, one of
/// the `config/dns.yaml` and `config/k8s.yaml` overlays for `seeds`, the `CFG__NODE_NAME` and
/// `CFG__ENDPOINT__ADVERTISED_ADDR` environment overrides for what differs per node, and
/// `CFG__ENDPOINT__APP_VERSION` for the application a rolling upgrade moves the node to.
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

/// Which application the node runs, by the major of its application version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generation {
    Legacy,
    Current,
}

/// What the HTTP API needs beyond the process-wide remoting endpoint.
struct NodeState {
    name: String,
    addr: SocketAddr,
    app_version: AppVersion,
    started_at: u64,
    phase: watch::Sender<Phase>,
    heard: Heard,
    events: Arc<EventLog<NodeEvent>>,
}

impl NodeState {
    fn new(name: String, addr: SocketAddr, app_version: AppVersion) -> Self {
        let started_at = now_millis();
        Self {
            name,
            addr,
            app_version,
            started_at,
            phase: watch::Sender::new(Phase::Bootstrapping),
            heard: Heard::default(),
            events: Arc::new(EventLog::new(started_at, EVENTS_KEPT)),
        }
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

/// The job of one application: what a probe asks a worker and what its answer says, the answering
/// node's Up count and its application version, `None` when the pong carries none.
trait Probing
where
    Self: Manifest + Send + 'static,
{
    type Pong: Schema + Send + 'static;

    fn ping(reply_to: ReplyTo<Self::Pong>) -> Self;

    fn stop() -> Self;

    fn outcome(pong: Self::Pong) -> (usize, Option<String>);
}

/// The actor every node registers: it answers a ping, so probing proves messaging works, not
/// only that the member lists agree.
struct Worker {
    addr: SocketAddr,
    heard: Heard,
    app_version: AppVersion,
}

impl Actor for Worker {
    type Message = Job;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        subscribe_listener(context, &self.heard);
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
                    app_version: Some(self.app_version.to_string()),
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

impl Schema for Job {}

impl Manifest for Job {
    const MANIFEST: &'static str = "job";
}

impl Probing for Job {
    type Pong = Pong;

    fn ping(reply_to: ReplyTo<Pong>) -> Self {
        Job::Ping { reply_to }
    }

    fn stop() -> Self {
        Job::Stop
    }

    fn outcome(pong: Pong) -> (usize, Option<String>) {
        (pong.up_members, pong.app_version)
    }
}

/// The current pong, version 1: version 0, which the legacy application still answers with, had
/// no application version. Encoded as version 0 for a peer below application version 2, decoded
/// from version 0 without an application version.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Pong {
    addr: SocketAddr,
    up_members: usize,
    app_version: Option<String>,
}

impl Schema for Pong {
    const VERSION: SchemaVersion = SchemaVersion::new(1);

    fn encode(
        &self,
        codec: &dyn Codec,
        peer: &Peer,
    ) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
        if peer.app_version().is_none_or(|version| version.major < 2) {
            let view = PongV0 {
                addr: self.addr,
                up_members: self.up_members,
            };
            return Ok((SchemaVersion::new(0), codec.encode(&view)?));
        }
        Ok((Self::VERSION, codec.encode(self)?))
    }

    fn decode(
        codec: &dyn Codec,
        schema_version: SchemaVersion,
        payload: &[u8],
    ) -> Result<Self, CodecError> {
        if schema_version == SchemaVersion::new(0) {
            let PongV0 { addr, up_members } = codec.decode_to(payload)?;
            return Ok(Pong {
                addr,
                up_members,
                app_version: None,
            });
        }
        if schema_version == Self::VERSION {
            return codec.decode_to(payload);
        }
        Err(CodecError::decoding(UnsupportedSchemaVersion::new::<Self>(
            schema_version,
        )))
    }
}

/// Version 0 of the pong, as encoded for and decoded from the legacy application.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PongV0 {
    addr: SocketAddr,
    up_members: usize,
}

/// The legacy application's worker: it answers in the old shape of the pong, which the current
/// application knows as version 0.
struct LegacyWorker {
    addr: SocketAddr,
    heard: Heard,
}

impl Actor for LegacyWorker {
    type Message = LegacyJob;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        subscribe_listener(context, &self.heard);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(job) = incoming else {
            unreachable!("worker only receives LegacyJob")
        };

        match job {
            LegacyJob::Ping { reply_to } => {
                reply_to.reply(LegacyPong {
                    addr: self.addr,
                    up_members: up_addrs().len(),
                });
                Ok(Control::Continue(state))
            }

            LegacyJob::Stop => Ok(Control::Stop),
        }
    }
}

/// The legacy application's job, under the same manifest as [Job]: its one shape is version 0 of
/// the current one.
#[derive(Serialize, Deserialize)]
enum LegacyJob {
    Ping { reply_to: ReplyTo<LegacyPong> },
    Stop,
}

impl Schema for LegacyJob {}

impl Manifest for LegacyJob {
    const MANIFEST: &'static str = "job";
}

impl Probing for LegacyJob {
    type Pong = LegacyPong;

    fn ping(reply_to: ReplyTo<LegacyPong>) -> Self {
        LegacyJob::Ping { reply_to }
    }

    fn stop() -> Self {
        LegacyJob::Stop
    }

    fn outcome(pong: LegacyPong) -> (usize, Option<String>) {
        (pong.up_members, None)
    }
}

/// The legacy application's pong, version 0 of [Pong].
#[derive(Debug, Serialize, Deserialize)]
struct LegacyPong {
    addr: SocketAddr,
    up_members: usize,
}

impl Schema for LegacyPong {}

/// The worker's child, subscribed to the event topic: it records the latest event of every
/// publisher, so a node's view shows whom pub-sub currently reaches it from.
struct Listener {
    heard: Heard,
}

impl Actor for Listener {
    type Message = Event;
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

impl Schema for Event {}

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

fn generation(app_version: &AppVersion) -> anyhow::Result<Generation> {
    match app_version.major {
        1 => Ok(Generation::Legacy),
        2 => Ok(Generation::Current),
        major => Err(anyhow!(
            "endpoint.app_version {app_version} has major {major}, this demo runs 1 and 2 only"
        )),
    }
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

/// Everything after the endpoint has started, for the application the node runs: the HTTP API,
/// bootstrap, the worker's registration and the node's end, downed or leaving.
async fn run<J, A>(
    seeds: Seeds,
    bootstrap: BootstrapConfig,
    state: Arc<NodeState>,
    inspect: Router,
    worker: A,
) -> anyhow::Result<()>
where
    J: Probing,
    A: Actor<Message = J> + Send + 'static,
    A::State: Send + 'static,
{
    tokio::spawn(serve_http::<J>(state.clone(), inspect));
    tokio::spawn(record_events::<J>(state.clone()));

    let bootstrap = match seeds {
        Seeds::Dns(query) => {
            let seeds = DnsSeeds::new(query).context("DNS seed discovery")?;
            bootstrap_or_shutdown(seeds, bootstrap).await?
        }

        Seeds::K8s(pods) => {
            let seeds = K8sSeeds::new(pods)
                .await
                .context("Kubernetes seed discovery")?;
            bootstrap_or_shutdown(seeds, bootstrap).await?
        }
    };
    if matches!(bootstrap, Bootstrap::Shutdown) {
        info!("shutdown requested before having joined");
        return Ok(());
    }

    let system = ActorSystem::new(worker);
    cluster::register(&Key::new(WORKER_KEY), system.root()).context("registering the worker")?;
    state.set_phase(Phase::Member);
    info!(name = %state.name, "member of the cluster");
    let publishing = tokio::spawn(publish_events(state.addr));

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
            system.root().tell(J::stop());
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

async fn serve_http<J>(state: Arc<NodeState>, inspect: Router)
where
    J: Probing,
{
    let router = Router::new()
        .route("/health", get(health))
        .route("/cluster", get(cluster::<J>))
        .route("/probe", get(probe::<J>))
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

async fn cluster<J>(State(state): State<Arc<NodeState>>) -> Json<ClusterView>
where
    J: Probing,
{
    let ReceptionistCounts {
        workers,
        subscribers,
    } = receptionist_counts::<J>();

    // The verdict first, then the state, so the verdict is never newer than the state.
    let settlement = receptionist::settlement().expect("remoting endpoint started");
    let cluster_state = cluster::cluster_state()
        .expect("remoting endpoint started")
        .current();
    Json(ClusterView {
        name: state.name.clone(),
        phase: state.phase(),
        app_version: state.app_version.to_string(),
        state: Arc::unwrap_or_clone(cluster_state),
        settlement,
        workers,
        subscribers,
        publishers: state.publishers(),
    })
}

fn receptionist_counts<J>() -> ReceptionistCounts
where
    J: Probing,
{
    let workers = receptionist::lookup(&Key::<J>::new(WORKER_KEY))
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

async fn probe<J>(State(state): State<Arc<NodeState>>) -> Json<ProbeReport>
where
    J: Probing,
{
    let mut tasks = JoinSet::new();
    for addr in up_addrs().into_iter().filter(|addr| *addr != state.addr) {
        tasks.spawn(async move {
            Probe {
                addr,
                outcome: probe_member::<J>(addr).await,
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
async fn probe_member<J>(addr: SocketAddr) -> ProbeOutcome
where
    J: Probing,
{
    let start = Instant::now();

    let worker = match receptionist::lookup_at(&Key::<J>::new(WORKER_KEY), addr) {
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

    match worker.ask(ASK_TIMEOUT, J::ping).await {
        Ok(pong) => {
            let (up_members, app_version) = J::outcome(pong);
            ProbeOutcome::Ok {
                millis: start.elapsed().as_millis(),
                up_members,
                app_version,
            }
        }

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
        app_version: state.app_version.to_string(),
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

/// Spawns the worker's listener and subscribes it to the event topic.
fn subscribe_listener<M>(context: &ActorContext<M>, heard: &Heard)
where
    M: Send + 'static,
{
    let listener = context.spawn(Listener {
        heard: heard.clone(),
    });
    pubsub::subscribe(&Topic::new(EVENT_TOPIC), &listener).expect("remoting endpoint started");
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

async fn record_events<J>(state: Arc<NodeState>)
where
    J: Probing,
{
    let mut phase_rx = state.phase.subscribe();
    let mut workers =
        receptionist::subscribe(&Key::<J>::new(WORKER_KEY)).expect("remoting endpoint started");
    let mut subscribers = receptionist::subscribe(Topic::<Event>::new(EVENT_TOPIC).key())
        .expect("remoting endpoint started");

    state.events.push(NodeEvent::Phase {
        phase: *phase_rx.borrow_and_update(),
    });
    let mut counts = receptionist_counts::<J>();
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
        let current = receptionist_counts::<J>();
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
    use crate::{Config, Generation, Job, LegacyJob, LegacyPong, Pong, PongV0, Seeds, generation};
    use configured::{Configured, LoadOptions};
    use std::{env, net::SocketAddr, num::NonZeroU16};
    use tellus::{
        SchemaVersion,
        cluster::{
            AppVersion, Schema,
            codec::{Codec, CodecError, Postcard},
        },
    };
    use tellus_bootstrap_dns::Query;
    use tellus_bootstrap_k8s::{Pods, Port};

    const CODEC: &dyn Codec = &Postcard;

    fn addr() -> SocketAddr {
        "172.28.0.11:7878".parse().expect("valid")
    }

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
        assert_eq!(config.endpoint.advertised_addr, addr());
        assert_eq!(config.endpoint.app_version, Some(AppVersion::new(1, 0, 0)));
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

        unsafe {
            env::set_var("CFG__ENDPOINT__APP_VERSION", "2.0.0");
        }
        let config =
            Config::load(options.clone().config_overlays(["k8s"])).expect("the k8s overlay loads");

        assert_eq!(config.endpoint.app_version, Some(AppVersion::new(2, 0, 0)));
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

    #[test]
    fn the_major_chooses_the_application() {
        let generation_of = |version: &str| generation(&version.parse().expect("semver")).ok();

        assert_eq!(generation_of("1.9.3"), Some(Generation::Legacy));
        assert_eq!(generation_of("2.0.0"), Some(Generation::Current));
        assert_eq!(generation_of("0.9.0"), None);
        assert_eq!(generation_of("3.0.0"), None);
    }

    #[test]
    fn a_pong_upcasts_from_version_zero_and_refuses_others() {
        let legacy = CODEC
            .encode(&PongV0 {
                addr: addr(),
                up_members: 4,
            })
            .expect("encodes");
        assert_eq!(
            Pong::decode(CODEC, SchemaVersion::new(0), &legacy).expect("decodes"),
            Pong {
                addr: addr(),
                up_members: 4,
                app_version: None,
            }
        );

        let current = Pong {
            addr: addr(),
            up_members: 5,
            app_version: Some("2.0.0".to_string()),
        };
        let payload = CODEC.encode(&current).expect("encodes");
        assert_eq!(
            Pong::decode(CODEC, Pong::VERSION, &payload).expect("decodes"),
            current
        );

        assert!(matches!(
            Pong::decode(CODEC, SchemaVersion::new(2), &payload),
            Err(CodecError::Decode(_))
        ));
    }

    /// The legacy application's shapes are what the current one encodes for it and decodes from
    /// it, byte for byte, and they are version 0, which the legacy application announces.
    #[test]
    fn the_legacy_shapes_are_version_zero_of_the_current_ones() {
        let legacy_pong = CODEC
            .encode(&LegacyPong {
                addr: addr(),
                up_members: 3,
            })
            .expect("encodes");
        let pong_v0 = CODEC
            .encode(&PongV0 {
                addr: addr(),
                up_members: 3,
            })
            .expect("encodes");
        assert_eq!(legacy_pong, pong_v0);
        assert_eq!(LegacyPong::VERSION, SchemaVersion::new(0));

        let legacy_stop = CODEC.encode(&LegacyJob::Stop).expect("encodes");
        let stop = CODEC.encode(&Job::Stop).expect("encodes");
        assert_eq!(legacy_stop, stop);
        assert_eq!(LegacyJob::VERSION, Job::VERSION);
    }
}
