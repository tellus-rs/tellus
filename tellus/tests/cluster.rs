//! Clustered remoting integration: a client node in this process runs scenarios against server
//! nodes in child processes. The scenarios prove reference serialization, discovery from the
//! replicated registrations at an address, replies through `reply_to`, per-sender FIFO across the
//! wire and per-target streams (a bulk message towards one actor does not delay messages towards
//! others). They also prove the remote death watch contract: a real termination signals behind all
//! delivered messages, every watcher of one actor gets its own signal, watching an already
//! terminated actor signals immediately, unwatch suppresses the signal, and a killed node yields a
//! synthesized signal via failure detection.
//!
//! The reconnect path is covered by an oversize message dead-lettering locally while its lane
//! stays usable, by a mid-stream sever proving per-sender FIFO stays "in order, with gaps" over
//! the reconnected lane, by a terminated frame dropped on the watched node being healed by the
//! watch refresh, and by a node killed under a watch and restarted at its old address. In that
//! last case the tombstone kills the old incarnation, not the address, and the directory plus
//! FIFO work against the new one.
//!
//! Request-response crosses nodes through the serializable `ReplyTo`. A remote `ask` resolves
//! with the reply, a reply stays FIFO with the responder's other messages to the asker, a
//! forwarded `ReplyTo` chains its reply over two hops, and a `ReplyTo` serialized and resolved on
//! its own node comes home (and refuses a second serialization). An ask resolves as `NoReply`
//! rather than by timeout when the responder drops its `ReplyTo` and when the reply is oversize.
//! A request targeting a terminated remote actor resolves as `NoReply` too, through the frame's
//! reply tags. A request beyond `max_frame_size` fails its ask at the send, likewise instead of by
//! timeout. Killing a node holding a `ReplyTo` fails the pending ask as `NoReply` via failure
//! detection, and killing an unwatched node holding a `ReplyTo` fails the pending ask as `NoReply`
//! once the member is downed.
//!
//! A terminated signal survives a sever in the middle of the stream it must stay behind, with no
//! message after it. A downed node frozen until its Down entry was swept is refused by the address
//! watermark alone when it comes back. Frozen nodes stand in for partitions in clusters of their
//! own: `KeepMajority` downs a minority of one, and decides a two node split by the lowest address
//! in both directions. A lookup at this node's own address answers its own registrations.
//!
//! A join is held back while a frozen member is unreachable and admitted once it is downed.
//!
//! Two independently formed clusters never merge: a join towards the other one is refused, while
//! every member of one cluster reports the same cluster ID. Two clusters formed on one seed list
//! find each other through bootstrap's probe and report each other as foreign clusters.
//!
//! Every spawned node but the majority, mutual TLS, cluster ID and bootstrap nodes joins the
//! cluster through the client as its seed; scenarios hence also exercise the join handshake, the
//! member snapshot and the supersession of a restarted incarnation on every bootstrap.

use anyhow::{Context, bail};
use derive_more::{Deref, DerefMut};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    env, fmt, fs,
    io::{self, BufRead, BufReader, Write},
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    num::NonZeroUsize,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    process::{self, Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, LazyLock, Mutex, MutexGuard, OnceLock, PoisonError,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorContext, ActorId, ActorRef, ActorSystem, AskError, Control, Incoming, ReplyTo,
    cluster::{
        self, BootstrapConfig, Change, Changes, ClusterState, DownError, EndpointConfig,
        FixedSeeds, JoinError, Key, Lifecycle, Manifest, Member, MemberState, Transition,
        downing::{Disconnected, DownAfterDeadline, Downing, DowningProvider, KeepMajority},
        failure::{Deadline, DeadlineFailureDetector},
        pubsub::{self, Topic},
        receptionist::{self, ReceptionistError, Subscription},
        transport::{
            ConnectedControl, QuicConnection, QuicTransport, QuicTransportError, Transport,
            TransportError,
        },
    },
};
use tokio::{
    runtime::Runtime,
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};

const ROLE_ENV: &str = "TELLUS_REMOTING_ROLE";
const ADDR_ENV: &str = "TELLUS_REMOTING_ADDR";
const SEEDS_ENV: &str = "TELLUS_REMOTING_SEEDS";
const BOOTSTRAP_ENV: &str = "TELLUS_REMOTING_BOOTSTRAP";
const CERT_ENV: &str = "TELLUS_REMOTING_CERT";
const KEY_ENV: &str = "TELLUS_REMOTING_KEY";
const ROOTS_ENV: &str = "TELLUS_REMOTING_ROOTS";
const CLUSTER_SIZE_ENV: &str = "TELLUS_REMOTING_CLUSTER_SIZE";
const STREAMS_ENV: &str = "TELLUS_REMOTING_STREAMS";
const FORCE_COLLISION_ENV: &str = "TELLUS_REMOTING_FORCE_COLLISION";
const ADDR_PREFIX: &str = "ADDR ";
const REF_PREFIX: &str = "REF ";
const JOIN_PREFIX: &str = "JOIN ";
const CLUSTER_PREFIX: &str = "CLUSTER ";
const FOREIGN_PREFIX: &str = "FOREIGN ";
const GOSSIP_PREFIX: &str = "GOSSIP ";
const MTLS_SERVER_NAME: &str = "tellus";
const MTLS_JOIN_TIMEOUT: Duration = Duration::from_secs(5);
const CONVERGENCE_POLL: Duration = Duration::from_millis(200);
const LEAVE_DEADLINE: Duration = Duration::from_secs(2);
const DEPARTURE_PINGS: u32 = 1_000;
const MAJORITY_AFTER: Duration = Duration::from_secs(1);
const JOIN_HOLD_GRACE: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_millis(500);
const ECHO_KEY: &str = "echo";
const TOPIC: &str = "notes";
const PINGS: u32 = 100;
const STREAMED: u32 = 50;
const WATCHERS: usize = 2;
const BULK_TARGETS: usize = 8;
const BULKS: u32 = 8;
const BULK_ACKNOWLEDGEMENTS: u32 = BULKS + BULK_TARGETS as u32 - 1;
const BULK_PAYLOAD: usize = 512 * 1_024;
const OVERSIZE_PAYLOAD: usize = 2 * 1_024 * 1_024;
const SEVER_BULKS: u32 = 300;
const SEVER_PAYLOAD: usize = 32 * 1_024;
const SEVER_DELAY: Duration = Duration::from_millis(10);
const MARKER_ATTEMPTS: usize = 10;
const MARKER_RETRY_DELAY: Duration = Duration::from_millis(500);
const LOOKUP_ATTEMPTS: usize = 20;
const LOOKUP_DELAY: Duration = Duration::from_millis(250);
const ASKS: u32 = 10;
const FORWARD_SEQ: u32 = 11;
const ROUND_TRIP_SEQ: u32 = 7;
const TIMEOUT: Duration = Duration::from_secs(30);
const GIVE_UP_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const UNWATCH_GRACE: Duration = Duration::from_millis(500);
const DEAD_LETTER_GRACE: Duration = Duration::from_millis(300);
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const COLLISION_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const BIND_COLLISION_EXIT: i32 = 75;
const SPAWN_ATTEMPTS: usize = 3;
const SCENARIO_ATTEMPTS: usize = 3;
const BENCH_PINGS: u32 = 50_000;
const BENCH_WINDOW: u32 = 4_096;
const BENCH_SERIAL: u32 = 2_000;
const BENCH_BULKS: u32 = 2_000;
const BENCH_BULK_WINDOW: u32 = 64;
const BENCH_BULK_PAYLOAD: usize = 64 * 1_024;
const GOSSIP_BENCH_SIZE: usize = 5;
const GOSSIP_BENCH_WARM_UP: Duration = Duration::from_secs(5);
const GOSSIP_BENCH_WINDOW: Duration = Duration::from_secs(30);
const STREAMS_BENCH_ECHOES: usize = 256;
const STREAMS_BENCH_BLOCKED: Duration = Duration::from_millis(10);
const STREAMS_BENCH_PROBERS: usize = 4;
const STREAMS_BENCH_WARM_UP_BULKS: u32 = 2_000;
const STREAMS_BENCH_BULKS: u32 = 16_000;

type Receiver = mpsc::Receiver<TestEvent>;

/// This node's advertised address, the seed every spawned node joins through: the scenarios run
/// against a star shaped cluster around the client, which gossip completes into a mesh.
static SELF_ADDR: OnceLock<SocketAddr> = OnceLock::new();

/// The stdout lines of every spawned node, read by one thread per node and handed back after
/// every read, so a node's lines can be awaited one prefix after another.
static NODE_LINES: LazyLock<Mutex<HashMap<u32, mpsc::Receiver<String>>>> =
    LazyLock::new(Mutex::default);

/// The address every spawned node reported once it was bound, by process ID.
static NODE_ADDRS: LazyLock<Mutex<HashMap<u32, SocketAddr>>> = LazyLock::new(Mutex::default);

/// The socket of a collision forced through [FORCE_COLLISION_ENV], held until the process exits.
static FORCED_COLLISION: Mutex<Option<UdpSocket>> = Mutex::new(None);

fn main() -> anyhow::Result<()> {
    let result = match env::var(ROLE_ENV).as_deref() {
        Ok(role) if role == Role::Echo.as_str() => echo_node(),
        Ok(role) if role == Role::Keeper.as_str() => keeper_node(),
        Ok(role) if role == Role::Mutual.as_str() => mutual_node(),
        Ok(role) if role == Role::SelfDown.as_str() => self_down_node(),
        Ok(role) if role == Role::Leave.as_str() => leave_node(),
        Ok(role) if role == Role::Zombie.as_str() => zombie_node(),
        Ok(role) if role == Role::Majority.as_str() => majority_node(),
        Ok(role) if role == Role::ClusterId.as_str() => cluster_id_node(),
        Ok(role) if role == Role::ForeignProbe.as_str() => foreign_probe_node(),
        Ok(role) if role == Role::Bench.as_str() => bench_client(),
        Ok(role) if role == Role::GossipBench.as_str() => gossip_bench_client(),
        Ok(role) if role == Role::GossipIdle.as_str() => gossip_idle_node(),
        Ok(role) if role == Role::StreamsBench.as_str() => streams_bench_client(),
        Ok(role) => Err(anyhow::anyhow!("unknown role {role}")),
        Err(_) => client(),
    };
    exit_on_bind_collision(result)
}

/// A node whose address was taken between its reservation and its bind exits with
/// [BIND_COLLISION_EXIT], so the spawning side can tell a collision from any other failure.
fn exit_on_bind_collision(result: anyhow::Result<()>) -> anyhow::Result<()> {
    if let Err(error) = &result
        && is_addr_in_use(error)
    {
        eprintln!("Error: {error:?}");
        process::exit(BIND_COLLISION_EXIT);
    }
    result
}

fn is_addr_in_use(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<QuicTransportError>(),
            Some(QuicTransportError::Bind { source, .. }) if source.kind() == io::ErrorKind::AddrInUse
        )
    })
}

/// Printed right after the endpoint has started and before joining or bootstrapping, so it means
/// bound, not ready: a spawning side waiting for it never waits for a cluster to form.
fn announce_bound(addr: SocketAddr) {
    println!("{ADDR_PREFIX}{addr}");
}

/// The role a spawned copy of this process plays; the closed set keeps a spawn site and the
/// dispatch in `main` from drifting apart, where a typo would spawn a second client.
#[derive(Debug, Clone, Copy)]
enum Role {
    Echo,
    Keeper,
    Mutual,
    SelfDown,
    Leave,
    Zombie,
    Majority,
    ClusterId,
    ForeignProbe,
    Bench,
    GossipBench,
    GossipIdle,
    StreamsBench,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Echo => "echo",
            Role::Keeper => "keeper",
            Role::Mutual => "mutual",
            Role::SelfDown => "self-down",
            Role::Leave => "leave",
            Role::Zombie => "zombie",
            Role::Majority => "majority",
            Role::ClusterId => "cluster-id",
            Role::ForeignProbe => "foreign-probe",
            Role::Bench => "bench",
            Role::GossipBench => "bench-gossip",
            Role::GossipIdle => "gossip-idle",
            Role::StreamsBench => "bench-streams",
        }
    }
}

/// The echo node: replies to every ping, stops on `Stop`.
fn echo_node() -> anyhow::Result<()> {
    serve(EchoServer, ECHO_KEY)
}

/// The keeper node: spawns a fresh streamer child per `Spawn` and hands its reference out.
fn keeper_node() -> anyhow::Result<()> {
    serve(Keeper, "keeper")
}

/// The mutual TLS node, with certificates from the paths named by [CERT_ENV], [KEY_ENV] and
/// [ROOTS_ENV]: forms a cluster of one when started without seeds, staying up until killed; with
/// seeds it joins, prints the verdict and exits, so the client can assert which certificates
/// the cluster admits.
fn mutual_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).context("bind address")?;
        let cert = fs::read(env::var(CERT_ENV).context("certificate path")?)?;
        let key = fs::read(env::var(KEY_ENV).context("key path")?)?;
        let ca = fs::read(env::var(ROOTS_ENV).context("roots path")?)?;

        let mut roots = RootCertStore::empty();
        roots.add(CertificateDer::from(ca))?;
        let transport = QuicTransport::mutual_tls(
            bind_addr.parse().context("bind address")?,
            vec![CertificateDer::from(cert)],
            PrivateKeyDer::Pkcs8(key.into()),
            roots,
            MTLS_SERVER_NAME,
        )?;
        let addr = transport.local_addr()?;
        cluster::start_endpoint(test_config(addr), transport)?;
        announce_bound(addr);

        match env::var(SEEDS_ENV) {
            Ok(seeds) => {
                let seeds = seeds
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<Vec<SocketAddr>, _>>()?;
                let verdict = match timeout(MTLS_JOIN_TIMEOUT, cluster::join(&seeds)).await {
                    Ok(Ok(())) => "ok",
                    Ok(Err(_)) => "refused",
                    Err(_) => "timeout",
                };
                println!("{JOIN_PREFIX}{verdict}");
                std::io::stdout().flush()?;
                Ok(())
            }

            Err(_) => {
                cluster::form().context("forming the cluster")?;
                println!("{JOIN_PREFIX}seed");
                std::io::stdout().flush()?;
                std::future::pending::<anyhow::Result<()>>().await
            }
        }
    })
}

/// The self downing node: it joins the cluster with a downing provider which gives way as soon as
/// it has a peer, then tries to rejoin until it is refused and prints that verdict, so the client
/// can assert what a provider's `SelfDown` does to the node deciding it.
fn self_down_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let transport = QuicTransport::dev("127.0.0.1:0".parse()?)?;
        let addr = transport.local_addr()?;
        let mut config = test_config(addr);
        config.downing_provider = Arc::new(|| Box::new(SelfDownWithPeers));
        cluster::start_endpoint(config, TimeoutTransport(transport))?;
        announce_bound(addr);

        let seeds = parse_addrs(&env::var(SEEDS_ENV).context("seed addresses")?)?;
        timeout(TIMEOUT, cluster::join(&seeds))
            .await
            .context("joining the cluster")??;

        let downed = async {
            loop {
                match cluster::join(&seeds).await {
                    Ok(()) => sleep(CONVERGENCE_POLL).await,

                    Err(JoinError::Downed) => return anyhow::Ok(()),

                    Err(error) => return Err(error.into()),
                }
            }
        };
        timeout(TIMEOUT, downed)
            .await
            .context("self down within the timeout")??;

        println!("{JOIN_PREFIX}downed");
        std::io::stdout().flush()?;
        Ok(())
    })
}

/// While set, [GatedDowning] downs nobody, so a scenario can observe a member as unreachable
/// before it is downed, whatever the timing.
static HOLD_DOWNING: AtomicBool = AtomicBool::new(false);

/// The scenarios' downing provider: [DownAfterDeadline] behind the [HOLD_DOWNING] flag.
struct GatedDowning(DownAfterDeadline);

impl DowningProvider for GatedDowning {
    fn down(&mut self, members: &[Member], disconnected: Disconnected<'_>, at: Instant) -> Downing {
        if HOLD_DOWNING.load(Ordering::Relaxed) {
            Downing::Members(Vec::new())
        } else {
            self.0.down(members, disconnected, at)
        }
    }
}

/// Clears the hold on drop, so a failing scenario leaves downing enabled for the next one.
struct HoldDowning;

impl HoldDowning {
    fn new() -> Self {
        HOLD_DOWNING.store(true, Ordering::Relaxed);
        Self
    }
}

impl Drop for HoldDowning {
    fn drop(&mut self) {
        HOLD_DOWNING.store(false, Ordering::Relaxed);
    }
}

/// Gives way as soon as this node has a peer, which is the verdict a quorum aware provider
/// reaches on the losing side of a partition; a partition itself is not stageable here.
struct SelfDownWithPeers;

impl DowningProvider for SelfDownWithPeers {
    fn down(&mut self, members: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
        if members.len() > 1 {
            Downing::SelfDown
        } else {
            Downing::Members(Vec::new())
        }
    }
}

/// The leaving node: an echo server which announces its departure once its actor system has
/// terminated, instead of exiting silently and leaving the cluster to detect the silence. It
/// checks its own departure report: the seed it joined through acknowledged the drain, and a
/// second leave returns the same report.
fn leave_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        start_endpoint().await?;
        let seeds = parse_addrs(&env::var(SEEDS_ENV).context("seed addresses")?)?;

        let system = ActorSystem::new(EchoServer);
        let bytes = cluster::serialize_ref(system.root())?;
        println!("{REF_PREFIX}{}", hex_encode(&bytes));
        std::io::stdout().flush()?;

        let mut state_rx = cluster::cluster_state()?;
        let departure = timeout(TIMEOUT, cluster::leave_on_terminated(system))
            .await
            .context("leaving the cluster")??;

        // The membership loop has ended by now, so the last value must show the departure.
        let own = *SELF_ADDR.get().context("endpoint not started")?;
        let state = state_rx.borrow_and_update().clone();
        if !is_left(&state, own) {
            bail!("the last cluster state does not show this node Left: {state:?}");
        }
        if !seeds
            .iter()
            .all(|seed| departure.acknowledged().contains(seed))
        {
            bail!("the seeds did not acknowledge the departure: {departure:?}");
        }
        let again = cluster::leave().await.context("leaving again")?;
        if again != departure {
            bail!("leaving again reported {again:?} instead of {departure:?}");
        }
        Ok(())
    })
}

/// The zombie node: a member which never downs anybody, so whatever happens while it is frozen,
/// it keeps dialing the client once resumed and can only learn its fate from the refusal; it
/// prints that verdict once its lifecycle says so.
fn zombie_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).unwrap_or_else(|_| "127.0.0.1:0".to_string());
        let transport = QuicTransport::dev(bind_addr.parse()?)?;
        let addr = transport.local_addr()?;
        let mut config = test_config(addr);
        config.downing_provider = Arc::new(|| Box::new(NeverDowning));
        cluster::start_endpoint(config, TimeoutTransport(transport))?;
        announce_bound(addr);

        let seeds = parse_addrs(&env::var(SEEDS_ENV).context("seed addresses")?)?;
        timeout(TIMEOUT, cluster::join(&seeds))
            .await
            .context("joining the cluster")??;

        let downed = async {
            while cluster::lifecycle()? != Lifecycle::Downed {
                sleep(CONVERGENCE_POLL).await;
            }
            anyhow::Ok(())
        };
        timeout(GIVE_UP_TIMEOUT, downed)
            .await
            .context("downed within the timeout")??;

        println!("{JOIN_PREFIX}downed");
        std::io::stdout().flush()?;
        Ok(())
    })
}

/// A member of a cluster of its own, apart from the client's, deciding partitions with
/// [KeepMajority]: it forms the cluster without [SEEDS_ENV] and joins through it otherwise. It
/// prints `up` once [CLUSTER_SIZE_ENV] members are Up, then one verdict: `downed` once it downed
/// itself, `survived` once it has seen another member Down.
fn majority_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).context("bind address")?;
        let size = env::var(CLUSTER_SIZE_ENV)
            .context("cluster size")?
            .parse::<usize>()?;
        let transport = QuicTransport::dev(bind_addr.parse()?)?;
        let addr = transport.local_addr()?;
        let mut config = test_config(addr);
        config.downing_provider = Arc::new(|| Box::new(KeepMajority::new(MAJORITY_AFTER)));
        cluster::start_endpoint(config, TimeoutTransport(transport))?;
        announce_bound(addr);

        match env::var(SEEDS_ENV) {
            Ok(seeds) => timeout(TIMEOUT, cluster::join(&parse_addrs(&seeds)?))
                .await
                .context("joining the cluster")??,

            Err(_) => cluster::form().context("forming the cluster")?,
        }

        let mut state_rx = cluster::cluster_state()?;
        timeout(
            TIMEOUT,
            state_rx.wait_for(|state| state.up().count() == size),
        )
        .await
        .context("every member Up within the timeout")??;
        println!("{JOIN_PREFIX}up");
        std::io::stdout().flush()?;

        let verdict = async {
            loop {
                if cluster::lifecycle()? == Lifecycle::Downed {
                    return anyhow::Ok("downed");
                }
                let other_down = state_rx
                    .borrow_and_update()
                    .members()
                    .iter()
                    .any(|member| member.addr() != addr && member.state() == MemberState::Down);
                if other_down {
                    return Ok("survived");
                }
                sleep(CONVERGENCE_POLL).await;
            }
        };
        let verdict = timeout(TIMEOUT, verdict)
            .await
            .context("a verdict within the timeout")??;
        println!("{JOIN_PREFIX}{verdict}");
        std::io::stdout().flush()?;
        Ok(())
    })
}

/// A node printing its cluster ID: without [SEEDS_ENV] it forms a cluster of its own and stays up
/// until killed, with seeds it joins through them, then leaves and exits.
fn cluster_id_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).context("bind address")?;
        let transport = QuicTransport::dev(bind_addr.parse()?)?;
        let addr = transport.local_addr()?;
        cluster::start_endpoint(test_config(addr), TimeoutTransport(transport))?;
        announce_bound(addr);

        let joined = match env::var(SEEDS_ENV) {
            Ok(seeds) => {
                timeout(TIMEOUT, cluster::join(&parse_addrs(&seeds)?))
                    .await
                    .context("joining the cluster")??;
                true
            }

            Err(_) => {
                cluster::form().context("forming the cluster")?;
                false
            }
        };

        let cluster_id = cluster::cluster_state()?
            .borrow()
            .cluster()
            .context("a member names its cluster")?;
        println!("{CLUSTER_PREFIX}{cluster_id}");
        std::io::stdout().flush()?;

        if joined {
            cluster::leave().await?;
            Ok(())
        } else {
            std::future::pending::<anyhow::Result<()>>().await
        }
    })
}

/// A node forming a cluster of its own and probing the addresses of [BOOTSTRAP_ENV] every
/// [PROBE_INTERVAL]: it prints its cluster ID, then the first foreign cluster it reports, as
/// address and ID, and stays up until killed.
fn foreign_probe_node() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let bind_addr = env::var(ADDR_ENV).context("bind address")?;
        let transport = QuicTransport::dev(bind_addr.parse()?)?;
        let addr = transport.local_addr()?;
        cluster::start_endpoint(test_config(addr), TimeoutTransport(transport))?;
        announce_bound(addr);
        cluster::form().context("forming the cluster")?;

        let seeds = parse_addrs(&env::var(BOOTSTRAP_ENV).context("seed addresses")?)?;
        let config = BootstrapConfig {
            probe_interval: Some(PROBE_INTERVAL),
            ..BootstrapConfig::new()
        };
        timeout(TIMEOUT, cluster::bootstrap(FixedSeeds::new(seeds), config))
            .await
            .context("bootstrapping a formed node")??;

        let mut state_rx = cluster::cluster_state()?;
        let cluster_id = state_rx
            .borrow()
            .cluster()
            .context("a formed node names its cluster")?;
        println!("{CLUSTER_PREFIX}{cluster_id}");
        std::io::stdout().flush()?;

        let foreign = timeout(TIMEOUT, async {
            loop {
                if let Some(foreign) = state_rx.borrow_and_update().foreign_clusters().first() {
                    return anyhow::Ok(*foreign);
                }
                state_rx.changed().await.context("the cluster state ends")?;
            }
        })
        .await
        .context("a foreign cluster within the timeout")??;
        println!("{FOREIGN_PREFIX}{} {}", foreign.addr(), foreign.cluster());
        std::io::stdout().flush()?;

        std::future::pending::<anyhow::Result<()>>().await
    })
}

/// Downs nobody, so a node only ever learns it is dead from the others.
struct NeverDowning;

impl DowningProvider for NeverDowning {
    fn down(&mut self, _: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
        Downing::Members(Vec::new())
    }
}

/// Serves its root both ways: registered under a key for discovery and printed as a serialized
/// reference, so the scenarios can bootstrap either way against the same node.
fn serve<A>(actor: A, name: &str) -> anyhow::Result<()>
where
    A: Actor + Send + 'static,
    A::Message: Manifest + DeserializeOwned + Send + 'static,
    A::State: Send + 'static,
{
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        start_endpoint().await?;

        let system = ActorSystem::new(actor);
        cluster::register(&Key::new(name), system.root())?;
        let bytes = cluster::serialize_ref(system.root())?;
        println!("{REF_PREFIX}{}", hex_encode(&bytes));
        std::io::stdout().flush()?;

        timeout(TIMEOUT, system.terminated())
            .await
            .context("server actor system termination")??;
        Ok(())
    })
}

struct EchoServer;

impl Actor for EchoServer {
    type Message = Request;
    type State = EchoState;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(EchoState::default())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Request::Ping { seq, reply_to }) => {
                reply_to.tell(Reply { seq });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Ask { seq, reply_to }) => {
                reply_to.reply(Reply { seq });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::AskThenTell {
                marker_to,
                reply_to,
            }) => {
                marker_to.tell(AskerMessage::Marker);
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Ignore { .. }) => Ok(Control::Continue(state)),

            Incoming::Message(Request::AskOversize { reply_to, .. }) => {
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Bulk { seq, reply_to, .. }) => {
                reply_to.reply(Reply { seq });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::AskOversizeReply { reply_to }) => {
                reply_to.reply(BulkReply {
                    payload: vec![0; OVERSIZE_PAYLOAD],
                });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Hold { reply_to }) => {
                state.holds.push(reply_to);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Forward { to, reply_to }) => {
                to.tell(ForwardedRequest { reply_to });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Members { reply_to }) => {
                let own = *SELF_ADDR.get().expect("endpoint started");
                let up = cluster::members()
                    .expect("endpoint started")
                    .iter()
                    .filter(|member| member.state() == MemberState::Up)
                    .map(|member| member.addr())
                    .collect();
                reply_to.reply(MemberAddrs { own, up });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Spawn { reply_to }) => {
                let child = context.spawn(EchoServer);
                cluster::register(&Key::new(ECHO_KEY), &child).expect("endpoint started");
                reply_to.reply(child);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Directory { reply_to }) => {
                let own = *SELF_ADDR.get().expect("endpoint started");
                let echoes = receptionist::lookup(&Key::<Request>::new(ECHO_KEY))
                    .expect("the echo key resolves")
                    .len();
                let version = cluster::cluster_state()
                    .expect("endpoint started")
                    .borrow()
                    .version();
                let settled = receptionist::settled(version).expect("endpoint started");
                reply_to.reply(Directory {
                    own,
                    echoes,
                    settled,
                });
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Subscribe { reply_to }) => {
                let recorder = context.spawn(Recorder {
                    echo: context.self_ref().clone(),
                });
                pubsub::subscribe(&Topic::new(TOPIC), &recorder).expect("endpoint started");
                reply_to.reply(recorder);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Publish { seq, reply_to }) => {
                let own = *SELF_ADDR.get().expect("endpoint started");
                let told = pubsub::publish(&Topic::new(TOPIC), Note { from: own, seq })
                    .expect("the topic resolves");
                reply_to.reply(told);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Subscribers { reply_to }) => {
                let subscribers = receptionist::lookup(Topic::<Note>::new(TOPIC).key())
                    .expect("the topic resolves")
                    .len();
                reply_to.reply(subscribers);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Noted { recorder, note }) => {
                state.notes.push((recorder, note));
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Notes { reply_to }) => {
                reply_to.reply(state.notes.iter().map(|(_, note)| note.clone()).collect());
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::RecorderNotes { recorder, reply_to }) => {
                let notes = state
                    .notes
                    .iter()
                    .filter(|(noted_by, _)| *noted_by == recorder)
                    .map(|(_, note)| note.clone())
                    .collect();
                reply_to.reply(notes);
                Ok(Control::Continue(state))
            }

            Incoming::Message(Request::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Default)]
struct EchoState {
    holds: Vec<ReplyTo<Reply>>,
    notes: Vec<(ActorId, Note)>,
}

/// Subscribed to [TOPIC] on behalf of its parent echo server, which it forwards every note to.
struct Recorder {
    echo: ActorRef<Request>,
}

impl Actor for Recorder {
    type Message = Note;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(note) = incoming {
            self.echo.tell(Request::Noted {
                recorder: context.self_ref().actor_id(),
                note,
            });
        }
        Ok(Control::Continue(state))
    }
}

#[derive(Serialize, Deserialize)]
enum Request {
    Ping {
        seq: u32,
        reply_to: ActorRef<Reply>,
    },

    Ask {
        seq: u32,
        reply_to: ReplyTo<Reply>,
    },

    AskThenTell {
        marker_to: ActorRef<AskerMessage>,
        reply_to: ReplyTo<Reply>,
    },

    Ignore {
        reply_to: ReplyTo<Reply>,
    },

    AskOversize {
        payload: Vec<u8>,
        reply_to: ReplyTo<Reply>,
    },

    AskOversizeReply {
        reply_to: ReplyTo<BulkReply>,
    },

    /// A payload acknowledged with its sequence number.
    Bulk {
        seq: u32,
        payload: Vec<u8>,
        reply_to: ReplyTo<Reply>,
    },

    Hold {
        reply_to: ReplyTo<Reply>,
    },

    Forward {
        to: ActorRef<ForwardedRequest>,
        reply_to: ReplyTo<Reply>,
    },

    Members {
        reply_to: ReplyTo<MemberAddrs>,
    },

    /// Spawns a child echo server registered under [ECHO_KEY] and replies with its reference.
    Spawn {
        reply_to: ReplyTo<ActorRef<Request>>,
    },

    /// What this node's receptionist resolves under [ECHO_KEY], and whether it is settled.
    Directory {
        reply_to: ReplyTo<Directory>,
    },

    /// Spawns a child subscribed to [TOPIC] and replies with its reference.
    Subscribe {
        reply_to: ReplyTo<ActorRef<Note>>,
    },

    /// Publishes a note to [TOPIC] and replies how many subscribers were told.
    Publish {
        seq: u64,
        reply_to: ReplyTo<usize>,
    },

    /// How many subscribers of [TOPIC] this node's receptionist resolves.
    Subscribers {
        reply_to: ReplyTo<usize>,
    },

    /// A note one of this node's recorders received, forwarded to its parent.
    Noted {
        recorder: ActorId,
        note: Note,
    },

    /// Every note this node received so far.
    Notes {
        reply_to: ReplyTo<Vec<Note>>,
    },

    /// Every note the given recorder of this node received so far, in arrival order.
    RecorderNotes {
        recorder: ActorId,
        reply_to: ReplyTo<Vec<Note>>,
    },

    Stop,
}

impl Manifest for Request {
    const MANIFEST: &'static str = "request";
}

/// One publish, naming the node which made it, so a subscriber can assert per-publisher order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Note {
    from: SocketAddr,
    seq: u64,
}

impl Manifest for Note {
    const MANIFEST: &'static str = "note";
}

#[derive(Debug, Serialize, Deserialize)]
struct Directory {
    own: SocketAddr,
    echoes: usize,
    settled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reply {
    seq: u32,
}

impl Manifest for Reply {
    const MANIFEST: &'static str = "reply";
}

#[derive(Debug, Serialize, Deserialize)]
struct BulkReply {
    payload: Vec<u8>,
}

/// A node's own address next to the Up members it sees, so the client can tell whose view it is
/// asserting on.
#[derive(Debug, Serialize, Deserialize)]
struct MemberAddrs {
    own: SocketAddr,
    up: Vec<SocketAddr>,
}

#[derive(Serialize, Deserialize)]
enum AskerMessage {
    Marker,
    Answer(Reply),
}

#[derive(Serialize, Deserialize)]
struct ForwardedRequest {
    reply_to: ReplyTo<Reply>,
}

struct Keeper;

impl Actor for Keeper {
    type Message = KeeperMessage;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(KeeperMessage::Spawn { reply_to }) => {
                let child = context.spawn(Streamer);
                reply_to.tell(ClientEvent::Child(child));
                Ok(Control::Continue(state))
            }

            Incoming::Message(KeeperMessage::DropTerminated { count, reply_to }) => {
                assert!(
                    cluster::drop_terminated_frames(count),
                    "endpoint not started"
                );
                reply_to.tell(ClientEvent::Armed);
                Ok(Control::Continue(state))
            }

            Incoming::Message(KeeperMessage::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KeeperMessage {
    Spawn {
        reply_to: ActorRef<ClientEvent>,
    },

    DropTerminated {
        count: u64,
        reply_to: ActorRef<ClientEvent>,
    },

    Stop,
}

impl Manifest for KeeperMessage {
    const MANIFEST: &'static str = "keeper-message";
}

/// Streams `count` numbered messages to `reply_to` and stops on `Go`, so its terminated signal
/// must arrive behind all of them; acknowledges a `Bulk` with its sequence number and keeps
/// running, so a payload's size only ever delays the stream it rides.
struct Streamer;

impl Actor for Streamer {
    type Message = StreamerMessage;
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
        match incoming {
            Incoming::Message(StreamerMessage::Go { count, reply_to }) => {
                for seq in 0..count {
                    reply_to.tell(ClientEvent::Streamed(seq));
                }
                Ok(Control::Stop)
            }

            Incoming::Message(StreamerMessage::Bulk { seq, reply_to, .. }) => {
                reply_to.tell(ClientEvent::Bulked(seq));
                Ok(Control::Continue(state))
            }

            Incoming::Message(StreamerMessage::GoBulk {
                count,
                size,
                reply_to,
            }) => {
                let payload = vec![0; size];
                for seq in 0..count {
                    reply_to.tell(ClientEvent::StreamedBulk {
                        seq,
                        payload: payload.clone(),
                    });
                }
                Ok(Control::Stop)
            }

            Incoming::Message(StreamerMessage::Ask { reply_to }) => {
                reply_to.reply(Reply { seq: 0 });
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum StreamerMessage {
    Go {
        count: u32,
        reply_to: ActorRef<ClientEvent>,
    },

    Bulk {
        seq: u32,
        payload: Vec<u8>,
        reply_to: ActorRef<ClientEvent>,
    },

    /// Like `Go`, with a payload of `size` bytes on every message, so the stream takes long
    /// enough on the wire to be severed in the middle.
    GoBulk {
        count: u32,
        size: usize,
        reply_to: ActorRef<ClientEvent>,
    },

    Ask {
        reply_to: ReplyTo<Reply>,
    },
}

#[derive(Serialize, Deserialize)]
enum ClientEvent {
    Child(ActorRef<StreamerMessage>),
    Streamed(u32),
    StreamedBulk { seq: u32, payload: Vec<u8> },
    Bulked(u32),
    Armed,
    Probe,
}

enum TestEvent {
    Watching,
    Bulking,
    Child(ActorRef<StreamerMessage>),
    StreamedAll,
    Done(Result<(), String>),
}

fn client() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    echo_scenario(&runtime).context("echo scenario")?;
    retried("discovery scenario", || discovery_scenario(&runtime)).context("discovery scenario")?;
    receptionist_scenario(&runtime).context("receptionist scenario")?;
    pubsub_scenario(&runtime).context("pub-sub scenario")?;

    let mut keeper_process = KillOnDrop(spawn_node(Role::Keeper)?);
    let keeper = resolve_ref::<KeeperMessage>(&mut keeper_process)?;

    ordered_termination_scenario(&runtime, &keeper).context("ordered termination scenario")?;
    watch_terminated_scenario(&runtime, &keeper).context("watch terminated scenario")?;
    unwatch_scenario(&runtime, &keeper).context("unwatch scenario")?;
    two_watchers_scenario(&runtime, &keeper).context("two watchers scenario")?;
    head_of_line_scenario(&runtime, &keeper).context("head of line scenario")?;
    oversize_scenario(&runtime, &keeper).context("oversize scenario")?;
    sever_scenario(&runtime, &keeper).context("sever scenario")?;
    sever_terminated_scenario(&runtime, &keeper).context("sever terminated scenario")?;
    lost_terminated_scenario(&runtime, &keeper).context("lost terminated scenario")?;
    dead_target_ask_scenario(&runtime, &keeper).context("dead target ask scenario")?;

    keeper.tell(KeeperMessage::Stop);
    expect_exit(&mut keeper_process, "keeper")?;

    let mut ask_echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let ask_echo = resolve_ref::<Request>(&mut ask_echo_process)?;

    remote_ask_scenario(&runtime, &ask_echo).context("remote ask scenario")?;
    reply_to_fifo_scenario(&runtime, &ask_echo).context("reply to fifo scenario")?;
    forwarded_reply_scenario(&runtime, &ask_echo).context("forwarded reply scenario")?;
    reply_serde_scenario(&runtime).context("reply serde scenario")?;

    ask_echo.tell(Request::Stop);
    expect_exit(&mut ask_echo_process, "ask echo")?;

    ask_node_death_scenario(&runtime).context("ask node death scenario")?;
    ask_down_scenario(&runtime).context("ask down scenario")?;

    relayed_reachability_scenario(&runtime).context("relayed reachability scenario")?;
    retried("restart scenario", || restart_scenario(&runtime)).context("restart scenario")?;

    changes_scenario(&runtime).context("changes scenario")?;
    retried("bootstrap scenario", || bootstrap_scenario(&runtime)).context("bootstrap scenario")?;
    retried("down scenario", || down_scenario(&runtime)).context("down scenario")?;
    zombie_scenario(&runtime).context("zombie scenario")?;
    held_join_scenario(&runtime).context("held join scenario")?;
    retried("keep majority scenario", keep_majority_scenario).context("keep majority scenario")?;
    leave_scenario(&runtime).context("leave scenario")?;
    self_down_scenario().context("self down scenario")?;
    identity_binding_scenario().context("identity binding scenario")?;
    foreign_cluster_scenario(&runtime).context("foreign cluster scenario")?;
    retried("foreign probe scenario", foreign_probe_scenario).context("foreign probe scenario")?;

    Ok(())
}

/// Measures the remote hot paths against fresh echo and keeper nodes and prints the numbers:
/// windowed pipelined round trips, serial round trip latency, pipelined and serial asks from
/// outside any actor and from inside one, and bulk payload throughput. Not a regression gate;
/// run via `just bench-cluster` and compare before and after a change.
fn bench_client() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let pipelined = bench_echo(
        &runtime,
        &echo,
        BenchRequest::Ping,
        BENCH_PINGS,
        BENCH_WINDOW,
    )?;
    println!(
        "pipelined: {:.0} round trips/s ({BENCH_PINGS} pings, window {BENCH_WINDOW})",
        f64::from(BENCH_PINGS) / pipelined.as_secs_f64()
    );

    let serial = bench_echo(&runtime, &echo, BenchRequest::Ping, BENCH_SERIAL, 1)?;
    println!(
        "serial: {:.1} us/round trip ({BENCH_SERIAL} pings)",
        serial.as_secs_f64() * 1e6 / f64::from(BENCH_SERIAL)
    );

    let ask_pipelined = bench_ask(&runtime, &echo, BENCH_PINGS, BENCH_WINDOW)?;
    println!(
        "ask pipelined: {:.0} asks/s ({BENCH_PINGS} asks, window {BENCH_WINDOW})",
        f64::from(BENCH_PINGS) / ask_pipelined.as_secs_f64()
    );

    let ask_serial = bench_ask(&runtime, &echo, BENCH_SERIAL, 1)?;
    println!(
        "ask serial: {:.1} us/ask ({BENCH_SERIAL} asks)",
        ask_serial.as_secs_f64() * 1e6 / f64::from(BENCH_SERIAL)
    );

    let actor_ask_pipelined = bench_echo(
        &runtime,
        &echo,
        BenchRequest::Ask,
        BENCH_PINGS,
        BENCH_WINDOW,
    )?;
    println!(
        "actor ask pipelined: {:.0} asks/s ({BENCH_PINGS} asks, window {BENCH_WINDOW})",
        f64::from(BENCH_PINGS) / actor_ask_pipelined.as_secs_f64()
    );

    let actor_ask_serial = bench_echo(&runtime, &echo, BenchRequest::Ask, BENCH_SERIAL, 1)?;
    println!(
        "actor ask serial: {:.1} us/ask ({BENCH_SERIAL} asks)",
        actor_ask_serial.as_secs_f64() * 1e6 / f64::from(BENCH_SERIAL)
    );

    let mut keeper_process = KillOnDrop(spawn_node(Role::Keeper)?);
    let keeper = resolve_ref::<KeeperMessage>(&mut keeper_process)?;

    let bulk = bench_bulk(&runtime, &keeper)?;
    let mebibytes = f64::from(BENCH_BULKS) * BENCH_BULK_PAYLOAD as f64 / (1024.0 * 1024.0);
    println!(
        "bulk: {:.1} MiB/s ({BENCH_BULKS} bulks of {} KiB, window {BENCH_BULK_WINDOW})",
        mebibytes / bulk.as_secs_f64(),
        BENCH_BULK_PAYLOAD / 1024
    );

    echo.tell(Request::Stop);
    keeper.tell(KeeperMessage::Stop);
    expect_exit(&mut echo_process, "bench echo")?;
    expect_exit(&mut keeper_process, "bench keeper")?;
    Ok(())
}

fn bench_echo(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    request: BenchRequest,
    total: u32,
    window: u32,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let event_rx = run_client(runtime, "bench echo client termination", |event_tx| {
        BenchEcho {
            server: echo.clone(),
            request,
            total,
            window,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    Ok(started.elapsed())
}

/// Keeps `window` asks outstanding from tasks outside any actor until `total` replies arrived, so
/// every request carries a reply destination no actor awaits.
fn bench_ask(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    total: u32,
    window: u32,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    runtime.block_on(async {
        let next = Arc::new(AtomicU32::new(0));
        let mut askers = JoinSet::new();
        for _ in 0..window.min(total) {
            let echo = echo.clone();
            let next = next.clone();
            askers.spawn(async move {
                loop {
                    let seq = next.fetch_add(1, Ordering::Relaxed);
                    if seq >= total {
                        return Ok::<_, anyhow::Error>(());
                    }
                    let reply = echo
                        .ask(TIMEOUT, |reply_to| Request::Ask { seq, reply_to })
                        .await?;
                    anyhow::ensure!(reply.seq == seq, "ask {seq} answered as {}", reply.seq);
                }
            });
        }
        while let Some(asked) = askers.join_next().await {
            asked??;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(started.elapsed())
}

fn bench_bulk(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let event_rx = run_client(runtime, "bench bulk client termination", |event_tx| {
        BenchBulk {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    Ok(started.elapsed())
}

/// An idle cluster of [CLUSTER_SIZE_ENV] nodes (default [GOSSIP_BENCH_SIZE]), this one and
/// [Role::GossipIdle] children: after a warm-up, the gossip bytes every node queues within the
/// window, summed over the cluster.
fn gossip_bench_client() -> anyhow::Result<()> {
    let size = match env::var(CLUSTER_SIZE_ENV) {
        Ok(size) => size.parse::<usize>().context("cluster size")?,
        Err(_) => GOSSIP_BENCH_SIZE,
    };
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    let mut nodes = Vec::with_capacity(size.saturating_sub(1));
    for _ in 1..size {
        let mut command = Command::new(env::current_exe()?);
        command
            .env(ROLE_ENV, Role::GossipIdle.as_str())
            .env(
                SEEDS_ENV,
                SELF_ADDR.get().context("endpoint started")?.to_string(),
            )
            .env(CLUSTER_SIZE_ENV, size.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let mut process = KillOnDrop(command.spawn().context("gossip idle process")?);
        let lines = stdout_lines(&mut process)?;
        nodes.push((process, lines));
    }
    for (_, lines) in &nodes {
        expect_line(lines, &format!("{JOIN_PREFIX}up"))?;
    }
    runtime.block_on(async {
        let mut state_rx = cluster::cluster_state()?;
        timeout(
            TIMEOUT,
            state_rx.wait_for(|state| state.up().count() == size),
        )
        .await
        .context("every member Up within the timeout")??;
        anyhow::Ok(())
    })?;

    thread::sleep(GOSSIP_BENCH_WARM_UP);
    let started = Instant::now();
    let before = gossip_bytes_sampled(&mut nodes)?;
    thread::sleep(GOSSIP_BENCH_WINDOW);
    let after = gossip_bytes_sampled(&mut nodes)?;
    let elapsed = started.elapsed();

    let per_node = after
        .iter()
        .zip(&before)
        .map(|(after, before)| after - before)
        .collect::<Vec<_>>();
    let total = per_node.iter().sum::<u64>();
    println!(
        "gossip: {:.0} bytes/s across {size} nodes ({total} bytes in {:.1} s, per node {per_node:?})",
        total as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64()
    );

    for (mut process, _) in nodes {
        drop(process.0.stdin.take());
        expect_exit(&mut process, "gossip idle")?;
    }
    Ok(())
}

/// The gossip bytes of this node first, then of every child, each asked through its stdin.
fn gossip_bytes_sampled(
    nodes: &mut [(KillOnDrop, mpsc::Receiver<String>)],
) -> anyhow::Result<Vec<u64>> {
    for (process, _) in nodes.iter_mut() {
        let stdin = process.0.stdin.as_mut().context("gossip idle stdin")?;
        writeln!(stdin)?;
        stdin.flush()?;
    }
    let mut samples = vec![cluster::gossip_bytes().context("endpoint started")?];
    for (_, lines) in nodes.iter() {
        samples.push(prefixed_line(lines, GOSSIP_PREFIX)?.parse()?);
    }
    Ok(samples)
}

/// A member of the [Role::GossipBench] cluster: prints `up` once [CLUSTER_SIZE_ENV] members are
/// Up, then its gossip bytes for every line on stdin, and exits at the end of stdin.
fn gossip_idle_node() -> anyhow::Result<()> {
    let size = env::var(CLUSTER_SIZE_ENV)
        .context("cluster size")?
        .parse::<usize>()?;
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        start_endpoint().await?;
        let mut state_rx = cluster::cluster_state()?;
        timeout(
            TIMEOUT,
            state_rx.wait_for(|state| state.up().count() == size),
        )
        .await
        .context("every member Up within the timeout")??;
        println!("{JOIN_PREFIX}up");
        std::io::stdout().flush()?;
        anyhow::Ok(())
    })?;

    for line in std::io::stdin().lines() {
        line?;
        println!(
            "{GOSSIP_PREFIX}{}",
            cluster::gossip_bytes().context("endpoint started")?
        );
        std::io::stdout().flush()?;
    }
    Ok(())
}

/// One bulk transfer from this node to one echo server while probers ask round trips of
/// [STREAMS_BENCH_ECHOES] echo children on the target's node, which share the transfer's lane,
/// and as many on a second node, which do not. Every node runs the `max_streams_per_peer` named
/// by [STREAMS_ENV]. After a warm-up transfer, prints the throughput and the ask latency
/// percentiles per node with the number of asks over [STREAMS_BENCH_BLOCKED]. Not a regression
/// gate; run via `just bench-streams <streams>`.
fn streams_bench_client() -> anyhow::Result<()> {
    let streams = env::var(STREAMS_ENV).unwrap_or_else(|_| "default".to_string());
    let runtime = Runtime::new()?;
    let _guard = runtime.enter();
    runtime.block_on(start_endpoint())?;

    let mut target_process = KillOnDrop(spawn_node(Role::Echo)?);
    let target = resolve_ref::<Request>(&mut target_process)?;
    let mut other_process = KillOnDrop(spawn_node(Role::Echo)?);
    let other = resolve_ref::<Request>(&mut other_process)?;

    runtime.block_on(async {
        let same_echoes = spawned_echoes(&target).await?;
        let other_echoes = spawned_echoes(&other).await?;
        bulk_round(
            &target,
            &same_echoes,
            &other_echoes,
            STREAMS_BENCH_WARM_UP_BULKS,
        )
        .await
        .context("warm-up")?;
        let round = bulk_round(&target, &same_echoes, &other_echoes, STREAMS_BENCH_BULKS).await?;

        let seconds = round.elapsed.as_secs_f64();
        let mebibytes =
            f64::from(STREAMS_BENCH_BULKS) * BENCH_BULK_PAYLOAD as f64 / (1024.0 * 1024.0);
        println!(
            "streams {streams}: bulk {:.0} messages/s, {:.1} MiB/s ({STREAMS_BENCH_BULKS} bulks of {} KiB, window {BENCH_BULK_WINDOW})",
            f64::from(STREAMS_BENCH_BULKS) / seconds,
            mebibytes / seconds,
            BENCH_BULK_PAYLOAD / 1024
        );
        for (node, latencies) in [("same node", round.same), ("other node", round.other)] {
            let blocked = latencies
                .iter()
                .filter(|latency| **latency > STREAMS_BENCH_BLOCKED)
                .count();
            println!(
                "streams {streams}: {node} asks p50 {} us, p99 {} us, p99.9 {} us, max {} us, {blocked} of {} over {} ms ({STREAMS_BENCH_ECHOES} echoes, {STREAMS_BENCH_PROBERS} probers)",
                percentile(&latencies, 500).as_micros(),
                percentile(&latencies, 990).as_micros(),
                percentile(&latencies, 999).as_micros(),
                percentile(&latencies, 1_000).as_micros(),
                latencies.len(),
                STREAMS_BENCH_BLOCKED.as_millis()
            );
        }
        anyhow::Ok(())
    })?;

    target.tell(Request::Stop);
    other.tell(Request::Stop);
    expect_exit(&mut target_process, "bench target")?;
    expect_exit(&mut other_process, "bench other")?;
    Ok(())
}

/// [STREAMS_BENCH_ECHOES] fresh children of the given echo server.
async fn spawned_echoes(echo: &ActorRef<Request>) -> anyhow::Result<Vec<ActorRef<Request>>> {
    let mut echoes = Vec::with_capacity(STREAMS_BENCH_ECHOES);
    for _ in 0..STREAMS_BENCH_ECHOES {
        echoes.push(
            echo.ask(TIMEOUT, |reply_to| Request::Spawn { reply_to })
                .await?,
        );
    }
    Ok(echoes)
}

/// Transfers `bulks` bulks to the target while [STREAMS_BENCH_PROBERS] probers ask the echoes,
/// alternating between the nodes, until the transfer is done.
async fn bulk_round(
    target: &ActorRef<Request>,
    same_echoes: &[ActorRef<Request>],
    other_echoes: &[ActorRef<Request>],
    bulks: u32,
) -> anyhow::Result<BulkRound> {
    let transferring = Arc::new(AtomicBool::new(true));
    let mut probers = JoinSet::new();
    for prober in 0..STREAMS_BENCH_PROBERS {
        probers.spawn(probe(
            same_echoes.to_vec(),
            other_echoes.to_vec(),
            prober * STREAMS_BENCH_ECHOES / STREAMS_BENCH_PROBERS,
            transferring.clone(),
        ));
    }

    let started = Instant::now();
    let transferred = bulk_transfer(target, bulks).await;
    let elapsed = started.elapsed();
    transferring.store(false, Ordering::Relaxed);

    let mut round = BulkRound {
        elapsed,
        same: Vec::new(),
        other: Vec::new(),
    };
    while let Some(probed) = probers.join_next().await {
        let (same, other) = probed??;
        round.same.extend(same);
        round.other.extend(other);
    }
    transferred?;
    round.same.sort_unstable();
    round.other.sort_unstable();
    Ok(round)
}

/// Keeps [BENCH_BULK_WINDOW] bulk asks of [BENCH_BULK_PAYLOAD] bytes outstanding until `total`
/// were acknowledged.
async fn bulk_transfer(target: &ActorRef<Request>, total: u32) -> anyhow::Result<()> {
    let next = Arc::new(AtomicU32::new(0));
    let mut senders = JoinSet::new();
    for _ in 0..BENCH_BULK_WINDOW.min(total) {
        let target = target.clone();
        let next = next.clone();
        senders.spawn(async move {
            loop {
                let seq = next.fetch_add(1, Ordering::Relaxed);
                if seq >= total {
                    return anyhow::Ok(());
                }
                let reply = target
                    .ask(TIMEOUT, |reply_to| Request::Bulk {
                        seq,
                        payload: vec![0; BENCH_BULK_PAYLOAD],
                        reply_to,
                    })
                    .await?;
                anyhow::ensure!(reply.seq == seq, "bulk {seq} answered as {}", reply.seq);
            }
        });
    }
    while let Some(sent) = senders.join_next().await {
        sent??;
    }
    Ok(())
}

/// Serial asks while `transferring` holds, alternating between an echo on the target's node and
/// one on the other node and cycling over all echoes from index `first` on, so every echo is asked
/// equally often; answers the latencies per node.
async fn probe(
    same_echoes: Vec<ActorRef<Request>>,
    other_echoes: Vec<ActorRef<Request>>,
    first: usize,
    transferring: Arc<AtomicBool>,
) -> anyhow::Result<(Vec<Duration>, Vec<Duration>)> {
    let mut same = Vec::new();
    let mut other = Vec::new();
    let mut index = first;
    let mut seq = 0_u32;
    while transferring.load(Ordering::Relaxed) {
        same.push(ask_latency(&same_echoes[index % same_echoes.len()], seq).await?);
        other.push(ask_latency(&other_echoes[index % other_echoes.len()], seq).await?);
        index += 1;
        seq += 1;
    }
    Ok((same, other))
}

async fn ask_latency(echo: &ActorRef<Request>, seq: u32) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let reply = echo
        .ask(TIMEOUT, |reply_to| Request::Ask { seq, reply_to })
        .await?;
    anyhow::ensure!(reply.seq == seq, "ask {seq} answered as {}", reply.seq);
    Ok(started.elapsed())
}

/// The latency at `per_mille` of the sorted latencies, zero for none.
fn percentile(latencies: &[Duration], per_mille: usize) -> Duration {
    let rank = (latencies.len() * per_mille).div_ceil(1_000);
    latencies
        .get(rank.saturating_sub(1))
        .copied()
        .unwrap_or_default()
}

/// The duration of one bulk transfer and the sorted ask latencies towards the echoes on the
/// target's node and on the other node during it.
struct BulkRound {
    elapsed: Duration,
    same: Vec<Duration>,
    other: Vec<Duration>,
}

/// Round trip and per-sender FIFO: ordered pings, ordered replies. The reference bytes must
/// resolve as the type they were serialized for and be refused as any other. Then the node is
/// killed and told anyway, and a second node is served afterwards: only that further round trip
/// proves the dead letters left the endpoint usable, which the sends alone cannot show.
fn echo_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let bytes = ref_bytes(&mut echo_process)?;
    let echo = cluster::deserialize_ref::<Request>(&bytes).context("server reference")?;

    let mistyped = cluster::deserialize_ref::<Reply>(&bytes);
    if !matches!(mistyped, Err(cluster::RefError::TypeMismatch)) {
        bail!("reference of another message type resolved: {mistyped:?}");
    }

    let event_rx = run_client(runtime, "echo client termination", |event_tx| EchoClient {
        server: echo.clone(),
        event_tx,
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "echo")?;

    runtime.block_on(async {
        echo.tell(Request::Stop);
        sleep(DEAD_LETTER_GRACE).await;
        echo.tell(Request::Stop);
    });

    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let event_rx = run_client(
        runtime,
        "echo client termination after dead letters",
        |event_tx| EchoClient {
            server: echo,
            event_tx,
        },
    )?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "echo")?;
    Ok(())
}

/// Discovery: a key registered on another node resolves into a working reference, given only that
/// node's address. The lookup starts before the node has joined and is retried while the directory
/// answers empty, until the node has joined and its registrations were replicated here, so
/// bootstrap order does not matter. An unregistered name answers empty while a wrong message type
/// is refused, since only the first is worth retrying. A key registered on this node resolves at
/// this node's own address into a reference which answers, and an unregistered one answers empty.
fn discovery_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);

    let echo = lookup_echo_with_retries(runtime, addr).context("lookup before the node is up")?;

    let missing = receptionist::lookup_at(&Key::<Request>::new("no-such-name"), addr)?;
    if !missing.is_empty() {
        bail!("unregistered name resolved to {missing:?}");
    }

    let mistyped = receptionist::lookup_at(&Key::<Reply>::new(ECHO_KEY), addr);
    if !matches!(mistyped, Err(ReceptionistError::TypeMismatch)) {
        bail!("key of another message type resolved to {mistyped:?}");
    }

    let event_rx = run_client(runtime, "discovered echo client termination", |event_tx| {
        EchoClient {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut echo_process, "discovered echo")?;

    let local = ActorSystem::new(EchoServer);
    cluster::register(&Key::new("own-echo"), local.root())?;
    let resolved = receptionist::lookup_at(&Key::<Request>::new("own-echo"), own())?
        .into_iter()
        .next()
        .context("own address echo")?;
    let reply = runtime.block_on(resolved.ask(TIMEOUT, |reply_to| Request::Ask {
        seq: ROUND_TRIP_SEQ,
        reply_to,
    }))?;
    if reply.seq != ROUND_TRIP_SEQ {
        bail!("own address echo replied {reply:?}");
    }
    let missing = receptionist::lookup_at(&Key::<Request>::new("no-such-name"), own())?;
    if !missing.is_empty() {
        bail!("unregistered name at this node's own address resolved to {missing:?}");
    }
    local.root().tell(Request::Stop);
    runtime
        .block_on(timeout(TIMEOUT, local.terminated()))
        .context("own address echo termination")??;
    Ok(())
}

/// A watched remote actor streams `STREAMED` messages and stops: the terminated signal must
/// arrive behind all of them, in order.
fn ordered_termination_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "ordered watch client termination", |event_tx| {
        OrderedWatch {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    let TestEvent::Child(_) = recv(&event_rx)? else {
        bail!("no child reference from the ordered watch client");
    };
    expect_done(&event_rx)
}

/// Watching an already terminated remote actor must still deliver the terminated signal, and
/// must do so via the watched node's immediate answer, since the node is alive and heartbeating.
/// The subject is terminated by this scenario itself, so the precondition is asserted where it is
/// relied upon rather than inherited from another scenario.
fn watch_terminated_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let dead_child = terminated_child(runtime, keeper).context("terminated child")?;

    let event_rx = run_client(runtime, "watch terminated client termination", |event_tx| {
        WatchTerminated {
            child: dead_child,
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A reference to a remote actor which has provably terminated: it is only returned after its
/// terminated signal has been received.
fn terminated_child(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<ActorRef<StreamerMessage>> {
    let event_rx = run_client(runtime, "terminate child client termination", |event_tx| {
        TerminateChild {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    let TestEvent::Child(child) = recv(&event_rx)? else {
        bail!("no terminated child reference");
    };
    expect_done(&event_rx)?;
    Ok(child)
}

/// After unwatch no terminated signal may be received, even though the remote actor terminates.
fn unwatch_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(UnwatchClient {
        keeper: keeper.clone(),
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::StreamedAll => {}
        TestEvent::Done(result) => bail!("done before probe: {result:?}"),
        _ => bail!("unexpected event from the unwatch client"),
    }
    thread::sleep(UNWATCH_GRACE);
    system.root().tell(ClientEvent::Probe);

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("unwatch client termination")??;
    expect_done(&event_rx)
}

/// Two watchers on this node watching one remote actor must each receive a terminated signal:
/// the wire watch is per watcher, so one registration on the watched node owes two signals.
fn two_watchers_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "two watchers client termination", |event_tx| {
        TwoWatchers {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;

    for _ in 0..WATCHERS {
        expect_done(&event_rx)?;
    }
    Ok(())
}

/// A large message towards one remote actor must not delay messages towards others: the busy
/// target's stream is not the one they ride. Told first and by far the largest, the busy target's
/// acknowledgement would be the first to arrive over a single lane.
fn head_of_line_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "head of line client termination", |event_tx| {
        HeadOfLine {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A message encoding beyond `max_frame_size` becomes a local dead letter instead of tearing down
/// the lane: an empty bulk told right behind it to the same target, riding the same stream, must
/// still be acknowledged, and the oversize one must never be.
fn oversize_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "oversize client termination", |event_tx| {
        Oversize {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// Severing every connection mid-stream loses frames but never reorders or duplicates them: the
/// acknowledgements arrive strictly ascending, i.e. "in order, with gaps", and a marker told
/// after the sever arrives over the reconnected lane, proving it survived with its queue. This
/// also exercises inbound reader supersession: the reconnected connection's reader takes over
/// from the severed one.
fn sever_scenario(runtime: &Runtime, keeper: &ActorRef<KeeperMessage>) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(SeverFifo {
        keeper: keeper.clone(),
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::Bulking => {}
        _ => bail!("unexpected event from the sever client"),
    }

    thread::sleep(SEVER_DELAY);
    if !cluster::sever_connections() {
        bail!("cannot sever, endpoint not started");
    }

    let mut done = None;
    for _ in 0..MARKER_ATTEMPTS {
        system.root().tell(ClientEvent::Probe);
        match event_rx.recv_timeout(MARKER_RETRY_DELAY) {
            Ok(TestEvent::Done(result)) => {
                done = Some(result);
                break;
            }

            Ok(_) => bail!("unexpected event from the sever client"),

            Err(_) => {}
        }
    }
    match done {
        Some(Ok(())) => {}
        Some(Err(message)) => bail!(message),
        None => bail!("no marker acknowledgement after the sever"),
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("sever client termination")??;
    Ok(())
}

/// A watched remote actor streams large messages and stops while the client severs every
/// connection: frames are lost, but the terminated signal still arrives, through the reconnect or
/// the watch refresh, and no message from the actor arrives after it.
fn sever_terminated_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(SeverTerminated {
        keeper: keeper.clone(),
        event_tx,
    });

    match recv(&event_rx)? {
        TestEvent::Bulking => {}
        _ => bail!("unexpected event from the sever terminated client"),
    }
    thread::sleep(SEVER_DELAY);
    if !cluster::sever_connections() {
        bail!("cannot sever, endpoint not started");
    }

    match recv(&event_rx)? {
        TestEvent::StreamedAll => {}
        TestEvent::Done(Err(message)) => bail!(message),
        _ => bail!("unexpected event instead of the terminated signal"),
    }
    thread::sleep(DEAD_LETTER_GRACE);
    system.root().tell(ClientEvent::Probe);
    expect_done(&event_rx)?;

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("sever terminated client termination")??;
    Ok(())
}

/// A terminated frame dropped on the watched node (fault injection) must still reach the watcher:
/// the periodic watch refresh re-asserts the watch, and the watched node answers a watch for a
/// meanwhile terminated actor with `Terminated`.
fn lost_terminated_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "lost terminated client termination", |event_tx| {
        LostTerminated {
            keeper: keeper.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// An ask towards a terminated remote actor is dead-lettered on the receiving node without being
/// decoded, so no proxy exists whose drop could answer; the reply tags riding the message frame
/// let that node answer nonetheless, resolving the ask as `NoReply` rather than by timeout.
fn dead_target_ask_scenario(
    runtime: &Runtime,
    keeper: &ActorRef<KeeperMessage>,
) -> anyhow::Result<()> {
    let dead_child = terminated_child(runtime, keeper).context("terminated child")?;

    let asked =
        runtime.block_on(dead_child.ask(TIMEOUT, |reply_to| StreamerMessage::Ask { reply_to }));
    if !matches!(asked, Err(AskError::NoReply)) {
        bail!("ask towards a terminated actor resolved to {asked:?}");
    }
    Ok(())
}

/// A remote `ask` resolves with the reply; a responder dropping its `ReplyTo` resolves the ask as
/// `NoReply` via the reply-dropped notification rather than by timeout; a request beyond
/// `max_frame_size` fails the ask at the send and an oversize reply resolves it as `NoReply` via
/// the reply-dropped notification, both instead of by timeout.
fn remote_ask_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    for seq in 0..ASKS {
        let reply = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Ask { seq, reply_to }))
            .context("ask")?;
        if reply.seq != seq {
            bail!("reply {} instead of {seq}", reply.seq);
        }
    }

    let ignored = runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::Ignore { reply_to }));
    if !matches!(ignored, Err(AskError::NoReply)) {
        bail!("ignored ask resolved to {ignored:?}");
    }

    let oversize = runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::AskOversize {
        payload: vec![0; OVERSIZE_PAYLOAD],
        reply_to,
    }));
    if !matches!(oversize, Err(AskError::TooLarge { .. })) {
        bail!("oversize ask resolved to {oversize:?}");
    }

    let oversize_reply =
        runtime.block_on(echo.ask(TIMEOUT, |reply_to| Request::AskOversizeReply { reply_to }));
    if !matches!(oversize_reply, Err(AskError::NoReply)) {
        bail!("oversize reply ask resolved to {oversize_reply:?}");
    }
    Ok(())
}

/// A reply created by `reply_to` stays FIFO with the responder's other messages to the asker: the
/// server tells a marker and then replies, so the marker must arrive first.
fn reply_to_fifo_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "ask then tell client termination", |event_tx| {
        AskThenTellClient {
            server: echo.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)
}

/// A `ReplyTo` forwarded to a third actor still resolves its ask: the echo node re-serializes it
/// towards a responder on the client node, chaining the reply over two hops.
fn forwarded_reply_scenario(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<()> {
    let system = ActorSystem::new(Responder);
    let responder = system.root().clone();

    let reply = runtime
        .block_on(echo.ask(TIMEOUT, |reply_to| Request::Forward {
            to: responder,
            reply_to,
        }))
        .context("forwarded ask")?;
    if reply.seq != FORWARD_SEQ {
        bail!("forwarded reply {} instead of {FORWARD_SEQ}", reply.seq);
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("responder termination")??;
    Ok(())
}

/// A `ReplyTo` serialized and resolved on its own node comes home as the original destination,
/// and a second serialization of the same value is refused.
fn reply_serde_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let event_rx = run_client(runtime, "serde round trip client termination", |event_tx| {
        SerdeRoundTrip { event_tx }
    })?;
    expect_done(&event_rx)
}

/// Killing a node holding a `ReplyTo` fails the pending ask as `NoReply` via failure detection,
/// next to the synthesized terminated signal, rather than leaving it to its timeout. A probe ask
/// behind the hold proves the hold arrived before the node is killed.
fn ask_node_death_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo.clone(),
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };

    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    echo_process.0.kill().context("killing the echo process")?;
    expect_done(&event_rx)?;

    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;
    Ok(())
}

/// Killing a node nothing watches still fails the pending ask as `NoReply`: every member is
/// heartbeated, so downing, not the ask's timeout, settles asks towards a dead member. The
/// connections are severed after the kill, since noticing the loss is the transport's business
/// and not what this scenario proves; a probe ask behind the hold proves the hold arrived before
/// the node is killed.
fn ask_down_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(GIVE_UP_TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };

    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    echo_process.0.kill().context("killing the echo process")?;
    if !cluster::sever_connections() {
        bail!("cannot sever, endpoint not started");
    }

    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }
    Ok(())
}

/// Downing derives from the shared reachability graph, not from this node's own detector: in a
/// three node cluster the client alone cannot put the killed node outside its component, because
/// the surviving node is still a path to it. The synthesized signal hence only arrives once the
/// survivor's observation has been relayed here, and the survivor itself stays Up throughout.
fn relayed_reachability_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut witness_process = KillOnDrop(spawn_node(Role::Echo)?);
    let witness = resolve_ref::<Request>(&mut witness_process)?;
    let mut victim_process = KillOnDrop(spawn_node(Role::Echo)?);
    let victim = resolve_ref::<Request>(&mut victim_process)?;
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;

    let deadline = Instant::now() + TIMEOUT;
    let (witness_addr, victim_addr) = loop {
        let witness_view = runtime
            .block_on(witness.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("witness members ask")?;
        let victim_view = runtime
            .block_on(victim.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("victim members ask")?;

        let all = [self_addr, witness_view.own, victim_view.own];
        let converged =
            |view: &MemberAddrs| all.iter().all(|member_addr| view.up.contains(member_addr));
        if converged(&witness_view) && converged(&victim_view) {
            break (witness_view.own, victim_view.own);
        }
        if Instant::now() >= deadline {
            bail!("members did not converge: {witness_view:?} versus {victim_view:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    };

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: victim,
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the relayed reachability client"),
    }
    victim_process
        .0
        .kill()
        .context("killing the victim process")?;

    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("relayed reachability client termination")??;

    let members = cluster::members().context("members")?;
    if !members
        .iter()
        .any(|member| member.addr() == victim_addr && member.state() == MemberState::Down)
    {
        bail!("the killed node is not Down in {members:?}");
    }
    if !members
        .iter()
        .any(|member| member.addr() == witness_addr && member.state() == MemberState::Up)
    {
        bail!("the surviving node is not Up in {members:?}");
    }

    witness.tell(Request::Stop);
    expect_exit(&mut witness_process, "relayed reachability witness")?;
    Ok(())
}

/// A node restarted at its old address is a new incarnation. The client first talks to the old
/// one, which binds the lane, then watches it and kills it. Failure detection then tombstones the
/// old incarnation and severs the lane. A retried lookup against the restarted node must resolve a
/// working reference, and full per-sender FIFO must hold on the fresh lane. That proves that a
/// tombstone kills an incarnation, never its address.
fn restart_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let event_rx = run_client(runtime, "ping once client termination", |event_tx| {
        PingOnce {
            server: echo.clone(),
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo,
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }
    echo_process.0.kill().context("killing the echo process")?;
    echo_process.0.wait().context("reaping the echo process")?;
    force_collision("rebind", addr);
    let held = hold_addr(addr)?;
    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;

    drop(held);
    force_collision("respawn", addr);
    let mut restarted_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);

    let echo =
        lookup_echo_with_retries(runtime, addr).context("no reference from the restarted node")?;

    let event_rx = run_client(runtime, "echo client after restart", |event_tx| {
        EchoClient {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;

    expect_exit(&mut restarted_process, "restarted echo")?;
    Ok(())
}

/// The change stream carries every version the watch shows, consecutively and without a gap
/// under this load, and every state keeps the invariants of the type; `diff` yields a joining
/// member Up, a killed member unreachable and then Down, and a leaving member Down without ever
/// being unreachable; every verdict names a version already streamed as a state; and the stream
/// ends where the watch stands.
fn changes_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut changes = cluster::changes()?;
    let state_rx = cluster::cluster_state()?;
    let mut observed = Vec::new();

    let mut echo_process = KillOnDrop(spawn_node(Role::Echo)?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;
    let echo_addr = own_addr(runtime, &echo)?;
    await_change(runtime, &mut changes, &mut observed, |state| {
        is_up(state, echo_addr)
    })?;

    let hold = HoldDowning::new();
    echo_process.0.kill().context("killing the echo process")?;
    await_change(runtime, &mut changes, &mut observed, |state| {
        is_up(state, echo_addr) && is_unreachable(state, echo_addr)
    })?;
    drop(hold);
    await_change(runtime, &mut changes, &mut observed, |state| {
        is_down(state, echo_addr)
    })?;

    let mut leave_process = KillOnDrop(spawn_node(Role::Leave)?);
    let leaver = resolve_ref::<Request>(&mut leave_process)?;
    let leaver_addr = own_addr(runtime, &leaver)?;
    await_change(runtime, &mut changes, &mut observed, |state| {
        is_up(state, leaver_addr)
    })?;
    leaver.tell(Request::Stop);
    await_change(runtime, &mut changes, &mut observed, |state| {
        is_left(state, leaver_addr)
    })?;
    expect_exit(&mut leave_process, "leaving echo")?;
    let current = state_rx.borrow().version();
    await_change(runtime, &mut changes, &mut observed, |state| {
        state.version() >= current
    })?;

    if let Some(gap) = observed
        .iter()
        .find(|change| matches!(change, Change::Gap { .. }))
    {
        bail!("the stream reported a gap under ordinary load: {gap:?}");
    }
    let states = observed
        .iter()
        .filter_map(|change| match change {
            Change::State(state) => Some(state),
            _ => None,
        })
        .collect::<Vec<_>>();
    for pair in states.windows(2) {
        if pair[1].version() != pair[0].version() + 1 {
            bail!("versions are not consecutive: {pair:?}");
        }
    }
    for state in &states {
        let addrs = state.members().iter().map(Member::addr).collect::<Vec<_>>();
        if !addrs.is_sorted() {
            bail!("members are not ordered by address: {state:?}");
        }
        for member in state.unreachable() {
            if member.state() != MemberState::Up || !state.members().contains(member) {
                bail!("an unreachable entry is not an Up member: {state:?}");
            }
        }
    }
    let transitions = states
        .windows(2)
        .flat_map(|pair| pair[1].diff(pair[0]))
        .collect::<Vec<_>>();
    let saw = |expected: fn(&Transition) -> bool, addr: SocketAddr| {
        transitions
            .iter()
            .any(|transition| expected(transition) && transition.member().addr() == addr)
    };
    let expected = [
        (
            (|transition: &Transition| matches!(transition, Transition::Up(_))) as fn(&_) -> _,
            echo_addr,
            "the echo's Up",
        ),
        (
            |transition: &Transition| matches!(transition, Transition::Unreachable(_)),
            echo_addr,
            "the echo's unreachability",
        ),
        (
            |transition: &Transition| matches!(transition, Transition::Down(_)),
            echo_addr,
            "the echo's Down",
        ),
        (
            |transition: &Transition| matches!(transition, Transition::Up(_)),
            leaver_addr,
            "the leaver's Up",
        ),
        (
            |transition: &Transition| matches!(transition, Transition::Left(_)),
            leaver_addr,
            "the leaver's Left",
        ),
    ];
    for (predicate, addr, what) in expected {
        if !saw(predicate, addr) {
            bail!("the diffs do not show {what}: {transitions:?}");
        }
    }
    if saw(
        |transition| matches!(transition, Transition::Unreachable(_)),
        leaver_addr,
    ) {
        bail!("the leaving member was diffed unreachable: {transitions:?}");
    }

    let mut streamed = HashSet::new();
    for change in &observed {
        match change {
            Change::State(state) => {
                streamed.insert(state.version());
            }

            Change::Settled(settlement) => {
                if !streamed.contains(&settlement.version()) {
                    bail!("a verdict precedes its state: {settlement:?}");
                }
            }

            Change::Gap { .. } => {}
        }
    }
    let last = states.last().context("no state was streamed")?;
    if **last != *state_rx.borrow() {
        bail!(
            "the stream ends at version {}, the watch at {}",
            last.version(),
            state_rx.borrow().version()
        );
    }
    Ok(())
}

/// Awaits the first streamed state satisfying the predicate, collecting every change seen.
fn await_change(
    runtime: &Runtime,
    changes: &mut Changes,
    observed: &mut Vec<Change>,
    satisfied: impl Fn(&ClusterState) -> bool,
) -> anyhow::Result<()> {
    let last_state = observed.iter().rev().find_map(|change| match change {
        Change::State(state) => Some(state),
        _ => None,
    });
    if last_state.is_some_and(&satisfied) {
        return Ok(());
    }

    runtime
        .block_on(timeout(TIMEOUT, async {
            loop {
                let change = changes.next().await;
                let done = matches!(&change, Change::State(state) if satisfied(state));
                observed.push(change);
                if done {
                    return;
                }
            }
        }))
        .context("awaiting a change")
}

/// Awaits the verdict that the receptionist is settled at the given version.
fn await_settled(runtime: &Runtime, changes: &mut Changes, version: u64) -> anyhow::Result<()> {
    runtime
        .block_on(timeout(TIMEOUT, async {
            loop {
                if let Change::Settled(settlement) = changes.next().await
                    && settlement.version() == version
                    && settlement.settled()
                {
                    return;
                }
            }
        }))
        .context("awaiting the settled verdict")
}

/// Awaits the first published value satisfying the predicate, collecting every value seen.
fn await_state(
    runtime: &Runtime,
    state_rx: &mut watch::Receiver<ClusterState>,
    observed: &mut Vec<ClusterState>,
    satisfied: impl Fn(&ClusterState) -> bool,
) -> anyhow::Result<()> {
    if observed.last().is_some_and(&satisfied) {
        return Ok(());
    }

    runtime
        .block_on(timeout(TIMEOUT, async {
            loop {
                state_rx
                    .changed()
                    .await
                    .context("cluster state channel closed")?;
                let state = state_rx.borrow_and_update().clone();
                observed.push(state.clone());
                if satisfied(&state) {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        }))
        .context("awaiting a cluster state")?
}

fn is_up(state: &ClusterState, addr: SocketAddr) -> bool {
    state.up().any(|member| member.addr() == addr)
}

fn is_left(state: &ClusterState, addr: SocketAddr) -> bool {
    state
        .members()
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Left)
}

fn is_down(state: &ClusterState, addr: SocketAddr) -> bool {
    state
        .members()
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Down)
}

fn is_unreachable(state: &ClusterState, addr: SocketAddr) -> bool {
    state
        .unreachable()
        .iter()
        .any(|member| member.addr() == addr)
}

/// The receptionist converges across nodes and shrinks on every kind of departure: a local
/// registration and two remote ones make three everywhere; a child spawned and registered on a
/// running node comes and goes through termination eviction while its node stays Up; a killed
/// node's actor is gone the moment its Down is published; and a wrong message type is refused.
fn receptionist_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let key = Key::<Request>::new(ECHO_KEY);
    let mut state_rx = cluster::cluster_state()?;
    let mut observed = vec![state_rx.borrow_and_update().clone()];
    let mut subscription = receptionist::subscribe(&key)?;
    let mut changes = cluster::changes()?;

    let system = ActorSystem::new(EchoServer);
    cluster::register(&key, system.root()).context("registering the local echo")?;
    await_listing(runtime, &mut subscription, |refs| refs.len() == 1)?;

    let mut a_process = KillOnDrop(spawn_node(Role::Echo)?);
    let a = resolve_ref::<Request>(&mut a_process)?;
    let mut b_process = KillOnDrop(spawn_node(Role::Echo)?);
    let b = resolve_ref::<Request>(&mut b_process)?;
    let a_addr = own_addr(runtime, &a)?;
    let b_addr = own_addr(runtime, &b)?;
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;

    let refs = await_listing(runtime, &mut subscription, |refs| refs.len() == 3)?;
    let mut owners = refs
        .iter()
        .map(|echo| own_addr(runtime, echo))
        .collect::<anyhow::Result<Vec<_>>>()?;
    owners.sort_unstable();
    let mut expected = vec![self_addr, a_addr, b_addr];
    expected.sort_unstable();
    if owners != expected {
        bail!("the listing resolves {owners:?}, not {expected:?}");
    }
    await_directory(runtime, &a, 3)?;
    await_directory(runtime, &b, 3)?;
    if receptionist::lookup(&key)?.len() != 3 {
        bail!("the local lookup does not resolve three echoes");
    }
    let version = state_rx.borrow().version();
    await_settled(runtime, &mut changes, version)
        .context("the receptionist is not settled with every member delivered")?;
    if !receptionist::settled(version)? {
        bail!("the receptionist is not settled with every member delivered");
    }
    match receptionist::lookup(&Key::<Reply>::new(ECHO_KEY)) {
        Err(ReceptionistError::TypeMismatch) => {}
        other => bail!("a key of another message type resolved to {other:?}"),
    }

    let child = runtime
        .block_on(a.ask(TIMEOUT, |reply_to| Request::Spawn { reply_to }))
        .context("spawning a child echo")?;
    await_listing(runtime, &mut subscription, |refs| refs.len() == 4)?;
    child.tell(Request::Stop);
    await_listing(runtime, &mut subscription, |refs| refs.len() == 3)?;
    if !is_up(&state_rx.borrow(), a_addr) {
        bail!("the child's node is not Up while its termination was replicated");
    }

    b_process.0.kill().context("killing echo b")?;
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_down(state, b_addr)
    })?;
    let refs = receptionist::lookup(&key)?;
    if refs.len() != 2 || refs.contains(&b) {
        bail!("a downed member's echo is still resolved: {refs:?}");
    }
    if !receptionist::settled(state_rx.borrow().version())? {
        bail!("the receptionist is not settled after a downing");
    }
    await_listing(runtime, &mut subscription, |refs| refs.len() == 2)?;

    a.tell(Request::Stop);
    expect_exit(&mut a_process, "echo a")?;
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_down(state, a_addr)
    })?;
    await_listing(runtime, &mut subscription, |refs| refs.len() == 1)?;

    system.root().tell(Request::Stop);
    await_listing(runtime, &mut subscription, |refs| refs.is_empty())?;
    if !receptionist::lookup(&key)?.is_empty() {
        bail!("the local echo is still resolved after its termination");
    }
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("local echo termination")??;
    Ok(())
}

/// Awaits the first listing satisfying the predicate, reading through the subscription.
fn await_listing(
    runtime: &Runtime,
    subscription: &mut Subscription<Request>,
    satisfied: impl Fn(&[ActorRef<Request>]) -> bool,
) -> anyhow::Result<Vec<ActorRef<Request>>> {
    runtime
        .block_on(timeout(TIMEOUT, async {
            loop {
                let refs = subscription.current().context("receptionist listing")?;
                if satisfied(&refs) {
                    return Ok::<_, anyhow::Error>(refs);
                }
                subscription.changed().await;
            }
        }))
        .context("awaiting a receptionist listing")?
}

/// Polls a node until its receptionist resolves the given number of echoes and is settled.
fn await_directory(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    echoes: usize,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let directory = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Directory { reply_to }))
            .context("directory ask")?;
        if directory.echoes == echoes && directory.settled {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("{} did not converge: {directory:?}", directory.own);
        }
        thread::sleep(CONVERGENCE_POLL);
    }
}

/// A publish reaches every subscriber the publishing node resolves, wherever it lives, and the
/// set shrinks with a departure: a killed node's subscriber is gone the moment its Down is
/// published, a local one's with its termination, and a topic of another message type is refused
/// rather than published to nobody. Several subscribers on one node receive the publishes in
/// order, interleaved with the messages told to them directly.
fn pubsub_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let topic = Topic::<Note>::new(TOPIC);
    let mut state_rx = cluster::cluster_state()?;
    let mut observed = vec![state_rx.borrow_and_update().clone()];
    let mut subscribers = receptionist::subscribe(topic.key())?;

    let system = ActorSystem::new(EchoServer);
    let echo = system.root().clone();
    runtime
        .block_on(echo.ask(TIMEOUT, |reply_to| Request::Subscribe { reply_to }))
        .context("subscribing the local recorder")?;

    let mut a_process = KillOnDrop(spawn_node(Role::Echo)?);
    let a = resolve_ref::<Request>(&mut a_process)?;
    let mut b_process = KillOnDrop(spawn_node(Role::Echo)?);
    let b = resolve_ref::<Request>(&mut b_process)?;
    let a_addr = own_addr(runtime, &a)?;
    let b_addr = own_addr(runtime, &b)?;
    for (echo, what) in [(&a, "echo a"), (&b, "echo b")] {
        runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Subscribe { reply_to }))
            .with_context(|| format!("subscribing the recorder of {what}"))?;
    }

    await_subscribers(runtime, &mut subscribers, 3)?;
    await_resolved(runtime, &a, 3)?;
    await_resolved(runtime, &b, 3)?;

    for seq in 1..=5 {
        let told = pubsub::publish(&topic, Note { from: own(), seq })?;
        if told != 3 {
            bail!("a local publish told {told} subscribers, not 3");
        }
    }
    publish_at(runtime, &a, 6..=8, 3)?;
    publish_at(runtime, &b, 9..=10, 3)?;

    let published = [
        (own(), vec![1, 2, 3, 4, 5]),
        (a_addr, vec![6, 7, 8]),
        (b_addr, vec![9, 10]),
    ];
    await_notes(runtime, &echo, &published)?;
    await_notes(runtime, &a, &published)?;
    await_notes(runtime, &b, &published)?;

    match pubsub::publish(&Topic::<Reply>::new(TOPIC), Reply { seq: 0 }) {
        Err(ReceptionistError::TypeMismatch) => {}
        other => bail!("a topic of another message type published to {other:?}"),
    }

    b_process.0.kill().context("killing echo b")?;
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_down(state, b_addr)
    })?;
    let told = pubsub::publish(
        &topic,
        Note {
            from: own(),
            seq: 11,
        },
    )?;
    if told != 2 {
        bail!("a publish after a downing told {told} subscribers, not 2");
    }
    await_resolved(runtime, &a, 2)?;
    publish_at(runtime, &a, 12..=12, 2)?;
    let published = [
        (own(), vec![1, 2, 3, 4, 5, 11]),
        (a_addr, vec![6, 7, 8, 12]),
        (b_addr, vec![9, 10]),
    ];
    await_notes(runtime, &echo, &published)?;
    await_notes(runtime, &a, &published)?;

    echo.tell(Request::Stop);
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("local echo termination")??;
    await_subscribers(runtime, &mut subscribers, 1)?;
    let told = pubsub::publish(
        &topic,
        Note {
            from: own(),
            seq: 13,
        },
    )?;
    if told != 1 {
        bail!("a publish after the local subscriber terminated told {told} subscribers, not 1");
    }
    let published = [
        (own(), vec![1, 2, 3, 4, 5, 11, 13]),
        (a_addr, vec![6, 7, 8, 12]),
        (b_addr, vec![9, 10]),
    ];
    await_notes(runtime, &a, &published)?;

    interleaved_pubsub(runtime, &topic, &mut subscribers, &a)?;

    a.tell(Request::Stop);
    expect_exit(&mut a_process, "echo a")
}

/// Subscribes two further recorders on the given node, which holds one already, and alternates
/// publishes with direct tells to the two: each recorder receives exactly what was sent to it, in
/// the order sent, since a publish rides the stream of each recipient like a direct tell.
fn interleaved_pubsub(
    runtime: &Runtime,
    topic: &Topic<Note>,
    subscribers: &mut Subscription<Note>,
    echo: &ActorRef<Request>,
) -> anyhow::Result<()> {
    const FIRST_SEQ: u64 = 100;

    let mut recorders = Vec::new();
    for _ in 0..2 {
        let recorder = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Subscribe { reply_to }))
            .context("subscribing a further recorder")?;
        recorders.push(recorder);
    }
    await_subscribers(runtime, subscribers, 3)?;
    let mut expected = receptionist::lookup(topic.key())?
        .iter()
        .map(|recorder| (recorder.actor_id(), Vec::new()))
        .collect::<Vec<_>>();

    for seq in FIRST_SEQ..FIRST_SEQ + 20 {
        let note = Note { from: own(), seq };
        if seq % 2 == 0 {
            let reached = pubsub::publish(topic, note)?;
            if reached != 3 {
                bail!("an interleaved publish told {reached} subscribers, not 3");
            }
            for (_, seqs) in &mut expected {
                seqs.push(seq);
            }
        } else {
            for recorder in &recorders {
                recorder.tell(note.clone());
            }
            for (recorder, seqs) in &mut expected {
                if recorders.iter().any(|told| told.actor_id() == *recorder) {
                    seqs.push(seq);
                }
            }
        }
    }

    let deadline = Instant::now() + TIMEOUT;
    for (recorder, seqs) in expected {
        loop {
            let received = runtime
                .block_on(echo.ask(TIMEOUT, |reply_to| Request::RecorderNotes {
                    recorder,
                    reply_to,
                }))
                .context("recorder notes ask")?
                .into_iter()
                .filter(|note| note.from == own() && note.seq >= FIRST_SEQ)
                .map(|note| note.seq)
                .collect::<Vec<_>>();
            if received == seqs {
                break;
            }
            if Instant::now() >= deadline {
                bail!("recorder {recorder} holds {received:?}, not {seqs:?}");
            }
            thread::sleep(CONVERGENCE_POLL);
        }
    }
    Ok(())
}

/// Awaits the subscriber set of [TOPIC] at the given size, reading through the subscription.
fn await_subscribers(
    runtime: &Runtime,
    subscribers: &mut Subscription<Note>,
    expected: usize,
) -> anyhow::Result<()> {
    runtime
        .block_on(timeout(TIMEOUT, async {
            loop {
                let resolved = subscribers.current().context("subscriber listing")?.len();
                if resolved == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                subscribers.changed().await;
            }
        }))
        .with_context(|| format!("awaiting {expected} subscribers"))?
}

/// Polls a node until its own receptionist resolves the given number of subscribers, since every
/// node converges on its own clock and a publish reaches what the publisher resolves.
fn await_resolved(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    expected: usize,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let resolved = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Subscribers { reply_to }))
            .context("subscribers ask")?;
        if resolved == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("a node resolves {resolved} subscribers, not {expected}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }
}

/// Publishes the sequence numbers on the given node, each expected to reach `expected`
/// subscribers.
fn publish_at(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    seqs: RangeInclusive<u64>,
    expected: usize,
) -> anyhow::Result<()> {
    for seq in seqs {
        let told = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Publish { seq, reply_to }))
            .context("publish ask")?;
        if told != expected {
            bail!("a remote publish told {told} subscribers, not {expected}");
        }
    }
    Ok(())
}

/// Awaits a node holding every published note, each publisher's in ascending order: the notes of
/// one publisher ride one stream towards one subscriber, so their order is the publish order.
fn await_notes(
    runtime: &Runtime,
    echo: &ActorRef<Request>,
    published: &[(SocketAddr, Vec<u64>)],
) -> anyhow::Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let notes = runtime
            .block_on(echo.ask(TIMEOUT, |reply_to| Request::Notes { reply_to }))
            .context("notes ask")?;
        let complete = published.iter().all(|(from, seqs)| {
            let received = notes
                .iter()
                .filter(|note| note.from == *from)
                .map(|note| note.seq)
                .collect::<Vec<_>>();
            received == *seqs
        });
        if complete {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("a node holds {notes:?}, not {published:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }
}

fn own() -> SocketAddr {
    *SELF_ADDR.get().expect("endpoint started")
}

fn own_addr(runtime: &Runtime, echo: &ActorRef<Request>) -> anyhow::Result<SocketAddr> {
    Ok(runtime
        .block_on(echo.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
        .context("members ask")?
        .own)
}

/// Every node bootstraps from the identical address list instead of being told whom to join:
/// each resolves the list, waits for it to settle and joins through the lowest address other
/// than its own, which keeps the join graph connected, so all three nodes end up in one cluster
/// whichever of them happens to hold the lowest address.
fn bootstrap_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;
    let addrs = reserved_addrs(2)?;
    let (first_addr, second_addr) = (addrs[0], addrs[1]);
    let seeds = vec![self_addr, first_addr, second_addr];
    force_collision("shared", first_addr);

    let mut first_process = KillOnDrop(spawn_bootstrap_node(Role::Echo, first_addr, &seeds)?);
    let mut second_process = KillOnDrop(spawn_bootstrap_node(Role::Echo, second_addr, &seeds)?);
    runtime.block_on(bootstrap_cluster(seeds))?;

    let first = resolve_ref::<Request>(&mut first_process)?;
    let second = resolve_ref::<Request>(&mut second_process)?;

    let deadline = Instant::now() + TIMEOUT;
    loop {
        let first_view = runtime
            .block_on(first.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("first members ask")?;
        let second_view = runtime
            .block_on(second.ask(TIMEOUT, |reply_to| Request::Members { reply_to }))
            .context("second members ask")?;

        let all = [self_addr, first_addr, second_addr];
        let converged =
            |view: &MemberAddrs| all.iter().all(|member_addr| view.up.contains(member_addr));
        let members = cluster::members().context("members")?;
        let client_converged = all.iter().all(|member_addr| {
            members
                .iter()
                .any(|member| member.addr() == *member_addr && member.state() == MemberState::Up)
        });
        if converged(&first_view) && converged(&second_view) && client_converged {
            break;
        }
        if Instant::now() >= deadline {
            bail!("bootstrap did not converge: {first_view:?} versus {second_view:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    first.tell(Request::Stop);
    second.tell(Request::Stop);
    expect_exit(&mut first_process, "first bootstrap echo")?;
    expect_exit(&mut second_process, "second bootstrap echo")?;
    Ok(())
}

/// Downing a live node this endpoint merely messages: the pending ask fails as `NoReply`, the
/// watcher receives the synthesized signal although the node is alive, the member is listed
/// Down, and the retention forgets the entry; a fresh incarnation joining at the same address
/// afterwards proves the fence binds the incarnation, never the address. Downing this node's
/// own address, or one no member advertises, is refused with its own error, so a caller can tell
/// either apart from a down which took effect.
fn down_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let addr = reserved_addr()?;
    let mut echo_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo = resolve_ref::<Request>(&mut echo_process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(DeathWatch {
        subject: echo.clone(),
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the death watch client"),
    }

    let (probe_tx, probe_rx) = mpsc::channel();
    let held = {
        let echo = echo.clone();
        runtime.spawn(async move {
            let hold = echo.ask(TIMEOUT, |reply_to| Request::Hold { reply_to });
            let probe = async {
                let probe = echo
                    .ask(TIMEOUT, |reply_to| Request::Ask { seq: 0, reply_to })
                    .await;
                let _ = probe_tx.send(probe);
            };
            let (hold, ()) = tokio::join!(hold, probe);
            hold
        })
    };
    let probe = probe_rx.recv_timeout(TIMEOUT).context("probe reply")?;
    if !matches!(probe, Ok(Reply { seq: 0 })) {
        bail!("probe ask resolved to {probe:?}");
    }

    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;
    match cluster::down(self_addr) {
        Err(DownError::ThisNode) => {}
        other => bail!("downing this node's own address resolved to {other:?}"),
    }
    match cluster::down(reserved_addr()?) {
        Err(DownError::NotAMember(_)) => {}
        other => bail!("downing an address no member advertises resolved to {other:?}"),
    }

    cluster::down(addr).context("downing the echo node")?;

    expect_done(&event_rx)?;
    let held = runtime.block_on(held).context("held ask task")?;
    if !matches!(held, Err(AskError::NoReply)) {
        bail!("held ask resolved to {held:?}");
    }
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("death watch client termination")??;

    let members = cluster::members().context("members")?;
    if !members
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Down)
    {
        bail!("downed member not listed Down in {members:?}");
    }

    let deadline = Instant::now() + TIMEOUT;
    while cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr)
    {
        if Instant::now() >= deadline {
            bail!("down entry at {addr} not forgotten");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    echo_process.0.kill().context("killing the downed echo")?;
    echo_process.0.wait().context("reaping the downed echo")?;
    force_collision("rebind", addr);
    let held = hold_addr(addr)?;
    drop(held);
    force_collision("respawn", addr);
    let restarted_process = KillOnDrop(spawn_node_at(Role::Echo, Some(addr))?);
    let echo =
        lookup_echo_with_retries(runtime, addr).context("no reference from the rejoined node")?;

    let event_rx = run_client(runtime, "ping once client after rejoin", |event_tx| {
        PingOnce {
            server: echo,
            event_tx,
        }
    })?;
    expect_done(&event_rx)?;
    drop(restarted_process);
    Ok(())
}

/// A downed node which is frozen meanwhile, a zombie, comes back after its detailed Down entry has
/// been swept: the address watermark alone must refuse it, so it learns its fate and downs itself
/// instead of being admitted again.
fn zombie_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut zombie = KillOnDrop(spawn_node_at(Role::Zombie, None)?);
    let addr = bound_addr(&zombie)?;
    let mut state_rx = cluster::cluster_state()?;
    let mut observed = Vec::new();
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_up(state, addr)
    })
    .context("the zombie joins")?;

    signal(&zombie, "STOP")?;
    cluster::down(addr).context("downing the zombie")?;
    let deadline = Instant::now() + TIMEOUT;
    while cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr)
    {
        if Instant::now() >= deadline {
            bail!("the zombie's Down entry at {addr} was not swept");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    signal(&zombie, "CONT")?;
    expect_join_line(&mut zombie, "downed").context("the zombie learns it was downed")?;
    expect_exit(&mut zombie, "zombie")?;

    if cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Up)
    {
        bail!("the zombie at {addr} was admitted again");
    }
    Ok(())
}

/// A join from a new address is held back while a member is unreachable, which a frozen zombie
/// stands in for, and admitted once that member is downed. The other zombie never downs anybody
/// and this process holds its downing, so nothing but the explicit down ends the hold. Any member
/// left Up by an earlier scenario would hold the joins as well, so the scenario waits for their
/// downing first.
fn held_join_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut state_rx = cluster::cluster_state()?;
    let mut observed = Vec::new();
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        state.up().count() == 1
    })
    .context("the nodes of earlier scenarios are downed")?;

    let hold = HoldDowning::new();
    let mut witness = KillOnDrop(spawn_node_at(Role::Zombie, None)?);
    let mut frozen = KillOnDrop(spawn_node_at(Role::Zombie, None)?);
    let (witness_addr, frozen_addr) = (bound_addr(&witness)?, bound_addr(&frozen)?);
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_up(state, witness_addr) && is_up(state, frozen_addr)
    })
    .context("the zombies join")?;

    signal(&frozen, "STOP")?;
    await_state(runtime, &mut state_rx, &mut observed, |state| {
        is_unreachable(state, frozen_addr)
    })
    .context("the frozen zombie becomes unreachable")?;

    let mut joiner = KillOnDrop(spawn_node_at(Role::Echo, None)?);
    let joiner_addr = bound_addr(&joiner)?;
    thread::sleep(JOIN_HOLD_GRACE);
    if is_up(&state_rx.borrow(), joiner_addr) {
        bail!("the join at {joiner_addr} was admitted while a member is unreachable");
    }

    cluster::down(frozen_addr).context("downing the frozen zombie")?;
    let echo = resolve_ref::<Request>(&mut joiner).context("the held join is admitted")?;
    if !is_up(&state_rx.borrow(), joiner_addr) {
        bail!("the admitted joiner at {joiner_addr} is not Up");
    }
    drop(hold);

    echo.tell(Request::Stop);
    expect_exit(&mut joiner, "joiner")?;
    signal(&frozen, "CONT")?;
    expect_join_line(&mut frozen, "downed").context("the frozen zombie learns it was downed")?;
    expect_exit(&mut frozen, "frozen zombie")?;
    cluster::down(witness_addr).context("downing the witness zombie")?;
    expect_join_line(&mut witness, "downed").context("the witness learns it was downed")?;
    expect_exit(&mut witness, "witness zombie")
}

/// [KeepMajority] resolves a partition towards one side, which a frozen node stands in for here,
/// in clusters of their own apart from the client's. Of three nodes the two which still see each
/// other down the third. Of two every failure is an even split, decided by the lowest address:
/// freezing the higher one, the lower survives; freezing the lower one, the higher downs itself
/// although it is the one which still runs.
fn keep_majority_scenario() -> anyhow::Result<()> {
    let mut nodes = majority_cluster(3)?;
    let frozen = nodes.pop().context("three nodes")?;
    signal(&frozen.process, "STOP")?;
    for node in &nodes {
        expect_line(&node.lines, &format!("{JOIN_PREFIX}survived"))
            .with_context(|| format!("node at {} in the majority", node.addr))?;
    }
    drop((nodes, frozen));

    let [lower, mut higher] = majority_pair()?;
    signal(&higher.process, "STOP")?;
    expect_line(&lower.lines, &format!("{JOIN_PREFIX}survived"))
        .context("the lower address survives the higher one")?;
    higher.process.0.kill().context("killing the frozen node")?;
    drop((lower, higher));

    let [mut lower, higher] = majority_pair()?;
    signal(&lower.process, "STOP")?;
    expect_line(&higher.lines, &format!("{JOIN_PREFIX}downed"))
        .context("the higher address gives way to the lower one")?;
    lower.process.0.kill().context("killing the frozen node")?;
    Ok(())
}

/// A [Role::Majority] node, its address and every line it prints.
struct MajorityNode {
    process: KillOnDrop,
    addr: SocketAddr,
    lines: mpsc::Receiver<String>,
}

/// A cluster of `size` majority nodes, ordered by address, once every one sees all of them Up.
fn majority_cluster(size: usize) -> anyhow::Result<Vec<MajorityNode>> {
    let mut addrs = reserved_addrs(size)?;
    addrs.sort();

    let mut nodes = Vec::with_capacity(size);
    for (index, addr) in addrs.iter().copied().enumerate() {
        let mut command = Command::new(env::current_exe()?);
        command
            .env(ROLE_ENV, Role::Majority.as_str())
            .env(ADDR_ENV, addr.to_string())
            .env(CLUSTER_SIZE_ENV, size.to_string())
            .stdout(Stdio::piped());
        if index > 0 {
            command.env(SEEDS_ENV, addrs[0].to_string());
        }
        let mut process = KillOnDrop(command.spawn().context("majority process")?);
        let lines = stdout_lines(&mut process)?;
        nodes.push(MajorityNode {
            process,
            addr,
            lines,
        });
    }
    for node in &nodes {
        expect_line(&node.lines, &format!("{JOIN_PREFIX}up"))
            .with_context(|| format!("node at {} sees every member Up", node.addr))?;
    }
    Ok(nodes)
}

fn majority_pair() -> anyhow::Result<[MajorityNode; 2]> {
    let mut nodes = majority_cluster(2)?;
    let higher = nodes.pop().context("two nodes")?;
    let lower = nodes.pop().context("two nodes")?;
    Ok([lower, higher])
}

/// A node which leaves announces it: every member lists it Left right away, well inside the
/// window in which failure detection plus downing could not have concluded anything. The
/// departure rides the control stream, the replies and the terminated signal a data stream, and
/// this node delivers what the leaver's connection still carries before the node death: every
/// one of [DEPARTURE_PINGS] replies arrives, in order, before the real terminated signal. The
/// leaving node checks its departure report itself and fails its exit otherwise.
fn leave_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let mut process = KillOnDrop(spawn_node_at(Role::Leave, None)?);
    let addr = bound_addr(&process)?;
    let echo = resolve_ref::<Request>(&mut process)?;

    let (event_tx, event_rx) = mpsc::channel();
    let stopped = Instant::now();
    let system = ActorSystem::new(DepartureWatch {
        subject: echo,
        event_tx,
    });
    match recv(&event_rx)? {
        TestEvent::Watching => {}
        _ => bail!("unexpected event from the departure watch client"),
    }

    while !cluster::members()
        .context("members")?
        .iter()
        .any(|member| member.addr() == addr && member.state() == MemberState::Left)
    {
        if stopped.elapsed() >= LEAVE_DEADLINE {
            bail!("the leaving member at {addr} was not listed Left within {LEAVE_DEADLINE:?}");
        }
        thread::sleep(CONVERGENCE_POLL);
    }

    expect_done(&event_rx)?;
    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context("departure watch client termination")??;
    expect_exit(&mut process, "leaving node")
}

/// A `SelfDown` verdict from the downing provider runs the node death sequence on the node which
/// decides it: the endpoint severs everything and refuses to rejoin, so the losing side of a
/// partition gives way instead of downing the side it cannot reach.
fn self_down_scenario() -> anyhow::Result<()> {
    let mut process = KillOnDrop(spawn_node(Role::SelfDown)?);

    expect_join_line(&mut process, "downed")?;
    expect_exit(&mut process, "self down")
}

/// Under mutual TLS a node's certificate must prove the address it advertises: a joiner whose
/// certificate carries its IP or its exact `tellus://` URI is admitted, one whose certificate
/// proves no address, or whose URI names another port of its IP, is dropped without an answer and
/// its join runs into its timeout. The nodes form their own cluster with their own certificate
/// authority; the dev-transport client is not part of it.
fn identity_binding_scenario() -> anyhow::Result<()> {
    let scratch = tempfile::tempdir().context("mutual TLS scratch directory")?;
    let dir = scratch.path();

    let ca_key = rcgen::KeyPair::generate().context("ca key")?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).context("ca parameters")?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).context("ca certificate")?;
    let ca_path = dir.join("ca.der");
    fs::write(&ca_path, ca_cert.der()).context("writing the ca certificate")?;

    let mint =
        |name: &str, sans: Vec<String>, uris: Vec<String>| -> anyhow::Result<(PathBuf, PathBuf)> {
            let key = rcgen::KeyPair::generate().context("node key")?;
            let mut params = rcgen::CertificateParams::new(sans).context("node parameters")?;
            for uri in uris {
                let uri = uri.try_into().context("node URI")?;
                params.subject_alt_names.push(rcgen::SanType::URI(uri));
            }
            let cert = params
                .signed_by(&key, &ca_cert, &ca_key)
                .context("node certificate")?;
            let cert_path = dir.join(format!("{name}.der"));
            let key_path = dir.join(format!("{name}.key"));
            fs::write(&cert_path, cert.der()).context("writing the node certificate")?;
            fs::write(&key_path, key.serialize_der()).context("writing the node key")?;
            Ok((cert_path, key_path))
        };

    let (seed_cert, seed_key) = mint(
        "seed",
        vec![MTLS_SERVER_NAME.to_string(), any_port().ip().to_string()],
        Vec::new(),
    )?;
    let mut seed_process = KillOnDrop(spawn_mutual_node(
        any_port(),
        &seed_cert,
        &seed_key,
        &ca_path,
        None,
    )?);
    let seed_addr = bound_addr(&seed_process)?;
    expect_join_line(&mut seed_process, "seed")?;

    let (good_cert, good_key) = mint(
        "good",
        vec![MTLS_SERVER_NAME.to_string(), any_port().ip().to_string()],
        Vec::new(),
    )?;
    let mut good_process = KillOnDrop(spawn_mutual_node(
        any_port(),
        &good_cert,
        &good_key,
        &ca_path,
        Some(seed_addr),
    )?);
    expect_join_line(&mut good_process, "ok")?;
    expect_exit(&mut good_process, "proven mutual TLS joiner")?;

    let mut uri_process = spawn_at_fresh_addr(|uri_addr| {
        let (uri_cert, uri_key) = mint(
            "uri",
            vec![MTLS_SERVER_NAME.to_string()],
            vec![format!("tellus://{uri_addr}")],
        )?;
        Ok(KillOnDrop(spawn_mutual_node(
            uri_addr,
            &uri_cert,
            &uri_key,
            &ca_path,
            Some(seed_addr),
        )?))
    })?;
    expect_join_line(&mut uri_process, "ok")?;
    expect_exit(&mut uri_process, "URI proven mutual TLS joiner")?;

    let mut port_process = spawn_at_fresh_addr(|port_addr| {
        let other_port = SocketAddr::new(port_addr.ip(), port_addr.port() ^ 1);
        let (port_cert, port_key) = mint(
            "port",
            vec![MTLS_SERVER_NAME.to_string(), port_addr.ip().to_string()],
            vec![format!("tellus://{other_port}")],
        )?;
        Ok(KillOnDrop(spawn_mutual_node(
            port_addr,
            &port_cert,
            &port_key,
            &ca_path,
            Some(seed_addr),
        )?))
    })?;
    expect_join_line(&mut port_process, "timeout")?;
    expect_exit(&mut port_process, "wrong port mutual TLS joiner")?;

    let (bad_cert, bad_key) = mint("bad", vec![MTLS_SERVER_NAME.to_string()], Vec::new())?;
    let mut bad_process = KillOnDrop(spawn_mutual_node(
        any_port(),
        &bad_cert,
        &bad_key,
        &ca_path,
        Some(seed_addr),
    )?);
    expect_join_line(&mut bad_process, "timeout")?;
    expect_exit(&mut bad_process, "unproven mutual TLS joiner")?;
    Ok(())
}

/// A node formed on its own is a cluster of its own, and the client's join towards it is refused
/// as foreign rather than merging the two. A node joining either cluster reports that cluster's
/// ID, so every member of one cluster names the same one.
fn foreign_cluster_scenario(runtime: &Runtime) -> anyhow::Result<()> {
    let this_cluster = cluster::cluster_state()?
        .borrow()
        .cluster()
        .context("the client names its cluster")?;
    let self_addr = *SELF_ADDR.get().context("endpoint not started")?;

    let mut foreign = KillOnDrop(spawn_cluster_id_node(any_port(), None)?);
    let foreign_addr = bound_addr(&foreign)?;
    let foreign_cluster =
        stdout_line(&mut foreign, CLUSTER_PREFIX).context("foreign cluster ID")?;
    anyhow::ensure!(
        foreign_cluster != this_cluster.to_string(),
        "an independently formed cluster has the client's ID {this_cluster}"
    );

    let mut foreign_member = KillOnDrop(spawn_cluster_id_node(any_port(), Some(foreign_addr))?);
    let joined_cluster =
        stdout_line(&mut foreign_member, CLUSTER_PREFIX).context("foreign member's cluster ID")?;
    anyhow::ensure!(
        joined_cluster == foreign_cluster,
        "a member of {foreign_cluster} names {joined_cluster}"
    );
    expect_exit(&mut foreign_member, "foreign member")?;

    let mut member = KillOnDrop(spawn_cluster_id_node(any_port(), Some(self_addr))?);
    let joined_cluster = stdout_line(&mut member, CLUSTER_PREFIX).context("member's cluster ID")?;
    anyhow::ensure!(
        joined_cluster == this_cluster.to_string(),
        "a member of {this_cluster} names {joined_cluster}"
    );
    expect_exit(&mut member, "member")?;

    let joined = runtime
        .block_on(timeout(TIMEOUT, cluster::join(&[foreign_addr])))
        .context("the join towards the foreign cluster ends within the timeout")?;
    anyhow::ensure!(
        matches!(joined, Err(JoinError::ForeignCluster)),
        "the join towards the foreign cluster ended as {joined:?}"
    );
    anyhow::ensure!(
        cluster::cluster_state()?.borrow().cluster() == Some(this_cluster),
        "the refused join changed the client's cluster"
    );
    Ok(())
}

/// Two nodes forming clusters of their own on one fixed seed list, as a split cold start leaves
/// them: each one's probe reports the other's address and cluster as foreign within a probe
/// interval of both being up, plus the time a probe takes.
fn foreign_probe_scenario() -> anyhow::Result<()> {
    let addrs = reserved_addrs(2)?;
    let (first_addr, second_addr) = (addrs[0], addrs[1]);
    let seeds = [first_addr, second_addr];

    let mut first = KillOnDrop(spawn_bootstrap_node(
        Role::ForeignProbe,
        first_addr,
        &seeds,
    )?);
    let mut second = KillOnDrop(spawn_bootstrap_node(
        Role::ForeignProbe,
        second_addr,
        &seeds,
    )?);
    let first_lines = stdout_lines(&mut first)?;
    let second_lines = stdout_lines(&mut second)?;

    let first_cluster = prefixed_line(&first_lines, CLUSTER_PREFIX)?;
    let second_cluster = prefixed_line(&second_lines, CLUSTER_PREFIX)?;
    anyhow::ensure!(
        first_cluster != second_cluster,
        "two independently formed clusters share the ID {first_cluster}"
    );
    let both_up = Instant::now();

    let first_foreign = prefixed_line(&first_lines, FOREIGN_PREFIX)?;
    let second_foreign = prefixed_line(&second_lines, FOREIGN_PREFIX)?;
    let elapsed = both_up.elapsed();
    anyhow::ensure!(
        first_foreign == format!("{second_addr} {second_cluster}"),
        "the first node reports {first_foreign} instead of the second's cluster"
    );
    anyhow::ensure!(
        second_foreign == format!("{first_addr} {first_cluster}"),
        "the second node reports {second_foreign} instead of the first's cluster"
    );
    anyhow::ensure!(
        elapsed <= PROBE_INTERVAL + CONNECT_TIMEOUT,
        "the foreign clusters were reported after {elapsed:?}"
    );
    Ok(())
}

struct EchoClient {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for EchoClient {
    type Message = Reply;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for seq in 0..PINGS {
            self.server.tell(Request::Ping {
                seq,
                reply_to: context.self_ref().clone(),
            });
        }
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        expected: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { seq }) = incoming else {
            return Ok(Control::Continue(expected));
        };

        if seq != expected {
            let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                "reply {seq} instead of {expected}"
            ))));
            return Ok(Control::Stop);
        }

        let next = expected + 1;
        if next == PINGS {
            let _ = self.event_tx.send(TestEvent::Done(Ok(())));
            self.server.tell(Request::Stop);
            Ok(Control::Stop)
        } else {
            Ok(Control::Continue(next))
        }
    }
}

/// Sends one `AskThenTell` and asserts the marker arrives before the reply.
struct AskThenTellClient {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for AskThenTellClient {
    type Message = AskerMessage;
    type State = bool;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.server.tell(Request::AskThenTell {
            marker_to: context.self_ref().clone(),
            reply_to: context.reply_to(AskerMessage::Answer),
        });
        Ok(false)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        marker_seen: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, marker_seen) {
            (Incoming::Message(AskerMessage::Marker), false) => Ok(Control::Continue(true)),

            (Incoming::Message(AskerMessage::Answer(_)), true) => {
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            (Incoming::Message(AskerMessage::Answer(_)), false) => {
                let _ = self
                    .event_tx
                    .send(TestEvent::Done(Err("reply before the marker".to_string())));
                Ok(Control::Stop)
            }

            (Incoming::Message(AskerMessage::Marker), true) => {
                let _ = self
                    .event_tx
                    .send(TestEvent::Done(Err("second marker".to_string())));
                Ok(Control::Stop)
            }

            (Incoming::Terminated(_), marker_seen) => Ok(Control::Continue(marker_seen)),
        }
    }
}

/// Answers a forwarded request and stops.
struct Responder;

impl Actor for Responder {
    type Message = ForwardedRequest;
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
        match incoming {
            Incoming::Message(ForwardedRequest { reply_to }) => {
                reply_to.reply(Reply { seq: FORWARD_SEQ });
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

/// Serializes its own `ReplyTo`, asserts a second serialization is refused, resolves the bytes on
/// its own node and replies through the resolved destination into its own mailbox.
struct SerdeRoundTrip {
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for SerdeRoundTrip {
    type Message = AskerMessage;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let fail = |message: String| {
            let _ = self.event_tx.send(TestEvent::Done(Err(message)));
            context.self_ref().tell(AskerMessage::Marker);
        };

        let reply_to = context.reply_to(AskerMessage::Answer);
        let bytes = match serde_json::to_vec(&reply_to) {
            Ok(bytes) => bytes,
            Err(error) => {
                fail(format!("serializing the reply destination: {error}"));
                return Ok(());
            }
        };

        if serde_json::to_vec(&reply_to).is_ok() {
            fail("a second serialization of the reply destination succeeded".to_string());
            return Ok(());
        }

        match serde_json::from_slice::<ReplyTo<Reply>>(&bytes) {
            Ok(reply_to) => reply_to.reply(Reply {
                seq: ROUND_TRIP_SEQ,
            }),

            Err(error) => fail(format!("resolving the reply destination bytes: {error}")),
        }
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(AskerMessage::Answer(Reply { seq })) => {
                let result = if seq == ROUND_TRIP_SEQ {
                    Ok(())
                } else {
                    Err(format!("reply {seq} instead of {ROUND_TRIP_SEQ}"))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            Incoming::Message(AskerMessage::Marker) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}

struct OrderedWatch {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for OrderedWatch {
    type Message = ClientEvent;
    type State = OrderedWatchState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(OrderedWatchState::AwaitingChild)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let fail = |message: String| {
            let _ = self.event_tx.send(TestEvent::Done(Err(message)));
            Ok(Control::Stop)
        };

        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), OrderedWatchState::AwaitingChild) => {
                context.watch(&child);
                child.tell(StreamerMessage::Go {
                    count: STREAMED,
                    reply_to: context.self_ref().clone(),
                });
                let child_id = child.actor_id();
                let _ = self.event_tx.send(TestEvent::Child(child));
                Ok(Control::Continue(OrderedWatchState::Streaming {
                    child_id,
                    next: 0,
                }))
            }

            (
                Incoming::Message(ClientEvent::Streamed(seq)),
                OrderedWatchState::Streaming { child_id, next },
            ) => {
                if seq != next {
                    return fail(format!("streamed {seq} instead of {next}"));
                }
                Ok(Control::Continue(OrderedWatchState::Streaming {
                    child_id,
                    next: next + 1,
                }))
            }

            (Incoming::Terminated(id), OrderedWatchState::Streaming { child_id, next }) => {
                if id != child_id {
                    return fail(format!("terminated signal for unexpected actor {id}"));
                }
                if next != STREAMED {
                    return fail(format!(
                        "terminated signal after {next} of {STREAMED} messages"
                    ));
                }
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            _ => fail("unexpected incoming".to_string()),
        }
    }
}

/// Spawns a streamer on the keeper's node, watches it, stops it and only reports its reference
/// once the terminated signal has arrived.
struct TerminateChild {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for TerminateChild {
    type Message = ClientEvent;
    type State = Option<ActorRef<StreamerMessage>>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                child.tell(StreamerMessage::Go {
                    count: 0,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(Some(child)))
            }

            Incoming::Terminated(id) => {
                let result = match &state {
                    Some(child) if child.actor_id() == id => Ok(()),
                    _ => Err(format!("terminated signal for unexpected actor {id}")),
                };

                if let Some(child) = state {
                    let _ = self.event_tx.send(TestEvent::Child(child));
                }
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

enum OrderedWatchState {
    AwaitingChild,
    Streaming { child_id: ActorId, next: u32 },
}

struct WatchTerminated {
    child: ActorRef<StreamerMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for WatchTerminated {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.child);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.child.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

struct UnwatchClient {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for UnwatchClient {
    type Message = ClientEvent;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        received: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                context.unwatch(&child);
                child.tell(StreamerMessage::Go {
                    count: STREAMED,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(received))
            }

            Incoming::Message(ClientEvent::Streamed(_)) => {
                let received = received + 1;
                if received == STREAMED {
                    let _ = self.event_tx.send(TestEvent::StreamedAll);
                }
                Ok(Control::Continue(received))
            }

            Incoming::Message(
                ClientEvent::Bulked(_) | ClientEvent::StreamedBulk { .. } | ClientEvent::Armed,
            ) => Ok(Control::Continue(received)),

            Incoming::Message(ClientEvent::Probe) => {
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            Incoming::Terminated(id) => {
                let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                    "terminated signal for {id} despite unwatch"
                ))));
                Ok(Control::Stop)
            }
        }
    }
}

/// Spawns a remote actor, has [WATCHERS] local actors watch it and stops it; it terminates only
/// once all of them have seen their signal.
struct TwoWatchers {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for TwoWatchers {
    type Message = ClientEvent;
    type State = usize;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        watching: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                for _ in 0..WATCHERS {
                    let watcher = context.spawn(ChildWatcher {
                        child: child.clone(),
                        event_tx: self.event_tx.clone(),
                    });
                    context.watch(&watcher);
                }

                child.tell(StreamerMessage::Go {
                    count: 0,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(WATCHERS))
            }

            Incoming::Terminated(_) => {
                let watching = watching - 1;
                if watching == 0 {
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(watching))
                }
            }

            _ => Ok(Control::Continue(watching)),
        }
    }
}

struct ChildWatcher {
    child: ActorRef<StreamerMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for ChildWatcher {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.child);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.child.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// Bulk messages to one of [BULK_TARGETS] remote actors, a tiny one to each of the others, all
/// told in that order: the first acknowledgement must come from one of the others.
struct HeadOfLine {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for HeadOfLine {
    type Message = ClientEvent;
    type State = HeadOfLineState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        for _ in 0..BULK_TARGETS {
            self.keeper.tell(KeeperMessage::Spawn {
                reply_to: context.self_ref().clone(),
            });
        }
        Ok(HeadOfLineState::AwaitingTargets(Vec::new()))
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (
                Incoming::Message(ClientEvent::Child(child)),
                HeadOfLineState::AwaitingTargets(mut targets),
            ) => {
                targets.push(child);
                if targets.len() < BULK_TARGETS {
                    return Ok(Control::Continue(HeadOfLineState::AwaitingTargets(targets)));
                }

                let payload = vec![0; BULK_PAYLOAD];
                for seq in 0..BULKS {
                    targets[0].tell(StreamerMessage::Bulk {
                        seq,
                        payload: payload.clone(),
                        reply_to: context.self_ref().clone(),
                    });
                }
                for (index, target) in targets.iter().enumerate().skip(1) {
                    let index = u32::try_from(index).expect("the target index fits");
                    target.tell(StreamerMessage::Bulk {
                        seq: BULKS + index,
                        payload: Vec::new(),
                        reply_to: context.self_ref().clone(),
                    });
                }

                Ok(Control::Continue(HeadOfLineState::Bulking {
                    acknowledged: 0,
                    ahead: 0,
                    overtaken: false,
                }))
            }

            (
                Incoming::Message(ClientEvent::Bulked(seq)),
                HeadOfLineState::Bulking {
                    acknowledged,
                    ahead,
                    overtaken,
                },
            ) => {
                let overtaken = overtaken || seq >= BULKS;
                let ahead = ahead + u32::from(!overtaken);

                let acknowledged = acknowledged + 1;
                if acknowledged < BULK_ACKNOWLEDGEMENTS {
                    return Ok(Control::Continue(HeadOfLineState::Bulking {
                        acknowledged,
                        ahead,
                        overtaken,
                    }));
                }

                let result = if ahead < BULKS {
                    Ok(())
                } else {
                    Err(format!(
                        "all {ahead} bulk messages were acknowledged before any other target"
                    ))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

/// `ahead` counts the bulk target's acknowledgements arriving before any other target's, which is
/// all of them when one lane carries everything and the bulk messages were told first.
enum HeadOfLineState {
    AwaitingTargets(Vec<ActorRef<StreamerMessage>>),

    Bulking {
        acknowledged: u32,
        ahead: u32,
        overtaken: bool,
    },
}

/// Tells the streamer a bulk beyond `max_frame_size` and an empty one right behind it, expecting
/// only the empty one to be acknowledged.
struct Oversize {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for Oversize {
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                child.tell(StreamerMessage::Bulk {
                    seq: 0,
                    payload: vec![0; OVERSIZE_PAYLOAD],
                    reply_to: context.self_ref().clone(),
                });
                child.tell(StreamerMessage::Bulk {
                    seq: 1,
                    payload: Vec::new(),
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(state))
            }

            Incoming::Message(ClientEvent::Bulked(seq)) => {
                let result = if seq == 1 {
                    Ok(())
                } else {
                    Err(format!("oversize bulk {seq} was acknowledged"))
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

/// Keeps `window` requests outstanding towards the echo server until `total` replies arrived; a
/// window of one measures serial latency, a large one pipelined throughput.
struct BenchEcho {
    server: ActorRef<Request>,
    request: BenchRequest,
    total: u32,
    window: u32,
    event_tx: mpsc::Sender<TestEvent>,
}

impl BenchEcho {
    fn request(&self, context: &ActorContext<Reply>, seq: u32) -> Request {
        match self.request {
            BenchRequest::Ping => Request::Ping {
                seq,
                reply_to: context.self_ref().clone(),
            },

            BenchRequest::Ask => Request::Ask {
                seq,
                reply_to: context.reply_to(|reply| reply),
            },
        }
    }
}

impl Actor for BenchEcho {
    type Message = Reply;
    type State = BenchState;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let sent = self.window.min(self.total);
        for seq in 0..sent {
            self.server.tell(self.request(context, seq));
        }
        Ok(BenchState { sent, received: 0 })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { .. }) = incoming else {
            return Ok(Control::Continue(state));
        };

        state.received += 1;
        if state.sent < self.total {
            self.server.tell(self.request(context, state.sent));
            state.sent += 1;
        }

        if state.received == self.total {
            let _ = self.event_tx.send(TestEvent::Done(Ok(())));
            Ok(Control::Stop)
        } else {
            Ok(Control::Continue(state))
        }
    }
}

/// What [BenchEcho] sends: a ping carrying its reference, or an ask carrying a [ReplyTo] created
/// through [ActorContext::reply_to].
#[derive(Clone, Copy)]
enum BenchRequest {
    Ping,
    Ask,
}

struct BenchState {
    sent: u32,
    received: u32,
}

/// Keeps [BENCH_BULK_WINDOW] bulks of [BENCH_BULK_PAYLOAD] bytes outstanding towards a streamer
/// until [BENCH_BULKS] acknowledgements arrived.
struct BenchBulk {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for BenchBulk {
    type Message = ClientEvent;
    type State = Option<BenchBulkState>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), None) => {
                let sent = BENCH_BULK_WINDOW.min(BENCH_BULKS);
                for seq in 0..sent {
                    child.tell(StreamerMessage::Bulk {
                        seq,
                        payload: vec![0; BENCH_BULK_PAYLOAD],
                        reply_to: context.self_ref().clone(),
                    });
                }
                Ok(Control::Continue(Some(BenchBulkState {
                    child,
                    counts: BenchState { sent, received: 0 },
                })))
            }

            (Incoming::Message(ClientEvent::Bulked(_)), Some(mut state)) => {
                state.counts.received += 1;
                if state.counts.sent < BENCH_BULKS {
                    state.child.tell(StreamerMessage::Bulk {
                        seq: state.counts.sent,
                        payload: vec![0; BENCH_BULK_PAYLOAD],
                        reply_to: context.self_ref().clone(),
                    });
                    state.counts.sent += 1;
                }

                if state.counts.received == BENCH_BULKS {
                    let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                    Ok(Control::Stop)
                } else {
                    Ok(Control::Continue(Some(state)))
                }
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

struct BenchBulkState {
    child: ActorRef<StreamerMessage>,
    counts: BenchState,
}

/// Fires a burst of bulks at the streamer and validates every acknowledgement arrives strictly
/// ascending; a `Probe` tells one further marker bulk, whose acknowledgement completes the run.
struct SeverFifo {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for SeverFifo {
    type Message = ClientEvent;
    type State = Option<SeverFifoState>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), None) => {
                let payload = vec![0; SEVER_PAYLOAD];
                for seq in 0..SEVER_BULKS {
                    child.tell(StreamerMessage::Bulk {
                        seq,
                        payload: payload.clone(),
                        reply_to: context.self_ref().clone(),
                    });
                }
                let _ = self.event_tx.send(TestEvent::Bulking);
                Ok(Control::Continue(Some(SeverFifoState {
                    child,
                    last: None,
                    next_marker: SEVER_BULKS,
                })))
            }

            (Incoming::Message(ClientEvent::Probe), Some(mut state)) => {
                state.child.tell(StreamerMessage::Bulk {
                    seq: state.next_marker,
                    payload: Vec::new(),
                    reply_to: context.self_ref().clone(),
                });
                state.next_marker += 1;
                Ok(Control::Continue(Some(state)))
            }

            (Incoming::Message(ClientEvent::Bulked(seq)), Some(mut state)) => {
                if state.last.is_some_and(|last| seq <= last) {
                    let _ = self.event_tx.send(TestEvent::Done(Err(format!(
                        "acknowledgement {seq} after {:?}",
                        state.last
                    ))));
                    return Ok(Control::Stop);
                }
                if seq >= SEVER_BULKS {
                    let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                    return Ok(Control::Stop);
                }
                state.last = Some(seq);
                Ok(Control::Continue(Some(state)))
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

struct SeverFifoState {
    child: ActorRef<StreamerMessage>,
    last: Option<u32>,
    next_marker: u32,
}

/// Watches the streamer while it streams bulky messages and stops: they must arrive strictly
/// ascending, possibly with gaps, and none after the terminated signal, which a `Probe` told after
/// a grace period confirms.
struct SeverTerminated {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for SeverTerminated {
    type Message = ClientEvent;
    type State = Option<SeverTerminatedState>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let fail = |message: String| {
            let _ = self.event_tx.send(TestEvent::Done(Err(message)));
            Ok(Control::Stop)
        };

        match (incoming, state) {
            (Incoming::Message(ClientEvent::Child(child)), None) => {
                context.watch(&child);
                child.tell(StreamerMessage::GoBulk {
                    count: SEVER_BULKS,
                    size: SEVER_PAYLOAD,
                    reply_to: context.self_ref().clone(),
                });
                let _ = self.event_tx.send(TestEvent::Bulking);
                Ok(Control::Continue(Some(SeverTerminatedState {
                    child_id: child.actor_id(),
                    last: None,
                    terminated: false,
                })))
            }

            (Incoming::Message(ClientEvent::StreamedBulk { seq, .. }), Some(mut state)) => {
                if state.terminated {
                    return fail(format!("message {seq} after the terminated signal"));
                }
                if state.last.is_some_and(|last| seq <= last) {
                    return fail(format!("message {seq} after {:?}", state.last));
                }
                state.last = Some(seq);
                Ok(Control::Continue(Some(state)))
            }

            (Incoming::Terminated(id), Some(mut state)) if id == state.child_id => {
                state.terminated = true;
                let _ = self.event_tx.send(TestEvent::StreamedAll);
                Ok(Control::Continue(Some(state)))
            }

            (Incoming::Message(ClientEvent::Probe), Some(state)) if state.terminated => {
                let _ = self.event_tx.send(TestEvent::Done(Ok(())));
                Ok(Control::Stop)
            }

            (_, state) => Ok(Control::Continue(state)),
        }
    }
}

struct SeverTerminatedState {
    child_id: ActorId,
    last: Option<u32>,
    terminated: bool,
}

/// Watches the streamer, arms the keeper's endpoint to drop the next terminated frame and only
/// then stops the streamer: the signal must arrive nonetheless, through the watch refresh.
struct LostTerminated {
    keeper: ActorRef<KeeperMessage>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for LostTerminated {
    type Message = ClientEvent;
    type State = Option<ActorRef<StreamerMessage>>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.keeper.tell(KeeperMessage::Spawn {
            reply_to: context.self_ref().clone(),
        });
        Ok(None)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ClientEvent::Child(child)) => {
                context.watch(&child);
                self.keeper.tell(KeeperMessage::DropTerminated {
                    count: 1,
                    reply_to: context.self_ref().clone(),
                });
                Ok(Control::Continue(Some(child)))
            }

            Incoming::Message(ClientEvent::Armed) => {
                if let Some(child) = &state {
                    child.tell(StreamerMessage::Go {
                        count: 0,
                        reply_to: context.self_ref().clone(),
                    });
                }
                Ok(Control::Continue(state))
            }

            Incoming::Terminated(id) => {
                let result = match &state {
                    Some(child) if child.actor_id() == id => Ok(()),
                    _ => Err(format!("terminated signal for unexpected actor {id}")),
                };
                let _ = self.event_tx.send(TestEvent::Done(result));
                Ok(Control::Stop)
            }

            _ => Ok(Control::Continue(state)),
        }
    }
}

/// Watches its subject, reports that it is watching and reports the terminated signal, whether
/// real or synthesized.
struct DeathWatch<N> {
    subject: ActorRef<N>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl<N> Actor for DeathWatch<N>
where
    N: Send + 'static,
{
    type Message = ClientEvent;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.subject);
        let _ = self.event_tx.send(TestEvent::Watching);
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Terminated(id) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if id == self.subject.actor_id() {
            Ok(())
        } else {
            Err(format!("terminated signal for unexpected actor {id}"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// Watches its subject, pings it [DEPARTURE_PINGS] times and stops it, then reports whether every
/// reply arrived, in order, before the terminated signal.
struct DepartureWatch {
    subject: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for DepartureWatch {
    type Message = Reply;
    type State = u32;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.watch(&self.subject);
        for seq in 0..DEPARTURE_PINGS {
            self.subject.tell(Request::Ping {
                seq,
                reply_to: context.self_ref().clone(),
            });
        }
        self.subject.tell(Request::Stop);
        let _ = self.event_tx.send(TestEvent::Watching);
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        received: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let result = match incoming {
            Incoming::Message(Reply { seq }) if seq == received => {
                return Ok(Control::Continue(received + 1));
            }

            Incoming::Message(Reply { seq }) => {
                Err(format!("reply {seq} arrived as reply number {received}"))
            }

            Incoming::Terminated(id) if id != self.subject.actor_id() => {
                Err(format!("terminated signal for unexpected actor {id}"))
            }

            Incoming::Terminated(_) if received != DEPARTURE_PINGS => Err(format!(
                "terminated signal after {received} of {DEPARTURE_PINGS} replies"
            )),

            Incoming::Terminated(_) => Ok(()),
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// One ping, one reply: proves the lane round trips and leaves the server running.
struct PingOnce {
    server: ActorRef<Request>,
    event_tx: mpsc::Sender<TestEvent>,
}

impl Actor for PingOnce {
    type Message = Reply;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.server.tell(Request::Ping {
            seq: 0,
            reply_to: context.self_ref().clone(),
        });
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Reply { seq }) = incoming else {
            return Ok(Control::Continue(state));
        };

        let result = if seq == 0 {
            Ok(())
        } else {
            Err(format!("reply {seq} instead of 0"))
        };
        let _ = self.event_tx.send(TestEvent::Done(result));
        Ok(Control::Stop)
    }
}

/// A lookup loop: the directory answers empty while the node there has not joined
/// yet, or has not registered yet, or its registrations have not been replicated here yet.
fn lookup_echo_with_retries(
    runtime: &Runtime,
    addr: SocketAddr,
) -> anyhow::Result<ActorRef<Request>> {
    for _ in 0..LOOKUP_ATTEMPTS {
        let key = Key::<Request>::new(ECHO_KEY);
        if let Some(reference) = receptionist::lookup_at(&key, addr)?.into_iter().next() {
            return Ok(reference);
        }
        runtime.block_on(sleep(LOOKUP_DELAY));
    }

    Err(anyhow::Error::msg(
        "no reference within the lookup attempts",
    ))
}

/// Run a client actor to completion and hand back the events it reported.
fn run_client<A, F>(runtime: &Runtime, what: &'static str, actor: F) -> anyhow::Result<Receiver>
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
    F: FnOnce(mpsc::Sender<TestEvent>) -> A,
{
    let (event_tx, event_rx) = mpsc::channel();
    let system = ActorSystem::new(actor(event_tx));

    runtime
        .block_on(timeout(TIMEOUT, system.terminated()))
        .context(what)??;
    Ok(event_rx)
}

/// Start this process's remoting endpoint, on the address named by [ADDR_ENV] or otherwise an OS
/// chosen port, and join the cluster through the seeds named by [SEEDS_ENV], if any, or by
/// bootstrapping from the address list named by [BOOTSTRAP_ENV]. It must run inside a Tokio
/// runtime.
async fn start_endpoint() -> anyhow::Result<SocketAddr> {
    let bind_addr = env::var(ADDR_ENV).unwrap_or_else(|_| "127.0.0.1:0".to_string());
    let transport = QuicTransport::dev(bind_addr.parse()?)?;
    let addr = transport.local_addr()?;
    let mut config = test_config(addr);
    if let Ok(streams) = env::var(STREAMS_ENV) {
        config.max_streams_per_peer = streams.parse().context("stream count")?;
    }
    cluster::start_endpoint(config, TimeoutTransport(transport))?;
    let _ = SELF_ADDR.set(addr);
    announce_bound(addr);

    if let Ok(seeds) = env::var(SEEDS_ENV) {
        timeout(TIMEOUT, cluster::join(&parse_addrs(&seeds)?))
            .await
            .context("joining the cluster")??;
    } else if let Ok(seeds) = env::var(BOOTSTRAP_ENV) {
        bootstrap_cluster(parse_addrs(&seeds)?).await?;
    } else {
        // A started endpoint is no cluster: the client is the seed the spawned nodes join, so it
        // is the one node here which forms rather than joins.
        cluster::form().context("forming the cluster")?;
    }
    Ok(addr)
}

/// Deterministic failure detection for the scenarios: a fixed deadline instead of the adaptive
/// default, unilateral downing on a short deadline instead of the keep majority default, and a
/// short retention so downed members do not pile up across scenarios. The keep majority default's
/// even split tie break would self down this process whenever the killed node holds the lower
/// address.
fn test_config(addr: SocketAddr) -> EndpointConfig {
    let deadline = Deadline::new(Duration::from_secs(3)).expect("3s is not zero");

    let mut config = EndpointConfig::new(addr);
    config.failure_detector = Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
    config.downing_provider =
        Arc::new(|| Box::new(GatedDowning(DownAfterDeadline::new(Duration::from_secs(2)))));
    config.down_retention = Duration::from_secs(5);
    config
}

fn parse_addrs(addrs: &str) -> anyhow::Result<Vec<SocketAddr>> {
    addrs
        .split(',')
        .map(|addr| addr.parse().context("address list"))
        .collect()
}

/// Bootstrap into the cluster the given addresses form; every node of a bootstrap scenario runs
/// the identical list, its own address included, with a settle window short enough for the
/// scenario timeouts. The probe stays off, since it would keep dialing the scenario's addresses
/// long after its nodes exited.
async fn bootstrap_cluster(seeds: Vec<SocketAddr>) -> anyhow::Result<()> {
    let config = BootstrapConfig {
        min_peers: NonZeroUsize::new(seeds.len()).expect("the seed list is not empty"),
        settle: Duration::from_millis(500),
        resolve_interval: Duration::from_millis(100),
        probe_interval: None,
        ..BootstrapConfig::new()
    };
    timeout(TIMEOUT, cluster::bootstrap(FixedSeeds::new(seeds), config))
        .await
        .context("bootstrapping the cluster")??;
    Ok(())
}

/// QUIC abandons a dial towards a silent address only after its 30 s handshake timeout, which
/// would stretch a lane's give-up over minutes; every scenario waiting on failed dials needs
/// each attempt bounded instead.
struct TimeoutTransport(QuicTransport);

impl Transport for TimeoutTransport {
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
            Err(_) => Err(TransportError::other("connect timed out")),
        }
    }

    async fn accept(&self, max_frame_size: usize) -> Result<QuicConnection, TransportError> {
        self.0.accept(max_frame_size).await
    }
}

/// An address the OS has just handed out and nothing holds anymore, so the node about to be
/// spawned can bind it: naming a node before it exists is what a lookup has to survive. Another
/// process can take it before the node binds it, which [BindCollision] reports.
fn reserved_addr() -> anyhow::Result<SocketAddr> {
    Ok(reserved_addrs(1)?.remove(0))
}

/// Distinct addresses: every socket is held until the last address is read, else one port could
/// be handed out twice.
fn reserved_addrs(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let sockets = (0..count)
        .map(|_| UdpSocket::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()?;
    sockets
        .iter()
        .map(|socket| socket.local_addr().map_err(anyhow::Error::from))
        .collect()
}

/// The address a node binds to when the OS is to choose its port, which it then reports.
fn any_port() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// The address a spawned node reported once it was bound.
fn bound_addr(child: &Child) -> anyhow::Result<SocketAddr> {
    lock(&NODE_ADDRS)
        .get(&child.id())
        .copied()
        .context("the node reported no bound address")
}

/// Waits for the node's address line. A node which closes its stdout and exits with
/// [BIND_COLLISION_EXIT] instead found its address taken.
fn await_bound(child: &mut Child, requested: SocketAddr) -> anyhow::Result<()> {
    match stdout_line(child, ADDR_PREFIX) {
        Ok(addr) => {
            let addr = addr.parse().context("bound address")?;
            lock(&NODE_ADDRS).insert(child.id(), addr);
            Ok(())
        }

        Err(error) => match wait_with_timeout(child, COLLISION_EXIT_TIMEOUT) {
            Ok(status) if status.code() == Some(BIND_COLLISION_EXIT) => {
                Err(BindCollision(requested).into())
            }
            _ => Err(error),
        },
    }
}

/// A spawned node found its address taken by another process before it could bind it.
#[derive(Debug)]
struct BindCollision(SocketAddr);

impl fmt::Display for BindCollision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} was taken before the node could bind it", self.0)
    }
}

impl std::error::Error for BindCollision {}

fn is_bind_collision(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<BindCollision>())
}

/// Binds the address to keep it from other processes while the node which owned it is dead.
fn hold_addr(addr: SocketAddr) -> anyhow::Result<UdpSocket> {
    UdpSocket::bind(addr).map_err(|_| BindCollision(addr).into())
}

/// Spawns a node at a freshly reserved address, and at another one if the address was taken
/// before the node bound it. Only valid for an address nothing else knows yet.
fn spawn_at_fresh_addr<T, F>(mut spawn: F) -> anyhow::Result<T>
where
    F: FnMut(SocketAddr) -> anyhow::Result<T>,
{
    let mut attempt = 1;
    loop {
        let addr = reserved_addr()?;
        force_collision("spawn", addr);
        match spawn(addr) {
            Err(error) if attempt < SPAWN_ATTEMPTS && is_bind_collision(&error) => {
                eprintln!("{error:#}, spawning at another address");
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Runs a scenario again from fresh reservations when one of its addresses was taken, the only
/// valid retry once that address was shared with other nodes or must survive a restart.
fn retried<F>(name: &str, mut scenario: F) -> anyhow::Result<()>
where
    F: FnMut() -> anyhow::Result<()>,
{
    let mut attempt = 1;
    loop {
        match scenario() {
            Err(error) if attempt < SCENARIO_ATTEMPTS && is_bind_collision(&error) => {
                eprintln!("{name}: {error:#}, running it again");
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Takes the address once, at the site [FORCE_COLLISION_ENV] names, so a run proves that a
/// collision there is recovered from.
fn force_collision(site: &str, addr: SocketAddr) {
    if env::var(FORCE_COLLISION_ENV).is_ok_and(|forced| forced == site) {
        let mut forced = lock(&FORCED_COLLISION);
        if forced.is_none() {
            match UdpSocket::bind(addr) {
                Ok(socket) => {
                    eprintln!("forcing a collision at {site} on {addr}");
                    *forced = Some(socket);
                }
                Err(error) => eprintln!("cannot force a collision at {site} on {addr}: {error}"),
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn spawn_node(role: Role) -> anyhow::Result<Child> {
    spawn_node_at(role, None)
}

fn spawn_node_at(role: Role, bind_addr: Option<SocketAddr>) -> anyhow::Result<Child> {
    let seed = SELF_ADDR.get().context("endpoint not started")?;
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, role.as_str())
        .env(SEEDS_ENV, seed.to_string())
        .stdout(Stdio::piped());
    if let Some(bind_addr) = bind_addr {
        command.env(ADDR_ENV, bind_addr.to_string());
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("{} process", role.as_str()))?;
    await_bound(&mut child, bind_addr.unwrap_or_else(any_port))?;
    Ok(child)
}

/// A node bootstrapping from the given address list instead of joining a configured seed.
fn spawn_bootstrap_node(
    role: Role,
    bind_addr: SocketAddr,
    seeds: &[SocketAddr],
) -> anyhow::Result<Child> {
    let seeds = seeds
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, role.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .env(BOOTSTRAP_ENV, seeds)
        .stdout(Stdio::piped());

    let mut child = command
        .spawn()
        .with_context(|| format!("bootstrap {} process", role.as_str()))?;
    await_bound(&mut child, bind_addr)?;
    Ok(child)
}

fn spawn_cluster_id_node(bind_addr: SocketAddr, seed: Option<SocketAddr>) -> anyhow::Result<Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, Role::ClusterId.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .stdout(Stdio::piped());
    if let Some(seed) = seed {
        command.env(SEEDS_ENV, seed.to_string());
    }

    let mut child = command.spawn().context("cluster ID process")?;
    await_bound(&mut child, bind_addr)?;
    Ok(child)
}

fn spawn_mutual_node(
    bind_addr: SocketAddr,
    cert: &Path,
    key: &Path,
    roots: &Path,
    seed: Option<SocketAddr>,
) -> anyhow::Result<Child> {
    let mut command = Command::new(env::current_exe()?);
    command
        .env(ROLE_ENV, Role::Mutual.as_str())
        .env(ADDR_ENV, bind_addr.to_string())
        .env(CERT_ENV, cert)
        .env(KEY_ENV, key)
        .env(ROOTS_ENV, roots)
        .stdout(Stdio::piped());
    if let Some(seed) = seed {
        command.env(SEEDS_ENV, seed.to_string());
    }

    let mut child = command.spawn().context("mutual TLS process")?;
    await_bound(&mut child, bind_addr)?;
    Ok(child)
}

fn resolve_ref<M>(child: &mut Child) -> anyhow::Result<ActorRef<M>>
where
    M: Manifest + Serialize + Send + 'static,
{
    let bytes = ref_bytes(child)?;
    cluster::deserialize_ref(&bytes).context("server reference")
}

fn ref_bytes(child: &mut Child) -> anyhow::Result<Vec<u8>> {
    let line = stdout_line(child, REF_PREFIX).context("server reference")?;
    hex_decode(&line)
}

fn expect_join_line(child: &mut Child, expected: &str) -> anyhow::Result<()> {
    let verdict = stdout_line(child, JOIN_PREFIX).context("join verdict")?;
    if verdict != expected {
        bail!("join verdict {verdict} instead of {expected}");
    }
    Ok(())
}

/// The bootstrap read must be bounded like every other wait in this file: a node which hangs
/// before printing the expected line must fail its scenario rather than hang the whole suite.
/// Lines before the expected one are skipped, later ones stay for the next read.
fn stdout_line(child: &mut Child, prefix: &'static str) -> anyhow::Result<String> {
    let lines = node_lines(child)?;
    let line = prefixed_line(&lines, prefix);
    lock(&NODE_LINES).insert(child.id(), lines);
    line
}

/// Every line of a node's stdout, for a node which reports more than once; [stdout_line] reads
/// one line at a time.
fn stdout_lines(child: &mut Child) -> anyhow::Result<mpsc::Receiver<String>> {
    node_lines(child)
}

/// The receiver of the node's lines, taken out of [NODE_LINES]. A child whose stdout is still
/// untaken is a new process, whatever an exited one with the same ID left behind.
fn node_lines(child: &mut Child) -> anyhow::Result<mpsc::Receiver<String>> {
    if let Some(stdout) = child.stdout.take() {
        let (line_tx, line_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line_tx.send(line).is_err() {
                    return;
                }
            }
        });
        return Ok(line_rx);
    }
    lock(&NODE_LINES)
        .remove(&child.id())
        .context("server process stdout")
}

/// Skips lines until the expected one, bounded like every other wait in this file.
fn expect_line(lines: &mpsc::Receiver<String>, expected: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(remaining)
            .with_context(|| format!("line {expected:?} within the timeout"))?;
        if line == expected {
            return Ok(());
        }
    }
}

/// Skips lines until one with the given prefix and answers the rest of it, bounded like every
/// other wait in this file.
fn prefixed_line(lines: &mpsc::Receiver<String>, prefix: &str) -> anyhow::Result<String> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(remaining)
            .with_context(|| format!("line {prefix:?} within the timeout"))?;
        if let Some(rest) = line.strip_prefix(prefix) {
            return Ok(rest.to_string());
        }
    }
}

/// Freezes (`STOP`) or resumes (`CONT`) a node, which to its peers looks like a partition: it
/// neither answers nor learns anything until resumed.
fn signal(child: &Child, signal: &str) -> anyhow::Result<()> {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(child.id().to_string())
        .status()
        .context("running kill")?;
    if !status.success() {
        bail!("kill -{signal} exited with {status}");
    }
    Ok(())
}

fn recv(event_rx: &mpsc::Receiver<TestEvent>) -> anyhow::Result<TestEvent> {
    event_rx.recv_timeout(TIMEOUT).context("test event")
}

fn expect_done(event_rx: &mpsc::Receiver<TestEvent>) -> anyhow::Result<()> {
    match recv(event_rx)? {
        TestEvent::Done(Ok(())) => Ok(()),
        TestEvent::Done(Err(message)) => bail!(message),
        _ => bail!("unexpected event instead of done"),
    }
}

/// Waiting without checking the exit status is what this helper makes impossible: a node which
/// died reporting an error must fail its scenario.
fn expect_exit(child: &mut Child, what: &str) -> anyhow::Result<()> {
    let status = wait_with_timeout(child, TIMEOUT)?;
    if !status.success() {
        bail!("{what} process exited with {status}");
    }
    Ok(())
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> anyhow::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            bail!("server process still running after {timeout:?}");
        }
        thread::sleep(EXIT_POLL_INTERVAL);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(hex: &str) -> anyhow::Result<Vec<u8>> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        bail!("odd length hex encoded reference");
    }

    pairs
        .iter()
        .map(|pair| {
            let pair = str::from_utf8(pair).context("hex encoded reference")?;
            u8::from_str_radix(pair, 16).context("hex encoded reference")
        })
        .collect()
}

#[derive(Deref, DerefMut)]
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
