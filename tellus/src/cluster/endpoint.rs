#[cfg(feature = "serde")]
use crate::cluster::{
    codec::CodecConfig, downing::DowningProviderConfig, failure::FailureDetectorConfig,
};
use crate::{
    ActorId, ActorRef, Backoff,
    actor_context::PanicPayload,
    cluster::{
        bootstrap::BootstrapConfig,
        codec::{Codec, Postcard},
        discovery::WireKey,
        downing::{Disconnected, Downing, DowningProvider, DowningProviderFactory, KeepMajority},
        failure::{
            self, FailureDetectorFactory, Liveness, PeerLiveness, PhiAccrualFailureDetector,
        },
        frame::{Frame, Handshake, RefusalReason},
        lanes::{Drain, Generation, LaneError, Lanes},
        leave::{Departure, LeaveError},
        membership::{self, DownOutcome, Member, MemberState, Membership},
        node::{ClusterId, NodeId},
        peer::{
            ConnectError, DialRequest, JoinRequest, ProbeRequest, accept_loop, dial_loop,
            join_loop, probe_loop,
        },
        reachability::{self, Reachability, WireReachability},
        receptionist::{self, PublishedChange, Receptionist, Snapshot},
        registry::{LimitExceeded, Registered, Registry},
        reply::{self, PendingReplies},
        schema::{AppVersion, Peer, Schema},
        state::{Changes, ClusterState, ForeignCluster},
        transport::Transport,
        watch::{WatcherTable, WireWatchTable},
    },
    sync::{lock, read, write},
};
use derive_more::Debug;
use flume::Sender;
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    ops::ControlFlow,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    sync::{broadcast, oneshot, watch},
    task::{self, JoinHandle},
    time::{self as tokio_time, MissedTickBehavior, interval, timeout_at},
};
use tracing::{debug, error, info, warn};

/// Above this tokio's broadcast channel panics rather than rounding up.
const MAX_CHANGE_CAPACITY: usize = usize::MAX / 2;

const REFUSAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_REFUSAL_BACKOFF: Duration = Duration::from_secs(10 * 60);

const FOREIGN_CLUSTER_REPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);

const UNREACHABLE_WARNING_INTERVAL: Duration = Duration::from_secs(60);

const FULL_GOSSIP_EVERY: u64 = 10;

static ENDPOINT: OnceLock<EndpointInner> = OnceLock::new();

thread_local! {
    static UPDATING: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    static CURRENT_FOR_TESTS: Cell<Option<&'static EndpointInner>> = const { Cell::new(None) };
}

/// Configuration for [start_endpoint], deserializable with the `serde` feature: the advertised
/// address is required, every other field falls back to its value in [EndpointConfig::new], and
/// the pluggable families are chosen by name, see docs/cluster.md.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedEndpointConfig")
)]
pub struct EndpointConfig {
    /// The address other nodes reach this node at; together with a per-process incarnation it is
    /// the node's identity carried inside serialized references. [start_endpoint] refuses an
    /// unspecified IP and a zero port, since other nodes cannot dial either.
    pub advertised_addr: SocketAddr,

    /// The capacity of the outbound queue per peer node; messages told to a full queue are
    /// dropped as dead letters. System frames like terminated signals bypass the capacity, so a
    /// full queue never drops one, but frames in flight can still be lost with the connection.
    /// The capacity covers the whole peer, however many streams carry it. Defaults to
    /// [EndpointConfig::DEFAULT_OUTBOUND_CAPACITY].
    pub outbound_capacity: NonZeroUsize,

    /// The upper bound on the data streams opened per peer connection, which frames are spread
    /// over by the actor they are delivered to, so a large message only delays frames toward
    /// targets hashing onto the same stream. A transport without streams lowers this to zero data
    /// streams, so every frame rides the control stream. `1` keeps one data stream for every
    /// frame delivered to an actor, beside the control stream. A transport with fewer streams
    /// than this lowers it to what it reports, e.g.
    /// [QuicTransport::DATA_STREAMS](crate::cluster::transport::QuicTransport::DATA_STREAMS).
    ///
    /// All of them are opened at connection setup, so a peer admitting fewer concurrent streams
    /// fails the connection right there instead of stalling a stream later.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_MAX_STREAMS_PER_PEER].
    pub max_streams_per_peer: NonZeroUsize,

    /// The maximum size of an encoded frame in bytes. An outbound frame beyond it becomes a local
    /// dead letter before reaching the transport, and an inbound one is refused. [start_endpoint]
    /// refuses a value too small to hold one member or one reachability record of a snapshot
    /// chunk. Defaults to [EndpointConfig::DEFAULT_MAX_FRAME_SIZE].
    pub max_frame_size: NonZeroUsize,

    /// The codec encoding and decoding message payloads, the same on every node of a cluster:
    /// its [name](Codec::name) is carried in every handshake, and a peer naming another one is
    /// refused. Defaults to [Postcard]; from a config file one of the provided codecs is chosen
    /// by name, see docs/cluster.md. [start_endpoint] refuses a name longer than 64 bytes.
    #[debug(skip)]
    pub codec: Arc<dyn Codec>,

    /// The version of the application this node runs, announced to every peer at the handshake
    /// and handed to [Schema::encode](crate::cluster::Schema::encode) on the other side as the
    /// peer's [app_version](crate::cluster::Peer::app_version), which is what lets a newer
    /// application encode the older shape of a message for it. From a config file a semantic
    /// version string, e.g. `app_version: "1.4.0"`. `None`, the default, announces nothing, which
    /// a peer sees as no information. [start_endpoint] refuses a version rendering longer than
    /// 64 bytes.
    pub app_version: Option<AppVersion>,

    /// The interval at which every Up member is sent a gossip frame: the member list, or an empty
    /// frame to a member already sent the current list, except on every 10th tick. Every gossip
    /// frame doubles as a heartbeat, and the tick also evaluates failure detection and polls the
    /// downing provider. Must not be zero; defaults to
    /// [EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL].
    pub heartbeat_interval: Duration,

    /// The interval at which this node's remote watches are re-asserted with idempotent `Watch`
    /// frames, healing a `Terminated` frame lost with the watched side's connection. Must stay
    /// below the failure detection deadline, else such a loss on a pair with no other watches is
    /// healed by a false node death instead of by the watched node's answer. The same interval
    /// re-offers this node's registrations to every Up peer whose lane missed a change.
    ///
    /// Must not be zero; defaults to [EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL].
    pub watch_refresh_interval: Duration,

    /// Creates the [FailureDetector] for a member, deciding when it is locally unreachable; the
    /// default is a [PhiAccrualFailureDetector] with its defaults. [DeadlineFailureDetector]
    /// remains the deterministic choice, e.g. for tests. From a config file one of the provided
    /// detectors is chosen by name, carrying its own tuning, see docs/cluster.md.
    ///
    /// [start_endpoint] arms one detector and feeds it a burst of heartbeats with no time between
    /// them. It refuses the configuration unless the member is still available after the longer
    /// of `heartbeat_interval` and `watch_refresh_interval`. Every inbound frame is a heartbeat,
    /// so traffic teaches a detector the shortest intervals there are.
    ///
    /// [DeadlineFailureDetector]: crate::cluster::failure::DeadlineFailureDetector
    /// [FailureDetector]: crate::cluster::failure::FailureDetector
    /// [PhiAccrualFailureDetector]: crate::cluster::failure::PhiAccrualFailureDetector
    #[debug(skip)]
    pub failure_detector: FailureDetectorFactory,

    /// Creates the [DowningProvider] deciding when a member outside this node's connected
    /// reachability component is downed; the default is [KeepMajority] with its own default
    /// deadline, which resolves a partition toward one side, see docs/cluster.md. From a config
    /// file one of the provided providers is chosen by name, carrying its own deadline.
    ///
    /// [DowningProvider]: crate::cluster::downing::DowningProvider
    /// [KeepMajority]: crate::cluster::downing::KeepMajority
    #[debug(skip)]
    pub downing_provider: DowningProviderFactory,

    /// How long a detailed Left or Down member is listed after this node learned of it. Must
    /// comfortably exceed the gossip convergence time so the detailed entry normally reaches every
    /// current member. It is also the age past which a watermark above `down_watermarks` may be
    /// evicted and a parked reachability record is dropped. Defaults to
    /// [EndpointConfig::DEFAULT_DOWN_RETENTION].
    pub down_retention: Duration,

    /// The number of per-address Left or Down watermarks, kept and sent on connection setup, above
    /// which those learned longer than `down_retention` ago are evicted, the earliest learned
    /// first. Watermarks learned within the retention are kept above it, up to twice this number;
    /// beyond that the earliest learned are evicted regardless. Eviction bounds this endpoint's
    /// memory and its setup cost. It lets a zombie at an evicted address be admitted again once
    /// its detailed Down entry has been swept too. A joining node accepts at most twice its own
    /// number of watermarks from its seed, so no node may have a larger value than the nodes
    /// joining it; in practice the value is the same on every node. See docs/cluster.md.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_DOWN_WATERMARKS].
    pub down_watermarks: NonZeroUsize,

    /// How long [leave](fn@crate::cluster::leave) waits for every peer to acknowledge the drain
    /// of the announced departure before severing anyway, one deadline for the whole exchange.
    /// Bounds a shutting down process, so it is sized by the deployment's termination grace
    /// period rather than by the gossip cadence; an acknowledgment costs a round trip. It also
    /// bounds how long this node keeps delivering from the connection of a departed node, and
    /// may acknowledge its drain, before running its node death sequence. Defaults to
    /// [EndpointConfig::DEFAULT_LEAVE_TIMEOUT].
    pub leave_timeout: Duration,

    /// The bounds pacing reconnection of a lost connection: the first attempt waits the minimum,
    /// each further one doubles it up to the maximum. Defaults to
    /// [EndpointConfig::DEFAULT_RECONNECT_BACKOFF_MIN] and
    /// [EndpointConfig::DEFAULT_RECONNECT_BACKOFF_MAX].
    pub reconnect_backoff: Backoff,

    /// After this many failed connection attempts an address no Up member advertises is given up
    /// and its queued messages become dead letters; a member's address is retried until the
    /// downing provider settles its fate. Giving up is not final: a later message dials again.
    /// An address no Up member advertises whose answer retrying cannot change is refused
    /// instead. Such an address does not speak this protocol or shares no version with it,
    /// fails the identity or address check, belongs to another cluster or sends a frame beyond
    /// `max_frame_size`. Sends toward a refused address fail without dialing for 10 s,
    /// doubling with every further refusal up to 10 min. A refusal expired for 10 min is
    /// forgotten. An inbound handshake from the address, an admitted join from it or a
    /// gossiped Up member at it lifts the refusal at once.
    ///
    /// Defaults to [EndpointConfig::DEFAULT_MAX_CONNECT_ATTEMPTS].
    pub max_connect_attempts: NonZeroU32,

    /// How long a join attempt which ended without proof whether its seed admitted this node
    /// blocks [form] at most. Such an attempt sent its `Join` handshake and then failed short of
    /// a definitive refusal, e.g. by a timeout or a lost connection, so the seed may count this
    /// node already. Forming is refused with [FormError::JoinAmbiguous] until that address
    /// answers a join definitively, discovery drops it from the universe, or this bound passes.
    /// Joins through any seed continue meanwhile. Defaults to
    /// [EndpointConfig::DEFAULT_AMBIGUOUS_JOIN_BOUND].
    pub ambiguous_join_bound: Duration,

    /// How many changes a subscriber of [changes](crate::cluster::changes) may fall behind
    /// before it is told of a gap and handed the current state and verdict; a buffer, not a
    /// history, holding one cluster state per version. A power of two no larger than half the
    /// address space, since the channel rounds up to one. Defaults to
    /// [EndpointConfig::DEFAULT_CHANGE_CAPACITY].
    pub change_capacity: NonZeroUsize,
}

impl EndpointConfig {
    /// The `outbound_capacity` of [EndpointConfig::new].
    pub const DEFAULT_OUTBOUND_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(8_192).expect("8192 is not zero");

    /// The `max_streams_per_peer` of [EndpointConfig::new].
    pub const DEFAULT_MAX_STREAMS_PER_PEER: NonZeroUsize =
        NonZeroUsize::new(16).expect("16 is not zero");

    /// The `max_frame_size` of [EndpointConfig::new].
    pub const DEFAULT_MAX_FRAME_SIZE: NonZeroUsize =
        NonZeroUsize::new(1_024 * 1_024).expect("1 MiB is not zero");

    /// The `heartbeat_interval` of [EndpointConfig::new].
    pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

    /// The `watch_refresh_interval` of [EndpointConfig::new].
    pub const DEFAULT_WATCH_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

    /// The `down_retention` of [EndpointConfig::new].
    pub const DEFAULT_DOWN_RETENTION: Duration = Duration::from_secs(300);

    /// The `down_watermarks` of [EndpointConfig::new].
    pub const DEFAULT_DOWN_WATERMARKS: NonZeroUsize =
        NonZeroUsize::new(4_096).expect("4096 is not zero");

    /// The `leave_timeout` of [EndpointConfig::new].
    pub const DEFAULT_LEAVE_TIMEOUT: Duration = Duration::from_secs(3);

    /// The minimum of the `reconnect_backoff` of [EndpointConfig::new].
    pub const DEFAULT_RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(250);

    /// The maximum of the `reconnect_backoff` of [EndpointConfig::new].
    pub const DEFAULT_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(3);

    /// The `max_connect_attempts` of [EndpointConfig::new].
    pub const DEFAULT_MAX_CONNECT_ATTEMPTS: NonZeroU32 = NonZeroU32::new(8).expect("8 is not zero");

    /// The `ambiguous_join_bound` of [EndpointConfig::new].
    pub const DEFAULT_AMBIGUOUS_JOIN_BOUND: Duration = Duration::from_secs(30);

    /// The `change_capacity` of [EndpointConfig::new].
    pub const DEFAULT_CHANGE_CAPACITY: NonZeroUsize =
        NonZeroUsize::new(256).expect("256 is not zero");

    /// A configuration with the given advertised address, every other field taken from its
    /// `DEFAULT_*` constant: the [Postcard] codec, the [PhiAccrualFailureDetector] with its
    /// defaults and [KeepMajority] downing with its own.
    pub fn new(advertised_addr: SocketAddr) -> Self {
        Self {
            advertised_addr,
            outbound_capacity: Self::DEFAULT_OUTBOUND_CAPACITY,
            max_streams_per_peer: Self::DEFAULT_MAX_STREAMS_PER_PEER,
            max_frame_size: Self::DEFAULT_MAX_FRAME_SIZE,
            codec: Arc::new(Postcard),
            app_version: None,
            heartbeat_interval: Self::DEFAULT_HEARTBEAT_INTERVAL,
            watch_refresh_interval: Self::DEFAULT_WATCH_REFRESH_INTERVAL,
            failure_detector: Arc::new(|| Box::new(PhiAccrualFailureDetector::default())),
            downing_provider: Arc::new(|| Box::new(KeepMajority::default())),
            down_retention: Self::DEFAULT_DOWN_RETENTION,
            down_watermarks: Self::DEFAULT_DOWN_WATERMARKS,
            leave_timeout: Self::DEFAULT_LEAVE_TIMEOUT,
            reconnect_backoff: Backoff::new(
                Self::DEFAULT_RECONNECT_BACKOFF_MIN,
                Self::DEFAULT_RECONNECT_BACKOFF_MAX,
            )
            .expect("the bounds are valid"),
            max_connect_attempts: Self::DEFAULT_MAX_CONNECT_ATTEMPTS,
            ambiguous_join_bound: Self::DEFAULT_AMBIGUOUS_JOIN_BOUND,
            change_capacity: Self::DEFAULT_CHANGE_CAPACITY,
        }
    }
}

/// The [EndpointConfig] given to [start_endpoint] is invalid.
#[derive(Debug, Error)]
pub enum InvalidEndpointConfig {
    /// The configured `advertised_addr` has an unspecified IP, e.g. `0.0.0.0`, which other nodes
    /// cannot dial.
    #[error("advertised_addr {advertised_addr} has an unspecified IP")]
    UnspecifiedAdvertisedIp {
        /// The configured address.
        advertised_addr: SocketAddr,
    },

    /// The configured `advertised_addr` has a zero port, which other nodes cannot dial.
    #[error("advertised_addr {advertised_addr} has a zero port")]
    ZeroAdvertisedPort {
        /// The configured address.
        advertised_addr: SocketAddr,
    },

    /// The configured `max_frame_size` cannot hold the widest handshake or the smallest chunk of
    /// a member snapshot or of a reachability snapshot, so a connection setup could not send one
    /// without the receiver refusing it.
    #[error("max_frame_size of {max_frame_size} bytes is below the minimum of {min} bytes")]
    MaxFrameSizeTooSmall {
        /// The configured size.
        max_frame_size: usize,

        /// The smallest size a handshake and a snapshot chunk can respect.
        min: usize,
    },

    /// The configured codec's name is longer than a handshake carries.
    #[error("codec name of {len} bytes exceeds {max} bytes")]
    CodecNameTooLong {
        /// The length of the name in bytes.
        len: usize,

        /// The longest name a handshake carries.
        max: usize,
    },

    /// The configured `app_version` renders longer than a handshake carries.
    #[error("app_version of {len} bytes exceeds {max} bytes")]
    AppVersionTooLong {
        /// The length of the rendered version in bytes.
        len: usize,

        /// The longest version a handshake carries.
        max: usize,
    },

    /// The configured `heartbeat_interval` is zero.
    #[error("heartbeat_interval is zero")]
    ZeroHeartbeatInterval,

    /// The configured `watch_refresh_interval` is zero.
    #[error("watch_refresh_interval is zero")]
    ZeroWatchRefreshInterval,

    /// The configured `change_capacity` is not a power of two, or too large for the channel.
    #[error("change_capacity of {change_capacity} is not a power of two up to {max}")]
    ChangeCapacity {
        /// The configured capacity.
        change_capacity: usize,

        /// The largest capacity the channel accepts.
        max: usize,
    },

    /// The configured failure detector declares a member unavailable within the given silence
    /// after a burst of traffic, although the heartbeat or the watch refresh interval, whichever
    /// is longer, may leave a healthy member that silent.
    #[error("failure detector does not tolerate a silence of {silence:?} after a burst of traffic")]
    FailureDetectorTooEager {
        /// The longer of the heartbeat and the watch refresh interval.
        silence: Duration,
    },
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedEndpointConfig {
    advertised_addr: SocketAddr,

    #[serde(default)]
    outbound_capacity: Option<NonZeroUsize>,

    #[serde(default)]
    max_streams_per_peer: Option<NonZeroUsize>,

    #[serde(default)]
    max_frame_size: Option<NonZeroUsize>,

    #[serde(default)]
    codec: Option<CodecConfig>,

    #[serde(default)]
    app_version: Option<AppVersion>,

    #[serde(default, with = "humantime_serde")]
    heartbeat_interval: Option<Duration>,

    #[serde(default, with = "humantime_serde")]
    watch_refresh_interval: Option<Duration>,

    #[serde(default)]
    failure_detector: Option<FailureDetectorConfig>,

    #[serde(default)]
    downing_provider: Option<DowningProviderConfig>,

    #[serde(default, with = "humantime_serde")]
    down_retention: Option<Duration>,

    #[serde(default)]
    down_watermarks: Option<NonZeroUsize>,

    #[serde(default, with = "humantime_serde")]
    leave_timeout: Option<Duration>,

    #[serde(default)]
    reconnect_backoff: Option<Backoff>,

    #[serde(default)]
    max_connect_attempts: Option<NonZeroU32>,

    #[serde(default, with = "humantime_serde")]
    ambiguous_join_bound: Option<Duration>,

    #[serde(default)]
    change_capacity: Option<NonZeroUsize>,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedEndpointConfig> for EndpointConfig {
    type Error = InvalidEndpointConfig;

    fn try_from(unchecked: UncheckedEndpointConfig) -> Result<Self, Self::Error> {
        let defaults = Self::new(unchecked.advertised_addr);

        let config = Self {
            advertised_addr: unchecked.advertised_addr,
            outbound_capacity: unchecked
                .outbound_capacity
                .unwrap_or(defaults.outbound_capacity),
            max_streams_per_peer: unchecked
                .max_streams_per_peer
                .unwrap_or(defaults.max_streams_per_peer),
            max_frame_size: unchecked.max_frame_size.unwrap_or(defaults.max_frame_size),
            codec: unchecked.codec.map_or(defaults.codec, CodecConfig::codec),
            app_version: unchecked.app_version,
            heartbeat_interval: unchecked
                .heartbeat_interval
                .unwrap_or(defaults.heartbeat_interval),
            watch_refresh_interval: unchecked
                .watch_refresh_interval
                .unwrap_or(defaults.watch_refresh_interval),
            failure_detector: unchecked
                .failure_detector
                .map_or(defaults.failure_detector, FailureDetectorConfig::factory),
            downing_provider: unchecked
                .downing_provider
                .map_or(defaults.downing_provider, DowningProviderConfig::factory),
            down_retention: unchecked.down_retention.unwrap_or(defaults.down_retention),
            down_watermarks: unchecked
                .down_watermarks
                .unwrap_or(defaults.down_watermarks),
            leave_timeout: unchecked.leave_timeout.unwrap_or(defaults.leave_timeout),
            reconnect_backoff: unchecked
                .reconnect_backoff
                .unwrap_or(defaults.reconnect_backoff),
            max_connect_attempts: unchecked
                .max_connect_attempts
                .unwrap_or(defaults.max_connect_attempts),
            ambiguous_join_bound: unchecked
                .ambiguous_join_bound
                .unwrap_or(defaults.ambiguous_join_bound),
            change_capacity: unchecked
                .change_capacity
                .unwrap_or(defaults.change_capacity),
        };
        validate(&config)?;

        Ok(config)
    }
}

/// The remoting endpoint cannot be started.
#[derive(Debug, Error)]
pub enum StartError {
    /// The endpoint has already been started; there is one per process.
    #[error("remoting endpoint already started")]
    AlreadyStarted,

    /// The given configuration is invalid.
    #[error(transparent)]
    Config(#[from] InvalidEndpointConfig),
}

/// This node cannot form a cluster.
#[derive(Debug, Error)]
pub enum FormError {
    /// The remoting endpoint has not been started, see [start_endpoint].
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// This node is already a member of a cluster.
    #[error("already a member of a cluster")]
    AlreadyFormed,

    /// This node's incarnation has been downed or has left; only a restarted process, with a
    /// fresh incarnation, can be a member again.
    #[error("this node has been downed")]
    Downed,

    /// A join attempt is in flight, which may be making this node a member of another cluster;
    /// worth retrying once it has resolved.
    #[error("a join attempt is in flight")]
    JoinInFlight,

    /// A cluster at this address answered a join attempt and may already count this node, so
    /// nothing else may be formed until that attempt resolves; worth retrying.
    #[error("pinned to the cluster at {0}")]
    ClusterPinned(SocketAddr),

    /// A join attempt via this address sent its `Join` handshake and ended without proof whether
    /// the seed admitted this node, so nothing else may be formed until that address answers
    /// definitively, leaves the bootstrap universe or
    /// [ambiguous_join_bound](EndpointConfig::ambiguous_join_bound) passes; worth retrying.
    #[error("the join attempt via {0} may have admitted this node")]
    JoinAmbiguous(SocketAddr),
}

/// Start the process wide remoting endpoint: accept connections from the given transport and
/// dial peers on demand. The started endpoint is not a cluster yet, so nothing can join it:
/// [form] makes it a cluster of one and [join](crate::cluster::join) makes it a member of
/// another node's. References can only be serialized and deserialized after this.
///
/// # Errors
/// Fails if the endpoint has been started already (there is one per process), or if the
/// configuration is invalid.
///
/// # Panics
/// Panics if called outside of a Tokio runtime.
pub fn start_endpoint<T>(config: EndpointConfig, transport: T) -> Result<(), StartError>
where
    T: Transport,
{
    validate(&config)?;

    let (dial_request_tx, dial_request_rx) = flume::unbounded();
    let (join_request_tx, join_request_rx) = flume::unbounded();
    let (probe_request_tx, probe_request_rx) = flume::unbounded();
    let data_streams = transport.data_streams().map_or(0, |streams| {
        config.max_streams_per_peer.get().min(streams.get())
    });
    let inner = EndpointInner::new(
        config,
        data_streams,
        dial_request_tx,
        join_request_tx,
        probe_request_tx,
    );
    ENDPOINT
        .set(inner)
        .map_err(|_| StartError::AlreadyStarted)?;
    let endpoint = ENDPOINT.get().expect("endpoint was just set");

    let transport = Arc::new(transport);
    task::spawn(accept_loop(transport.clone(), endpoint));
    task::spawn(dial_loop(transport.clone(), dial_request_rx, endpoint));
    task::spawn(join_loop(transport.clone(), join_request_rx, endpoint));
    task::spawn(probe_loop(transport, probe_request_rx, endpoint));
    task::spawn(membership_loop(endpoint));
    task::spawn(watch_refresh_loop(endpoint));
    task::spawn(replication_loop(endpoint));
    Ok(())
}

/// Form a cluster of this node alone, for a deployment deciding formation itself rather than
/// leaving it to [bootstrap](fn@crate::cluster::bootstrap): a started endpoint is not a cluster, so
/// nothing can join this node until it has formed one or joined one. A refusal because of a join
/// attempt is transient and worth retrying.
///
/// # Errors
/// Fails if the endpoint is not started, if this node is already a member, or if it has been
/// downed or has left. Fails as well if a join attempt could be making it a member of another
/// cluster right now or may have made it one.
pub fn form() -> Result<(), FormError> {
    get().ok_or(FormError::EndpointNotStarted)?.form_cluster()
}

/// Where the endpoint stands between being started and being done, see
/// [lifecycle](crate::cluster::lifecycle).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum Lifecycle {
    /// Started, but a member of no cluster yet: joins are refused until this node forms one or
    /// joins one.
    #[default]
    Unformed,

    /// A member of a cluster, of one or of many.
    Formed,

    /// Downed or left, for good: only a restarted process rejoins.
    Downed,
}

impl Lifecycle {
    fn code(self) -> u8 {
        match self {
            Lifecycle::Unformed => 0,
            Lifecycle::Formed => 1,
            Lifecycle::Downed => 2,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            0 => Lifecycle::Unformed,
            1 => Lifecycle::Formed,
            _ => Lifecycle::Downed,
        }
    }
}

pub(crate) struct EndpointInner {
    node: NodeId,
    config: EndpointConfig,
    registry: Arc<Registry>,
    lanes: Lanes,
    inbound_readers: tokio::sync::Mutex<HashMap<SocketAddr, InboundReader>>,
    membership: Membership,
    update_lock: Mutex<()>,
    state_tx: watch::Sender<Arc<ClusterState>>,
    changes_tx: broadcast::Sender<PublishedChange>,
    refusals: Mutex<HashMap<SocketAddr, Refusal>>,
    lifecycle: CoordinatedLifecycle,
    leaving: AtomicBool,
    leave_tx: watch::Sender<bool>,
    departure: OnceLock<watch::Receiver<Option<Departed>>>,
    departures: Mutex<Departures>,
    liveness: Liveness,
    reachability: Reachability,
    watchers: WatcherTable,
    wire_watches: WireWatchTable,
    pending_replies: PendingReplies,
    receptionist: Receptionist,
    join_request_tx: Sender<JoinRequest>,
    probe_request_tx: Sender<ProbeRequest>,
    foreign_clusters: Mutex<ForeignClusters>,
    unreachable_warnings: Mutex<UnreachableWarnings>,
    sever_tx: watch::Sender<u64>,
    gossiped_versions: Mutex<HashMap<NodeId, u64>>,
    // A leaf lock: never held while user code runs!
    peers: RwLock<HashMap<NodeId, Arc<Peer>>>,
    gossip_ticks: AtomicU64,
    #[cfg(feature = "cluster-dev")]
    dropped_terminated: AtomicU64,
    #[cfg(feature = "cluster-dev")]
    gossip_bytes: AtomicU64,
    #[cfg(test)]
    departure_panics: AtomicBool,
}

impl EndpointInner {
    /// Leaked, and none of the loops is started: a test spawns the one it exercises.
    #[cfg(test)]
    pub(crate) fn for_tests(
        config: EndpointConfig,
    ) -> (
        &'static EndpointInner,
        flume::Receiver<DialRequest>,
        flume::Receiver<JoinRequest>,
    ) {
        Self::for_tests_with_data_streams(config, 0)
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with_data_streams(
        config: EndpointConfig,
        data_streams: usize,
    ) -> (
        &'static EndpointInner,
        flume::Receiver<DialRequest>,
        flume::Receiver<JoinRequest>,
    ) {
        let (endpoint, dial_request_rx, join_request_rx, _) =
            Self::for_tests_with_probes(config, data_streams);
        (endpoint, dial_request_rx, join_request_rx)
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with_probes(
        config: EndpointConfig,
        data_streams: usize,
    ) -> (
        &'static EndpointInner,
        flume::Receiver<DialRequest>,
        flume::Receiver<JoinRequest>,
        flume::Receiver<ProbeRequest>,
    ) {
        let (dial_request_tx, dial_request_rx) = flume::unbounded();
        let (join_request_tx, join_request_rx) = flume::unbounded();
        let (probe_request_tx, probe_request_rx) = flume::unbounded();
        let inner = Self::new(
            config,
            data_streams,
            dial_request_tx,
            join_request_tx,
            probe_request_tx,
        );
        (
            Box::leak(Box::new(inner)),
            dial_request_rx,
            join_request_rx,
            probe_request_rx,
        )
    }

    pub(crate) fn node(&self) -> NodeId {
        self.node
    }

    pub(crate) fn config(&self) -> &EndpointConfig {
        &self.config
    }

    pub(crate) fn codec(&self) -> &dyn Codec {
        self.config.codec.as_ref()
    }

    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub(crate) fn membership(&self) -> &Membership {
        &self.membership
    }

    pub(crate) fn state_rx(&self) -> watch::Receiver<Arc<ClusterState>> {
        self.state_tx.subscribe()
    }

    /// Subscribed before the snapshot is taken, so nothing published in between is missed.
    pub(crate) fn changes(&'static self) -> Changes {
        Changes::new(self.changes_tx.subscribe(), &self.receptionist)
    }

    #[cfg(test)]
    pub(crate) fn published_changes(&self) -> broadcast::Receiver<PublishedChange> {
        self.changes_tx.subscribe()
    }

    /// Lock order: `update_lock`, then `Membership::entries`, then `Reachability::state`, then
    /// `foreign_clusters`, then the watch write, then, with all of those but `update_lock`
    /// released, the receptionist's replicas for the channel send. The closure must not call back
    /// into `update`, and a caller must not hold a borrow of the watch.
    pub(crate) fn update<R>(
        &self,
        mutate: impl FnOnce(&Membership, &Reachability) -> R,
    ) -> Updated<R> {
        let _reentrancy = UpdateGuard::enter();
        let _guard = lock(&self.update_lock);
        let result = mutate(&self.membership, &self.reachability);
        self.reachability.prune_pending(
            |node| self.membership.is_down(node),
            self.config.down_retention,
        );

        let members = self.membership.members();
        let up = members
            .iter()
            .filter(|member| member.state() == MemberState::Up)
            .map(Member::node)
            .collect::<HashSet<_>>();
        self.reachability.promote_pending(&up);
        let disconnected = self.reachability.unreachable_members(&members);
        let unreachable = disconnected
            .iter()
            .map(|(member, _)| *member)
            .collect::<Vec<_>>();
        let broken_links = self.reachability.broken_links(&members);
        let cluster = self.committed_cluster();
        let foreign_clusters = lock(&self.foreign_clusters).retained(&up);
        let changed = self.state_tx.send_if_modified(|state| {
            if state.cluster() == cluster
                && state.members() == members
                && state.unreachable() == unreachable
                && state.broken_links() == broken_links
                && state.foreign_clusters() == foreign_clusters
            {
                return false;
            }
            *state = Arc::new(ClusterState::new(
                self.node(),
                cluster,
                state.version() + 1,
                members.clone(),
                unreachable,
                broken_links,
                foreign_clusters,
            ));
            true
        });
        if changed {
            // The watch first, then the channel, so the stream never leads the current value.
            let state = self.state_tx.borrow().clone();
            self.receptionist.publish(ClusterState::clone(&state));
        }

        Updated {
            result,
            members,
            disconnected,
        }
    }

    pub(crate) fn watchers(&self) -> &WatcherTable {
        &self.watchers
    }

    pub(crate) fn wire_watches(&self) -> &WireWatchTable {
        &self.wire_watches
    }

    pub(crate) fn pending_replies(&self) -> &PendingReplies {
        &self.pending_replies
    }

    pub(crate) fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    pub(crate) fn register_own<M>(
        &self,
        key: WireKey,
        actor_ref: &ActorRef<M>,
    ) -> Result<(), LimitExceeded>
    where
        M: Schema + Send + 'static,
    {
        let applied = match self.registry.register(key, actor_ref)? {
            Registered::Unchanged => true,

            Registered::Added {
                version,
                registration,
            } => self.receptionist.add_own(version, registration),

            Registered::Changed => false,
        };
        if !applied {
            self.apply_own_registrations();
        }
        Ok(())
    }

    pub(crate) fn registrations_snapshot(&self) -> Arc<Snapshot> {
        let registrations = self.registry.registrations();
        Arc::new(receptionist::snapshot(
            registrations.version,
            registrations.groups,
            self.config.max_frame_size.get(),
        ))
    }

    pub(crate) fn offer_snapshot(&self, peer: NodeId, snapshot: &Arc<Snapshot>) {
        self.lanes.offer_snapshot(peer.addr(), snapshot);
    }

    pub(crate) fn merge_reachability(&self, observations: &[WireReachability]) {
        self.update(|membership, reachability| {
            reachability.merge(observations, &membership.up_nodes());
        });
    }

    pub(crate) fn reconnect_backoff(&self, attempts: u32) -> Duration {
        self.config
            .reconnect_backoff
            .duration(attempts.saturating_sub(1))
    }

    pub(crate) fn send(&self, peer: NodeId, frame: Frame<'static>) -> Result<(), LaneError> {
        if self.downed() || self.leaving() {
            return Err(LaneError::NodeUnreachable(peer));
        }

        #[cfg(feature = "cluster-dev")]
        if matches!(frame, Frame::Terminated { .. }) && self.drop_terminated() {
            debug!(%peer, "dropping terminated frame, fault injection");
            return Ok(());
        }
        #[cfg(feature = "cluster-dev")]
        let gossip_len = self.gossip_len(&frame);

        let enqueued = self.lanes.enqueue(
            peer,
            frame,
            || {
                if self.membership.is_down(peer) {
                    return Err(LaneError::NodeUnreachable(peer));
                }
                // Built before the write lock, which every send waits for: it copies every
                // registration.
                Ok(self.registrations_snapshot())
            },
            // Under the lock, since a leave collects the lanes once, under the same lock.
            || !self.leaving() && !self.is_refused(peer.addr()),
        );
        #[cfg(feature = "cluster-dev")]
        if enqueued.is_ok() {
            self.gossip_bytes.fetch_add(gossip_len, Ordering::Relaxed);
        }
        enqueued
    }

    pub(crate) fn data_stream(&self, actor: ActorId) -> Option<usize> {
        self.lanes.data_stream(actor)
    }

    pub(crate) fn track_liveness(&self, peer: NodeId) -> PeerLiveness {
        self.liveness.track(peer, &self.config.failure_detector)
    }

    pub(crate) fn refuse(&self, addr: SocketAddr) -> Duration {
        let now = tokio_time::Instant::now();
        let mut refusals = lock(&self.refusals);
        refusals.retain(|_, refusal| now < refusal.until + MAX_REFUSAL_BACKOFF);
        let strikes = refusals
            .get(&addr)
            .map_or(1, |refusal| refusal.strikes.saturating_add(1));
        let backoff = refusal_backoff(strikes);
        refusals.insert(
            addr,
            Refusal {
                until: now + backoff,
                strikes,
            },
        );
        backoff
    }

    /// A refusal only means whatever answered there last did not speak this protocol.
    pub(crate) fn unrefuse(&self, addr: SocketAddr) {
        lock(&self.refusals).remove(&addr);
    }

    pub(crate) fn request_join(&self, request: JoinRequest) {
        if self.join_request_tx.send(request).is_err() {
            error!("dial loop gone, join will never be attempted");
            debug_assert!(false, "dial loop gone");
        }
    }

    pub(crate) fn lifecycle(&self) -> Lifecycle {
        self.lifecycle.current()
    }

    pub(crate) fn formed(&self) -> bool {
        self.lifecycle() == Lifecycle::Formed
    }

    pub(crate) fn downed(&self) -> bool {
        self.lifecycle() == Lifecycle::Downed
    }

    pub(crate) fn leaving(&self) -> bool {
        self.leaving.load(Ordering::Relaxed)
    }

    pub(crate) fn leaving_flag(&'static self) -> &'static AtomicBool {
        &self.leaving
    }

    /// Turns true when this node begins to leave, so a lane waiting out a reconnect backoff dials
    /// again in time to drain.
    pub(crate) fn leave_rx(&self) -> watch::Receiver<bool> {
        self.leave_tx.subscribe()
    }

    /// Exposes a failed departure only after this node is downed and severed.
    pub(crate) async fn leave(&'static self) -> Result<Departure, LeaveError> {
        // Decided under the lifecycle lock, so a down landing concurrently either precedes the
        // departure or is ignored by it.
        let mut departed_rx = self.lifecycle.coordinate(|coordinator| {
            if coordinator.lifecycle == Lifecycle::Downed && self.departure.get().is_none() {
                return Err(LeaveError::Downed);
            }

            Ok(self
                .departure
                .get_or_init(|| self.spawn_departure())
                .clone())
        })?;

        match departed_rx.wait_for(Option::is_some).await {
            Ok(departed) => match &*departed {
                Some(Departed::Reported(departure)) => Ok(departure.clone()),
                Some(Departed::Incomplete) => Err(LeaveError::Incomplete),
                None => unreachable!("waited for a published departure"),
            },

            Err(_) => {
                error!("departure supervisor gone before this node left, downing this node");
                self.fail_departure();
                Err(LeaveError::Incomplete)
            }
        }
    }

    /// The cluster state naming the new cluster is published before this returns.
    pub(crate) fn form_cluster(&self) -> Result<(), FormError> {
        let now = tokio_time::Instant::now();
        self.lifecycle
            .coordinate(|coordinator| coordinator.form(now))?;
        self.update(|_, _| ());
        Ok(())
    }

    /// Observed by [form]; exclusivity itself comes from the single join loop, not from this.
    /// `false` if this node is downed.
    pub(crate) fn enter_join(&self) -> bool {
        self.lifecycle.coordinate(Coordinator::enter_join)
    }

    /// The single commit point of a join attempt: permit release and outcome happen under one
    /// lock, so no formation slips in between; a down decided here runs its side effects once.
    pub(crate) fn finish_join(
        &self,
        addr: SocketAddr,
        result: Result<(), ConnectError>,
    ) -> Result<(), ConnectError> {
        let finished = self
            .lifecycle
            .coordinate(|coordinator| coordinator.finish_join(addr, result));

        if finished.newly_downed {
            self.sever_down();
            error!("this node has been downed, restart the process to rejoin");
        }
        finished.result
    }

    pub(crate) fn pin_cluster(&self, addr: SocketAddr, cluster: ClusterId) {
        self.lifecycle
            .coordinate(|coordinator| coordinator.pin(addr, cluster));
    }

    pub(crate) fn pinned_cluster(&self) -> Option<SocketAddr> {
        self.lifecycle
            .coordinate(|coordinator| coordinator.pinned.map(|(addr, _)| addr))
    }

    /// Only once the joined cluster's snapshot is complete, so a node is never committed to a
    /// cluster whose member list it does not hold.
    pub(crate) fn commit_cluster(&self, cluster: ClusterId) {
        self.lifecycle
            .coordinate(|coordinator| coordinator.commit(cluster));
    }

    pub(crate) fn committed_cluster(&self) -> Option<ClusterId> {
        self.lifecycle.committed_cluster()
    }

    /// The committed cluster ID, else the pinned one: a pinned cluster may count this node
    /// already, so a peer of any other cluster is refused.
    pub(crate) fn effective_cluster(&self) -> Option<ClusterId> {
        self.lifecycle
            .coordinate(|coordinator| coordinator.effective_cluster())
    }

    /// Decided under one lock, so evidence a running attempt has gathered is never mistaken for
    /// the stale pin.
    pub(crate) fn release_pin_if_gone(
        &self,
        universe: &BTreeSet<SocketAddr>,
    ) -> Option<SocketAddr> {
        self.lifecycle
            .coordinate(|coordinator| coordinator.release_pin_if_gone(universe))
    }

    /// Recorded while the attempt still holds the join permit, so no formation slips in before.
    pub(crate) fn record_ambiguous_join(&self, addr: SocketAddr) {
        let bound = self.config.ambiguous_join_bound;
        let until = tokio_time::Instant::now() + bound;
        if self
            .lifecycle
            .coordinate(|coordinator| coordinator.record_ambiguous_join(addr, until))
        {
            warn!(
                seed_addr = %addr,
                ?bound,
                "join attempt ended without proof whether the seed admitted this node, \
                 refusing to form until it answers"
            );
        }
    }

    pub(crate) fn release_ambiguous_joins_if_gone(
        &self,
        universe: &BTreeSet<SocketAddr>,
    ) -> Vec<SocketAddr> {
        self.lifecycle
            .coordinate(|coordinator| coordinator.release_ambiguous_joins_if_gone(universe))
    }

    pub(crate) fn request_probe(
        &self,
        addr: SocketAddr,
    ) -> oneshot::Receiver<Result<Option<ClusterId>, ConnectError>> {
        let (result_tx, result_rx) = oneshot::channel();
        if self
            .probe_request_tx
            .send(ProbeRequest { addr, result_tx })
            .is_err()
        {
            error!(probe_addr = %addr, "probe loop gone, address will never be probed");
        }
        result_rx
    }

    /// Only a committed cluster is compared: a provisional one may still be released.
    pub(crate) fn sight_cluster(&self, addr: SocketAddr, cluster: Option<ClusterId>) {
        let (Some(this_cluster), Some(cluster)) = (self.committed_cluster(), cluster) else {
            return;
        };
        if cluster == this_cluster
            || self
                .membership
                .up_nodes()
                .iter()
                .any(|node| node.addr() == addr)
        {
            return;
        }

        let now = tokio_time::Instant::now();
        if lock(&self.foreign_clusters).sight(addr, cluster, now) {
            self.update(|_, _| ());
        }
    }

    pub(crate) fn retain_foreign_clusters_for(&self, retention: Duration) {
        lock(&self.foreign_clusters).retention = retention;
    }

    /// Honoring this Down keeps other nodes' synthesized signals true; only a restart rejoins.
    pub(crate) fn self_down(&self) {
        if self.leaving() {
            return;
        }
        if self.latch_down() {
            error!("this node has been downed, restart the process to rejoin");
        }
    }

    pub(crate) fn snapshot_frames(&self, members: Vec<Member>) -> Vec<Frame<'static>> {
        membership::snapshot_frames(members, self.config.max_frame_size.get())
    }

    pub(crate) fn reachability_snapshot_frames(&self) -> Vec<Frame<'static>> {
        let up = self.membership.up_nodes();
        reachability::snapshot_chunks(
            self.reachability.snapshot(&up),
            self.config.max_frame_size.get(),
        )
        .into_iter()
        .map(|observations| Frame::Reachability { observations })
        .collect()
    }

    /// Skips a disconnected lane, whose uncounted queues would otherwise grow.
    pub(crate) fn push_reachability(&self, observations: Vec<WireReachability>) {
        if observations.is_empty() || self.downed() {
            return;
        }

        let chunks = reachability::snapshot_chunks(observations, self.config.max_frame_size.get());
        for peer in self.membership.up_peers() {
            if self.lane_connected(peer.addr()) == Some(false) {
                continue;
            }
            if let Err(error) = chunks.iter().try_for_each(|chunk| {
                self.send(
                    peer,
                    Frame::Reachability {
                        observations: chunk.clone(),
                    },
                )
            }) {
                debug!(%peer, %error, "cannot send reachability observations");
            }
        }
    }

    /// Pushes to every Up member whose lane is live; the periodic tick covers everyone else.
    pub(crate) fn push_gossip(&self) {
        if self.downed() {
            return;
        }

        self.gossip_to_up_members(LaneFilter::Connected, false);
    }

    /// A peer already sent the current member list gets an empty frame, except on every
    /// [FULL_GOSSIP_EVERY]th tick.
    pub(crate) fn gossip_tick(&self) {
        let tick = self.gossip_ticks.fetch_add(1, Ordering::Relaxed);
        self.gossip_to_up_members(
            LaneFilter::NotDisconnected,
            tick.is_multiple_of(FULL_GOSSIP_EVERY),
        );
    }

    /// The next tick sends this peer the full member list.
    pub(crate) fn reset_gossiped_version(&self, peer: NodeId) {
        lock(&self.gossiped_versions).remove(&peer);
    }

    /// The peer a payload for the node is encoded for, without an application version until a
    /// handshake with it has completed.
    pub(crate) fn peer(&self, node: NodeId) -> Arc<Peer> {
        read(&self.peers)
            .get(&node)
            .cloned()
            .unwrap_or_else(|| Arc::new(Peer::new(node, None)))
    }

    /// Lock order: the peers, then `Membership::entries`. The Down check runs under the peers
    /// lock, which `flush_fenced` evicts under, so a death between the admission and this record
    /// leaves no entry behind.
    pub(crate) fn record_peer(&self, node: NodeId, app_version: Option<AppVersion>) {
        let mut peers = write(&self.peers);
        if self.membership.is_down(node) {
            return;
        }
        peers.insert(node, Arc::new(Peer::new(node, app_version)));
    }

    /// `None` if no lane exists; gossip skips on connectivity alone, never on suspicion.
    pub(crate) fn lane_connected(&self, addr: SocketAddr) -> Option<bool> {
        self.lanes.connected(addr)
    }

    pub(crate) fn mark_lane_disconnected(&self, addr: SocketAddr, lane_id: Generation) {
        self.lanes.disconnect(addr, lane_id);
    }

    /// Mark Down, close the lane, quiesce deliveries, then fail asks and flush synthesized signals.
    pub(crate) fn node_death(&self, peer: NodeId) {
        let outcome = self
            .update(|membership, _| membership.down(peer, Instant::now()))
            .result;
        self.death_consequences(peer, outcome);
    }

    pub(crate) fn death_consequences(&self, peer: NodeId, outcome: DownOutcome) {
        if !outcome.member_changed && !outcome.watermark_changed {
            return;
        }

        self.flush_fenced(peer);
        if outcome.member_changed {
            self.push_gossip();
        }
    }

    /// Ends every deferred departure the fence covers, since the death runs here instead.
    pub(crate) fn flush_fenced(&self, fence: NodeId) {
        lock(&self.departures)
            .deferred
            .retain(|node, _| !fence.covers(*node));
        write(&self.peers).retain(|node, _| !fence.covers(*node));
        self.lanes.remove_fenced(fence);
        self.liveness.quiesce_fenced(fence);
        self.update(|_, reachability| reachability.prune_fenced(fence));
        self.pending_replies.fail_fenced(fence);
        self.receptionist.evict_fenced(fence);

        for (target, watchers) in self.watchers.take_fenced(fence) {
            for watcher in watchers {
                if let Err(error) = watcher.handle_terminated(target) {
                    debug!(watcher_id = %watcher.watcher_id(), other_id = %target, %error, "cannot send synthesized terminated signal");
                }
            }
        }

        for watch in self.wire_watches.take_fenced(fence) {
            if let Some(watcher_registry) = self.registry.watcher_registry(watch.target) {
                watcher_registry.remove(watch.wire_watcher_id);
            }
        }
        self.liveness.untrack_fenced(fence);
    }

    /// Lock order: the departures, then `Membership::entries`.
    pub(crate) fn departures(&self) -> MutexGuard<'_, Departures> {
        lock(&self.departures)
    }

    /// Defers the death of a departed node until every inbound connection from it has drained,
    /// including one it is yet to open to drain, for at most `leave_timeout`; `true` if one is
    /// open. A connection is awaited from a node which was Up here.
    pub(crate) fn defer_departure(
        &self,
        departures: &mut Departures,
        node: NodeId,
        was_up: bool,
    ) -> bool {
        let deadline = tokio_time::Instant::now() + self.config.leave_timeout;
        let deferral = departures.deferred.entry(node).or_insert(Deferral {
            deadline,
            awaited: false,
        });
        deferral.awaited |= was_up;
        departures.inbound.contains_key(&node)
    }

    /// Whether frames from the peer are still delivered: it is Up, or its departure is deferred.
    /// Membership is read first, since a deferral is recorded under the departures lock held
    /// across its merge.
    pub(crate) fn delivers_from(&self, peer: NodeId) -> bool {
        !self.membership.is_down(peer) || lock(&self.departures).deferred.contains_key(&peer)
    }

    /// Decides a Member handshake in one critical section with the departures: a departure merged
    /// in between could otherwise turn an Up member into a refused one, although its death waits
    /// for exactly this connection.
    pub(crate) fn admit_member(&'static self, peer: NodeId) -> MemberAdmission {
        let mut departures = self.departures();
        if self.membership.is_up(peer) {
            return MemberAdmission::Member(self.count_inbound(&mut departures, peer));
        }
        if let Some(deferral) = departures.deferred.get_mut(&peer)
            && self.membership.state(peer) == Some(MemberState::Left)
        {
            deferral.awaited = true;
            return MemberAdmission::Draining(self.count_inbound(&mut departures, peer));
        }
        if self.membership.is_down(peer) {
            MemberAdmission::Dead
        } else {
            MemberAdmission::Unknown
        }
    }

    /// Counts an admitted inbound connection until its [InboundSlot] is dropped.
    pub(crate) fn count_inbound(
        &'static self,
        departures: &mut Departures,
        peer: NodeId,
    ) -> InboundSlot {
        *departures.inbound.entry(peer).or_default() += 1;
        InboundSlot {
            endpoint: self,
            peer,
            reading: false,
        }
    }

    /// Runs the deferred death of a departed peer once its last inbound connection has drained,
    /// so a reader superseded by a reconnect leaves the deferral to its successor. A connection
    /// abandoned before its reader started has drained nothing, so the deferral keeps waiting
    /// for another one, until its deadline.
    fn inbound_closed(&self, peer: NodeId, read: bool) {
        let deferred = {
            let mut departures = lock(&self.departures);
            let remaining = departures.inbound.get_mut(&peer).map_or(0, |count| {
                *count -= 1;
                *count
            });
            if remaining == 0 {
                departures.inbound.remove(&peer);
                read && departures.deferred.remove(&peer).is_some()
            } else {
                false
            }
        };
        if deferred {
            info!(%peer, "node death, its departure has drained");
            self.flush_fenced(peer);
        }
    }

    pub(crate) fn departure_deferred(&self, peer: NodeId) -> bool {
        lock(&self.departures).deferred.contains_key(&peer)
    }

    #[cfg(test)]
    pub(crate) fn inbound_open(&self, peer: NodeId) -> bool {
        lock(&self.departures).inbound.contains_key(&peer)
    }

    #[cfg(test)]
    pub(crate) fn departure_awaited(&self, peer: NodeId) -> bool {
        lock(&self.departures)
            .deferred
            .get(&peer)
            .is_some_and(|deferral| deferral.awaited)
    }

    /// Runs the deferred deaths whose deadline has passed. Where a connection was awaited, the
    /// departure has not drained within `leave_timeout`.
    pub(crate) fn end_overdue_departures(&self, now: tokio_time::Instant) {
        let overdue = lock(&self.departures)
            .deferred
            .extract_if(|_, deferral| deferral.deadline <= now)
            .map(|(peer, deferral)| (peer, deferral.awaited))
            .collect::<Vec<_>>();
        for (peer, awaited) in overdue {
            if awaited {
                warn!(%peer, "node death, its departure has not drained within leave_timeout");
            } else {
                debug!(%peer, "node death, its departure was learned without a connection");
            }
            self.flush_fenced(peer);
        }
    }

    /// The kept incarnations are reread inside the retention, else a fresh entry is lost for good.
    pub(crate) fn untrack_idle_peers(&self) {
        self.liveness.retain_with(|| self.membership.up_nodes());
        let up = self.membership.up_nodes();
        lock(&self.gossiped_versions).retain(|node, _| up.contains(node));
    }

    pub(crate) fn bind_lane(&self, addr: SocketAddr, lane_id: Generation, peer: NodeId) {
        self.lanes
            .bind(addr, lane_id, peer, || self.membership.is_down(peer));
    }

    /// Held from the old reader's shutdown to the new one's spawn, else two readers overlap.
    pub(crate) async fn supersede_inbound_reader<F>(&self, addr: SocketAddr, spawn_reader: F)
    where
        F: FnOnce(watch::Receiver<()>) -> JoinHandle<()>,
    {
        let mut readers = self.inbound_readers.lock().await;

        if let Some(superseded) = readers.remove(&addr) {
            let _ = superseded.shutdown_tx.send(());
            if let Err(error) = superseded.reader.await {
                warn!(peer_addr = %addr, %error, "superseded inbound reader panicked");
            }
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let reader = spawn_reader(shutdown_rx);
        readers.insert(
            addr,
            InboundReader {
                shutdown_tx,
                reader,
            },
        );
    }

    pub(crate) fn sever_rx(&self) -> watch::Receiver<u64> {
        self.sever_tx.subscribe()
    }

    pub(crate) fn sever(&self) {
        self.sever_tx.send_modify(|generation| *generation += 1);
    }

    #[cfg(feature = "cluster-dev")]
    pub(crate) fn arm_terminated_drop(&self, count: u64) {
        self.dropped_terminated.store(count, Ordering::Relaxed);
    }

    #[cfg(feature = "cluster-dev")]
    pub(crate) fn gossip_bytes(&self) -> u64 {
        self.gossip_bytes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn arm_departure_panic(&self) {
        self.departure_panics.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_lane_open(&self, addr: SocketAddr, lane_id: Generation) -> bool {
        self.lanes.is_open(addr, lane_id)
    }

    /// `true` if this call removed the lane, i.e. it was still the address's current one.
    pub(crate) fn remove_lane(&self, addr: SocketAddr, lane_id: Generation) -> bool {
        self.lanes.remove(addr, lane_id)
    }

    fn sweep_foreign_clusters(&self, now: tokio_time::Instant) {
        if lock(&self.foreign_clusters).expire(now) {
            self.update(|_, _| ());
        }

        let foreign_clusters = self.state_tx.borrow().foreign_clusters().to_vec();
        if lock(&self.foreign_clusters).report_due(!foreign_clusters.is_empty(), now) {
            error!(
                ?foreign_clusters,
                "discovered addresses are members of another cluster, which never merges with this one"
            );
        }
    }

    fn new(
        config: EndpointConfig,
        data_streams: usize,
        dial_request_tx: Sender<DialRequest>,
        join_request_tx: Sender<JoinRequest>,
        probe_request_tx: Sender<ProbeRequest>,
    ) -> Self {
        let node = NodeId::new(config.advertised_addr);
        let membership = Membership::new(node, config.down_watermarks, config.down_retention);
        let state = ClusterState::new(
            node,
            None,
            0,
            vec![Member::new(node, MemberState::Up)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let (changes_tx, _) = broadcast::channel(config.change_capacity.get());
        let receptionist = Receptionist::new(node, state.clone(), changes_tx.clone());
        let (state_tx, _) = watch::channel(Arc::new(state));

        let lanes = Lanes::new(data_streams, config.outbound_capacity, dial_request_tx);

        Self {
            node,
            config,
            registry: Arc::new(Registry::new()),
            lanes,
            inbound_readers: tokio::sync::Mutex::new(HashMap::new()),
            membership,
            update_lock: Mutex::new(()),
            state_tx,
            changes_tx,
            refusals: Mutex::new(HashMap::new()),
            lifecycle: CoordinatedLifecycle::new(),
            leaving: AtomicBool::new(false),
            leave_tx: watch::channel(false).0,
            departure: OnceLock::new(),
            departures: Mutex::new(Departures::default()),
            liveness: Liveness::new(),
            reachability: Reachability::new(node),
            watchers: WatcherTable::new(),
            wire_watches: WireWatchTable::new(),
            pending_replies: PendingReplies::new(),
            receptionist,
            join_request_tx,
            probe_request_tx,
            foreign_clusters: Mutex::new(ForeignClusters::new()),
            unreachable_warnings: Mutex::new(UnreachableWarnings::default()),
            sever_tx: watch::channel(0).0,
            gossiped_versions: Mutex::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            gossip_ticks: AtomicU64::new(0),
            #[cfg(feature = "cluster-dev")]
            dropped_terminated: AtomicU64::new(0),
            #[cfg(feature = "cluster-dev")]
            gossip_bytes: AtomicU64::new(0),
            #[cfg(test)]
            departure_panics: AtomicBool::new(false),
        }
    }

    /// A task of its own, since a canceled caller must not cancel the departure.
    fn spawn_departure(&'static self) -> watch::Receiver<Option<Departed>> {
        let (departed_tx, departed_rx) = watch::channel(None);
        let departure = task::spawn(self.depart());
        task::spawn(async move {
            let departed = match departure.await {
                Ok(departure) => Departed::Reported(departure),

                Err(error) => {
                    error!(%error, "departure failed, downing this node");
                    self.fail_departure();
                    Departed::Incomplete
                }
            };
            departed_tx.send_replace(Some(departed));
        });
        departed_rx
    }

    async fn depart(&self) -> Departure {
        // Announce before leaving, which refuses every send, and wait for the lanes before the
        // sever, since a sever turns whatever is still queued or unacknowledged into dead letters.
        self.update(|membership, _| membership.leave(self.node, Instant::now()));
        self.announce();
        let lanes_done = self.begin_leave();
        #[cfg(test)]
        if self.departure_panics.load(Ordering::Relaxed) {
            panic!("departure bug");
        }

        let deadline = tokio_time::Instant::now() + self.config.leave_timeout;
        let mut departure = Departure::default();
        for (addr, lane_done) in lanes_done {
            match timeout_at(deadline, lane_done).await {
                Ok(Ok(Drain::Acknowledged)) => departure.acknowledged.push(addr),
                Ok(Ok(Drain::Unacknowledged) | Err(_)) => departure.unacknowledged.push(addr),
                Err(_) => departure.timed_out.push(addr),
            }
        }
        departure.sort();
        if !departure.is_acknowledged() {
            warn!(
                unacknowledged = ?departure.unacknowledged,
                timed_out = ?departure.timed_out,
                "leaving with the departure not acknowledged by every peer"
            );
        }

        self.leave_down();
        departure
    }

    /// Pushes to every Up member, a disconnected lane included: a departure is sent once.
    fn announce(&self) {
        let members = self.membership.members();
        for peer in self.membership.up_peers() {
            for frame in self.snapshot_frames(members.clone()) {
                if let Err(error) = self.send(peer, frame) {
                    debug!(%peer, %error, "cannot announce the departure");
                }
            }
        }
    }

    fn begin_leave(&self) -> Vec<(SocketAddr, oneshot::Receiver<Drain>)> {
        self.leaving.store(true, Ordering::Relaxed);
        self.leave_tx.send_replace(true);
        self.lanes.drain()
    }

    /// This node's own entry stays Left: only a downing moves it to Down.
    fn leave_down(&self) {
        if self.lifecycle.coordinate(Coordinator::down) {
            self.sever();
            info!("this node has left the cluster, restart the process to rejoin");
        }
    }

    fn fail_departure(&self) {
        if !self.latch_down() {
            self.sever();
        }
    }

    fn apply_own_registrations(&self) {
        let registrations = self.registry.registrations();
        self.receptionist
            .apply(self.node, registrations.version, registrations.groups, true);
    }

    fn gossip_to_up_members(&self, lanes: LaneFilter, full: bool) {
        let (version, members) = self.membership.versioned_members();
        for peer in self.membership.up_peers() {
            if !lanes.admits(self.lane_connected(peer.addr())) {
                continue;
            }

            let current = !full && lock(&self.gossiped_versions).get(&peer) == Some(&version);
            let frames = if current {
                vec![Frame::Gossip {
                    members: Vec::new(),
                    more: false,
                }]
            } else {
                self.snapshot_frames(members.clone())
            };
            match frames
                .into_iter()
                .try_for_each(|frame| self.send(peer, frame))
            {
                Ok(()) if !current => {
                    lock(&self.gossiped_versions).insert(peer, version);
                }

                Ok(()) => {}

                Err(error) => debug!(%peer, %error, "cannot send gossip"),
            }
        }
    }

    fn is_refused(&self, addr: SocketAddr) -> bool {
        lock(&self.refusals)
            .get(&addr)
            .is_some_and(|refusal| tokio_time::Instant::now() < refusal.until)
    }

    /// `true` if this call ended the endpoint, so the caller logs why exactly once.
    fn latch_down(&self) -> bool {
        if !self.lifecycle.coordinate(Coordinator::down) {
            return false;
        }

        self.sever_down();
        true
    }

    fn sever_down(&self) {
        self.update(|membership, _| membership.down(self.node, Instant::now()));
        self.sever();
    }

    #[cfg(feature = "cluster-dev")]
    fn drop_terminated(&self) -> bool {
        self.dropped_terminated
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    }

    #[cfg(feature = "cluster-dev")]
    fn gossip_len(&self, frame: &Frame<'_>) -> u64 {
        if !matches!(frame, Frame::Gossip { .. }) {
            return 0;
        }

        match frame.encoded_len() {
            Ok(len) => len as u64,

            Err(error) => {
                warn!(%error, "cannot measure a gossip frame");
                0
            }
        }
    }
}

pub(crate) struct Updated<R> {
    pub(crate) result: R,
    pub(crate) members: Vec<Member>,
    pub(crate) disconnected: Vec<(Member, Instant)>,
}

/// The admitted inbound connections per peer, and the departed peers whose death waits for theirs
/// to drain, until a deadline. A connection is awaited from a peer which was Up here or has
/// connected to drain: only then is a death at the deadline a departure which has not drained.
#[derive(Default)]
pub(crate) struct Departures {
    inbound: HashMap<NodeId, usize>,
    deferred: HashMap<NodeId, Deferral>,
}

/// An admitted inbound connection, counted in [Departures] from its admission until it is
/// dropped, whether its reader ends or its setup is abandoned before the reader starts.
pub(crate) struct InboundSlot {
    endpoint: &'static EndpointInner,
    peer: NodeId,
    reading: bool,
}

impl InboundSlot {
    /// Marks the connection's reader as started, from when its end is a drained connection.
    pub(crate) fn reading(&mut self) {
        self.reading = true;
    }
}

impl Drop for InboundSlot {
    fn drop(&mut self) {
        self.endpoint.inbound_closed(self.peer, self.reading);
    }
}

/// How a Member handshake is admitted, see [EndpointInner::admit_member].
pub(crate) enum MemberAdmission {
    Member(InboundSlot),
    Draining(InboundSlot),
    Dead,
    Unknown,
}

fn validate(config: &EndpointConfig) -> Result<(), InvalidEndpointConfig> {
    let advertised_addr = config.advertised_addr;
    if advertised_addr.ip().is_unspecified() {
        return Err(InvalidEndpointConfig::UnspecifiedAdvertisedIp { advertised_addr });
    }
    if advertised_addr.port() == 0 {
        return Err(InvalidEndpointConfig::ZeroAdvertisedPort { advertised_addr });
    }
    let min_frame_size = Handshake::MAX_ENCODED_LEN
        .max(membership::MIN_FRAME_SIZE)
        .max(reachability::MIN_FRAME_SIZE);
    if config.max_frame_size.get() < min_frame_size {
        return Err(InvalidEndpointConfig::MaxFrameSizeTooSmall {
            max_frame_size: config.max_frame_size.get(),
            min: min_frame_size,
        });
    }
    let codec_name_len = config.codec.name().len();
    if codec_name_len > Handshake::MAX_STRING_LEN {
        return Err(InvalidEndpointConfig::CodecNameTooLong {
            len: codec_name_len,
            max: Handshake::MAX_STRING_LEN,
        });
    }
    let app_version_len = config
        .app_version
        .as_ref()
        .map_or(0, |app_version| app_version.to_string().len());
    if app_version_len > Handshake::MAX_STRING_LEN {
        return Err(InvalidEndpointConfig::AppVersionTooLong {
            len: app_version_len,
            max: Handshake::MAX_STRING_LEN,
        });
    }
    if config.heartbeat_interval.is_zero() {
        return Err(InvalidEndpointConfig::ZeroHeartbeatInterval);
    }
    if config.watch_refresh_interval.is_zero() {
        return Err(InvalidEndpointConfig::ZeroWatchRefreshInterval);
    }
    if !config.change_capacity.is_power_of_two()
        || config.change_capacity.get() > MAX_CHANGE_CAPACITY
    {
        return Err(InvalidEndpointConfig::ChangeCapacity {
            change_capacity: config.change_capacity.get(),
            max: MAX_CHANGE_CAPACITY,
        });
    }
    let silence = config.heartbeat_interval.max(config.watch_refresh_interval);
    if !failure::tolerates_silence(&config.failure_detector, silence) {
        return Err(InvalidEndpointConfig::FailureDetectorTooEager { silence });
    }

    Ok(())
}

pub(crate) fn get() -> Option<&'static EndpointInner> {
    #[cfg(test)]
    if let Some(endpoint) = CURRENT_FOR_TESTS.get() {
        return Some(endpoint);
    }

    ENDPOINT.get()
}

/// What [get] answers on this thread while `f` runs, for code reaching the endpoint through it.
#[cfg(test)]
pub(crate) fn with_current<F, R>(endpoint: &'static EndpointInner, f: F) -> R
where
    F: FnOnce() -> R,
{
    /// Must restore the outer endpoint even if `f` unwinds!
    struct Restore(Option<&'static EndpointInner>);

    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT_FOR_TESTS.set(self.0);
        }
    }

    let _restore = Restore(CURRENT_FOR_TESTS.replace(Some(endpoint)));
    f()
}

#[derive(Clone, Copy)]
enum LaneFilter {
    Connected,
    NotDisconnected,
}

impl LaneFilter {
    fn admits(self, connected: Option<bool>) -> bool {
        match self {
            LaneFilter::Connected => connected == Some(true),
            LaneFilter::NotDisconnected => connected != Some(false),
        }
    }
}

struct UpdateGuard;

impl UpdateGuard {
    fn enter() -> Self {
        UPDATING.with(|updating| {
            if updating.get() {
                panic!("re-entrant cluster state update, which would deadlock");
            }
            updating.set(true);
        });
        Self
    }
}

impl Drop for UpdateGuard {
    fn drop(&mut self) {
        UPDATING.with(|updating| updating.set(false));
    }
}

struct CoordinatedLifecycle {
    coordinator: Mutex<Coordinator>,
    lifecycle: AtomicU8,
    cluster: OnceLock<ClusterId>,
}

impl CoordinatedLifecycle {
    fn new() -> Self {
        Self {
            coordinator: Mutex::new(Coordinator::default()),
            lifecycle: AtomicU8::new(Lifecycle::Unformed.code()),
            cluster: OnceLock::new(),
        }
    }

    fn current(&self) -> Lifecycle {
        Lifecycle::from_code(self.lifecycle.load(Ordering::Relaxed))
    }

    fn committed_cluster(&self) -> Option<ClusterId> {
        self.cluster.get().copied()
    }

    fn coordinate<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Coordinator) -> R,
    {
        let mut coordinator = lock(&self.coordinator);
        let result = f(&mut coordinator);
        // Stored before the lock is released, else the lock free reads drift from the pin.
        self.lifecycle
            .store(coordinator.lifecycle.code(), Ordering::Relaxed);
        if let Some(committed) = coordinator.committed {
            let cluster = self.cluster.get_or_init(|| committed);
            debug_assert_eq!(*cluster, committed, "a committed cluster ID never changes");
        }
        result
    }
}

/// Every lifecycle transition runs here, so a formation decision cannot slip between a join's
/// admission and the record of it.
#[derive(Default)]
struct Coordinator {
    lifecycle: Lifecycle,
    join_in_flight: bool,
    pinned: Option<(SocketAddr, ClusterId)>,
    committed: Option<ClusterId>,
    ambiguous_joins: BTreeMap<SocketAddr, tokio_time::Instant>,
}

impl Coordinator {
    fn enter_join(&mut self) -> bool {
        if self.lifecycle == Lifecycle::Downed {
            return false;
        }

        self.join_in_flight = true;
        true
    }

    /// A down landing mid attempt leaves nothing to pin: the evidence would outlive the node.
    /// Another cluster never replaces the pinned one, which may count this node already.
    fn pin(&mut self, addr: SocketAddr, cluster: ClusterId) {
        if self.lifecycle == Lifecycle::Downed {
            return;
        }
        if let Some((_, pinned)) = self.pinned
            && pinned != cluster
        {
            return;
        }

        self.pinned = Some((addr, cluster));
    }

    fn commit(&mut self, cluster: ClusterId) {
        if self.lifecycle == Lifecycle::Downed {
            return;
        }

        debug_assert!(
            self.committed.is_none_or(|committed| committed == cluster),
            "a committed cluster ID never changes"
        );
        self.committed.get_or_insert(cluster);
    }

    fn effective_cluster(&self) -> Option<ClusterId> {
        self.committed.or(self.pinned.map(|(_, cluster)| cluster))
    }

    /// `false` if a membership, a down or a pin rules out formation already.
    fn record_ambiguous_join(&mut self, addr: SocketAddr, until: tokio_time::Instant) -> bool {
        if self.lifecycle != Lifecycle::Unformed || self.pinned.is_some() {
            return false;
        }

        self.ambiguous_joins.insert(addr, until);
        true
    }

    /// Downed dominates every raw outcome, since a down may land anywhere inside the attempt.
    fn finish_join(&mut self, addr: SocketAddr, result: Result<(), ConnectError>) -> Finished {
        self.join_in_flight = false;
        if self.lifecycle == Lifecycle::Downed {
            self.pinned = None;
            return Finished {
                result: Err(ConnectError::SelfDowned),
                newly_downed: false,
            };
        }

        if let Err(error) = &result
            && error.proves_no_admission()
        {
            self.ambiguous_joins.remove(&addr);
        }

        let mut newly_downed = false;
        let result = match result {
            Ok(()) => {
                self.pinned = None;
                self.ambiguous_joins.clear();
                self.lifecycle = Lifecycle::Formed;
                Ok(())
            }

            Err(ConnectError::Refused(RefusalReason::Down)) => {
                newly_downed = self.down();
                Err(ConnectError::Refused(RefusalReason::Down))
            }

            Err(ConnectError::Refused(RefusalReason::NoCluster)) => {
                if self.pinned.is_some_and(|(pinned, _)| pinned == addr) {
                    self.pinned = None;
                }
                Err(ConnectError::Refused(RefusalReason::NoCluster))
            }

            Err(error) => Err(error),
        };

        Finished {
            result,
            newly_downed,
        }
    }

    fn release_pin_if_gone(&mut self, universe: &BTreeSet<SocketAddr>) -> Option<SocketAddr> {
        if self.join_in_flight {
            return None;
        }

        match self.pinned {
            Some((pinned, _)) if !universe.contains(&pinned) => {
                self.pinned = None;
                Some(pinned)
            }

            _ => None,
        }
    }

    fn release_ambiguous_joins_if_gone(
        &mut self,
        universe: &BTreeSet<SocketAddr>,
    ) -> Vec<SocketAddr> {
        let gone = self
            .ambiguous_joins
            .keys()
            .filter(|addr| !universe.contains(addr))
            .copied()
            .collect::<Vec<_>>();
        for addr in &gone {
            self.ambiguous_joins.remove(addr);
        }
        gone
    }

    fn form(&mut self, now: tokio_time::Instant) -> Result<Lifecycle, FormError> {
        if self.lifecycle == Lifecycle::Downed {
            return Err(FormError::Downed);
        }
        if self.join_in_flight {
            return Err(FormError::JoinInFlight);
        }
        if let Some((addr, _)) = self.pinned {
            return Err(FormError::ClusterPinned(addr));
        }
        self.ambiguous_joins.retain(|_, until| now < *until);
        if let Some(addr) = self.ambiguous_joins.keys().next() {
            return Err(FormError::JoinAmbiguous(*addr));
        }

        match self.lifecycle {
            Lifecycle::Unformed => {
                self.committed = Some(ClusterId::new());
                self.lifecycle = Lifecycle::Formed;
                Ok(self.lifecycle)
            }

            Lifecycle::Formed => Err(FormError::AlreadyFormed),

            Lifecycle::Downed => unreachable!("checked above"),
        }
    }

    fn down(&mut self) -> bool {
        if self.lifecycle == Lifecycle::Downed {
            return false;
        }

        self.lifecycle = Lifecycle::Downed;
        self.pinned = None;
        self.ambiguous_joins.clear();
        true
    }
}

struct Finished {
    result: Result<(), ConnectError>,
    newly_downed: bool,
}

struct InboundReader {
    shutdown_tx: watch::Sender<()>,
    reader: JoinHandle<()>,
}

enum Departed {
    Reported(Departure),
    Incomplete,
}

struct Deferral {
    deadline: tokio_time::Instant,
    awaited: bool,
}

struct Refusal {
    until: tokio_time::Instant,
    strikes: u32,
}

struct ForeignClusters {
    retention: Duration,
    sightings: BTreeMap<SocketAddr, Sighting>,
    reported_at: Option<tokio_time::Instant>,
}

impl ForeignClusters {
    fn new() -> Self {
        Self {
            retention: BootstrapConfig::DEFAULT_PROBE_INTERVAL.saturating_mul(3),
            sightings: BTreeMap::new(),
            reported_at: None,
        }
    }

    /// `true` if the published list changes.
    fn sight(&mut self, addr: SocketAddr, cluster: ClusterId, now: tokio_time::Instant) -> bool {
        let sighting = Sighting {
            cluster,
            confirmed_at: now,
        };
        self.sightings
            .insert(addr, sighting)
            .is_none_or(|previous| previous.cluster != cluster)
    }

    /// `true` if a sighting expired.
    fn expire(&mut self, now: tokio_time::Instant) -> bool {
        let retention = self.retention;
        let sightings = self.sightings.len();
        self.sightings
            .retain(|_, sighting| now < sighting.confirmed_at + retention);
        self.sightings.len() != sightings
    }

    fn retained(&mut self, up: &HashSet<NodeId>) -> Vec<ForeignCluster> {
        let up = up.iter().map(|node| node.addr()).collect::<HashSet<_>>();
        self.sightings.retain(|addr, _| !up.contains(addr));
        self.sightings
            .iter()
            .map(|(addr, sighting)| ForeignCluster::new(*addr, sighting.cluster))
            .collect()
    }

    fn report_due(&mut self, seen: bool, now: tokio_time::Instant) -> bool {
        if !seen {
            self.reported_at = None;
            return false;
        }
        if self
            .reported_at
            .is_some_and(|reported_at| now < reported_at + FOREIGN_CLUSTER_REPORT_INTERVAL)
        {
            return false;
        }

        self.reported_at = Some(now);
        true
    }
}

struct Sighting {
    cluster: ClusterId,
    confirmed_at: tokio_time::Instant,
}

#[derive(Default)]
struct UnreachableWarnings {
    warned_at: HashMap<NodeId, tokio_time::Instant>,
}

impl UnreachableWarnings {
    /// The peers among `unreachable` not warned about for an interval; any other peer is
    /// forgotten, so its next failure is warned about at once.
    fn due(&mut self, unreachable: &[NodeId], now: tokio_time::Instant) -> Vec<NodeId> {
        self.warned_at.retain(|peer, _| unreachable.contains(peer));
        unreachable
            .iter()
            .copied()
            .filter(|peer| {
                let due = self
                    .warned_at
                    .get(peer)
                    .is_none_or(|warned_at| now >= *warned_at + UNREACHABLE_WARNING_INTERVAL);
                if due {
                    self.warned_at.insert(*peer, now);
                }
                due
            })
            .collect()
    }
}

async fn membership_loop(endpoint: &'static EndpointInner) {
    let mut provider = (endpoint.config.downing_provider)();
    let mut ticks = interval(endpoint.config.heartbeat_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticks.tick().await;
        if endpoint.downed() {
            break;
        }

        match catch_unwind(AssertUnwindSafe(|| {
            membership_tick(endpoint, provider.as_mut())
        })) {
            Ok(ControlFlow::Continue(())) => {}

            Ok(ControlFlow::Break(())) => break,

            Err(panic) => {
                error!(panic = %PanicPayload(panic.as_ref()), "membership tick panicked");
                provider = (endpoint.config.downing_provider)();
            }
        }
    }
}

fn membership_tick(
    endpoint: &'static EndpointInner,
    provider: &mut dyn DowningProvider,
) -> ControlFlow<()> {
    {
        endpoint.gossip_tick();

        let Updated {
            result: (outbound, unreachable),
            members,
            disconnected,
        } = endpoint.update(|membership, reachability| {
            let mut unreachable = Vec::new();
            for peer in membership.up_peers() {
                if endpoint.liveness.is_available(peer) {
                    if reachability.observe(peer, true) {
                        debug!(%peer, "member reachable again");
                    }
                } else {
                    reachability.observe(peer, false);
                    unreachable.push(peer);
                }
            }
            (
                reachability.take_outbound(&membership.up_nodes()),
                unreachable,
            )
        });
        endpoint.push_reachability(outbound);
        let due =
            lock(&endpoint.unreachable_warnings).due(&unreachable, tokio_time::Instant::now());
        for peer in due {
            warn!(
                %peer,
                remedy = "cluster::down one end unless the link heals",
                "member directly unreachable"
            );
        }

        match provider.down(&members, Disconnected::new(&disconnected), Instant::now()) {
            Downing::Members(members) => {
                if members.iter().any(|member| member.node() == endpoint.node) {
                    warn!("self down, this node named by the downing provider");
                    endpoint.self_down();
                    return ControlFlow::Break(());
                }
                for member in members {
                    warn!(%member, "node death, downed by the downing provider");
                    endpoint.node_death(member.node());
                }
            }

            Downing::SelfDown => {
                warn!("self down, decided by the downing provider");
                endpoint.self_down();
                return ControlFlow::Break(());
            }
        }

        endpoint.update(|membership, _| membership.sweep(Instant::now()));
        endpoint.end_overdue_departures(tokio_time::Instant::now());
        endpoint.sweep_foreign_clusters(tokio_time::Instant::now());
        endpoint.untrack_idle_peers();
        reply::send_cancellations(endpoint, endpoint.pending_replies.release_abandoned());
    }
    ControlFlow::Continue(())
}

/// The tick covers a lane which did not exist or was disconnected when a change was published.
async fn replication_loop(endpoint: &'static EndpointInner) {
    let mut changes = endpoint.registry.changes();
    let mut ticks = interval(endpoint.config.watch_refresh_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            changed = changes.changed() => {
                if changed.is_err() {
                    break;
                }
            }

            _ = ticks.tick() => {}
        }
        if endpoint.downed() {
            break;
        }

        endpoint.apply_own_registrations();
        let snapshot = endpoint.registrations_snapshot();
        for peer in endpoint.membership.up_peers() {
            endpoint.offer_snapshot(peer, &snapshot);
        }
    }
}

/// Re-asserting a watch compensates a `Terminated` frame lost with its connection; like gossip it
/// skips a disconnected lane, whose uncounted queues would otherwise grow.
async fn watch_refresh_loop(endpoint: &'static EndpointInner) {
    let mut ticks = interval(endpoint.config.watch_refresh_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticks.tick().await;
        if endpoint.downed() {
            break;
        }

        for peer in endpoint.watchers.peers() {
            if endpoint.membership.is_down(peer)
                || endpoint.lane_connected(peer.addr()) == Some(false)
            {
                continue;
            }

            for (target, watcher) in endpoint.watchers.watches(peer) {
                if let Err(error) = endpoint.send(peer, Frame::Watch { target, watcher }) {
                    debug!(%peer, actor_id = %target, %error, "cannot refresh remote watch");
                }
            }
        }
    }
}

fn refusal_backoff(strikes: u32) -> Duration {
    let doublings = strikes.saturating_sub(1).min(u32::BITS - 1);
    REFUSAL_BACKOFF
        .saturating_mul(1 << doublings)
        .min(MAX_REFUSAL_BACKOFF)
}

#[cfg(test)]
mod tests {
    use crate::cluster::{
        ClusterState,
        codec::{Codec, DecodeVisitor, Postcard},
        downing::{Disconnected, Downing, DowningProvider},
        endpoint::{
            Coordinator, EndpointConfig, EndpointInner, FULL_GOSSIP_EVERY, ForeignClusters,
            FormError, InvalidEndpointConfig, Lifecycle, MAX_CHANGE_CAPACITY, MAX_REFUSAL_BACKOFF,
            REFUSAL_BACKOFF, UNREACHABLE_WARNING_INTERVAL, UnreachableWarnings, membership_loop,
            validate,
        },
        failure::{Deadline, DeadlineFailureDetector},
        frame::{Frame, Handshake, RefusalReason},
        lanes::{Drain, LaneError},
        leave::LeaveError,
        membership::{self, Member, MemberState},
        node::{ClusterId, NodeId},
        peer::{ConnectError, DialRequest},
        reachability::{self, Reachability, WireReachability},
        receptionist::{PublishedChange, Settlement, SnapshotSource},
        state::{BrokenLink, Change, ForeignCluster},
    };
    #[cfg(feature = "serde")]
    use crate::cluster::{downing::KeepMajority, failure::PhiAccrual};
    use semver::Version;
    use std::{
        collections::{BTreeSet, HashSet},
        net::SocketAddr,
        num::NonZeroUsize,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use tokio::{
        task,
        time::{self as tokio_time, sleep, timeout},
    };

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn pinned_addr(coordinator: &Coordinator) -> Option<SocketAddr> {
        coordinator.pinned.map(|(addr, _)| addr)
    }

    /// Once admission has classified an address as incompatible, later frames fail locally
    /// instead of opening one new lane (and therefore one new dial) per message.
    #[test]
    fn a_refused_address_is_not_redialed_per_frame() {
        let local = addr(1);
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(local));
        endpoint.update(|membership, _| membership.add_up(peer));
        endpoint.refuse(peer.addr());

        for _ in 0..2 {
            assert!(matches!(
                endpoint.send(peer, gossip()),
                Err(LaneError::NodeUnreachable(node)) if node == peer
            ));
        }
        assert!(dial_requests.is_empty());

        endpoint.unrefuse(peer.addr());
        endpoint
            .send(peer, gossip())
            .expect("an address can be retried after it is explicitly unrefused");
        assert_eq!(dial_requests.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_expires_after_its_backoff() {
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));

        assert_eq!(endpoint.refuse(peer.addr()), REFUSAL_BACKOFF);
        sleep(REFUSAL_BACKOFF - Duration::from_millis(1)).await;
        assert!(matches!(
            endpoint.send(peer, gossip()),
            Err(LaneError::NodeUnreachable(node)) if node == peer
        ));
        assert!(dial_requests.is_empty());

        sleep(Duration::from_millis(1)).await;
        endpoint
            .send(peer, gossip())
            .expect("an expired refusal dials again");
        assert_eq!(dial_requests.len(), 1);
    }

    /// Every refusal before a lift doubles the previous backoff, up to the cap.
    #[tokio::test(start_paused = true)]
    async fn a_repeated_refusal_doubles_its_backoff_up_to_the_cap() {
        let peer = addr(2);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        let mut backoffs = Vec::new();
        for _ in 0..9 {
            let backoff = endpoint.refuse(peer);
            backoffs.push(backoff.as_secs());
            sleep(backoff - Duration::from_millis(1)).await;
            assert!(endpoint.is_refused(peer));
            sleep(Duration::from_millis(1)).await;
            assert!(!endpoint.is_refused(peer));
        }
        assert_eq!(backoffs, [10, 20, 40, 80, 160, 320, 600, 600, 600]);

        endpoint.unrefuse(peer);
        assert_eq!(endpoint.refuse(peer), REFUSAL_BACKOFF);
    }

    /// A refusal expired for longer than the cap is forgotten, so the next one starts over.
    #[tokio::test(start_paused = true)]
    async fn a_long_expired_refusal_is_forgotten() {
        let peer = addr(2);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        endpoint.refuse(peer);
        sleep(REFUSAL_BACKOFF + MAX_REFUSAL_BACKOFF - Duration::from_millis(1)).await;
        assert_eq!(endpoint.refuse(peer), 2 * REFUSAL_BACKOFF);

        sleep(2 * REFUSAL_BACKOFF + MAX_REFUSAL_BACKOFF).await;
        assert_eq!(endpoint.refuse(peer), REFUSAL_BACKOFF);
    }

    /// A gossiped Up member at a refused address proves a tellus node is there, so the refusal is
    /// lifted at once rather than after its backoff.
    #[tokio::test(start_paused = true)]
    async fn a_gossiped_up_member_lifts_a_refusal() {
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.refuse(peer.addr());

        membership::on_gossip(endpoint, &[Member::new(peer, MemberState::Up)]);

        assert!(!endpoint.is_refused(peer.addr()));
        endpoint
            .send(peer, gossip())
            .expect("a gossiped Up member is dialed at once");
        assert!(!dial_requests.is_empty());
    }

    /// A broken link between two members which still reach each other through this node is
    /// published with the state, and cleared once its observer reports the link healed.
    #[test]
    fn a_broken_link_is_published_and_cleared_when_it_heals() {
        let observer = NodeId::new(addr(2));
        let subject = NodeId::new(addr(3));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        for member in [observer, subject] {
            endpoint.update(|membership, _| membership.add_up(member));
        }
        let up = HashSet::from([endpoint.node(), observer, subject]);
        let record = |version, reachable| WireReachability {
            observer,
            subject,
            version,
            reachable,
        };
        let broken_links = || endpoint.state_rx().borrow().broken_links().to_vec();
        let version = || endpoint.state_rx().borrow().version();
        assert!(broken_links().is_empty());

        let before = version();
        endpoint.update(|_, reachability| reachability.merge(&[record(1, false)], &up));
        assert_eq!(
            broken_links(),
            [BrokenLink::new(
                Member::new(observer, MemberState::Up),
                Member::new(subject, MemberState::Up),
            )]
        );
        assert!(endpoint.state_rx().borrow().unreachable().is_empty());
        assert_eq!(version(), before + 1);

        endpoint.update(|_, reachability| reachability.merge(&[record(2, true)], &up));
        assert!(broken_links().is_empty());
        assert_eq!(version(), before + 2);
    }

    /// A push carries a statement once, so a node which was disconnected while it went out has no
    /// second chance at it; connection setup hence replays the whole table. Both an own and a
    /// relayed statement must survive that replay, else a reconnecting member keeps deriving a
    /// component the cluster has already left behind.
    #[test]
    fn a_reconnect_snapshot_transfers_established_observations() {
        let relay = NodeId::new(addr(2));
        let subject = NodeId::new(addr(3));
        let reconnecting_node = NodeId::new(addr(4));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let members = [endpoint.node(), relay, subject, reconnecting_node];
        for member in members {
            endpoint.update(|membership, _| membership.add_up(member));
        }
        let up = members.iter().copied().collect::<HashSet<_>>();

        endpoint.update(|_, reachability| {
            reachability.observe(subject, false);
            reachability.merge(
                &[WireReachability {
                    observer: relay,
                    subject,
                    version: 1,
                    reachable: false,
                }],
                &up,
            );
        });

        let observations = endpoint
            .reachability_snapshot_frames()
            .into_iter()
            .flat_map(|frame| match frame {
                Frame::Reachability { observations } => observations,
                frame => panic!("a reachability snapshot holds no {frame:?}"),
            })
            .collect::<Vec<_>>();

        // The reconnecting member saw none of the deltas and reaches the subject through either of
        // the others until the snapshot arrives.
        let reconnecting = Reachability::new(reconnecting_node);
        reconnecting.observe(subject, false);
        let all = members
            .iter()
            .map(|node| membership::Member::new(*node, membership::MemberState::Up))
            .collect::<Vec<_>>();
        assert!(reconnecting.unreachable_members(&all).is_empty());

        reconnecting.merge(&observations, &up);

        let outside = reconnecting
            .unreachable_members(&all)
            .iter()
            .map(|(member, _)| member.node())
            .collect::<Vec<_>>();
        assert_eq!(outside, vec![subject]);
    }

    /// A push skips a member whose lane is known disconnected, so a partition cannot turn every
    /// observation into another dial at the peers which just went silent. A member with no lane at
    /// all is still dialed, exactly as gossip does it.
    #[test]
    fn a_reachability_push_skips_a_disconnected_lane() {
        let local = addr(1);
        let silent = NodeId::new(addr(2));
        let fresh = NodeId::new(addr(3));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(local));
        endpoint.update(|membership, _| membership.add_up(silent));
        endpoint.update(|membership, _| membership.add_up(fresh));

        // A lane is disconnected until its dial completes, which this one never does.
        endpoint.send(silent, gossip()).expect("the lane is opened");
        let silent_lane = dial_requests.recv().expect("the silent peer is dialed");
        assert_eq!(endpoint.lane_connected(silent.addr()), Some(false));

        endpoint.push_reachability(vec![WireReachability {
            observer: NodeId::new(local),
            subject: silent,
            version: 1,
            reachable: false,
        }]);

        assert_eq!(silent_lane.control_rx.len(), 1);
        assert_eq!(dial_requests.len(), 1);
    }

    /// The value names this node, so its own entry is found by identity, not by address.
    #[test]
    fn a_state_knows_its_own_entry() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();
        let state = state_rx.borrow().clone();
        assert_eq!(state.this_addr(), addr(1));
        assert_eq!(state.this_incarnation(), endpoint.node().incarnation());
        assert_eq!(
            state.this_member().map(Member::state),
            Some(MemberState::Up)
        );

        endpoint.update(|membership, _| membership.down(endpoint.node(), Instant::now()));
        let state = state_rx.borrow().clone();
        assert_eq!(
            state.this_member().map(Member::state),
            Some(MemberState::Down)
        );
    }

    /// The published value is what `members()` answers, one version per observable change and
    /// none for a change nothing can observe.
    #[test]
    fn an_update_publishes_what_members_answers() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();
        assert_eq!(state_rx.borrow().version(), 0);

        assert!(
            endpoint
                .update(|membership, _| membership.add_up(peer))
                .result
        );
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 1);
        assert_eq!(state.members(), endpoint.membership().members());
        assert!(state.unreachable().is_empty());

        assert!(
            !endpoint
                .update(|membership, _| membership.add_up(peer))
                .result
        );
        assert_eq!(state_rx.borrow().version(), 1);
    }

    /// An unreachable member is published as such and leaves the set with its death, which is
    /// one version, since the Down and the derivation are published together.
    #[test]
    fn an_unreachable_member_is_published_and_cleared_by_its_death() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let state_rx = endpoint.state_rx();

        endpoint.update(|_, reachability| reachability.observe(peer, false));
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 2);
        assert_eq!(state.unreachable(), [Member::new(peer, MemberState::Up)]);

        endpoint.node_death(peer);
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 3);
        assert!(state.unreachable().is_empty());
        assert_eq!(state_of(&state, peer), Some(MemberState::Down));
    }

    /// A gossiped supersession is one value in which the superseded incarnation is already Down:
    /// no value ever holds two Up incarnations at one address.
    #[test]
    fn a_gossiped_supersession_publishes_one_consistent_value() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(older));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(endpoint, &[Member::new(younger, MemberState::Up)]);

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(
            state.up().filter(|member| member.addr() == shared).count(),
            1
        );
        assert_eq!(state_of(&state, older), Some(MemberState::Down));
        assert_eq!(state_of(&state, younger), Some(MemberState::Up));
    }

    /// A self-down decided by the same merge neither skips the other outcomes nor publishes
    /// twice: one value shows the superseded peer Down, the successor Up and this node Down.
    #[test]
    fn a_self_down_merge_still_applies_every_outcome() {
        let shared = addr(2);
        let older = NodeId::new(shared);
        let younger = NodeId::new(shared);
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let successor = NodeId::new(addr(1));
        endpoint.update(|membership, _| membership.add_up(older));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(
            endpoint,
            &[
                Member::new(younger, MemberState::Up),
                Member::new(successor, MemberState::Down),
            ],
        );

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(
            state.up().filter(|member| member.addr() == shared).count(),
            1
        );
        assert_eq!(state_of(&state, older), Some(MemberState::Down));
        assert_eq!(state_of(&state, younger), Some(MemberState::Up));
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
    }

    /// A self-down's fence covers this node's own observations, so pruning them can make a
    /// survivor reachable again; that happens inside the merge's update, never as a second value.
    #[test]
    fn a_self_down_merge_prunes_before_publishing() {
        let survivor = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let successor = NodeId::new(addr(1));
        endpoint.update(|membership, _| membership.add_up(survivor));
        endpoint.update(|_, reachability| reachability.observe(survivor, false));
        let state_rx = endpoint.state_rx();
        let state = state_rx.borrow().clone();
        assert_eq!(
            state.unreachable(),
            [Member::new(survivor, MemberState::Up)]
        );
        let version = state.version();

        membership::on_gossip(endpoint, &[Member::new(successor, MemberState::Down)]);

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert!(state.unreachable().is_empty());
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
        assert_eq!(state_of(&state, survivor), Some(MemberState::Up));
    }

    /// A gossiped Down is applied in the same value as the rest of the merge.
    #[test]
    fn a_gossiped_down_is_applied_in_the_same_value() {
        let peer = NodeId::new(addr(2));
        let other = NodeId::new(addr(3));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        membership::on_gossip(
            endpoint,
            &[
                Member::new(peer, MemberState::Down),
                Member::new(other, MemberState::Up),
            ],
        );

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(state_of(&state, peer), Some(MemberState::Down));
        assert_eq!(state_of(&state, other), Some(MemberState::Up));
    }

    /// Parked evidence naming a member added by the same update derives that member unreachable
    /// at once, not one tick later after a value which showed it reachable.
    #[test]
    fn parked_evidence_is_promoted_before_deriving() {
        let relay = NodeId::new(addr(2));
        let newcomer = NodeId::new(addr(3));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(relay));
        let up = HashSet::from([endpoint.node(), relay]);
        endpoint.update(|_, reachability| {
            reachability.observe(newcomer, false);
            reachability.merge(
                &[WireReachability {
                    observer: relay,
                    subject: newcomer,
                    version: 1,
                    reachable: false,
                }],
                &up,
            );
        });
        let state_rx = endpoint.state_rx();
        assert!(state_rx.borrow().unreachable().is_empty());

        endpoint.update(|membership, _| membership.add_up(newcomer));

        let state = state_rx.borrow().clone();
        assert_eq!(
            state.unreachable(),
            [Member::new(newcomer, MemberState::Up)]
        );
    }

    /// A self-down publishes this node's own Down, the last value before the loops end.
    #[test]
    fn a_self_down_publishes_its_own_down() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let state_rx = endpoint.state_rx();

        endpoint.self_down();

        assert!(endpoint.downed());
        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), 1);
        assert_eq!(state_of(&state, endpoint.node()), Some(MemberState::Down));
    }

    /// A sweep publishes the removal, since the swept entry leaves what `members()` answers.
    #[test]
    fn a_sweep_publishes_the_removal() {
        let peer = NodeId::new(addr(2));
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        endpoint.node_death(peer);
        let state_rx = endpoint.state_rx();
        let version = state_rx.borrow().version();

        endpoint.update(|membership, _| {
            membership.sweep(Instant::now() + endpoint.config.down_retention)
        });

        let state = state_rx.borrow().clone();
        assert_eq!(state.version(), version + 1);
        assert_eq!(state_of(&state, peer), None);
    }

    /// Versions count observable changes exactly, whichever threads make them.
    #[test]
    fn versions_count_changes_across_threads() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        let updates = (2..12)
            .map(|port| {
                thread::spawn(move || {
                    endpoint.update(|membership, _| membership.add_up(NodeId::new(addr(port))));
                })
            })
            .collect::<Vec<_>>();
        for update in updates {
            update.join().expect("an update does not panic");
        }

        let state = endpoint.state_rx().borrow().clone();
        assert_eq!(state.version(), 10);
        assert_eq!(state.members().len(), 11);
    }

    /// A re-entrant update panics instead of deadlocking, and the check resets for the next one.
    #[test]
    fn a_reentrant_update_panics_and_resets() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));

        let reentrant = catch_unwind(AssertUnwindSafe(|| {
            endpoint.update(|_, _| {
                endpoint.update(|_, _| ());
            });
        }));

        assert!(reentrant.is_err());
        endpoint.update(|_, _| ());
    }

    /// A peer which already holds the current member list gets an empty frame, still a
    /// heartbeat, and the full list again once the list changes.
    #[test]
    fn an_unchanged_member_list_is_gossiped_as_an_empty_frame() {
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let lane = connected_lane(endpoint, &dial_requests, peer);

        endpoint.gossip_tick();
        assert_eq!(gossip_members(&lane), [endpoint.membership().members()]);

        endpoint.gossip_tick();
        assert_eq!(gossip_members(&lane), [Vec::new()]);

        endpoint.update(|membership, _| membership.add_up(NodeId::new(addr(3))));
        endpoint.gossip_tick();
        let members = endpoint.membership().members();
        assert_eq!(members.len(), 3);
        assert_eq!(gossip_members(&lane), [members]);

        endpoint.gossip_tick();
        assert_eq!(gossip_members(&lane), [Vec::new()]);
    }

    /// Every [FULL_GOSSIP_EVERY]th tick sends the full list, even to a peer holding it already.
    #[test]
    fn every_tenth_tick_gossips_the_full_member_list() {
        let peer = NodeId::new(addr(2));
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.update(|membership, _| membership.add_up(peer));
        let lane = connected_lane(endpoint, &dial_requests, peer);

        let full_ticks = (0..=2 * FULL_GOSSIP_EVERY)
            .filter(|_| {
                endpoint.gossip_tick();
                let gossip = gossip_members(&lane);
                assert_eq!(gossip.len(), 1);
                !gossip[0].is_empty()
            })
            .collect::<Vec<_>>();
        assert_eq!(full_ticks, [0, FULL_GOSSIP_EVERY, 2 * FULL_GOSSIP_EVERY]);
    }

    fn state_of(state: &ClusterState, node: NodeId) -> Option<MemberState> {
        state
            .members()
            .iter()
            .find(|member| member.node() == node)
            .map(Member::state)
    }

    fn gossip() -> Frame<'static> {
        Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
    }

    /// Opens the lane to `peer` with an empty frame, drained again, and marks it connected.
    fn connected_lane(
        endpoint: &EndpointInner,
        dial_requests: &flume::Receiver<DialRequest>,
        peer: NodeId,
    ) -> DialRequest {
        endpoint.send(peer, gossip()).expect("the lane opens");
        let lane = dial_requests.try_recv().expect("the lane is dialed");
        endpoint.bind_lane(peer.addr(), lane.lane_id, peer);
        assert_eq!(gossip_members(&lane), [Vec::new()]);
        lane
    }

    /// The members of every Gossip frame queued on the lane since the last call.
    fn gossip_members(lane: &DialRequest) -> Vec<Vec<Member>> {
        lane.control_rx
            .drain()
            .filter_map(|frame| match frame {
                Frame::Gossip { members, .. } => Some(members),
                _ => None,
            })
            .collect()
    }

    /// A frame size below one snapshot chunk is refused at start, since a connection setup could
    /// not send its member snapshot without the receiver refusing the frame. A zero interval is
    /// refused too: both intervals drive a Tokio interval, which panics on a zero period.
    #[test]
    fn an_invalid_config_is_refused() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let required_min = Handshake::MAX_ENCODED_LEN
            .max(membership::MIN_FRAME_SIZE)
            .max(reachability::MIN_FRAME_SIZE);
        let mut config = EndpointConfig::new(addr);
        config.max_frame_size =
            NonZeroUsize::new(required_min - 1).expect("the minimum is above one");

        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::MaxFrameSizeTooSmall { max_frame_size, min })
                if max_frame_size == required_min - 1
                    && min == required_min
        ));

        config.max_frame_size = NonZeroUsize::new(required_min).expect("the minimum is not zero");
        assert!(validate(&config).is_ok());

        config.heartbeat_interval = Duration::ZERO;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::ZeroHeartbeatInterval)
        ));

        config.heartbeat_interval = EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL;
        config.watch_refresh_interval = Duration::ZERO;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::ZeroWatchRefreshInterval)
        ));

        config.watch_refresh_interval = EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL;
        for rejected in [3, 1 << (usize::BITS - 1)] {
            config.change_capacity = NonZeroUsize::new(rejected).expect("not zero");
            assert!(matches!(
                validate(&config),
                Err(InvalidEndpointConfig::ChangeCapacity { change_capacity, max })
                    if change_capacity == rejected && max == MAX_CHANGE_CAPACITY
            ));
        }
        for accepted in [1, 1_024] {
            config.change_capacity = NonZeroUsize::new(accepted).expect("not zero");
            assert!(validate(&config).is_ok());
        }
    }

    /// An unspecified IP or a zero port cannot be dialed by other nodes, yet the advertised
    /// address becomes this node's identity, so such an address is refused at start.
    #[test]
    fn an_undialable_advertised_addr_is_refused() {
        for unspecified in ["0.0.0.0:2552", "[::]:2552", "0.0.0.0:0"] {
            let advertised = unspecified.parse().expect("valid address");
            assert!(matches!(
                validate(&EndpointConfig::new(advertised)),
                Err(InvalidEndpointConfig::UnspecifiedAdvertisedIp { advertised_addr })
                    if advertised_addr == advertised
            ));
        }

        for zero_port in ["127.0.0.1:0", "[::1]:0"] {
            let advertised = zero_port.parse().expect("valid address");
            assert!(matches!(
                validate(&EndpointConfig::new(advertised)),
                Err(InvalidEndpointConfig::ZeroAdvertisedPort { advertised_addr })
                    if advertised_addr == advertised
            ));
        }

        assert!(validate(&EndpointConfig::new(addr(2552))).is_ok());
    }

    struct PanickingDowning(Arc<AtomicUsize>);

    impl DowningProvider for PanickingDowning {
        fn down(&mut self, _: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("downing provider bug");
        }
    }

    struct DowningThisNode;

    impl DowningProvider for DowningThisNode {
        fn down(&mut self, members: &[Member], _: Disconnected<'_>, _: Instant) -> Downing {
            Downing::Members(members.to_vec())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_downing_provider_is_rebuilt() {
        let (polls, built) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut config = EndpointConfig::new(addr(1));
        config.downing_provider = Arc::new({
            let (polls, built) = (polls.clone(), built.clone());
            move || {
                built.fetch_add(1, Ordering::Relaxed);
                Box::new(PanickingDowning(polls.clone()))
            }
        });
        let heartbeat_interval = config.heartbeat_interval;
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);

        let looping = task::spawn(membership_loop(endpoint));
        for _ in 0..3 {
            sleep(heartbeat_interval).await;
        }

        assert!(polls.load(Ordering::Relaxed) >= 3);
        assert!(built.load(Ordering::Relaxed) >= 3);
        assert!(!looping.is_finished());
        looping.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_provider_naming_this_node_self_downs_it() {
        let mut config = EndpointConfig::new(addr(1));
        config.downing_provider = Arc::new(|| Box::new(DowningThisNode));
        let heartbeat_interval = config.heartbeat_interval;
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);
        endpoint.form_cluster().expect("a fresh endpoint forms");

        let looping = task::spawn(membership_loop(endpoint));
        sleep(heartbeat_interval * 2).await;

        assert!(endpoint.downed());
        assert!(looping.is_finished());
    }

    #[tokio::test]
    async fn a_second_leave_waits_for_the_first_departure() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint
            .send(NodeId::new(addr(2)), gossip())
            .expect("a lane opens");
        let dial = dial_requests.try_recv().expect("the lane is dialed");

        let first = task::spawn(endpoint.leave());
        while !endpoint.leaving() {
            task::yield_now().await;
        }

        assert!(
            timeout(Duration::from_millis(100), endpoint.leave())
                .await
                .is_err()
        );
        assert!(!endpoint.downed());

        let _ = dial.done_tx.send(Drain::Acknowledged);
        let departure = first
            .await
            .expect("the first leave completes")
            .expect("a departure");
        assert_eq!(departure.acknowledged(), [addr(2)]);
        assert!(departure.is_acknowledged());
        assert!(endpoint.downed());
        assert_eq!(
            endpoint.leave().await.expect("the same departure"),
            departure
        );
        assert_eq!(
            state_of(&endpoint.state_rx().borrow(), endpoint.node()),
            Some(MemberState::Left)
        );
    }

    /// A lane which ends unacknowledged and one which never answers are reported apart, and the
    /// departure still ends at the leave timeout.
    #[tokio::test(start_paused = true)]
    async fn a_departure_reports_how_each_lane_drained() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        for port in [2, 3, 4] {
            endpoint
                .send(NodeId::new(addr(port)), gossip())
                .expect("a lane opens");
        }
        let mut dials = dial_requests.try_iter().collect::<Vec<_>>();
        dials.sort_by_key(|dial| dial.addr);
        let mut dials = dials.into_iter();
        let acknowledged = dials.next().expect("a lane toward port 2");
        let unacknowledged = dials.next().expect("a lane toward port 3");
        let _silent = dials.next().expect("a lane toward port 4");
        let _ = acknowledged.done_tx.send(Drain::Acknowledged);
        drop(unacknowledged);

        let departure = endpoint.leave().await.expect("a departure");

        assert_eq!(departure.acknowledged(), [addr(2)]);
        assert_eq!(departure.unacknowledged(), [addr(3)]);
        assert_eq!(departure.timed_out(), [addr(4)]);
        assert!(!departure.is_acknowledged());
        assert!(endpoint.downed());
    }

    #[tokio::test]
    async fn a_downed_node_cannot_leave() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.self_down();

        assert!(matches!(endpoint.leave().await, Err(LeaveError::Downed)));
        assert_eq!(
            state_of(&endpoint.state_rx().borrow(), endpoint.node()),
            Some(MemberState::Down)
        );
    }

    /// A departure which panics downs and severs this node before any caller sees it fail, and
    /// every caller, a concurrent and a later one included, sees it fail.
    #[tokio::test]
    async fn a_panicking_departure_downs_this_node_for_every_leave() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint
            .send(NodeId::new(addr(2)), gossip())
            .expect("a lane opens");
        let _dial = dial_requests.try_recv().expect("the lane is dialed");
        let sever_rx = endpoint.sever_rx();
        endpoint.arm_departure_panic();

        let concurrent = task::spawn(endpoint.leave());
        let first = endpoint.leave().await;
        let severed = sever_rx.has_changed().expect("the sever sender is alive");
        let downed = endpoint.downed();

        assert!(matches!(first, Err(LeaveError::Incomplete)));
        assert!(downed);
        assert!(severed);
        assert!(matches!(
            concurrent.await.expect("the concurrent leave completes"),
            Err(LeaveError::Incomplete)
        ));
        assert!(matches!(
            endpoint.leave().await,
            Err(LeaveError::Incomplete)
        ));
        assert_eq!(
            state_of(&endpoint.state_rx().borrow(), endpoint.node()),
            Some(MemberState::Down)
        );
    }

    #[tokio::test]
    async fn an_aborted_leave_keeps_the_departure_running() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint
            .send(NodeId::new(addr(2)), gossip())
            .expect("a lane opens");
        let dial = dial_requests.try_recv().expect("the lane is dialed");

        let first = task::spawn(endpoint.leave());
        while !endpoint.leaving() {
            task::yield_now().await;
        }
        let second = task::spawn(endpoint.leave());
        task::yield_now().await;
        first.abort();
        assert!(first.await.is_err_and(|error| error.is_cancelled()));

        assert!(
            timeout(Duration::from_millis(100), endpoint.leave())
                .await
                .is_err()
        );
        assert!(!endpoint.downed());

        let _ = dial.done_tx.send(Drain::Acknowledged);
        second
            .await
            .expect("the second leave completes")
            .expect("a departure");
        assert!(endpoint.downed());
    }

    #[test]
    fn a_detector_too_eager_for_the_intervals_is_refused() {
        let addr = "127.0.0.1:2552".parse().expect("valid address");
        let mut config = EndpointConfig::new(addr);
        assert!(validate(&config).is_ok());

        let too_slow = Duration::from_secs(5);
        config.heartbeat_interval = too_slow;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { silence }) if silence == too_slow
        ));

        config.heartbeat_interval = EndpointConfig::DEFAULT_HEARTBEAT_INTERVAL;
        config.watch_refresh_interval = too_slow;
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { silence }) if silence == too_slow
        ));

        config.watch_refresh_interval = EndpointConfig::DEFAULT_WATCH_REFRESH_INTERVAL;
        let deadline = Deadline::new(Duration::from_millis(500)).expect("not zero");
        config.failure_detector =
            Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::FailureDetectorTooEager { .. })
        ));

        let deadline = Deadline::new(Duration::from_secs(3)).expect("not zero");
        config.failure_detector =
            Arc::new(move || Box::new(DeadlineFailureDetector::new(deadline)));
        assert!(validate(&config).is_ok());
    }

    /// Every version reaches a subscriber which keeps up, each followed by its verdict, and a
    /// change nothing can observe sends nothing.
    #[tokio::test]
    async fn changes_stream_every_version_with_its_verdict() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let mut changes = endpoint.changes();
        let initial = ClusterState::clone(&endpoint.state_rx().borrow());
        assert_eq!(changes.next().await, Change::State(initial));
        assert_eq!(
            changes.next().await,
            Change::Settled(Settlement::new(0, true))
        );

        let first = NodeId::new(addr(2));
        let second = NodeId::new(addr(3));
        endpoint.update(|membership, _| membership.add_up(first));
        endpoint.update(|membership, _| membership.add_up(second));
        endpoint.update(|membership, _| membership.add_up(second));

        for (version, node) in [(1, first), (2, second)] {
            match changes.next().await {
                Change::State(state) => {
                    assert_eq!(state.version(), version);
                    assert_eq!(state_of(&state, node), Some(MemberState::Up));
                }

                other => panic!("expected a state, got {other:?}"),
            }
            assert_eq!(
                changes.next().await,
                Change::Settled(Settlement::new(version, false))
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), changes.next())
                .await
                .is_err()
        );
    }

    /// A borrow of the cluster state watch may be held while asking the receptionist, and updates
    /// keep coming: the receptionist must never wait on the watch while holding its lock, or the
    /// writer-preferring watch lock deadlocks the three of them.
    #[test]
    fn a_borrowed_state_and_a_settlement_query_never_deadlock_an_update() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let consumer = std::thread::spawn(move || {
            let state_rx = endpoint.state_rx();
            for _ in 0..2_000 {
                let borrowed = state_rx.borrow();
                let settlement = endpoint.receptionist().settlement();
                assert!(settlement.version() <= borrowed.version());
            }
        });
        let updater = std::thread::spawn(move || {
            for _ in 0..1_000 {
                let peer = NodeId::new(addr(2));
                endpoint.update(|membership, _| membership.add_up(peer));
                endpoint.update(|membership, _| membership.down(peer, Instant::now()));
            }
        });
        std::thread::spawn(move || {
            consumer.join().expect("the consumer completes");
            updater.join().expect("the updater completes");
            let _ = done_tx.send(());
        });

        assert!(
            done_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
            "the borrowing consumer and the updater deadlocked"
        );
    }

    /// Probabilistic: snapshots racing updates. A verdict for N after the state for N+1 would
    /// betray a verdict sent after the state's channel send.
    #[test]
    fn a_verdict_never_trails_the_next_state() {
        let mut config = EndpointConfig::new(addr(1));
        config.change_capacity = NonZeroUsize::new(1 << 16).expect("not zero");
        let (endpoint, _dial_requests, _join_requests) = EndpointInner::for_tests(config);
        let mut published = endpoint.published_changes();

        let snapshots = std::thread::spawn(move || {
            for _ in 0..2_000 {
                endpoint.receptionist().snapshot();
            }
        });
        for _ in 0..500 {
            let peer = NodeId::new(addr(2));
            endpoint.update(|membership, _| membership.add_up(peer));
            endpoint.update(|membership, _| membership.down(peer, Instant::now()));
        }
        snapshots.join().expect("the snapshots complete");

        let mut last_state_version = 0;
        while let Ok(change) = published.try_recv() {
            match change {
                PublishedChange::State(state) => last_state_version = state.version(),
                PublishedChange::Settled { settlement, .. } => {
                    assert_eq!(settlement.version(), last_state_version);
                }
            }
        }
        assert_eq!(last_state_version, 1_000);
    }

    /// The documented config form, which a config file provides and which the `DEFAULT_*`
    /// constants back: the ordinary fields carry what was given, and the three pluggable ones are
    /// told apart by what they do, since they are trait objects.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<EndpointConfig>(
            r#"{
                "advertised_addr": "127.0.0.1:2552",
                "outbound_capacity": 128,
                "max_streams_per_peer": 4,
                "max_frame_size": 65536,
                "codec": "postcard",
                "app_version": "1.4.0-rc.1",
                "heartbeat_interval": "500ms",
                "watch_refresh_interval": "1s",
                "failure_detector": { "deadline": "3s" },
                "downing_provider": { "down_after_deadline": { "after": "2s" } },
                "down_retention": "1m",
                "down_watermarks": 64,
                "leave_timeout": "10s",
                "reconnect_backoff": { "min": "100ms", "max": "1s" },
                "max_connect_attempts": 3,
                "ambiguous_join_bound": "10s",
                "change_capacity": 64
            }"#,
        )
        .expect("the documented config form deserializes");

        assert_eq!(config.advertised_addr, addr(2552));
        assert_eq!(config.outbound_capacity.get(), 128);
        assert_eq!(config.max_streams_per_peer.get(), 4);
        assert_eq!(config.max_frame_size.get(), 65_536);
        assert_eq!(
            config.app_version,
            Some("1.4.0-rc.1".parse().expect("a semantic version"))
        );
        assert_eq!(config.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(config.watch_refresh_interval, Duration::from_secs(1));
        assert_eq!(config.down_retention, Duration::from_secs(60));
        assert_eq!(config.down_watermarks.get(), 64);
        assert_eq!(config.leave_timeout, Duration::from_secs(10));
        assert_eq!(config.reconnect_backoff.min(), Duration::from_millis(100));
        assert_eq!(config.reconnect_backoff.max(), Duration::from_secs(1));
        assert_eq!(config.max_connect_attempts.get(), 3);
        assert_eq!(config.ambiguous_join_bound, Duration::from_secs(10));
        assert_eq!(config.change_capacity.get(), 64);

        let message = "hello".to_string();
        assert_eq!(
            config.codec.encode(&message).expect("encodes"),
            Postcard.encode(&message).expect("encodes")
        );

        let mut detector = (config.failure_detector)();
        let now = Instant::now();
        detector.record_heartbeat(now);
        assert!(detector.is_available(now + Duration::from_secs(3)));
        assert!(!detector.is_available(now + Duration::from_secs(4)));

        let member = Member::new(NodeId::new(addr(2553)), MemberState::Up);
        let since = Instant::now();
        assert_eq!(
            (config.downing_provider)().down(
                &[member],
                Disconnected::new(&[(member, since)]),
                since + Duration::from_secs(2)
            ),
            Downing::Members(vec![member])
        );
    }

    /// Everything but the advertised address is optional and falls back to what
    /// [EndpointConfig::new] installs, so a config file names only what it changes.
    #[cfg(feature = "serde")]
    #[test]
    fn an_omitted_field_falls_back_to_its_default() {
        let config =
            serde_json::from_str::<EndpointConfig>(r#"{ "advertised_addr": "127.0.0.1:2552" }"#)
                .expect("the advertised address alone deserializes");
        let defaults = EndpointConfig::new(addr(2552));

        assert_eq!(config.advertised_addr, defaults.advertised_addr);
        assert_eq!(config.outbound_capacity, defaults.outbound_capacity);
        assert_eq!(config.max_streams_per_peer, defaults.max_streams_per_peer);
        assert_eq!(config.max_frame_size, defaults.max_frame_size);
        assert_eq!(config.app_version, None);
        assert_eq!(config.heartbeat_interval, defaults.heartbeat_interval);
        assert_eq!(
            config.watch_refresh_interval,
            defaults.watch_refresh_interval
        );
        assert_eq!(config.down_retention, defaults.down_retention);
        assert_eq!(config.down_watermarks, defaults.down_watermarks);
        assert_eq!(config.leave_timeout, defaults.leave_timeout);
        assert_eq!(
            config.reconnect_backoff.min(),
            defaults.reconnect_backoff.min()
        );
        assert_eq!(
            config.reconnect_backoff.max(),
            defaults.reconnect_backoff.max()
        );
        assert_eq!(config.max_connect_attempts, defaults.max_connect_attempts);
        assert_eq!(config.ambiguous_join_bound, defaults.ambiguous_join_bound);
        assert_eq!(config.change_capacity, defaults.change_capacity);

        let message = "hello".to_string();
        assert_eq!(
            config.codec.encode(&message).expect("encodes"),
            Postcard.encode(&message).expect("encodes")
        );

        let mut detector = (config.failure_detector)();
        let now = Instant::now();
        detector.record_heartbeat(now);
        assert!(
            detector.is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE),
            "the default detector is phi accrual, falling back to its warmup deadline"
        );
        assert!(
            !detector
                .is_available(now + PhiAccrual::DEFAULT_WARMUP_DEADLINE + Duration::from_secs(1))
        );

        let members = [
            Member::new(NodeId::new(addr(2553)), MemberState::Up),
            Member::new(NodeId::new(addr(2554)), MemberState::Up),
            Member::new(NodeId::new(addr(2555)), MemberState::Up),
        ];
        let since = Instant::now();
        let disconnected = [(members[1], since), (members[2], since)];
        assert_eq!(
            (config.downing_provider)().down(
                &members,
                Disconnected::new(&disconnected),
                since + KeepMajority::DEFAULT_AFTER
            ),
            Downing::SelfDown,
            "the default provider is keep majority, self downing the minority side"
        );
    }

    /// Deserialization goes through the same validation as [start_endpoint], so an invalid config
    /// cannot come out of a config file either; a misspelled key is an error rather than a
    /// silently applied default.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_validates_the_config() {
        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "heartbeat_interval": "0s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "watch_refresh_interval": "0s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "max_frame_size": 1 }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "app_version": "latest" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "leave_timeuot": "10s" }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "outbound_capacity": 128 }"#;
        assert!(
            serde_json::from_str::<EndpointConfig>(json).is_err(),
            "the advertised address is required"
        );
    }

    /// An advertised address with an unspecified IP or a zero port is refused from a config file
    /// as well.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_refuses_an_undialable_advertised_addr() {
        for json in [
            r#"{ "advertised_addr": "0.0.0.0:2552" }"#,
            r#"{ "advertised_addr": "[::]:2552" }"#,
        ] {
            let error = serde_json::from_str::<EndpointConfig>(json)
                .expect_err("an unspecified advertised IP is refused");
            assert!(error.to_string().contains("unspecified IP"), "{error}");
        }

        let json = r#"{ "advertised_addr": "127.0.0.1:0" }"#;
        let error = serde_json::from_str::<EndpointConfig>(json)
            .expect_err("a zero advertised port is refused");
        assert!(error.to_string().contains("zero port"), "{error}");
    }

    /// Zero connection attempts would give up an address before dialing it at all, so the type
    /// of `max_connect_attempts` makes zero unrepresentable, from a config file too.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_refuses_zero_max_connect_attempts() {
        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "max_connect_attempts": 0 }"#;
        assert!(serde_json::from_str::<EndpointConfig>(json).is_err());

        let json = r#"{ "advertised_addr": "127.0.0.1:2552", "max_connect_attempts": 1 }"#;
        let config =
            serde_json::from_str::<EndpointConfig>(json).expect("one connection attempt is valid");
        assert_eq!(config.max_connect_attempts.get(), 1);
    }

    /// Forming is a decision, so it is taken once and only by an endpoint which is nothing yet.
    #[test]
    fn an_endpoint_forms_once() {
        let mut coordinator = Coordinator::default();

        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Ok(Lifecycle::Formed)
        ));
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::AlreadyFormed)
        ));
    }

    /// A join attempt which has sent its handshake may already have been admitted somewhere,
    /// with nothing local recording it yet, so forming waits for it to resolve.
    #[test]
    fn a_join_in_flight_postpones_forming() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            ..Default::default()
        };

        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::JoinInFlight)
        ));

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Ok(Lifecycle::Formed)
        ));
    }

    /// An ambiguous join blocks forming; a definitive refusal from that address proves it did not
    /// admit this node and lifts the block, while a refusal from another address does not.
    #[test]
    fn a_definitive_refusal_lifts_an_ambiguous_join() {
        let definitive = [
            ConnectError::Refused(RefusalReason::NoCluster),
            ConnectError::Refused(RefusalReason::UnknownMember),
            ConnectError::Refused(RefusalReason::JoinsHeld),
            ConnectError::Refused(RefusalReason::ForeignCluster),
            ConnectError::ForeignCluster(ClusterId::new()),
        ];
        let until = tokio_time::Instant::now() + Duration::from_secs(30);

        for refusal in definitive {
            let mut coordinator = Coordinator::default();
            assert!(coordinator.record_ambiguous_join(addr(1), until));
            assert!(matches!(
                coordinator.form(tokio_time::Instant::now()),
                Err(FormError::JoinAmbiguous(ambiguous)) if ambiguous == addr(1)
            ));

            coordinator.enter_join();
            coordinator.finish_join(
                addr(2),
                Err(ConnectError::Refused(RefusalReason::NoCluster)),
            );
            assert!(matches!(
                coordinator.form(tokio_time::Instant::now()),
                Err(FormError::JoinAmbiguous(_))
            ));

            coordinator.enter_join();
            coordinator.finish_join(addr(1), Err(ConnectError::Closed));
            assert!(matches!(
                coordinator.form(tokio_time::Instant::now()),
                Err(FormError::JoinAmbiguous(_))
            ));

            coordinator.enter_join();
            coordinator.finish_join(addr(1), Err(refusal));
            assert!(matches!(
                coordinator.form(tokio_time::Instant::now()),
                Ok(Lifecycle::Formed)
            ));
        }

        let mut coordinator = Coordinator::default();
        coordinator.record_ambiguous_join(addr(1), until);
        coordinator.enter_join();
        coordinator.finish_join(addr(1), Err(ConnectError::Refused(RefusalReason::Down)));
        assert!(coordinator.ambiguous_joins.is_empty());
        assert!(!coordinator.record_ambiguous_join(addr(1), until));
    }

    /// An ambiguous join blocks forming for its bound at most, and no longer than its address is
    /// part of the universe; a member records none.
    #[test]
    fn an_ambiguous_join_ends_with_its_bound_or_its_address() {
        let now = tokio_time::Instant::now();
        let bound = Duration::from_secs(30);
        let mut coordinator = Coordinator::default();
        coordinator.record_ambiguous_join(addr(1), now + bound);

        assert!(matches!(
            coordinator.form(now + bound - Duration::from_millis(1)),
            Err(FormError::JoinAmbiguous(_))
        ));
        assert!(matches!(
            coordinator.form(now + bound),
            Ok(Lifecycle::Formed)
        ));
        assert!(!coordinator.record_ambiguous_join(addr(1), now + bound));

        let mut coordinator = Coordinator::default();
        coordinator.record_ambiguous_join(addr(1), now + bound);
        coordinator.record_ambiguous_join(addr(2), now + bound);
        assert_eq!(
            coordinator.release_ambiguous_joins_if_gone(&BTreeSet::from([addr(2), addr(3)])),
            vec![addr(1)]
        );
        assert!(matches!(
            coordinator.form(now),
            Err(FormError::JoinAmbiguous(ambiguous)) if ambiguous == addr(2)
        ));
        assert_eq!(
            coordinator.release_ambiguous_joins_if_gone(&BTreeSet::from([addr(3)])),
            vec![addr(2)]
        );
        assert!(matches!(coordinator.form(now), Ok(Lifecycle::Formed)));
    }

    /// A sighting of another cluster is published at once, stays while it is confirmed within
    /// the retention, and goes once it is not, or once its address is an Up member here. Neither
    /// this node's own cluster nor an unformed node is a sighting.
    #[tokio::test(start_paused = true)]
    async fn a_foreign_cluster_is_published_while_it_is_confirmed() {
        let (endpoint, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        endpoint.form_cluster().expect("an unformed endpoint forms");
        let this_cluster = endpoint.committed_cluster();
        let other = ClusterId::new();
        let foreign_clusters = || endpoint.state_rx().borrow().foreign_clusters().to_vec();
        endpoint.retain_foreign_clusters_for(Duration::from_secs(3));

        endpoint.sight_cluster(addr(2), this_cluster);
        endpoint.sight_cluster(addr(2), None);
        assert!(foreign_clusters().is_empty());

        endpoint.sight_cluster(addr(2), Some(other));
        assert_eq!(foreign_clusters(), [ForeignCluster::new(addr(2), other)]);

        tokio_time::advance(Duration::from_secs(2)).await;
        endpoint.sight_cluster(addr(2), Some(other));
        tokio_time::advance(Duration::from_secs(2)).await;
        endpoint.sweep_foreign_clusters(tokio_time::Instant::now());
        assert_eq!(foreign_clusters(), [ForeignCluster::new(addr(2), other)]);

        tokio_time::advance(Duration::from_secs(1)).await;
        endpoint.sweep_foreign_clusters(tokio_time::Instant::now());
        assert!(foreign_clusters().is_empty());

        endpoint.sight_cluster(addr(2), Some(other));
        assert_eq!(foreign_clusters().len(), 1);
        endpoint.update(|membership, _| membership.add_up(NodeId::new(addr(2))));
        assert!(foreign_clusters().is_empty());
        endpoint.sight_cluster(addr(2), Some(other));
        assert!(foreign_clusters().is_empty());

        let (unformed, _lane_dials, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(3)));
        unformed.sight_cluster(addr(2), Some(other));
        assert!(unformed.state_rx().borrow().foreign_clusters().is_empty());
    }

    /// The error naming the foreign clusters repeats every five minutes while any is seen, and
    /// comes at once when one is seen again after none was.
    #[test]
    fn a_foreign_cluster_is_reported_every_five_minutes() {
        let now = tokio_time::Instant::now();
        let mut foreign_clusters = ForeignClusters::new();

        assert!(!foreign_clusters.report_due(false, now));
        assert!(foreign_clusters.report_due(true, now));
        assert!(!foreign_clusters.report_due(true, now + Duration::from_secs(299)));
        assert!(foreign_clusters.report_due(true, now + Duration::from_secs(300)));
        assert!(!foreign_clusters.report_due(false, now + Duration::from_secs(301)));
        assert!(foreign_clusters.report_due(true, now + Duration::from_secs(302)));
    }

    /// The warning about a directly unreachable member repeats every minute per member while
    /// the member stays unreachable, and comes at once when it fails again after recovering.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_member_is_warned_about_every_minute() {
        let (first, second) = (NodeId::new(addr(2)), NodeId::new(addr(3)));
        let mut warnings = UnreachableWarnings::default();
        let due = |warnings: &mut UnreachableWarnings, unreachable: &[NodeId]| {
            warnings.due(unreachable, tokio_time::Instant::now())
        };

        assert_eq!(due(&mut warnings, &[first]), [first]);
        tokio_time::advance(Duration::from_secs(30)).await;
        assert_eq!(due(&mut warnings, &[first, second]), [second]);
        tokio_time::advance(UNREACHABLE_WARNING_INTERVAL - Duration::from_secs(30)).await;
        assert_eq!(due(&mut warnings, &[first, second]), [first]);
        tokio_time::advance(Duration::from_secs(30)).await;
        assert_eq!(due(&mut warnings, &[first, second]), [second]);

        assert!(due(&mut warnings, &[second]).is_empty());
        assert_eq!(due(&mut warnings, &[first, second]), [first]);
        assert!(due(&mut warnings, &[first, second]).is_empty());
    }

    /// A cluster which admitted this node may already count it, so nothing may be formed beside
    /// it however that attempt ended.
    #[test]
    fn a_pinned_cluster_postpones_forming() {
        let mut coordinator = Coordinator {
            pinned: Some((addr(1), ClusterId::new())),
            ..Default::default()
        };

        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::ClusterPinned(_))
        ));
    }

    /// A completed join is what the pin was held for, so committing it makes a member and
    /// releases the pin; committing again is the documented snapshot refresh and stays harmless.
    #[test]
    fn a_completed_join_makes_a_member_and_releases_the_pin() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some((addr(1), ClusterId::new())),
            ..Default::default()
        };

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(finished.result.is_ok());
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Formed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(finished.result.is_ok());
        assert_eq!(coordinator.lifecycle, Lifecycle::Formed);
    }

    /// A down landing while the attempt ran wins over its success: the caller learns it was
    /// downed rather than joined, and the side effects, already run by the down, are not rerun.
    #[test]
    fn a_join_completing_after_a_down_reports_the_down() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            ..Default::default()
        };
        assert!(coordinator.down());

        let finished = coordinator.finish_join(addr(1), Ok(()));
        assert!(matches!(finished.result, Err(ConnectError::SelfDowned)));
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Downed);
    }

    /// A refusal as dead downs this node in the same step which releases the permit, so no
    /// formation fits between the two, and the node is never formed in passing.
    #[test]
    fn a_refusal_as_dead_downs_atomically_with_the_release() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some((addr(1), ClusterId::new())),
            ..Default::default()
        };

        let finished =
            coordinator.finish_join(addr(1), Err(ConnectError::Refused(RefusalReason::Down)));
        assert!(matches!(
            finished.result,
            Err(ConnectError::Refused(RefusalReason::Down))
        ));
        assert!(finished.newly_downed);
        assert_eq!(coordinator.lifecycle, Lifecycle::Downed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::Downed)
        ));

        let finished =
            coordinator.finish_join(addr(1), Err(ConnectError::Refused(RefusalReason::Down)));
        assert!(!finished.newly_downed);
    }

    /// Only the pinned address answering that it is no cluster releases the pin; an answer from
    /// elsewhere, or any other failure, leaves the evidence standing.
    #[test]
    fn only_the_pinned_address_answering_no_cluster_releases_the_pin() {
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some((addr(1), ClusterId::new())),
            ..Default::default()
        };

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert_eq!(pinned_addr(&coordinator), Some(addr(1)));

        coordinator.finish_join(
            addr(2),
            Err(ConnectError::Refused(RefusalReason::NoCluster)),
        );
        assert_eq!(pinned_addr(&coordinator), Some(addr(1)));

        coordinator.finish_join(
            addr(1),
            Err(ConnectError::Refused(RefusalReason::NoCluster)),
        );
        assert_eq!(coordinator.pinned, None);
    }

    /// Discovery dropping the pinned address releases the pin, but never while an attempt runs,
    /// since that attempt may be gathering fresh evidence for the same address.
    #[test]
    fn a_pin_discovery_dropped_is_released_only_between_attempts() {
        let universe = BTreeSet::from([addr(2), addr(3)]);
        let mut coordinator = Coordinator {
            join_in_flight: true,
            pinned: Some((addr(1), ClusterId::new())),
            ..Default::default()
        };

        assert_eq!(coordinator.release_pin_if_gone(&universe), None);
        assert_eq!(pinned_addr(&coordinator), Some(addr(1)));

        coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert_eq!(coordinator.release_pin_if_gone(&universe), Some(addr(1)));
        assert_eq!(coordinator.pinned, None);

        coordinator.pinned = Some((addr(2), ClusterId::new()));
        assert_eq!(coordinator.release_pin_if_gone(&universe), None);
        assert_eq!(pinned_addr(&coordinator), Some(addr(2)));
    }
    /// A down may land anywhere inside an attempt, and must win everywhere. No attempt starts on
    /// a downed node, and no pin is written after it. Whatever the attempt's raw outcome, its
    /// caller learns of the down and nothing stays pinned.
    #[test]
    fn a_down_dominates_every_stage_of_a_join() {
        let mut coordinator = Coordinator::default();
        assert!(coordinator.enter_join());
        assert!(coordinator.down());
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::Downed)
        ));

        coordinator.pin(addr(1), ClusterId::new());
        assert_eq!(coordinator.pinned, None);

        let finished = coordinator.finish_join(addr(1), Err(ConnectError::Closed));
        assert!(matches!(finished.result, Err(ConnectError::SelfDowned)));
        assert!(!finished.newly_downed);
        assert_eq!(coordinator.pinned, None);
        assert!(!coordinator.join_in_flight);

        assert!(!coordinator.enter_join());
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::Downed)
        ));
    }

    /// Forming mints the cluster ID and commits it, so the formed node has an effective ID at
    /// once, and a node formed later mints a different one.
    #[test]
    fn forming_mints_and_commits_a_cluster_id() {
        let mut coordinator = Coordinator::default();
        assert_eq!(coordinator.effective_cluster(), None);

        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Ok(Lifecycle::Formed)
        ));
        let committed = coordinator.committed.expect("forming commits an ID");
        assert_eq!(coordinator.effective_cluster(), Some(committed));

        let mut other = Coordinator::default();
        assert!(matches!(
            other.form(tokio_time::Instant::now()),
            Ok(Lifecycle::Formed)
        ));
        assert_ne!(other.committed, Some(committed));
    }

    /// A pinned cluster's ID is the effective one until a join commits it; another cluster's ID
    /// never replaces the pin, and releasing the pin drops the provisional ID with it.
    #[test]
    fn a_pin_holds_a_provisional_cluster_id() {
        let pinned = ClusterId::new();
        let mut coordinator = Coordinator::default();
        assert!(coordinator.enter_join());

        coordinator.pin(addr(1), pinned);
        assert_eq!(coordinator.effective_cluster(), Some(pinned));
        assert_eq!(coordinator.committed, None);

        coordinator.pin(addr(2), ClusterId::new());
        assert_eq!(coordinator.pinned, Some((addr(1), pinned)));

        coordinator.pin(addr(2), pinned);
        assert_eq!(coordinator.pinned, Some((addr(2), pinned)));

        coordinator.finish_join(addr(2), Err(ConnectError::Closed));
        assert_eq!(
            coordinator.release_pin_if_gone(&BTreeSet::from([addr(3)])),
            Some(addr(2))
        );
        assert_eq!(coordinator.effective_cluster(), None);
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Ok(Lifecycle::Formed)
        ));
        assert_ne!(coordinator.committed, Some(pinned));
    }

    /// A committed ID outlives the pin it came with and is what a completed join leaves behind.
    #[test]
    fn a_completed_join_keeps_the_committed_cluster_id() {
        let cluster = ClusterId::new();
        let mut coordinator = Coordinator::default();
        assert!(coordinator.enter_join());
        coordinator.pin(addr(1), cluster);
        coordinator.commit(cluster);

        assert!(coordinator.finish_join(addr(1), Ok(())).result.is_ok());
        assert_eq!(coordinator.pinned, None);
        assert_eq!(coordinator.committed, Some(cluster));
        assert_eq!(coordinator.effective_cluster(), Some(cluster));
        assert!(matches!(
            coordinator.form(tokio_time::Instant::now()),
            Err(FormError::AlreadyFormed)
        ));
    }

    /// The endpoint publishes the committed ID in its cluster state the moment it forms.
    #[test]
    fn a_formed_endpoint_publishes_its_cluster_id() {
        let (endpoint, _dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        assert_eq!(endpoint.state_rx().borrow().cluster(), None);

        endpoint.form_cluster().expect("an unformed endpoint forms");
        let cluster = endpoint.committed_cluster();
        assert!(cluster.is_some());
        assert_eq!(endpoint.effective_cluster(), cluster);
        assert_eq!(endpoint.state_rx().borrow().cluster(), cluster);
    }

    /// The codec name and the application version travel in the handshake, so a value longer
    /// than it carries is refused at start, while the exact length passes.
    #[test]
    fn handshake_strings_are_bounded_at_start() {
        struct Named(&'static str);

        impl Codec for Named {
            fn name(&self) -> &'static str {
                self.0
            }

            fn encode(
                &self,
                message: &dyn erased_serde::Serialize,
            ) -> Result<Vec<u8>, crate::cluster::codec::CodecError> {
                Postcard.encode(message)
            }

            fn decode(
                &self,
                payload: &[u8],
                visit: DecodeVisitor<'_>,
            ) -> Result<(), crate::cluster::codec::CodecError> {
                Postcard.decode(payload, visit)
            }
        }

        let max = Handshake::MAX_STRING_LEN;
        let mut config = EndpointConfig::new(addr(2552));

        config.codec = Arc::new(Named("c".repeat(max).leak()));
        assert!(validate(&config).is_ok());
        config.codec = Arc::new(Named("c".repeat(max + 1).leak()));
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::CodecNameTooLong { len, max: limit })
                if len == max + 1 && limit == max
        ));

        config.codec = Arc::new(Postcard);
        let with_build = |len: usize| {
            Version::parse(&format!("1.0.0+{}", "b".repeat(len - 6))).expect("a semantic version")
        };
        config.app_version = Some(with_build(max));
        assert!(validate(&config).is_ok());
        config.app_version = Some(with_build(max + 1));
        assert!(matches!(
            validate(&config),
            Err(InvalidEndpointConfig::AppVersionTooLong { len, max: limit })
                if len == max + 1 && limit == max
        ));
    }
}
