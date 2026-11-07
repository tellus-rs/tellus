//! Clustered remoting: actors on different nodes messaging each other through ordinary,
//! serializable [ActorRef]s, within a cluster they are members of (`cluster` feature).
//!
//! Each process runs at most one remoting endpoint, started via [start_endpoint] with a [Transport]
//! (the provided one is [QuicTransport]) and an [EndpointConfig]. [ActorRef] implements `Serialize`
//! and `Deserialize`, both failing with [RefError::EndpointNotStarted] until the endpoint is
//! started: message types embed reference fields, e.g. `reply_to: ActorRef<Reply>`, and work
//! unchanged no matter where their counterpart lives. Serializing a local reference lazily binds
//! the actor in the endpoint's registry, so inbound messages find it; the binding is evicted once
//! the actor terminates. Message payloads are encoded by a [Codec], by default [Postcard].
//!
//! The nodes messaging each other form a *cluster*, and membership is not optional. A started
//! endpoint is not a cluster and admits nobody: [form] makes it a cluster of one, [join] makes it a
//! member of the cluster one of its seed addresses is in, and only members message each other.
//! The member list is gossiped, so every member learns every other from one seed address. [members]
//! lists them, [cluster_state] holds the current view, [changes] streams every version of it, and
//! [down] removes one by operator decision. Every cluster has a [ClusterId], which every handshake
//! carries, so a node of another cluster is refused and two clusters never merge. A node shutting
//! down cleanly announces its own departure with [leave](fn@leave), or with [leave_on_terminated]
//! where one actor system's lifetime is the node's lifetime. Its peers drain what it sent and
//! remove it at once instead of detecting its silence, and the [Departure] reports which of them
//! acknowledged the drain.
//!
//! Every member is heartbeated, and a [FailureDetector] (default: the adaptive
//! [PhiAccrualFailureDetector]) marks silent members locally unreachable. The [DowningProvider]
//! (default: [KeepMajority]) decides when an unreachable member is *downed*, i.e. dead for good.
//! Downing fails the member's pending asks, signals its watchers and refuses its incarnation from
//! then on, so a downed node rejoins only as a restarted process. The default resolves a partition
//! toward one side: the majority downs what it cannot reach, the minority downs itself. Under
//! mutual TLS a peer's certificate must cover the address it advertises, by a `tellus://<ip:port>`
//! URI SAN or, without one, by an IP SAN.
//!
//! Actors find each other by name. [register] names a local actor under a [Key], which names the
//! message type by its [Manifest]. The [receptionist] is the directory over those registrations,
//! replicated to every Up member, so nothing but a name and the seed addresses has to be
//! configured. [receptionist::lookup] answers every actor under a key on every Up member and
//! [receptionist::lookup_at] those on the member at one address, both without a round trip.
//! [receptionist::subscribe] watches that set, and [receptionist::settled] tells whether every
//! member's registrations have arrived. [pubsub] is that directory used for fan-out:
//! [pubsub::subscribe] registers a local actor under a [pubsub::Topic] and [pubsub::publish] tells
//! every subscriber the receptionist resolves.
//!
//! The seed addresses themselves can be discovered: [bootstrap](fn@bootstrap) resolves them through
//! a [SeedDiscovery] and joins through them. The provided discoveries are [FixedSeeds] here, DNS in
//! the `tellus-bootstrap-dns` crate and Kubernetes pods in the `tellus-bootstrap-k8s` crate. When
//! no address of a settled view is a member of anything, the [FormationProvider] decides whether
//! this node forms a cluster. The default, [Majority], forms at the lowest address of the universe
//! once a majority of it is known to be no cluster. Requiring a majority keeps a partitioned node
//! from coming back as a cluster of its own. Bootstrap runs once; from then on membership owns
//! failure detection and downing, and a probe reports any other cluster at a discovered address in
//! [ClusterState::foreign_clusters].
//!
//! Alternatively [serialize_ref] turns a local reference into bytes to be shared via configuration,
//! command line or any other channel. [deserialize_ref] resolves them on another node, refusing
//! bytes serialized for another message type as [RefError::TypeMismatch]. Any further reference
//! travels inside messages.
//!
//! # Guarantees
//!
//! Remote [ActorRef::tell] keeps the local contract: fire-and-forget, at-most-once, undeliverable
//! messages are dropped and logged as dead letters. This covers an unreachable or crashed node, a
//! full outbound queue and a payload which cannot be decoded on the receiving node. Messages from
//! one sender to one target arrive in send order. All messages toward one actor ride one ordered
//! stream of the lane toward its node, enqueued at tell time and delivered to mailboxes in arrival
//! order, so a large message only delays frames toward actors sharing its stream. A lost
//! connection is reconnected with backoff; frames queued while the link is down flush in order,
//! so per-sender FIFO across reconnects is "in order, with gaps".
//!
//! Death watch works across nodes through the ordinary [ActorContext::watch], with a two tier
//! contract. A terminated signal for a *real termination* keeps the full local guarantee: it is
//! ordered behind all messages the terminated actor delivered to the watcher and proves its
//! destructors have run. A signal *synthesized* when the watched actor's member is downed only
//! proves that no message from that actor will ever be delivered through this endpoint again. The
//! two are indistinguishable in the API. A watch on an incarnation already Left or Down fires the
//! synthesized signal immediately, or with the node's death while its departure still drains. A
//! node not known yet is watched like any other. See docs/cluster.md for the full contract and
//! its rationale.
//!
//! Request-response crosses nodes the same way: [ReplyTo] is serializable and travels inside
//! messages, so [ActorRef::ask] and [ActorContext::reply_to] work unchanged against remote
//! actors. An ask still resolves exactly once, at latest at its timeout, and a reply keeps
//! per-sender FIFO with the responder's other messages to the asker. The `NoReply` detection
//! weakens to best-effort. A reply destination dropped on another node is signaled
//! fire-and-forget, and so is a request dead-lettered undecoded on the receiving node. Downing a
//! member fails the asks pending toward it. See docs/cluster.md for the exact contract.
//!
//! [Codec]: crate::cluster::codec::Codec
//! [DowningProvider]: crate::cluster::downing::DowningProvider
//! [FailureDetector]: crate::cluster::failure::FailureDetector
//! [FormationProvider]: crate::cluster::formation::FormationProvider
//! [KeepMajority]: crate::cluster::downing::KeepMajority
//! [Majority]: crate::cluster::formation::Majority
//! [PhiAccrualFailureDetector]: crate::cluster::failure::PhiAccrualFailureDetector
//! [Postcard]: crate::cluster::codec::Postcard
//! [QuicTransport]: crate::cluster::transport::QuicTransport
//! [Transport]: crate::cluster::transport::Transport
//! [ActorContext::reply_to]: crate::ActorContext::reply_to
//! [ActorContext::watch]: crate::ActorContext::watch
//! [ActorRef]: crate::ActorRef
//! [ActorRef::ask]: crate::ActorRef::ask
//! [ActorRef::tell]: crate::ActorRef::tell
//! [ReplyTo]: crate::ReplyTo

pub mod codec;
pub mod downing;
pub mod failure;
pub mod formation;
pub mod pubsub;
pub mod receptionist;
pub mod transport;

mod bootstrap;
mod discovery;
mod endpoint;
mod frame;
mod lanes;
mod leave;
mod manifest;
mod membership;
mod node;
mod peer;
mod reachability;
mod registry;
mod reply;
mod schema;
mod sink;
mod state;
mod watch;
mod wire;

pub use crate::cluster::{
    bootstrap::{
        BootstrapConfig, BootstrapError, FixedSeeds, InvalidBootstrapConfig, SeedDiscovery,
        bootstrap,
    },
    discovery::{Key, RegisterError, register},
    endpoint::{
        EndpointConfig, FormError, InvalidEndpointConfig, Lifecycle, StartError, form,
        start_endpoint,
    },
    leave::{Departure, LeaveError, leave, leave_on_terminated},
    manifest::Manifest,
    membership::{DownError, JoinError, Member, MemberState, MembersError, down, join, members},
    node::{ClusterId, Incarnation},
    schema::{AppVersion, Peer, Schema, UnsupportedSchemaVersion},
    state::{
        BrokenLink, Change, Changes, ClusterState, ClusterStateError, ForeignCluster, Transition,
        changes, cluster_state, lifecycle,
    },
    wire::{RefError, deserialize_ref, serialize_ref},
};

pub(crate) use crate::cluster::{
    node::NodeId,
    reply::{AskGuard, ReplyToWire},
    sink::RemoteSink,
    watch::{unwatch_remote, watch_remote},
};

/// Severs every live connection of this process's endpoint, for tests: frames in flight are
/// lost, lanes keep their queues and reconnect, which is the fault the reconnect guarantees are
/// stated for. `false` if the endpoint is not started.
///
/// Only available with the `cluster-dev` feature, so it cannot reach a production build which
/// does not ask for it.
#[cfg(feature = "cluster-dev")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-dev")))]
pub fn sever_connections() -> bool {
    match endpoint::get() {
        Some(endpoint) => {
            endpoint.sever();
            true
        }

        None => false,
    }
}

/// The bytes of the Gossip frames this process's endpoint has queued for its peers since it
/// started, without framing or transport overhead. `None` if the endpoint is not started.
///
/// Only available with the `cluster-dev` feature, so it cannot reach a production build which
/// does not ask for it.
#[cfg(feature = "cluster-dev")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-dev")))]
pub fn gossip_bytes() -> Option<u64> {
    endpoint::get().map(|endpoint| endpoint.gossip_bytes())
}

/// Arms the endpoint to silently drop the next `count` outbound terminated signal frames, for
/// tests: the loss the periodic watch refresh must heal. `false` if the endpoint is not started.
///
/// Only available with the `cluster-dev` feature, so it cannot reach a production build which
/// does not ask for it.
#[cfg(feature = "cluster-dev")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-dev")))]
pub fn drop_terminated_frames(count: u64) -> bool {
    match endpoint::get() {
        Some(endpoint) => {
            endpoint.arm_terminated_drop(count);
            true
        }

        None => false,
    }
}
