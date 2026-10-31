#![cfg_attr(docsrs, feature(doc_cfg))]

//! An actor framework for Rust, built on Tokio: typed messages, supervision trees, death watch
//! and event sourcing.
//!
//! An actor is created by implementing [Actor]: [Actor::init] creates its initial state and
//! [Actor::receive] applies received messages and signals to that state until it returns
//! [Control::Stop].
//!
//! Actors form a tree: the root actor is spawned by creating an [ActorSystem] and any actor can
//! spawn child actors via [ActorContext::spawn]. When an actor stops, its child actors are stopped
//! first; only once all descendants have terminated does it terminate itself. Hence
//! [ActorSystem::terminated] resolves once the whole actor tree has terminated, and
//! [ActorSystem::stop] ends the tree from the outside, between two messages of the root actor.
//!
//! When [Actor::init] or [Actor::receive] fails with an error or a panic, the actor's
//! [SupervisionStrategy] decides what happens: [SupervisionStrategy::Stop] terminates the actor,
//! [SupervisionStrategy::Restart] rebuilds its state via [Actor::init], limited and paced by a
//! [RestartPolicy].
//!
//! Actors are addressed by [ActorRef], which is used to [ActorRef::tell] them messages and to
//! [ActorRef::ask] them requests from outside the actor tree, awaiting the reply; between actors,
//! [ActorContext::reply_to] creates a [ReplyTo] which delivers the reply as an ordinary message
//! instead. Actors can observe each other via [ActorContext::watch], which delivers an
//! [Incoming::Terminated] signal, and [ActorContext::unwatch], which reverts that. The signal is
//! ordered behind all messages the terminated actor has delivered to the watcher. Receiving it
//! hence proves that the watcher has seen every message from that actor it will ever see: each
//! arrived before the signal or was dropped as a dead letter. An actor declaring `type Timers =
//! Timed` can message itself after a delay via [ActorContext::start_timer], behind the messages
//! queued for it; a timer replaced under its key or canceled via [ActorContext::cancel_timer] is
//! never received. With `type Timers =` [Untimed], the mode of the plain [ActorContext], an actor
//! has no timers and pays nothing for them. An actor can defer an [Incoming] with
//! [ActorContext::stash] and receive the stashed items again, ahead of its mailbox, after
//! [ActorContext::unstash_all].
//!
//! A [Host] hosts entities: actors identified by a key, spawned on the first [HostEnvelope] for
//! that key and, if configured, passivated once nothing has messaged them for a while, with
//! messages arriving meanwhile buffered for their next incarnation. See `docs/actors.md` in the
//! repository for the guarantees.
//!
//! With the `persistence` feature, actors can be event sourced by implementing [EventSourced] and
//! spawning via [ActorContext::spawn_event_sourced] or [ActorSystem::event_sourced]: commands are
//! handled against the current state, the events they cause are appended to an [EventStore] and
//! only then applied, and the state is recovered by replay, optionally shortcut by snapshots. See
//! `docs/persistence.md` in the repository for the guarantees. The `persistence-tests` feature
//! adds [persistence_tests], the contract test suite any store implementation must pass, and the
//! `persistence-in-memory` feature adds [InMemoryStore], which keeps events and snapshots in
//! memory, for testing event-sourced actors without a database.
//!
//! The `test-util` feature adds [testing], which unit tests an actor's logic without a running
//! actor system: [testing::TestContext] provides the [ActorContext] to call [Actor::receive],
//! `EventSourced::handle` and the like directly, and collects what the actor sends to itself.
//!
//! The `cluster` feature adds remoting in the [cluster] module: [ActorRef] becomes serializable and
//! actors on different nodes message and watch each other through the same API. Without that
//! feature tellus is purely local and pulls in none of the remoting dependencies.
//!
//! The `serde` feature makes the configuration deserializable, [ActorConfig] as well as the
//! cluster's [EndpointConfig](cluster::EndpointConfig) and
//! [BootstrapConfig](cluster::BootstrapConfig), so a deployment can read it from a config file with
//! human readable durations. tellus pulls in no parser of its own, see the README.

#![doc(
    html_logo_url = "https://tellus.rs/brand/logo-tile.svg",
    html_favicon_url = "https://tellus.rs/favicon.svg"
)]
#![warn(missing_docs, clippy::missing_errors_doc)]

#[cfg(feature = "cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
pub mod cluster;
#[cfg(feature = "persistence-tests")]
pub mod persistence_tests;
#[cfg(feature = "test-util")]
pub mod testing;

mod actor;
mod actor_config;
mod actor_context;
mod actor_id;
mod actor_ref;
mod actor_system;
mod backoff;
mod host;
mod mailbox;
#[cfg(feature = "persistence")]
mod persistence;
mod quota;
mod request_response;
mod stash;
mod sync;
mod timers;
mod watch;

pub use crate::{
    actor::{Actor, Control, Incoming, Nothing},
    actor_config::{ActorConfig, MailboxCapacity, RestartPolicy, SupervisionStrategy},
    actor_context::ActorContext,
    actor_id::ActorId,
    actor_ref::ActorRef,
    actor_system::{ActorSystem, TerminatedError},
    backoff::{Backoff, InvalidBackoff},
    host::{Host, HostConfig, HostEnvelope, InvalidHostConfig},
    request_response::{AskError, ReplyTo},
    timers::{Timed, TimerMode, Untimed},
};

#[cfg(feature = "persistence")]
pub use crate::persistence::{
    Persistence,
    codec::{Cbor, Codec, EncodeError, Json, PayloadError},
    effect::Effect,
    event_sourced::EventSourced,
    persistence_id::{InvalidPersistenceId, PersistenceId, PersistenceIdSegment},
    schema_version::SchemaVersion,
    seq_no::SeqNo,
    store::{
        AppendError, EncodedEvent, EncodedSnapshot, EventStore, NoSnapshots, SnapshotStore,
        StoredEvent, StoredSnapshot,
    },
    versioned::{DecodeError, Versioned},
};

#[cfg(feature = "persistence-in-memory")]
pub use crate::persistence::in_memory_store::InMemoryStore;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct Readme;
