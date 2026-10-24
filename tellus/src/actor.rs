#[cfg(feature = "persistence")]
use crate::persistence::{schema_version::SchemaVersion, versioned::Versioned};
use crate::{ActorContext, ActorId};
#[cfg(feature = "persistence")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::error::Error;

/// A stateful actor, handling the messages or signals it receives.
///
/// On receiving a message an actor can send messages to other actors via [ActorRef::tell], spawn
/// child actors via [ActorContext::spawn] and designate the state handling the next message via
/// [Control::Continue].
///
/// [ActorRef::tell]: crate::ActorRef::tell
pub trait Actor {
    /// The type of the received messages.
    type Message;

    /// The type of the state. For stateless actors use `()`.
    type State;

    /// The type of the failures returned by [Actor::init] and [Actor::receive]. For infallible
    /// actors use [std::convert::Infallible].
    type Error: Error;

    /// Create the initial state, possibly spawning child actors or sending messages.
    ///
    /// Returning an error or panicking makes the configured [SupervisionStrategy] decide what
    /// happens. This holds for the first initialization at spawn as well as on the restart path.
    /// Under [SupervisionStrategy::Stop] the actor stops and its watchers get an
    /// [Incoming::Terminated] signal, under [SupervisionStrategy::Restart] the initialization is
    /// retried with backoff.
    ///
    /// # Errors
    /// Implementations fail when they choose to, e.g. through `?` on a domain error; tellus
    /// handles a failure exactly like a panic, through the configured [SupervisionStrategy].
    ///
    /// [SupervisionStrategy]: crate::SupervisionStrategy
    /// [SupervisionStrategy::Stop]: crate::SupervisionStrategy::Stop
    /// [SupervisionStrategy::Restart]: crate::SupervisionStrategy::Restart
    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error>;

    /// Receive an incoming message or signal, apply it to the given state and designate the state
    /// handling the next message.
    ///
    /// Returning an error or panicking makes the configured [SupervisionStrategy] decide
    /// what happens. Use `?` to escalate a failure to the supervisor and an explicit `match` to
    /// handle it as part of the domain.
    ///
    /// # Errors
    /// Implementations fail when they choose to, e.g. through `?` on a domain error; tellus
    /// handles a failure exactly like a panic, through the configured [SupervisionStrategy].
    ///
    /// # Deadlock
    /// An actor cannot be stopped while `receive` is running, which also blocks a Tokio worker. A
    /// `receive` which never completes hence keeps all ancestors from terminating. For long running
    /// or blocking work use [ActorContext::pipe_to_self].
    ///
    /// [SupervisionStrategy]: crate::SupervisionStrategy
    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error>;
}

/// An uninhabited type for actors which don't react to messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nothing {}

#[cfg(feature = "persistence")]
impl Versioned for Nothing {
    const MANIFEST: &'static str = "nothing";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[cfg(feature = "persistence")]
impl Serialize for Nothing {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {}
    }
}

#[cfg(feature = "persistence")]
impl<'de> Deserialize<'de> for Nothing {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(de::Error::custom(
            "no value of the uninhabited type Nothing",
        ))
    }
}

/// A message or signal received by an actor. Signals are currently limited to
/// [Incoming::Terminated].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(align(16))]
pub enum Incoming<M> {
    /// A message sent to this actor.
    Message(M),

    /// The signal that a watched actor has terminated. It is ordered behind all messages that actor
    /// has delivered to this one. Receiving it hence proves that this actor has seen every message
    /// from the terminated one it will ever see: each arrived before the signal or was dropped as a
    /// dead letter.
    Terminated(ActorId),
}

/// The decision of an actor after handling a message: designate the state handling the next one
/// or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Control<S> {
    /// Keep receiving messages and signals, handling the next one with the given state.
    Continue(S),

    /// Stop this actor.
    Stop,
}
