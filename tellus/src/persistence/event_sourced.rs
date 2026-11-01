use crate::{
    ActorContext, Incoming, TimerMode,
    persistence::{effect::Effect, persistence_id::PersistenceId, versioned::Versioned},
};
use std::error::Error;

/// An event-sourced actor: it persists what happened, not what its state is. [handle] validates a
/// command against the current state and decides which events it causes; once those events are
/// durable they are folded into the state by [apply], the one and only state transition. The
/// state itself is never stored: it is recovered by replaying the events, optionally shortcut by
/// a snapshot. See `docs/persistence.md` for the guarantees.
///
/// [handle]: EventSourced::handle
/// [apply]: EventSourced::apply
pub trait EventSourced
where
    Self: Sized,
{
    /// The type of the received commands.
    type Command;

    /// Whether this actor uses timers: [Untimed](crate::Untimed), for an actor without timers, or
    /// [Timed](crate::Timed), which provides `start_timer` and `cancel_timer` on the context.
    type Timers: TimerMode;

    /// The type of the persisted events.
    type Event: Versioned;

    /// The type of the state.
    type State;

    /// The type of the snapshots. For actors without snapshots use [Nothing](crate::Nothing).
    type Snapshot: Versioned;

    /// The type of the failures returned by the fallible methods. For infallible actors use
    /// [std::convert::Infallible].
    type Error: Error;

    /// The stable identity of this actor's event stream, chosen by the application and unchanged
    /// across incarnations; unrelated to [ActorId](crate::ActorId), which is fresh per spawn.
    fn persistence_id(&self) -> PersistenceId;

    /// The state before any event is applied. It is re-run on every recovery without a snapshot, so
    /// it must produce the same seed the stored events were originally folded onto. It must hence
    /// be pure and do no world-touching work, which would silently disappear the moment snapshots
    /// are enabled. Such work belongs in [recovered](EventSourced::recovered).
    ///
    /// tellus handles a failure exactly like a panic, through the configured
    /// [SupervisionStrategy](crate::SupervisionStrategy).
    ///
    /// # Errors
    /// Implementations fail if they choose to, e.g. through `?` on a domain error.
    fn init(&self) -> Result<Self::State, Self::Error>;

    /// Turn a decoded snapshot into the state as of the sequence number the snapshot covers;
    /// recovery then replays only the events after it. Actors without snapshots use
    /// `match snapshot {}`.
    ///
    /// tellus handles a failure exactly like a panic, through the configured
    /// [SupervisionStrategy](crate::SupervisionStrategy).
    ///
    /// # Errors
    /// Implementations fail if they choose to, e.g. through `?` on a domain error.
    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error>;

    /// Reconcile the recovered state with the world, e.g. spawn child actors or re-arm timers.
    /// Run once per successful recovery, on every spawn and every restart, after replay or
    /// snapshot load and before the first command, never during replay.
    ///
    /// tellus handles a failure exactly like a panic, through the configured
    /// [SupervisionStrategy](crate::SupervisionStrategy).
    ///
    /// # Errors
    /// Implementations fail if they choose to, e.g. through `?` on a domain error.
    fn recovered(
        &self,
        _context: &ActorContext<Self::Command, Self::Timers>,
        state: Self::State,
    ) -> Result<Self::State, Self::Error> {
        Ok(state)
    }

    /// Handle an incoming command or signal against the current state and decide its [Effect]:
    /// which events to persist, whether to stop and what to do once the events are durable. The
    /// state is only borrowed: the sole state transition is [apply](EventSourced::apply), driven
    /// by the persisted events.
    ///
    /// Returning an error or panicking makes the configured
    /// [SupervisionStrategy](crate::SupervisionStrategy) decide what happens; under
    /// [SupervisionStrategy::Restart](crate::SupervisionStrategy::Restart) the actor recovers by
    /// replaying from the store.
    ///
    /// # Errors
    /// Implementations fail if they choose to, e.g. through `?` on a domain error.
    fn handle(
        &self,
        context: &ActorContext<Self::Command, Self::Timers>,
        incoming: Incoming<Self::Command>,
        state: &Self::State,
    ) -> Result<Effect<Self>, Self::Error>;

    /// Fold an event into the state: the only state transition, run on replay exactly as on a live
    /// event. It must hence be pure and total: no I/O, no failure, no dependence on the clock, on
    /// randomness or on per-incarnation values.
    fn apply(&self, state: Self::State, event: Self::Event) -> Self::State;

    /// Offer a snapshot of the given state, called once the events of an [Effect] with at least
    /// one event have been applied and its continuations have run, never for an effect without
    /// events. [Some] is saved to the snapshot store, shortening future recoveries, and [None]
    /// means no snapshot is due. Snapshots are a discardable derivative of the events, never a
    /// source of truth, so a failure to build or save one is logged and never fails the actor.
    ///
    /// # Errors
    /// Implementations fail if they choose to.
    fn snapshot(&self, _state: &Self::State) -> Result<Option<Self::Snapshot>, Self::Error> {
        Ok(None)
    }
}
