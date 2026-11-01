use crate::persistence::event_sourced::EventSourced;
use derive_more::Debug;

pub(crate) type Then<A> = Box<dyn FnOnce(&<A as EventSourced>::State) + Send>;

/// The decision of an event-sourced actor after handling a command: which events to persist,
/// whether to stop and, via [then](Effect::then), what to do once the events are durable.
#[derive(Debug)]
#[debug(bound())]
pub struct Effect<A>
where
    A: EventSourced,
{
    #[debug("{}", events.len())]
    pub(crate) events: Vec<A::Event>,

    pub(crate) stop: bool,

    #[debug("{}", thens.len())]
    pub(crate) thens: Vec<Then<A>>,
}

impl<A> Effect<A>
where
    A: EventSourced,
{
    /// Persist and apply nothing, e.g. for a command which only reads the state.
    pub fn none() -> Self {
        Self::persist_all([])
    }

    /// Persist the given event and apply it once durable.
    pub fn persist(event: A::Event) -> Self {
        Self::persist_all([event])
    }

    /// Persist the given events atomically, all or none, and apply them in order once durable.
    pub fn persist_all<E>(events: E) -> Self
    where
        E: IntoIterator<Item = A::Event>,
    {
        Self {
            events: events.into_iter().collect(),
            stop: false,
            thens: Vec::new(),
        }
    }

    /// Persist nothing and stop this actor.
    pub fn stop() -> Self {
        Self::none().and_stop()
    }

    /// Stop this actor once this effect is settled, i.e. after its events are durable and applied
    /// and its [then](Effect::then) continuations have run.
    pub fn and_stop(mut self) -> Self {
        self.stop = true;
        self
    }

    /// The events this effect persists, in order.
    #[cfg(feature = "test-util")]
    pub fn events(&self) -> &[A::Event] {
        &self.events
    }

    /// Whether this effect stops the actor once settled.
    #[cfg(feature = "test-util")]
    pub fn stop_requested(&self) -> bool {
        self.stop
    }

    /// Run the given continuation on the state once the events of this effect are durable and
    /// applied, hence never on replay: the only safe place for outward-facing actions such as
    /// replying or telling another actor. On an effect without events it runs right after
    /// [EventSourced::handle]. Continuations run in the order they were added; a crash between
    /// durability and the continuation loses it, so outward-facing actions are at-most-once.
    pub fn then<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&A::State) + Send + 'static,
    {
        self.thens.push(Box::new(f));
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorContext, EventSourced, Incoming, Nothing, PersistenceId, SchemaVersion, Unstashed,
        Untimed, Versioned, persistence::effect::Effect,
    };
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;

    /// Deliberately not [Debug]: the [Effect] output must not require it of the event type.
    #[derive(Serialize, Deserialize)]
    struct Increased;

    impl Versioned for Increased {
        const MANIFEST: &'static str = "increased";
        const VERSION: SchemaVersion = SchemaVersion::new(1);
    }

    struct Counter;

    impl EventSourced for Counter {
        type Command = ();
        type Timers = Untimed;
        type Stash = Unstashed;
        type Event = Increased;
        type State = ();
        type Snapshot = Nothing;
        type Error = Infallible;

        fn persistence_id(&self) -> PersistenceId {
            PersistenceId::new("counter", "1").expect("the segments are valid")
        }

        fn init(&self) -> Result<Self::State, Self::Error> {
            Ok(())
        }

        fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
            match snapshot {}
        }

        fn handle(
            &self,
            _context: &ActorContext<Self::Command>,
            _incoming: Incoming<Self::Command>,
            _state: &Self::State,
        ) -> Result<Effect<Self>, Self::Error> {
            Ok(Effect::none())
        }

        fn apply(&self, state: Self::State, _event: Self::Event) -> Self::State {
            state
        }
    }

    /// An effect is inspectable in logs and assertions even though its continuations are boxed
    /// closures and its events need not be [Debug]: the counts stand in for both.
    #[test]
    fn debug_reports_the_counts_without_requiring_debug_events() {
        let effect = Effect::<Counter>::none();
        assert_eq!(
            format!("{effect:?}"),
            "Effect { events: 0, stop: false, thens: 0 }"
        );

        let effect = Effect::<Counter>::persist_all([Increased, Increased])
            .then(|_| ())
            .and_stop();
        assert_eq!(
            format!("{effect:?}"),
            "Effect { events: 2, stop: true, thens: 1 }"
        );
    }
}
