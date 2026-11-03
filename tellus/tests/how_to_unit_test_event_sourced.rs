#![cfg(all(feature = "test-util", feature = "persistence"))]

use serde::{Deserialize, Serialize};
use std::assert_matches;
use tellus::{
    ActorContext, Effect, EventSourced, Incoming, Nothing, PersistenceId, ReplyTo, SchemaVersion,
    Unstashed, Untimed, Versioned,
    testing::{TestContext, settle},
};
use thiserror::Error;

// ANCHOR: decide
/// A valid reading causes exactly one event, and settling it makes it the state.
#[test]
fn a_reading_is_persisted_and_applied() {
    let test_context = TestContext::new();

    let effect = Device
        .handle(
            test_context.context(),
            Incoming::Message(Command::Record(21.5)),
            &None,
        )
        .expect("the reading is valid");
    assert_eq!(effect.events(), [Recorded(21.5)]);
    assert!(!effect.stop_requested());

    let settlement = settle(&Device, None, effect);
    assert_eq!(settlement.state, Some(21.5));
}
// ANCHOR_END: decide

// ANCHOR: reject
/// An invalid reading fails the device and causes no event.
#[test]
fn an_invalid_reading_fails() {
    let test_context = TestContext::new();

    let result = Device.handle(
        test_context.context(),
        Incoming::Message(Command::Record(f64::NAN)),
        &Some(21.5),
    );

    assert_matches!(result, Err(InvalidReading));
}
// ANCHOR_END: reject

// ANCHOR: replay
/// Replay is a fold of `apply` over `init`.
#[test]
fn replay_recovers_the_last_reading() {
    let seed = Device.init().expect("the seed cannot fail");

    let state = [Recorded(21.5), Recorded(22.0)]
        .into_iter()
        .fold(seed, |state, event| Device.apply(state, event));

    assert_eq!(state, Some(22.0));
}
// ANCHOR_END: replay

// ANCHOR: reply
/// A read causes no event; its reply is a continuation, which `settle` runs.
#[test]
fn a_read_replies_with_the_last_reading() {
    let test_context = TestContext::new();
    let mut asker = TestContext::<Option<f64>>::new();
    let reply_to = asker.context().reply_to(|last_reading| last_reading);

    let effect = Device
        .handle(
            test_context.context(),
            Incoming::Message(Command::Read(reply_to)),
            &Some(21.5),
        )
        .expect("a read cannot fail");
    assert!(effect.events().is_empty());

    settle(&Device, Some(21.5), effect);
    assert_eq!(asker.take_incoming(), [Incoming::Message(Some(21.5))]);
}
// ANCHOR_END: reply

struct Device;

impl EventSourced for Device {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Recorded;
    type State = Option<f64>;
    type Snapshot = Nothing;
    type Error = InvalidReading;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("device", "left").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn handle(
        &self,
        _: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let Incoming::Message(command) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        match command {
            Command::Record(degrees) if degrees.is_nan() => Err(InvalidReading),

            Command::Record(degrees) => Ok(Effect::persist(Recorded(degrees))),

            Command::Read(reply_to) => {
                Ok(Effect::none().then(move |last_reading| reply_to.reply(*last_reading)))
            }
        }
    }

    fn apply(&self, _: Self::State, Recorded(degrees): Self::Event) -> Self::State {
        Some(degrees)
    }
}

enum Command {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Recorded(f64);

impl Versioned for Recorded {
    const MANIFEST: &'static str = "recorded";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

#[derive(Debug, Error)]
#[error("reading is not a number")]
struct InvalidReading;
