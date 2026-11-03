#![cfg(all(feature = "test-util", feature = "persistence"))]

use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tellus::{
    ActorContext, Effect, EventSourced, Incoming, Nothing, PersistenceId, SchemaVersion, Unstashed,
    Untimed, Versioned,
    testing::{TestContext, settle},
};

/// `handle` only decides: its events and stop flag are inspectable, and no continuation has run.
#[test]
fn handle_decides_without_running_continuations() {
    let mut test_context = TestContext::new();

    let effect = Ledger
        .handle(
            test_context.context(),
            Incoming::Message(Command::Deposit(vec![1, 2])),
            &vec![],
        )
        .unwrap();

    assert_eq!(effect.events(), [Deposited(1), Deposited(2)]);
    assert!(!effect.stop_requested());
    assert!(test_context.take_incoming().is_empty());
}

/// Events are applied in order before the continuations run, and the continuations see the
/// resulting state and run in the order they were added.
#[test]
fn settle_applies_events_then_runs_continuations_in_order() {
    let mut test_context = TestContext::new();
    let effect = Ledger
        .handle(
            test_context.context(),
            Incoming::Message(Command::Deposit(vec![1, 2])),
            &vec![],
        )
        .unwrap();

    let settlement = settle(&Ledger, vec![], effect);

    assert_eq!(settlement.state, [1, 2]);
    assert!(!settlement.stop);
    assert_eq!(
        test_context.take_incoming(),
        [
            Incoming::Message(Command::Observed(2)),
            Incoming::Message(Command::Observed(3)),
        ]
    );
}

#[test]
fn an_effect_without_events_still_runs_its_continuations() {
    let mut test_context = TestContext::new();
    let effect = Ledger
        .handle(
            test_context.context(),
            Incoming::Message(Command::Poll),
            &vec![5],
        )
        .unwrap();
    assert!(effect.events().is_empty());

    let settlement = settle(&Ledger, vec![5], effect);

    assert_eq!(settlement.state, [5]);
    assert_eq!(
        test_context.take_incoming(),
        [Incoming::Message(Command::Observed(1))]
    );
}

#[test]
fn settle_reports_the_stop() {
    let test_context = TestContext::new();
    let effect = Ledger
        .handle(
            test_context.context(),
            Incoming::Message(Command::Close),
            &vec![],
        )
        .unwrap();
    assert!(effect.stop_requested());

    let settlement = settle(&Ledger, vec![], effect);

    assert!(settlement.stop);
}

/// Recovery is a fold of the stored events over the seed, which needs no context at all.
#[test]
fn replaying_events_recovers_the_state() {
    let events = [Deposited(4), Deposited(5)];

    let state = events
        .into_iter()
        .fold(Ledger.init().unwrap(), |state, event| {
            Ledger.apply(state, event)
        });

    assert_eq!(state, [4, 5]);
}

struct Ledger;

#[derive(Debug, PartialEq)]
enum Command {
    Deposit(Vec<u64>),
    Poll,
    Close,
    Observed(u64),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Deposited(u64);

impl Versioned for Deposited {
    const MANIFEST: &'static str = "deposited";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

impl EventSourced for Ledger {
    type Command = Command;
    type Timers = Untimed;
    type Stash = Unstashed;
    type Event = Deposited;
    type State = Vec<u64>;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("ledger", "1").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(vec![])
    }

    fn init_from_snapshot(&self, snapshot: Self::Snapshot) -> Result<Self::State, Self::Error> {
        match snapshot {}
    }

    fn handle(
        &self,
        context: &ActorContext<Self::Command>,
        incoming: Incoming<Self::Command>,
        _: &Self::State,
    ) -> Result<Effect<Self>, Self::Error> {
        let this = context.self_ref().clone();
        let then_this = this.clone();

        match incoming {
            Incoming::Message(Command::Deposit(amounts)) => Ok(Effect::<Self>::persist_all(
                amounts.into_iter().map(Deposited),
            )
            .then(move |ledger| this.tell(Command::Observed(ledger.len() as u64)))
            .then(move |ledger| then_this.tell(Command::Observed(ledger.iter().sum())))),

            Incoming::Message(Command::Poll) => Ok(Effect::<Self>::none()
                .then(move |ledger| this.tell(Command::Observed(ledger.len() as u64)))),

            Incoming::Message(Command::Close) => Ok(Effect::stop()),

            _ => Ok(Effect::none()),
        }
    }

    fn apply(&self, mut ledger: Self::State, Deposited(amount): Self::Event) -> Self::State {
        ledger.push(amount);
        ledger
    }
}
