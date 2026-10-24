# Persistence

This document defines the guarantees of tellus persistence: event sourcing with optional
snapshots, behind the `persistence` feature. The implementation lives in
[`tellus/src/persistence`](../tellus/src/persistence) and the integration tests in
[`persistence.rs`](../tellus/tests/persistence.rs), which runs them over the in-memory store of
the `persistence-in-memory` feature, encode these guarantees; everything builds on
the mechanics and guarantees of [actors.md](actors.md).

## Overview

An event-sourced actor persists what happened, not what the state is. Commands arrive as ordinary
messages; [`EventSourced::handle`](../tellus/src/persistence/event_sourced.rs) validates a command
against the current state and decides which events it causes; the events are appended to an event
store and only then applied via `apply`, the one and only state transition. The state is therefore
a fold of the persisted events over a seed and is itself never stored: after a crash, a restart or
a redeployment it is recovered by replaying the events. Snapshots are an optional shortcut for
that replay, a discardable derivative of the events, never a source of truth.

The stores are pluggable behind two traits,
[`EventStore` and `SnapshotStore`](../tellus/src/persistence/store.rs), which operate on encoded
bytes; implementations live in separate backend crates, except the in-memory one for tests, which
`tellus` provides behind the `persistence-in-memory` feature. Everything below holds for any
conforming store.

## Defining an event-sourced actor

The [`EventSourced`](../tellus/src/persistence/event_sourced.rs) trait follows
[`Actor`](../tellus/src/actor.rs), split along the event-sourcing boundaries:

- `Command` is the type of the received messages; `Event` and `Snapshot` are the persisted types,
  both [`Versioned`](../tellus/src/persistence/versioned.rs) for schema evolution; actors without
  snapshots use the uninhabited `Nothing`.
- `persistence_id` names the actor's event stream.
- `init` is the pure seed of the fold and `init_from_snapshot` its snapshot-based alternative;
  `recovered` runs once after recovery and is the place to touch the world.
- `handle` takes the state by reference and returns an
  [`Effect`](../tellus/src/persistence/effect.rs): `persist`/`persist_all` name the caused events,
  `stop`/`and_stop` stop the actor, and `then` attaches a continuation running once the events are
  durable and applied, the only safe place for outward-facing actions.
- `apply` folds one event into the state and `snapshot` optionally offers a snapshot after an
  effect's events have been applied.

Spawning uses dedicated entry points carrying the [`Persistence`](../tellus/src/persistence.rs)
wiring, an event store plus optional snapshot store and codec: `ActorSystem::event_sourced` for
the root and `ActorContext::spawn_event_sourced` for children, each with a `_with_config` variant
taking the ordinary `ActorConfig`. Unlike a plain actor value, which only needs `Send`, an
event-sourced one must also be `Sync`: it is borrowed across the store awaits during recovery and
settlement. Everything else, mailboxes, supervision, death watch and termination, works exactly as
for plain actors.

## Identity

An event stream is identified by a `PersistenceId`, an application-chosen pair of `entity_type`
and `entity_id` which is stable across incarnations: every spawn of "order 42" names the same
stream. The identity remains stable across time and nodes. It is unrelated to `ActorId`, which is
fresh per spawn and distinguishes incarnations; over time many `ActorId`s serve one
`PersistenceId`.

## Recovery

On every spawn and every restart, before the first command:

1. The latest snapshot, if any, is loaded; `init_from_snapshot` turns it into the state covering
   every event before the snapshot's sequence number. Without a snapshot, `init` produces the seed
   state.
2. The events from that sequence number on are read in order and folded via `apply`.
3. `recovered` runs exactly once on the resulting state. This is the place to touch the world:
   spawn children, re-arm timers, register with other actors.

`init` is skipped entirely when a snapshot exists, so it must be nothing but the pure seed of the
fold; world-touching work in `init` would silently disappear the moment snapshots are enabled.
`recovered` always runs, and never during replay.

`init` has no context, so timers (see [actors.md](actors.md#timers)) start in `recovered` and
`handle`. A timer is runtime state, neither persisted nor recreated by replay, and a restart cancels
it. `recovered` runs on every restart, so it is where to start the timer again. A `then`
continuation gets only the state and has no context, so it cannot start a timer.

## Replay and live execution

`apply` is pure and total: no I/O, no failure, no dependence on the clock, on randomness or on
per-incarnation values such as `ActorRef`s. Replay runs exactly the `apply` calls the live actor
ran, on the same events in the same order, and therefore reconstructs the same state. Effects
never run during replay; recovery is observable only through `recovered`.

## Effects and durability

`handle` returns an `Effect`: which events to persist, whether to stop, and what to do afterwards.
Events are durable, as defined by the store, before they are applied. A continuation attached via
`then` runs after its events are durable and applied, and never on replay: it is the only safe
place for outward-facing actions such as replying to an ask or telling another actor. On an effect
without events, `then` runs right after `handle`.

All events of one `Effect` are appended atomically: after a crash the stream contains all of them
or none, never a prefix.

Durability of events is guaranteed; delivery of effects is not. A crash between the append and
`then` loses the continuation, and it is not re-run on recovery, so outward-facing actions are
at-most-once, exactly like `tell`.

## Ordering

Writes are strict: a command is fully settled, meaning its events are durable and applied and its
`then` has run, before the next incoming is taken from the mailbox. The mailbox guarantees of
[actors.md](actors.md) carry over unchanged: messages from one sender arrive in send order, and a
terminated signal arrives behind all messages the terminated actor delivered. A parent's stop is
honored between commands and at any point during recovery, never in the middle of one command
settling, and the termination sequence is the same as for any actor.

## Failure and supervision

A failure of `handle`, a panic, or a failed append is an actor failure and consults the
`SupervisionStrategy`, exactly like a failure of `receive` in a plain actor. `Restart` recovers by
replaying from the store, which also reconciles an ambiguous append. If the append reached the
store before the failure, replay picks its events up. If not, they were never applied either,
since `apply` runs only after the append. There is no retry logic besides supervision.

Failures during recovery divide in two. A failure of `init`, `init_from_snapshot`, event decoding
or `apply` is deterministic: a restart fails identically, and so it escalates through the restart
limit to a stop. An undecodable snapshot is the one exception, discarded in favor of full replay
(see schema evolution below). A failure of `recovered`, or a failure or panic of a store itself
while loading the snapshot or reading events, is an ordinary startup failure, e.g. a struggling
dependency, which a restart with backoff may well fix. A failed snapshot load is deliberately not
traded for full replay: only an undecodable snapshot is discarded; a snapshot store failure
surfaces through supervision instead of silently turning every recovery into a full replay.

A failure to save a snapshot is logged and never fails the actor: losing a snapshot only makes the
next recovery replay more events.

## Fencing

Every append is conditional on the expected next sequence number, and the store rejects an
append whose expectation does not match the stream head, below or above. Hence no two appends
extend a stream from the same head, and each append batch is atomic. Two incarnations of one
`PersistenceId` can be alive at once, through a network partition under remoting or through plain
misconfiguration. Then one of them loses each append race: its failure goes through supervision,
and a restarted loser replays the winner's events instead of overwriting them. The stream never
loses events, and the store is the arbiter, not the nodes.

This is optimistic concurrency, not exclusive ownership. A restarted loser may win the next
append race, so two live incarnations can alternate as writers of one stream, each append
consistent with the events before it. Keeping one writer for a whole period needs an ownership
fence in the store, which tellus does not provide.

## Sequence numbers

Sequence numbers are per `PersistenceId`, gapless, and start at 0, carried by the `SeqNo` type. A
`SeqNo` names either a stored event's position or the position at which the next event is
appended, which is `SeqNo::ZERO` for an empty stream. One type covers both because every use is a
position replay resumes at, never one it resumes after: a snapshot records the sequence number at
which replay resumes, the fencing expectation is the position the next append claims, and a read
starts at the given position inclusively. Nothing in the persistence path adds or subtracts one.

## Schema evolution

Events outlive the code that wrote them, so the stored representation carries what evolution
needs. Every stored event and snapshot carries a manifest, a stable name independent of the Rust
type path, and a schema version, both outside the payload
([`Versioned`](../tellus/src/persistence/versioned.rs)); the payload itself is encoded
self-describingly by a [`Codec`](../tellus/src/persistence/codec.rs), CBOR by default. A custom
codec wraps its serializer's errors via `EncodeError::new` and `PayloadError::new`. Old versions
can be upcast on read by overriding `Versioned::decode`: the current code declares which versions
it still decodes and converts them while replaying; a version which is neither current nor upcast
is rejected on read, and the store is never rewritten. A rejected event fails recovery; a snapshot
that can no longer be decoded is discarded and recovery falls back to full replay, so snapshots
never need migration code.

`EncodedEvent::manifest` and `EncodedSnapshot::manifest` are `Cow<'static, str>`: framework-created
values borrow `Versioned::MANIFEST` without allocating per event, while stores use the owned form
for manifests read from durable storage. Custom stores should bind or compare `manifest.as_ref()`
and build a value read from storage with `.into()` on the `String`.

## Testing

The methods of `EventSourced` are plain synchronous functions, so an entity's logic can be unit
tested without a system, a mailbox or a store. `init`, `init_from_snapshot`, `apply` and
`snapshot` take no context at all; replaying events is a fold of `apply` over `init`. `handle` and
`recovered` need an `ActorContext`, which `TestContext` provides with the `test-util` feature.
`Effect` is inspectable with `events` and `stop_requested`, and `settle` runs it the way
settlement does once the events are durable: it applies the events to the state in order and then
runs the `then` continuations in order on the resulting state. Whatever a continuation sends to
the entity itself is read back with `take_incoming`.

```rust
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tellus::{
    ActorContext, Effect, EventSourced, Incoming, Nothing, PersistenceId, ReplyTo,
    SchemaVersion, Versioned,
    testing::{TestContext, settle},
};

struct Counter;

enum Command {
    Increase(ReplyTo<u64>),
    Total(u64),
}

#[derive(Serialize, Deserialize)]
struct Increased;

impl Versioned for Increased {
    const MANIFEST: &'static str = "increased";
    const VERSION: SchemaVersion = SchemaVersion::new(1);
}

impl EventSourced for Counter {
    type Command = Command;
    type Event = Increased;
    type State = u64;
    type Snapshot = Nothing;
    type Error = Infallible;

    fn persistence_id(&self) -> PersistenceId {
        PersistenceId::new("counter", "1").expect("the segments are valid")
    }

    fn init(&self) -> Result<Self::State, Self::Error> {
        Ok(0)
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
        match incoming {
            Incoming::Message(Command::Increase(reply_to)) => {
                Ok(Effect::persist(Increased).then(move |count| reply_to.reply(*count)))
            }

            _ => Ok(Effect::none()),
        }
    }

    fn apply(&self, count: Self::State, _: Self::Event) -> Self::State {
        count + 1
    }
}

let mut test_context = TestContext::new();
let reply_to = test_context.context().reply_to(Command::Total);

let effect = Counter
    .handle(
        test_context.context(),
        Incoming::Message(Command::Increase(reply_to)),
        &0,
    )
    .unwrap();
assert_eq!(effect.events().len(), 1);
assert!(!effect.stop_requested());

let settlement = settle(&Counter, 0, effect);
assert_eq!(settlement.state, 1);
assert!(!settlement.stop);
assert!(matches!(
    test_context.take_incoming().as_slice(),
    [Incoming::Message(Command::Total(1))]
));
```

`settle` leaves out everything about the store: encoding, appending, snapshots and sequence
numbers. Those are covered by the contract suite every store passes, and by the tests of
`tellus` itself, which run the guarantees above over an in-memory store. A test which spawns the
entity, sends a command with `tell` and then reads the store has to wait for the settlement, since
`tell` returns before the command is handled; it mostly tests those guarantees again.

## Guarantees and limitations

- An appended event is never modified or deleted: the stream is append-only and is the single
  source of truth.
- The recovered state is exactly the live state: the same fold over the same events, with effects
  suppressed and `recovered` as the only world-touching step.
- Events of one effect are atomic and durable before they are applied; sequence numbers are
  gapless per `PersistenceId`.
- Conditional appends fence concurrent incarnations per append: of two appends from the same
  head one wins, the loser fails into supervision and replays. This is optimistic concurrency, not
  exclusive ownership: two live incarnations can alternate as writers.
- Outward-facing actions are at-most-once: a crash can lose a `then` continuation, never an
  event.
- Strict writes serialize commands: nothing overlaps a command's settlement, which bounds
  throughput per actor by store latency.
- Saving a due snapshot is part of settlement, so a command which triggers one also waits for the
  save. Snapshots shorten recovery; they do not speed up writes.
