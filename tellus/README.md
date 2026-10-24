<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://tellus.rs/brand/lockup-dark.svg">
    <img alt="tellus" src="https://tellus.rs/brand/lockup.svg" height="56">
  </picture>
</h1>

**Typed actors for Rust, on Tokio.**

[![Crates.io][crates-badge]][crates-url]
[![license][license-badge]][license-url]
[![build][build-badge]][build-url]
[![docs][docs-badge]][docs-url]
[![benchmarks][benchmarks-badge]][benchmarks-url]
[![comparison][comparison-badge]][comparison-url]

[crates-badge]: https://img.shields.io/crates/v/tellus
[crates-url]: https://crates.io/crates/tellus
[license-badge]: https://img.shields.io/github/license/tellus-rs/tellus
[license-url]: https://github.com/tellus-rs/tellus/blob/main/LICENSE
[build-badge]: https://img.shields.io/github/actions/workflow/status/tellus-rs/tellus/ci.yml
[build-url]: https://github.com/tellus-rs/tellus/actions/workflows/ci.yml
[docs-badge]: https://img.shields.io/docsrs/tellus/latest
[docs-url]: https://docs.rs/tellus/latest/tellus/
[benchmarks-badge]: https://img.shields.io/badge/benchmarks-dashboard-informational
[benchmarks-url]: https://tellus.rs/tellus/dev/bench/
[comparison-badge]: https://img.shields.io/badge/comparison-dashboard-informational
[comparison-url]: https://tellus.rs/tellus/comparison/

An actor framework for Rust, built on [Tokio](https://tokio.rs): typed messages, supervision
trees, death watch with an ordering guarantee, optional event-sourced persistence and optional
clustered remoting over QUIC. Inspired by Carl Hewitt's
[Actor Model](https://en.wikipedia.org/wiki/Actor_model) and strongly influenced by Akka, continued
as open source by [Apache Pekko](https://pekko.apache.org).

tellus is under active development and its API is still settling.

## Highlights

- **Typed actors as state machines**: `receive` maps the current state and a message to the next
  state, no `&mut self`, no async in actor code.
- **Supervision tree**: actors form a tree below the root actor of an `ActorSystem` and stop
  bottom-up.
- **Fire-and-forget messaging**: `ActorRef::tell` never blocks and delivers at most once;
  undeliverable messages are logged as dead letters (structured logging via `tracing`).
- **Request-response**: `ActorRef::ask` from outside the actor tree, `ActorContext::reply_to`
  between actors, keeping actor code free of futures.
- **Death watch with an ordering guarantee**: a terminated signal proves the watcher has seen every
  message from that actor it will ever see; `unwatch` holds even against an enqueued signal.
- **Supervision strategies**: on an error or panic, `Restart` with a restart limit and exponential
  backoff, or `Stop`.
- **Bounded or unbounded mailboxes**: bounded ones drop messages beyond capacity as dead letters
  but never drop terminated signals.
- **Event-sourced persistence** (feature `persistence`): events are appended to a pluggable store
  and only then applied, the state is recovered by replay, and conditional appends fence
  concurrent incarnations.
- **Clustered remoting** (feature `cluster`): actors on other nodes are told, asked and watched
  through the same API, over QUIC with mutual TLS binding each node to the address it advertises.
  It comes with seed discovery, gossiped membership, pluggable failure detection and downing,
  drained departures, a versioned cluster state watch, a cluster-wide receptionist and pub-sub on
  top of it. Message types travel by an application-chosen manifest, and every cluster has an ID,
  so two clusters refuse each other rather than merge. With the feature off, none of its
  dependencies are pulled in.

## Getting started

tellus is available on [crates.io](https://crates.io/crates/tellus):

```sh
cargo add anyhow
cargo add tokio --features macros,rt-multi-thread
cargo add tellus
```

A minimal actor system with a single actor which handles one message and stops:

```rust
use anyhow::Context;
use std::convert::Infallible;
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Greeter);

    system.root().tell(Greet("Tellus".to_string()));

    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}

struct Greeter;

impl Actor for Greeter {
    type Message = Greet;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(Greet(name)) = incoming {
            println!("Hello, {name}!");
        }
        Ok(Control::Stop)
    }
}

struct Greet(String);
```

`ActorSystem::new` and `ActorContext::spawn` use the default `ActorConfig`; use
`ActorSystem::with_config` and `ActorContext::spawn_with_config` to choose a mailbox capacity or
supervision strategy.

For remoting, enable the `cluster` feature:

```toml
tellus = { git = "https://github.com/tellus-rs/tellus", features = [ "cluster" ] }
```

For development and tests there is also `cluster-dev`, which adds `QuicTransport::dev`, a transport
that does not verify certificates. It is a separate feature so it cannot end up in a production
build which does not ask for it.

## Core concepts

This is a short tour. For the full picture, top-down with links into the implementation, see
[docs/actors.md](../docs/actors.md), and [docs/cluster.md](../docs/cluster.md) for remoting.

### Actors and state

An actor is defined by implementing the `Actor` trait. `init` creates the initial state, possibly
spawning child actors or sending messages. `receive` gets the current state by value along with an
incoming message or signal and designates the state for the next one: a state machine rather than
mutation. For stateless actors the state is `()`; actors which never receive messages (pure
supervisors, for example) use the uninhabited `Nothing` as message type.

The `Error` associated type makes a failure an ordinary return value (`Infallible` for actors
that cannot fail). Inside `receive`, use `?` to escalate a failure to supervision and an explicit
`match` to handle it as part of the domain.

`receive` is synchronous and runs on a Tokio worker: an actor cannot be stopped while `receive` is
running, so a `receive` which never completes keeps all its ancestors from terminating. For long
running or blocking work, use `ActorContext::pipe_to_self`: it runs a future on its own task and
delivers the output to the actor as an ordinary message. For a deadline, a retry or an idle timeout,
use `ActorContext::start_timer`: it delivers a message to the actor itself once a delay has elapsed.
A timer replaced under its key or canceled with `ActorContext::cancel_timer` is never received.

### The actor tree and termination

Creating an `ActorSystem` spawns the root actor; every actor can spawn children via
`ActorContext::spawn`. When an actor stops (by returning `Control::Stop`, by failing under the
`Stop` strategy, or because its parent stopped), its children are stopped first; only once all
descendants have terminated does it terminate itself. Consequently `ActorSystem::terminated`
resolves exactly when the entire tree has terminated.

### Messaging

`ActorRef::tell` is non-blocking, fire-and-forget and at-most-once. If the actor has terminated,
or its bounded mailbox is full, the message is dropped and logged as a dead letter. Delivery does
not imply processing: even a delivered message may go unprocessed if the actor stops before
getting to it. The contract holds unchanged for an actor on another node, where an unreachable
node, a full outbound queue and an undecodable payload are dead letters as well.

Request-response builds on the same delivery: a request message carries a `ReplyTo`, a single-shot
reply destination consumed by `reply`. From outside the actor tree, `ActorRef::ask` sends the
request and awaits the reply. It returns an `AskError` instead of only logging when the mailbox is
full, the actor has terminated or it is detected that no reply can arrive anymore. That detection
is best-effort, so every ask carries a timeout which resolves the future at the latest when it
elapses. Between actors, `ActorContext::reply_to` creates a `ReplyTo` which delivers the reply
into the asking actor's own mailbox, converted into its message type, so the reply arrives through
`receive` like any other message.

### Watch

`ActorContext::watch` registers interest in another actor's termination: the watcher receives an
`Incoming::Terminated` signal carrying the terminated actor's `ActorId`. The signal is ordered
behind all messages the terminated actor has delivered to the watcher. Receiving it hence proves
the watcher has seen every message from that actor it will ever see: each arrived before the
signal or was dropped as a dead letter; see
[`05_query.rs`](examples/tutorials/getting_started/05_query.rs) for putting this to work.
Watching an actor that has already terminated delivers the signal right away, and terminated
signals are delivered even when a bounded mailbox is full. `ActorContext::unwatch` stops watching:
after it returns, no terminated signal for that actor is received, even if the signal was already
enqueued.

Actors on other nodes are watched the same way. A signal travelling from there keeps the ordering
guarantee, but one synthesized because the node was downed can only promise that no further
message from that actor will ever arrive; see [docs/cluster.md](../docs/cluster.md).

### Supervision

Each actor is configured with a `SupervisionStrategy` deciding what happens when `init` or
`receive` returns an error or panics: `Stop` terminates the actor, `Restart` stops its children
and re-runs `init` for a fresh state. Restarts are limited and paced by a `RestartPolicy`: they
back off exponentially between the backoff's `min` and `max`, the actor stops when it fails again
after `max_restarts` restarts, and running for `reset_after` without failure resets that count.
Failures are logged at error level either way.

### Configuration

`ActorConfig` currently holds the mailbox capacity and the supervision strategy:

```rust
use std::num::NonZeroUsize;
use tellus::{ActorConfig, MailboxCapacity};

let config =
    ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
```

Pass it to `ActorSystem::with_config` for the root actor or to `ActorContext::spawn_with_config`
for a child.

With the `serde` feature the configuration is deserializable, `ActorConfig` as well as the
cluster's `EndpointConfig`, `BootstrapConfig` and `QuicConfig`, so it can be read from a config
file, with human readable durations:

```toml
mailbox_capacity = { bounded = 100 }

[supervision_strategy.restart]
max_restarts = 3
reset_after  = "30s"
backoff      = { min = "250ms", max = "4s" }
```

```yaml
mailbox_capacity:
  bounded: 100

supervision_strategy:
  restart:
    max_restarts: 3
    reset_after: 30s
    backoff:
      min: 250ms
      max: 4s
```

tellus stays format agnostic and pulls in no parser of its own, so picking the loader is up to the
application. [`config`](https://crates.io/crates/config) is the recommended one: it normalizes
every format into one value tree before deserializing, which makes the YAML above plain maps,
and it reports the key path along with any error. Note that `serde_yaml` deserializes the
same types differently, expecting a YAML tag (`supervision_strategy: !restart`) instead.

Anything omitted falls back to its default. Invalid backoff bounds are rejected rather than
silently repaired: `Backoff::new` is fallible and deserialization goes through it, so an invalid
pair is unrepresentable whether it comes from code or from a file. Bounds which contradict each
other are one case, a zero minimum the other, since that would make every step zero and hence the
backoff no backoff at all:

```text
max backoff 1s below min backoff 10s for key `supervision_strategy.backoff`
min backoff is zero for key `supervision_strategy.backoff`
```

The cluster configuration works the same way, with the `cluster` and `serde` features. In the
`endpoint` and `bootstrap` sections only the advertised address is required; everything else falls
back to the `DEFAULT_*` constant of its field, except that a `reconnect_backoff` needs both `min`
and `max`. Every field of the `transport` section is required. Each pluggable family is chosen by
the name of one of the provided implementations. The three top-level keys below belong to no
tellus type: the application defines the struct holding them.

```yaml
endpoint:
  advertised_addr: 10.0.0.1:7878
  heartbeat_interval: 1s
  failure_detector:
    phi_accrual:
      threshold: 8.0
  downing_provider:
    keep_majority:
      after: 10s
  reconnect_backoff:
    min: 250ms
    max: 3s

bootstrap:
  min_peers: 5
  settle: 3s
  formation: majority
  probe_interval: 30s

transport:
  bind_addr: 0.0.0.0:7878
  cert_chain: /etc/tellus/tls/cert.pem
  key: /etc/tellus/tls/key.pem
  roots: /etc/tellus/tls/ca.pem
  server_name: tellus
```

A custom codec, failure detector, downing provider or formation provider is not something a
config file can name, so those stay code: the fields are public and take the implementation after
the config was loaded. Validation holds across the boundary here too. An advertised address with
an unspecified IP or a zero port, a zero heartbeat interval, a frame size too small for one member
or one reachability record and a zero resolve interval are refused by deserialization exactly as by
`start_endpoint` and `bootstrap`. The seed
addresses are configuration as well: a plain list for `FixedSeeds`, the DNS query of
[tellus-bootstrap-dns](../tellus-bootstrap-dns) or the pod selector of
[tellus-bootstrap-k8s](../tellus-bootstrap-k8s). `QuicConfig` names its PEM files by path,
which `QuicTransport::from_config` reads.

### Event-sourced persistence

With the `persistence` feature, an actor can be event sourced by implementing `EventSourced`
instead of `Actor` and spawning it via `ActorSystem::event_sourced` or
`ActorContext::spawn_event_sourced`. `handle` validates a command against the current state and
returns an `Effect` naming the events it causes. The events are appended to an `EventStore` and
only then folded into the state by `apply`. After a crash or a restart the state is recovered by
replaying the events, optionally shortcut by snapshots. The stores are pluggable;
[`tellus-persistence-postgres`](../tellus-persistence-postgres) provides PostgreSQL-backed ones,
and the `persistence-tests` feature adds the contract test suite any store implementation must
pass, meant for a backend crate's integration tests. The `persistence-in-memory` feature adds
`InMemoryStore`, which keeps events and snapshots in memory, so event-sourced actors can be tested
without a database. For the guarantees, from replay equals live execution to fencing and schema
evolution, see [docs/persistence.md](../docs/persistence.md).

## Testing

The `test-util` feature adds `testing::TestContext`, which provides the `ActorContext` that `init`,
`receive`, `handle` and `recovered` require, so an actor's logic can be unit tested by calling
those methods directly, without a running actor system. Everything the actor sends to itself can
be read back in order. Together with the `persistence` feature, `testing::settle` runs the `Effect`
returned by `handle` against the entity's state, without a store. See the Testing sections of
[docs/actors.md](../docs/actors.md) and [docs/persistence.md](../docs/persistence.md).

## Examples

The examples in [`examples/tutorials/getting_started`](examples/tutorials/getting_started) are the
code of the [getting started tutorial](https://tellus.rs/docs/tutorials/getting-started/), one
program per step: from a single actor via child actors, watch and a per-request query to
supervision and a device manager on top. Each step builds on the previous one. All examples print
their results to stdout; those which set up logging log to stderr, with the log level configured
via `RUST_LOG`.

```shell
cargo run --quiet -p tellus --example getting_started_01_hello
```

The examples in [`examples/tutorials/clustered`](examples/tutorials/clustered) are the code of the
[clustered actors tutorial](https://tellus.rs/docs/tutorials/clustered-actors/), which spreads the
device manager over a cluster: two nodes bootstrapping from fixed seeds, messages that cross the
wire, the receptionist, a node that dies and one that leaves. Each step starts its other nodes as
child processes, so one command runs the whole cluster:

```shell
cargo run --quiet -p tellus --features cluster-dev --example clustered_01_two_nodes
```

The examples in [`examples/how_to`](examples/how_to) belong to the
[how-to guides](https://tellus.rs/docs/how-to/), among them shutting a node down gracefully
(`how_to_shut_down_gracefully`), observing a cluster (`how_to_observe_a_cluster`) and securing it
with mutual TLS (`how_to_secure_with_mutual_tls`, which needs `--features cluster,serde`). The
guides for running on Kubernetes and for serving the cluster state over HTTP use examples in
[`tellus-bootstrap-k8s`](../tellus-bootstrap-k8s/examples) and
[`tellus-cluster-inspect`](../tellus-cluster-inspect/examples).

Beyond the tutorials:

- [`remote_scatter_gather`](examples/remote_scatter_gather.rs): a scatter-gather across two nodes,
  where a root actor scatters a workload across worker actors on another node and gathers their
  partial results through a serialized `reply_to: ActorRef<Partial>`. The worker node joins the
  gatherer's cluster via `cluster::join`, and the worker pool is found by name and address through
  `cluster::register` and `receptionist::lookup_at`. It starts the second node as a child process,
  so one command runs both:

  ```shell
  RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example remote_scatter_gather
  ```

- [`cluster`](examples/cluster.rs): a four node cluster where three member nodes join through one
  seed address and learn the rest from gossip, each node asked what it sees rather than trusted.
  Then one node is killed, the survivors down it, and a death watch on an actor of that node fires
  a synthesized terminated signal. Stopping the survivors at the end shows the other way out, an
  announced departure. It starts the member nodes as child processes, so one command runs the
  whole cluster:

  ```shell
  RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster-dev --example cluster
  ```


- [`work_pulling`](examples/work_pulling.rs): workers request jobs from a manager whenever they
  are ready, so a bounded mailbox of capacity one suffices, which gives backpressure without
  dropping work:

  ```shell
  RUST_LOG=tellus=debug cargo run --quiet -p tellus --example work_pulling
  ```

The event-sourced examples live with the PostgreSQL stores in
[`tellus-persistence-postgres`](../tellus-persistence-postgres); both need the `postgres` service
from the root `docker-compose.yaml`:

```shell
docker compose up -d --wait postgres
RUST_LOG=tellus=debug cargo run --quiet -p tellus-persistence-postgres \
  --example event_sourced_counter
RUST_LOG=tellus=debug cargo run --quiet -p tellus-persistence-postgres \
  --example event_sourced_supervision
```

## Status and roadmap

tellus is under active development; expect the API to change without notice.

The open items on the remoting side are listed under the limitations in
[docs/cluster.md](../docs/cluster.md). What comes next, the cluster singleton and then sharding,
is laid out in [docs/roadmap.md](../docs/roadmap.md).

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
