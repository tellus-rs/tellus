# Roadmap

This document lays out the layers built on clustered remoting, those shipped and those still ahead,
and what each piece depends on. It is a
strategy, not a schedule: each layer is small enough to ship on its own, and each is started only
once what it depends on has landed. The design documents remain the authority for what is built:
[actors.md](actors.md) for the core, [persistence.md](persistence.md) for event sourcing and
[cluster.md](cluster.md) for remoting; every layer below ends with its guarantees written into one
of them.

## Where tellus stands

The core is complete for its purpose: actors as state machines with one Tokio task each, bounded
mailboxes, death watch with the ordering guarantee, supervision with restart and backoff. Timers are
opt-in per actor type, and a [`Host`](../tellus/src/host.rs) spawns entities identified by a key on
demand and can passivate them after an idle timeout. Two extensions have landed behind features.
`persistence` adds event-sourced actors whose replay equals live execution, with effects gated on
durability, fencing via conditional append and schema evolution via manifests;
[`tellus-persistence-postgres`](../tellus-persistence-postgres) is the first store. `cluster` makes
`ActorRef` serializable, so actors on different nodes tell, ask and watch each other through the
same API. It adds membership, gossip, failure detection, pluggable downing and a drained,
acknowledged leave. A discovery registry with `register` names message types by an
application-chosen `Manifest`, the receptionist replicates those registrations, and pub-sub sends
one multicast frame per node and stream on top of it. A versioned cluster state reports the members,
the unreachable ones, broken links and foreign clusters. Bootstrap goes through seed discovery with
the [DNS](../tellus-bootstrap-dns) and [Kubernetes](../tellus-bootstrap-k8s) backends. Every cluster
has an ID which every handshake carries, so two clusters refuse each other rather than merge. The
handshake negotiates the protocol version, and mutual TLS binds each node to the address it
advertises. [`tellus-cluster-demo`](../tellus-cluster-demo) holds five nodes of that to convergence
and cross-node messaging under continuous crashes, departures and partitions.

The known gaps of the remoting layer are listed under the limitations in
[cluster.md](cluster.md#limitations). Two building blocks were missing when this plan was written,
and the features people ask for next (singleton, sharding) need at least one of them. Both have
shipped:

- **Cluster state was a snapshot.** `cluster::members()` answered what the member list was at the
  moment of the call, and reachability, an observation derived from relayed edges rather than
  member state (see [cluster.md](cluster.md#failure-detection-and-node-death)), was not exposed at
  all, so nothing outside the module could react to a change. Layer 1 below closed this gap:
  `cluster::cluster_state()` publishes a versioned value on every change (see
  [cluster.md](cluster.md#cluster-state)).
- **Discovery** is now the receptionist. `receptionist::lookup_at` resolves a key at one address and
  `receptionist::lookup` across every Up member, both from the replicated registrations, see
  [cluster.md](cluster.md#discovery): more than one actor may hold a key, and node identity stays
  out of `Key`.

## Principles

- **Nothing changes an existing guarantee.** Every layer is additive and gated behind `cluster`
  (a new feature only where a layer pulls in a dependency of its own). The tell contract stays
  fire-and-forget and at-most-once, and no layer promises delivery the transport cannot give.
- **Uniqueness is conditioned, and the condition is spelled out.** A singleton or a shard owner is
  unique once the state it is elected from has converged, membership and the receptionist alike,
  and once the stops which resolve the concurrent claims or the moved shards have completed.
  Between a partition and the downing decision two instances can exist; that window is fundamental
  for the same reason the synthesized terminated signal is, and the default `KeepMajority` provider
  bounds it rather than closing it. Each layer says so in its guarantees section instead of
  implying more.
- **Fencing serializes, it does not exclude.** The conditional append of
  [persistence.md](persistence.md#fencing) guarantees that two incarnations never both extend a
  stream from the same head. It does not keep the loser out: the loser fails into supervision,
  restarts, replays the winner's events and may win the next append, so two live incarnations can
  alternate as writers. A layer which needs one writer for the whole of the window needs an
  ownership fence in the store, which is a design item of its own below, not something the existing
  fence provides.
- **Every layer is proven the same way.** A scenario in the multi-process
  [`cluster.rs`](../tellus/tests/cluster.rs) test for the guarantee, an invariant in the chaos
  demo's [verifier](../tellus-cluster-demo/src/bin/verifier.rs) for the behavior under partitions,
  and a section in the design document.

## Dependencies

The layers form a small graph, not a chain:

- Cluster state depends on nothing.
- The receptionist depends on cluster state for its settled predicate, which is scoped to a
  cluster state version. Its eviction of Down incarnations does not go through the watch channel;
  it is part of the node death sequence.
- Pub-sub depends on the receptionist only.
- The singleton depends on the receptionist, which is its election medium, and, for a persistent
  singleton, on an ownership fence in the store.
- Sharding depends on cluster state, on `receptionist::lookup_at` for resolving a region on
  another node, and, for persistent entities, on the same ownership fence. It does not depend on
  pub-sub or the singleton.

The recommended release order is the order of the sections below: pub-sub before the singleton
because it promises nothing new, the singleton before sharding because sharding's ownership rule
and hand-off are the singleton's, applied to many keys.

## Layers

### 1. Cluster state

Shipped; [cluster.md](cluster.md#cluster-state) is the authority
from here on.

**Mechanism.** `cluster::cluster_state()` returns a `tokio::sync::watch::Receiver<ClusterState>`
once the endpoint is started and fails before. A `ClusterState` is a versioned snapshot holding the
cluster ID, the member list, the members this node currently derives as unreachable, the broken
links and the foreign clusters, next to this node's own address and incarnation. It is published
by the mutation, not by a loop: every path which changes the
[`Membership`](../tellus/src/cluster/membership.rs) or the [reachability
graph](../tellus/src/cluster/reachability.rs) goes through one state-update operation which applies
the change and publishes the resulting value under the same lock. The
[`membership_loop`](../tellus/src/cluster/endpoint.rs) is one caller of it; the gossip merge, a
`down` call, node death, a leave and the retention sweep are the others. That matters for the last
value most: a self-down and a leave stop the loop for good, so a state published only from the loop
would never show the transition which ended it. There are no deltas: a subscriber computes
transitions by diffing two consecutive values, which is what a watch channel is for and what removes
the snapshot-versus-delta race a transition stream with catch-up snapshots would have. Membership
and reachability are two fields of one value rather than two streams because every consumer below
needs them together, and because the reachability set is derived from the member list of the same
version.

**Guarantee.** Every value a subscriber observes was this node's cluster state after some change;
versions increase strictly. The watch receiver holds the current value only, so a subscriber which
reads less often than the state changes skips intermediate values, which a renderer deriving from
the current value never notices. A consumer which needs every version takes `cluster::changes`: one
whole value per version and one settled verdict per change of it, in publication order from a
bounded channel, with an explicit gap followed by the current value and verdict where it fell
behind, never a silent skip.

**Open questions, settled.** `members()` reads the current published value, so the guarantee
that the two agree holds by construction. The value does not carry the per-member unreachability
instant: it feeds the downing provider's deadline, and a consumer deriving from the Up set has no
use for it; adding it later is additive.

The consumers are in place: the chaos demo's nodes log every transition they diff out of
`cluster::changes`, [`tellus-cluster-inspect`](../tellus-cluster-inspect) streams every change to
whoever mounts it, and the demo's [terminal observer](../tellus-cluster-demo-tui) renders that
stream.

### 2. Receptionist

Shipped; [cluster.md](cluster.md#receptionist) is the authority
from here on. What shipped differs from the sketch below in three points, each forced by a
review of the plan: a snapshot rides a latest-value slot on the lane rather than the control
queue, which is what makes "one pending snapshot per peer" true; chunks are numbered rather than
flagged, so reassembly does not depend on contiguity; and `settled` uses the replica presence
rule stated there rather than a per-version reset.

**Mechanism.** A cluster-wide directory keyed by the existing `Key<M>`.
`receptionist::lookup(&key)` answers every actor registered under the key on every Up member;
`receptionist::subscribe(&key)` returns a `Subscription` whose `changed` resolves when that set
changes and whose `current` reads it; the set is updated as registrations, terminations and
cluster state changes come in. The wire entry is the
[`WireKey`](../tellus/src/cluster/discovery.rs) of `register`, kind, name and manifest, next to
the actor ids, so a type mismatch is refused by name and type alike.

Replication is per origin, where an origin is a member incarnation, never an address. Each node
replicates the full set of its own registrations, stamped with a per-origin version, to every Up
member: on connection setup, the way the member snapshot and the down watermarks already ride
connection setup rather than the heartbeat, and on change over the connected lanes only. Every
transmission is one logical versioned snapshot, chunked within `max_frame_size` with numbered
chunks, and a receiver applies it as one snapshot once every chunk is present, so a set which
outgrows a frame costs chunks, not correctness. An empty set is a snapshot too and is sent as one
chunk: it is how an origin's last registration is removed and how a member with nothing registered
proves completeness to the settled predicate. The chunker
([`snapshot`](../tellus/src/cluster/receptionist.rs)) therefore always emits a chunk, unlike the
reachability push ([`snapshot_chunks`](../tellus/src/cluster/reachability.rs)), which emits none for
an empty set. Sending the full set instead of deltas is what makes tombstones
unnecessary: a receiver keeps, per origin, the set with the highest version, and a removal is a set
without the entry. Per peer, at most one snapshot is pending: a change while one waits replaces it
with the newer version, so a disconnected or slow peer costs one pending snapshot, not a growing
queue.

Eviction is synchronous. An update from an incarnation this node has marked Down is dropped
whenever it arrives, and the node death sequence
([`node_death`](../tellus/src/cluster/endpoint.rs)) evicts the incarnation's whole set at the
point where it takes the watcher entries, before the synthesized signals are flushed. The
receptionist does not learn of a Down through the cluster state channel, since a lookup between
the downing and an asynchronous eviction would still answer the dead node's actors; for the same
reason a lookup filters its answer against the current member list. Locally, an entry lives as
long as its actor, through the same watcher the [registry](../tellus/src/cluster/registry.rs)
evicts routes with.

A node knows when it has received the snapshot of each Up member, since snapshots ride connection
setup. `receptionist::settled(version)` is a predicate on a cluster state version: it holds iff
`version` is the current one and every member Up in it, other than this node, has delivered a
snapshot which lookups answer from, an empty one included. A replica exists only through its
incarnation's own snapshot, so a version whose Up set gained a member is not settled until that
member has delivered and is Up, which the singleton below relies on.

**Guarantee.** Eventual: after the last change, every connected member's answer converges to the
same set, and no answer names an actor of an incarnation this node has marked Down, made true by
the synchronous eviction and the filter rather than by observation. A subscriber sees a
registration once the origin's set has reached this node and the origin is Up, never earlier and
never after the origin is Down.
Cluster-aware routers, round robin or consistent hashing over the resolved set, fall out of this
layer without further work.

**Open questions, settled.** A node holds at most 2^20 registrations and 64 MiB of them, and a
single registration is bounded by the frame size, all listed under the limitations. A `lookup` does
not report the members whose snapshot is missing: `settled` is the one predicate which tells an
empty answer from an incomplete one.

### 3. Pub-sub

Shipped; [cluster.md](cluster.md#pub-sub) is the authority from here
on. A topic and a discovery key of the same name are distinct keys. There is no unsubscribe, for
the same reason there is no `cluster::unregister`: a subscription lives as long as its actor.

**Mechanism.** Topics on top of the receptionist: `Topic<M>` is a `Key<M>` under another name,
subscribing registers the local actor under it, and publishing tells every actor the receptionist
resolves. There is no mediator and no second queue. A publish serializes the message once, and
the subscribers of one node hashing onto one stream share one multicast frame, which that node
delivers to each of them. It answers how many subscribers it told.

**Guarantee.** The tell contract, restated for a set of recipients: at-most-once per subscriber,
and sequential publishes from one node reach one subscriber in publish order, "in order, with gaps"
across reconnects, since each publish rides the same queue the publisher's other messages to that
subscriber ride, while concurrent publishes fan out interleaved. A subscriber can receive only the
publishes made after its registration reached the publisher's node and its origin was Up there,
none earlier and none after its node is Down there. Nothing new is promised, which is why this
layer comes before the singleton despite being less asked for.

**Open questions.** The multicast frame still sends one frame per node and stream. A per-node
mediator, receiving one frame per publish per node and fanning out locally, would send one per
node. It introduces a hop and a second queue between publisher and subscriber and hence changes
what FIFO means, which is why the multicast keeps the subscribers' own streams instead and why a
mediator, if it comes, comes as a second delivery mode rather than a replacement.

### 4. Cluster singleton

**Mechanism.** `singleton::spawn(name, factory)` is called on every node. The receptionist is the
election medium: an instance is a registration under the singleton's key, and a node claims
ownership by spawning the instance and registering it. A node claims under a cluster state version,
and only if `receptionist::settled(version)` holds for that version, no registration for the key is
visible and this node is the lowest address among the members Up in that version; a claim whose
version is superseded before it completes is abandoned and re-evaluated. `singleton::spawn` returns
a proxy `ActorRef` on every node, the owner included, which resolves the instance through the
receptionist and forwards, locally on the owner. Ownership can move while the old owner's node stays
Up, e.g. when its instance stops and a lower-address member which joined later claims; a direct
reference to the old instance would die with it, the proxy survives every hand-off.

Ownership is non-preemptive: a visible registration is respected by every node, whatever its
address, so a join never moves the singleton. Nodes claim concurrently only when their views diverge
so that none of them saw another's registration in time, one claim per divergent view, and then the
deterministic tiebreak, lowest address, decides which instance stays and which ones are stopped.
Waiting for `settled(version)` before claiming is what keeps the join out of that race: a joining
node claims only after it has seen the registrations of every member Up in its version. The rule
does not compare incarnations across hosts. Incarnations order restarts at one address, subject to
that host's clock (see [cluster.md](cluster.md#membership)), and nothing more.

On a hand-off the successor claims as soon as the owner's registration is gone, which happens
either through the owner's leave or through its downing. It does not wait for the old instance to
stop, because across a partition it cannot: an owner which is downed while alive keeps running
until its own downing provider tells it to exit. A clean leave keeps the real tier as the normal
case rather than as a promise, exactly as [cluster.md](cluster.md#guarantees) words it for remote
watch: the departing owner stops its instance before it announces the departure, and the proxies
get a real terminated signal unless the owner's connection does not drain within their node's
`leave_timeout`, in which case they get the synthesized one.

**Guarantee.** Two-tier like remote watch, and eventual: once membership and the receptionist have
converged and the tiebreak's stops have completed, exactly one instance exists. Convergence alone is
not that point, since replicas can agree on several concurrent claims while the losing instances are
still stopping. Until then the number of instances is bounded by the number of divergent views, one
per side of a partition and one per concurrent claim, and the tiebreak reduces it to one. A
persistent singleton keeps its stream serialized throughout, since the conditional append lets only
one incarnation extend it from a given head, but not its writer unique: as the principles state, two
live instances can alternate. Making the writer unique needs an **ownership fence in the store**: a
lease per `PersistenceId`, taken by the claiming instance, carried in the append condition and
released by a clean stop or expiring otherwise, so the loser's restart cannot append until the lease
has passed to it. That is an extension of the [`EventStore`](../tellus/src/persistence/store.rs)
contract with a section of its own in [persistence.md](persistence.md), designed together with this
layer and shared with sharding.

**Open questions.** The proxy's behavior while the instance is unknown: the current preference is
to buffer up to a bound and dead-letter beyond, mirroring the bounded mailbox. Since the proxy is
an actor and the resolution arrives as a message, the buffer is flushed in order and ahead of the
messages queued behind the resolution, so FIFO from one sender holds across the resolution. Buffered
entries do not expire; the bound is the only limit, and a full buffer refuses the newest message,
as a full mailbox refuses a send. Whether the lease is optional, so a non-persistent singleton and
a store without lease support keep the weaker guarantee, or required for every persistent
singleton.

### 5. Sharding

**Mechanism.** Entities are actors identified by an entity id, spawned on demand on the member
that owns their shard, and passivated when idle. A message names its entity and rides an
`Envelope<M>`; `sharding::region::<E>(config)` on every node returns the local
`ActorRef<Envelope<M>>` entities are messaged through, and a region resolves the region of another
member through `receptionist::lookup_at` at that member's address.

The single node half of this mechanism has shipped: a [`Host`](../tellus/src/host.rs) spawns
entities identified by a key on the first `HostEnvelope` for that key and can passivate them after
an idle timeout. It buffers the messages arriving while an entity stops, up to a bound. A region can
build on a host per node and add the shard ownership and the routing between members below.

Ownership is computed, not coordinated: entity id to shard by hashing modulo a fixed shard count,
shard to owner by rendezvous hashing over the Up member set of the current cluster state, so every
member derives the same owner from the same member list, and a membership change moves few shards: a
member which joins takes only the shards it newly wins, a member which leaves gives up only the
shards it owned, and every other shard stays where it is. This deliberately avoids a coordinator
singleton holding persisted shard allocations: there is no state to recover, no coordinator hand-off
to get wrong and nothing to add to the member list. Identical owners on every node need identical
hashing: a fixed algorithm with fixed seeds, named next to the wire format version so a change is a
wire change, over canonical inputs, the entity id's UTF-8 bytes for the shard and the shard number
followed by the member's advertised address in its canonical textual form for the owner. Rust's
default `Hasher` is not that, being unspecified and randomly seeded per process. During the
convergence window two members may compute different owners; a region forwards a message it does not
own instead of dropping it on the first miss. Each forward is a tell and can fail like any other,
and a hop limit dead-letters a message which still misses, so forwarding narrows the loss to the
convergence window without closing it. On a membership change the old owner stops the entities of
the moved shards, and the new owner spawns them lazily on their next message.

**Guarantee.** Ownership follows the singleton's and is eventual in the same way: once membership
has converged and the old owners have finished stopping the entities of the moved shards, every
shard has exactly one owner, and every active entity has at most one instance, on that owner; a
passivated entity has none. Membership converges before those stops complete, so until then a shard
has at most one owner per side of a partition, plus the old and the new owner of a moved shard both
running its entities. Persistent entities keep their streams serialized through the conditional
append and get a unique writer only with the ownership fence above, keyed by the entity's
`PersistenceId`. Ordering towards an entity is per-sender FIFO while the sender's and the owner's
views of the member list agree. Across an ownership change there is no ordering guarantee at all:
two consecutive messages from one sender may take different routes, one forwarded by the old owner
and one sent to the new owner directly, and arrive in either order. FIFO resumes once the views have
stabilized. That is weaker than remote FIFO "with gaps", which reorders nothing, and the first
version says so instead of promising otherwise.

**Open questions.** Whether the shard count is per entity type or global; the passivation policy
(idle timeout per entity type is the preference); how messages are buffered during shard
recovery or migration, which the host's buffer for a stopping entity does not decide; whether an
owner should hand over a moved shard's in-flight messages explicitly rather than dropping them into
the forwarding path. Remembering entities across a move so they are respawned eagerly is out of the
first version.

## A parallel track: the persistence read side

Event sourcing without a way to read events other than by replaying one actor stays half a
pattern. A read side adds an event stream per tag or per persistence id, with offsets so a
projection can resume, and extends [`EventStore`](../tellus/src/persistence/store.rs)
accordingly, with the PostgreSQL store first. This does not depend on the cluster work and can
proceed alongside it. The ownership lease the singleton and sharding need is the other planned
extension of the store contract, and both are best designed at the same time, so the contract
test suite in [`persistence_tests.rs`](../tellus/src/persistence_tests.rs) grows once. Together
with sharding the read side completes the picture of sharded event-sourced entities feeding
projections.

## Not planned

- **Reliable or exactly-once delivery** as a framework feature. It is possible on top of the
  at-most-once tell contract of [cluster.md](cluster.md#guarantees), through acknowledgments,
  retries and deduplication, and it is left out as a matter of scope: the framework keeps one
  delivery contract, and an application which needs more gets it from persistence, since an
  event-sourced actor which records what it sent can redeliver.
- **A shard coordinator with persisted allocations**, for the reasons under sharding.
- **A replicated data store** (CRDT maps and the like). The member list is a lattice because
  membership needs one; general-purpose replicated data is a different library.
- **Growing the cluster beyond the gossip model**, hundreds of nodes with partial views. The
  limitations in [cluster.md](cluster.md#limitations) state the O(n^2) gossip cost, and the target
  stays clusters of tens of nodes.
