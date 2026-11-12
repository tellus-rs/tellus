# Clustered remoting

This document explains how tellus remoting (the `cluster` feature) works and which of the core
guarantees from [actors.md](actors.md) carry over the network, which weaken, and why. The
implementation lives in [`tellus/src/cluster`](../tellus/src/cluster). Its public API is flat for
what every node calls, with one submodule per pluggable family (`transport`, `codec`, `downing`,
`failure`, `formation`) and one each for the directory and the fan-out over it (`receptionist`,
`pubsub`). [`tellus-cluster-demo`](../tellus-cluster-demo) runs five nodes of it under continuous
crashes, departures and partitions.

## Overview

Remoting makes actors on different nodes message each other through the same API as local ones,
and every set of nodes doing so is a *cluster*.

`ActorRef` is location transparent: it implements `Serialize` and `Deserialize`, both failing with
`RefError::EndpointNotStarted` until the endpoint is started. Message types embed reference fields,
e.g. `reply_to: ActorRef<Reply>`, and work unchanged no matter where their counterpart lives.

Each process runs at most one remoting endpoint, started via `cluster::start_endpoint` with a
`Transport` (the provided one is QUIC via quinn, TLS included) and an `EndpointConfig`. The separate
`cluster-dev` feature adds `QuicTransport::dev`, which skips certificate verification and hence
exists only where it is asked for. It also adds the test hooks `cluster::sever_connections`,
`cluster::gossip_bytes` and `cluster::drop_terminated_frames`. [Configuration](#configuration) lists
the keys of the endpoint, of bootstrap and of the QUIC transport.

Membership is not optional. A started endpoint is *not* a cluster and admits nobody, `cluster::form`
makes it a cluster of one, and `cluster::join` makes it a member of the cluster one of its seed
addresses is in. Only members message each other. A handshake from a node which is neither a member
nor joining is refused, and so are a join toward a node which is a member of nothing and a handshake
from a member of another cluster. The member list is gossiped, so every member learns every other
from one seed address. A node shutting down cleanly announces its own departure with
`cluster::leave` instead of leaving the cluster to detect its silence. `cluster::lifecycle` reports
where an endpoint stands: `Unformed` after the start, `Formed` once it is a member of a cluster of
one or of many, and `Downed` once it was downed or left, which is final for the process.

A node gets into a cluster by joining it, and it gets its first reference to a remote actor by
discovery: `cluster::register` names a local actor and `receptionist::lookup_at` resolves that name
at a member's address from the replicated registrations. The seed addresses themselves can be
discovered instead of configured: `cluster::bootstrap` resolves them through a `SeedDiscovery`,
joins through them on every resolution and forms a cluster only once the view settles, see
Membership. `serialize_ref` and `deserialize_ref` remain for exchanging a reference out of band,
e.g. through configuration; the bytes carry the message type's manifest, so a wrong-typed resolution
is refused like a mistyped lookup. Every further reference travels inside messages.

Death watch works across nodes through the ordinary `ActorContext::watch`, with the weakened
contract below. Request-response works across nodes too: `ReplyTo` is serializable like a
reference, so `ActorRef::ask` and `ActorContext::reply_to` work unchanged against remote actors,
with the `NoReply` detection weakened as described below.

A node's identity is its advertised address plus an *incarnation*, a UUID minted exactly once per
process start, so a restarted node is distinguishable from its predecessor and no process can ever
resurrect an incarnation.

## Configuration

With the `serde` feature `EndpointConfig`, `BootstrapConfig` and `QuicConfig` are deserializable,
e.g. from YAML. A key left out takes its default, an unknown key is refused, and a duration is
written like `250ms`, `10s` or `5m`. The codec, the failure detector, the downing provider and the
formation provider are each chosen by the name of one of their provided implementations. An
implementation without keys is given by its bare name, as in `codec: postcard` or
`formation: majority`. One with keys maps its name to them, as in
`downing_provider: { keep_majority: { after: 10s } }`, where `{}` takes every default, as in
`failure_detector: { phi_accrual: {} }`. The `deadline` detector maps its name to the deadline
itself, as in `failure_detector: { deadline: 3s }`. A custom implementation is still assigned in
code. The transport is not chosen by name:
`QuicTransport::from_config` builds it from a `QuicConfig`.

An invalid `EndpointConfig` or `BootstrapConfig` is refused by deserialization just as it is by
`start_endpoint` and `cluster::bootstrap`. Every constraint below is checked that way, except the
one on `down_watermarks`, which relates the values of different nodes. A `QuicConfig` is plain data:
its server name and the PEM files its paths name are validated by `QuicTransport::from_config`,
which reads them. Deserializing one is hence not yet a check that the node can present an identity.

`EndpointConfig`:

| Key | Default | Constraint |
| --- | --- | --- |
| `advertised_addr` | required | neither an unspecified IP nor port 0 |
| `outbound_capacity` | 8192 | not 0 |
| `max_streams_per_peer` | 16 | not 0; lowered to what the transport offers, 100 for QUIC |
| `max_frame_size` | 1048576 (1 MiB) | at least 195, the widest handshake |
| `codec` | `postcard` | `postcard`; a custom codec's name at most 64 bytes |
| `app_version` | none | a semantic version, at most 64 bytes rendered |
| `heartbeat_interval` | 1s | not 0 |
| `watch_refresh_interval` | 2s | not 0 |
| `failure_detector` | `phi_accrual` | `phi_accrual` or `deadline`, see below |
| `failure_detector.phi_accrual.threshold` | 8.0 | finite and positive |
| `failure_detector.phi_accrual.max_sample_size` | 200 | at least 2 |
| `failure_detector.phi_accrual.min_std_deviation` | 100ms | not 0 |
| `failure_detector.phi_accrual.acceptable_pause` | 3s | |
| `failure_detector.phi_accrual.warmup_deadline` | 5s | |
| `failure_detector.deadline` | none | not 0 |
| `downing_provider` | `keep_majority` | `keep_majority` or `down_after_deadline` |
| `downing_provider.keep_majority.after` | 10s | |
| `downing_provider.down_after_deadline.after` | none | |
| `down_retention` | 5m | |
| `down_watermarks` | 4096 | not 0; not larger than on the nodes joining this one |
| `leave_timeout` | 3s | |
| `reconnect_backoff.min` | 250ms | not 0; required once `reconnect_backoff` is given |
| `reconnect_backoff.max` | 3s | not below `min`; required once `reconnect_backoff` is given |
| `max_connect_attempts` | 8 | not 0 |
| `ambiguous_join_bound` | 30s | |
| `change_capacity` | 256 | a power of two, at most half the address space |

The failure detector must tolerate the longer of `heartbeat_interval` and `watch_refresh_interval`
after a burst of traffic, see [Failure detection](#failure-detection-and-node-death).

`BootstrapConfig`:

| Key | Default | Constraint |
| --- | --- | --- |
| `min_peers` | 2 | not 0 |
| `settle` | 3s | |
| `resolve_interval` | 1s | not 0 |
| `formation` | `majority` | `majority`, `majority_with_fallback`, `unanimous` or `explicit` |
| `formation.majority_with_fallback.after` | none | |
| `probe_interval` | 30s | not 0; `null` turns the probe off |

`QuicConfig`:

| Key | Default | Constraint |
| --- | --- | --- |
| `bind_addr` | required | port 0 lets the OS choose |
| `cert_chain` | required | the path of a PEM file holding this node's certificate chain |
| `key` | required | the path of a PEM file holding the chain's private key |
| `roots` | required | the path of a PEM file holding the cluster's certificate authority |
| `server_name` | required | a DNS name or an IP address the certificates are issued for |

## Membership

A member's state is a three point lattice per incarnation: `Up < Left < Down`, and removal is
absence. `Left` is a departure the member announced itself, `Down` a death decided by the cluster,
so a downing after a departure wins. A detailed Left or Down entry is terminal and is the
incarnation's *tombstone*. Its address can also retain a *watermark*, the greatest dead incarnation
seen there (see Retention). Both *fence* what they cover, refusing it as dead: a tombstone its own
incarnation, a watermark its incarnation and every predecessor at that address. Everything which
asks whether an incarnation is dead treats Left as dead; only the receiving side of its connection
treats it differently, see Leaving. A restarted node returns as a fresh incarnation, never by
leaving Left or Down. That monotonicity makes the member list a state-based CRDT: merging is union
by node with the larger state per incarnation, hence commutative, associative and idempotent. So
duplicated, reordered or crossed gossip cannot corrupt it, and no versioning or anti-entropy
machinery is needed.

*Joining*: `cluster::join` tries its seed addresses in order with the reconnect backoff between
rounds, no internal timeout, until one admits the joiner. It fails at once with `NoSeeds` when the
seeds hold no address but this node's own, since joining never forms. A join rides one dedicated
connection: the handshake names the joiner with `Join` intent, the seed admits it as Up and answers
with its member snapshot, which the joiner merges as *one* snapshot once its last chunk has
arrived. Reading that snapshot is bounded by `heartbeat_interval`; a seed which takes longer counts
as a failed attempt. A connection lost mid stream hence leaves no fragment of a cluster the joiner
never joined. Every further frame rides ordinary lanes. Only a node which is itself a member admits
a join; a node which is not answers `NoCluster`, which is retryable, so a seed list shared by nodes
that are all still starting forms nothing on its own. The joiner's own address is skipped in every
position. A cluster is the transitive closure of joins. Independent clusters must not share seeds:
a joiner becomes a member of whichever seed admits it first, and the two clusters never merge,
since every handshake names its cluster (see cluster identity below).

*Join holdback*: a member holds back joins while any Up member is outside its reachability
component, the same members `ClusterState::unreachable` reports. A broken link between two members
inside the component does not count. A held join is answered `JoinsHeld`, which is retryable, so
`cluster::join` keeps retrying until the unreachable member is reachable again or downed. Why hold
back? A side of a partition which admitted new nodes could grow into a majority of its own, while
the other side still counts itself the majority too. A join from a younger incarnation at the
address of an Up member is admitted all the same, since it supersedes that member (see
supersession below), and so is a member joining again. A member which is leaving holds back every
join which would add a member, see Leaving. Bootstrap retries a held seed and never asks the
formation provider about that round, since the refusal proves that a cluster exists there.

Joining is exclusive and, once a cluster has answered, pinned. At most one join attempt runs per
endpoint, because two overlapping ones could be admitted by two different clusters before either is
recorded, leaving both counting this node; further callers queue. An attempt is abandoned if its
caller goes away while it waits its turn or for the transport to connect, since nothing has been
sent by then. From the `Join` handshake on it runs to its conclusion, since the peer may already
count this node from that moment. Once a seed's handshake, address and identity have been validated,
that address is *pinned*: it is the only one tried afterward, and nothing may be formed beside it,
because no other seed can be proven to be that same cluster. The pin is released by a completed
join, by that address answering `NoCluster`, by this node being downed, or by bootstrap seeing the
address leave a newly settled discovery view. A round in which the pinned address answers
`NoCluster` goes on to the other seeds. The process at that address has restarted since, but the
cluster which admitted this node may live on in the other seeds. Formation is hence decided on a
round over every seed, not on that answer alone.

*Ambiguous joins*: an attempt can also fail after its `Join` handshake was sent but before a
validated answer pins anything. That covers a timeout, while exchanging handshakes or while opening
the data streams, a closed connection, an undecodable or unexpected first frame and a transport
error. It also covers a reply which fails the dialer's own checks: an advertised address other than
the dialed one, an identity which does not cover it, a dead incarnation or a reply naming no
cluster. A member snapshot larger than the joiner accepts counts, and so does a join task which
panics once it has started to send the handshake. The seed may have admitted this node in each of
these cases, so the endpoint records the address as *ambiguous* and logs a warning. While any
address is ambiguous, `cluster::form` fails with `FormError::JoinAmbiguous`, and bootstrap reports
that as a declined formation. Joins continue through any seed, unlike under a pin, since nothing
proves which cluster, if any, admitted this node. The record ends once that address answers a join
definitively (`NoCluster`, `UnknownMember`, `Down`, `JoinsHeld`, `ForeignCluster` or a reply naming
another cluster), once bootstrap sees it leave a settled discovery view, or once
`EndpointConfig::ambiguous_join_bound` (see [Configuration](#configuration)) has passed. A
definitive refusal proves that the seed did not admit this node, so initial formation proceeds
normally after one.

*Cluster identity*: every cluster has an ID, a UUIDv7 minted when a node forms it, so clusters order
by formation time. A joiner learns its seed's ID from the seed's reply handshake and holds it
*provisionally*, together with the pin. It *commits* the ID only once the seed's member snapshot has
arrived whole, right before merging it, and from then on it is a member of that cluster for good.
`ClusterState::cluster` reports the committed ID, `None` before there is one. While the pin lasts,
the provisional ID counts like a committed one: every handshake this node sends, `Member` and `Join`
alike, carries the committed ID, else the provisional one, and every admission compares the peer's
ID against that same one. A handshake naming another cluster is refused as `ForeignCluster` and
logged at error level, and a dialer refuses a reply handshake naming another cluster the same way.
Both directions compare cluster IDs before the Down fences, so the refusal is `ForeignCluster` even
at the address of an incarnation this node holds Down, and a dial answered that way fails as
`ForeignCluster`, not as dead. An old fence hence never downs a node of another cluster. A handshake
naming this cluster, or none, still meets the fence. So a node which one cluster admitted cannot
also be admitted by an independently formed one while the pin lasts, and a seed answering with
another ID never replaces the pin. Releasing the pin drops the provisional ID with it, so a later
formation mints a fresh one. A node which has formed or joined a cluster is never admitted by
another: `cluster::join` fails with `ForeignCluster` once every seed of a round answered as a member
of another cluster. A `Probe` handshake is answered with the reply handshake, which names this
node's cluster, and is never admitted.

*Forming*: a cluster comes into being by decision, never as a side effect. `cluster::form` makes
this node a cluster of one for a deployment which decides for itself. It is refused if this node is
already a member, has been downed, or has a join attempt, a pinned cluster or an ambiguous join
outstanding. Otherwise `cluster::bootstrap` decides through a `FormationProvider`, a pluggable trait
like the `DowningProvider`; its provided implementations are `Majority` (the default),
`MajorityWithFallback`, `Unanimous` and `Explicit`. The provider sees the *universe*, every resolved
address united with this node's own, what each attempted address answered and when the round ended.
An answer is `NoCluster`, `Held` (a cluster holding back joins) or `Unavailable`. Bootstrap passes
every round which did not admit this node to `observe`, including the rounds which must not form. It
asks `form` only on a round over a settled view which no seed admitted. `form` answers
`Decision::Form` or `Decision::Decline` with a reason: `NotLowest`, `NoMajority`, `Explicit` or
`Other` for a custom provider, while bootstrap adds `Ambiguous` for a formation the endpoint
refused. A decline is logged at warn level when its reason or its universe differs from the previous
decline, at debug level otherwise, so a node waiting for others says why once. `Majority` forms once
a strict majority of the universe, this node included, is known to be no cluster, and only at the
lowest address of the universe. The lowest-address condition ensures that only one node forms for a
shared universe. The majority stops a partitioned minority from re-forming. Both conditions are on
the universe rather than on who answered, so a node which cannot reach the lowest address forms
nothing. Formation liveness is hence a requirement on discovery, which must stop resolving nodes
that are gone. A reduced view must still satisfy `min_peers` for the next-lowest address to inherit
the role. `Unanimous` forms only once every address of the universe is known to be no cluster, again
at the lowest, so any one unreachable address blocks a cold start. `Explicit` never forms: the
deployment calls `cluster::form` on one node, and bootstrap only ever joins.

`MajorityWithFallback` is the opt-in for a discovery which cannot be trusted to drop nodes that are
gone (`formation: { majority_with_fallback: { after: 60s } }`). It keeps the strict majority. The
address which may form, though, is the lowest of the universe minus every address which has answered
nothing but unavailable for at least `after`. An address's timer starts with the first round it is
unavailable in; any other answer, and any round which did not attempt it, resets the timer. The
timers follow every observed round, so a round answered `Held` resets the timer as well, although it
never forms. An address unavailable again afterward waits a fresh `after`. It trades uniqueness for
liveness: a lowest address which is merely unreachable from some nodes, not gone, can form a cluster
of its own beside the one they form. The foreign cluster probe (see bootstrap below) reports such a
double formation in `ClusterState::foreign_clusters` on both sides, but the two clusters never
merge.

*Bootstrap*: `cluster::bootstrap` turns the seed addresses from configuration into discovery. A
`SeedDiscovery` resolves them; the provided ones are `FixedSeeds` in tellus, SRV and A/AAAA records
in the [`tellus-bootstrap-dns`](../tellus-bootstrap-dns) crate and pods listed by label selector in
the [`tellus-bootstrap-k8s`](../tellus-bootstrap-k8s) crate. The *universe* is the resolved set
united with this node's own address. Bootstrap resolves every `resolve_interval` and attempts to
join through every resolved address but this node's own, lowest first, on every resolution, so a
node finds an existing cluster at once. Only the formation decision waits for the view to settle. It
is taken on a round over a universe which has held at least `min_peers` addresses unchanged for the
settle window (`settle`), and only when none of them admitted this node. A settled view holding
fewer than `min_peers` addresses blocks that decision, so it is logged at warn level on every
resolution. A round held back by a seed (`JoinsHeld`) is only observed and never asked to `form`.
Bootstrap returns as soon as this node is a member, its own formation included, and at once when it
already is. Membership owns failure detection and downing from then on. `min_peers` counts the
universe while the formation provider counts what its addresses answered. That distinction keeps a
restarted node from re-forming inside a partition: discovery is unaffected by a partition,
reachability is not. Resolve failures are retried and restart the settle window, since nothing was
observed during them. There is no internal timeout, so the call is bounded by the caller's timeout.
`Downed` is fatal per process and means the caller should exit, letting the restart mint the fresh
incarnation. What does this not prevent? Nodes deciding on views with *different lowest addresses*
can still form separate clusters, which never merge on their own, since the formation rule is only
as good as the universe it is taken over. The settle window and the `min_peers` floor make that less
likely, so size `min_peers` to the deployment when possible (see Limitations).

*Foreign cluster probe*: once bootstrap has made this node a member, it spawns a task which keeps
the discovery and resolves it again every `probe_interval` (see [Configuration](#configuration);
`None` turns it off). It probes every resolved address which is neither this node nor an Up member
here, all of a round at once and the round bounded by the interval. A probe connects, sends a
`Probe` handshake, reads the reply handshake and closes. A reply naming another cluster than this
node's committed one is a *foreign cluster*. So are the handshakes this node refuses as
`ForeignCluster` and the dials answered by another cluster's reply. Each appears in
`ClusterState::foreign_clusters` with its address and cluster ID, and the list is logged at error
level every 5 min while it is not empty. An entry is dropped once it has gone unconfirmed for three
probe intervals (90 s by default), or once its address is an Up member here. Two clusters never
merge, so an entry is a split for an operator to resolve, see Limitations. The task stops once this
node is downed. Nothing is reported before this node has committed a cluster.

*Gossip*: every `heartbeat_interval` each node sends one gossip frame to every Up member. That
frame carries the full member list unless the member has been sent the current list already, in
which case it is empty. Every 10th tick sends the full list regardless. A node records per member
the version of the list it last sent in full, and only once the frames were queued on the lane. A
reconnect drops that record, so the next tick sends the full list again. A node also pushes its
full member list whenever the list changes locally, and the acceptor of every connection sends its
snapshot as the first frame behind the handshake. Every gossip frame, empty or not, doubles as a
heartbeat, so there is no separate ping. A member with a currently disconnected lane is skipped.
The skip is keyed on transport connectivity, never on suspicion. The uncounted system-frame queues
hence stay bounded while a member is retried, and two healthy nodes can never talk each other into
mutual silence. The push on a local change goes only to members whose lane is connected; the
periodic one also dials a member which has no lane yet. A state change hence reaches every member
with a live path within one gossip round per hop of connectivity; convergence is eventual, not
agreed, see downing below. Direct-link reachability rides the same tick: an edge change is versioned
by its observer, and every statement a node mints, accepts from a peer or promotes is queued and
fanned out once per tick, deduplicated by version at every hop. A statement is *merged* the moment
it arrives, so this node's own decisions never wait, and only its *forwarding* costs a round.
Reachability hence spreads one hop per tick exactly as membership does, and a burst of edge changes
costs one push per member per tick rather than one fan-out per record. Like gossip, a push skips a
disconnected lane. A statement naming a node which is not a member here yet cannot be merged, and
its observer resends only to its own peers, so a relayed one would be lost for good. Such a record
is therefore parked in a bounded table and admitted once membership catches up, which resumes the
relay past this node. A parked record naming a node which is Down here, or parked for longer than
`down_retention`, can never be admitted and is dropped. Connection setup sends the full observation
table, and every tick re-asserts this node's own standing unreachable statements, which together
repair a record lost with a connection rather than dropped on arrival.

Batching bounds the frames, not the replication. Every node forwards every statement it accepts to
every Up peer, the one it came from included. A single edge change hence still costs O(n^2) copies
of that record across the cluster, and a partition breaking e edges costs O(e * n^2). That is an
accepted bound, not one this design removes: it is the same order as the membership push, and a
smaller fan-out would trade the deterministic spread that downing agreement rests on for a
probabilistic one. Reachability is free while a cluster is healthy, since nothing is then queued or
re-asserted; membership is not, since every node sends a gossip frame to every peer each tick,
which is O(n^2) frames per interval across the cluster. While the list is unchanged those frames
are empty, but every 10th tick still copies the whole list, O(n^3) entries. Both are
deliberately simple for the cluster sizes this targets, and neither is meant for hundreds of
nodes.

*Refusal*: an inbound `Member` handshake from a node which is not an Up member is answered with a
refusal the dialer treats as retryable, since gossip may not have carried that node's join
everywhere yet. The backoff retry succeeds once it has. A dead incarnation's handshake is refused
finally. The outbound direction is optimistic: a send toward a node this endpoint does not know yet,
e.g. a reference which arrived ahead of the gossip naming its node, dials anyway and lets the far
side judge. A send toward a Left or Down incarnation is refused locally. Three more sends fail
locally: a send while this node is downed or leaving, a send toward an address currently refused
(see the wire model), and a send toward a lane already bound to another incarnation at that address.
In that last case a send toward a successor fails until this node learns of its predecessor's death,
one gossip hop away, since a reference alone is no proof the predecessor is gone.

*Supersession*: a join from a younger incarnation at an address an Up member already advertises
proves the old process is gone, so the old incarnation is downed and the new one admitted. A join
from an older incarnation is refused as Down and downs the joiner instead. Where both directions
race, the younger incarnation wins: UUIDv7 incarnations order by minting time, assuming the
restarting host's clock did not regress. The direct join handshake stays the primary path; the
ordering only makes the gossip merge convergent.

*Leaving*: `cluster::leave` announces a departure instead of leaving it to be detected: this node
moves its own entry to Left, pushes that member list to every Up member, stops sending anything
else, drains every lane with its peer, and severs. The announcement goes to every Up member, a lane
still being dialed included: it is the last thing this endpoint enqueues and can hence not grow a
queue the way the periodic gossip would. Each lane's writers write out what is queued, the frame a
writer holds included, and finish every data stream. Then the control stream carries a `Drained`
marker as its final frame, naming the number of data streams this node opened on that connection.
The peer answers with a `DrainAck` once exactly that many data streams have ended cleanly and every
frame they carried was dispatched. This node then counts the peer as acknowledged and closes the
connection. An acknowledgment proves that every frame queued for the peer reached the framework's
delivery path there, a local mailbox or a logged dead letter. It does not prove that any actor
handled them. The whole exchange is bounded by `leave_timeout`: one deadline for every lane, not
restarted between the phases. A lane waiting out a reconnect backoff when the departure begins dials
again at once, and a lane keeps dialing until the departure has ended, so a peer is reached whatever
state its lane was in. A connection closed before the `DrainAck`, a read error and a writer which
failed or could not finish its stream never count as acknowledged; the marker is not even sent after
a failed writer. A member which is gone already costs that timeout and then learns the departure
from another member's gossip, or, with no live path at all, from its own failure detection. The call
returns a `Departure`, which lists the peers which acknowledged the drain, those which did not since
the connection ended first or a stream failed (each logged as a warning), and those which had not
answered when `leave_timeout` passed. Concurrent calls share the one departure and its report, which
runs in a task of its own. So none severs the connections another is still waiting on, and a caller
which is canceled only stops waiting. Leaving again after the departure has ended returns its report
as well. A node downed before it began to leave fails with `LeaveError::Downed`. A departure which
ends without a report, e.g. since its task panicked, fails safe. A supervisor awaiting that task
then latches this node Downed and severs every connection. Only after that does it publish the
failure, so every caller, concurrent or later, gets `LeaveError::Incomplete` and never a report
claiming an acknowledgment. Leaving is terminal per incarnation exactly as being downed is, so the
process is expected to exit, and only a restart rejoins.

A leaving node admits no new member. From the moment its own entry is Left, this node answers a join
from a new address, and one which would supersede a member, with `JoinsHeld`. The announcement of
its departure is the last thing it enqueues, so no other member would learn of a later admission,
and the joiner would be a member in this node's list alone. The joiner retries as after any held
join. Such a join ends the joiner's round, so the seeds behind this node are asked only once it is
gone, at the latest after `leave_timeout`. A member joining again is still admitted, since that adds
nothing to announce.

The departure is part of the gossiped state, so a member recognizes it however it arrives: from the
leaver itself, or relayed by a third member, which can reach it before the leaver's own announcement
does. A member learning it treats the incarnation as dead at once: it sends nothing more to it,
refuses its joins and stops counting it as a member. The node death sequence, though, waits for
every inbound connection from that incarnation, counted from its admission, and for one the leaver
is yet to open. While the departure is deferred, this member admits the leaver's `Member` handshake
to drain, and only that one. That connection carries everything the leaver queued for this member,
and its streams are not ordered against the control stream the departure rides. So every stream of
the connection is read to its end, and a closed connection ends no reader early: what a stream still
buffers is delivered first. Data streams which ended before the leaver's `Drained` marker count
toward the number it announces, and so do streams accepted only after it. Once that number has
ended, this member answers with a `DrainAck` and keeps the connection open until the leaver closes
it. The wait is bounded by this member's own `leave_timeout`, counted from when it learned the
departure and not restarted by the marker; past it, no `DrainAck` is sent. Then the death runs,
once, after the last connection's readers have finished: pending asks fail, the leaver's
registrations are evicted, the remaining watchers get synthesized signals. A reconnect which
supersedes a reader takes over its drain, since the superseded reader is no longer the last
connection. One abandoned before its reader started has drained nothing and does not end the wait.
Without any connection from the leaver, the death waits for one, up to the same deadline. A death
which runs at that deadline is logged as a warning only where a connection of the leaver was
awaited, since the leaver was Up here or has connected to drain. The death of a leaver whose
departure this member merely learned of, as a joiner does from its seed's snapshot, is logged at
debug level. A downing of the leaver learned meanwhile ends the wait immediately, and the member is
Down from then on. Completing the drain changes only local state; the replicated state stays Left. A
drain frame outside a deferred departure breaks the protocol and closes its connection, like an
undecodable frame. That is a `Drained` marker on a data stream or from a member whose departure this
node does not defer, and a `DrainAck` on an accepted connection or toward a node which is not
leaving.

*Self-down*: a Down watermark at a node's own address can cover its incarnation. When gossip or a
final refusal tells the node so, it honors the verdict: it logs at error level, severs every
connection, and refuses everything from then on; `cluster::join` fails as `Downed`. A younger
incarnation at this node's own address, arriving as a join or as an Up entry in gossip, has the same
effect, since it proves this process is the predecessor. Only a restarted process, with a fresh
incarnation, can rejoin. Honoring the verdict keeps the synthesized signals other nodes flushed
true. A leave is the same sever and the same latch, chosen by this node and announced first. It is
hence logged as the departure it is rather than at error level. A leaving node told its own Down
back, by gossip or a refusal, leaves that latch to its leave. Neither settles anything locally:
watchers of remote actors get no signal, asks resolve only by their timeout and actor-origin
requests not at all, which is why the process is expected to exit.

*Retention*: a detailed Left or Down entry is dropped `down_retention` after this node learned of
it, but its address keeps the greatest Left or Down UUIDv7 incarnation as a compact watermark,
together with its state, merged along the same lattice. A node learning a swept departure from a
setup snapshot hence learns a departure, not a downing. That watermark refuses the dead process and
every predecessor while allowing a restart, whose incarnation compares greater. The retention window
covers frames and gossip still naming the detailed entry in flight; after it expires,
`cluster::members` omits the dead incarnation while the fence stays. Only an incarnation this node
saw Up gets a detailed entry: one which was Down before this node learned of it becomes its fence
alone. So `cluster::members` and `ClusterState` lists may differ between nodes in which dead
incarnations they still show. Adopting every Down as a detailed entry instead would make a node
joining a long-running cluster gossip every fence it was handed, at every heartbeat, for a whole
retention window.

Watermarks ride connection setup, not the heartbeat. The periodic gossip carries the member list
alone, so it stays the size of the cluster however many addresses the cluster has outlived.
Connection setup exchanges snapshots carrying the watermarks too. The acceptor sends its snapshot
behind its handshake, and the dialer sends its own over the freshly bound lane. That is enough to
converge, because a node absent longer than the retention has to reconnect before it can say
anything, and either side of that reconnect hands the other its fences. A snapshot is split into as
many `Gossip` frames as `max_frame_size` demands, every one but the last marked `more` so a joiner
reads the whole of it; the merge, being a CRDT union, absorbs the chunks in any order. A
`max_frame_size` too small to hold one member or one reachability record, each with its envelope, is
refused by `cluster::start_endpoint`, since no chunking could keep a snapshot within it.
`down_watermarks` caps the set, which bounds both the memory an endpoint spends on outlived
addresses and the time a connection setup takes to hand them over. Above the cap, a watermark this
node learned longer than `down_retention` ago is evicted, the earliest learned first. Learning a
newer incarnation at an address counts as learning its watermark anew; hearing the same one again
does not. A watermark learned within the retention is kept above the cap, so a death this node has
just learned is not forgotten at once, and a warning is logged when the set first exceeds the cap.
Twice `down_watermarks` is a hard ceiling, beyond which the earliest learned watermark is evicted
regardless. A detailed Left or Down entry still refuses its own incarnation until it is swept,
whether or not its watermark remains. A joiner accepts a snapshot of at most 2^16 members plus twice
its own `down_watermarks` fences. Hence `down_watermarks` must not be larger on any node than on the
nodes joining it, and in practice it is the same on every node. An address whose watermark has been
evicted and whose detailed entry, if any, has been swept falls back to the behavior it had before
watermarks. A zombie's `Member` handshake is then refused as an unknown member rather than told it
is dead, and its `Join` handshake is admitted again.

The fence relies on UUIDv7 incarnations being monotone at an address. In particular, a host whose
clock moves backward across a restart can mint an incarnation below the watermark and be refused
until an operator intervenes.

`cluster::members` answers the member list of the current cluster state (see below), `cluster::down`
downs a member by address, e.g. for an operator who knows a node is gone for good, and
`cluster::leave` leaves the cluster. `cluster::down` fails with `DownError::NotAMember` when no Up
member advertises the address, which is worth retrying while gossip converges. It fails with
`DownError::ThisNode` for this node's own address, since a node ends its own membership with
`cluster::leave`. A process whose lifetime is one actor system's lifetime calls
`cluster::leave_on_terminated` in place of `ActorSystem::terminated`: waiting for the tree first is
what puts the departure behind every terminated signal the tree owes remote watchers. When the actor
system did not terminate cleanly, it still announces the departure and then fails with
`LeaveError::Terminated`.

[`examples/cluster.rs`](../tellus/examples/cluster.rs) runs all of this in four processes. It joins
through one seed address and converges on a member list nothing was configured with. Every survivor
downs a killed node into a synthesized terminated signal, and at the end the survivors leave, which
shows both ways out of a cluster in one run.

## The wire model

Every connection opens with one handshake in each direction. Its *envelope* is frozen across all
protocol versions: the `Frame` discriminant 0, the protocol magic, and the oldest and the newest
protocol version the sender speaks. The receiver decodes the envelope on its own first, negotiates
the newest version both ranges hold, and only then decodes the body with that version's schema. A
version extends the previous one's body only by appending fields, so the body of any version
decodes as the body of an older one. The dialer sends the body of its newest version, and the
acceptor answers with the body of the negotiated version. Without a version in common the acceptor
answers with its bare envelope and closes, the dialer fails the attempt as incompatible, and both
log the two ranges. A frame or field a later version adds is sent only to a peer which negotiated
that version. This build speaks version 2 only, whose body names the node, its intent (`Member`,
`Join` or `Probe`), its cluster ID, if it has one, the name of its codec and the version of its
application, if one is configured. A peer naming another codec is refused as `CodecMismatch`. The
frames carrying a message, a reply or a multicast carry the schema version of their payload, see
Rolling upgrades; nothing gates that field, since version 1, the one of tellus 0.5, is not spoken:
a 0.5 node and this one refuse each other at the handshake.

All frames from one node toward another ride one lane: a set of outbound queues drained by one
connection. Sending enqueues synchronously at `tell` time; the receiving endpoint injects into local
mailboxes in arrival order. The queues are unbounded underneath with a reservation counter in front,
exactly like a local mailbox. One counter serves the whole lane, so `outbound_capacity` means the
same thing however many queues share it. Ordinary messages, multicasts and replies are subject to
the capacity, a multicast as one slot however many targets it names. System frames (watch
registration, terminated signals, reply-dropped notifications, gossip) bypass it but ride the same
queues, since a terminated signal must never be dropped and must never overtake. One frame kind does
not ride a queue at all: a registration snapshot for the receptionist sits in a slot next to the
control queue, replaced by every newer one. That slot bounds the pending snapshots to one per peer.

A lane is not one queue but a *control* queue plus a bounded pool of *data* queues, one per stream
of the connection, `max_streams_per_peer` of them at most. A frame delivered to an actor picks its
stream by hashing that actor's ID. The terminated and reply frames below hash another key, and a
`ReplyCanceled` frame rides the stream of the request it cancels (see Guarantees). `Watch`,
`Unwatch` and every remaining frame ride the control stream. A `Multicast` frame, which carries one
payload to several actors of the receiving node, names only targets hashing onto one stream and
rides it, so each target still receives it on its own stream. FIFO holds per recipient rather than
per node, and a large message only delays frames toward recipients hashing onto the same stream. The
mapping never travels the wire: the receiving side dispatches by the target named in the frame, so
only the sender has to agree with itself.

That the terminated signal names its *watcher* is what carries the ordering guarantee across this
split. A `Terminated` frame hashes onto the watcher's stream, the same one the messages the dying
actor sent that watcher ride, so the shared queue orders them. It costs one frame per watcher rather
than one per node. A `Reply` frame names the actor it is *delivered to* for the same reason: it
rides that actor's stream, behind whatever the responder told it before replying. A reply no actor
awaits, the answer to an `ask`, names none and keys on its nonce instead, which spreads such replies
over the data streams rather than putting a user payload on the control stream.

How many data streams a lane gets is a property of the transport, not an assumption: `Transport`
reports it, and a transport without streams reports none. QUIC reports the concurrent stream limit
it sets on its own transport config, `QuicTransport::DATA_STREAMS` (100). A larger
`max_streams_per_peer` is hence lowered to that limit rather than failing every connection. With no
data stream every frame rides the control stream, which is one ordered lane per peer carrying
everything. The guarantees hold there by the same argument rather than as a special case, which
keeps the abstraction implementable over a stream-less transport such as TCP. `Transport::connect`
must bound itself, since a silent address would stall the join attempt holding it, and joins run one
at a time per endpoint. `QuicTransport` inherits QUIC's handshake timeout of tens of seconds.

How much a larger pool buys was measured with `just bench-streams <streams>`, which sets
`max_streams_per_peer` on every node. One node keeps 64 asks of 64 KiB outstanding toward one actor
of a second node, 16,000 in all after a warm-up of 2,000. Meanwhile four serial probers ask 256 echo
actors on that second node, which share the transfer's lane, and 256 on a third node, which do not.
Every prober cycles over all echoes, so each echo is asked equally often. All three processes ran on
one 10 core arm64 machine over loopback, in release mode. Five runs per stream count followed one
discarded run each, interleaved:

| Streams | Bulk throughput (best, range) | Same node p50 | Same node p99 (best, range) | Other node p99 |
| --- | --- | --- | --- | --- |
| 16 | 1,569 messages/s, 98.1 MiB/s (97.3 to 98.1) | 71 to 77 us | 39.9 ms (39.9 to 40.4 ms) | 205 to 232 us |
| 100 | 1,537 messages/s, 96.1 MiB/s (90.1 to 96.1) | 74 to 109 us | 0.79 ms (0.79 to 39.6 ms) | 154 to 226 us |

An ask toward an echo hashing onto the transfer's stream waits behind the queued bulk, about 40 ms
here; every other ask stays below 1 ms. That share is about one recipient in `max_streams_per_peer`:
5 to 8% of the same node asks with 16 streams, 0 to 2% with 100. So 100 streams lowered the p99 in
four of five runs, but not in the run where more than 1% of the echoes hashed onto the transfer's
stream. The p99.9 stayed at about 42 ms in four of five runs. Every run with 100 streams moved less
bulk than every run with 16, 2% less at best and 4% less on average. The default hence stays at 16:
a larger pool shrinks the share of blocked recipients without removing the blocking, and costs
throughput.

All data streams are opened when the connection is established, not on first use. A peer admitting
fewer concurrent streams than this node opens then fails the connection at setup, into the ordinary
reconnect path. Opened on demand, a stream that was never granted would instead stall whichever
queue happened to hash onto it, silently and forever, since these streams live as long as the
connection. For the same reason a stream failing at the receiver, with an oversize or truncated
frame or a reset, ends the whole connection there. The other streams are not left running beside a
dead one, and only a stream its sender finished ends quietly.

A departing node ends each of its connections with two control frames, see Leaving. `Drained`
is the last frame on its control stream, sent after every data stream is finished, and names how
many data streams the connection had. `DrainAck` answers it on the reply direction of the control
stream, which otherwise carries only the accepting side's setup frames.

A lost connection is reconnected with exponential backoff; frames queued while the link is down are
delivered after the reconnect, in order. There is no replay of frames already handed to a dead
connection: delivery stays at-most-once, per-sender FIFO becomes "in order, with gaps". An address
no Up member advertises is given up after `max_connect_attempts` failed attempts, turning its queued
frames into dead letters. A member's address is retried until the downing provider settles its fate,
whatever the attempts fail with, since giving it up would fire synthesized signals for a node which
is Up. Giving up is not final: a later message dials again. An address no Up member advertises is
given up at the first attempt, not after `max_connect_attempts`, when the failure is one which
retrying cannot change. That is an answer which does not speak this protocol or shares no version
with it, a failed identity or address check, a node of another cluster, or a frame beyond
`max_frame_size`. Such an address is refused, and a send toward it fails without dialing. The
refusal expires after 10 s. Every further refusal doubles that backoff, up to 10 min. A refusal
expired for longer than 10 min is forgotten, so the next one starts at 10 s again. An inbound
handshake from the address, an admitted join from it or a gossiped Up member at it lifts the refusal
at once and resets the backoff. Any other transport failure, a TLS handshake failure included, is
not refused: it is retried up to `max_connect_attempts` times per lane like any other retryable
failure. A refusal as Down refuses no address either: it tells this node that it is dead itself, so
this node downs itself (see *Self-down* above). A leaving node leaves that to its leave, and its
departure counts that peer as unacknowledged. A connection's reader is aborted before the next one
is dialed, so two readers for one peer can never interleave a frame buffered on the dead connection
behind a frame from the new one.

A lane belongs to an address but serves one incarnation: the handshake names the peer it is
connected to. From then on a frame addressed to any other incarnation at that address is a local
dead letter rather than traffic written onto its successor's connection. A reference which outlived
the node it names hence fails fast, close to the sender, instead of being dropped as an unknown
target on the far side. Frames already sitting in the lane's queues when a reconnect's handshake
reveals the successor are the exception: they drain onto the new connection and die on the far side
as unknown targets, at-most-once either way.

## Guarantees

- **The tell contract extends verbatim.** Fire-and-forget, at-most-once; an unreachable node, a
  full outbound queue, a message encoding beyond `max_frame_size` and an undecodable payload all
  become logged dead letters. Undecodable includes a payload whose embedded reference names an
  actor of the receiving node which has already terminated: the whole message is the dead letter
  then, unlike a local tell to a terminated actor, which costs only itself.
- **Per-sender FIFO holds** for messages from one sender to one target, "with gaps" across
  reconnects as above.
- **Remote death watch has two tiers**, indistinguishable in the API:
  - A *real termination*, delivered over the wire, keeps the full local contract: the terminated
    signal arrives behind all messages the terminated actor delivered to the watcher, and it proves
    the actor's destructors have run. This is because the wire watcher fires inside the target's
    local termination sequence. Its `Terminated` frame rides the same queue as the messages the
    actor sent that watcher before, the one their shared recipient hashes onto.
  - A *synthesized signal*, flushed when the watched actor's member is downed, proves none of that:
    the actor may be alive across a network partition and its destructors may never run. It
    guarantees exactly one thing, made true by construction rather than observation: **after the
    signal, no message from that actor is ever delivered through this endpoint again.** The node
    death sequence finds the incarnation Left or Down and stops all delivery from it before the
    signals are flushed. From then on a handshake from a Left or Down incarnation is refused; a Left
    one is admitted only while its death is deferred, before any signal is flushed. Distinguishing a
    crashed node from an unreachable one is impossible in an asynchronous network; this weakening is
    fundamental, not an implementation gap.

    Two cases fire the same signal without the node death sequence, and the promise above does not
    cover them. The first is a `Watch` frame this node cannot send: toward a refused address, toward
    a lane bound to another incarnation, or while this node is downed or leaving. The second is a
    lane given up for an address no Up member advertises, which signals every incarnation watched
    there. Neither marks the watched incarnation Down.
  - A *clean leave* keeps the real tier as the normal case rather than as a promise:
    `cluster::leave_on_terminated` announces the departure only once the local tree has terminated.
    So every `Terminated` frame it owes is queued first, and the drain covers those streams too. The
    announcement rides the control stream while the signals ride their watchers' data streams, and
    those are not ordered against each other. A peer learning the departure first, from the leaver
    or relayed, keeps reading every stream of the leaver's connection to its end, a closed
    connection included. So the messages and real signals behind it are still delivered before the
    node death sequence runs. The peer acknowledges the drain only once every data stream the leaver
    announced has ended and its frames were dispatched. Only a stream which does not end within the
    peer's `leave_timeout`, or a downing of the leaver in between, turns what is still unread into
    synthesized signals. Exactly once survives either way: the node death sequence takes the watcher
    entries, so a real signal arriving second finds none and fires nothing.
- **Watching a dead incarnation signals immediately.** A watch on a node already Left or Down fires
  the synthesized signal right away, except on a Left node whose connection still drains, where it
  fires with that node's deferred death. A node merely not known yet does not fire on that account:
  gossip converges per hop, and sending to such a node works, so firing on it would break the one
  promise the synthesized signal makes. It fires only when the `Watch` frame cannot be sent, see
  above. A node which never becomes a member is answered instead when its lane is given up, together
  with the asks that lane fails.
- **Unwatch keeps its contract.** It is enforced on the watcher's side in the run loop, which does not
  know or care whether a signal came from the local or a remote actor.
- **Watching is race-free.** A `Watch` for an already terminated (or never bound) actor is
  answered with an immediate terminated signal by the watched node, mirroring the local atomic
  registration close.
- **Request-response crosses nodes through the same API.** Serializing a `ReplyTo` moves the reply
  destination into a nonce-keyed pending table on its origin node. The receiving node gets a proxy
  whose reply rides a `Reply` frame back to the origin, and dropping the proxy without a reply sends
  a `ReplyDropped` frame instead. An ask still resolves exactly once, at latest at its timeout, and
  a `reply_to` reply stays FIFO with the responder's other messages to the asker, since the `Reply`
  frame names that actor and hence rides its stream. The `NoReply` detection weakens to best-effort.
  A request which cannot be queued fails the ask as it does locally: a full outbound queue as
  `MailboxFull`, and an unreachable node as `ActorTerminated`. An unreachable node means a Left or
  Down incarnation, a refused address, a lane bound to another incarnation or this node being downed
  or leaving. A request which encodes beyond `max_frame_size` fails as `TooLarge` and one the codec
  cannot encode as `NotEncodable`, both at the send rather than by timeout.

  `ReplyDropped` is fire-and-forget: one lost with its connection resolves the ask by its timeout,
  since a reply is not idempotent and nothing reply-related is replayed on a reconnect. An ask
  canceled before that releases its entry itself, so a lost notification costs it nothing.

  A proxy's reply which cannot be sent, because it cannot be encoded, for lack of outbound capacity,
  because it exceeds `max_frame_size` or because the node is unreachable, is logged as a dead
  letter. A panic in the reply's `Schema::encode` counts as a reply which cannot be encoded. It is
  logged at error level and reaches neither the responder nor, on a node which forwards the reply,
  that node's stream reader. The proxy then attempts the same `ReplyDropped` as for a dropped one.
  That attempt fails if the node is unreachable, and a reply which was queued successfully and lost
  afterward takes no such path, so the ask's timeout remains the backstop. Downing a member fails
  every pending ask toward it as `NoReply`, after the tombstone and quiesce, so such a `NoReply` is
  never followed by its reply. Every member is heartbeated, so a vanished node's asks are settled by
  the downing provider, watched or not. Giving up a stale lane, one toward an address no Up member
  advertises anymore, fails the asks stamped with its peers the same way. If the transport reports
  the loss too late, the ask timeout still resolves the request.

  A message frame names its payload's reply destinations next to the payload. So a request the
  receiving node dead-letters undecoded, e.g. toward a meanwhile terminated actor, is answered with
  `ReplyDropped` for each of them, and such an ask resolves as `NoReply` rather than by its timeout.
  A request this node itself drops, one whose framed size exceeds `max_frame_size` or one lost with
  its connection, releases the same destinations locally, since such a frame never reaches its peer
  as a whole. Only the peer a request was sent to settles it. A pending entry is stamped with that
  peer before its frame is queued. A `Reply` frame naming a nonce stamped with anybody else is
  dropped as a dead letter, and a `ReplyDropped` frame naming one is ignored. Nonces are unique to
  this node, not to the member naming one.

  An ask which ends without its reply, by its timeout or by its future being dropped, releases its
  own reply destination, the one no actor awaits. So an ask which nothing answers costs its entry
  only until its timeout, not until the node dies. A destination the request carried for an actor is
  that actor's to wait for and stays. An actor-origin request has no timeout at all, so its entry is
  instead bounded by the life of the actor awaiting the reply: that actor's termination evicts it,
  through the same watcher mechanism the reference registry evicts routes with.

  A `ReplyTo` may be forwarded to a third node, and each hop chains its reply through the previous
  one. The hop in the middle holds an entry of its own, stamped with the next node and remembering
  the origin it forwards for. Downing evicts it with either: with its next node, which owes the
  reply, as for any request, and with its origin, which nothing would deliver the reply to anymore.
  An entry which was sent can also be released without its reply, by an ask's timeout or drop or by
  its recipient's termination. Its node then sends a `ReplyCanceled` frame to the peer it was sent
  to, on the stream which carried the request. That frame is never sent before the request is
  queued, so the cancellation never overtakes it. That peer releases what it forwarded on behalf of
  that entry and cancels further down the chain in turn, so no hop outlives the origin's interest.
  The cancellation outlives the receipt: a reply destination forwarded only after it was canceled is
  canceled at its next peer right behind the request carrying it. A cancellation is fire-and-forget
  like `ReplyDropped`: one lost with its connection leaves the hop's entry to downing.

  A `ReplyTo` serialized anywhere else, e.g. through a codec call of the application's own, is
  stamped with no peer, so neither downing nor a cancellation reaches its entry, and its bytes
  may never come home. No `Reply` or `ReplyDropped` frame settles such an entry either, since a
  frame settles only an entry stamped with its sender; only the bytes returning to this node and
  being deserialized there do. An ask's destination is hence also bound by the ask itself: once the
  ask has returned or its future was dropped, the membership tick releases the entry, canceling it
  at its peer if it was sent. An actor-origin destination stays bound by its actor.

## Discovery

The first reference cannot travel inside a message, so it is resolved by name instead.
`cluster::register(&Key::new("worker-pool"), actor_ref)` names an actor of this node, and fails with
`RegisterError::NotLocal` for a reference to an actor of another node.
`receptionist::lookup_at(&Key::new("worker-pool"), addr)` resolves that name at the Up member
advertising `addr`, from the registrations that member replicated here (see Receptionist below). No
frame is sent for a lookup: the answer is this node's copy of the member's registrations. Only a
join makes a node a member, so an address which is not a member's answers empty.

A `Key<M>` carries the message type next to the name, and the type travels as its manifest:
`M` implements `cluster::Manifest`, whose `MANIFEST` is a stable name chosen by the application,
e.g. `"worker-job"`. A key naming another manifest is refused as `TypeMismatch` rather than
resolved into a reference which drops every message told to it. Serialized references carry the
manifest the same way.

tellus compares manifests, but it cannot check that a manifest names the same type on every node. A
manifest must be unique per type across the cluster, and every node must use a compatible payload
schema for it, which the schema version every payload carries checks, see Rolling upgrades below.
Collisions across nodes are unsupported: the receiving node decodes a payload with
its own type for the manifest. Two different types whose serialized layouts are compatible hence
decode, and the message delivered is silently misinterpreted; incompatible layouts are dead-lettered
as undecodable. Debug builds panic when two types of one process claim the same manifest, and that
is the only collision detected. tellus implements `Manifest` for no type of its own or of the
standard library. So every manifest in a cluster is one the application chose.

Discovery has these properties:

- **A lookup names one address, the directory holds every member.** Resolution composes with
  whatever names addresses already, DNS or an orchestrator, rather than duplicating it;
  `receptionist::lookup` is the same answer over every Up member at once.
- **An empty answer is the ordinary answer while a cluster starts up**: while the node at the
  address has not joined yet, while it has joined but not registered anything, and while its
  registrations have not been replicated here yet. Replication takes a round with no stated bound.
  Callers poll `lookup_at` or watch `receptionist::subscribe`. `receptionist::settled` says only
  that a snapshot from every Up member has arrived; a registration or termination after that
  snapshot shows up with the next replication.
- **More than one actor may hold one key**, and a lookup answers with every one of them at that
  address; pub-sub below is the directory used for fan-out.
- **A registration lives as long as the actor.** Naming an actor binds it exactly as serializing a
  reference to it does, and the same eviction drops both once it terminates.
- **The answer names an incarnation**, since a replica is held per member incarnation, so a
  resolved reference is the same kind of reference as one which arrived inside a message and stops
  working when that node is replaced.
- **Registering an actor which has terminated already registers nothing**, and returns `Ok`, since
  the registration would outlive its actor by no time at all.

## Rolling upgrades

A message type changes while nodes of the old and of the new application run side by side, so
every payload carries what crossing that boundary needs. Every type crossing nodes implements
`cluster::Schema`, which `Manifest` requires: a schema version, `VERSION`, sent alongside every
payload of the type; `decode`, which the receiver calls with the version the payload carries; and
`encode`, which the sender calls with the `Peer` the payload is for. The defaults encode and decode
the current shape as `VERSION`, 0 unless overridden, and refuse every other version. tellus
implements `Schema` for the types with one shape forever: `bool`, `char`, the integer and float
primitives, `String`, `()`, and `Option` and `Vec` of each of those, plus `ActorRef` and `ReplyTo`.
A container of an application type is no `Schema`, since its elements' hooks would never run; the
application wraps it in a type of its own. A field of an application type inside a message is the
same case: its hooks never run either, so a change inside it bumps the enclosing type's `VERSION`.
`Result` is no `Schema` either, so a reply of `Result<T, E>` crosses nodes in a type of the
application, usually an enum. Since `Manifest` requires `Schema`, a published or registered type
deserializes as well as it serializes.

Old code cannot be taught a new shape, so only the new code bridges the two. New code reading an
old payload is an upcast: `decode` matches the old version, decodes the old shape into its own type
and converts, as persistence does in `Versioned::decode`. Old code reading a new payload needs the
new code to send the old shape: `encode` reads `Peer::app_version`, the version of the application
the peer announced at its handshake, and encodes the old shape when that version predates the
change, usually through a view borrowing the fields of the current shape. The announced version is
the `app_version` of `EndpointConfig`, a semantic version compared by semver precedence. It is
`None` when the peer configured none, and before a handshake with the peer has completed, which a
message enqueued ahead of the first connection sees, since a payload is encoded when it is told. An
`encode` override treats `None` as the oldest version it still encodes. That makes those first
messages lossy, between two new nodes too: they go out in the oldest shape, and the receiver's
upcast delivers them without a trace of what the old shape lacks. Encoding the current shape for
`None` instead would make them dead letters at an old peer during a rollout, so delivering them is
the better of the two, and the window is one handshake, see Limitations. A downcast which meets a
value the old shape has no place for, a new variant for instance, fails, which makes the message a
dead letter at the sender for a tell and `NotEncodable` for an ask.

A payload of a version the receiving type does not decode is a dead letter: the warning names the
type and both versions, an ask fails as `NoReply`, and nothing is decoded with the wrong shape. That
holds in every codec. With postcard, which is positional, a shape changed without a version bump is
the one thing nothing catches. Two fields of one type swapped, a field replaced by another of the
same type, a field whose type changed, or a variant inserted or reordered decode silently wrong; a
`u32` read as `i32` turns 7 into -4, since postcard zigzag-encodes signed integers. A field added or
removed at the end is a dead letter, and elsewhere usually one: a field added in front of a string
or a vec can be absorbed by it. So every change to a shape bumps `VERSION`.

A rolling upgrade then takes one rollout: the new binary bumps `VERSION`, decodes the old version
and encodes it for peers whose application version predates the change, and the operator rolls it
out node by node. A rollback brings old peers back, which the same `encode` serves, as long as the
next release keeps the downcast. Without the sender route, two rollouts do the same: first a release
which decodes both shapes but still sends the old one, then one which sends the new one. A reply
forwarded through a node is re-encoded there with that node's schema and the origin's application
version, as is any payload the node sends. `tellus-cluster-demo` runs that rollout as one of its
faults: five nodes of one binary alternate between application versions 1.0.0 and 2.0.0, one node
per quiet window, with every mixed cluster verified.

Every node of a cluster uses one codec. Its name rides every handshake, and a peer naming another
one is refused as `CodecMismatch`, terminal like `ForeignCluster`, so switching the codec is no
rolling upgrade. The codec name and the rendered application version are bounded to 64 bytes each,
refused by `start_endpoint` and at the handshake alike, and the widest handshake, 195 bytes, is the
floor of `max_frame_size`.

## Receptionist

`cluster::receptionist::lookup(&key)` answers every actor registered under a key on every Up member,
this node included, without sending a frame. `receptionist::subscribe(&key)` returns a
`Subscription` whose `changed` resolves whenever that set changes and whose `current` reads it.
`receptionist::settled(version)` says whether the answer is complete for a cluster state version.
The key is the `Key<M>` of discovery, so a name registered on an Up member only for other message
types is refused as `TypeMismatch` by `lookup` and `current` alike, never answered as an empty set.

Every node replicates the full set of its own registrations as one versioned snapshot per origin
incarnation, grouped by key: each key (kind, name, manifest) once, followed by the IDs of the actors
registered under it. It goes out on every connection setup, in both directions, on every change of
the registry, and it is offered again to every Up peer each `watch_refresh_interval`. That offer
catches up a lane which did not exist when the registry changed; a lane already holding the current
version is sent nothing. Toward a peer the snapshot is not queued but put into a slot on the lane,
which a newer snapshot replaces. The control stream's single writer writes the slot's snapshot chunk
by chunk, back to back. So at most one snapshot is pending per peer whatever the writer's speed, and
a snapshot is contiguous on the stream. Chunks are numbered and sized within `max_frame_size`, an
empty set being one chunk; a key's group may be split across chunks, and a registration which cannot
fit one frame is refused at `register` as `Oversize`. The receiver reassembles chunks per version in
a sparse buffer local to the connection, so memory follows what is received rather than what a frame
declares, and merges the parts of a split group. It drops a snapshot whose chunks hold more
registrations or more bytes than a node may register. It refuses an empty group and an empty chunk
of a snapshot of several, so the chunk count is bounded as well. It applies a snapshot once every
chunk is present, drops an older version when a newer one begins, and keeps the highest version per
origin; a partial snapshot dies with its connection. This node is an origin too: its own registry
set is applied as the self replica through the same path, so local and remote registrations are one
thing to a lookup.

Eviction is synchronous. A member's replica is removed in the node death sequence, before the
synthesized terminated signals are flushed. Every answer is filtered against the Up members at the
moment of reading, both the entries and the origins holding a name under other types. So a lookup
never names an actor of an incarnation this node has marked Down, and a Down origin never turns an
answer into a type mismatch. A snapshot which arrived before its origin was Up here is kept hidden
and wakes nobody, since the set it answers does not change. It becomes visible when the origin is
Up, which is the one change subscribers are woken for. Likewise a snapshot which arrives after its
origin's Down, one already past the delivery gate, is discarded and the published set kept: the
eviction which follows is the one change, an empty snapshot included. A snapshot is stored and
published under the membership lock which judged its origin Up, so a death landing meanwhile waits
for it and then evicts what was published, a second real change rather than a duplicate of the
first. A lane given up on evicts nothing, since a member may stay Up through the other direction.

`settled(version)` is true exactly when `version` is the current cluster state version and every
member Up in it, other than this node, has delivered a snapshot which lookups answer from, an empty
one included. A snapshot held for a member not yet Up does not count, since lookups skip it until
the member is. A replica exists only through its incarnation's own snapshot and leaves with its
Down, so a version whose Up set gained a member is not settled until that member has delivered and
is Up. This is what tells an empty answer from an incomplete one. `cluster::changes` carries the
same judgment as `Change::Settled`, a `Settlement` of version and verdict. It is sent under the
replica lock whenever the verdict or the version it judges changes, in order with the states. So a
consumer learns a flip without polling and never sees a verdict before the state it judges.
`receptionist::settlement` and `settled(version)` compute fresh.

The guarantee is eventual: after the last change, every connected member's answer converges to
the same set, and no answer names an actor of an incarnation this node has marked Down. A
subscriber sees a registration once the origin's set has reached this node and the origin is Up,
never earlier and never after the origin is Down. Cluster-aware routers, round robin or consistent
hashing over the resolved set, fall out of this without further work.

## Pub-sub

A topic is a key under another name: `cluster::pubsub::Topic<M>` wraps the `Key<M>` of discovery.
`pubsub::subscribe(&topic, actor_ref)` registers an actor of this node under it, and
`pubsub::publish(&topic, message)` resolves the topic through the receptionist and tells each
resolved subscriber, this node's own included. This node's own subscribers are told first, each a
clone of the message, which is why `M: Clone` is required. There is no mediator and no second queue.
The message is serialized only if a subscriber lives on another node, and then once per node, since
`Schema::encode` may shape it per peer. Toward each node, the subscribers hashing onto one data
stream share one `Multicast` frame, which names them all and carries the payload once. A group too large for `max_frame_size` is split over several
frames on that stream, in order. The receiving node delivers a multicast to each of its targets in
turn, and a target it cannot deliver to is a dead letter of its own. The answer is the number of
subscribers told, never of deliveries: an unreachable node or a full outbound queue makes the
message a dead letter for every subscriber its frame names, as for a `tell`.

Serializing a `ReplyTo` moves its destination into this node's pending replies, so one
destination cannot travel to several nodes. A message whose serialization takes one is possible
only through a cloneable wrapper an application writes around a `ReplyTo`. The node whose encoding
took the destination gets it as an ordinary message frame to its first subscriber alone, which
receives the working destination, and every further subscriber of that node is told on its own,
finds the destination taken and gets a dead letter. A node whose encoding left the destination
alone, e.g. an older peer for which `encode` omits it, gets its multicast as usual. A local
subscriber, told first, receives its clone as for any local `tell`.

A topic and a discovery key of the same name are distinct keys, since the key's kind travels with
it. So a lookup of `Key::new(name)` never lists the subscribers of the topic `name`, and neither
turns the other into a type mismatch. `receptionist::lookup` and `receptionist::subscribe` on
`topic.key()` are how an application reads or watches the subscriber set.

There is no unsubscribe, exactly as there is no `cluster::unregister`: a subscription lives as long
as its actor, and an actor which leaves a topic while running subscribes a child it can stop
instead. The type rule is the receptionist's: a publish is refused as `TypeMismatch` only when the
name is held on Up members for other message types and for none of the topic's. Subscribers of the
matching type are told whatever else the name is held for.

The guarantee is the `tell` contract restated for a set of recipients, and nothing beyond it.
Delivery is at-most-once per subscriber. Sequential publishes from one node reach one subscriber in
publish order, interleaved with that node's direct tells to it in send order. That holds since every
frame toward one actor rides the one stream of the lane toward its node, a multicast included, "in
order, with gaps" across reconnects. Concurrent publishes fan out interleaved and may reach two
subscribers in either order. A subscriber can receive only the publishes made after its registration
reached the publishing node and its origin was Up there. It receives none earlier and none after its
node is Down there, which is the receptionist's visibility and eviction rule rather than anything
pub-sub adds.

## Failure detection and node death

Every Up member is heartbeated: the periodic gossip is the heartbeat, and every inbound frame counts
as one. A pluggable `FailureDetector` per member turns silence into a local *directly unreachable*
mark, cleared again by the next heartbeat. The default is the phi accrual detector, which learns
each member's inter-arrival distribution and turns one threshold into a per-member deadline. The
deadline detector remains the deterministic choice, e.g. for tests. Since traffic counts, a burst
teaches a detector intervals near zero, after which the silence until the next heartbeat or watch
refresh is the longest a healthy member stays quiet. `cluster::start_endpoint` hence feeds one
detector such a burst and refuses a configuration under which it would not survive the longer of
`heartbeat_interval` and `watch_refresh_interval`. Direct unreachability is an observation, not
member state: its versioned edge is relayed so every connected member can derive the same
reachability graph.

A node warns `member directly unreachable` for every Up member its own detector marks, and repeats
the warning every minute while the mark lasts. A member outside this node's component is the
downing provider's to resolve. A broken link is not: it is a direct edge reported unreachable while
both of its ends stay inside the component, since they still reach each other through other
members. No provider downs either end, so the link lasts until it heals or an operator removes one
end with `cluster::down`, which the warning names as the remedy. Once the statement has been
relayed, every member of the component lists the link in `ClusterState::broken_links` (see
Cluster state).

Downing is a separate decision, made by the pluggable `DowningProvider`. It is polled every tick
with the members outside this node's connected component, the *unreachable* members which
`ClusterState::unreachable` lists. They are passed as `Disconnected` rather than as direct
unreachability marks. An edge is absent if either endpoint reports the other unreachable; an absent
direct edge does not split a component while an alternate path exists. A member hence leaves the
component only once every direct edge between it and the rest of the component has been reported
broken, by either end, and those statements have been relayed here. How long downing takes from then
on is the provider's choice: `KeepMajority` downs only once every unreachable member has been
unreachable for its `after`. A provider's verdict is either the members to down or a *self-down*,
which is what lets a provider resolve a partition instead of only observing one.

Downing runs the node death sequence: mark Down (the tombstone), close the lane, wait out in-flight
deliveries, fail the pending asks, evict the member's registrations, flush the synthesized signals.
A new incarnation joining at a member's address downs the old incarnation the same way, as does
`cluster::down`. So does a member's announced departure, once that member's connection has drained
(see Leaving). A self-down severs every connection instead, exactly as an incarnation which learns
through gossip that it was downed elsewhere.

The default is `KeepMajority`, with the deadline `after` from [Configuration](#configuration). Once
every unreachable member has been unreachable that long, the side seeing a strict majority of the Up
members downs the others and any other side downs itself. An even split goes to the side holding the
member with the lowest address. At most one side of a partition hence survives, provided the member
lists agreed before it. A partition into more than two sides can leave none: when no side holds a
strict majority, nor exactly half including the lowest address, every side downs itself. Two sides
which never gossiped a new member to each other can both count themselves the majority; deciding
from gossiped membership rather than from an external arbiter cannot rule that out. In a cluster of
two every failure is an even split: the higher address downs itself even when it is the lower one
which crashed. So a two-node deployment which must survive either one failing needs a third member.
`DownAfterDeadline` is the unilateral alternative, downing whatever is unreachable past a deadline:
each side of a partition downs the other and one cluster becomes two, each honestly satisfying its
own synthesized-signal contract. It is a development and testing choice, where downing on nothing
but a deadline is what makes a node death reproducible.

What no provider removes is the fundamental part: a crashed node and an unreachable one are
indistinguishable, so a surviving side downs members which may well still be running, and a
minority keeps working until it notices and downs itself. Either way a downed node rejoins only as
a restarted process, and the synthesized signals stay true for the endpoint which flushed them,
which is exactly what the weaker tier of the watch contract promises.

The watch protocol is self-healing without any redelivery machinery: `Watch` frames are re-sent
after every reconnect and re-asserted every `watch_refresh_interval` (registration is idempotent
on the watched node). A `Watch` or `Terminated` frame lost with a broken connection is hence
compensated on the next connection or the next refresh. A re-sent `Watch` for a meanwhile
terminated actor is answered with `Terminated` right away. The refresh covers the loss a reconnect
cannot see: a `Terminated` frame lost with the *watched* side's connection while the watcher's own
lane never broke. The healed answer still reflects a real termination, delayed by at most the
refresh interval, which stays below the failure detection deadline, so a quiet pair heals by
answer rather than by a false node death. The downing provider covers the remaining case of a node
that never comes back.

## Cluster state

`cluster::cluster_state` subscribes to this node's view of the cluster. It returns a
`ClusterStateSubscription` whose `current` answers the versioned `ClusterState` as of now, whose
`changed` resolves once a newer version has been published and whose `wait_for` resolves with the
first state it reads which satisfies a predicate. Underneath is a `tokio::sync::watch` channel which
holds the current value in an `Arc`. A value is the member list plus the Up members this node
derives as unreachable, both of the same version. A value also names this node, address and
incarnation, so a consumer tells its own entry from a predecessor's at the same address.
`Member::incarnation` exposes the same identity per member, opaque and ordered. A value also names
its cluster as a `ClusterId` once this node has committed one, `None` before. A `ClusterId` is
opaque too, and ordered by formation time. `Member` always serializes, since gossip carries it in
that form. `ClusterState` serializes with the `serde` feature, and a deserialized `ClusterState` is
a mirror, nothing in tellus consumes it. There are no deltas: `ClusterState::diff` computes the
transitions between two values. A stream of whole values hence needs no catch-up snapshot, which
removes the race between a snapshot and a stream of transitions. A `Transition` names one member:
`Up`, `Left` or `Down` when it is newly listed in that state, and `Forgotten` when its Left or Down
entry was swept after its retention. `Unreachable` and `Reachable` say that an Up member entered or
left the unreachable set. A value also lists the foreign clusters seen at discovered addresses (see
the foreign cluster probe under Membership), each with its address and cluster ID, ordered by
address. A change of that list is a change of the value like any other, but `ClusterState::diff`
yields no transition for it, since it says nothing about this cluster's members.

A value also lists the broken links (see Failure detection and node death). Each is the observer
and the subject of one unreachable statement whose two ends are Up members inside this node's
component, ordered by observer, then subject. A link both ends report broken is hence listed
twice, once per direction. Members outside the component are in the unreachable set instead, so a
statement involving one of them is never a broken link. The list is derived from the same member
list and reachability graph as the unreachable set. A change of it is a change of the value, and
`ClusterState::diff` yields no transition for it either.

The value is published by the mutation, not by a loop. Every path which changes the member list or
the reachability graph runs through one update operation on the endpoint. Under one guard, it
applies the change and promotes the parked reachability evidence the change made admissible. It then
derives the unreachable set and the broken links from the resulting member list and publishes the
value. The membership loop is one caller, its retention sweep and foreign cluster expiry included.
The gossip and reachability merges, a join admission, a `down` call, node death, a self-down, a
leave, forming a cluster and a foreign cluster sighting are the others. A gossip merge applies every
outcome, a superseded incarnation's Down included, inside the same update. So no value ever holds
two Up incarnations at one address or a member Up which the merged gossip declared Down. Only the
consequences of a death (closing the lane, failing asks, flushing synthesized signals) run outside.
The last value matters most: a self-down and a leave end the membership loop for good. Both publish
this node's own entry before that, Down after a self-down and Left after a leave, which a
loop-driven publication could never show.

Two things follow from publishing at the mutation rather than at the tick. A reachability
statement which arrives from a peer and cuts this node's component dates the member's
unreachability on arrival rather than at the next tick, so a deadline provider can fire up to one
heartbeat earlier. And the value is published only on an observable difference: a heartbeat whose
merge changes nothing publishes nothing, so versions count observable changes exactly.

The guarantee: every value a subscriber observes was this node's cluster state after some change,
derived under the same guard as the change. Versions increase strictly, one per observable change. A
value's members are what `cluster::members` answered at that version, since `members` reads the
current published value. Its unreachable set is derived from that member list. Separate calls may
straddle an update. The subscription holds the current value only: a subscriber which reads less
often than the state changes skips intermediate values, which a renderer deriving from the current
value never notices. A consumer which needs every version takes `cluster::changes` instead: one
whole value per version and one settled verdict per change of it, in publication order. They come
from a single bounded channel of `change_capacity` entries (see [Configuration](#configuration)).
Where the consumer fell behind it gets an explicit gap naming how many changes the channel
overwrote. The current value and verdict follow and supersede whatever the channel still held. A lag
is hence always announced and ends on the current value, and the order of publication is kept
throughout. The channel buffers for a subscriber that exists; a new one starts from the current
value and verdict. Both are fed by the same update: the watch first, then the channel under the
receptionist's lock. That lock keeps its own copy of the published state and never touches the
watch. So the stream never shows a version the watch has not, nor a verdict before the state it
judges. A consumer holds no lock of the endpoint: the subscription hands every state out as an
`Arc`. So a consumer may keep a state for as long as it likes, ask the receptionist with it in hand
and call `cluster::down` or `cluster::form`, which publish on the caller's thread. The value
deliberately omits the instant since which a member has been unreachable: that feeds the downing
provider's deadline, and a consumer deriving from the Up set has no use for it.

## Trust

TLS is mandatory for QUIC, but server-only TLS authenticates just the dialed side: any client able
to reach the port can complete a handshake. Production deployments hence use mutual TLS via
`QuicTransport::mutual_tls`. Every node presents one certificate as both its server and its client
identity, verified against the cluster's certificate authority. A stranger hence cannot complete a
connection and reaches neither the protocol nor the membership. Every node's certificate must also
be issued for the cluster's `server_name`, which every dialer verifies with TLS; the address
binding below comes on top of that. Issuing and renewing the
certificates is established automation, e.g. cert-manager on Kubernetes, whose `Certificate`
requests both forms of address binding below. SPIRE cannot issue either form. The SPIFFE X.509
SVID specification forbids a URI SAN next to the `spiffe://` ID, which excludes the `tellus://`
form, but it permits other SAN types, IP addresses included. A SPIRE registration entry adds DNS
names only, though, so an SVID issued by SPIRE carries no IP address SAN either.

The TLS configuration uses rustls's process default `CryptoProvider` if the application has
installed one, otherwise ring's default provider. tellus never installs a process default itself.
QUIC needs TLS 1.3 with the `TLS13_AES_128_GCM_SHA256` cipher suite. An installed provider lacking
either makes `QuicTransport::mutual_tls` fail; it does not fall back to ring. `QuicTransport::dev`
chooses the provider of its client side the same way, while its server side always uses ring's
default provider.

An authenticated node is additionally bound to the address it advertises: where the transport
proves a peer identity, the admission requires the certificate to cover the advertised address, in
both directions, and drops a mismatch without an answer. A certificate covers an address in one of
two ways. If it carries a URI subject alternative name of the form `tellus://<ip:port>`, the
advertised address must equal one of these URIs exactly, and its IP address SANs are ignored. The
scheme is matched case-insensitively, and an IPv6 address is written in brackets, as in
`tellus://[fd00::7]:4000`. Without such a URI, the advertised IP must be among the certificate's
iPAddress SANs, which covers every port of that IP. URIs of other schemes, such as SPIFFE IDs, are
ignored. DNS names are not resolved and hence never matched against the advertised address.

The two forms differ in what an authenticated-but-lying node can claim. With a `tellus://` URI it
cannot claim another member's address and have the healthy node there downed. With IP SANs alone the
binding holds per IP rather than per address: the node can claim any port of an IP its certificate
names. That is safe only while every IP hosts at most one node, as with one pod per IP on
Kubernetes. Nodes sharing an IP need certificates with the URI form. cert-manager issues it via the
`uris` field of a `Certificate`, next to `ipAddresses` for the IP form. Its CSI driver, which issues
a certificate per pod, accepts IP address SANs only literally and substitutes no pod IP into either
form. So pods with changing IPs get their certificates from a step of the deployment's own, e.g. an
init container requesting one for the pod's IP. Only mutual TLS closes the gap, because without a
client certificate the accepting side sees no identity to check.

`QuicTransport::dev` mints its certificate with the bind IP and asks for no client certificate. In
development the check hence holds for the dialer only, and only where the bind IP is the advertised
one: a node bound to an unspecified address but advertising another fails it. Everything under that
transport is unverified. One boundary remains documented rather than closed: certificates are read
once at startup, so a renewal takes a process restart until hot rotation via quinn's server config
swap is added.

## Limitations

- **One endpoint per process, at one address.** An endpoint is never stopped or restarted, so a
  node which was downed or has left exits, and only a restarted process, with a fresh incarnation,
  rejoins. Every member must reach a node at the address it advertises, since a dialer refuses a
  reply handshake naming another address than the dialed one. The advertised address is an IP and
  a port, so it can be neither a hostname nor an address which some members see translated by
  NAT. A node coming back at a new address is a new node. Its old incarnation stays Up at the old
  address until failure detection and the downing provider down it.
- **Both directions must be dialable, and one capacity serves each peer.** A node sends everything
  toward a peer over the connection it dialed itself, so a pair in which only one side can dial the
  other looks like a broken link or a partition. All actors of a node share one `outbound_capacity`
  per peer. A burst toward one actor of a node can hence turn every other tell toward that node into
  a dead letter until the queue drains.
- **Head-of-line blocking is bounded rather than gone.** Recipients hashing onto the same stream
  still delay each other, and a pool of `max_streams_per_peer` streams cannot separate an unbounded
  number of recipients. About one recipient in `max_streams_per_peer` shares a stream with a given
  bulk transfer. With the default of 16, an ask toward such a recipient waited about 40 ms behind a
  transfer of 64 asks of 64 KiB on loopback, which set the p99 of all asks toward that node. 100
  streams lowered that p99 below 1 ms in four of five runs, at 2 to 4% less bulk throughput.
- **A partition can split the cluster for a while, or for good.** Downing decides from gossiped
  membership and versioned reachability, not from an external arbiter. `KeepMajority` lets at most
  one side of a partition survive, provided the member lists agreed before it. Two sides whose lists
  disagreed can both count themselves the majority. Joins are held back while a member is outside
  the component, so neither side can grow into a majority by admitting new nodes. A partition into
  more than two sides downs every side when no side holds a strict majority, nor exactly half
  including the lowest address. Even when one side survives, both run until the minority's own
  deadline passes and it downs itself. In that window the majority may already have downed the
  minority's members and flushed synthesized signals for actors which still run. `DownAfterDeadline`
  splits the cluster by design. Downed members must restart to rejoin, and the split-brain problem
  itself is fundamental.
- **Gossip costs a frame per member and heartbeat interval.** Every node sends one frame to every
  member every heartbeat interval, O(n^2) frames per interval across the cluster. A frame carries
  the full member list only after a change, after a reconnect and on every 10th tick, so an idle
  cluster copies its O(n^3) entries once per 10 intervals rather than every interval. On an idle
  cluster on loopback (`just bench-gossip <size>`, default intervals, frame payload without framing
  or transport overhead), gossip took 320 bytes/s across 5 nodes and 1,333 bytes/s across 8 nodes.
  When every frame carried the full list, it took 2,660 and 11,815 bytes/s. An empty frame still
  pays the per-frame framing and transport overhead. That is fine for the cluster sizes this
  targets, not for hundreds of nodes.
- **A broken link is left to an operator.** When two members cannot reach each other directly but
  both stay inside the component, no provider downs either. Between exactly those two, frames are
  queued while the lane keeps dialing, and messages become dead letters once `outbound_capacity`
  is full. Asks between them resolve only by their timeout, and actor-origin requests not at all.
  Watches between them fire neither a real nor a synthesized signal. The link lasts until it heals
  or an operator downs one end with `cluster::down`. It is visible: each end warns
  `member directly unreachable` every minute and names `cluster::down` as the remedy. Once the
  statement has been relayed, every member of the component lists the link in
  `ClusterState::broken_links`. Why not down one end automatically? That would let one end of a
  working cluster manufacture the other's tombstone.
- **A leave is acknowledged by the peer's delivery path, not by its actors.** The departure is
  gossiped as Left, however it reaches a member. A peer answers the leaver's `Drained` marker with a
  `DrainAck` once every data stream has ended and its frames were dispatched to a mailbox or a
  logged dead letter. No actor need have handled them. A peer which does not acknowledge within the
  leaver's `leave_timeout` appears in the `Departure` as unacknowledged or timed out. A peer which
  learns the departure before the leaver's connection exists, e.g. right after the cluster
  converged, waits for that connection up to its own `leave_timeout`. When the leaver never reaches
  it, its death and its watchers' synthesized signals come that much later. A peer the leaver cannot
  reach still learns the departure through relayed gossip or, with no live path at all, through
  failure detection and downing. A peer defers the leaver's death for at most its own
  `leave_timeout`. Whatever is still unread then, e.g. behind a stalled stream, is lost, and its
  watchers get synthesized signals instead of real ones. A departure which ends without a report
  fails safe: this node is downed and severed first, and every caller gets `LeaveError::Incomplete`.
  Whether the peers received what was queued for them is then unknown.
- **Bootstrap decides from what discovery shows.** Under `Majority`, two nodes form separate
  clusters only when their views have different lowest addresses, e.g. when discovery shows each of
  them a different subset of the deployment. Take a deployment of N nodes whose discovery resolves
  no other address. With `min_peers > N/2`, every two settled views share an address. That rules out
  disjoint views, but not overlapping ones whose lowest addresses differ. A view missing an address
  throughout the settle window is what the majority is computed against. Formation fails closed,
  which costs liveness: while the lowest address of the universe does not answer, nothing forms. A
  discovery which keeps resolving nodes that are gone hence never lets a cold start finish.
  `MajorityWithFallback` is the opt-in which restores that liveness at the price of uniqueness: a
  lowest address which is unreachable rather than gone can form a second cluster.
- **Two clusters never merge.** The foreign cluster probe lists the other cluster in
  `ClusterState::foreign_clusters` on both sides, and each side logs it at error level every 5
  min. Recovering is an operator's task: stop every node of the younger cluster, the one with the
  greater ID, since cluster IDs order by formation time, then start them again. A node restarted
  while others of the younger cluster still run can join that cluster again, since bootstrap joins
  through whichever resolved address admits it first.
- **An ambiguous join postpones formation, within a bound.** A join attempt which fails or panics
  after it started to send its `Join` handshake, short of a definitive answer, leaves an ambiguity:
  the seed may have admitted this node. The endpoint refuses to form until that address answers
  definitively, leaves a settled discovery view or `ambiguous_join_bound` passes. Joins continue
  through any seed meanwhile. Past the bound this node can form although the seed's cluster still
  counts it as a member, until that cluster's failure detection and downing remove it. Pinning on
  the ambiguity instead would wedge bootstrap behind any slow seed, which costs more than the
  ambiguity does.
- **Fences are bounded.** Detailed Left and Down entries are remembered for `down_retention`. After
  that, the greatest incarnation per address remains as a watermark with its state, which refuses
  zombie processes while admitting a newer restart. Watermarks ride connection setup rather than the
  heartbeat. `down_watermarks` caps them softly, since a watermark learned within the retention is
  kept, and twice `down_watermarks` is the hard ceiling. An address evicted from a long-churning
  cluster falls back to the behavior before watermarks: a zombie's `Join` handshake is admitted
  again. `down_watermarks` must not be larger on any node than on the nodes joining it, so in
  practice it is the same on every node.
- **The identity binding matches `tellus://` URI SANs and IP address SANs only.** A deployment
  whose certificates carry nothing but DNS names, the `server_name` among them, must issue them
  with the advertised addresses added. SPIRE cannot issue either form, see Trust.
- **A refusal costs one attempt per backoff.** An address no Up member advertises is refused when it
  answered without speaking this protocol, shares no version with it, failed the identity or address
  check, belongs to another cluster or sent a frame beyond `max_frame_size`. A refused address is
  not dialed again until the refusal expires, after 10 s doubling up to 10 min with every further
  refusal. A refusal expired for 10 min is forgotten, so the next one starts at 10 s again. A
  member's address is never refused but retried, logging each failure. A tellus node coming up at a
  refused address is reachable at once when it dials this node, joins it or is gossiped as Up.
  Otherwise it is reachable from here once the refusal expires.
- **Registrations are replicated whole.** Every change of a node's registrations sends its full set
  to every Up peer as one snapshot; there are no deltas. A node with many registrations which change
  often hence pays for the whole set on every change. 2^20 registrations and 64 MiB per node are
  safety caps, not a sizing target. `cluster::register` enforces them with `TooMany` and `TooLarge`,
  and every receiver enforces them again while reassembling a snapshot, so a peer cannot make this
  node buffer more. The bytes are counted as if every registration carried its key, which bounds the
  snapshot however it is grouped. A single registration is bounded by the frame size. The cap of
  2^16 members, plus twice `down_watermarks` fences, applies to join snapshots only.
- **A shape is known to the application alone.** tellus carries a schema version with every
  payload and hands the peer's application version to every `encode`, but which shape a peer
  understands, how an old one is upcast and how a new one is encoded for an old peer is the
  application's `Schema` implementation. A shape changed without a version bump is caught by
  nothing under postcard, see Rolling upgrades, and a tellus 0.5 node does not speak this protocol
  version at all. A message told toward a fresh incarnation before its handshake has completed is
  encoded with `None` as the peer's version, hence in the oldest shape the application still
  encodes, and the receiver's upcast delivers it without a trace of what that shape lacks. The
  window is the handshake of the first connection. A lane toward a new member usually opens through
  gossip before an application holds a ref at that address, but a ref carried in a message from a
  third node can arrive earlier.
- **A publish costs one frame per node and data stream, not one per node.** A node's subscribers
  spread over the streams of the lane toward it like any recipients, so a topic with many
  subscribers on one node costs up to `max_streams_per_peer` frames toward it. The receiving node
  decodes the payload once per subscriber. A per-node mediator would send one frame, at the price of
  a hop and a second queue between publisher and subscriber; it is not part of this version.
