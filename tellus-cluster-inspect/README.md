# tellus-cluster-inspect

An [axum](https://crates.io/crates/axum) `Router` which lets you look into the
[tellus](../tellus) cluster node it runs in. Mount it into your application's own HTTP server:

```rust
let inspect = tellus_cluster_inspect::router(InspectConfig::default())?;
let app = Router::new().nest("/inspect", inspect);
```

Call `router` inside a Tokio runtime, since it spawns the task recording the node's changes. Call
it after `cluster::start_endpoint`; before that it fails with `InspectError::EndpointNotStarted`.
`cargo run -p tellus-cluster-inspect --example how_to_serve_cluster_state` serves a cluster of one
this way.

It reveals the cluster's topology, so mount it behind whatever protects the rest of your server;
it brings no authentication of its own and binds no port.

tellus-cluster-inspect is available on [crates.io](https://crates.io/crates/tellus-cluster-inspect):

```sh
cargo add tellus --features cluster
cargo add tellus-cluster-inspect
```

## What it serves

- `GET /cluster`: a `Snapshot` as JSON, in three parts. The node's current `ClusterState` holds the
  version, this node's address and incarnation, the cluster ID and the member list with each
  member's address, incarnation and state. It also holds the unreachable set, the broken links
  between reachable members and the addresses seen in foreign clusters. The endpoint's `Lifecycle`
  is `unformed`, `formed` or `downed`, and the receptionist's `Settlement` says whether every Up
  member's registrations are what lookups answer from, with the version it judged.
- `GET /events`: a stream of server-sent events, one `Stamped` JSON object per message with the
  unix milliseconds it was recorded at and an `InspectEvent` tagged by `kind`:
  - `hello` opens every stream: the log's `started_at` and whether the stream `resumed` right
    after the client's `Last-Event-ID`.
  - `state`: the whole `ClusterState` at a new version, never a delta.
  - `settled`: the receptionist's verdict, whenever the verdict or the version it judges changes.
  - `gap`: the node's own subscriber fell behind by `dropped` changes; the current state and
    verdict follow.

  These are the items of `tellus::cluster::changes`, recorded in publication order. Within the live
  part of a stream, a verdict never precedes the state it judges and versions are consecutive except
  across a gap. A recovery (see below) can start its history at any retained item, and the current
  state and verdict it ends with repeat the latest ones. A client therefore derives no transitions
  from a state of the version it already holds, but still takes that state as its baseline; only an
  older version is ignored. `Snapshot`, `Stamped` and `InspectEvent` implement `Deserialize` as well
  as `Serialize`, so a Rust client reads both endpoints with the same types.

## Resuming

Every logged message carries an `id` in the form `{started_at}:{seq}`. A client which reconnects
with that id in `Last-Event-ID` continues right after it, provided the log (`events_kept`
messages, 1024 by default) still retains that position; the hello then says `resumed: true`.

Otherwise the hello says `resumed: false` and the client is handed a recovery: the retained
history oldest first, without ids, then the current state and the current verdict, of which only
the last carries an id. So a client that drops in the middle of the recovery holds no position and
recovers again, and one that got through resumes from a position at which everything newer than
the pair follows. A changed `started_at` between two hellos means the node restarted.

A client which falls more than `events_kept` messages behind has its stream closed by the server.
Its reconnect then gets `resumed: false` and a recovery.

## Configuration

`InspectConfig` has one field, `events_kept`, the size of the log. It is not part of the node's
`EndpointConfig`, since it is a property of this router, not of the endpoint.

## Events module

The bounded log behind `GET /events` is public as `tellus_cluster_inspect::events` (`EventLog`,
`Replay`, `Stamped`, `Cursor`, `InvalidCursor`, `sse`, `last_event_id` and `now_millis`), so an
application can serve a resumable stream of its own events the same way.
