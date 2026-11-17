# tellus-cluster-inspect

An [axum](https://crates.io/crates/axum) `Router` which lets you look into the
[tellus](../tellus) cluster node it runs in. Mount it into your application's own HTTP server:

```rust
let inspect = tellus_cluster_inspect::router(InspectConfig::default(), shutdown)?;
let app = Router::new().nest("/inspect", inspect);
```

Call `router` inside a Tokio runtime, since it spawns the task recording the node's changes. Call it
after `cluster::start_endpoint`; before that it fails with `InspectError::EndpointNotStarted`.
`shutdown` is a future of your own. Once it resolves, that task stops and every `/events` stream
ends. Hand it the signal on which your server shuts down gracefully, or that shutdown waits for
every client following the stream. A future has one taker, so the server and the router each get one
of their own. With a `tokio::sync::Notify` in an `Arc` that is a `notified_owned()` each, and
`notify_waiters()` fires both. Take both before it fires: `notify_waiters()` stores no permit, so a
`notified_owned()` taken later never resolves. A server which never shuts down gracefully hands the
router `std::future::pending()`. `cargo run -p tellus-cluster-inspect --example
how_to_serve_cluster_state` serves a cluster of one that way.

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
history oldest first, without IDs, then the current state and the current verdict, of which only
the last carries an id. So a client that drops in the middle of the recovery holds no position and
recovers again, and one that got through resumes from a position at which everything newer than
the pair follows. A changed `started_at` between two hellos means the node restarted.

A client which falls more than `events_kept` messages behind has its stream closed by the server.
Its reconnect then gets `resumed: false` and a recovery.

## Wire format

A client in another language reads plain JSON. This is what the example above answers, a node which
formed a cluster of one. `GET /cluster`:

```json
{
  "state": {
    "version": 1,
    "this": {
      "addr": "127.0.0.1:46377",
      "incarnation": "01a1e9d7-b22d-72f9-8f5c-1881bcf17826"
    },
    "cluster": "01a1e9d7-b22e-70a3-9952-f9da27c41049",
    "members": [
      {
        "addr": "127.0.0.1:46377",
        "incarnation": "01a1e9d7-b22d-72f9-8f5c-1881bcf17826",
        "state": "up"
      }
    ],
    "unreachable": [],
    "broken_links": [],
    "foreign_clusters": []
  },
  "lifecycle": "formed",
  "settlement": {
    "version": 1,
    "settled": true
  }
}
```

Incarnations and cluster IDs are UUIDv7 strings, and `cluster` is `null` while the node is a member
of none. A member's `state` is `up`, `left` or `down`. `unreachable` holds members in the same
shape, `broken_links` holds `{"observer": member, "subject": member}` and `foreign_clusters` holds
`{"addr": ..., "cluster": ...}`.

The first connection to `GET /events` gets a hello and a recovery (`this` and `members` elided):

```text
data: {"at":1794924589621,"event":{"kind":"hello","started_at":1794924589614,"resumed":false}}

data: {"at":1794924589614,"event":{"kind":"state","version":1,"this":{...},"cluster":"01a1e9d7-b22e-70a3-9952-f9da27c41049","members":[...],"unreachable":[],"broken_links":[],"foreign_clusters":[]}}

data: {"at":1794924589614,"event":{"kind":"settled","version":1,"settled":true}}

data: {"at":1794924589614,"event":{"kind":"state","version":1,"this":{...},"cluster":"01a1e9d7-b22e-70a3-9952-f9da27c41049","members":[...],"unreachable":[],"broken_links":[],"foreign_clusters":[]}}

data: {"at":1794924589614,"event":{"kind":"settled","version":1,"settled":true}}
id: 1794924589614:2
```

A `state` event flattens the `ClusterState` next to `kind`, and a `settled` event the `Settlement`.
A gap reads `{"kind":"gap","dropped":3}`. Reconnecting with `Last-Event-ID: 1794924589614:2` gets
the hello with `"resumed":true` and then only what is newer.

## Configuration

`InspectConfig` has one field, `events_kept`, the size of the log. It is not part of the node's
`EndpointConfig`, since it is a property of this router, not of the endpoint. It implements
`Deserialize`, so it can come from the same configuration file, for example in YAML:

```yaml
events_kept: 256
```

A missing field falls back to its default, and an unknown field or an `events_kept` of zero is
refused.

## Events module

The bounded log behind `GET /events` is public as `tellus_cluster_inspect::events` (`EventLog`,
`Replay`, `Stamped`, `Cursor`, `InvalidCursor`, `sse`, `last_event_id` and `now_millis`), so an
application can serve a resumable stream of its own events the same way.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
