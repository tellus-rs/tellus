# tellus-cluster-demo-tui

A terminal observer for the [cluster demo](../tellus-cluster-demo): five nodes under continuous
chaos, watched from one screen while the chaos agent keeps running. It changes nothing in the
cluster.

This package is not published; it is part of `just all`.

## Running

With the Docker Compose stack up (`just start-cluster-demo-compose`), or the Kubernetes (kind) stack
(`just start-cluster-demo-k8s`, see the demo's README for its prerequisites):

```shell
just run-cluster-demo-tui     # watch the running demo
```

The observer is built from source and runs on the host, not in a container; both stacks map the
same host ports, so it works against either unchanged.

Keys: `q`, `Esc` or `Ctrl-C` quit, `↑`/`↓`, `j`/`k`, `Tab` or `Shift-Tab` select a node, `p`
pauses the timeline, `c` clears it. Pausing freezes the visible timeline window only; everything
else keeps updating and entries keep arriving behind the window.

## Configuration

Two environment variables, both optional:

- `TELLUS_NODES`: comma separated base URLs of the nodes' HTTP APIs, by default
  `http://localhost:8091` to `http://localhost:8095`, the host ports both stacks map.
- `TELLUS_VERIFIER`: the verifier's base URL, by default `http://localhost:8081`.

## What it shows and where it comes from

- **The membership matrix**: one row per node and one column per address any node lists or
  advertises. Each cell is what the row's node currently says about the column's address: `up`,
  `left` (departed through a leave), `down`, `up+down`, `unreach` (Up but derived unreachable),
  `me`, or `?`. `up+down` is a retained Left or Down entry beside a fresh Up one, i.e. a restart
  whose old incarnation is not forgotten yet. A partition shows as a block pattern, convergence as
  the matrix turning uniform. A row whose inspect stream is lost is dimmed and marked stale, since
  nothing feeds it anymore.
- **The selected node's detail**: both streams' health, the cluster state version, the phase and the
  cluster ID. It shows the receptionist's counts and its verdict with the version it judged, flagged
  when that is not the state's version. The member list with incarnations, the unreachable members,
  the publishers heard within the freshness window and the last probe round trips follow. It also
  lists the broken links, each as `observer ↛ subject`, and the addresses which answered as members
  of a foreign cluster, each with that cluster's ID. Both read `none` while there is none, and any
  entry is highlighted. A broken link lasts until it heals or `cluster::down` removes one of its
  ends; a foreign cluster never merges.
- **The verifier strip**: the chaos agent's current action, how long it has been quiet,
  verifications, violations, the load balancer's availability and its longest outage.
- **The timeline**: membership transitions per node, verdict flips, phase changes, chaos
  transitions, verdicts, violations and load balancer changes, merged across sources and ordered
  by the servers' own timestamps. Times are UTC, and the timeline keeps the latest 1000 entries.

The detail needs a terminal at least 100 columns wide or 35 rows tall; a smaller one shows the
matrix and the timeline only. Below 60x15 the observer shows nothing but a note that the terminal is
too small.

Every node is followed through two streams. `/inspect/events` is
[`tellus-cluster-inspect`](../tellus-cluster-inspect): one whole `ClusterState` per version and the
receptionist's verdict whenever it or the version it judges changes, in the order tellus published
them. `/events` is the demo's own: the phase and the receptionist's counts for the demo's keys. The
matrix, the detail and the strip are rendered from the latest snapshot each source delivered. The
timeline is the one history, built only from the streams, which replay their retained history on
connect and continue after `Last-Event-ID` on a reconnect. The polled `/cluster`, `/probe` and
`/status` fill in only what no stream has said yet.

Some consequences worth knowing:

- Transitions name the member, address plus the last four digits of its incarnation, so a restart
  reads as the old incarnation down and the new one up. The cell stays per address and reads
  `up+down` until the old entry is forgotten.
- A node restart, seen as a changed `started_at` on the inspect stream's hello, resets that node's
  row and its diff baseline; the first state afterward seeds the baseline without producing
  transitions. When a stream reconnects and the server no longer retains the client's position
  ("history lost"), the baseline is cleared as well. The server then hands out the retained history
  followed by the current state and verdict. A state equal to the displayed one re-arms the diff,
  and an older one is ignored.
- A `fell behind` entry is tellus's own `Change::Gap`: the node's subscriber missed that many
  changes, and the current state and verdict follow, so nothing stays stale.
- A verdict streams the moment it flips and is shown with the version it judged, never as if it
  judged another state.
- Each stream's glyph turns red when it is lost, and a loss is one timeline entry, not one per
  reconnect attempt. Such connection entries carry the observer's own clock, since no server
  stamped them.
