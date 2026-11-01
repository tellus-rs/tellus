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
[![hotpath-cloud][hotpath-cloud-badge]][hotpath-cloud-url]

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
[hotpath-cloud-badge]: https://img.shields.io/endpoint?url=https://hotpath.rs/badge-cloud.json
[hotpath-cloud-url]: https://hotpath.rs/app/repos/tellus-rs/tellus

An actor framework for Rust, built on [Tokio](https://tokio.rs): typed messages, supervision
trees, death watch with an ordering guarantee, optional event-sourced persistence and optional
clustered remoting over QUIC. Inspired by Carl Hewitt's
[Actor Model](https://en.wikipedia.org/wiki/Actor_model) and strongly influenced by Akka, continued
as open source by [Apache Pekko](https://pekko.apache.org).

tellus is available on [crates.io](https://crates.io/crates/tellus):

```sh
cargo add tellus
```

## Packages

- [`tellus`](tellus): the actor framework itself. See [its README](tellus/README.md) for
  highlights, getting started and the core concepts. Behind the `test-util` feature it provides
  `TestContext` for unit testing actors without a running actor system.
- [`tellus-bootstrap-dns`](tellus-bootstrap-dns): DNS seed discovery for cluster bootstrap, SRV or
  A/AAAA records; the discovery contract and the bootstrap logic ship with `tellus` itself, behind
  the `cluster` feature.
- [`tellus-bootstrap-k8s`](tellus-bootstrap-k8s): Kubernetes seed discovery for cluster bootstrap,
  the pods matching a label selector listed through the API, which sees a pod before it is ready.
- [`tellus-persistence-postgres`](tellus-persistence-postgres): PostgreSQL event and snapshot
  stores for tellus persistence. The contract test suite any store implementation must pass ships
  with `tellus` itself, behind the `persistence-tests` feature, and an in-memory store for tests
  behind `persistence-in-memory`.
- [`tellus-cluster-inspect`](tellus-cluster-inspect): an axum router to mount into your own HTTP
  server, answering a snapshot of the node's cluster view and streaming every change of it as
  server-sent events, resumable and never silently skipping. See
  [its README](tellus-cluster-inspect/README.md).
- [`tellus-cluster-demo`](tellus-cluster-demo): a five node cluster running forever under
  continuous chaos, managed by Docker Compose or in kind: DNS or Kubernetes seed discovery,
  crashes, departures and network partitions once a cycle, and a verifier checking convergence,
  cross-node messaging, receptionist settlement and the pub-sub publisher set in every quiet
  window. See [its README](tellus-cluster-demo/README.md).
- [`tellus-cluster-demo-tui`](tellus-cluster-demo-tui): a terminal observer for the running demo,
  fed by the nodes' and the verifier's event streams: the membership matrix as each node sees it,
  a merged timeline of transitions and chaos, and the verifier's verdicts. See
  [its README](tellus-cluster-demo-tui/README.md).
- [`tellus-comparison`](tellus-comparison): messaging benchmarks comparing tellus against
  [kameo](https://crates.io/crates/kameo) and [ractor](https://crates.io/crates/ractor). See
  [its README](tellus-comparison/README.md) for the methodology, fairness rules and caveats and
  the [comparison dashboard][comparison-url] for published results.

## Status

tellus is under active development and its API is still settling.

Both extensions have landed: event-sourced actors and serializable actor refs, so actors on
different nodes message and watch each other through the same API. Both are feature gated behind
`persistence` and `cluster`, so tellus stays purely local and free of their dependencies by
default. Remoting is clustered: nodes join through seed addresses, the member list is gossiped, and
a pluggable failure detector plus downing provider replace vanished members with synthesized
terminated signals. A node shutting down cleanly announces its own departure. Every node publishes
its view of the cluster as a versioned value. A receptionist answers every actor registered under a
name across the cluster, and pub-sub tells every subscriber of a topic the publisher's receptionist
currently resolves.

The open items on the remoting side are listed under the limitations in
[docs/cluster.md](docs/cluster.md). What comes next, the cluster singleton and then sharding, is
laid out in [docs/roadmap.md](docs/roadmap.md).

## Documentation

How tellus works internally, top-down with links into the implementation:

- [docs/actors.md](docs/actors.md): the core, from the `Actor` trait down to the run loop.
- [docs/persistence.md](docs/persistence.md): event-sourced persistence, from the `EventSourced`
  trait to recovery, fencing and schema evolution.
- [docs/cluster.md](docs/cluster.md): the `cluster` feature, from cluster membership and gossip
  to the wire model, in particular which of the core guarantees carry over the network and which
  weaken.
- [docs/roadmap.md](docs/roadmap.md): what comes next and in which order, from the cluster
  singleton to sharding.

## Development

The [justfile](justfile) defines the usual tasks; `just all` runs check, fmt, lint, test and doc.
Check and lint run across the feature combinations, test across the ones which run tests
differently. Formatting uses nightly rustfmt options, which `just fmt` takes care of. The
`tellus-persistence-postgres` tests run against a container, so `just all` needs Docker.

Regression benchmarks (criterion) run with `just bench`: messaging, and persistence against an
in-memory store. On CI every pull request is benchmarked against its merge base and the comparison
posted as a comment, and every commit on `main` is tracked over time on the [benchmark
dashboard][benchmarks-url].

For a local before and after, `just bench-save <baseline>` and `just bench-compare <baseline>` (and
the `bench-persistence-` pair) compare against a saved baseline. `just profile` and `just
profile-persistence` attribute the time of the hot paths per function with
[hotpath](https://crates.io/crates/hotpath), and their `-alloc` variants the allocations. The
`-alloc-gate` recipes fail if a function which must not allocate does. `just criterion-profile`
runs one criterion benchmark as a hotpath report. `just bench-cluster`, `bench-gossip` and
`bench-streams` measure remoting across spawned processes and do not run on CI.

The benchmarks against kameo and ractor run with `just bench-comparison`; they are excluded from
per-pull-request CI and published to the [comparison dashboard][comparison-url] on version tags
and manual runs.

The cluster demo runs with `just start-cluster-demo-compose`, which needs Docker, or `just
start-cluster-demo-k8s` in a kind cluster, and `just run-cluster-demo-tui` watches it from a
terminal. Both crates are checked, linted and tested by `just all` like the rest; running the
stack itself is a soak test and stays manual.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
