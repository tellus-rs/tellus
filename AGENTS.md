# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Commands

Tasks are defined in the [justfile](justfile):

- `just all`: check, fmt, lint, test and doc; the full local gate for every crate except
  `tellus-comparison`. The `tellus-persistence-postgres` tests use testcontainers, so the gate
  needs Docker.
- `just check` / `just lint` / `just test`: the individual steps, each a feature matrix, every run
  scoped to one crate. Check and lint drive the matrix with `cargo hack --feature-powerset`: the
  `powerset` variable in the justfile takes `hotpath`, `hotpath-alloc`, `hotpath-callgrind` and `hotpath-cloud` off the axes and
  declares nothing mutually exclusive, which leaves 60 runs of `-p tellus`. No feature may be
  excluded against one which implies it: cargo-hack resolves a group, so excluding `persistence`
  against `persistence-tests` also drops `persistence-tests,persistence-in-memory`, the one
  combination which compiles `tests/persistence_tests.rs`. Implications need no group anyway,
  since cargo-hack drops a combination whose features resolve to the same set. Then there is plus one `hotpath` run, one
  `--all-features` run (cargo-hack drops its own once either of those flags is given), the two-run
  powerset of each of `-p tellus-bootstrap-dns` and `-p tellus-bootstrap-k8s`, and one run each of
  `-p tellus-persistence-postgres`, `-p tellus-cluster-inspect`, `-p tellus-cluster-demo` and `-p
  tellus-cluster-demo-tui`. Adding a feature therefore extends check and lint by itself; only a
  new implication pair needs a hand edit. Test is hand-picked and deliberately short: check and
  lint already compile every combination, so a feature set earns a test run only if it runs tests
  differently. `cluster` is the only feature the code branches on (`ReplyTo` in
  `request_response.rs` has one implementation with it and one without); every other feature just
  adds items and tests. That leaves two runs of `-p tellus`, no features (the published default,
  and the only run exercising the non-cluster `ReplyTo`) and
  `persistence-tests,persistence-in-memory,cluster-dev,serde,test-util` (every feature that has
  tests, with `cluster-dev` in place of `cluster` since the remoting test needs it,
  `persistence-in-memory` since `persistence.rs`, `host_persistence.rs` and
  `persistence_tests.rs` are gated on it, and `test-util` for `test_context.rs`), plus one run each of `-p tellus-bootstrap-dns` and `-p tellus-bootstrap-k8s` with
  `--all-features`, `-p tellus-persistence-postgres`, `-p tellus-cluster-inspect`, `-p
  tellus-cluster-demo` and `-p tellus-cluster-demo-tui`. `--all-features` is not among them: it
  adds nothing but the `hotpath` instrumentation, which check already compiles, and would run the
  whole suite under it. The remoting test takes about 100 seconds, so running it once instead of
  three times is most of what the short matrix saves. Doctests are the one thing the short matrix
  would miss, since `--all-targets` excludes them and nothing else builds them, hence the separate
  `cargo hack test -p tellus --doc` over the full powerset: it costs seconds, because rustdoc
  consumes the crate rather than producing test binaries and edition 2024 merges a configuration's
  doctests into one binary. No run spans the workspace: a build containing multiple crates
  feature-unifies `tellus` with their features enabled, so only scoped runs exercise the
  reduced-feature configurations, and only a scoped run builds a backend crate against the feature
  set it actually declares (`cargo hack` keeps that property: it is invoked per crate, never with
  `--workspace`). `just doc` runs workspace-wide with all features and `--cfg docsrs`. Every
  exported fallible function and trait method carries a `# Errors` section, enforced by
  `clippy::missing_errors_doc` in the library crates (which misses async trait methods, so those
  are kept by hand), worded as conditions (`Fails if ...`, `Implementations fail if ...`) directly
  under the heading, with consequences left to the prose.
- `.cargo/config.toml` sets `K8S_OPENAPI_ENABLED_VERSION`, which `k8s-openapi` needs whenever
  dev-dependencies are not built: plain `cargo check`, `cargo build`, `just doc` and
  rust-analyzer. Only a binary may pick the Kubernetes version through a `v1_*` feature, so
  `tellus-bootstrap-k8s` enables `latest` in its dev-dependency alone, which is what covers every
  `--all-targets` run, while `tellus-cluster-demo`, being a binary, names `v1_36` itself. All
  three must name the same version; a mismatch fails the build loudly.
- `just fmt`: formats Rust (nightly rustfmt, the justfile derives the matching nightly from the
  installed stable) and TOML (taplo). Plain `cargo fmt` is not enough; the rustfmt config uses
  unstable options.
- Single test: `cargo test -p tellus --test watch <test_name>`; a persistence test needs the
  features its file is gated on, or it silently runs none (`--features persistence-in-memory` for
  `persistence.rs`, `host_persistence.rs` and `persistence_tests.rs`, which also needs
  `persistence-tests`). Integration tests live in `tellus/tests/`: `ask.rs`, `persistence.rs`,
  `fairness.rs`, `how_to_unit_test.rs`, `how_to_unit_test_event_sourced.rs`,
  `persistence_tests.rs`, `pipe_to_self.rs`, `queued.rs`, `supervision.rs`, `termination.rs`,
  `test_context.rs`, `test_context_persistence.rs`, `timers.rs`, `watch.rs`, plus `cluster.rs`,
  which needs `--features cluster-dev`, has no test harness and runs its scenarios from `main`,
  spawning itself as further nodes. `test_context.rs` and `how_to_unit_test.rs` need `--features
  test-util`, and `test_context_persistence.rs` and `how_to_unit_test_event_sourced.rs` also
  `persistence`, or they silently run none. `timers.rs` holds one scheduler-sensitive test,
  `stale_timers_do_not_delay_a_stop`, which is `#[ignore]`d and runs only with `-- --ignored`.
- Examples: `cargo run --quiet -p tellus --example getting_started_01_hello` and so on; the getting
  started tutorial lives in `tellus/examples/tutorials/getting_started/`, one example per step.
  The remoting examples need `--features cluster-dev`, e.g. `cargo run --quiet -p tellus --features
  cluster-dev --example remote_scatter_gather` (or `cluster`).
- Cluster demo: `just start-cluster-demo-compose` starts the forever running five node chaos
  cluster (`tellus-cluster-demo`) in Docker Compose and `stop-cluster-demo-compose` stops it and
  drops its volumes; `start-cluster-demo-k8s [dns|k8s]` and `stop-cluster-demo-k8s` do the same in
  kind; `logs-cluster-demo-compose` and `logs-cluster-demo-k8s` follow the logs of the respective
  stack, while `status-cluster-demo` and `violations-cluster-demo` inspect whichever runs, since
  both serve the same ports; `run-cluster-demo-tui` watches it from a terminal
  (`tellus-cluster-demo-tui`). Both crates are part of `just all` like every crate but
  `tellus-comparison`; running the stack itself stays manual. Recipe names are action first:
  `start-`, `stop-`, `logs-`, `status-`, `violations-`, `run-`, and `bench-`, `check-`, `lint-`
  for the comparison crate.

Benchmarks:

- `just profile` / `just profile-alloc`: run `tellus/examples/profile.rs` with hotpath profiling,
  reporting per-function timings or allocation bytes for the instrumented hot path (send path,
  mailbox, run loop, termination). Instrumentation is gated behind the off-by-default `hotpath`
  feature; read the report as relative attribution, criterion stays the source of truth for
  absolute regressions.
- `just profile-alloc-gate`: run the profiling workload with allocation tracking and fail unless
  `tell`, `reserve` and `receive_incoming` allocate exactly 0 bytes; `profile-alloc-check <file>`
  applies that check to an existing JSON report. CI (`profile.yml`) runs each workload twice on a pinned runner image, toolchain and Valgrind package (`VALGRIND_VERSION`), since instruction counts compare only within one such environment. The first run is native and writes `native.json`. The second runs under Callgrind (x86_64 Linux only) with `HOTPATH_MERGE_REPORT` pointing at it. The merged report takes everything but the instruction counts from the native run. Instruction counts complement Criterion and do not show contention, since Valgrind serializes threads. It uploads the merged reports to hotpath Cloud as the `profile` and `profile_persistence` series (PR reports go through `hotpath-relay.yml`), which comments on every PR and posts a `hotpath / <benchmark>` check on its head commit. Under `pr_check = "fail"` in `hotpath/<benchmark>-policy.toml` that check fails only when a zero-alloc rule (an `alloc.total = "0 B"` budget) breaks; allocation regressions above 2% and instruction count changes above 3% are reported in the comment without failing it, and timing is listed (`judged = false`) but never judged. The profile jobs themselves pass unless a report is not merged or lacks instruction counts or timing; make the check required to block merging.
- `just profile-persistence` / `just profile-persistence-alloc`: the same for the persistence code
  (`tellus/examples/profile_persistence.rs`, features `hotpath` plus `persistence` and
  `persistence-in-memory`): command
  settlement (encode, append, apply, snapshot) and recovery (snapshot load, paged read, replay)
  against an in-memory store. Settlement allocates by design (payload buffers, manifests), so most
  of the alloc report is a budget, not a zero gate; the exceptions are `apply_events` and
  `replay_page`, which must stay allocation-free: `just profile-persistence-alloc-gate` runs the
  workload and fails on a violation, `profile-persistence-alloc-check <file>` applies that check
  to an existing JSON report, and CI enforces it as a budget that fails the `hotpath / profile_persistence` check in the same way.
- `just criterion-profile <bench> <id> <iterations> [features]`: run one criterion benchmark
  through hotpath's criterion adapter (the benches import `hotpath::criterion`), executing its
  routine exactly `iterations` times in criterion's test mode and writing one hotpath report.
  CI (`criterion-profile.yml`) runs it for `ping_pong/pairs/1` the same way `profile.yml` runs the
  profile examples (native run, then a merged Callgrind run on the same pinned environment), as the
  `criterion-ping_pong-pairs-1` series, judged under `hotpath/criterion-ping_pong-pairs-1-policy.toml`
  with `pr_check = "fail"` and relayed by `hotpath-relay.yml`. It covers what the profile examples
  do not exercise: spawning, watching and termination under steady messaging. It never builds a
  mailbox backlog, so its allocations and instruction counts repeat across runs; benchmarks that
  flood a mailbox (`flood/bounded`, `persist/snapshots`) are left out, since how far flume's queue
  grows depends on the scheduler, which makes their allocations vary by run. Its zero-alloc budgets
  cover `tell` and `reserve`, but not `receive_incoming`, which allocates in this benchmark. Without
  `HOTPATH_CRITERION_ITERATIONS` the adapter forwards to criterion, so `just bench` is unaffected.
- `just check-hotpath-policies`: validate every policy in `hotpath/` against hotpath.rs with
  `hotpath cloud validate-policy`, which needs the hotpath CLI built with its `cloud` feature. The
  server refuses every upload judged under a policy it finds invalid. CI (`hotpath-policy.yml`)
  runs the recipe whenever a policy, the justfile or the workflow changes, with the CLI version
  taken from `Cargo.lock`.
- `just bench`: tellus's own criterion regression benchmarks, messaging
  (`tellus/benches/messaging.rs`) and persistence against an in-memory store
  (`tellus/benches/persistence.rs`, features `persistence` and `persistence-in-memory`); `just
  bench-save <baseline>` / `just
  bench-compare <baseline>` and the `bench-persistence-*` pair for local before/after comparisons.
  CI benchmarks every PR against its merge base, both benches, and posts the comparison as an
  informational PR comment; it does not fail the build. The report flags a benchmark whose 95%
  confidence lower bound exceeds `bench_regression_threshold`, but that verdict is not gate-grade
  on a shared CI runner: `flood/bounded` measured +19.9%, +13.3%, -9.5%, -35.7% and +8.1% across
  five runs of identical code, each with a tight criterion confidence interval, because criterion
  sees within-run variance only. Pushes to `main` additionally publish both benches to the
  gh-pages dashboard (`dev/bench`) via `bench-bencher` and `bench-persistence-bencher`, one
  section per store-result step name: "Core" (messaging) and "Persistence". Adding a section means
  a bencher recipe plus a benchmark and a store-result step in `bench.yml`; `dev/bench/index.html`
  on `gh-pages` is a customized copy of the action's default page (headline plus one sub-heading
  per section) which the action leaves alone once it exists.
- `Stack sweep` (`.github/workflows/stack-sweep.yml`): the performance gate, with its protocol in
  `.github/stack-sweep/README.md`. The label `sweep` on a pull request sweeps the messaging bench,
  `sweep-persistence` the persistence bench, of its head against its parent; `gh workflow run
  stack-sweep.yml -f base=<parent> -f candidate=<commit> [-f bench=persistence]` does the same by
  hand, and `-f null_run=true` without a candidate sweeps the base against itself. It runs the
  bench at 16 worker-stack positions on `ubuntu-latest` and `ubuntu-24.04-arm`, 4 jobs each with an
  A/A control. Where a worker stack lies moves single rows by tens of percent either way
  (`docs/investigations/actor-id-alignment/`), so a regression from one stack position, the PR
  comparison included, needs this before it counts. A `Report` job applies `report.py`'s fixed
  limits (below +3% passes, above +5% fails, borderline rows get a second run whose ID is passed as
  well); `test_report.py` pins them, and CI runs it. `.github/stack-sweep/codegen.sh <base>
  <candidate>` compares the generated code of a bench of two commits per function, a diagnostic
  and never a verdict; `test_codegen.py` pins its comparison rules.
- `just bench-cluster`: the remoting throughput and latency numbers (pipelined and serial tells and
  asks, a bulk transfer), run by the `cluster.rs` test in its bench role across spawned processes;
  not part of CI, for comparing before and after a change to `tellus/src/cluster`.
- `just bench-gossip <size>`: the gossip bytes per second of an idle cluster of `size` spawned
  processes (default 5), after a warm-up, from the `cluster-dev` counter `cluster::gossip_bytes`;
  not part of CI.
- `just bench-streams <streams>`: bulk throughput from one node to one actor and ask latency p50,
  p99 and p99.9 towards echo actors on the target's node and on a third node during the transfer,
  with `max_streams_per_peer` set to `streams` (default 16) on every node; not part of CI.
- `just bench-comparison` plus `check-comparison` / `lint-comparison`: the `tellus-comparison`
  crate benchmarking tellus against kameo and ractor. It is deliberately excluded from `just all` and
  from per-PR CI so its dependencies stay out of tellus's build; touching it means running its own
  check and lint recipes.

Releases:

- release-plz opens or updates a release PR whenever `main` has unreleased changes; merging it
  publishes the crates and tags them (`tellus-vX.Y.Z`, `tellus-persistence-postgres-vX.Y.Z`).
- The website [tellus-rs.github.io](https://github.com/tellus-rs/tellus-rs.github.io) pins
  tellus as a submodule at a release tag. It shows code from the examples and tests of the pinned
  tag and reads the version it displays from there. Its daily `pin-tellus.yml` workflow notices a
  new `tellus-vX.Y.Z` tag, pins it on the branch `pin/tellus-vX.Y.Z` and opens a draft PR listing
  every tellus change since the last pin; `gh workflow run pin-tellus.yml -R
  tellus-rs/tellus-rs.github.io` starts it right away. Before that PR is marked ready, every listed
  change is reviewed for statements on the site which it makes false, with the site's
  `update-site-docs` skill. A tellus PR changing such a statement can say so in its description,
  as a hint for that review.

CI enforces that a PR consists of exactly one commit; squash before pushing.

## Conventions

Commits:

- A commit message is a single subject line of at most 72 characters, without a body, following
  conventional commits (release-plz derives the changelog from it). Commit with `git commit -s`, so
  it carries a `Signed-off-by` trailer, the one exception to the no-body rule.

Writing style, everywhere (code, comments, docs, commit messages, PR descriptions):

- No em or en dashes (U+2014, U+2013) as punctuation; use commas, colons, semicolons or
  parentheses instead.
- US English spelling, in identifiers too: "behavior", "color", "neighbor", "initialize",
  "center", "analyze". Rename an identifier rather than keep a British spelling.
- No AI slop: no "delve", "dive into", "it's important to note", "seamlessly" as filler,
  "game-changer", rule-of-three padding, or bullet lists where prose flows better.
- No marketing bluster: no superlatives about tellus; show results instead of adjectives.
- No essayist metaphor nouns: "seam", "streak", "load-bearing", "surface area", "the shape of X",
  "friction". Say the concrete thing: not "until a streak exceeds `max_restarts`" but "until
  `max_restarts` consecutive restarts have failed". A literal, domain-established use is fine.
- One point per sentence. A sentence carrying a condition, a mechanism and a consequence at once
  is split. Not "For the actor itself the value is a lower bound, since only the actor removes
  items; and because the run loop drops the terminated signals of unwatched actors, the count can
  exceed what `receive` will get by that many." but "For the actor itself the value is a lower
  bound, since only the actor removes items. The run loop drops the terminated signals of unwatched
  actors, so the count can exceed what `receive` will get by that many." Long sentences chain with
  "and" and "but"; they do not nest.
- No aphoristic sentence fragments as punchlines ("Failures are values."). Use a full sentence,
  often opened by a rhetorical question: "Why bother with an uninhabited type? Because ...".

Comments in code:

- A comment states a constraint the code cannot express: an ordering requirement, a type-system
  limitation, a non-obvious invariant. It never explains why a change was made, what the next line
  does or why the code is correct; that is talk for a reviewer and noise once the PR is merged.
  Good: `// A full quota never drains after termination!`. Bad: `/// Crate-private on purpose: for
  a remote reference the depth would need a round trip, so other actors ask through the actor's
  own protocol instead.` A comment running past one line is a sign it explains rather than
  constrains.
- Item documentation describes the item, never anything downstream of it. Naming a dependency is
  fine, naming a dependent is not: no "what the viewer draws" or "what the tests compare". Generic
  role words ("the caller", "an implementation") are fine. This applies to every `///` and `//!`.
- Doc comments are for `pub` items. A non-public item gets one only where the surrounding code
  documents its non-public items too; stay consistent with the file. Integration tests
  (`tests/`) are exempt: a `///` describing what a test proves is fine.

Rust style:

- Avoid type annotations; use turbofish, or `Target::from` / `Target::try_from` where `into` /
  `try_into` does not infer.
- Use simple type names imported via `use`; `anyhow::Result` is the exception and is always
  written fully qualified.
- A qualified path is as short as possible but as long as necessary to be obvious: import the
  module that makes the last segment unambiguous. So `f32::consts::TAU` (with `use std::f32;`),
  never `std::f32::consts::TAU` nor a bare `consts::TAU`; likewise `mem::swap`.
- Import with `crate::` paths, never `super::`.
- Bounds on type parameters and supertraits go in a where clause, never inline.
- Do not swallow errors, e.g. by turning a `Result` into an `Option` via `ok`; an error which
  cannot or should not be propagated is logged at error or warn level.
- No item is `pub` or `pub(crate)` unless it needs that visibility.
- Dependency features belong in the crate's own `Cargo.toml`, not in the workspace root.
- A name says what the thing is, not what it is about. A function name names what it returns; a
  collection name names what it holds, in the plural. `_by_x` is reserved for a collection indexed
  by `x`; anything else leads with the item, so `cell_grouped_positions`, not
  `positions_by_cell`.
- A constructor validates the values it derives as well as the values it takes: anything derived
  by multiplication, addition or conversion can overflow or lose finiteness from valid inputs.
- Once related values have an order in a struct, keep that order in every parameter list, tuple,
  return type and destructuring.
- No blank line between a block and the closing `Ok(...)` which follows it.
- While tellus is 0.x, no `#[non_exhaustive]` or other compatibility machinery: a new variant is a
  breaking change for the next minor release, and exhaustive matches make downstream code decide.

Item order within a file:

- `mod`, `use`, `const`, `static` and `type` come first: `pub mod` before `mod` with a blank line
  between them, then after a blank line `pub use`, and after another blank line `use`.
- Then the items by visibility tier: `fn main` (the only exception), pub types and their impls, pub
  free functions, `pub(crate)` types and their impls, `pub(crate)` free functions, then private
  types and free functions. `#[cfg(test)] mod tests` stays at the bottom.
- Each type's impls follow the type immediately; an impl belongs with the concrete type it is
  `for`.
- Within a tier, items are ordered top-down by call order: a caller comes before what it calls, in
  the sequence the in-file caller calls them, and a struct containing another comes before it.
  Where nothing in the file orchestrates them, keep source order.
- Functions within an impl follow the same order: by visibility first, then by call order.

## Workspace layout

- `tellus/`: the actor framework. The bootstrap crates and `tellus-persistence-postgres` are
  published too (`publish = true`); the inspect crate, the demo and the TUI are not.
- `tellus-bootstrap-dns/`: DNS seed discovery (`DnsSeeds` implements `SeedDiscovery`, SRV or
  A/AAAA records) for cluster bootstrap; the trait and the run-once `cluster::bootstrap` loop live
  in `tellus/src/cluster/bootstrap.rs` (feature `cluster`). Its tests resolve against an in-process
  hickory-server, no network.
- `tellus-bootstrap-k8s/`: Kubernetes seed discovery (`K8sSeeds` implements `SeedDiscovery`,
  listing the pods matching a label selector) for cluster bootstrap; one address per pod, the
  primary one, since bootstrap counts addresses and admits a peer only at the address it
  advertises. Its tests answer the kube client from a `tower-test` mock, no cluster.
- `tellus-cluster-inspect/`: an axum `Router` an application mounts into its own server (behind
  its own authentication, since it reveals topology) to inspect the tellus node it runs in:
  `GET /cluster` answers a `Snapshot` (the serialized `ClusterState`, the `Lifecycle` and the
  receptionist's `Settlement`), `GET /events` streams every change from `cluster::changes` as
  server-sent events (`InspectEvent`: hello, state, settled, gap), resumable with `Last-Event-ID`
  from a bounded `EventLog` (`src/events.rs`). A client that cannot resume gets the retained
  history and then the current state and verdict, captured under the recorder's one lock
  (`src/recorder.rs`) so the pair and the log agree; only the pair's last item carries an id, so a
  client dropping mid-recovery recovers again. Part of `just all` like the bootstrap crates.
- `tellus-persistence-postgres/`: the PostgreSQL stores (`PostgresStore` implements `EventStore`
  and `SnapshotStore`). Tests need Docker (testcontainers) and run as part of `just all`; their
  image tag and credentials come from the root `docker-compose.yaml` via `composed`, so there is
  one place to change either. Its examples need the `postgres` service from the root
  `docker-compose.yaml`: start it with `docker compose up -d --wait postgres`, then `cargo run
  --quiet -p tellus-persistence-postgres --example event_sourced_counter` (or
  `event_sourced_supervision`); `docker compose down` stops it, `down -v` resets the data.
- `tellus/src/persistence_tests.rs` (feature `persistence-tests`, implies `persistence`): the
  contract test suite for persistence stores; backend crates run it from their integration tests
  against real servers. `tellus/src/persistence/in_memory_store.rs` (feature
  `persistence-in-memory`, implies `persistence`): an in-memory `EventStore` and `SnapshotStore`
  for tests, re-exported as `tellus::InMemoryStore`. `tellus/tests/persistence_tests.rs` validates
  the contract suite against it, `tellus/tests/persistence.rs` runs the guarantees over it behind a
  `FaultyStore` which injects delays, failures, panics and hangs,
  `tellus/tests/host_persistence.rs` uses it directly, and the persistence bench and the profiling
  example run on it too.
- `composed`: a crates.io dependency which reads image name, tag and environment variables out of
  a `docker-compose.yaml` at compile time, so testcontainers-based tests use the same image
  version and credentials as the local stack instead of hardcoding them.
  `tellus-persistence-postgres/tests/store.rs` holds it in a `static COMPOSE: LazyLock<Compose>`
  built by the `compose!` macro, whose path argument is relative to the *calling* crate's
  manifest. Lookups panic like assertions, naming the compose file. Deliberately exposes no
  `ports` or `command`: tests use the port testcontainers maps.
- `tellus-cluster-demo/`: a five node cluster running forever under continuous chaos, in two
  stacks running the same two binaries. Docker Compose (`start-cluster-demo-compose`,
  `stop-cluster-demo-compose`, `logs-cluster-demo-compose`, plus the stack-agnostic
  `status-cluster-demo` and `violations-cluster-demo`): DNS seed discovery through a Docker
  network alias shared by all five
  nodes, a chaos agent killing one node (pumba), stopping one with SIGTERM or partitioning two off
  the other three every cycle, Caddy load
  balancing the nodes' HTTP APIs, and a second network carrying nothing but HTTP so a partitioned
  node stays observable while it self-downs. Kubernetes in kind (`start-cluster-demo-k8s
  [dns|k8s]`, `stop-cluster-demo-k8s`, `logs-cluster-demo-k8s`, needing `kind` v0.33 or later and
  `kubectl`): a StatefulSet behind a headless service, a service instead of Caddy, faults from
  `k8s/chaos.sh` through kubectl with the partition a pair of label-driven NetworkPolicies, and
  either discovery backend. Which one a node uses is `CONFIG_OVERLAYS` naming `config/dns.yaml`
  or `config/k8s.yaml`; `seeds` is deliberately absent from `config/default.yaml`, since overlays
  merge and a default would leave both variants. The verifier is the same binary on both stacks
  and holds the cluster to convergence and cross-node messaging in every quiet window. Every node
  mounts `tellus-cluster-inspect` at `/inspect` (the cluster state snapshot and its change
  stream), logs the transitions `ClusterState::diff` yields between versions of
  `cluster::changes`, and serves the demo's own facts, phase and receptionist counts, as a small
  server-sent events stream at `/events`; the verifier serves chaos transitions, verdicts,
  violations and load balancer changes the same way, both through the inspect crate's
  `EventLog`. The crate is a default workspace member gated by `just all`; only running a stack is
  manual. Read its README and `k8s/README.md` before changing either stack.
- `tellus-cluster-demo-tui/`: a ratatui terminal observer for the running demo, read only: it
  follows two streams per node, the inspect crate's `/inspect/events` (cluster state per version,
  verdicts, gaps) and the demo's `/events` (phase, counts), plus the verifier's stream, and polls
  `/cluster`, `/probe` and `/status` slowly for what no stream has said, showing the membership
  matrix (what each node says about every address, dimmed while its inspect stream is lost), the
  selected node's detail, the verifier's counters and a timeline merged by the servers' own
  timestamps with transitions from `ClusterState::diff` naming each member's incarnation.
  Everything but the timeline is rendered from the latest snapshot; the timeline is built only
  from the streams, so a restart, a lost history or a gap resets the diff baseline (an equal
  recovery state re-arms it, an older one is ignored) rather than inventing transitions. Depends
  on tellus, the inspect crate and the demo's library for the wire types; a default workspace
  member gated by `just all`, run with `just run-cluster-demo-tui`.
- `tellus/src/testing.rs` (feature `test-util`): `TestContext`, a real `ActorContext` plus the mailbox behind it, for calling `Actor` and `EventSourced` methods directly in unit tests; its `terminate` is the production `actor_context::terminate` run with `()` as the actor value, so the descendant barrier is not duplicated. `TestContext::new` is the context of an `Untimed` actor and `TestContext::timed` that of a `Timed` one; only the latter has `timers`, listing the pending ones as `PendingTimer` values, and `fire_timer`, which fires one as the run loop would, whatever its delay, since no timer fires by itself. With `persistence` it also holds `settle`, the store-free part of `spawn.rs`'s settlement (`apply_events`, then the `then` continuations), and `Effect::events` / `Effect::stop_requested` are gated on `test-util`. `docs/actors.md` and `docs/persistence.md` show the two doctests of this module as their Testing examples; keep them identical.
- `tellus-comparison/`: competitive benchmarks with strict fairness rules (unbounded mailboxes
  everywhere, fire-and-forget sends only, identical timing boundaries); read its README before
  changing any benchmark.
- `docs/actors.md`: the authoritative top-down explanation of the core, from the `Actor` trait to
  the run loop, with links into the implementation. Read it before changing `tellus/src`; keep it
  consistent with implementation changes.
- `docs/persistence.md`: the equally authoritative contract of event-sourced persistence (feature
  `persistence`, `tellus/src/persistence/`): replay equals live execution, effects gated on
  durability, strict per-command settlement, fencing via conditional append, schema evolution via
  manifest plus schema version. `tellus/tests/persistence.rs` encodes these guarantees.
- `docs/cluster.md`: the same for the feature-gated `cluster` module, in particular which core
  guarantees carry over the network and which weaken; keep it consistent with `tellus/src/cluster`.
- `mentor/`: generated code-review artifacts, not source code.

## Architecture

The core is small (~2300 lines in `tellus/src`, plus ~2600 for persistence) but dense with
cross-file invariants:

- An actor is a state machine: `Actor::receive` (`actor.rs`) is a synchronous function from owned
  state and an `Incoming` (message or terminated signal) to `Control::Continue(next_state)` or
  `Control::Stop`. No async, no `&mut self`; each actor runs as one Tokio task.
- `actor_context.rs` holds the run loop, spawning and the termination sequence. Actors form a
  tree; termination is bottom-up, with each actor's Tokio watch channel closing (all child
  receivers dropped) as the barrier proving every descendant has terminated. The state is dropped
  before the children are stopped; only the actor value waits for the barrier, and the watchers
  are signaled last: a terminated signal must prove the actor's destructors have run. The run loop
  also owns the timers: it polls the one sleep of the table in `timers.rs`, which `start_timer`
  creates and moves only for an earlier deadline. A due timer never enters the mailbox: the table
  records it with a position, the items received so far plus the mailbox's length when it fired,
  and the run loop takes it, one per iteration and stale ones dropped, once it has received that
  many items. Which loop an actor runs is its `Timers` type's choice (`Actor::Timers` and
  `EventSourced::Timers`, `Untimed` or `Timed`), made by static dispatch through the sealed
  `TimerMode` before `task::spawn`; the untimed loop neither polls a sleep nor counts items, and
  neither loop checks the mode. `start_timer` and `cancel_timer` exist only on
  `ActorContext<M, Timed>`.
- `mailbox.rs` is where messaging and death watch meet: one FIFO flume channel per actor, always
  unbounded underneath, with a bounded capacity enforced by a reservation counter (`quota.rs`) in
  front. Terminated signals bypass the capacity check but ride the same FIFO channel as ordinary
  messages; that shared queue is the entire mechanism behind the ordering guarantee (a terminated
  signal arrives behind all messages the terminated actor delivered to the watcher). Death watch
  itself lives in `watch.rs` (`Watcher`, `WatcherRegistry` and the `TerminatedHandler` a watcher
  runs); the mailbox half holds the registry and implements that trait for its own sender, and
  registration closes atomically with termination, so watch is race-free.
- `unwatch` is enforced on the watcher's side: the run loop drops a terminated signal whose sender
  is no longer watched before `receive` ever sees it.
- Supervision (`actor_config.rs`): errors and panics (both `init` and `receive` run under
  `catch_unwind`) are handled identically; `Restart` rebuilds only the state via `init` on the
  same actor value, retains the mailbox, cancels the timers, and backs off exponentially between
  consecutive restarts.
- `actor_system.rs`: `spawn_root` spawns the root through the same `spawn` path as any child and
  registers a watcher directly in the root's watcher registry; the watcher's handler resolves
  `terminated()` and owns the sender keeping the root running.
- Clustered remoting (`cluster/`, `cluster` feature) hangs off one place: `ActorRef` holds a `Sink`
  enum whose second variant is a remote sink, so `tell`, `watch` and `unwatch` stay one API. The
  module's public shape is flat for what every node calls (`start_endpoint`, `form`, `join`,
  `leave`, `members`, `down`, `register`, `bootstrap`, `serialize_ref`, `deserialize_ref`, and for
  observing the node `lifecycle`, `cluster_state` and `changes`, defined in `state.rs` with
  `ClusterState`) plus one submodule per pluggable family, each holding a trait, its factory and the
  provided implementations: `cluster::transport` (with QUIC in `transport/quic.rs`),
  `cluster::codec`, `cluster::downing`, `cluster::failure` and `cluster::formation`, and one each
  for the directory and the fan-out over it: `cluster::receptionist` (`lookup`, `lookup_at`,
  `subscribe`, `settled`) and `cluster::pubsub` (`Topic`, `subscribe`, `publish`, which tells the
  local subscribers the receptionist resolves and serializes once for the remote ones, one
  `Multicast` frame per node and data stream). `ActorRef` is serializable via a process-wide
  endpoint singleton (`endpoint.rs`) plus a lazy registry (`registry.rs`) evicting through the
  core's watcher mechanism. All frames towards a node ride one lane, split into a control queue and
  a bounded pool of data queues over the transport's streams (`max_streams_per_peer`), each frame
  picking its queue by hashing the actor it is delivered to, so FIFO holds per recipient; system
  frames (watch, terminated, gossip) bypass the outbound capacity but not the order, mirroring
  `mailbox.rs`. A terminated signal names its watcher, which is what puts it on the same queue as
  the messages it must stay behind. Request-response crosses nodes via a nonce-keyed pending-replies
  table (`reply.rs`): serializing a `ReplyTo` moves its sink into the table, the far side gets a
  proxy whose `Reply` frame names the actor it is delivered to and hence rides that actor's queue,
  keying on its nonce when no actor awaits it (a dropped proxy sends `ReplyDropped` instead);
  downing a member evicts the entries stamped with it, and a terminating recipient the entries
  awaiting it. Membership (`membership.rs`) is always on: a started endpoint is *not* a cluster and
  refuses joins (`RefusalReason::NoCluster`), `form` (`endpoint.rs`) makes it a cluster of one and
  `join` makes it a member through a seed's member snapshot, applied as one snapshot once its last
  chunk arrived; joins are exclusive per endpoint and pin the cluster which answered, a join failing
  after its `Join` handshake short of a definitive answer is recorded as ambiguous and refuses
  `form` (`FormError::JoinAmbiguous`) until that address answers, leaves the universe or
  `ambiguous_join_bound` passes, and `bootstrap` asks a `FormationProvider` (`formation.rs`, default
  `Majority`: the lowest address of the discovered universe, once a majority of it is known to be no
  cluster; the opt-in `MajorityWithFallback` skips a lowest address unavailable for its `after`; a
  provider answers a `Decision` with a `Declined` reason) when no address of a settled view is a
  member of anything, which is what stops a downed minority from re-forming inside a partition,
  `leave` (`leave.rs`) announces a departure by moving this node's own entry to Left, pushing it to
  every Up member, draining every lane within one `leave_timeout` deadline (the lane finishes its
  data streams, sends a `Drained` frame naming their number as its last control frame and counts as
  acknowledged only once the peer answers with `DrainAck`), severing and returning a `Departure`
  report (`leave_on_terminated` folds that into the `ActorSystem::terminated` wait, so the departure
  is queued behind the tree's terminated signals), while a member learning the Left, from the leaver
  or relayed, defers the node death until its inbound connection from the leaver has closed and
  every reader of it has finished, or its own `leave_timeout` passed (`Departures` in `endpoint.rs`;
  `read_streams` in `peer.rs` answers `Drained` once that many data streams have ended cleanly; a
  downing ends the wait), the member list (a per-incarnation `Up < Left < Down` lattice, so gossip
  merge is a convergent CRDT union) rides `Gossip` frames which double as heartbeats (empty towards
  a peer already holding the current list version, full on a change, a reconnect and every 10th
  tick), non-members are refused at the handshake (retryably while gossip converges), and a detailed
  Left or Down entry is swept after `down_retention` while its address keeps a capped incarnation
  watermark with its state, which rides connection setup (chunked into frames within
  `max_frame_size`) rather than the heartbeat, so gossip stays the size of the cluster. Every
  cluster has an ID (`ClusterId`, minted by `form`, committed by a join only once the seed's
  snapshot is complete, provisional with the pin until then); every handshake carries it and a peer
  of another cluster is refused as `ForeignCluster`. A member holds back joins from new addresses
  (`RefusalReason::JoinsHeld`, retryable) while an Up member is outside its reachability component;
  a join superseding an Up incarnation at its own address is still admitted. An address no Up member
  advertises which fails a check retrying cannot change is refused for a backoff from 10 s doubling
  to 10 min, lifted by an inbound handshake, an admitted join or a gossiped Up member there. Failure
  detection (`failure.rs`, default phi accrual) marks members locally unreachable per tick of
  `membership_loop` in `endpoint.rs`; the pluggable `DowningProvider` (`downing.rs`, default
  `KeepMajority`, which self-downs the minority side of a partition; `DownAfterDeadline` is the
  unilateral one the tests and only they use) decides node death, whose sequence (mark Down, close
  lane, quiesce the delivery gate, fail asks, flush synthesized signals) makes the weaker watch
  contract true by construction. No provider downs either end of a broken link, a direct edge
  reported unreachable between two members inside the component (`reachability.rs`);
  `ClusterState::broken_links` lists it and the `member directly unreachable` warning repeats every
  minute, naming `cluster::down`. Remote watch is two tier: a real termination keeps the local
  guarantees, a synthesized signal only promises that nothing from that node is delivered again.
  Under mutual TLS the admission (`peer.rs`) binds a peer's advertised address to its certificate's
  `tellus://<ip:port>` URI SANs, or without one its advertised IP to the IP SANs, both directions.
  Discovery (`discovery.rs`) is `register`, which names a local actor in a second registry keyspace
  evicted with the route; the receptionist replicates those registrations and
  `receptionist::lookup_at` answers a name at an Up member's address from them, empty for any other
  address. A key names its message type by the application's `Manifest` (`manifest.rs`), never by
  `std::any::type_name`, and its kind keeps topics apart from discovery keys. Bootstrap
  (`bootstrap.rs`) turns seeds into discovery: `cluster::bootstrap` (a thin wrapper around the
  unit-testable `run`) polls a `SeedDiscovery` and joins through every resolved address but its own,
  lowest first (a connected join graph), on every resolution; only the formation decision waits
  until the resolved set has held `min_peers` addresses through the settle window. It returns once
  this node is a member, and `Downed` means the process must exit and restart. With a
  `probe_interval` it leaves a task behind which keeps the discovery and probes every resolved
  address that is neither this node nor an Up member with a `Probe` handshake; another cluster found
  that way, or refused as `ForeignCluster` at a handshake, is listed in
  `ClusterState::foreign_clusters` and logged at error level.

The wire protocol is versioned through the handshake envelope (`Frame` discriminant 0, magic,
oldest and newest version, frozen for every version), negotiated before the body is decoded
(`PROTOCOL_VERSIONS` in `cluster/frame.rs`). A new version adds handshake body fields only by
appending, and every new frame or field is gated on the negotiated version. Every wire change
updates the pinned-byte tests in `frame.rs`.

Changes to the run loop, mailbox or termination sequence almost always affect the ordering and
watch guarantees spelled out in `docs/actors.md`; the integration tests in `tellus/tests/` encode
those guarantees.
