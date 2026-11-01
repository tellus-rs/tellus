# tellus-comparison

Messaging benchmarks comparing [tellus](../tellus) against two actively maintained Rust actor
frameworks, [kameo](https://crates.io/crates/kameo) and [ractor](https://crates.io/crates/ractor).

This package is never published and is not part of `just all`, so its dependencies stay out of
tellus's own build and out of the CI of pull requests which do not touch it. A pull request which
does touch it is checked and linted.

## Running

```shell
just bench-comparison           # run the benchmarks
just bench-comparison-report    # render the HTML report from the results
just check-comparison     # cargo check
just lint-comparison      # clippy
```

Results are written to `target/criterion-comparison`, deliberately separate from the
`target/criterion` tree used by tellus's own regression benchmarks, so the two never mix. The
report goes to `target/comparison-report` as `index.html` and `results.json`, tagged `local`
unless `just bench-comparison-report <tag>` names another tag.

Runs published by CI are collected on the
[comparison dashboard](https://tellus.rs/tellus/comparison/), one directory per version
tag and per manual run; [`latest`](https://tellus.rs/tellus/comparison/latest/) holds a copy of
the run for the most recent version tag. The pages take their look from `dashboards.css` on
tellus.rs. To re-render a published run from its `results.json`, for example after changing the
page layout, and to regenerate the overview page:

```shell
cargo run -p tellus-comparison --bin report -- --from-json <run>/results.json --output <run>
cargo run -p tellus-comparison --bin report -- --index <comparison>
```

## Benchmarks

Three workloads, modeled on [`tellus/benches/messaging.rs`](../tellus/benches/messaging.rs):

- `flood`: the bench thread floods a single counting actor with 100,000 messages.
- `ping_pong`: pairs of actors play ping-pong for 1,000 rounds, with one pair and eight pairs.
- `fan_out`: the bench thread sends 100,000 messages round-robin to eight and to 32 workers.

Actor counts are fixed, so benchmark ids stay stable across machines and published results remain
comparable. Unlike the tellus bench, `fan_out` has the bench thread rather than a root actor
distribute the messages.

## Why these two competitors

Both are actively released and both run on a plain Tokio runtime like tellus, so all three are
measured on the same executor. `actix` was considered and rejected despite being
far more popular: it uses its own `System`/`Arbiter` and places actors on a single-threaded
arbiter by default, so a fair comparison would require spreading actors across arbiters and would
still be architecturally apples-to-oranges. `xtra` and `coerce` are dormant.

## Fairness rules

The point of these benchmarks is that all three frameworks perform the *same work*:

1. **One run, one machine, back-to-back.** Numbers are only ever compared within a single run.
   Never compare figures across runs or machines.
2. **Unbounded mailboxes everywhere.** tellus defaults to unbounded, ractor is unbounded, and
   kameo is explicitly spawned via `spawn_with_mailbox(.., mailbox::unbounded())` because its
   default is a *bounded* mailbox of capacity 64, which would otherwise apply backpressure the
   others do not.
3. **Non-blocking fire-and-forget sends only.** tellus `ActorRef::tell`, kameo
   `tell(..).try_send()`, ractor `ActorRef::send_message`. No awaited sends (that is backpressure,
   a different guarantee) and no request-response calls.
4. **Identical timing boundaries.** Every framework goes through the same `measure` helper:
   spawning happens outside the measured region, and the timer covers sending plus awaiting
   termination. In `ping_pong` that includes both actors of a pair: tellus tears the ponger down
   as a child of the pinger, kameo and ractor stop and await it in the pinger's stop hook.
5. **Identical runtime**: one multi-threaded Tokio runtime, same configuration for all.
6. **Competitors get their fastest configuration** (see below).

Termination is also the correctness check: each actor only stops once it has processed exactly its
expected number of messages, so a dropped or lost message makes a benchmark hang rather than
finish early.

## Competitors are configured for speed, not defaults

Both competitors ship per-message instrumentation enabled by default, which tellus has no
equivalent of (tellus depends on `tracing` too, but emits nothing per message). Benchmarking them
as-shipped would charge them for an observability feature while measuring tellus without one, so
both are built with those features off:

- `kameo`: `default-features = false`, dropping `tracing` (and `macros`, which has no runtime
  cost). Measured **12.5% faster** than with defaults on `flood`.
- `ractor`: `default-features = false, features = ["tokio_runtime"]`, dropping
  `message_span_propogation`. Measured **21% faster** than with defaults on `flood`.

This deliberately biases the setup *in the competitors' favor*, which is the appropriate
direction for a comparison published by tellus's own maintainer. Anyone reproducing the
out-of-the-box experience should expect both to be correspondingly slower.

## Caveats

Read these before drawing conclusions from any number. The published report repeats this list,
folding the speed configuration above into it; keep both in sync with
[`src/bin/report.rs`](src/bin/report.rs).

1. **tellus's `receive` is synchronous; kameo's and ractor's handlers are `async fn`.** tellus
   therefore avoids allocating and polling a future per message, but in exchange it cannot await
   inside `receive`. This is a capability difference, not only a speed difference, and it favors
   tellus on exactly these microbenchmarks.
2. **tellus's mailbox is statically typed; the others erase message types.** kameo and ractor box
   messages to support their richer messaging APIs, which costs an allocation and a dynamic
   dispatch per message that tellus does not pay.
3. **These are messaging microbenchmarks only.** They say nothing about supervision, distribution,
   ergonomics, memory use or production readiness. kameo and ractor are mature, feature-rich
   frameworks; tellus is under active development and does far less.
4. **CI numbers come from a shared GitHub hosted runner**, so absolute figures there are not
   representative of real deployments; only the relative comparison within a run is meaningful.
5. **Written and run by tellus's maintainer.** The methodology and every line of the benchmark are
   in this package; corrections and pull requests are welcome.
