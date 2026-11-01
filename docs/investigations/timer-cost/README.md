# Timer cost of #174

PR #174 adds timers to actors. Measured against its parent, it makes actors slower that never start
a timer. This page is the starting point for finding and fixing that cost: what the PR changes,
what is measured, what is established and what is not, and how to measure the next step. It
assumes [AGENTS.md](../../../AGENTS.md) and the
[actor ID alignment investigation](../actor-id-alignment/README.md), whose protocol and tools it
reuses.

## Status

#174 was closed unmerged on 2026-10-27. Its head is `283e5e7`, one commit on top of `main` at
`39aec83`. It could only be merged once actors without timers were no measurably slower, i.e. every
messaging row passed the protocol below. The results are posted on #174; the tracking issue #178 is
closed in favor of the PR. Timers shipped with #185 (`8252b06`), opt-in per actor type, and the
variants are kept as patches in [`patches/`](patches) (#190).

A diagnostic with the mailbox item `Queued<M>` and its gate alone reproduces most of the cost. That
isolates a configuration, not an instruction-level cause. Two designs which keep timer items out of
the mailbox are built and measured: the depth cursor ([`cursor.patch`](patches/cursor.patch), built
as `de24ce8`) and the same with its timed loop out of line
([`cursor-out-of-line.patch`](patches/cursor-out-of-line.patch), `7fe7858`). The cursor is the
working design; the out of line variant is an experiment. Neither passes every row. On x86_64 the
cursor fails five rows at +3.1 to +3.8%, passes two and leaves three inconclusive. On Neoverse-N2 it
passes nine rows and leaves `ping_pong/pairs/1` inconclusive. With its timed loop removed, the
cursor passes those rows. One validated code placement moved no row by more than 2.5%. Which part of
the timed path costs is not established, see next steps below. The cursor with an inlined recv
future ([`cursor-inline-recv.patch`](patches/cursor-inline-recv.patch), `b30e728`) runs the `queued`
and `flood` rows 4 to 7% faster than `main` on both architectures and every timer row 7 to 13%
faster than the cursor. It fails `fan_out/workers/16` on Neoverse-N2 at +10.7% and leaves four
x86_64 rows inconclusive. Varying the message count shows that cost to be per message, not per
actor, and the Xeon 6973P-C shows it as well. Receive counts on the N2 show the variant's `/16`
workers waiting for a message more often than `main`'s. Producer bursts cut that waiting by two
thirds without removing the penalty and by a factor of ten with removing it; the variant's excess
waiting over `main` stays at bursts 1 and 4. Sampling profiles of the plain binaries locate the
added CPU in user space fast paths: a 32 bit lock compare and swap (+56 to +127 ns per message) and
tokio's wake path (+14 to +95 ns), against savings in `poll_notified` and flume's send and receive;
the mutex slow path and the kernel stay small. Call chains attribute the compare and swap to two
lock words, the mailbox's channel mutex in flume's `send` and `recv` (+16 to +68 ns) and the stop
channel's `Notify` mutex in `poll_notified` (+35 to +44 ns). The stop channel's path costs the same
in both binaries in total; the added time is in flume's channel path (+27 to +28 ns at bursts 1 and
4, +2 at burst 16) and in the wake and scheduling paths (+9 to +35 ns). The cursor with a single
fixed-shape select ([`cursor-single-select.patch`](patches/cursor-single-select.patch), `ad83454`),
the shape proposed on #174, restores `main`'s inlining of the recv future, passes `ping_pong` on the
N2 and fails five x86_64 rows at +7.5 to +13.6%, with four more inconclusive. `main` plus the
inlined recv future alone ([`inline-recv.patch`](patches/inline-recv.patch), `48ac3d0`) reproduces
the N2 `fan_out/workers/16` failure at +9.0% without any timer code and is faster than `main` on
every other row, so that tradeoff belongs to the receive optimization, not to the timers. #174's own
candidate ([`inline-gate.patch`](patches/inline-gate.patch), `a8ab639`, the gate inlined over the
`Queued` item, swept by the contributor) fails two x86_64 rows at +5.6 and +5.7% with five more
borderline. The cursor with the timer poll folded into the mailbox's waiting future
([`cursor-combined.patch`](patches/cursor-combined.patch), `f55717e`) fails eight x86_64 rows at
+7.4 to +17.4% and `flood/bounded` on the N2 at +5.0%, the worst result of any variant. Every loop
variant under the universal timer API failed the x86_64 throughput rows, so timers became an opt-in
per actor type (#185, swept as `077c74a`): the untimed loop is `main`'s per function on the message
path, and the sweep passes eighteen rows with none failing or borderline; N2 `flood/bounded` is
inconclusive by too few jobs passing A/A and x86_64 `flood/unbounded` by spread, both with the
candidate within ±2.3% in every job. That is the first variant without a failing row. A confirmation
run (40499387401) under a protocol extension recorded before it passes the two rows and leaves
x86_64 `flood/bounded` undecided by two A/A failures, where the first run passed it, so the
candidate is accepted under that extension: every row passes in at least one of the two whole
sweeps, none fails or is borderline in either. PR #185 carries the implementation. Its review fix
(`1800f0c`) touches only the timed path. Two whole messaging sweeps of it pass every row but N2
`flood/bounded`, which is inconclusive in both by too few jobs passing A/A. The maintainer accepts
the fix on the messaging bench as an exception, with the reason recorded below. Two whole
persistence sweeps accept it under the confirmation rule.

## What #174 changes on every actor's path

All of this applies whether or not an actor starts a timer:

- **The mailbox item.** The channel carries `Queued<M>` instead of `Incoming<M>` (`mailbox.rs`):
  `Queued::Incoming(Incoming<M>)` or `Queued::Timer(TimerId)`. `TimerId` is a slot and a generation
  stored as `[u8; 4]` and `[u8; 8]`, so it has alignment 1 and `Queued<M>` stays as large as
  `Incoming<M>`. The test `a_queued_item_is_no_larger_than_an_incoming` pins that. Every send
  wraps the incoming in `Queued::Incoming`, and `Mailbox::recv` returns `Queued<M>`.
- **The gate.** After receiving, the run loop calls `ActorContext::gate`, which unwraps
  `Queued::Incoming` and resolves a `Queued::Timer` against the timer table.
- **The select.** The run loop's `tokio::select!` (biased) gets a branch for the timer sleep
  between the stop branch and the mailbox, guarded by `if armed`. `armed` is read through
  `ActorContext::timer_armed` before every select.
- **The context.** `ActorContext` holds `timers: RefCell<Option<Box<Timers<M>>>>`, allocated on the
  first `start_timer`.
- **Termination** drops pending timer messages after the drained mailbox.

The semantics the fix must keep: a canceled or replaced timer is never received, even when its
item is already queued; a timer item rides the same FIFO as messages and terminated signals; due
timers are enqueued behind the items already queued. `docs/actors.md` on #174 and
`tellus/tests/timers.rs` spell these out.

## What is measured

The `Stack sweep` workflow ([report](../actor-id-alignment/report.md#8-the-protocol)) with base
`39aec83` and candidate `283e5e7`, runs 40118974183 and 40122831636. None of the ten messaging rows
starts a timer. Medians across 16 worker-stack positions; positive means slower.

| Row | Neoverse-N2 | x86_64 |
|---|---|---|
| `queued/never/4`, `/16` | fail, +7.3%, +8.0% | inconclusive (spread) |
| `queued/every/4`, `/16` | fail, +7.5%, +7.3% | inconclusive (spread) |
| `flood/unbounded`, `/bounded` | fail, +8.9%, +8.2% | inconclusive (too few jobs) |
| `fan_out/workers/4` | fail, +3.9% (8 jobs) | inconclusive (spread) |
| `fan_out/workers/16` | pass, -7.1% | inconclusive (spread) |
| `ping_pong/pairs/1`, `/4` | fail, +3.4%, +3.6% (8 jobs) | fail, +3.5%, +4.0% (8 jobs) |

By CPU model on x86_64, over all 8 jobs and not a verdict: EPYC 7763 (6 jobs) `queued` +12.6 to
+16.2%, `fan_out/workers/4` +21.2%, `flood` +10.4 to +15.5% (3 jobs); EPYC 9V45 and Xeon 8573C one
job each, lower. On x86_64 every job is slower on every row but `fan_out/workers/16`, where the
Xeon job was 19.7% faster.

## What is established and what is not

Established:

- The cost exists without timers: the measured actors never start one, and the result holds across
  all 16 positions, all jobs and both runner types, with A/A controls mostly within ±1%.
- The first comparison on #174 (+55% on `queued`) was a stack layout effect, traced to an unaligned
  copy in `Mailbox::recv` (see the alignment investigation). It said nothing about the timers.
- The item diagnostic reproduces most of the cost (see attribution below). The select diagnostic
  costs up to about 3% on x86_64 and the field diagnostic about 1%. These results hold for the
  diagnostic configurations; none of them names an instruction.
- With the depth cursor, every `queued`, `flood` and `fan_out` row passes on Neoverse-N2.

Not established:

- **Which part of the timed path the cursor pays for.** Removing the timed loop clears every row
  the cursor fails, `ping_pong` included. That implicates the timed path's changes together: the
  second select, the out of line recv future, the larger run loop and its future.
- **How much of the remaining x86_64 cost is code placement.** Moving the timed loop out of line
  takes the `queued` rows from +3.1 to +3.8% down to +1.1 to +2.1%. That variant also changes
  function boundaries, inlining and the run loop's future, so it does not isolate placement. One
  validated placement of `main`'s code moved no row by more than 2.5%; other placements are
  untested.

## Generated code

`-Zprint-type-sizes` and the disassembly of the messaging bench (bench profile, rustc 1.99.0), for
`main` at `39aec83` and #174 at `283e5e7`. The bench messages are zero-sized, so an item is a tag
plus an `ActorId`.

- `Queued<Tick>` is 17 bytes like `Incoming<Tick>`. The timer variant takes a third value of
  `Incoming`'s tag byte and puts `TimerId` right behind it.
- `ActorContext<Tick>` grows from 144 to 160 bytes, and the run loop's future by 48 bytes.
- `Mailbox::recv` executes the same instructions; only the tag constants differ, on both
  architectures.
- flume's send and receive paths keep their sizes. A few branches are ordered differently: 8 and 16
  lines on aarch64, 9 and 14 on x86_64.
- `RecvFut::poll` orders its blocks differently and copies the received item with one 16-byte and
  one byte move.
- The `Countdown` run loop grows from 1092 to 1144 instructions on aarch64 and from 1045 to 1125 on
  x86_64. Its select function grows from 340 to 552 bytes on aarch64.

None of these is a hot loop doing visibly more work. The cost shows up only in the measurements.

## Attribution

Each diagnostic is one commit on top of `39aec83`. Before sweeping, each was checked against #174's
disassembly on both architectures:

- **Item** (`f75495e`): `Queued<M>` in the channel and the gate, no table, no select branch, no
  field. Its channel functions match #174's structurally on both architectures.
- **Select** (`c5393b0`): the third select branch over a real boxed `Sleep`, guarded by
  `hint::black_box(false)`, no table, no field. Its select function differs from #174's only in
  reaching the sleep from a local instead of through the context.
- **Field** (`07258b7`): #174's `timers` field and its `armed` read through `hint::black_box`, with
  the read at the same offsets as in #174.

The results hold for these variants, not for #174's parts as such. Workflow runs 40206545756,
40206552920 and 40206559819; medians across 16 positions, verdicts by `report.py`.

| Row | Item N2 | Select N2 | Field N2 | Item x86 | Select x86 | Field x86 |
|---|---|---|---|---|---|---|
| `queued/never/4` | borderline, +4.3% | pass, -2.4% | pass, -0.1% | spread, +14.9% | pass, +1.9% | pass, +0.5% |
| `queued/never/16` | fail, +5.6% | pass, -1.1% | pass, -0.6% | spread, +15.2% | borderline, +3.4% | pass, +1.1% |
| `queued/every/4` | borderline, +4.8% | pass, -1.7% | pass, +1.1% | spread, +11.9% | pass, +2.0% | pass, +0.6% |
| `queued/every/16` | fail, +5.3% | pass, -0.0% | pass, +1.5% | spread, +12.0% | pass, +2.6% | pass, +0.2% |
| `flood/unbounded` | fail, +6.7% | pass, +1.8% | pass, -0.1% | fail, +10.3% | pass, +0.5% | pass, +1.2% |
| `flood/bounded` | spread, +4.2% | pass, +0.5% | pass, -0.3% | spread, +12.7% | pass, -0.1% | too few jobs |
| `fan_out/workers/4` | pass, +2.1% | pass, +1.9% | pass, +0.4% | spread, +17.1% | pass, +0.7% | pass, +1.0% |
| `fan_out/workers/16` | pass, -8.8% | pass, -0.3% | pass, +2.0% | spread, +11.0% | pass, +0.9% | pass, +0.6% |
| `ping_pong/pairs/1` | pass, +2.9% | pass, +1.3% | pass, +1.0% | pass, +2.9% | spread, +4.8% | too few jobs |
| `ping_pong/pairs/4` | borderline, +3.0% | pass, +1.5% | pass, +0.4% | borderline, +3.2% | pass, +2.0% | pass, +1.2% |

The item alone reproduces most of #174's cost on both runner types, e.g. `queued` +4.3 to +5.6% on
Neoverse-N2 against #174's +7.3 to +8.0%, and +12 to +15% on EPYC 7763 jobs against #174's +12.6 to
+16.2%.

## The depth cursor

The cursor ([`cursor.patch`](patches/cursor.patch) on #174's head `283e5e7`, built as `de24ce8`, one
commit on `39aec83`) keeps #174's API, timer table, tests and documentation, but a due timer never
enters the mailbox. When the sleep elapses, the run loop records the timer's position: the number of
items received so far plus the mailbox's length. Once it has received that many items, it takes the
timer instead of the next item, checking the generation as #174's gate does, one fired timer per
iteration. The channel carries `Incoming<M>` as on `main`. An actor without a timer table runs
`main`'s loop unchanged; the loop checks `has_timers` once per message to switch.

The channel functions are structurally identical to `main`'s on aarch64. `just all` and the
allocation gate pass. All of #174's timer tests pass unchanged, and two tests are new: a timer
started in `recovered`, and a unit test that one call takes one fired timer, even when several stale
ones are deliverable. The fairness of skipping stale timers is checked by the scheduler-sensitive,
`#[ignore]`d test `stale_timers_do_not_delay_a_stop`: it passed 50 of 50 runs on the cursor and
failed 50 of 50 on a mutant draining all stale timers at once.

Runs 40208098132 and 40210705002 (the second decides the borderline rows):

| Row | Cursor N2 | Cursor x86 |
|---|---|---|
| `queued/never/4` | pass, +1.2% | fail, +3.5% |
| `queued/never/16` | pass, +1.3% | fail, +3.8% |
| `queued/every/4` | pass, +0.2% | fail, +3.1% |
| `queued/every/16` | pass, -0.1% | pass, +2.9% |
| `flood/unbounded` | pass, +0.7% | too few jobs |
| `flood/bounded` | pass, +0.4% | spread, +5.6% |
| `fan_out/workers/4` | pass, +0.2% | fail, +3.7% |
| `fan_out/workers/16` | pass, +1.7% | pass, +2.2% |
| `ping_pong/pairs/1` | spread, +4.4% | spread, +3.2% |
| `ping_pong/pairs/4` | pass, +2.8% | fail, +3.6% |

On x86_64 that is five failures, two passes and three inconclusive rows.

The untimed select of `Countdown` is structurally identical to `main`'s on both architectures. That
does not hold for the actors of `ping_pong`, see the next section. The run loop holds both loops:
1328 instructions for `Countdown` on x86_64 against 1045 on `main`.
Marking the timed branch with `hint::cold_path` changed six instructions and was not swept.

## The timed loop out of line

The out of line variant ([`cursor-out-of-line.patch`](patches/cursor-out-of-line.patch) on the
cursor, built as `7fe7858`) moves the timed loop into an `async fn`, polled through a wrapper whose
`poll` is `#[inline(never)]`. The `Countdown` run loop shrinks to 1091 instructions on x86_64. Its
tests and clippy pass. Run 40211281691:

| Row | Out of line N2 | Out of line x86 |
|---|---|---|
| `queued/never/4` | pass, +0.4% | pass, +2.1% |
| `queued/never/16` | pass, +0.7% | pass, +2.0% |
| `queued/every/4` | pass, -0.2% | pass, +1.2% |
| `queued/every/16` | pass, +0.9% | pass, +1.1% |
| `flood/unbounded` | pass, +0.6% | pass, +0.2% |
| `flood/bounded` | spread, -0.3% | borderline, +4.9% |
| `fan_out/workers/4` | pass, -1.5% | pass, +3.0% |
| `fan_out/workers/16` | pass, +0.0% | pass, +1.5% |
| `ping_pong/pairs/1` | spread, +4.5% | fail, +5.5% |
| `ping_pong/pairs/4` | borderline, +3.3% | borderline, +3.6% |

Actors with timers pay for it. Local aarch64 sweep over 8 positions, medians, against the cursor;
the cursor itself against #174 for comparison:

| Row | Out of line against cursor | Cursor against #174 |
|---|---|---|
| `timers/none/4` | +0.3% | -10.5% |
| `timers/armed_idle/4` | +18.2% | -10.9% |
| `timers/restart_each/4` | +4.4% | -4.9% |

A/A controls were within ±1%.

## The `ping_pong` path

A comparison of every function of the bench binary, `main` against the cursor, both architectures,
with immediates and symbol names stripped. The functions on the per-message path of `Pinger` and
`Ponger` are the run loops, their selects, `Mailbox::recv`, flume's `send`, `RecvFut::poll` and
`Shared::recv`.

- `RecvFut::poll` and `Shared::recv` are identical on both architectures.
- flume's `send` is identical on aarch64. On x86_64 its register allocation differs and it has 9
  more instructions, for every message type. The field diagnostic compiles `send` to exactly the
  cursor's code and passed x86_64 at about +1%, and the out of line variant has `main`'s `send` and
  fails `ping_pong`. So this difference alone does not explain the result.
- `Mailbox::recv`'s future is inlined into the select on `main`, for `Ping` and `Pong`, which only
  one actor each receives. In the cursor every actor has two selects, so the future has two callers
  and stays out of line, on both architectures: one more call per message. `Tick`, which several
  bench actors receive, has an out of line future on `main` as well, so the throughput rows do not
  see this change.

| Binary | `Ping` and `Pong` recv future |
|---|---|
| `main`, #174, item, select and field diagnostics | inlined |
| cursor, out of line variant | out of line |

The two designs which move it out of line are the two whose `ping_pong` rows remain unexplained.
That is a correlation. `#[inline(always)]` on the `async fn` does not reach the future's `poll`. A
hand-written future whose `poll` is `#[inline(always)]` is inlined everywhere, `Tick` included,
which makes it a different variant from `main`, not a restoration. A local aarch64 sweep over 16
positions and three repetitions, with the plain cursor in the slot of the A/A control (so this run
has none):

| Row | Cursor with the hand-written future | Cursor |
|---|---|---|
| `ping_pong/pairs/1` | -4.0% | +1.9% |
| `ping_pong/pairs/4` | -1.7% | +1.3% |

This local machine showed the cursor's `ping_pong` cost only weakly before (+1.8% against an A/A of
-0.9%), so the result is a lead for a sweep, not a finding.

## The cursor without its timed loop

The `diag/*` branches named from here on have been deleted, and with them the workflows
`diag-receive.yml` and `diag-burst.yml`, which were on two of them. Their names and commit IDs
identify the builds measured; only the variants in [`patches/`](patches) are kept.

`ff5950f` (one commit on `39aec83`, was on `diag/timer-switch-off`) is the cursor with both run
loops reduced to `main`'s `next_incoming!`. The timer table, the context field and the restart and
termination paths stay. A first attempt only made `has_timers` return `false`. That removed nothing:
the timed select's await points stay reachable through the future's resume switch, which dispatches
on a state stored in memory. Checked before sweeping, both architectures:

- No timed select is left. The selects are `main`'s sizes and the `Ping` and `Pong` recv futures
  are inlined as on `main`.
- The run loops are within 0 to 10 instructions of the field diagnostic's. Nine `Timers` symbols
  remain, on the setup and restart paths.
- flume's `send` on x86_64 is the cursor's and the field diagnostic's code.

So the diagnostic measures everything the timed path changes, taken together: the second select,
the out of line recv future, the larger run loop and its future. It does not single out one of
them. Run 40241036879:

| Row | Without timed loop N2 | Without timed loop x86 |
|---|---|---|
| `queued/never/4` | pass, -0.6% | pass, +1.1% |
| `queued/never/16` | pass, -1.2% | pass, +1.0% |
| `queued/every/4` | pass, +0.2% | pass, +0.7% |
| `queued/every/16` | pass, +0.6% | pass, +0.9% |
| `flood/unbounded` | pass, -0.5% | spread, -1.7% |
| `flood/bounded` | spread, -0.8% | too few jobs |
| `fan_out/workers/4` | pass, +0.9% | pass, +0.4% |
| `fan_out/workers/16` | borderline, +3.9% | pass, +0.5% |
| `ping_pong/pairs/1` | pass, +0.7% | pass, +1.3% |
| `ping_pong/pairs/4` | pass, +1.1% | pass, -0.7% |

Removing the timed path removes the cursor's five x86_64 failures and its `ping_pong` results on
both runner types. `fan_out/workers/16` on Neoverse-N2 is borderline, where the cursor passed at
+1.7%; it was not given a second run. The x86_64 jobs ran on three EPYC 7763 and one EPYC 9V45.

## Placement control

`d51c268` (one commit on `39aec83`, was on `diag/placement-control`) is `main` plus 1056 bytes
placed in front of `.text` at link time. The padding is an assembly file, passed to the bench link
alone by a build script (`cargo::rustc-link-arg-benches`), with a linker script that inserts its
section before `.text`. Two earlier attempts failed their checks:

- `global_asm!` in the bench crate was discarded by `--gc-sections`. It also changed the generated
  code anyway: `Probing::receive` was no longer inlined, and aarch64 reordered functions.
- Naming the section `.text.hot.*` puts it first with GNU ld, but lld ignores the prefix and
  placed it near the end.

The x86_64 Linux toolchain links with rust-lld by default since Rust 1.90, so the CI x86_64 runner
presumably does; that was not verified. The aarch64 runner links with GNU ld. This host links both
with GNU ld, so x86_64 was checked with both linkers. Checked on all three links:

- The padding survives linking, and the number of load segments is unchanged.
- Every hot function moves: the run loops, selects, `receive_incoming`, `tell`, flume's `send`,
  `RecvFut::poll` and `Shared::recv`, and tokio's `run_task`, `wake_by_val` and `park_internal`.
  On x86_64 they move by 1056 or 1072 bytes, on aarch64 by 764 or 1088.
- Their instructions are identical with every immediate kept except address-dependent ones
  (RIP-relative displacements, branch targets, `adrp` pages and their low 12 bits). Stack offsets
  are kept, so the frame layouts are unchanged.
- On aarch64 no erratum 843419 stubs are added or removed. An earlier layout did add one, which
  replaced an instruction in `send` for `Tick` with a branch to a stub. GNU ld's address-dependent
  rewriting can therefore change executed instructions when code moves, the cursor's included.

Run 40241039674:

| Row | Placement N2 | Placement x86 |
|---|---|---|
| `queued/never/4` | pass, -2.5% | pass, -0.2% |
| `queued/never/16` | pass, -0.5% | pass, +0.3% |
| `queued/every/4` | pass, -0.3% | pass, -0.5% |
| `queued/every/16` | pass, -1.0% | pass, -0.1% |
| `flood/unbounded` | pass, -0.3% | spread, +0.7% |
| `flood/bounded` | pass, -0.3% | pass, +0.8% |
| `fan_out/workers/4` | pass, -0.3% | pass, +0.6% |
| `fan_out/workers/16` | pass, +0.9% | pass, +0.2% |
| `ping_pong/pairs/1` | pass, +0.4% | pass, +0.0% |
| `ping_pong/pairs/4` | pass, +0.1% | pass, +0.4% |

This one placement moves no row by more than 2.5%. That clears this placement only; it does not
rule out placement sensitivity in general. Its x86_64 jobs ran on one EPYC 7763, two Xeon 8573C and
one EPYC 9V45, while the cursor's failures were measured mostly on EPYC 7763.

## The cursor with an inlined recv future

The inlined recv variant ([`cursor-inline-recv.patch`](patches/cursor-inline-recv.patch) on the
cursor, built as `b30e728`) is the cursor with one change in `mailbox.rs`. `Mailbox::recv` returns a
hand-written future `Recv<'_, M>` which wraps flume's `RecvFut` and releases the quota for a message
in the same `poll` that sees it. Its `poll` is `#[inline(always)]`. Cancellation goes through
flume's `RecvFut`, as on `main`. With the `hotpath` feature, `recv` stays an `async fn` with the
measure attribute which awaits that future, so `just profile` reports `mailbox::recv` with 110000
calls, as on `main`; a measure on `poll` would count polls instead of receives. The bench binary is
byte-identical with and without that wrapper.

Generated code, both architectures:

- On `main` the recv future is out of line for `Go` and `Tick`, which several actors receive, and
  inlined for `Ping` and `Pong`. In the cursor all four are out of line. In this variant none is:
  the run loop calls flume's `poll_inner` directly, one call per receive as on `main`, where the
  cursor had two.
- The run loops grow by 3 to 7 instructions, stores when the select's futures are constructed.
- Outside the per-message path, the drop glue of `Mailbox`, `ActorRef` and flume's receiver changes
  size, and tokio's `spawn` is inlined differently. Both run at spawn or termination.

`just all` and `just profile-alloc-gate` pass.

Run 40285300081 against `39aec83`:

| Row | Inlined recv N2 | Inlined recv x86 |
|---|---|---|
| `queued/never/4` | pass, -4.7% | pass, -7.2% |
| `queued/never/16` | pass, -5.2% | pass, -6.2% |
| `queued/every/4` | pass, -4.7% | pass, -5.8% |
| `queued/every/16` | pass, -4.8% | pass, -5.4% |
| `flood/unbounded` | pass, -6.3% | spread, -4.6% |
| `flood/bounded` | pass, -4.5% | pass, -7.3% |
| `fan_out/workers/4` | pass, -1.7% | spread, -8.6% |
| `fan_out/workers/16` | fail, +10.7% | spread, -4.8% |
| `ping_pong/pairs/1` | pass, +0.4% | too few jobs, +3.0% and +0.8% |
| `ping_pong/pairs/4` | pass, -0.1% | pass, +0.0% |

The x86_64 jobs drew four CPU models: EPYC 7763, EPYC 9V45, EPYC 9V74 and Xeon 6973P-C; the last
two had not appeared before. `ping_pong/pairs/1` lost the 9V45 and 9V74 jobs to A/A failures (+4.1%
and +5.6%); the retained jobs measured +3.0% on the 7763 and +0.8% on the Xeon. The `fan_out` rows
span -18.7% (9V74) to +15.2% (Xeon, `/16`) per job with A/A within ±2.4%, so they are inconclusive
by spread between CPU models, not by noise within a job.

On Neoverse-N2 `fan_out/workers/16` fails with every job between +9.3% and +12.4% and every position
between +7% and +15%, with A/A within ±2.6%. The cursor passed that row at +1.7%, and the cursor
without its timed loop was borderline at +3.9%.

`measure` starts its clock after the system is created and stops it at the system's termination.
Every row's window therefore contains the root's termination, and `ping_pong`'s and `fan_out`'s
also contain the spawning of their children: `PingPongRoot` spawns the pingers in `receive` and
each pinger its ponger in `init`; `FanOutRoot` spawns its workers in `init`, which may run on
either side of the clock start. The functions this variant changes outside the per-message path,
tokio's `spawn` and the drop glue of `Mailbox`, `ActorRef` and `SelfRef`, run in those parts of the
window. A uniform per-actor cost does not fit the result: `ping_pong/pairs/4` spawns nine actors
per 8000 messages and passes at -0.1%, `fan_out/workers/16` seventeen per 100000 messages and
fails. A lifecycle cost specific to `fan_out`'s termination path is not excluded. What else singles
out `fan_out/workers/16` is seventeen actors receiving concurrently on four worker threads; that is
a distinguishing condition, not an explanation.

A local aarch64 sweep over 16 positions and three repetitions, `main` against this variant with an
A/A control, did not reproduce the failure: `fan_out/workers/16` +2.4% with A/A +0.3% (median of the
per-position ratios; per position -9.8% to +5.8%; the medians over all runs are 2.70 ms for `main`
and 2.73 ms for the variant, a different aggregate), `fan_out/workers/4` -4.2% with A/A +1.5%. This
machine is not a Neoverse-N2, so that leaves the N2 result unresolved rather than cleared.

Actors with timers gain. Local aarch64 sweep over 8 positions and three repetitions, medians,
against the plain cursor, with the cursor's binary as the A/A control:

| Row | Inlined recv against cursor | A/A |
|---|---|---|
| `timers/none/4` | -12.5% | +0.4% |
| `timers/none/16` | -12.9% | -0.3% |
| `timers/armed_idle/4` | -8.6% | -0.9% |
| `timers/armed_idle/16` | -6.9% | +0.4% |
| `timers/restart_each/4` | -7.7% | +0.1% |
| `timers/restart_each/16` | -7.8% | +0.3% |

Every position is below the cursor; the per-position range is -24.0% to -0.4%.

## `fan_out/workers/16` against the message count

Two sweeps vary `FAN_OUT_MESSAGES` at fixed worker counts. Each base is `39aec83` with the constant
changed (`756c86a` and `be7c1d9`, which were on `diag/fan-out-25k-base` and
`diag/fan-out-400k-base`), each candidate the full `b30e728` change on top of it (`c7ab1af` and
`68eec1b`, which were on `diag/fan-out-25k` and `diag/fan-out-400k`):
runs 40294926674 and 40294931397. On aarch64 the constant changes `messaging::main` by two or three
instructions and the erratum stubs; the run loops, the mailbox and flume are identical to the sweep
binaries. Every stub replaces an `adrp` in the drop glue of a mutex guard or in criterion's plotting
code: in the two bases the guard of `Option<Incoming<Go>>`, which is what shows as two or three
instructions in its drop glue; in the variant the guards of criterion's connection and of a
`PoisonError`. `Incoming<Go>` is the root's item, a `Go` and the workers' terminated signals per
iteration, so that work is fixed with respect to the message count and cannot grow with it.

Absolute time per iteration on Neoverse-N2, medians per job over 16 positions, with the variant's
added time against `main` and the A/A difference next to it:

| Messages | `/16` on `main` | `/16` added | `/16` A/A | `/4` added |
|---|---|---|---|---|
| 25000 | 5.0 to 5.8 ms | +0.50 to +0.71 ms (+9.0 to +11.7%) | -0.11 to +0.03 ms | -0.14 to +0.03 ms |
| 100000 | 20.0 to 20.7 ms | +1.7 to +2.6 ms (+9.2 to +13.4%) | -0.58 to +0.13 ms | -0.32 to +0.21 ms |
| 400000 | 76.7 to 88.7 ms | +7.0 to +11.0 ms (+6.4 to +13.5%) | -1.28 to +0.89 ms | -1.49 to +0.14 ms |

The added time grows in proportion to the message count. It is a per-message cost, not a fixed one,
which takes the lifecycle hypothesis off the list for this row. `/4` is 1 to 3% faster at every
count.

On x86_64, per CPU model across the three runs: the EPYC 7763 and EPYC 9V74 run `/4` 13 to 19% and
`/16` 5 to 10% faster with the variant at every count; the EPYC 9V45 (100000 only) is within ±2.4%;
the Xeon 6973P-C runs `/16` slower by +14.3% at 100000 (+0.86 ms) and +24.7% at 400000 (+5.14 ms)
with A/A within ±2.5%, and `/4` by -1.0% and -2.9%. So the `/16` cost is not a property of the N2:
it appears on two of five CPU models and grows with the count on both. The verdict rules keep the
x86_64 rows inconclusive, since the jobs of one run draw different models.

What distinguishes `/16` from `/4` and from the other rows is one producer feeding sixteen
consumers on four worker threads, where a worker's mailbox can run empty between the root's sends,
while four workers stay saturated. How often the workers park is not measured. The variant's
change concerns the future which parks: flume's `RecvFut::poll` is inlined into the run loop and
calls `poll_inner` out of line. Whether the wait and wake path costs more in that form is a
hypothesis.

## Receive counts in `fan_out`

Do the `/16` workers wait for messages, and does the variant make them wait more often? Three
diagnostic commits add the same counters to `main`, the cursor and the variant
(`da9f028`, `400de4b` and `5c3c4ec`, which were on `diag/recv-counts-main`,
`diag/recv-counts-cursor` and `diag/recv-counts-inline`): a per-mailbox counter of completed
receives (an item delivered), of completed receives whose flume future returned `Pending` at least
once, and of `Pending` returns, the last including receives later canceled. A mailbox's counter adds
itself to a process-wide table keyed by message type when it is dropped, so the hot path writes
plain fields and the table is locked once per actor. The bench takes the table before each `fan_out`
row and prints it after, so a row's counts, warm-up included, are isolated; the ratios per delivered
message do not depend on criterion's iteration count. `Tick` is the workers' type, `Go` the root's.

The counters change the generated code: the run loops grow by 40 to 90 instructions, `Mailbox`'s
drop glue by 16, and the recv future by 21 where it is out of line. They preserve what is under
investigation: the recv future stays out of line for `Go` and `Tick` on `main`, for all four types
in the cursor and for none in the variant. The workflow `diag-receive.yml`, which was on
`diag/recv-counts-workflow`, ran one `ubuntu-24.04-arm` job: the three plain binaries and the three
counted ones on `fan_out/workers/4` and `/16` at four stack positions and three repetitions, then
each counted binary once per row for its counts. Run 40306893604:

| Binaries | `/4` variant | `/4` cursor | `/16` on `main` | `/16` variant | `/16` cursor |
|---|---|---|---|---|---|
| plain | -4.1% | -5.0% | 18.5 ms | +8.9% (+4.3 to +11.5%) | +0.9% |
| counted | -8.7% | +0.5% | 18.9 ms | +2.3% (-2.9 to +6.2%) | -3.8% |

Medians over the four positions, against `main`, no A/A control in this job. The plain binaries
show the `/16` slowdown of the sweeps; the counted ones keep its direction at every position but one
and lose most of its size, so the counts below describe the instrumented workload.

| Binary | Row | `Tick` receives | Waited at least once | `Pending` returns |
|---|---|---|---|---|
| `main` | `/4` | 8600000 | 2.3% | 2.3% |
| cursor | `/4` | 8600000 | 2.4% | 2.4% |
| variant | `/4` | 8600000 | 2.5% | 2.5% |
| `main` | `/16` | 7000000 | 33.6% | 33.6% |
| cursor | `/16` | 7000000 | 32.4% | 32.4% |
| variant | `/16` | 4500000 | 41.5% | 41.5% |

Both ratios are per delivered worker message; the receive totals differ with criterion's iteration
count. The root's `Go` receives wait 49 to 51% of the time at `/4` and 32 to 38% at `/16` in every
binary. Three things follow:

- The `/4` workers are saturated and the `/16` workers are not: one receive in forty waits at `/4`,
  one in three at `/16`. The condition the hypothesis assumed is measured.
- Every waiting receive returns `Pending` exactly once. The two integers are equal in each of the
  twelve count lines, 2269879 of 7000000 for `main`'s `/16` workers for instance, so no receive
  returned `Pending` twice and no receive returned `Pending` and was then canceled. The counts
  bound less than that: a receive canceled before any `Pending` is not counted, and a wakeup which
  coalesces with another is not a `Pending` return, so neither spurious wakeups nor cancellations
  are excluded by them.
- The variant's `/16` workers wait more often than `main`'s: 41.5% against 33.6% of receives, a
  quarter more wakeups per message. The cursor is at `main`'s level, and the cursor is at `main`'s
  time. A faster receive drains a mailbox sooner and parks more often; each wakeup is a scheduling
  round trip between the root's send and the worker's task. Whether those extra wakeups account for
  the added time is the question now, not the cost of the receive itself. That is a hypothesis
  about scheduling, measured here on the instrumented binaries only.

## Producer bursts in `fan_out`

Does the `/16` penalty go away when the workers wait less often? The intervention is in the
producer, not in the runtime: the root sends each worker a burst of messages before it moves to
the next worker, so a worker's mailbox holds several items when it wakes and it drains them
without parking in between. Each worker still gets its 6250 messages in order, and the total
stays at 100000. Four commits, each one commit on its parent:

- `1863d07` (was on `diag/fan-out-burst-base`): `39aec83` plus the bench change. `FanOutRoot`
  gets a `burst` field; the rows `fan_out/workers/4` and `/16` run with bursts of one, which is the
  send sequence of `main`'s round-robin loop, and the rows `fan_out/burst/4` and `/16` run
  sixteen workers with bursts of four and sixteen. The burst is a runtime parameter of one binary,
  so the four rows share one layout. 6250 is not a multiple of four or sixteen; the last burst is
  shorter.
- `183ca75` (was on `diag/fan-out-burst`): the complete `b30e728` variant on that base; the diff
  is the variant's diff to `39aec83`, shifted by the bench change's lines.
- `660f590` and `8058f4e` (were on `diag/fan-out-burst-counts-main` and
  `diag/fan-out-burst-counts-inline`): the receive counters of the previous section on the base
  and on the candidate, printing a row's counts after the row.

The workflow `diag-burst.yml`, which was on `diag/fan-out-burst-workflow`, runs one
`ubuntu-24.04-arm` job with six binaries: `main`, the variant and a copy of `main` as A/A control,
plain and counted. Its `sweep.py` keeps the `RECV_COUNTS` lines of each execution's stderr in that
execution's result line, so a counted execution's time and its counts come from the same process;
the earlier round took them from separate executions. Four rows, four stack positions, three
repetitions, the execution order rotated per repetition as in the sweep.

Generated code, checked on the local aarch64 build before the run: the base keeps the recv
closure out of line for `Go` and `Tick`, the candidate inlines it and calls flume's `poll_inner`
directly, and both carry the burst loop in `FanOutRoot`'s run loop. The counters add 43 to 88
instructions to `main`'s run loops and 26 to 101 to the variant's, 21 to the recv closure where it
is out of line, and a 158 instruction `RecvCounter::drop`; the inlining state of the recv future
is the same with and without them. A local one position pass through the driver shows what the
local machine does with the workload: its `/16` workers wait on 0.9% of their receives against
33.6% on the Neoverse-N2, which is why the row never reproduced here, and bursts of four and
sixteen cut the waiting to 0.0 to 0.1%.

Run 40323020372, one Neoverse-N2 job. Timings are medians over the four positions, against
`main`, with the per position range of the variant where it matters:

| Binaries | `/4`, burst 1 | `/16`, burst 1 | `/16`, burst 4 | `/16`, burst 16 |
|---|---|---|---|---|
| plain, `main` | 12.7 ms | 21.2 ms | 10.2 ms | 6.5 ms |
| plain, variant | -1.7% | +9.4% (+1.6 to +11.2%) | +17.8% (+4.4 to +25.4%) | -0.5% (-0.8 to +1.2%) |
| plain, A/A | -3.7% | +1.0% | +0.2% | -1.1% |
| counted, variant | -2.1% | +3.6% | +16.5% (+5.9 to +22.4%) | +3.5% (+2.2 to +3.7%) |
| counted, A/A | -0.8% | +3.0% | +2.5% | -0.5% |

In absolute time the plain variant adds 1.9 ms at burst 1 (0.3 to 2.3 ms over the positions),
1.6 ms at burst 4 (0.5 to 2.5 ms) and nothing at burst 16 (-0.05 to +0.07 ms), while the A/A
control stays within 1 ms at bursts 1 and 4 and within 0.15 ms at burst 16. The bursts halve and
then third `main`'s time, so the same added time doubles as a percentage at burst 4. The `/4` row
moves by up to 10% per position in both directions, A/A included; it is the saturated reference and
says nothing here.

The counts of the same executions, worker (`Tick`) receives which returned `Pending`, per 100000
delivered messages, summed over the twelve executions of a row and binary:

| Binary | `/4`, burst 1 | `/16`, burst 1 | `/16`, burst 4 | `/16`, burst 16 |
|---|---|---|---|---|
| `main` | 2371 (2.4%) | 36370 (36.4%) | 11789 (11.8%) | 3149 (3.1%) |
| variant | 2584 (2.6%) | 39895 (39.9%) | 15893 (15.9%) | 5042 (5.0%) |
| A/A | 2377 (2.4%) | 35641 (35.6%) | 12034 (12.0%) | 3164 (3.2%) |

The completed and `Pending` integers are equal in all 72 count lines again. The bursts do what
they were meant to do: `main`'s `/16` workers wait on a third of their receives at burst 1, on an
eighth at burst 4 and on one in thirty at burst 16. Read against the plan's three outcomes:

- At burst 16 the waiting falls by a factor of ten and the variant's penalty is gone in the plain
  binaries. That is the outcome which supports a cost tied to waiting.
- At burst 4 the total waiting falls by two thirds and the penalty stays, with the same added
  1.6 to 1.9 ms as at burst 1 and twice the ratio. The variant's excess waiting over `main` stays
  as well: 4.1 percentage points against 3.5 at burst 1. Similar added time with similar excess
  waiting is compatible with a cost per excess wait. What the row rules out is a cost proportional
  to the total waiting.
- Instrumentation changes the `/16` burst 1 row again: +9.4% plain, +3.6% counted with the A/A at
  +3.0%. It preserves the burst 4 row (+16.5%) and turns the burst 16 row from -0.5% into a
  consistent +3.5%. The count conclusions hold for the counted binaries.

Two more things the same executions show. Within a row, an execution's time moves with its waiting:
across the twelve executions of a row the variant's time ratio correlates with its added waiting
share at 0.66 to 0.92 (Pearson), and each binary's own time with its own waiting share at 0.3 to
0.9. Across rows, the counted binaries alone, paired per execution and the median of the twelve
differences taken (a different aggregate from the tables above):

| Burst | Added time | Excess waited receives per 100000 | Descriptive ratio |
|---|---|---|---|
| 1 | +1.87 ms (A/A +0.18) | +5219 (A/A +623) | 0.36 us per excess wait |
| 4 | +1.54 ms (A/A +0.33) | +4235 (A/A +164) | 0.36 us per excess wait |
| 16 | +0.20 ms (A/A -0.02) | +1988 (A/A -52) | 0.10 us per excess wait |

Bursts 1 and 4 give the same ratio, burst 16 a third of it. That challenges one fixed price per
excess wait across the workloads; it does not eliminate waiting or scheduling costs, and a
`Pending` return is not a measured scheduler wakeup. The plain timings cannot be set against these
counts at all, since the instrumentation changes the timings materially. The variant's workers wait
more often than `main`'s in every row, the two rows without a penalty included. The root's `Go`
receives, a `Go` and sixteen terminated signals per iteration, wait 46 and 60% of the time on
`main` at bursts 4 and 16 and 36 and 41% on the variant; that is the root parked while its workers
terminate, not a per-message quantity.

What the run leaves open is where the added time goes. Excess waits with a cost per wait which
differs between burst 16 and the other two rows fit the counted binaries. Contention between the
producer's send and a consumer's receive on one channel fits as well: at bursts 1 and 4 the
producer rotates through sixteen channels while up to three workers drain theirs, at burst 16 it
stays on one channel for sixteen sends. Both are hypotheses. The next measurement locates the
added CPU time or waiting in the plain binaries, by sampling profiles on the N2, before anything
about channel locking or parking changes.

## Sampling profiles of the plain binaries

Where does the added time go? The workflow `diag-profile.yml` (kept with the run's artifacts)
runs one `ubuntu-24.04-arm` job on the plain binaries of the burst commits (`1863d07` as `main`,
`183ca75` as the variant, a copy of `main` as A/A): first the timing sweep of the three `/16` rows
at four positions and three repetitions, then, per binary, row, position and repetition in rotated
order, one execution under `perf stat` (task clock, context switches, migrations, elapsed) and one
under `perf record` (`cpu-clock` at 1999 Hz, instruction addresses and symbols, no call chains).
Each profiled execution keeps criterion's estimates and its iteration counts. The profile covers
the whole process, warm-up and harness included, while the denominator is criterion's measured
iterations times 100000, so every per message figure below is an approximate whole process CPU
cost per measured message; the warm-up is 0.05 s of a 3 s measurement. Run 40328088331, perf
6.17 on a Neoverse-N2, 108 profiled executions:

| Row | Sweep, variant | Sweep, A/A | Under `perf record` | Under `perf stat` |
|---|---|---|---|---|
| `/16`, burst 1 | +9.1% | -2.2% | +14.1% (A/A -0.7%) | +5.2% (A/A -4.2%) |
| `/16`, burst 4 | +17.7% | +1.5% | +20.1% (A/A -0.3%) | +21.3% (A/A +1.8%) |
| `/16`, burst 16 | -0.3% | +0.5% | +0.6% (A/A +0.8%) | +0.5% (A/A +0.0%) |

Profiling leaves the pattern in place: burst 4 largest, burst 16 nothing, burst 1 between, with the
burst 1 row moving between +5 and +14% across the three kinds of execution. Per delivered message,
`main`'s value and the median of the paired variant minus `main` differences over the twelve
executions of a row (A/A in parentheses):

| Row | Wall ns | CPU ns (`perf stat`) | Sampled ns | Thread context switches per 100000 messages |
|---|---|---|---|---|
| burst 1 | 204, +16 (-5) | 756, +31 (-25) | 731, +83 (-4) | 800, +210 (-58) |
| burst 4 | 93, +19 (0) | 368, +64 (0) | 369, +61 (+4) | 232, +237 (-1) |
| burst 16 | 64, 0 (0) | 257, -1 (0) | 256, -1 (+2) | 40, +62 (0) |

The variant adds more CPU than wall time at bursts 1 and 4, so part of its extra work runs in
parallel with the rest. It parks and wakes its worker threads more often in every row, burst 16
included; these are thread level switches, a few hundred per 100000 messages against 2000 to 5000
excess task level waits, so most excess waits are served by a running thread. The sampled CPU by
symbol, `main` and the paired difference, in ns per message:

| Symbol | burst 1 | burst 4 | burst 16 |
|---|---|---|---|
| `__aarch64_cas4_acq` (32 bit compare and swap, acquire) | 61, +127 | 33, +70 | 17, +56 |
| `__aarch64_swp4_rel` (32 bit swap, release) | 115, +9 | 61, +11 | 45, +2 |
| `__aarch64_cas8_acq_rel` | 70, -50 | 21, -12 | 7, -4 |
| `__aarch64_cas1_acq` | 26, -26 | 8, -7 | 2, -2 |
| `__aarch64_ldadd8_acq_rel` and `ldset8_acq_rel` | 43, +20 | 18, +9 | 4, +4 |
| `NotifiedProject::poll_notified` (tokio) | 53, -43 | 48, -40 | 52, -45 |
| `AsyncSignal::fire` (flume, waking a receiver) | 1, +31 | 0, +12 | 0, +4 |
| `Steal::steal_into` (tokio) | 5, +23 | 1, +9 | 1, +4 |
| `State::transition_to_running` (tokio) | 1, +21 | 0, +8 | 0, +3 |
| `waker::wake_by_ref` (tokio) | 1, +20 | 0, +7 | 0, +3 |
| `Shared::recv` with `SyncSignal` (flume, the blocking path) | 51, -26 | 16, -6 | 5, -1 |
| `Shared::recv` with `AsyncSignal` (flume) | 29, -15 | 19, -8 | 17, -8 |
| `Sender::send` (flume) | 38, -19 | 16, -3 | 13, -6 |
| `Mutex::lock_contended` (std, the futex slow path) | 13, -4 | 10, -4 | 9, -5 |
| kernel, all symbols | 20, +6 | 8, +5 | 4, 0 |

Four things follow:

- The mutex slow path is small and smaller in the variant, and the kernel's share is a few ns per
  message in both. Whatever the variant pays, it pays in user space fast paths.
- The one symbol which rises in every row is the 32 bit compare and swap with acquire ordering,
  +56 to +127 ns per message, while its release partner, the swap, rises by at most 11. std's futex
  mutex locks with exactly that compare and swap and unlocks with that swap, and std's thread
  parker uses the same pair. More lock operations would raise both; a longer wait for the lock
  word's cache line raises the acquire alone. That is consistent with cache line contention on a
  32 bit lock word, not a measurement of it: samples do not separate more attempts from longer
  attempts, and a failed acquisition has no matching unlock, so the asymmetry alone does not
  establish cache line latency. Which lock word it is, flume's channel mutex, the scheduler's or
  the parker's, needs call chains.
- The wake path rises in step with the penalty: `fire`, `wake_by_ref`, `transition_to_running`
  and `steal_into` together add 95 ns at burst 1, 36 at burst 4 and 14 at burst 16. This
  supports the scheduling hypothesis on its own; it does not price the counted runs' excess waits,
  which came from different, instrumented executions.
- The variant saves 43 ns per message in `poll_notified` in every row and 30 to 60 ns in flume's
  receive and send and the 64 bit compare and swap at burst 1. Both run loops poll the same pinned
  `changed()` future first in every iteration, so the `poll_notified` saving is observed, not
  explained. At burst 16 the savings and the added lock and wake costs cancel, which is why that
  row shows no penalty; at bursts 1 and 4 the additions win.

The next measurement attributes the compare and swap and the wake path to their callers: the same
job with `perf record --call-graph dwarf` at a lower rate, `main` and the variant at the three
rows, folding the chains on the runner. Nothing about channel locking or parking changes before
that.

## Call chains of the lock compare and swap

Which lock word? A pilot (run 40334799503) probed call graph configurations on the N2 runner:
DWARF unwinding is rejected on the software clocks (`cpu-clock`, `task-clock`, user only, fixed
period: "PMU Hardware doesn't support sampling/overflow-interrupts") and accepted on the hardware
`cycles` event; frame pointer chains are accepted on both, since Rust keeps frame pointers in
non-leaf functions on aarch64. Both unwind the outlined `__aarch64_cas4_acq` to its caller in
99.8% of samples and name the same callers; frame pointers give four times the samples at a tenth
of the data. The full job (`diag-callchain.yml`, kept with the artifacts, run 40335459950) records
`main`, the variant and the A/A copy on the three `/16` rows at four positions in two rotated
repetitions with frame pointers on `cpu-clock` at 1999 Hz, 72 recordings of 23000 to 43000
samples, none lost, at most 0.23% of leaf callers unresolved (every chain's root is unresolved,
libc's thread start has no frame pointer, which does not touch the attribution), plus eight DWARF
recordings on `cycles` at 499 Hz for `main` and the variant at bursts 1 and 4 as a cross check.
Timing under the recording: +10.5% (A/A +0.3%), +17.3% (A/A -2.1%), +1.3% (A/A +0.7%); the pattern
holds. Per message figures are approximate whole process costs per measured message as before.

The compare and swap by its first caller outside std's mutex, exclusive sampled ns per message,
`main` and the paired difference:

| Caller | burst 1 | burst 4 | burst 16 |
|---|---|---|---|
| flume `Shared::recv`, the first attempt in `poll_inner` | 9, +34 | 2, +14 | 1, +5 |
| flume `Sender::send` | 18, +24 | 15, +8 | 6, +6 |
| flume `Shared::recv`, the hook registration | 5, +12 | 4, +7 | 3, +5 |
| flume, all callers | 32, +68 | 22, +30 | 10, +16 |
| tokio `poll_notified`, the stop channel | 24, +44 | 11, +35 | 7, +38 |
| all other callers | 4, +9 | 1, +3 | 1, +1 |

The DWARF recordings agree: 60 to 69% of `main`'s compare and swap samples sit under flume and 29
to 34% under `poll_notified`, the variant's 47 to 58% and 38 to 50%. The release swap moves little
in either family (flume 90, +5 / 50, +5 / 38, +2; the stop channel 13, +2 / 8, +1 / 6, 0), and the
futex slow path `lock_contended` is small and lower in the variant. So the lock operations are not
much more numerous and few fail; the acquires take longer. That is consistent with the lock words'
cache lines being held elsewhere; it is not a direct measurement of coherence traffic.

Two lock words, then. The first is the mailbox's channel mutex: flume takes it in `send` and in
every receive attempt, so the root's send and a worker's receive on the same mailbox contend for
it, which is the one producer and sixteen consumers hypothesis of the burst section. The second is
the stop channel's: `watch::Receiver::changed` registers on one of eight `Notify` instances chosen
at random per `changed()` call, and a waiting `Notified` takes that instance's waiter mutex on every
poll ("In order to access the waker fields, we must acquire the lock", tokio 1.53 `notify.rs`).
Both run loops poll the pinned stop future first in every select iteration, and sixteen workers
share eight instances, two per mutex on average, so every iteration of every worker locks a mutex
another worker may hold.

Inclusive totals by family, a sample counted once per family present anywhere in its chain, so the
rows do not add up, `main` and the paired difference in ns per message:

| Family | burst 1 | burst 4 | burst 16 |
|---|---|---|---|
| flume channel (`send`, `recv`, `len`) | 354, +28 | 168, +27 | 107, +2 |
| flume hook and signal (`fire`, hook cleanup) | 134, +19 | 42, +19 | 12, +10 |
| tokio scheduler and task (`schedule`, `steal_into`, state) | 117, +16 | 36, +18 | 10, +9 |
| tokio park and unpark | 38, +11 | 13, +10 | 5, +1 |
| kernel | 24, +9 | 11, +6 | 7, -1 |
| tokio notify, the stop channel | 94, +2 | 70, -5 | 67, -7 |
| whole process | 756, +57 | 373, +56 | 253, +2 |

Three things follow:

- The stop channel's path costs the same in both binaries. The variant's 43 ns saving in
  `poll_notified`'s body and its 35 to 44 ns more in that function's lock acquire are the same
  path's samples landing elsewhere; the path's total moves by -7 to +2. The eight way sharing of
  the stop channel's mutexes is a cost both binaries pay; it is not where the added time is.
- The added time at bursts 1 and 4 is in flume's channel path (+28, +27) and in the wake and
  scheduling paths (+19 and +16 hook and scheduler, +11 park and unpark, +9 kernel at burst 1).
  Inside flume's path the lock acquire grows by 68 and 30 while the bodies of `send` and `recv`
  shrink by 61 and 19. At burst 16 flume's path is unchanged (+2), the wake and scheduling paths
  still add 10 and 9, and savings in the run loop and the stop channel cancel them.
- The excess wakeups are the one cost present in every row. The channel lock contention is the
  cost which rises and falls with the penalty: the producer visits a channel 6250 times per
  iteration at burst 1, 1563 times at burst 4 and 391 times at burst 16, and the longer acquires
  go with the more frequent visits.

What remains is a design question, not a measurement. The variant's receive reaches the channel
mutex sooner after a wake, and each message costs one lock acquisition on each side; a receive
which took several queued items per acquisition would change the second, and how a woken worker
drains its mailbox the first. Neither is tried here.

## The cursor with a single select

A measurement posted on #174 (https://github.com/tellus-rs/tellus/pull/174#issuecomment-6418223816)
proposes a select of fixed shape: a third branch polling a hand-written future whose poll is a null
check of the boxed table, instead of an `async` block behind an `if armed` precondition. The single
select variant ([`cursor-single-select.patch`](patches/cursor-single-select.patch) on the cursor,
built as `ad83454`, one commit on `39aec83`) applies that shape to the cursor. Both run loops use
`main`'s `next_incoming!` macro with one branch added between the stop signal and the mailbox: a
`poll_fn` over the context. Without a table it returns `Pending` and registers no waker; timers
change only in the actor's own synchronous callbacks on the same task (`init`, `receive`,
`recovered`, `handle`), so nothing has to wake the loop for them, and an armed sleep registers its
own waker. With a table it polls the sleep first and then checks the head of the fired timers. One
fired timer is taken per iteration, and the channel items are counted before the unwatched filter.
`has_timers`, the second macro and the switch are gone; `persistence/spawn.rs` is `main`'s. Clippy,
the timer, test context, persistence and core tests, the `#[ignore]`d fairness test (50 of 50) and
the allocation gate pass.

Generated code against `main`, both architectures, before the sweep:

- The `Ping` and `Pong` recv future is inlined and the `Go` and `Tick` one out of line, as on
  `main`. `RecvFut::poll` and `Mailbox::recv` are identical. This is what the variant set out to
  restore.
- One select per run loop. Its poll function grows by 40 instructions on aarch64 (134 to 174 for
  `Pinger`) and by 34 on x86_64. The `Countdown` run loop grows from 1092 to 1112 instructions on
  aarch64 and from 1047 to 1086 on x86_64, where the plain cursor had 1328.
- flume's `send` and `Shared::recv` keep their sizes within 1% (723 to 717 and 735 to 728 on
  aarch64, 691 to 674 and 728 to 726 on x86_64) with a different register allocation and block
  order. `send` calls the same functions and has the same lock instructions; it has one more site of
  an `Arc` decrement (a tenth `ldadd8_rel` call on aarch64, eleven `lock decq` instead of nine on
  x86_64). Whether the per-message path executes it is not established. `RecvFut::drop`, run once
  per iteration, shrinks from 309 to 240 instructions on aarch64 and 312 to 253 on x86_64.

Run 40355604054:

| Row | Single select N2 | Single select x86 |
|---|---|---|
| `queued/never/4` | pass, +2.2% | spread, +9.8% |
| `queued/never/16` | pass, +2.3% | spread, +10.1% |
| `queued/every/4` | borderline, +3.2% | fail, +8.6% |
| `queued/every/16` | borderline, +3.2% | fail, +8.6% |
| `flood/unbounded` | borderline, +3.2% | fail, +7.5% |
| `flood/bounded` | pass, +2.7% | fail, +13.6% |
| `fan_out/workers/4` | pass, -0.7% | spread, +12.4% |
| `fan_out/workers/16` | pass, -2.2% | fail, +8.0% |
| `ping_pong/pairs/1` | pass, +1.6% | spread, +0.8% |
| `ping_pong/pairs/4` | pass, +1.9% | pass, +0.9% |

On x86_64 that is five failures, four inconclusive rows and one pass. The jobs ran on two EPYC
7763 and two EPYC 9V74, with A/A within ±2.5% on all but one job. Per CPU model the throughput rows
are +9.4 to +16.6% on the 7763 and +6.5 to +13.2% on the 9V74. On the `queued` and `fan_out` rows
every job and every stack position is above +6%; the `flood` rows have exceptions (`flood/unbounded`
ranges from -5% to +21% over the positions, and one `flood/bounded` job measured +4.8%). No second
run was spent on the N2's borderline rows, since the criterion is every row on both runner types.

Three things follow. The `ping_pong` rows pass on the N2 (+1.6 and +1.9%), and on x86_64
`ping_pong/pairs/4` passes at +0.9% while `ping_pong/pairs/1` stays inconclusive by spread with a
median of +0.8%; the plain cursor was inconclusive at +4.4% and failed at +3.6% there. That is
consistent with the two-caller explanation of the cursor's `ping_pong` results and does not prove
it, since the variant changes more than the call site. The throughput rows are the worst of any
cursor variant on x86_64 (+7.5 to +13.6% against the plain cursor's +3.1 to +3.8% and the inlined
recv future's -4.6 to -8.6%) and 2 to 3 points above the cursor on the N2. So restoring the
inlining of the recv future seen on `main` is not sufficient for this timer design to meet the
criterion. The per-message differences from the cursor's untimed loop are the third branch's poll,
the `count_received` check on every item, the select's larger poll function and the register
allocation of flume's `send` and `recv`; the result does not isolate any of them as the cause. The
variant fails the criterion and is not a candidate.

## `main` with the inlined recv future

Does the inlined recv future have its `fan_out/workers/16` tradeoff without any timer code?
[`inline-recv.patch`](patches/inline-recv.patch) (on `39aec83`, built as `48ac3d0`) applies
`b30e728`'s change to `mailbox.rs`, and nothing else, to `main`: `Mailbox::recv` returns the
hand-written `Recv` future whose `#[inline(always)]` poll wraps flume's `RecvFut` and releases the
quota in the same poll. Clippy, the test suite, the `hotpath` profile (`mailbox::recv` at 110000
calls, as on `main`) and the allocation gate pass.

Generated code against `main`, both architectures, revalidated on this base rather than carried
over from the cursor:

- No recv future is out of line for any of the four message types; the run loops call flume's
  `poll_inner` directly, and `RecvFut::poll` disappears in its favor at the same size.
- The run loops grow by 2 to 4 instructions. The select poll functions of all four actor shapes
  converge on one size (100 instructions on aarch64, 88 on x86_64), from 85 and 134 on `main`.
- flume's `send`, `Shared::recv` and `pull_pending` are identical in size. The drop glue of
  `Mailbox`, `ActorRef` and `SelfRef` and tokio's `spawn` are unchanged, unlike on the cursor base,
  where the same one-file change moved them. The only other differences are criterion's `block_on`
  and `iter_custom` wrappers around the measured iterations.

Run 40410138410:

| Row | Inlined recv on `main`, N2 | Inlined recv on `main`, x86 |
|---|---|---|
| `queued/never/4` | pass, -5.7% | pass, -7.2% |
| `queued/never/16` | pass, -5.5% | pass, -7.5% |
| `queued/every/4` | pass, -5.6% | pass, -6.9% |
| `queued/every/16` | pass, -5.0% | pass, -7.2% |
| `flood/unbounded` | pass, -6.0% | spread, -8.3% |
| `flood/bounded` | pass, -3.6% | spread, -9.1% |
| `fan_out/workers/4` | pass, -2.5% | pass, -17.4% |
| `fan_out/workers/16` | fail, +9.0% | pass, -10.0% |
| `ping_pong/pairs/1` | pass, -0.7% | spread, -2.3% |
| `ping_pong/pairs/4` | pass, -1.0% | pass, -1.2% |

CPU coverage: four Neoverse-N2 jobs; on x86_64 three EPYC 7763 and one EPYC 9V74, so neither the
Xeon 6973P-C, which showed the `/16` cost in the message count sweeps, nor the EPYC 9V45 was drawn.
A/A is within ±2.9% on every job. The three inconclusive x86_64 rows are negative in every job
(`flood/unbounded` -4.8 to -11.3%, `flood/bounded` -3.1 to -13.6%, `ping_pong/pairs/1` -5.3 to
+2.2%) and inconclusive by spread, which a second run does not decide under `report.py`'s rules.

The N2 `/16` row fails at +9.0% with every job between +8.2 and +11.4%, where the inlined recv
cursor measured +10.7% and the counted and burst diagnostics +8.9 and +9.4%. So the receive
optimization has this tradeoff on its own: timers are not required to trigger it, and the cursor's
timer code, which is not on the `fan_out` path, did not contribute to it. Every other row is faster
than `main` on both runner types. On x86_64 `/16` is 10% faster on the models drawn, consistent with
the earlier per-model split (EPYC 7763 and 9V74 faster, Xeon 6973P-C slower); the Xeon result is
not revisited here. Two things follow for the decisions. The `/16` question belongs to the receive
optimization, a core change with its own tradeoff, and not to the timer PR. And a timer design can
be evaluated against `main`'s receive without carrying that row, which is how the cursor was
measured: it fails x86_64 at +3.1 to +3.8%, and no timer design on `main`'s receive has passed the
x86_64 throughput rows so far.

## #174's candidate

The contributor built the design proposed on #174, recorded here as
[`inline-gate.patch`](patches/inline-gate.patch) (on #174's head `283e5e7`, built as `a8ab639`, one
commit on `39aec83`): the gate inlined into the run loop as two arms and a fixed-shape select, with
the `Queued<M>` item kept in the channel. Run 40373337251, swept by the contributor
(https://github.com/tellus-rs/tellus/pull/174#issuecomment-6424377550): on the Neoverse-N2 eight
rows pass (-3.3 to +2.8%) and `flood/unbounded` and `flood/bounded` are borderline at +3.2 and
+4.1%; on x86_64 (two EPYC 9V45, two EPYC 9V74) `queued/never/4` fails at +5.6% and
`flood/unbounded` at +5.7%, five rows are borderline at +4.2 to +4.8%, two are inconclusive by
spread and `ping_pong/pairs/4` passes. That is the failure the item diagnostic predicted for a
configuration which keeps `Queued<M>` in the channel. The candidate does not meet the criterion.

## The cursor with a combined waiting future

The single select failed with the timers polled in a third branch. Does one waiting future which
checks the timers and then polls `main`'s unchanged `Mailbox::recv` avoid that cost, without the
hand-written always-inlined recv future and its `fan_out/workers/16` tradeoff? The combined variant
([`cursor-combined.patch`](patches/cursor-combined.patch) on the cursor, built as `f55717e`, one
commit on `39aec83`) is the single select with its timer branch folded into the mailbox branch. The
select has `main`'s two branches, the stop signal first. The second branch is a `poll_fn` over
`ActorContext::poll_awaited`, which polls the sleep and checks the fired timers' head when a table
exists and otherwise does nothing, and then polls the recv future pinned in the loop body
(`pin!(mailbox.recv())`). It returns an `Awaited` enum: a timer event (fired, or deliverable) or the
received item. The outer loop fires due timers and takes one fired timer per iteration as before,
charges the cooperative budget before every wait and counts the channel items before the unwatched
filter. `mailbox.rs` and `persistence/spawn.rs` are `main`'s; the source has one call site of
`Mailbox::recv`. Clippy on three feature sets, the timer, test context, persistence and core tests,
the `#[ignore]`d fairness test, the `hotpath` profile (`mailbox::recv` at 110000 calls) and the
allocation gate pass.

Generated code against `main`, both architectures, before the sweep:

- `poll_awaited` is out of line for all four message types (109 instructions on aarch64), and the
  recv future is inlined into it, so there is one recv call site in the binary as in the source.
  The select poll function is 86 instructions for every actor shape, which on `main` is 85 with
  the recv future out of line (`Go`, `Tick`) and 134 with it inlined (`Ping`, `Pong`). So the
  `Ping` and `Pong` paths gain one call per poll compared with `main`; the `Go` and `Tick` paths
  keep one call, into `poll_awaited` instead of into the recv future.
- The run loops grow by 26 to 58 instructions (`Countdown` 1092 to 1118 on aarch64, 1047 to 1102
  on x86_64), more than the single select's 20 and 39.
- flume's `send` and `Shared::recv` keep their sizes within 1% with the same register allocation
  changes as on the single select (723 to 717 and 735 to 728 on aarch64, 691 to 674 and 728 to 726
  on x86_64). `RecvFut::drop` shrinks from 309 to 240 and 312 to 253. The drop glue of the pinned
  recv future is `PinMacroHelper`'s instead of the select tuple's.
- With `#[inline]` on `poll_awaited`, built locally and not swept, the compiler restores `main`'s
  pattern: `poll_awaited` is inlined into the `Ping` and `Pong` select polls (134 to 170
  instructions) and stays out of line for `Go` and `Tick` (select poll 85 to 87). The swept
  candidate carries no hint.

Timed actors, the `timers` group locally (aarch64, 10 cores, one stack position, three alternating
runs each), the combined future against the plain cursor's best of three:

| Row | Combined against cursor, best | median |
|---|---|---|
| `timers/none/4` | +7.7% | +4.2% |
| `timers/none/16` | +4.6% | +5.1% |
| `timers/armed_idle/4` | +7.5% | +7.3% |
| `timers/armed_idle/16` | +6.5% | +7.0% |
| `timers/restart_each/4` | +1.6% | +2.2% |
| `timers/restart_each/16` | -0.6% | +0.8% |

The `none` rows run no timer, so on the plain cursor they take `main`'s loop and on the combined
future the new one; the 4 to 5% there is the untimed cost at this stack position. The armed idle
timer costs 7% on top of the cursor, where the cursor's armed sleep is polled through its own
branch under `if armed`. Restarting the timer on every message is within noise.

Run 40467666634:

| Row | Combined future N2 | Combined future x86 |
|---|---|---|
| `queued/never/4` | pass, +1.9% | fail, +14.4% |
| `queued/never/16` | pass, +1.2% | fail, +14.4% |
| `queued/every/4` | pass, +2.9% | fail, +11.9% |
| `queued/every/16` | pass, +2.8% | fail, +11.9% |
| `flood/unbounded` | borderline, +4.5% | too few jobs, +1.1 to +19.3% |
| `flood/bounded` | fail, +5.0% | spread, +11.9% |
| `fan_out/workers/4` | pass, +1.1% | fail, +17.4% |
| `fan_out/workers/16` | pass, -3.7% | fail, +9.7% |
| `ping_pong/pairs/1` | pass, +2.2% | fail, +7.5% |
| `ping_pong/pairs/4` | borderline, +3.0% | fail, +7.4% |

CPU coverage: four Neoverse-N2 jobs and four EPYC 7763 jobs, no other x86_64 model. On x86_64 eight
rows fail at +7.4 to +17.4% and two are inconclusive, `flood/unbounded` with two of four jobs
failing A/A and `flood/bounded` by spread, both positive in every job. The failing rows have a
spread of 0.5 to 1.7 pp except `ping_pong/pairs/1` at 5.6 pp, with A/A within ±1.6%. On the N2
`flood/bounded` fails at +5.0%, `flood/unbounded` and `ping_pong/pairs/4` are borderline and seven
rows pass; no second run was spent, since the x86_64 verdict stands alone.

This is the worst x86_64 result of any variant: the plain cursor failed at +3.1 to +3.8%, the single
select at +7.5 to +13.6%, and the combined future fails every decided row at +7.4 to +17.4%, the
`ping_pong` rows included, which the single select passed. The N2 result is also behind the plain
cursor's. Folding the timer poll into the mailbox's waiting future does not avoid the cost of the
third branch; measured on `main`'s receive, it adds to it. The generated code offers one difference
to the single select, the out of line `poll_awaited` and the extra call on the `Ping` and `Pong`
paths, but the `queued` and `fan_out` rows, whose recv future is out of line on `main` as well, fail
by 10 to 17%, so the call alone does not explain the result. The variant fails the criterion and
is not a candidate. With it, every bounded loop variant on `main`'s receive has failed the x86_64
throughput rows: the plain cursor, the single select, the combined future and #174's own candidate.

## Timers as an opt-in per actor type

The decision of 2026-10-27 implemented: #185, swept as `077c74a`, one commit on `main` at `ac59a98`;
the later commits of the branch add this record, the protocol extension below and the review fix
described at the end of this section. `Actor` and `EventSourced` gain `type Timers: TimerMode;` with
the two sealed modes `Untimed` and `Timed`; the context is `ActorContext<M, T = Untimed>`, and
`start_timer` and `cancel_timer` exist only on `ActorContext<M, Timed>`. `spawn` dispatches on
`A::Timers` through the sealed trait to one of two functions, each with its own task future: the
untimed one expands `main`'s wait macro, the timed one the cursor's, from one macro over `main`'s
loop body. Neither loop checks the mode, and the timer table of a timed actor lives in its context
from the start (`RefCell<TimerTable<M>>`, no `Option`, no box). An untimed actor cannot call
`start_timer`, and no code outside tellus can call the dispatch: both are pinned by `compile_fail`
doctests and were checked from a separate crate (E0599 and E0277; a diverging expression such as
`unreachable!()` type-checks as the private token, so the call can be written but never executed).
Every `impl Actor` and `impl EventSourced` in the workspace gained `type Timers = Untimed;` (178
sites); the timer tests' actors and the bench's `IdleTimeout` are `Timed`. Four tests pin that
parent and child modes are independent, for plain and event-sourced children.

Generated code against `main`, both architectures, bench binaries built with v0 symbol mangling and
the candidate's names normalized (`spawn_untimed` to `spawn`, the `Untimed` argument dropped):

- The select poll functions, the recv futures (two out of line as on `main`), flume's `send`,
  `Shared::recv` and `pull_pending`, `receive_incoming` and `should_restart` are identical in
  instruction count on both architectures; on x86_64 `tell` is identical too.
- The untimed run loop futures grow by 5 to 7 instructions on aarch64 and 6 to 8 on x86_64. The
  diff is confined to the path handling a terminated signal: `HashMap::remove` in
  `take_watched_for` is now inlined there (`hash_one` plus `RawTable::remove_entry` instead of one
  call), the rest is branch offsets. The per-message path is unchanged.
- On aarch64 `ActorRef::tell` has 7 instructions fewer by block layout and tail merging (one
  epilogue copy less, no change in loads, stores or calls).
- Outside the message path, inlining moved: `WatcherRegistration::close` 354 to 116 instructions,
  `unwatch` 114 to 31 on aarch64 and 25 to 189 on x86_64, `WatcherRegistry::remove` out of line,
  `PingPongRoot::receive` out of line, `Pinger::init` 69 to 79 (it now calls the dispatcher).
- `ActorContext<Tick>` stays 144 bytes; `ActorContext<Tick, Timed>` is 304 bytes. The untimed run
  loop futures have `main`'s sizes exactly (`-Zprint-type-sizes`: `Countdown` 1016 bytes,
  `FanOutRoot` and `Pinger` 1064, `PingPongRoot` and `Probing` 1040, `Ponger` 1128); the timed
  `IdleTimeout` future is 1520 bytes.

Checks before the sweep: `just lint` (the full matrix), `just test` with the cluster test rerun
alone after a timing failure under four concurrent release builds, `just doc`, the comparison
crate's check and lint, the `#[ignore]`d fairness test, `just profile` (`mailbox::recv` at 110000
calls) and the allocation gate (0 B).

Timed actors, the `timers` group locally (aarch64, 10 cores, one stack position), the candidate
against the plain cursor `de24ce8`, three rounds of two runs each per binary, so the A/A spread is
the median of a binary's second runs against its first:

| Row | Candidate against cursor | A/A cursor | A/A candidate |
|---|---|---|---|
| `timers/none/4` | +2.7% | +0.4% | +0.1% |
| `timers/none/16` | +2.4% | +0.2% | +0.4% |
| `timers/armed_idle/4` | +5.6% | +0.8% | +0.3% |
| `timers/armed_idle/16` | +5.9% | +0.7% | -0.1% |
| `timers/restart_each/4` | -2.6% | -0.9% | +0.9% |
| `timers/restart_each/16` | -1.5% | +0.3% | -0.7% |

The `none` rows compare the candidate's timed loop, which a `Timed` actor runs from its first
message, with the cursor's untimed loop, which the cursor ran until the first `start_timer`: 2.4 to
2.7% is the price of declaring timers without using them, at this position. The `armed_idle` rows
are 5.6 to 5.9% slower than the cursor with an A/A spread below 1%, so this is a real difference at
this position; both binaries run the same wait macro, and the candidate's context holds the table
inline (304 bytes) where the cursor's boxed it. `restart_each` is faster. None of these rows is a
gate; they are recorded for the design of the timed loop.

Run 40490137650:

| Row | Opt-in N2 | Opt-in x86 |
|---|---|---|
| `queued/never/4` | pass, -3.1% | pass, +0.2% |
| `queued/never/16` | pass, -2.6% | pass, +0.2% |
| `queued/every/4` | pass, -3.6% | pass, -0.3% |
| `queued/every/16` | pass, -1.0% | pass, -0.3% |
| `flood/unbounded` | pass, -0.0% | spread, +0.4% |
| `flood/bounded` | too few jobs, -2.3 to +1.2% | pass, -1.1% |
| `fan_out/workers/4` | pass, +0.1% | pass, +0.3% |
| `fan_out/workers/16` | pass, -1.4% | pass, +0.3% |
| `ping_pong/pairs/1` | pass, +1.0% | pass, +1.2% |
| `ping_pong/pairs/4` | pass, +0.2% | pass, -0.5% |

CPU coverage: four Neoverse-N2 jobs; on x86_64 three EPYC 7763 and one EPYC 9V45, so neither the
EPYC 9V74 nor the Xeon 6973P-C was drawn. Eighteen rows pass and none fails or is borderline; this
is the first variant without a failing row. Eighteen passes and two inconclusive rows are not
twenty passes, so the gate is not passed by this run. The two rows are undecided, not failed. N2
`flood/bounded` has only two of four jobs passing A/A (the others measured +4.6 and -4.5% on
identical code) and the candidate at -2.3 to +1.2% in all four, so there the runners decided
nothing. x86_64 `flood/unbounded` has a spread of 6.1 points among the three jobs which passed A/A
(-2.1, +4.6 and -1.5%, and the +4.6% job's own A/A was +2.4%), with a median of +0.4%; runner noise
is one explanation of that spread, not an established one. Under the protocol a second run decides
borderline rows only, so these two stay inconclusive as measured; the confirmation run below
follows the extension added to the protocol for this case.

The confirmation run, 40499387401 on `bcabb43` (the same code plus this record), evaluated whole:

| Row | Opt-in N2, run 2 | Opt-in x86, run 2 |
|---|---|---|
| `queued/never/4` | pass, -4.1% | pass, +0.3% |
| `queued/never/16` | pass, -3.4% | pass, -0.1% |
| `queued/every/4` | pass, -3.9% | pass, -0.4% |
| `queued/every/16` | pass, -2.0% | pass, -0.0% |
| `flood/unbounded` | pass, +0.1% | pass, -0.5% |
| `flood/bounded` | pass, +0.9% | too few jobs, -2.3 to +1.3% |
| `fan_out/workers/4` | pass, +1.0% | pass, +0.5% |
| `fan_out/workers/16` | pass, -0.8% | pass, +0.4% |
| `ping_pong/pairs/1` | pass, +0.7% | pass, +2.5% |
| `ping_pong/pairs/4` | pass, +0.1% | pass, -0.5% |

CPU coverage: four Neoverse-N2 jobs; on x86_64 two EPYC 7763, one EPYC 9V74 and one EPYC 9V45. All
ten N2 rows pass, including `flood/bounded`, undecided in the first run. On x86_64 nine rows pass,
including `flood/unbounded`, undecided in the first run; `flood/bounded` is undecided here because
two jobs failed A/A (-5.4 and -4.0% on identical code), and it passed in the first run at -1.1%.
Over the two runs every row passes in at least one and no row fails or is borderline in either, so
the candidate is accepted under the confirmation rule. That rule is an extension of the original
gate, added for this case before the run, and the acceptance is described as such, not as twenty
passes in one sweep. The N2 `queued` rows are again 2 to 4% faster than `main`.

The code review of #185 found one ordering defect, inherited from the cursor: `fire_due_timers`
read the mailbox depth before the clock. A deadline passing between the two reads hence gave its
timer the earlier position, ahead of a message queued in between, which had arrived before that
deadline. The table now takes both samples itself, the clock first. A unit test passes a sampler
pair which changes the world between the two reads; it fails against the old order. The fix
touches only the timed path. Against the reviewed commit, the untimed run loops, their select polls
and the message functions keep their instruction counts on both architectures. On x86_64 their
stripped disassembly is identical. On aarch64 it differs in address immediates only, and the linker
inserted one Cortex-A53 erratum 843419 veneer into flume's async `recv` for `Tick`, after
`lock_contended`, which costs two branches on the contended mutex path. The N2 `queued` rows are 1
to 3.6% faster than `main`, which the generated code does not explain (the message path is
identical per function); A/A on those jobs is within ±1.4%, so the difference is real for this
build and position, and it is recorded, not claimed as a property of the change.

The fixed commit, swept as `1800f0c` against `main` at `ac59a98`, on the messaging bench, run
40518363938 and its confirmation run 40538689771, each evaluated whole:

| Row | N2, run 1 | N2, run 2 | x86, run 1 | x86, run 2 |
|---|---|---|---|---|
| `queued/never/4` | pass, -1.5% | pass, -3.2% | pass, +1.5% | pass, -0.0% |
| `queued/never/16` | pass, -1.4% | pass, -3.0% | pass, +1.9% | pass, +0.4% |
| `queued/every/4` | pass, -2.4% | pass, -2.4% | pass, +0.5% | pass, -0.2% |
| `queued/every/16` | pass, -2.2% | pass, -2.2% | pass, +0.7% | pass, -0.0% |
| `flood/unbounded` | pass, -0.7% | pass, -0.5% | pass, +0.2% | pass, -0.6% |
| `flood/bounded` | too few jobs | too few jobs | spread, +5.0% | pass, +0.9% |
| `fan_out/workers/4` | pass, -0.3% | pass, -0.4% | pass, +0.5% | pass, +1.0% |
| `fan_out/workers/16` | pass, +0.7% | pass, +0.7% | pass, +0.9% | pass, -1.5% |
| `ping_pong/pairs/1` | pass, +0.5% | pass, +0.1% | pass, +0.8% | pass, +0.2% |
| `ping_pong/pairs/4` | pass, +0.3% | pass, +0.1% | pass, +0.1% | pass, +0.3% |

CPU coverage: run 1 has three Neoverse-N2 jobs, and on x86_64 three EPYC 7763 and one EPYC 9V45.
Run 2 has four Neoverse-N2 jobs, and on x86_64 one each of EPYC 9V45, EPYC 9V74, Xeon 6973P-C and
Xeon Platinum 8573C. No row fails or is borderline in either run. x86_64 `flood/bounded` had a
spread of 10.9 points in run 1 and passed in run 2. N2 `flood/bounded` kept only two jobs after A/A
in both runs. A row inconclusive in both runs keeps the candidate pending under the protocol.

The maintainer accepted the fixed commit on the messaging bench as an exception on 2026-10-27. The
reason is the four null runs of `main` recorded with the protocol (40554618267, 40554629237,
40554639973 and the label run 40554253825): N2 `flood/bounded` passed in all four, with every job
kept after A/A and medians from -0.9 to +2.5%. The null runs measure the protocol's noise on
identical code, not the candidate, so the row stays undecided as measured; the exception does not
claim otherwise.

The same commit on the persistence bench, with the workflow and scripts of #188 (`--ref
ci/bench-protocol`), run 40554680520 and its confirmation run 40565297827:

| Row | N2, run 1 | N2, run 2 | x86, run 1 | x86, run 2 |
|---|---|---|---|---|
| `persist/no_snapshots` | pass, +1.0% | pass, +0.8% | spread, -0.5% | pass, +0.2% |
| `persist/snapshots` | pass, +0.5% | pass, +0.6% | pass, +0.1% | pass, -0.2% |
| `recover/replay` | pass, +0.0% | pass, +0.0% | pass, -0.1% | pass, +0.2% |
| `recover/snapshot` | pass, -0.1% | pass, -0.1% | pass, -0.2% | pass, +0.3% |

CPU coverage: four Neoverse-N2 jobs in each run. On x86_64, run 1 drew two EPYC 7763 and two EPYC
9V74, and run 2 one EPYC 7763, two EPYC 9V74 and one Xeon Platinum 8573C. x86_64
`persist/no_snapshots` had a spread of 6.4 points in run 1 and passed in run 2. Every row passes in
at least one run and none fails or is borderline in either, so the fixed commit is accepted on the
persistence bench under the confirmation rule.

The generated code of `ac59a98` against `1800f0c`, compared with `codegen.sh` from #188, which
resolves every call, branch and data reference to the symbol it points into, marks the rest
unresolved and never classifies a function with an unresolved reference as identical. The
candidate's names are normalized with `--rename '_untimed\b='` and `--rename
', tellus::timers::Untimed>=>'`, on both architectures:

- **Persistence bench:** of the 15 selected functions, 5 are identical: one select poll of the
  event-sourced run loop, `replay_page`, the event encoding and two settlement wrappers. The other
  10, the event-sourced run loop itself among them (2412 instructions, 81 unresolved references),
  match instruction for instruction except in references with no symbol to compare by, such as
  string literals and `anon` constants. Those references are not established as equal, so these
  functions are not shown to be identical.
- **Messaging bench:** the six untimed run loop futures differ by 5 to 8 instructions, as described
  above. On aarch64, the four `tell` instantiations and flume's async `recv` for `Tick` differ as
  described above, too.
- **Not listed above:** on aarch64, `receive_incoming::<PingPongRoot>` goes from 138 to 118
  instructions, since it calls `PingPongRoot::receive` instead of inlining it. That actor receives
  one `Go` and one terminated signal per pair, not a message per round. On x86_64 the function
  differs only in unresolved references.
- Every other function of the selection is identical or differs only in unresolved references,
  which are not established as equal.

## Next steps

- **`fan_out/workers/16`.** The inlined recv future removes the per-message cost on the `queued`,
  `flood` and timer rows and leaves this one failing row, whose added time grows with the message
  count on the Neoverse-N2 and the Xeon 6973P-C. The receive counts show its workers waiting on
  a third of their receives, and the variant's on two fifths. Producer bursts show the penalty
  is not proportional to the total waiting, and the variant's excess waiting over `main` is
  similar at bursts 1 and 4. Profiles with call chains put the added time in flume's channel path
  (the mailbox mutex, taken by the root's `send` and the worker's `recv`, whose acquire takes
  longer in the variant) and in tokio's wake and scheduling paths; the stop channel's `Notify`
  mutex is contended too, but its path costs the same in both binaries. The next step is a design
  decision, not a measurement: whether to change how a woken worker drains its mailbox (several
  queued items per lock acquisition), or to accept the row. `main` plus the inlined recv future
  alone reproduces the row at +9.0%, so the decision concerns the receive optimization as a core
  change, separately from the timers. Any measurement of such a change needs the N2 runner; the
  local machine does not reproduce the row (its `/16` workers wait on 0.9% of receives). On x86_64
  the row is 10% faster on the EPYC 7763 and 9V74 and was slower on the Xeon 6973P-C; the x86_64
  verdict depends on the CPU draw.
- **x86_64.** Four rows are inconclusive, none fails. A second run would decide `ping_pong/pairs/1`
  if its jobs pass A/A; the `fan_out` rows differ by CPU model, which a second run cannot settle.
- **Which design.** The cursor stays the working design. The out of line variant costs timed actors
  up to 18% and fails `ping_pong`. The single select variant restores `main`'s recv inlining and
  still fails five x86_64 rows by 7.5 to 13.6%; its per-message additions over the cursor's untimed
  loop (the third branch's poll, the `count_received` check, the larger select, the channel code's
  register allocation) are not separated. The combined waiting future fails eight x86_64 rows by 7.4
  to 17.4%. #174's own candidate fails two. With four loop variants on `main`'s receive failing the
  x86_64 throughput rows, the next decision is one of scope, not another loop variant: the timer API
  (what an actor without timers may pay, or whether timers are opt-in per actor at spawn so the
  untimed loop stays `main`'s), or the performance requirement. A calibration of how much added work
  in `main`'s loop costs on the sweep's runners would describe the gate; it does not authorize
  relaxing it. Decided on 2026-10-27: the gate stays, and timers become opt-in at the type level.
  `Actor` and `EventSourced` gain an associated type `type Timers: TimerMode;` with `Untimed` and
  `Timed`, the context becomes `ActorContext<M, T>`, and `start_timer` and `cancel_timer` exist only
  on `ActorContext<M, Timed>`. `spawn` picks one of two task futures by `T` before spawning; the
  untimed one is `main`'s loop over `main`'s context. The mode is fixed per spawn and survives
  restarts. Opt-in alone establishes nothing: the untimed loop's generated code must match `main`'s
  per function on both architectures, and the unchanged sweep decides. Done, see the opt-in section
  above: the message path matches per function, and the two whole sweeps accept the candidate under
  the confirmation rule. The code review on #185 found one ordering defect in the timed path, fixed
  (see above); the fix is accepted on the persistence bench, and on the messaging bench as a
  maintainer's exception for N2 `flood/bounded`. #185 was merged on 2026-10-28 (`8252b06`) and #174
  closed unmerged. What remains is the `armed_idle` rows of the `timers` group, 5.6 to 5.9% slower
  than the plain cursor at one local position with the table inline instead of boxed.
- **The receive optimization.** `main` plus the inlined recv future is faster than `main` on every
  row but N2 `fan_out/workers/16` (+9.0%). It is a core change with its own tradeoff and stays
  experimental until that row is decided; it is not part of the timer decision.
- **Placement.** One validated placement moved nothing; a second one with a different shift would
  widen that, if a later result rests on it.

## How to measure

- **Rebuilding a variant:** each variant is a patch in [`patches/`](patches) against its parent
  design: `cursor.patch` and `inline-gate.patch` against #174's head `283e5e7` (kept by GitHub as
  `refs/pull/174/head`), the `cursor-*.patch` files against the cursor, and `inline-recv.patch`
  against `39aec83`. For all but `inline-recv.patch`, `git worktree add --detach <dir> 283e5e7`,
  then `git apply --index` the patches in order and `git reset --soft 39aec83` before committing.
  For `inline-recv.patch`, `git worktree add --detach <dir> 39aec83`, then `git apply --index` it
  and commit. Either gives one commit on `39aec83` whose tree is that of the build named here, as
  the sweep requires. A patch reproduces the tree byte for byte, not the commit ID.
- **On GitHub's runners:** push the variant as a branch (the workflow fetches every branch and
  every pull request head), then `gh workflow run stack-sweep.yml -f base=<full parent ID> -f
  candidate=<full commit ID>`. The candidate must be exactly one commit on top of the base. A run
  takes about 25 minutes. Evaluate with `python3 -I .github/stack-sweep/report.py <run ID>`; for
  borderline rows, run the workflow again and pass both run IDs. Only borderline rows are decided
  over both runs.
- **Locally on aarch64:** build the shift library and the probe as the workflow does, check the
  probe with `.github/stack-sweep/probe.sh`, and run `.github/stack-sweep/sweep.py run`. The
  binaries go before `--rows` and `--shifts`, since both take a variable number of values. A full
  messaging sweep is 2304 runs.
- **A confirmation run.** Added on 2026-10-27, before the first such run (opt-in timers), and not
  shaped by its result. The rule now lives in the
  [stack sweep protocol](../../../.github/stack-sweep/README.md#a-confirmation-run).
- **x86_64 assembly from aarch64:** `rustup target add --toolchain 1.99.0 x86_64-unknown-linux-gnu`
  is installed, as are `gcc-x86-64-linux-gnu` and `gdb`. Build with
  `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc` and
  `CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc`, then `x86_64-linux-gnu-objdump -d -C`.
- **Comparing two builds' code:** compare `.text`, `.rodata`, `.eh_frame` and `.gcc_except_table`
  per section with `objcopy -O binary --only-section`. Whole files differ in line numbers, symbol
  names and the build ID even when the code is identical.
- **Comparing two builds' functions:** `nm -C <binary> | grep 'recv::{closure'` lists the recv
  futures left out of line. `.github/stack-sweep/fncounts.py A B` lists the functions of tellus,
  flume and the bench whose instruction count differs between two bench binaries, `fndiff.py A B
  <regex>` diffs the disassembly of the matching functions with addresses stripped, and `fnmnem.py
  A B <regex>` compares their mnemonic histograms. All three read `OBJDUMP` for a cross objdump.

## Pitfalls

- A single stack position decides nothing: unrelated changes move rows by tens of percent. That
  includes the PR comparison in `bench.yml`, where the head also always runs second.
- `/tmp` is a 12 GB RAM tmpfs. Keep Cargo output under `target/` on `/`, and delete extra target
  directories afterward.
- Worktrees must not share a `CARGO_TARGET_DIR`. Cargo reuses an output whose sources look older
  than it, so a second worktree's bench came out byte-identical to the first one's.
- `39aec83` predates `.github/stack-sweep/`; take the tools from `main` for a local sweep.
- rustfmt does not format inside `macro_rules!`, so check a macro's line length by hand.
- Running the Python scripts writes `__pycache__`; it is ignored, but set
  `PYTHONDONTWRITEBYTECODE=1` anyway.
- `gh run download` fails now and then with a TLS timeout; retry.
- LLVM removes a local wrapper and its alignment unless its address escapes; only
  `hint::black_box` kept an aligned slot in place. A change in one function can also change the
  frame of an unrelated generic function, as A did to `flume::Sender::send` on x86_64.
- `#[inline(always)]` on an `async fn` applies to the function returning the future, not to the
  future's `poll`. A hand-written future carries the attribute on `poll`.
- A condition that folds to `false` only after inlining does not remove an `await` behind it: the
  future's resume switch still reaches it. Remove the code from the source instead.
- An unreferenced padding section is discarded by `--gc-sections`; flag it `R` (retain). Code
  added to the bench crate, even `global_asm!`, can change inlining elsewhere in the crate.
- objdump's symbol annotations contain nested `<>`; strip from the first `<` to the end of the
  line, or functions differ that do not.
- `just all` writes tens of GB under `target/`; the caches of two checkouts filled the 118 GB disk
  in the middle of a run. Check `df` before a gate run in a second worktree.
- `perf record --call-graph dwarf` is rejected on the N2 runner for the software clocks and
  accepted for `cycles`; frame pointer chains work with either event.
- Identical code folding gives a merged function one of its names: the variant's `Countdown` run
  loop shows up as `PollFn<spawn<IdleTimeout>::{closure#5}>`, and a family pattern `Idle` meant for
  tokio's idle set matched that actor and inflated an inclusive total until it was anchored.
- `target/` is not durable: it was removed between two sessions, and local copies of the runs'
  artifacts went with it. Keep downloaded artifacts outside `target/`; GitHub keeps them for 90
  days.
- `git reset --soft` followed by `git commit` commits the index only. An unstaged change stays
  out, and the sweep then measures the wrong commit; check `git diff --stat` against the previous
  commit before pushing.
- Writing the same code inside a block expression changed the generated code. When a variant must
  match a reference, keep the statements identical and gate each one.
