# Measuring a performance change

Three tools judge a change to code the benchmarks run, and one rule says where a feature's
benchmark lands. Only the stack sweep decides.

1. **The stack sweep is the performance gate.** It runs a bench of the candidate and its parent at
   16 worker-stack positions on two runner types, and the rules below turn the figures into a
   verdict per row.
2. **The codegen comparison is a diagnostic.** It shows which functions changed and helps decide
   what to measure. It never certifies "no regression": layout affects caches and branch
   prediction, linker veneers add executed instructions, and no list of functions covers everything
   a timed window runs. Skipping the sweep is a maintainer's decision, recorded in the pull request
   with its reason.
3. **The per-PR comparison is a hint, never a verdict.** `bench.yml` measures one stack position,
   and the head always runs second. On identical code, `flood/bounded` measured anywhere from
   -35.7% to +19.9% there.
4. **A feature's benchmark lands on `main` before the feature,** when it compiles against `main`'s
   API. Otherwise the feature's pull request names a comparable baseline workload on `main` or a
   separate feature-cost comparison, as #185 compared its `timers` group against the plain cursor.

The rules change only in a commit of their own, never while a case is being decided.

## The stack sweep

### Running it

- **On a pull request:** add the label `sweep` for the messaging bench or `sweep-persistence` for
  the persistence bench. The candidate is the pull request's head and the base its parent; CI
  already makes a pull request one commit. To run it again, remove the label and add it again.
- **By hand:** `gh workflow run stack-sweep.yml -f base=<full parent ID> -f candidate=<full commit
  ID> [-f bench=persistence]`. The candidate must be exactly one commit on top of the base and
  pushed, as a branch or a pull request head. `--ref <branch>` takes the workflow and these scripts
  from that branch, the code from the two commits.
- **The report:** the `Report` job runs `report.py` once every sweep job has succeeded and writes it
  to the run's summary. `python3 -I .github/stack-sweep/report.py <run ID>` does the same locally.
- **Locally on aarch64:** build the shift library and the probe as the workflow does, check the
  probe with `probe.sh`, and run `sweep.py run --bench <bench>`. The binaries go before `--rows`
  and `--shifts`, since both take a variable number of values.

A messaging run sweeps every row of the messaging bench, the groups `flood`, `ping_pong`, `fan_out`,
`queued` and `timers`: 2304 bench runs per job, 16 rows. A persistence run sweeps every row of the
persistence bench, the groups `persist` and `recover`: 576 runs, 4 rows.

### The protocol

- **Runners:** 4 jobs on `ubuntu-latest` (x86_64) and 4 on `ubuntu-24.04-arm` (Neoverse-N2). Each
  job builds both commits with the pinned toolchain and `--locked`, and logs the CPU model and the
  binary hashes.
- **Binaries:** base, candidate, and a copy of the base as an A/A control.
- **Stack positions:** 16 worker-stack shifts, 0 to 3840 in steps of 256, through
  `stack_shift.c`.
  - The probe checks first that the shift moves a new thread's stack by exactly that amount.
  - Every bench run must report `stack_shift=<shift>` and `stack_shift_threads` of at least the
    bench's 4 Tokio workers, so the bench's own threads went through the shift.
- **Schedule:** 3 repetitions. Within each, the shifts ascend, the rows follow in a fixed order,
  and the binary order rotates by one per repetition.
- **Job figure:** per row, the median of the candidate's 48 runs over the median of the base's,
  minus 1. The A/A figure is the same for the copy.
- **Exclusion:** a job's row is excluded if its A/A figure is off by more than 3%.
- **Verdict per row:** inconclusive if fewer than 3 of the 4 jobs remain, or if the retained
  figures spread by more than 6 points. Otherwise the median decides: below +3% passes, above +5%
  fails, and in between the row is borderline.
- **Precedence:** inconclusive always wins over the median.
- **Scope:** the figure is the median across the 16 sampled positions. The grid can miss narrow
  penalties or hit them systematically, so it says nothing about robustness.

`report.py` applies these rules, and `test_report.py` pins them.

### A borderline row

The workflow runs again with the same base and candidate, and `report.py <first ID> <second ID>`
decides the borderline rows over all 8 jobs. At least 6 must be retained and their figures must
spread by at most 6 points, else the row is inconclusive. Otherwise a median of at most +3% passes
and anything above fails. Every other row keeps its verdict from the first run.

### A confirmation run

When a sweep has no failing and no borderline row and only inconclusive rows prevent acceptance,
one independent repeat of the whole sweep is run with the same base and candidate code.

- Both runs are evaluated whole, `report.py <run ID>` with one ID each, and both reports are kept.
- The candidate is accepted when every row passes in at least one run and no row fails or is
  borderline in either.
- Rows are not pooled across the runs, so the borderline rule is not used, and the limits are
  unchanged.
- The CPU models of both runs are reported.

### Decided in advance

- **A lost job:** a job lost to a runner fault is rerun within its run, with
  `gh run rerun <run ID> --failed`, which reruns the report as well. It is not an extra run.
- **A row inconclusive in both runs** leaves the candidate pending. Any exception is the
  maintainer's decision, recorded in the pull request with its reason.
- **CPU models:** the verdict does not depend on them. The report lists each row by CPU model, and
  a reading of a row names the models a run did not draw.

## The null mode

`gh workflow run stack-sweep.yml -f base=<full commit ID> -f null_run=true [-f bench=persistence]`
builds only the base and copies its executable into all three slots. The build step checks that
B, C and A have one hash. The report names the run `<base>-null`. A null run measures the protocol's
own noise: a verdict other than pass is a false alarm by construction.

A sweep of a commit which changes no bench code can build two identical executables, as the label
runs of #188 did, but only the null mode asserts it.

## The codegen comparison

`codegen.sh [--bench messaging|persistence] <base> <candidate> [--rename REGEX=REPLACEMENT]...`
builds the bench of both commits with v0 symbol mangling, each in its own worktree and target
directory under `target/`. It then runs:

- `fncounts.py`: per-function instruction counts of tellus, flume and the bench, with the functions
  only in one binary and the name collisions;
- `fndiff.py`: the diff of each function on a per-message path kept in the script, one list per
  bench. Every pattern must match in both binaries, and a collision among the matches fails it.

`TARGET=x86_64-unknown-linux-gnu` cross-builds from aarch64 with the x86_64 GNU toolchain
(`gcc-x86-64-linux-gnu` and the target installed for the pinned toolchain).

`disasm.py` makes the instructions comparable:

- **Names:** `.llvm.N` and legacy hash suffixes are removed. A `--rename` applies to the candidate
  only, split at the last `=`. #185, for example, needs `--rename '_untimed\b='` and
  `--rename ', tellus::timers::Untimed>=>'`.
- **References:** a call, branch, `%rip`-relative operand or aarch64 `adrp` pair becomes the symbol
  it points into plus the offset, `@symbol+offset`. Of several names at one address, from identical
  code folding, the smallest is taken, since the set of aliases differs between builds. A GOT slot
  outside every symbol becomes the target of its relocation, a PLT stub its name.
- **Unresolved references:** an address in no symbol (such as a string literal in `.rodata`), in
  an `anon.<hash>` constant, whose hash differs between builds, or an `adrp` without a completing
  instruction becomes its full address, marked `?`. Equality never rests on it.

`fndiff.py` classifies each function:

- **identical:** equal instructions and equal resolved references, with no unresolved reference on
  either side;
- **unresolved only:** equal except for operands marked `?`, equal or not, which may or may not be
  layout;
- **differing:** anything else, with the mnemonic and call target deltas and the lines which
  differ beyond unresolved operands.

`test_codegen.py` pins these rules on canned objdump output.

## Calibration

Three null runs of `main` (`ac59a98`) per bench, plus the label runs of #188 (`b49a9f5` against
`ac59a98`), whose base and candidate executables had one hash and which therefore count as null
runs, too. They predate the `timers` group, so the messaging figures cover the other 10 rows and
none of the 6 `timers` rows. Every bench run reported `stack_shift_threads` of at least 4. Treating
a row differently because of these data is a separate decision, which names either replacement
coverage or the coverage it gives up.

### Messaging

Runs 40554618267, 40554629237, 40554639973 and the label run 40554253825.

| Runner | Pass | Inconclusive | Fail or borderline |
|---|---|---|---|
| `ubuntu-24.04-arm` | 40 of 40 | 0 | 0 |
| `ubuntu-latest` | 36 of 40 | 4 | 0 |

- **Inconclusive, all on `ubuntu-latest`:** `flood/unbounded` twice, with too few jobs (40554629237,
  40554253825), and `ping_pong/pairs/1` twice, with a spread of 9.8 points (40554629237) and with
  too few jobs (40554639973).
- **A/A exclusions, out of 16 jobs per row:** on `ubuntu-latest`, `flood/unbounded` 7,
  `ping_pong/pairs/1` 2 and `fan_out/workers/4` 1; on `ubuntu-24.04-arm`, `fan_out/workers/4` 2.
- **Largest median of a passing row:** +2.5% for `flood/bounded` on `ubuntu-24.04-arm`
  (40554629237). On `ubuntu-latest` it is -1.8% for `flood/unbounded` (40554639973).
- **CPU models:** `ubuntu-latest` drew AMD EPYC 7763 in 7 jobs, AMD EPYC 9V45 in 4, Intel Xeon
  Platinum 8573C in 4 and AMD EPYC 9V74 in 1. `ubuntu-24.04-arm` drew Neoverse-N2 in all 16.

### Persistence

Runs 40554650519, 40554660631, 40554670498 and the label run 40554252733.

| Runner | Pass | Inconclusive | Fail or borderline |
|---|---|---|---|
| `ubuntu-24.04-arm` | 16 of 16 | 0 | 0 |
| `ubuntu-latest` | 14 of 16 | 2 | 0 |

- **Inconclusive, both on `ubuntu-latest`:** `persist/snapshots` twice, with too few jobs
  (40554670498, 40554252733).
- **A/A exclusions, out of 16 jobs per row:** on `ubuntu-latest`, `persist/snapshots` 5 and
  `persist/no_snapshots` 2; none on `ubuntu-24.04-arm`.
- **Largest median of a passing row:** +2.2% for `persist/no_snapshots` on `ubuntu-latest`
  (40554252733). On `ubuntu-24.04-arm` no median exceeds 0.5% either way.
- **CPU models:** `ubuntu-latest` drew AMD EPYC 7763 in 9 jobs, AMD EPYC 9V74 in 5, AMD EPYC 9V45 in
  1 and Intel Xeon 6973P-C in 1. `ubuntu-24.04-arm` drew Neoverse-N2 in all 16.

No null run failed a row or left one borderline. Every inconclusive verdict fell on `ubuntu-latest`,
in the rows which also lost the most jobs to A/A exclusion.
