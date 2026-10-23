# Actor ID alignment

`ActorId` is `#[repr(align(16))]`. This page says why and what it costs; the
[report](report.md) describes how we got there.

## The problem

`ActorId` wraps a `Uuid`, which is `[u8; 16]` with alignment 1. A mailbox item is an `Incoming<M>`,
which holds either a message or an `ActorId`, so `Incoming<()>` was 17 bytes with alignment 1. On
the receive path the compiler copies the ID through stack temporaries with unaligned 16-byte loads,
for example `ldur q0, [sp, #13]` in `Mailbox::recv`. When such a load crosses a 4 KB page boundary,
it is slow on aarch64.

Whether it crosses depends on where the worker thread's stack lies, and that is fixed for a given
binary. So a build either pays the penalty on every message or never does. In our measurements, a
build stuck in that state was 30 to 52% slower in the affected benchmarks. An unrelated change
can flip a build into or out of it: PR #174 (timers) looked 55% slower on `queued` and 24% faster
on `flood` for exactly this reason.

## The change

One attribute on `ActorId`. The derives, the API and the wire format stay as they are, and the
cluster's pinned wire-byte tests pass unchanged. Every type holding an `ActorId` inherits
alignment 16. On the receive path we inspected, every 16-byte copy of an `Incoming` or `ActorId`
is then aligned, and the stable 30 to 52% outliers disappear.

Aligning `Incoming` instead, which would leave `ActorRef` and the other types alone, does not
work. The ID stays at offset 1 inside the enum, behind its tag, and is still copied unaligned.

## Wins and costs

Measured on aarch64 against `main` 39aec83, as the median over 16 stack positions and 3
repetitions. A negative change means faster.

| Effect | Measured |
|---|---|
| Builds stuck with the penalty | 30 to 52% slower before, not seen after |
| `flood/bounded`, `payload_flood/u64`, `payload_queued/u64/4`, `payload_queued/u64x6/4` | -2.2% to -4.9% |
| Other messaging rows | within ±1% |
| `queued/every/4`, `queued/every/16` (mailbox depth read on every message) | +1.7%, +2.8% |
| Backlog of 1,000,000 items, `()` / `u64` / `[u64; 6]` | 17.1 to 33.7 MB / 25.3 to 33.7 MB / 58.9 to 67.1 MB |
| `ActorRef`, `ActorContext` | +8 bytes each |
| Several cluster types | +8 to +32 bytes |

A queued item grows by at most 15 bytes of padding. How much a given message type pays depends on
its size and alignment; the table shows only the three payloads we measured.

## What it does not fix

Both builds still show a run-to-run variation in some stack positions: a run is either fast or
about 25% slower. It is not caused by the starting stack positions or by CPU placement, and we
could not explain it. We had required, before measuring, that no stack position stay more than
15% slower than the median; the change fails that requirement because of this variation.
Accepting the change is therefore a deliberate decision based on the narrower benefit: it removes
the demonstrated penalty.

Everything was measured on aarch64 only; x86_64 is untested.
