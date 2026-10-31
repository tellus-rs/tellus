//! Messaging throughput benchmarks, reported as messages per second:
//!
//! - `flood`: the bench thread floods a single counting actor, with an unbounded mailbox and with a
//!   bounded one whose capacity is large enough that no message is ever dropped.
//! - `ping_pong`: pairs of actors play ping-pong, one pair and one pair per worker thread.
//! - `fan_out`: the root actor sends messages round-robin to its workers, one worker and four
//!   workers per worker thread.
//! - `queued`: concurrent sender tasks, one and four per worker thread, flood a single counting
//!   actor which reads its own mailbox depth on every message or never.
//! - `timers`: concurrent sender tasks, one and four per worker thread, flood a single counting
//!   actor which restarts an idle timer on every message, starts one in `init` and leaves it
//!   pending, or starts none.
//!
//! All benchmarks run on a Tokio runtime with a fixed worker thread count, which also sizes the
//! workloads, so measurements do not depend on the host's core count.

use hotpath::criterion::{
    BenchmarkId, Config, Criterion, Throughput, criterion_group, criterion_main,
};
use std::{
    convert::Infallible,
    hint::black_box,
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Control, Incoming, MailboxCapacity,
    Timed, Untimed,
};
use tokio::runtime::{Builder, Runtime};

const WORKER_THREADS: usize = 4;
const FLOOD_MESSAGES: usize = 100_000;
const FLOOD_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(FLOOD_MESSAGES).expect("flood message count is not zero");
const PING_PONG_ROUNDS: usize = 1_000;
const FAN_OUT_MESSAGES: usize = 100_000;
const QUEUED_MESSAGES: usize = 100_000;
const TIMERS_MESSAGES: usize = 100_000;
const IDLE_TIMEOUT: Duration = Duration::from_secs(3600);

fn flood(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("flood");
    group.throughput(Throughput::Elements(FLOOD_MESSAGES as u64));

    let mailbox_capacities = [
        ("unbounded", MailboxCapacity::Unbounded),
        ("bounded", MailboxCapacity::Bounded(FLOOD_CAPACITY)),
    ];
    for (label, mailbox_capacity) in mailbox_capacities {
        group.bench_function(label, |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                measure(
                    iters,
                    || {
                        let actor = Countdown {
                            messages: FLOOD_MESSAGES,
                        };
                        let config = ActorConfig::default().with_mailbox_capacity(mailbox_capacity);
                        (actor, config)
                    },
                    |root| {
                        for _ in 0..FLOOD_MESSAGES {
                            root.tell(Tick);
                        }
                    },
                )
                .await
            });
        });
    }

    group.finish();
}

fn ping_pong(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("ping_pong");

    for pairs in [1, WORKER_THREADS] {
        group.throughput(Throughput::Elements((pairs * PING_PONG_ROUNDS * 2) as u64));

        group.bench_function(BenchmarkId::new("pairs", pairs), |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                measure(
                    iters,
                    || {
                        let actor = PingPongRoot {
                            pairs,
                            rounds: PING_PONG_ROUNDS,
                        };
                        (actor, ActorConfig::default())
                    },
                    |root| root.tell(Go),
                )
                .await
            });
        });
    }

    group.finish();
}

fn fan_out(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("fan_out");

    for workers in [WORKER_THREADS, 4 * WORKER_THREADS] {
        let share = FAN_OUT_MESSAGES / workers;
        group.throughput(Throughput::Elements((share * workers) as u64));

        group.bench_function(BenchmarkId::new("workers", workers), |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                measure(
                    iters,
                    || (FanOutRoot { workers, share }, ActorConfig::default()),
                    |root| root.tell(Go),
                )
                .await
            });
        });
    }

    group.finish();
}

fn queued(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("queued");

    for senders in [WORKER_THREADS, 4 * WORKER_THREADS] {
        let share = QUEUED_MESSAGES / senders;
        group.throughput(Throughput::Elements((share * senders) as u64));

        for (label, probe) in [("never", Probe::Never), ("every", Probe::Every)] {
            group.bench_function(BenchmarkId::new(label, senders), |b| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    measure(
                        iters,
                        || {
                            let actor = Probing {
                                messages: share * senders,
                                probe,
                            };
                            (actor, ActorConfig::default())
                        },
                        |root| {
                            for _ in 0..senders {
                                let root = root.clone();
                                tokio::spawn(async move {
                                    for _ in 0..share {
                                        root.tell(Tick);
                                    }
                                });
                            }
                        },
                    )
                    .await
                });
            });
        }
    }

    group.finish();
}

fn timers(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("timers");

    for senders in [WORKER_THREADS, 4 * WORKER_THREADS] {
        let share = TIMERS_MESSAGES / senders;
        group.throughput(Throughput::Elements((share * senders) as u64));

        let idle_timers = [
            ("none", IdleTimer::None),
            ("armed_idle", IdleTimer::ArmedIdle),
            ("restart_each", IdleTimer::RestartEach),
        ];
        for (label, idle_timer) in idle_timers {
            group.bench_function(BenchmarkId::new(label, senders), |b| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    measure(
                        iters,
                        || {
                            let actor = IdleTimeout {
                                messages: share * senders,
                                idle_timer,
                            };
                            (actor, ActorConfig::default())
                        },
                        |root| {
                            for _ in 0..senders {
                                let root = root.clone();
                                tokio::spawn(async move {
                                    for _ in 0..share {
                                        root.tell(Tick);
                                    }
                                });
                            }
                        },
                    )
                    .await
                });
            });
        }
    }

    group.finish();
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .enable_all()
        .build()
        .expect("tokio runtime can be created")
}

async fn measure<A, F, D>(iters: u64, make: F, drive: D) -> Duration
where
    A: Actor + Send + 'static,
    A::Message: Send + 'static,
    A::State: Send + 'static,
    F: Fn() -> (A, ActorConfig),
    D: Fn(&ActorRef<A::Message>),
{
    let mut elapsed = Duration::ZERO;

    for _ in 0..iters {
        let (actor, config) = make();
        let system = ActorSystem::with_config(actor, config);

        let start = Instant::now();
        drive(system.root());
        system
            .terminated()
            .await
            .expect("awaiting actor system termination");
        elapsed += start.elapsed();
    }

    elapsed
}

fn next_remaining(remaining: usize) -> Option<usize> {
    remaining.checked_sub(1).filter(|n| *n > 0)
}

fn next_control(remaining: usize) -> Control<usize> {
    next_remaining(remaining).map_or(Control::Stop, Control::Continue)
}

struct Tick;

struct Countdown {
    messages: usize,
}

impl Actor for Countdown {
    type Message = Tick;
    type Timers = Untimed;
    type State = usize;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.messages)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(next_control(remaining))
    }
}

/// Whether the actor reads its own mailbox depth on every message.
#[derive(Clone, Copy)]
enum Probe {
    Never,
    Every,
}

/// A countdown which reads its mailbox depth on every message if its probe says so.
struct Probing {
    messages: usize,
    probe: Probe,
}

impl Actor for Probing {
    type Message = Tick;
    type Timers = Untimed;
    type State = usize;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.messages)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match self.probe {
            Probe::Every => {
                black_box(context.queued());
            }

            Probe::Never => {}
        }
        Ok(next_control(remaining))
    }
}

/// Whether the actor starts no idle timer, starts one in `init` and leaves it pending, or restarts
/// one on every message.
#[derive(Clone, Copy)]
enum IdleTimer {
    None,
    ArmedIdle,
    RestartEach,
}

/// A countdown which starts an idle timer as its `idle_timer` says. The timer's delay outlasts the
/// run, so it never fires.
struct IdleTimeout {
    messages: usize,
    idle_timer: IdleTimer,
}

impl Actor for IdleTimeout {
    type Message = Tick;
    type Timers = Timed;
    type State = usize;
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        match self.idle_timer {
            IdleTimer::ArmedIdle => context.start_timer("idle", Tick, IDLE_TIMEOUT),

            IdleTimer::None | IdleTimer::RestartEach => {}
        }
        Ok(self.messages)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        _: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match self.idle_timer {
            IdleTimer::RestartEach => context.start_timer("idle", Tick, IDLE_TIMEOUT),

            IdleTimer::None | IdleTimer::ArmedIdle => {}
        }
        Ok(next_control(remaining))
    }
}

struct Go;

struct PingPongRoot {
    pairs: usize,
    rounds: usize,
}

impl Actor for PingPongRoot {
    type Message = Go;
    type Timers = Untimed;
    type State = usize;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.pairs)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        remaining: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Go) => {
                for _ in 0..self.pairs {
                    let pinger = context.spawn(Pinger {
                        rounds: self.rounds,
                    });
                    context.watch(&pinger);
                }

                Ok(Control::Continue(remaining))
            }

            Incoming::Terminated(_) => Ok(next_control(remaining)),
        }
    }
}

struct Pong;

struct Pinger {
    rounds: usize,
}

impl Actor for Pinger {
    type Message = Pong;
    type Timers = Untimed;
    type State = Pinging;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let ponger = context.spawn(Ponger {
            pinger: context.self_ref().clone(),
        });
        ponger.tell(Ping);

        Ok(Pinging {
            ponger,
            remaining: self.rounds,
        })
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match next_remaining(state.remaining) {
            Some(remaining) => {
                state.ponger.tell(Ping);
                Ok(Control::Continue(Pinging { remaining, ..state }))
            }

            None => Ok(Control::Stop),
        }
    }
}

struct Pinging {
    ponger: ActorRef<Ping>,
    remaining: usize,
}

struct Ping;

struct Ponger {
    pinger: ActorRef<Pong>,
}

impl Actor for Ponger {
    type Message = Ping;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        self.pinger.tell(Pong);

        Ok(Control::Continue(()))
    }
}

struct FanOutRoot {
    workers: usize,
    share: usize,
}

impl Actor for FanOutRoot {
    type Message = Go;
    type Timers = Untimed;
    type State = FanningOut;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let workers = (0..self.workers)
            .map(|_| {
                let worker = context.spawn(Countdown {
                    messages: self.share,
                });
                context.watch(&worker);
                worker
            })
            .collect::<Vec<_>>();

        Ok(FanningOut {
            workers,
            remaining: self.workers,
        })
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Go) => {
                for worker in state
                    .workers
                    .iter()
                    .cycle()
                    .take(self.share * state.workers.len())
                {
                    worker.tell(Tick);
                }

                Ok(Control::Continue(state))
            }

            Incoming::Terminated(_) => Ok(next_remaining(state.remaining)
                .map_or(Control::Stop, |remaining| {
                    Control::Continue(FanningOut { remaining, ..state })
                })),
        }
    }
}

struct FanningOut {
    workers: Vec<ActorRef<Tick>>,
    remaining: usize,
}

criterion_group!(
    name = benches;
    config = Config::default()
        .sample_size(30)
        .noise_threshold(0.05)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10));
    targets = flood, ping_pong, fan_out, queued, timers
);
criterion_main!(benches);
