//! Cross-framework messaging benchmarks, reported as messages per second:
//!
//! - `flood`: the bench thread floods a single counting actor.
//! - `ping_pong`: pairs of actors play ping-pong, one pair and eight pairs.
//! - `fan_out`: the bench thread sends messages round-robin to eight and to 32 workers.
//!
//! Every framework is configured with an unbounded mailbox and driven through its non-blocking
//! fire-and-forget send, so all of them perform the same work. Spawning, up to the end of every
//! actor's startup hook, happens outside the measured region; the timer covers sending plus
//! awaiting termination.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use std::{
    env,
    fs::{self, File},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::{Duration, Instant, SystemTime},
};
use tokio::runtime::{Builder, Runtime};

const FLOOD_MESSAGES: usize = 100_000;
const PING_PONG_ROUNDS: usize = 1_000;
const FAN_OUT_MESSAGES: usize = 100_000;
const PAIRS: [usize; 2] = [1, 8];
const WORKERS: [usize; 2] = [8, 32];

// Must match the report's `RUN_MARKER`, whose modification time starts the latest run!
const RUN_MARKER: &str = "run-started";

// Boxed once per criterion sample, far outside the measured loop!
type BoxedBench = Pin<Box<dyn Future<Output = Duration> + Send>>;

type PlainBench = (&'static str, fn(u64) -> BoxedBench);

type ParameterizedBench = (&'static str, fn(u64, usize) -> BoxedBench);

const FLOOD_BENCHES: [PlainBench; 3] = [
    ("tellus", |iters| Box::pin(tellus_bench::flood(iters))),
    ("kameo", |iters| Box::pin(kameo_bench::flood(iters))),
    ("ractor", |iters| Box::pin(ractor_bench::flood(iters))),
];

const PING_PONG_BENCHES: [ParameterizedBench; 3] = [
    ("tellus", |iters, pairs| {
        Box::pin(tellus_bench::ping_pong(iters, pairs))
    }),
    ("kameo", |iters, pairs| {
        Box::pin(kameo_bench::ping_pong(iters, pairs))
    }),
    ("ractor", |iters, pairs| {
        Box::pin(ractor_bench::ping_pong(iters, pairs))
    }),
];

const FAN_OUT_BENCHES: [ParameterizedBench; 3] = [
    ("tellus", |iters, workers| {
        Box::pin(tellus_bench::fan_out(iters, workers))
    }),
    ("kameo", |iters, workers| {
        Box::pin(kameo_bench::fan_out(iters, workers))
    }),
    ("ractor", |iters, workers| {
        Box::pin(ractor_bench::fan_out(iters, workers))
    }),
];

fn main() {
    if is_benchmark_run() {
        mark_run_start();
    }

    benches();

    Criterion::default().configure_from_args().final_summary();
}

// Must agree with `Criterion::configure_from_args` on which arguments measure!
fn is_benchmark_run() -> bool {
    let args = env::args().skip(1).collect::<Vec<_>>();
    args.iter().any(|arg| arg == "--bench")
        && !args
            .iter()
            .any(|arg| arg == "--test" || arg == "--list" || arg.starts_with("--profile-time"))
}

fn mark_run_start() {
    let directory = workspace_output_directory();
    fs::create_dir_all(&directory).expect("criterion output directory is created");
    File::create(directory.join(RUN_MARKER))
        .and_then(|marker| marker.set_modified(SystemTime::now()))
        .expect("run marker is written");
}

fn workspace_output_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/criterion-comparison")
}

fn flood(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("flood");
    group.throughput(Throughput::Elements(FLOOD_MESSAGES as u64));

    for (name, bench) in FLOOD_BENCHES {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter_custom(bench);
        });
    }

    group.finish();
}

fn ping_pong(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("ping_pong");

    for pairs in PAIRS {
        group.throughput(Throughput::Elements((pairs * PING_PONG_ROUNDS * 2) as u64));

        for (name, bench) in PING_PONG_BENCHES {
            group.bench_function(BenchmarkId::new(name, pairs), |b| {
                b.to_async(&rt)
                    .iter_custom(move |iters| bench(iters, pairs));
            });
        }
    }

    group.finish();
}

fn fan_out(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("fan_out");

    for workers in WORKERS {
        let share = fan_out_share(workers);
        group.throughput(Throughput::Elements((share * workers) as u64));

        for (name, bench) in FAN_OUT_BENCHES {
            group.bench_function(BenchmarkId::new(name, workers), |b| {
                b.to_async(&rt)
                    .iter_custom(move |iters| bench(iters, workers));
            });
        }
    }

    group.finish();
}

// Every framework is timed through this helper, so the timing boundaries are identical!
async fn measure<S, SFut, T, R, RFut>(iters: u64, setup: S, run: R) -> Duration
where
    S: Fn() -> SFut,
    SFut: Future<Output = T>,
    R: Fn(T) -> RFut,
    RFut: Future<Output = ()>,
{
    let mut elapsed = Duration::ZERO;

    for _ in 0..iters {
        let state = setup().await;

        let start = Instant::now();
        run(state).await;
        elapsed += start.elapsed();
    }

    elapsed
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime can be created")
}

// The reported throughput and the per actor countdown must both derive from here!
const fn fan_out_share(workers: usize) -> usize {
    FAN_OUT_MESSAGES / workers
}

// Every framework counts down through this function, so all stop after the same count!
const fn count_down(remaining: usize) -> Option<usize> {
    match remaining.checked_sub(1) {
        Some(remaining) if remaining > 0 => Some(remaining),
        _ => None,
    }
}

mod tellus_bench {
    use crate::{FLOOD_MESSAGES, PING_PONG_ROUNDS, count_down, fan_out_share, measure};
    use std::time::Duration;
    use tellus::{
        Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, Unstashed, Untimed,
    };
    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

    pub async fn flood(iters: u64) -> Duration {
        measure(
            iters,
            || async {
                let (started, startups) = mpsc::unbounded_channel();
                let system = ActorSystem::new(Countdown {
                    messages: FLOOD_MESSAGES,
                    started,
                });
                let root = system.root().clone();
                await_startups(startups, 1).await;
                (system, root)
            },
            |(system, root)| async move {
                for _ in 0..FLOOD_MESSAGES {
                    root.tell(Tick);
                }
                system.terminated().await.expect("tellus flood terminates");
            },
        )
        .await
    }

    pub async fn ping_pong(iters: u64, pairs: usize) -> Duration {
        measure(
            iters,
            || async {
                let (started, startups) = mpsc::unbounded_channel();
                let systems = (0..pairs)
                    .map(|_| {
                        ActorSystem::new(Pinger {
                            rounds: PING_PONG_ROUNDS,
                            started: started.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                await_startups(startups, 2 * pairs).await;
                systems
            },
            |systems| async move {
                for system in &systems {
                    system.root().tell(PingerMessage::Go);
                }
                for system in systems {
                    system
                        .terminated()
                        .await
                        .expect("tellus ping_pong terminates");
                }
            },
        )
        .await
    }

    pub async fn fan_out(iters: u64, workers: usize) -> Duration {
        let share = fan_out_share(workers);
        measure(
            iters,
            || async {
                let (started, startups) = mpsc::unbounded_channel();
                let systems = (0..workers)
                    .map(|_| {
                        ActorSystem::new(Countdown {
                            messages: share,
                            started: started.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                let roots = systems
                    .iter()
                    .map(|system| system.root().clone())
                    .collect::<Vec<_>>();
                await_startups(startups, workers).await;
                (systems, roots)
            },
            |(systems, roots)| async move {
                for root in roots.iter().cycle().take(share * workers) {
                    root.tell(Tick);
                }
                for system in systems {
                    system
                        .terminated()
                        .await
                        .expect("tellus fan_out terminates");
                }
            },
        )
        .await
    }

    async fn await_startups(mut startups: UnboundedReceiver<()>, count: usize) {
        for _ in 0..count {
            startups.recv().await.expect("tellus actor starts");
        }
    }

    struct Tick;

    struct Countdown {
        messages: usize,
        started: UnboundedSender<()>,
    }

    impl Actor for Countdown {
        type Message = Tick;
        type Timers = Untimed;
        type Stash = Unstashed;
        type State = usize;
        type Error = !;

        fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
            self.started.send(()).expect("tellus startup is awaited");

            Ok(self.messages)
        }

        fn receive(
            &self,
            _: &ActorContext<Self::Message>,
            _: Incoming<Self::Message>,
            remaining: Self::State,
        ) -> Result<Control<Self::State>, Self::Error> {
            match count_down(remaining) {
                Some(remaining) => Ok(Control::Continue(remaining)),
                None => Ok(Control::Stop),
            }
        }
    }

    enum PingerMessage {
        Go,
        Pong,
    }

    struct Pinger {
        rounds: usize,
        started: UnboundedSender<()>,
    }

    impl Actor for Pinger {
        type Message = PingerMessage;
        type Timers = Untimed;
        type Stash = Unstashed;
        type State = Pinging;
        type Error = !;

        fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
            let ponger = context.spawn(Ponger {
                pinger: context.self_ref().clone(),
                started: self.started.clone(),
            });
            self.started.send(()).expect("tellus startup is awaited");

            Ok(Pinging {
                ponger,
                remaining: self.rounds,
            })
        }

        fn receive(
            &self,
            _: &ActorContext<Self::Message>,
            incoming: Incoming<Self::Message>,
            state: Self::State,
        ) -> Result<Control<Self::State>, Self::Error> {
            match incoming {
                Incoming::Message(PingerMessage::Go) => {
                    state.ponger.tell(Ping);
                    Ok(Control::Continue(state))
                }

                Incoming::Message(PingerMessage::Pong) => match count_down(state.remaining) {
                    Some(remaining) => {
                        state.ponger.tell(Ping);
                        Ok(Control::Continue(Pinging { remaining, ..state }))
                    }

                    None => Ok(Control::Stop),
                },

                Incoming::Terminated(_) => Ok(Control::Continue(state)),
            }
        }
    }

    struct Pinging {
        ponger: ActorRef<Ping>,
        remaining: usize,
    }

    struct Ping;

    struct Ponger {
        pinger: ActorRef<PingerMessage>,
        started: UnboundedSender<()>,
    }

    impl Actor for Ponger {
        type Message = Ping;
        type Timers = Untimed;
        type Stash = Unstashed;
        type State = ();
        type Error = !;

        fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
            self.started.send(()).expect("tellus startup is awaited");

            Ok(())
        }

        fn receive(
            &self,
            _: &ActorContext<Self::Message>,
            _: Incoming<Self::Message>,
            _: Self::State,
        ) -> Result<Control<Self::State>, Self::Error> {
            self.pinger.tell(PingerMessage::Pong);

            Ok(Control::Continue(()))
        }
    }
}

mod kameo_bench {
    use crate::{FLOOD_MESSAGES, PING_PONG_ROUNDS, count_down, fan_out_share, measure};
    use kameo::{
        Actor,
        actor::{ActorRef, Spawn, WeakActorRef},
        error::{ActorStopReason, Infallible},
        mailbox,
        message::{Context, Message},
    };
    use std::time::Duration;

    pub async fn flood(iters: u64) -> Duration {
        measure(
            iters,
            || spawn_countdown(FLOOD_MESSAGES),
            |actor_ref| async move {
                for _ in 0..FLOOD_MESSAGES {
                    actor_ref
                        .tell(Tick)
                        .try_send()
                        .expect("kameo flood message is sent");
                }
                actor_ref.wait_for_shutdown().await;
            },
        )
        .await
    }

    pub async fn ping_pong(iters: u64, pairs: usize) -> Duration {
        measure(
            iters,
            || async {
                let mut pingers = Vec::with_capacity(pairs);
                for _ in 0..pairs {
                    let pinger = Pinger::spawn_with_mailbox(PING_PONG_ROUNDS, mailbox::unbounded());
                    pinger.wait_for_startup().await;
                    pingers.push(pinger);
                }
                pingers
            },
            |pingers| async move {
                for pinger in &pingers {
                    pinger.tell(Go).try_send().expect("kameo ping_pong starts");
                }
                for pinger in pingers {
                    pinger.wait_for_shutdown().await;
                }
            },
        )
        .await
    }

    pub async fn fan_out(iters: u64, workers: usize) -> Duration {
        let share = fan_out_share(workers);
        measure(
            iters,
            || async {
                let mut actor_refs = Vec::with_capacity(workers);
                for _ in 0..workers {
                    actor_refs.push(spawn_countdown(share).await);
                }
                actor_refs
            },
            |actor_refs| async move {
                for actor_ref in actor_refs.iter().cycle().take(share * workers) {
                    actor_ref
                        .tell(Tick)
                        .try_send()
                        .expect("kameo fan_out message is sent");
                }
                for actor_ref in actor_refs {
                    actor_ref.wait_for_shutdown().await;
                }
            },
        )
        .await
    }

    async fn spawn_countdown(messages: usize) -> ActorRef<Countdown> {
        let actor_ref = Countdown::spawn_with_mailbox(messages, mailbox::unbounded());
        actor_ref.wait_for_startup().await;
        actor_ref
    }

    struct Tick;

    struct Countdown {
        remaining: usize,
    }

    impl Actor for Countdown {
        type Args = usize;
        type Error = Infallible;

        async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Countdown { remaining: args })
        }
    }

    impl Message<Tick> for Countdown {
        type Reply = ();

        async fn handle(&mut self, _: Tick, context: &mut Context<Self, Self::Reply>) {
            match count_down(self.remaining) {
                Some(remaining) => self.remaining = remaining,
                None => context.stop(),
            }
        }
    }

    struct Go;

    struct Pong;

    struct Pinger {
        ponger: ActorRef<Ponger>,
        remaining: usize,
    }

    impl Actor for Pinger {
        type Args = usize;
        type Error = Infallible;

        async fn on_start(
            args: Self::Args,
            actor_ref: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            let ponger = Ponger::spawn_with_mailbox(actor_ref, mailbox::unbounded());
            ponger.wait_for_startup().await;

            Ok(Pinger {
                ponger,
                remaining: args,
            })
        }

        // Must complete before wait_for_shutdown resolves, like tellus's child barrier does!
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            self.ponger
                .stop_gracefully()
                .await
                .expect("kameo ponger stop is sent");
            self.ponger.wait_for_shutdown().await;

            Ok(())
        }
    }

    impl Message<Go> for Pinger {
        type Reply = ();

        async fn handle(&mut self, _: Go, _: &mut Context<Self, Self::Reply>) {
            self.ponger
                .tell(Ping)
                .try_send()
                .expect("kameo ping is sent");
        }
    }

    impl Message<Pong> for Pinger {
        type Reply = ();

        async fn handle(&mut self, _: Pong, context: &mut Context<Self, Self::Reply>) {
            match count_down(self.remaining) {
                Some(remaining) => {
                    self.remaining = remaining;
                    self.ponger
                        .tell(Ping)
                        .try_send()
                        .expect("kameo ping is sent");
                }

                None => context.stop(),
            }
        }
    }

    struct Ping;

    struct Ponger {
        pinger: ActorRef<Pinger>,
    }

    impl Actor for Ponger {
        type Args = ActorRef<Pinger>;
        type Error = Infallible;

        async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Ponger { pinger: args })
        }
    }

    impl Message<Ping> for Ponger {
        type Reply = ();

        async fn handle(&mut self, _: Ping, _: &mut Context<Self, Self::Reply>) {
            self.pinger
                .tell(Pong)
                .try_send()
                .expect("kameo pong is sent");
        }
    }
}

mod ractor_bench {
    use crate::{FLOOD_MESSAGES, PING_PONG_ROUNDS, count_down, fan_out_share, measure};
    use ractor::{Actor, ActorProcessingErr, ActorRef};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    pub async fn flood(iters: u64) -> Duration {
        measure(
            iters,
            || async { spawn_countdown(FLOOD_MESSAGES).await },
            |(actor_ref, handle)| async move {
                for _ in 0..FLOOD_MESSAGES {
                    actor_ref
                        .send_message(Tick)
                        .expect("ractor flood message is sent");
                }
                handle.await.expect("ractor flood terminates");
            },
        )
        .await
    }

    pub async fn ping_pong(iters: u64, pairs: usize) -> Duration {
        measure(
            iters,
            || async {
                let mut pingers = Vec::with_capacity(pairs);
                for _ in 0..pairs {
                    pingers.push(
                        Actor::spawn(None, Pinger, PING_PONG_ROUNDS)
                            .await
                            .expect("ractor pinger is spawned"),
                    );
                }
                pingers
            },
            |pingers| async move {
                for (pinger, _) in &pingers {
                    pinger
                        .send_message(PingerMessage::Go)
                        .expect("ractor ping_pong starts");
                }
                for (_, handle) in pingers {
                    handle.await.expect("ractor ping_pong terminates");
                }
            },
        )
        .await
    }

    pub async fn fan_out(iters: u64, workers: usize) -> Duration {
        let share = fan_out_share(workers);
        measure(
            iters,
            || async {
                let mut actors = Vec::with_capacity(workers);
                for _ in 0..workers {
                    actors.push(spawn_countdown(share).await);
                }
                actors
            },
            |actors| async move {
                for (actor_ref, _) in actors.iter().cycle().take(share * workers) {
                    actor_ref
                        .send_message(Tick)
                        .expect("ractor fan_out message is sent");
                }
                for (_, handle) in actors {
                    handle.await.expect("ractor fan_out terminates");
                }
            },
        )
        .await
    }

    async fn spawn_countdown(messages: usize) -> (ActorRef<Tick>, JoinHandle<()>) {
        Actor::spawn(None, Countdown, messages)
            .await
            .expect("ractor countdown is spawned")
    }

    struct Tick;

    struct Countdown;

    impl Actor for Countdown {
        type Msg = Tick;
        type State = usize;
        type Arguments = usize;

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            _: Self::Msg,
            remaining: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match count_down(*remaining) {
                Some(next) => *remaining = next,
                None => myself.stop(None),
            }
            Ok(())
        }
    }

    enum PingerMessage {
        Go,
        Pong,
    }

    struct Pinger;

    impl Actor for Pinger {
        type Msg = PingerMessage;
        type State = Pinging;
        type Arguments = usize;

        async fn pre_start(
            &self,
            myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            let (ponger, ponger_handle) = Actor::spawn(None, Ponger { pinger: myself }, ())
                .await
                .expect("ractor ponger is spawned");

            Ok(Pinging {
                ponger,
                ponger_handle,
                remaining: args,
            })
        }

        // Must complete before the pinger's own join handle resolves, like tellus's child barrier!
        async fn post_stop(
            &self,
            _: ActorRef<Self::Msg>,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            state.ponger.stop(None);
            (&mut state.ponger_handle).await?;

            Ok(())
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                PingerMessage::Go => {
                    state
                        .ponger
                        .send_message(Ping)
                        .expect("ractor ping is sent");
                }

                PingerMessage::Pong => match count_down(state.remaining) {
                    Some(remaining) => {
                        state.remaining = remaining;
                        state
                            .ponger
                            .send_message(Ping)
                            .expect("ractor ping is sent");
                    }

                    None => myself.stop(None),
                },
            }
            Ok(())
        }
    }

    struct Pinging {
        ponger: ActorRef<Ping>,
        ponger_handle: JoinHandle<()>,
        remaining: usize,
    }

    struct Ping;

    struct Ponger {
        pinger: ActorRef<PingerMessage>,
    }

    impl Actor for Ponger {
        type Msg = Ping;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            _: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(())
        }

        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            _: Self::Msg,
            _: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            self.pinger
                .send_message(PingerMessage::Pong)
                .expect("ractor pong is sent");

            Ok(())
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .output_directory(&workspace_output_directory())
        .sample_size(50)
        .noise_threshold(0.05)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10));
    targets = flood, ping_pong, fan_out
);
