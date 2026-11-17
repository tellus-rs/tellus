use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc as std_mpsc,
    },
    thread,
    time::Duration,
};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, Unstashed, Untimed};
use tokio::{runtime::Builder, sync::oneshot, task, time::sleep};

const MESSAGES: usize = 100_000;
const DEADLINE: Duration = Duration::from_secs(30);

/// An actor with a never empty mailbox must not monopolize its worker: on a current-thread
/// runtime, a task woken by its first message runs while it still has messages to handle.
#[test]
fn busy_actor_lets_a_ready_task_run() {
    let observed = run_with_deadline(|| async {
        let handled = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let probe = task::spawn({
            let handled = handled.clone();
            async move {
                started_rx
                    .await
                    .expect("the busy actor dropped the start signal");
                handled.load(SeqCst)
            }
        });

        let system = ActorSystem::new(Busy::new(handled.clone(), started_tx));
        system
            .terminated()
            .await
            .expect("watching the root actor failed");
        assert_eq!(handled.load(SeqCst), MESSAGES);

        probe.await.expect("the probe task panicked")
    });

    assert!(
        observed < MESSAGES,
        "the ready task only ran after all {MESSAGES} messages"
    );
}

/// Stopping a busy actor takes effect while it still has messages to handle, also on a
/// current-thread runtime, where the stopping task shares the worker with the actor.
#[test]
fn busy_actor_can_be_stopped() {
    let handled = run_with_deadline(|| async {
        let handled = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();

        let system = ActorSystem::new(Busy::new(handled.clone(), started_tx));
        task::spawn(async move {
            started_rx
                .await
                .expect("the busy actor dropped the start signal");
            system.stop();
            system
                .terminated()
                .await
                .expect("watching the root actor failed");
        })
        .await
        .expect("the stopping task panicked");

        handled.load(SeqCst)
    });

    assert!(
        handled < MESSAGES,
        "the actor was only stopped after all {MESSAGES} messages"
    );
}

/// A permanently ready Flume receive must still charge Tokio's cooperative budget. The external
/// stop keeps a regression from hanging the test process: without fairness it wins and fails.
#[test]
fn a_self_telling_actor_yields_to_a_current_thread_timer() {
    let (root_tx, root_rx) = std_mpsc::sync_channel(1);
    let (stopped_tx, stopped_rx) = std_mpsc::sync_channel(1);
    let runtime_thread = thread::spawn(move || {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(async move {
                let system = ActorSystem::new(FairLoop { stopped_tx });
                let root = system.root().clone();
                root_tx.send(root.clone()).expect("test receives root");
                task::spawn(async move {
                    sleep(Duration::from_millis(10)).await;
                    root.tell(FairMessage::TimerStop);
                });
                system.terminated().await.expect("actor system terminates");
            });
    });

    let root = root_rx.recv().expect("runtime publishes root");
    let stopped_by = match stopped_rx.recv_timeout(DEADLINE) {
        Ok(stopped_by) => stopped_by,
        Err(std_mpsc::RecvTimeoutError::Timeout) => {
            root.tell(FairMessage::WatchdogStop);
            stopped_rx.recv().expect("watchdog stops actor")
        }
        Err(error) => panic!("stop channel failed: {error}"),
    };
    runtime_thread
        .join()
        .expect("runtime thread does not panic");
    assert_eq!(stopped_by, FairMessage::TimerStop);
}

/// Runs the test body on a current-thread runtime on its own thread, so the deadline is enforced
/// from outside: a starved runtime cannot fire a timer of its own.
fn run_with_deadline<B, F, T>(body: B) -> T
where
    B: FnOnce() -> F + Send + 'static,
    F: Future<Output = T>,
    T: Send + 'static,
{
    let (result_tx, result_rx) = std_mpsc::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let result = runtime.block_on(body());
        result_tx.send(result).expect("the test thread is gone");
    });

    result_rx
        .recv_timeout(DEADLINE)
        .expect("the test did not finish before its deadline")
}

/// Sends itself `MESSAGES` messages, one after the other, and signals the first one.
struct Busy {
    handled: Arc<AtomicUsize>,
    started: Mutex<Option<oneshot::Sender<()>>>,
}

impl Busy {
    fn new(handled: Arc<AtomicUsize>, started: oneshot::Sender<()>) -> Self {
        Self {
            handled,
            started: Mutex::new(Some(started)),
        }
    }
}

impl Actor for Busy {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.self_ref().tell(());
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Some(started) = self.started.lock().expect("lock poisoned").take() {
            started
                .send(())
                .expect("the test dropped the start receiver");
        }

        if self.handled.fetch_add(1, SeqCst) + 1 == MESSAGES {
            return Ok(Control::Stop);
        }
        context.self_ref().tell(());
        Ok(Control::Continue(()))
    }
}

struct FairLoop {
    stopped_tx: std_mpsc::SyncSender<FairMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FairMessage {
    Tick,
    TimerStop,
    WatchdogStop,
}

impl Actor for FairLoop {
    type Message = FairMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        context.self_ref().tell(FairMessage::Tick);
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(FairMessage::Tick) => {
                context.self_ref().tell(FairMessage::Tick);
                Ok(Control::Continue(state))
            }

            Incoming::Message(
                stopped_by @ (FairMessage::TimerStop | FairMessage::WatchdogStop),
            ) => {
                let _ = self.stopped_tx.send(stopped_by);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(state)),
        }
    }
}
