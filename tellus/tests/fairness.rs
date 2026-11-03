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
use tokio::{runtime::Builder, sync::oneshot};

const MESSAGES: usize = 100_000;
const DEADLINE: Duration = Duration::from_secs(30);

/// An actor with a never empty mailbox must not monopolize its worker: on a current-thread
/// runtime, a task woken by its first message runs while it still has messages to handle.
#[test]
fn busy_actor_lets_a_ready_task_run() {
    let observed = run_with_deadline(|| async {
        let handled = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let probe = tokio::spawn({
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
        tokio::spawn(async move {
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

/// Runs the test body on a current-thread runtime on its own thread, so the deadline is enforced
/// from outside: a starved runtime cannot fire a timer of its own.
fn run_with_deadline<F, T>(body: impl FnOnce() -> F + Send + 'static) -> T
where
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
