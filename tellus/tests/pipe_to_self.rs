mod logs;

use crate::logs::Logs;
use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering::SeqCst},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorSystem, Backoff, Control, Incoming, MailboxCapacity,
    RestartPolicy, SupervisionStrategy, Unstashed, Untimed,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

const TIMEOUT: Duration = Duration::from_secs(5);

/// The output of a piped future arrives as an ordinary message, converted by the given function.
#[tokio::test]
async fn delivers_output_as_message() {
    let (system, mut events) = probe_system(ActorConfig::default());

    system.root().tell(Message::Pipe(Box::pin(async { 42 })));

    assert_eq!(
        next_event(&mut events).await,
        Event::Piped {
            generation: 1,
            value: 42
        }
    );
    stop(system).await;
}

/// The actor keeps handling messages while the piped future is pending.
#[tokio::test]
async fn handles_messages_while_future_is_pending() {
    let (system, mut events) = probe_system(ActorConfig::default());
    let (release_tx, release_rx) = oneshot::channel::<()>();

    system.root().tell(Message::Pipe(Box::pin(async move {
        release_rx.await.expect("release sender dropped");
        7
    })));
    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);

    release_tx.send(()).expect("piped future dropped");
    assert_eq!(
        next_event(&mut events).await,
        Event::Piped {
            generation: 1,
            value: 7
        }
    );
    stop(system).await;
}

/// A panicking future delivers no message and leaves the actor running. The panic is logged at
/// error level with the actor ID, rather than vanishing with the task's discarded `JoinHandle`.
#[tokio::test]
async fn panic_in_future_delivers_nothing() {
    let (system, mut events) = probe_system(ActorConfig::default());
    let mut logs = Logs::of(system.root().actor_id());
    let (panicking_tx, panicking_rx) = oneshot::channel();

    system.root().tell(Message::Pipe(Box::pin(async move {
        panicking_tx.send(()).expect("test dropped the receiver");
        panic!("piped future panics on purpose")
    })));
    panicking_rx.await.expect("piped future did not run");
    assert_logged_as_error(&mut logs, "piped future panics on purpose").await;

    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A panic in the conversion of the output is contained like one in the future: it is logged at
/// error level with the actor ID, no message is delivered, and the actor keeps running.
#[tokio::test]
async fn panic_in_conversion_delivers_nothing() {
    let (system, mut events) = probe_system(ActorConfig::default());
    let mut logs = Logs::of(system.root().actor_id());

    system
        .root()
        .tell(Message::PipeIntoPanic(Box::pin(async { 11 })));
    assert_logged_as_error(&mut logs, "the conversion panics on purpose").await;

    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A restart does not cancel a piped future: its output is received by the new state.
#[tokio::test]
async fn restarted_actor_receives_output() {
    let backoff = Backoff::new(Duration::from_millis(1), Duration::from_millis(1))
        .expect("the bounds are valid");
    let config = ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(NonZeroU32::MIN).with_backoff(backoff),
    ));
    let (system, mut events) = probe_system(config);
    let (release_tx, release_rx) = oneshot::channel::<()>();

    system.root().tell(Message::Pipe(Box::pin(async move {
        release_rx.await.expect("release sender dropped");
        3
    })));
    system.root().tell(Message::Fail);
    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);

    release_tx.send(()).expect("piped future dropped");
    assert_eq!(
        next_event(&mut events).await,
        Event::Piped {
            generation: 2,
            value: 3
        }
    );
    stop(system).await;
}

/// Stopping the actor does not wait for a pending piped future, whose output then becomes a dead
/// letter.
#[tokio::test]
async fn stopped_actor_does_not_wait_for_future() {
    let (system, mut events) = probe_system(ActorConfig::default());
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let (finished_tx, finished_rx) = oneshot::channel();

    system.root().tell(Message::Pipe(Box::pin(async move {
        release_rx.await.expect("release sender dropped");
        finished_tx.send(()).expect("test dropped the receiver");
        5
    })));
    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    let mut logs = Logs::of(system.root().actor_id());
    stop(system).await;

    release_tx.send(()).expect("piped future dropped");
    timeout(TIMEOUT, finished_rx)
        .await
        .expect("piped future did not finish")
        .expect("piped future dropped the sender");
    let dead_letter = logs.until("dead letter").await;
    assert_eq!(
        dead_letter.last().and_then(|fields| fields.get("level")),
        Some(&"WARN".to_string())
    );
    assert_eq!(events.recv().await, None);
}

/// A full bounded mailbox still receives the output: it is enqueued past the capacity. The future
/// fills the single slot itself right before it completes. The test runs on one thread, so the
/// actor cannot free the slot before the output is sent.
#[tokio::test]
async fn full_bounded_mailbox_still_receives_output() {
    let config =
        ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
    let (system, mut events) = probe_system(config);

    let probe = system.root().clone();
    system.root().tell(Message::Pipe(Box::pin(async move {
        probe.tell(Message::Ping);
        9
    })));

    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(
        next_event(&mut events).await,
        Event::Piped {
            generation: 1,
            value: 9
        }
    );
    stop(system).await;
}

struct Probe {
    events: mpsc::UnboundedSender<Event>,
    generations: Arc<AtomicU32>,
}

impl Actor for Probe {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = u32;
    type Error = Failed;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self.generations.fetch_add(1, SeqCst) + 1)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        generation: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the probe watches no actor, hence never gets a terminated signal")
        };

        match message {
            Message::Pipe(future) => context.pipe_to_self(future, Message::Piped),

            Message::PipeIntoPanic(future) => context.pipe_to_self(future, |_| {
                panic!("the conversion panics on purpose");
            }),

            Message::Piped(value) => {
                self.events
                    .send(Event::Piped { generation, value })
                    .expect("test dropped the receiver");
            }

            Message::Ping => self
                .events
                .send(Event::Ping)
                .expect("test dropped the receiver"),

            Message::Fail => return Err(Failed),
        }
        Ok(Control::Continue(generation))
    }
}

enum Message {
    Pipe(Pin<Box<dyn Future<Output = u32> + Send>>),
    PipeIntoPanic(Pin<Box<dyn Future<Output = u32> + Send>>),
    Piped(u32),
    Ping,
    Fail,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Piped { generation: u32, value: u32 },
    Ping,
}

#[derive(Debug, Error)]
#[error("probe failed on purpose")]
struct Failed;

fn probe_system(config: ActorConfig) -> (ActorSystem<Message>, mpsc::UnboundedReceiver<Event>) {
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let probe = Probe {
        events: events_tx,
        generations: Arc::new(AtomicU32::new(0)),
    };
    (ActorSystem::with_config(probe, config), events_rx)
}

/// Await the error the actor's piped task logged, carrying the given panic message.
async fn assert_logged_as_error(logs: &mut Logs, panic: &str) {
    let events = logs.until("piped future failed").await;
    let failed = events
        .last()
        .expect("`until` returns the event it waited for");
    assert_eq!(failed.get("level"), Some(&"ERROR".to_string()));
    assert_eq!(failed.get("panic"), Some(&panic.to_string()));
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    timeout(TIMEOUT, events.recv())
        .await
        .expect("no event in time")
        .expect("probe dropped the sender")
}

async fn stop(system: ActorSystem<Message>) {
    system.stop();
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}
