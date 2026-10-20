#![cfg(feature = "test-util")]

use std::{
    convert::Infallible,
    sync::{Mutex, mpsc as std_mpsc},
    time::Duration,
};
use tellus::{Actor, ActorContext, Control, Incoming, testing::TestContext};
use tokio::{sync::mpsc, task::block_in_place, time::timeout};

const TIMEOUT: Duration = Duration::from_secs(5);

/// The state transition of a plain actor is asserted by calling `receive` directly, no actor
/// system involved.
#[test]
fn receive_can_be_called_directly() {
    let test_context = TestContext::new();

    let control = Summer
        .receive(test_context.context(), Incoming::Message(2), 1)
        .unwrap();

    assert_eq!(control, Control::Continue(3));
}

/// Messages an actor sends to itself, directly and through a `ReplyTo`, arrive in the order they
/// were sent.
#[test]
fn messages_to_self_and_replies_arrive_in_order() {
    let mut test_context = TestContext::new();

    let control = Chatter
        .receive(test_context.context(), Incoming::Message(Chat::Start), ())
        .unwrap();

    assert_eq!(control, Control::Continue(()));

    assert_eq!(
        test_context.take_incoming(),
        [
            Incoming::Message(Chat::Echo(1)),
            Incoming::Message(Chat::Echo(2)),
            Incoming::Message(Chat::Echo(3)),
        ]
    );
}

/// Draining returns each message once, and an empty mailbox yields an empty result instead of
/// waiting.
#[test]
fn take_incoming_drains_the_mailbox() {
    let mut test_context = TestContext::new();
    assert!(test_context.take_incoming().is_empty());

    test_context.context().self_ref().tell(1);
    test_context.context().self_ref().tell(2);

    assert_eq!(
        test_context.take_incoming(),
        [Incoming::Message(1), Incoming::Message(2)]
    );
    assert!(test_context.take_incoming().is_empty());
}

/// The depth an actor would read is what it has not received yet, so a test can assert on it
/// without a running system.
#[test]
fn queued_counts_the_incoming_not_yet_taken() {
    let mut test_context = TestContext::new();
    assert_eq!(test_context.context().queued(), 0);

    test_context.context().self_ref().tell(1);
    test_context.context().self_ref().tell(2);
    assert_eq!(test_context.context().queued(), 2);

    test_context.take_incoming();
    assert_eq!(test_context.context().queued(), 0);
}

#[tokio::test]
async fn watching_a_terminated_context_signals_right_away() {
    let other = TestContext::<()>::new();
    let other_ref = other.context().self_ref().clone();
    let mut watcher = TestContext::<()>::new();

    other.terminate().await;
    watcher.context().watch(&other_ref);

    assert_eq!(
        watcher.take_incoming(),
        [Incoming::Terminated(other_ref.actor_id())]
    );
}

#[tokio::test]
async fn watching_a_live_context_signals_once_it_terminates() {
    let other = TestContext::<()>::new();
    let other_ref = other.context().self_ref().clone();
    let mut watcher = TestContext::<()>::new();

    watcher.context().watch(&other_ref);
    assert!(watcher.take_incoming().is_empty());

    other.terminate().await;

    assert_eq!(
        watcher.take_incoming(),
        [Incoming::Terminated(other_ref.actor_id())]
    );
}

#[tokio::test]
async fn an_unwatched_context_is_not_signaled() {
    let other = TestContext::<()>::new();
    let other_ref = other.context().self_ref().clone();
    let mut watcher = TestContext::<()>::new();

    watcher.context().watch(&other_ref);
    watcher.context().unwatch(&other_ref);
    other.terminate().await;

    assert!(watcher.take_incoming().is_empty());
}

#[tokio::test]
async fn watching_twice_signals_once() {
    let other = TestContext::<()>::new();
    let other_ref = other.context().self_ref().clone();
    let mut watcher = TestContext::<()>::new();

    watcher.context().watch(&other_ref);
    watcher.context().watch(&other_ref);
    other.terminate().await;

    assert_eq!(
        watcher.take_incoming(),
        [Incoming::Terminated(other_ref.actor_id())]
    );
}

/// The watcher must not be signaled while a descendant is still shutting down. The child's
/// destructor reports that it has been entered and then blocks on a barrier, so it is provably
/// mid-shutdown while the test asserts that nothing has been signaled and termination has not
/// finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn termination_signals_watchers_only_after_descendants_terminated() {
    let (entered_tx, entered_rx) = std_mpsc::channel();
    let (release_tx, release_rx) = std_mpsc::channel();

    let other = TestContext::<()>::new();
    let other_ref = other.context().self_ref().clone();
    let mut watcher = TestContext::<()>::new();
    watcher.context().watch(&other_ref);
    other.context().spawn(BlockingChild {
        barrier: Mutex::new(Some(Barrier {
            entered_tx,
            release_rx,
        })),
    });

    let terminating = tokio::spawn(other.terminate());
    block_in_place(|| entered_rx.recv_timeout(TIMEOUT)).expect("child did not start shutting down");

    assert!(watcher.take_incoming().is_empty());
    assert!(!terminating.is_finished());

    release_tx.send(()).expect("child awaits the release");
    timeout(TIMEOUT, terminating)
        .await
        .expect("termination did not finish")
        .expect("termination task panicked");

    assert_eq!(
        watcher.take_incoming(),
        [Incoming::Terminated(other_ref.actor_id())]
    );
}

#[tokio::test]
async fn spawn_starts_a_working_child() {
    let test_context = TestContext::<()>::new();
    let (reported_tx, mut reported_rx) = mpsc::unbounded_channel();

    let child = test_context.context().spawn(Reporter { reported_tx });
    child.tell(7);

    let reported = timeout(TIMEOUT, reported_rx.recv()).await;
    assert_eq!(reported, Ok(Some(7)));
}

#[tokio::test]
async fn dropping_the_context_asks_children_to_stop() {
    let test_context = TestContext::<()>::new();
    let (dropped_tx, mut dropped_rx) = mpsc::unbounded_channel();

    test_context.context().spawn(Notifier {
        dropped_tx: dropped_tx.clone(),
    });
    timeout(TIMEOUT, dropped_rx.recv())
        .await
        .expect("child did not initialize")
        .expect("channel is open");

    drop(test_context);

    let dropped = timeout(TIMEOUT, dropped_rx.recv()).await;
    assert_eq!(dropped, Ok(Some(Event::Dropped)));
}

struct Summer;

impl Actor for Summer {
    type Message = u32;
    type State = u32;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        sum: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(n) => Ok(Control::Continue(sum + n)),
            Incoming::Terminated(_) => Ok(Control::Stop),
        }
    }
}

struct Chatter;

#[derive(Debug, PartialEq)]
enum Chat {
    Start,
    Echo(u32),
}

impl Actor for Chatter {
    type Message = Chat;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        (): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        context.self_ref().tell(Chat::Echo(1));
        context.reply_to(Chat::Echo).reply(2);
        context.self_ref().tell(Chat::Echo(3));

        Ok(Control::Continue(()))
    }
}

struct BlockingChild {
    barrier: Mutex<Option<Barrier>>,
}

impl Actor for BlockingChild {
    type Message = ();
    type State = Barrier;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(self
            .barrier
            .lock()
            .expect("the lock is not poisoned")
            .take()
            .expect("the child is initialized once"))
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        barrier: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(barrier))
    }
}

struct Barrier {
    entered_tx: std_mpsc::Sender<()>,
    release_rx: std_mpsc::Receiver<()>,
}

impl Drop for Barrier {
    fn drop(&mut self) {
        self.entered_tx
            .send(())
            .expect("the test awaits the entered report");
        self.release_rx
            .recv_timeout(TIMEOUT)
            .expect("the test releases the barrier");
    }
}

struct Reporter {
    reported_tx: mpsc::UnboundedSender<u32>,
}

impl Actor for Reporter {
    type Message = u32;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        (): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(n) = incoming {
            self.reported_tx
                .send(n)
                .expect("the test awaits the report");
        }

        Ok(Control::Continue(()))
    }
}

struct Notifier {
    dropped_tx: mpsc::UnboundedSender<Event>,
}

#[derive(Debug, PartialEq)]
enum Event {
    Initialized,
    Dropped,
}

struct NotifyOnDrop(mpsc::UnboundedSender<Event>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(Event::Dropped);
    }
}

impl Actor for Notifier {
    type Message = ();
    type State = NotifyOnDrop;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let _ = self.dropped_tx.send(Event::Initialized);
        Ok(NotifyOnDrop(self.dropped_tx.clone()))
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}
