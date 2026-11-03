use std::{
    convert::Infallible,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::pin,
    task::{Context, Waker},
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AskError, Backoff, Control, Incoming,
    MailboxCapacity, ReplyTo, RestartPolicy, SupervisionStrategy, Unstashed, Untimed,
};
use tokio::{
    sync::mpsc,
    task::{block_in_place, spawn},
    time::timeout,
};

const TIMEOUT: Duration = Duration::from_secs(5);
const BOUNDED_TO_ONE: MailboxCapacity = MailboxCapacity::Bounded(NonZeroUsize::MIN);

/// An ask from outside the actor tree resolves with exactly the value the actor passed to `reply`.
#[tokio::test]
async fn ask_resolves_with_the_reply() {
    let system = ActorSystem::new(Responder);

    let reply = system
        .root()
        .ask(TIMEOUT, |reply_to| Request::Reply {
            value: 21,
            reply_to,
        })
        .await
        .expect("ask failed");
    assert_eq!(reply, 42);

    system.root().tell(Request::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// An ask to a terminated actor fails up front with `AskError::ActorTerminated` instead of
/// hanging. The termination is proven by `ActorSystem::terminated` having resolved.
#[tokio::test]
async fn ask_a_terminated_actor_fails_as_terminated() {
    let system = ActorSystem::new(Responder);
    let root = system.root().clone();

    root.tell(Request::Stop);
    assert_terminates(system, "actor system did not terminate").await;

    let error = root
        .ask(TIMEOUT, |reply_to| Request::Reply {
            value: 21,
            reply_to,
        })
        .await
        .expect_err("ask to a terminated actor must fail");
    assert!(matches!(error, AskError::ActorTerminated));
}

/// An ask to a full bounded mailbox fails up front with `AskError::MailboxFull`. The responder,
/// blocked in `receive`, confirms it has dequeued the blocking message, so the single capacity
/// slot provably holds the stop message when the ask is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ask_a_full_mailbox_fails_as_full() {
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();
    let (blocked_tx, mut blocked_rx) = mpsc::channel(1);
    let root = BlockedResponder {
        unblock_rx,
        blocked_tx,
    };
    let config = ActorConfig::default().with_mailbox_capacity(BOUNDED_TO_ONE);
    let system = ActorSystem::with_config(root, config);

    system.root().tell(BlockedRequest::Block);
    recv(&mut blocked_rx, "responder did not block").await;

    system.root().tell(BlockedRequest::Stop);

    let error = system
        .root()
        .ask(TIMEOUT, |reply_to| BlockedRequest::Reply {
            value: 21,
            reply_to,
        })
        .await
        .expect_err("ask into a full mailbox must fail");
    assert!(matches!(error, AskError::MailboxFull));

    unblock_tx.send(()).expect("unblock channel closed");
    assert_terminates(system, "actor system did not terminate").await;
}

/// An actor dropping the `ReplyTo` without replying resolves the ask with `AskError::NoReply`
/// instead of hanging.
#[tokio::test]
async fn a_dropped_reply_to_fails_as_no_reply() {
    let system = ActorSystem::new(Responder);

    let error = system
        .root()
        .ask(TIMEOUT, Request::Discard)
        .await
        .expect_err("a discarded request must not produce a reply");
    assert!(matches!(error, AskError::NoReply));

    system.root().tell(Request::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// A responder which keeps the `ReplyTo` alive without replying resolves the ask as
/// `AskError::Timeout` once the given duration has elapsed; the paused clock auto-advances, so
/// the test does not actually wait.
#[tokio::test(start_paused = true)]
async fn an_unanswered_ask_fails_as_timeout() {
    let system = ActorSystem::new(Keeper);

    let error = system
        .root()
        .ask(TIMEOUT, KeeperMessage::Keep)
        .await
        .expect_err("a kept request must not produce a reply");
    assert!(matches!(error, AskError::Timeout(within) if within == TIMEOUT));

    system.root().tell(KeeperMessage::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// An actor stopping with the ask message still queued resolves the ask with `AskError::NoReply`
/// rather than leaving it pending forever: termination drains the mailbox, dropping the queued
/// request and its reply channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_with_the_ask_still_queued_fails_as_no_reply() {
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();
    let (blocked_tx, mut blocked_rx) = mpsc::channel(1);
    let root = BlockedResponder {
        unblock_rx,
        blocked_tx,
    };
    let system = ActorSystem::new(root);

    system.root().tell(BlockedRequest::Block);
    recv(&mut blocked_rx, "responder did not block").await;

    system.root().tell(BlockedRequest::Stop);

    // Poll once before unblocking: the first poll performs the send, queueing behind the stop!
    let root = system.root().clone();
    let mut ask = pin!(root.ask(TIMEOUT, |reply_to| BlockedRequest::Reply {
        value: 21,
        reply_to
    }));
    let mut poll_context = Context::from_waker(Waker::noop());
    assert!(ask.as_mut().poll(&mut poll_context).is_pending());

    unblock_tx.send(()).expect("unblock channel closed");

    let error = ask
        .await
        .expect_err("a drained request must not produce a reply");
    assert!(matches!(error, AskError::NoReply));

    assert_terminates(system, "actor system did not terminate").await;
}

/// A reply through `ActorContext::reply_to` arrives in the asking actor's own mailbox, converted
/// into its message type by the given function, here an enum variant constructor.
#[tokio::test]
async fn reply_to_delivers_the_reply_as_a_message() {
    let (doubled_tx, mut doubled_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Asker(doubled_tx));

    let doubled = recv(&mut doubled_rx, "asker did not receive the reply message").await;
    assert_eq!(doubled, 42);

    assert_terminates(system, "actor system did not terminate").await;
}

/// Replying after the asker has terminated drops the reply as a dead letter: the responder
/// neither panics nor stops, proven by it still answering a probe afterward. The coordinator
/// replies only after the asker's terminated signal, which is sent after its mailbox is gone.
#[tokio::test]
async fn a_reply_after_the_asker_terminated_is_a_dead_letter() {
    let (probed_tx, mut probed_rx) = mpsc::channel(1);
    let system = ActorSystem::new(Coordinator(probed_tx));

    let probed = recv(&mut probed_rx, "keeper did not answer the probe").await;
    assert_eq!(probed, 99);

    assert_terminates(system, "actor system did not terminate").await;
}

/// A `ReplyTo` moved into a spawned task can reply after `receive` has returned, and the reply
/// still resolves the ask.
#[tokio::test]
async fn a_reply_from_a_spawned_task_reaches_the_asker() {
    let system = ActorSystem::new(Responder);

    let reply = system
        .root()
        .ask(TIMEOUT, |reply_to| Request::Detached {
            value: 21,
            reply_to,
        })
        .await
        .expect("ask failed");
    assert_eq!(reply, 42);

    system.root().tell(Request::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// The mailbox surviving a restart keeps a queued ask answerable: the request queued behind the
/// panicking message is answered by the restarted incarnation.
#[tokio::test]
async fn an_ask_queued_across_a_restart_is_still_answered() {
    let system = ActorSystem::with_config(FragileResponder, restart_config());

    system.root().tell(FragileRequest::Panic);

    let reply = system
        .root()
        .ask(TIMEOUT, |reply_to| FragileRequest::Reply {
            value: 21,
            reply_to,
        })
        .await
        .expect("ask failed");
    assert_eq!(reply, 42);

    system.root().tell(FragileRequest::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// An ask message consumed by a failing `receive` resolves as `AskError::NoReply`: the `ReplyTo`
/// dies in the unwound frame and the message is not redelivered, also under `Restart`. The
/// second ask proves the actor restarted.
#[tokio::test]
async fn a_failing_ask_message_fails_as_no_reply() {
    let system = ActorSystem::with_config(FragileResponder, restart_config());

    let error = system
        .root()
        .ask(TIMEOUT, FragileRequest::PanicWithReply)
        .await
        .expect_err("a request consumed by a failing receive must not produce a reply");
    assert!(matches!(error, AskError::NoReply));

    let reply = system
        .root()
        .ask(TIMEOUT, |reply_to| FragileRequest::Reply {
            value: 21,
            reply_to,
        })
        .await
        .expect("ask failed");
    assert_eq!(reply, 42);

    system.root().tell(FragileRequest::Stop);
    assert_terminates(system, "actor system did not terminate").await;
}

/// A `reply_to` reply is enqueued past the asker's full bounded mailbox, behind the queued
/// messages, and counts against the capacity. The second `Block` fills the single slot while the
/// asker is blocked in the first. Once the asker holds the second `Block`, only the reply is
/// queued, so an ordinary message is refused only because the reply counts. Observing the reply
/// and then the probe proves the delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_to_a_full_asker_mailbox_is_received_and_counted() {
    let (unblock_tx, unblock_rx) = std::sync::mpsc::channel();
    let (blocked_tx, mut blocked_rx) = mpsc::channel(1);
    let (reply_to_tx, mut reply_to_rx) = mpsc::channel(1);
    let (observed_tx, mut observed_rx) = mpsc::channel(3);

    let root = BlockedAsker {
        unblock_rx,
        blocked_tx,
        reply_to_tx,
        observed_tx,
    };
    let config = ActorConfig::default().with_mailbox_capacity(BOUNDED_TO_ONE);
    let system = ActorSystem::with_config(root, config);

    let reply_to = recv(
        &mut reply_to_rx,
        "asker did not hand out its reply destination",
    )
    .await;

    system.root().tell(BlockedAskerMessage::Block);
    recv(&mut blocked_rx, "asker did not block").await;

    system.root().tell(BlockedAskerMessage::Block);
    reply_to.reply(42);

    unblock_tx.send(()).expect("unblock channel closed");
    recv(&mut blocked_rx, "asker did not block again").await;

    let refused = system
        .root()
        .ask::<(), _>(TIMEOUT, |_| BlockedAskerMessage::Filler)
        .await;
    assert!(matches!(refused, Err(AskError::MailboxFull)));

    unblock_tx.send(()).expect("unblock channel closed");

    let observed = recv(&mut observed_rx, "asker did not observe the reply").await;
    assert_eq!(observed, Observed::Reply(42));

    system.root().tell(BlockedAskerMessage::Probe);

    let observed = recv(&mut observed_rx, "asker did not observe the probe").await;
    assert_eq!(observed, Observed::Probe);

    assert_terminates(system, "actor system did not terminate").await;
}

async fn recv<T>(rx: &mut mpsc::Receiver<T>, not_received: &str) -> T {
    timeout(TIMEOUT, rx.recv())
        .await
        .expect(not_received)
        .expect("channel closed")
}

async fn assert_terminates<M>(system: ActorSystem<M>, not_terminated: &str)
where
    M: Send + 'static,
{
    timeout(TIMEOUT, system.terminated())
        .await
        .expect(not_terminated)
        .expect("watching the root actor failed");
}

fn restart_config() -> ActorConfig {
    ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(RestartPolicy {
        max_restarts: NonZeroU32::MIN,
        backoff: Backoff::new(Duration::from_nanos(1), Duration::from_nanos(1))
            .expect("the bounds are valid"),
        reset_after: Duration::ZERO,
    }))
}

/// Reply with the doubled value, from `receive` or a spawned task, discard the request or stop,
/// as asked.
struct Responder;

impl Actor for Responder {
    type Message = Request;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Request::Reply { value, reply_to }) => {
                reply_to.reply(2 * value);
                Ok(Control::Continue(()))
            }

            Incoming::Message(Request::Detached { value, reply_to }) => {
                spawn(async move { reply_to.reply(2 * value) });
                Ok(Control::Continue(()))
            }

            Incoming::Message(Request::Discard(reply_to)) => {
                drop(reply_to);
                Ok(Control::Continue(()))
            }

            Incoming::Message(Request::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

enum Request {
    Reply { value: u64, reply_to: ReplyTo<u64> },
    Detached { value: u64, reply_to: ReplyTo<u64> },
    Discard(ReplyTo<u64>),
    Stop,
}

/// Block inside `receive` on demand, confirming the block, so the tests control when queued
/// messages are processed. `block_in_place`, else it strands whatever task sits in this worker's
/// LIFO slot.
struct BlockedResponder {
    unblock_rx: std::sync::mpsc::Receiver<()>,
    blocked_tx: mpsc::Sender<()>,
}

impl Actor for BlockedResponder {
    type Message = BlockedRequest;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(BlockedRequest::Block) => {
                let _ = self.blocked_tx.try_send(());
                block_in_place(|| {
                    self.unblock_rx
                        .recv_timeout(TIMEOUT)
                        .expect("unblock channel closed or timed out")
                });
                Ok(Control::Continue(()))
            }

            Incoming::Message(BlockedRequest::Reply { value, reply_to }) => {
                reply_to.reply(2 * value);
                Ok(Control::Continue(()))
            }

            Incoming::Message(BlockedRequest::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

enum BlockedRequest {
    Block,
    Reply { value: u64, reply_to: ReplyTo<u64> },
    Stop,
}

/// Request the doubled value from a responder child via `reply_to` and report the reply, which
/// arrives as an ordinary message, back to the test.
struct Asker(mpsc::Sender<u64>);

impl Actor for Asker {
    type Message = AskerMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let responder = context.spawn(Responder);
        responder.tell(Request::Reply {
            value: 21,
            reply_to: context.reply_to(AskerMessage::Doubled),
        });
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(AskerMessage::Doubled(value)) => {
                let _ = self.0.try_send(value);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

enum AskerMessage {
    Doubled(u64),
}

/// Spawn the keeper and a short-lived asker whose request the keeper stores; once the asker has
/// terminated, make the keeper reply into the void, then probe it: the probe's answer proves the
/// keeper survived the dead-lettered reply.
struct Coordinator(mpsc::Sender<u64>);

impl Actor for Coordinator {
    type Message = CoordinatorMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ActorRef<KeeperMessage>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let keeper = context.spawn(Keeper);
        let asker = context.spawn(ShortAsker(keeper.clone()));
        context.watch(&asker);
        asker.tell(());
        Ok(keeper)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        keeper: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(CoordinatorMessage::Probed(value)) => {
                let _ = self.0.try_send(value);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => {
                keeper.tell(KeeperMessage::ReplyNow);
                keeper.tell(KeeperMessage::Probe(
                    context.reply_to(CoordinatorMessage::Probed),
                ));
                Ok(Control::Continue(keeper))
            }
        }
    }
}

enum CoordinatorMessage {
    Probed(u64),
}

/// Send the keeper a request whose reply can never arrive: the asker stops on the first message,
/// told right after spawning.
struct ShortAsker(ActorRef<KeeperMessage>);

impl Actor for ShortAsker {
    type Message = ();
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        self.0.tell(KeeperMessage::Keep(context.reply_to(|_| ())));
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

/// Store the kept request and reply to it only when told to; answer probes regardless.
struct Keeper;

impl Actor for Keeper {
    type Message = KeeperMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Option<ReplyTo<u64>>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        kept: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(KeeperMessage::Keep(reply_to)) => {
                Ok(Control::Continue(Some(reply_to)))
            }

            Incoming::Message(KeeperMessage::ReplyNow) => {
                if let Some(reply_to) = kept {
                    reply_to.reply(42);
                }
                Ok(Control::Continue(None))
            }

            Incoming::Message(KeeperMessage::Probe(reply_to)) => {
                reply_to.reply(99);
                Ok(Control::Continue(kept))
            }

            Incoming::Message(KeeperMessage::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(kept)),
        }
    }
}

enum KeeperMessage {
    Keep(ReplyTo<u64>),
    ReplyNow,
    Probe(ReplyTo<u64>),
    Stop,
}

/// Reply to well-formed requests, panic on the poisoned ones; run under `Restart` so the tests
/// observe the retained mailbox across a restart.
struct FragileResponder;

impl Actor for FragileResponder {
    type Message = FragileRequest;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(FragileRequest::Reply { value, reply_to }) => {
                reply_to.reply(2 * value);
                Ok(Control::Continue(()))
            }

            Incoming::Message(FragileRequest::Panic) => panic!("poisoned"),

            Incoming::Message(FragileRequest::PanicWithReply(_reply_to)) => {
                panic!("poisoned with reply")
            }

            Incoming::Message(FragileRequest::Stop) => Ok(Control::Stop),

            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

enum FragileRequest {
    Reply { value: u64, reply_to: ReplyTo<u64> },
    Panic,
    PanicWithReply(ReplyTo<u64>),
    Stop,
}

/// Hand out a reply destination, block on demand, then report every message it processes, so the
/// test can prove where the reply was delivered. `block_in_place`, else it strands whatever task
/// sits in this worker's LIFO slot.
struct BlockedAsker {
    unblock_rx: std::sync::mpsc::Receiver<()>,
    blocked_tx: mpsc::Sender<()>,
    reply_to_tx: mpsc::Sender<ReplyTo<u64>>,
    observed_tx: mpsc::Sender<Observed>,
}

impl Actor for BlockedAsker {
    type Message = BlockedAskerMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let _ = self
            .reply_to_tx
            .try_send(context.reply_to(BlockedAskerMessage::Reply));
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(BlockedAskerMessage::Block) => {
                let _ = self.blocked_tx.try_send(());
                block_in_place(|| {
                    self.unblock_rx
                        .recv_timeout(TIMEOUT)
                        .expect("unblock channel closed or timed out")
                });
                Ok(Control::Continue(()))
            }

            Incoming::Message(BlockedAskerMessage::Filler) => {
                let _ = self.observed_tx.try_send(Observed::Filler);
                Ok(Control::Continue(()))
            }

            Incoming::Message(BlockedAskerMessage::Reply(value)) => {
                let _ = self.observed_tx.try_send(Observed::Reply(value));
                Ok(Control::Continue(()))
            }

            Incoming::Message(BlockedAskerMessage::Probe) => {
                let _ = self.observed_tx.try_send(Observed::Probe);
                Ok(Control::Stop)
            }

            Incoming::Terminated(_) => Ok(Control::Continue(())),
        }
    }
}

enum BlockedAskerMessage {
    Block,
    Filler,
    Reply(u64),
    Probe,
}

#[derive(Debug, PartialEq)]
enum Observed {
    Filler,
    Reply(u64),
    Probe,
}
