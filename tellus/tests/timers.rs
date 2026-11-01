use std::{
    collections::HashMap,
    convert::Infallible,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        self, Arc,
        atomic::{AtomicBool, AtomicU32, Ordering::SeqCst},
    },
    thread,
    time::Duration,
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AskError, Backoff, Control, Incoming,
    MailboxCapacity, ReplyTo, RestartPolicy, SupervisionStrategy, Timed, Untimed,
};
use thiserror::Error;
use tokio::{
    runtime::Builder,
    select,
    sync::mpsc,
    time::{Instant, advance, sleep, timeout},
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
};

const TIMEOUT: Duration = Duration::from_secs(5);
const FLOOD_BOUND: u32 = 100_000;
const BACKLOG: u32 = 200_000;
const MONEY_TIMEOUT: Duration = Duration::from_secs(1);

/// A timer fires once its delay has elapsed, and not before.
#[tokio::test(start_paused = true)]
async fn a_timer_fires_after_its_delay() {
    let started = Instant::now();
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::from_secs(1)));
    ping(&system, &mut events).await;
    advance(Duration::from_millis(999)).await;
    ping(&system, &mut events).await;
    assert!(events.try_recv().is_err());

    advance(Duration::from_millis(1)).await;
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    stop(system).await;
}

/// The modes of parent and child are independent: a child spawned by an untimed parent runs the
/// timed loop and receives its timer.
#[tokio::test(start_paused = true)]
async fn an_untimed_parent_spawns_a_timed_child() {
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let (child_tx, mut children) = mpsc::unbounded_channel();
    let system = ActorSystem::new(UntimedParent {
        events: events_tx,
        child_tx,
    });

    let child = next_event(&mut children).await;
    child.tell(start("t", Duration::from_secs(1)));
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    stop(system).await;
}

/// The modes of parent and child are independent: a child spawned by a timed parent runs the
/// untimed loop and receives its messages.
#[tokio::test(start_paused = true)]
async fn a_timed_parent_spawns_an_untimed_child() {
    let (echoes_tx, mut echoes) = mpsc::unbounded_channel();
    let (child_tx, mut children) = mpsc::unbounded_channel();
    let system = ActorSystem::new(TimedParent {
        echoes: echoes_tx,
        child_tx,
    });

    let child = next_event(&mut children).await;
    child.tell(7);
    assert_eq!(next_event(&mut echoes).await, 7);
    stop(system).await;
}

/// Timers under different keys fire in deadline order, each after its own delay.
#[tokio::test(start_paused = true)]
async fn timers_fire_in_deadline_order() {
    let started = Instant::now();
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("late", Duration::from_secs(2)));
    system.root().tell(start("early", Duration::from_secs(1)));
    assert_eq!(next_event(&mut events).await, Event::Fired("early", 1));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(next_event(&mut events).await, Event::Fired("late", 1));
    assert_eq!(started.elapsed(), Duration::from_secs(2));
    stop(system).await;
}

/// Restarting a key restarts its deadline: one timeout at 1.5 s, none at 1 s.
#[tokio::test(start_paused = true)]
async fn restarting_a_key_restarts_its_deadline() {
    let started = Instant::now();
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::from_secs(1)));
    ping(&system, &mut events).await;
    advance(Duration::from_millis(500)).await;
    system.root().tell(start("t", Duration::from_secs(1)));
    ping(&system, &mut events).await;
    advance(Duration::from_millis(500)).await;
    ping(&system, &mut events).await;
    assert!(events.try_recv().is_err());

    advance(Duration::from_millis(500)).await;
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    assert_eq!(started.elapsed(), Duration::from_millis(1500));
    ping(&system, &mut events).await;
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A canceled timer is never received.
#[tokio::test(start_paused = true)]
async fn a_canceled_timer_is_never_received() {
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::from_secs(1)));
    system.root().tell(Message::Cancel("t"));
    ping(&system, &mut events).await;
    advance(Duration::from_secs(2)).await;
    ping(&system, &mut events).await;
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// Canceling the earliest timer keeps the next one on time, although the sleep then wakes early.
#[tokio::test(start_paused = true)]
async fn canceling_the_earliest_timer_keeps_the_next_on_time() {
    let started = Instant::now();
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("a", Duration::from_secs(1)));
    system.root().tell(start("b", Duration::from_secs(2)));
    system.root().tell(Message::Cancel("a"));
    ping(&system, &mut events).await;
    advance(Duration::from_millis(1999)).await;
    ping(&system, &mut events).await;
    assert!(events.try_recv().is_err());

    advance(Duration::from_millis(1)).await;
    assert_eq!(next_event(&mut events).await, Event::Fired("b", 1));
    assert_eq!(started.elapsed(), Duration::from_secs(2));
    stop(system).await;
}

/// A timer canceled once it is due is never received, although it has fired already and waits for
/// its position.
#[tokio::test(start_paused = true)]
async fn a_timer_canceled_once_due_is_never_received() {
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::from_secs(1)));
    ping(&system, &mut events).await;
    // Advanced before Cancel is told: told first, Cancel would arrive while the timer is pending.
    // Tokio polls this task before the actor processes the passed deadline; the tell relies on it.
    advance(Duration::from_secs(1)).await;
    system.root().tell(Message::Cancel("t"));
    ping(&system, &mut events).await;
    ping(&system, &mut events).await;
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A message which arrived before the deadline is handled before the timeout.
#[tokio::test(start_paused = true)]
async fn a_message_before_the_deadline_is_handled_before_the_timeout() {
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::from_secs(1)));
    ping(&system, &mut events).await;
    // Told before advancing: told after, the Ping would arrive at the deadline, not before it.
    system.root().tell(Message::Ping);
    advance(Duration::from_secs(1)).await;
    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    stop(system).await;
}

/// A timer started in `init` fires, although no message has been received yet; the paused clock
/// auto-advances to its deadline while the test waits.
#[tokio::test(start_paused = true)]
async fn a_timer_started_in_init_fires() {
    let started = Instant::now();
    let (system, mut events) =
        probe_system(ActorConfig::default(), Some((1, Duration::from_secs(1))));

    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    stop(system).await;
}

/// A zero-delay timer is due at once but still lands behind messages already queued.
#[tokio::test(start_paused = true)]
async fn a_zero_delay_timer_lands_behind_queued_messages() {
    let (system, mut events) = probe_system(ActorConfig::default(), None);

    system.root().tell(start("t", Duration::ZERO));
    system.root().tell(Message::Ping);
    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    stop(system).await;
}

/// A deadline which passes during a long `receive` places the timer behind the messages which
/// arrived meanwhile, since they are queued when the run loop fires the timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_passing_during_receive_lands_behind_messages_arriving_meanwhile() {
    let (system, mut events) = probe_system(ActorConfig::default(), None);
    let (release_tx, release_rx) = sync::mpsc::channel();

    system.root().tell(Message::Block {
        delay: Duration::from_millis(20),
        release: release_rx,
    });
    assert_eq!(next_event(&mut events).await, Event::Blocked);
    sleep(Duration::from_millis(100)).await;
    system.root().tell(Message::Ping);
    release_tx.send(()).expect("the probe awaits the release");
    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    stop(system).await;
}

/// A full bounded mailbox still receives its timer: a fired timer takes no place in the mailbox,
/// so it does not count against the capacity. The test fills the mailbox and a second `Ping` is a
/// dead letter before the run loop fires the timer.
#[tokio::test(start_paused = true)]
async fn a_full_bounded_mailbox_still_receives_its_timer() {
    let config =
        ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN));
    let (system, mut events) = probe_system(config, Some((1, Duration::from_secs(1))));

    ping(&system, &mut events).await;
    // Tokio polls this task before the actor processes the passed deadline; the tell relies on it.
    advance(Duration::from_secs(1)).await;
    system.root().tell(Message::Ping);
    system.root().tell(Message::Ping);
    assert_eq!(next_event(&mut events).await, Event::Ping);
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 1));
    stop(system).await;
    assert_eq!(events.recv().await, None);
}

/// A restart cancels the old incarnation's timers, due and pending alike, even when the new
/// incarnation reuses the key: only the timer the new incarnation starts is received.
#[tokio::test(start_paused = true)]
async fn a_restart_cancels_the_old_incarnations_timer() {
    let backoff = Backoff::new(Duration::from_nanos(1), Duration::from_nanos(1))
        .expect("the bounds are valid");
    let config = ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(
        RestartPolicy::new(NonZeroU32::MIN).with_backoff(backoff),
    ));
    let (system, mut events) = probe_system(config, Some((2, Duration::from_secs(3))));

    system.root().tell(start("t", Duration::from_secs(1)));
    system.root().tell(start("u", Duration::from_secs(2)));
    ping(&system, &mut events).await;
    // Advanced before Fail is told, so the old "t" is due and fires behind Fail.
    // Tokio polls this task before the actor processes the passed deadline; the tell relies on it.
    advance(Duration::from_secs(1)).await;
    system.root().tell(Message::Fail);
    ping(&system, &mut events).await;
    advance(Duration::from_secs(1)).await;
    ping(&system, &mut events).await;
    assert!(events.try_recv().is_err());

    advance(Duration::from_secs(2)).await;
    assert_eq!(next_event(&mut events).await, Event::Fired("t", 2));
    stop(system).await;
}

/// A timer message whose destructor panics at termination does not unwind the actor's task: the
/// destructor runs, and the system still terminates.
#[tokio::test(start_paused = true)]
async fn a_panicking_timer_message_destructor_is_contained() {
    let dropped = Arc::new(AtomicBool::new(false));
    let system = ActorSystem::new(PanickingTimer(dropped.clone()));

    stop(system).await;
    assert!(dropped.load(SeqCst));
}

/// A replaced timer's message whose destructor panics is dropped under containment: the actor
/// keeps receiving.
#[tokio::test]
async fn a_panicking_destructor_of_a_replaced_timer_message_is_contained() {
    assert_discarding_is_contained(Discard::Replace).await;
}

/// A canceled timer's message whose destructor panics is dropped under containment: the actor
/// keeps receiving.
#[tokio::test]
async fn a_panicking_destructor_of_a_canceled_timer_message_is_contained() {
    assert_discarding_is_contained(Discard::Cancel).await;
}

/// A `ReplyTo` inside a pending timer's message resolves its ask as `NoReply` at termination,
/// since termination drops the timer messages.
#[tokio::test]
async fn a_reply_to_in_a_pending_timer_resolves_as_no_reply_at_termination() {
    let (armed_tx, mut armed) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Holder(armed_tx));
    let root = system.root().clone();

    let asking = tokio::spawn(async move { root.ask(TIMEOUT, |reply_to| reply_to).await });
    next_event(&mut armed).await;
    stop(system).await;
    let reply = timeout(TIMEOUT, asking)
        .await
        .expect("the ask did not resolve")
        .expect("the asking task panicked");
    assert!(matches!(reply, Err(AskError::NoReply)), "{reply:?}");
}

/// A mailbox which never runs empty does not starve a due timer: the run loop fires a due timer
/// before it receives the next item, so the timer lands behind the one tick queued at that moment
/// rather than behind the whole flood, and the flood stops long before its bound.
#[tokio::test(start_paused = true)]
async fn a_flooded_mailbox_does_not_starve_a_due_timer() {
    let (ticks_tx, mut ticks_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Flooder(ticks_tx));

    let ticks = timeout(TIMEOUT, ticks_rx.recv())
        .await
        .expect("no tick count in time")
        .expect("flooder dropped the sender");
    assert!(ticks < FLOOD_BOUND / 100);
    stop(system).await;
}

/// Skipping a due timer which was canceled or replaced since it fired costs the run loop one
/// iteration, with its cooperative budget and its stop check, however many such timers wait at one
/// position. Hence an actor stopped while it skips them stops before the item queued behind them.
///
/// Scheduler sensitive, hence ignored by default: it passes only if Tokio runs the test task while
/// the actor still skips stale timers, which Tokio does not guarantee. Run it explicitly with
/// `cargo test -p tellus --test timers stale_timers_do_not_delay_a_stop -- --ignored`.
#[tokio::test(start_paused = true)]
#[ignore = "scheduler sensitive, run explicitly"]
async fn stale_timers_do_not_delay_a_stop() {
    let (stale_tx, mut stale_rx) = mpsc::unbounded_channel();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(StaleTimers(stale_tx)),
    );
    let (ends_tx, mut ends_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Replacer(ends_tx));

    // Every advance lets the actor's timer fire once more and go stale on its next replacement.
    let skipping = async {
        loop {
            select! {
                biased;

                _ = stale_rx.recv() => break,

                _ = advance(Duration::from_millis(1)) => {}
            }
        }
    };
    timeout(TIMEOUT, skipping)
        .await
        .expect("no stale timer was skipped");
    system.stop();
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
    assert!(
        ends_rx.try_recv().is_err(),
        "the item behind the stale timers was received"
    );
}

/// An actor which never starts a timer needs no time driver: on a runtime without one it receives a
/// message and stops as usual.
#[test]
fn an_actor_without_timers_needs_no_time_driver() {
    let runtime = Builder::new_current_thread()
        .build()
        .expect("a runtime without a time driver can be built");

    runtime.block_on(async {
        let (system, mut events) = probe_system(ActorConfig::default(), None);
        system.root().tell(Message::Ping);
        assert_eq!(events.recv().await, Some(Event::Ping));

        system.stop();
        system
            .terminated()
            .await
            .expect("watching the root actor failed");
    });
}

/// Starting a timer on a runtime without the time driver fails the actor like any panic in
/// `receive`, so the actor still terminates as usual.
#[test]
fn starting_a_timer_without_a_time_driver_fails_the_actor() {
    let (terminated_tx, terminated_rx) = sync::mpsc::channel();

    // Its own thread: a regression would leave the termination pending forever.
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .build()
            .expect("a runtime without a time driver can be built");

        runtime.block_on(async {
            let (system, mut events) = probe_system(ActorConfig::default(), None);
            system.root().tell(start("t", Duration::from_secs(1)));
            assert_eq!(events.recv().await, None);
            terminated_tx
                .send(system.terminated().await.is_ok())
                .expect("the test waits for the outcome");
        });
    });

    let terminated = terminated_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the actor terminates");
    assert!(terminated);
}

/// A machine which has not been initialized refuses a request.
#[tokio::test(start_paused = true)]
async fn vending_machine_errors_when_not_initialized() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Error("MachineNotInitialized")
    );
    stop(machine).await;
    stop(customer).await;
}

/// A request for a product which is not stocked is refused.
#[tokio::test(start_paused = true)]
async fn vending_machine_reports_a_product_not_available() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(stocked(10, 3));
    machine.root().tell(request(&customer, "sandwich"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Error("ProductNotAvailable")
    );
    stop(machine).await;
    stop(customer).await;
}

/// Without money the request times out, a money timeout after it.
#[tokio::test(start_paused = true)]
async fn vending_machine_times_out_if_no_money_is_inserted() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(stocked(10, 3));
    let requested = Instant::now();
    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 3 dollars".to_string())
    );

    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Error("RequestTimedOut")
    );
    assert_eq!(requested.elapsed(), MONEY_TIMEOUT);
    stop(machine).await;
    stop(customer).await;
}

/// Partial money restarts the wait: the timeout comes a money timeout after the last insertion, not
/// after the request, and the money inserted so far is given back.
#[tokio::test(start_paused = true)]
async fn vending_machine_handles_the_reception_of_partial_money() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(stocked(10, 3));
    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 3 dollars".to_string())
    );

    advance(MONEY_TIMEOUT / 2).await;
    let inserted = Instant::now();
    machine.root().tell(VendingMessage::ReceiveMoney(1));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 2 dollars".to_string())
    );

    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Error("RequestTimedOut")
    );
    // Counted from the money, not from the request: the money restarted the wait.
    assert_eq!(inserted.elapsed(), MONEY_TIMEOUT);
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::GiveBackChange(1)
    );
    stop(machine).await;
    stop(customer).await;
}

/// Inserting all the money delivers the product.
#[tokio::test(start_paused = true)]
async fn vending_machine_delivers_the_product_if_all_the_money_is_inserted() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(stocked(10, 3));
    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 3 dollars".to_string())
    );

    machine.root().tell(VendingMessage::ReceiveMoney(3));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Deliver("coke".to_string())
    );
    stop(machine).await;
    stop(customer).await;
}

/// Change is given back and the machine takes a new request. The first wait's timer is gone: one
/// timeout arrives, a money timeout after the new request, and nothing after it.
#[tokio::test(start_paused = true)]
async fn vending_machine_gives_back_change_and_takes_a_new_request() {
    let (customer, mut messages) = customer_system();
    let machine = ActorSystem::new(VendingMachine);

    machine.root().tell(stocked(10, 3));
    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 3 dollars".to_string())
    );

    machine.root().tell(VendingMessage::ReceiveMoney(4));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Deliver("coke".to_string())
    );
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::GiveBackChange(1)
    );

    // The first wait's deadline falls inside the new wait, where a stale timeout would show.
    advance(MONEY_TIMEOUT / 2).await;
    let requested = Instant::now();
    machine.root().tell(request(&customer, "coke"));
    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Instruction("Please insert 3 dollars".to_string())
    );

    assert_eq!(
        next_event(&mut messages).await,
        CustomerMessage::Error("RequestTimedOut")
    );
    assert_eq!(requested.elapsed(), MONEY_TIMEOUT);
    assert!(
        timeout(Duration::from_millis(1500), messages.recv())
            .await
            .is_err()
    );
    stop(machine).await;
    stop(customer).await;
}

struct Clock {
    events: mpsc::UnboundedSender<Event>,
    incarnations: AtomicU32,

    /// The incarnation whose `init` starts the timer "t", and the timer's delay.
    init_timer: Option<(u32, Duration)>,
}

impl Actor for Clock {
    type Message = Message;
    type Timers = Timed;
    type State = u32;
    type Error = Failed;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        let incarnation = self.incarnations.fetch_add(1, SeqCst) + 1;
        if let Some((init_incarnation, delay)) = self.init_timer
            && init_incarnation == incarnation
        {
            context.start_timer("t", Message::Fired("t"), delay);
        }
        Ok(incarnation)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        incarnation: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the probe watches no actor, hence never gets a terminated signal")
        };

        match message {
            Message::Start { key, delay } => context.start_timer(key, Message::Fired(key), delay),

            Message::Cancel(key) => context.cancel_timer(key),

            Message::Fired(key) => self
                .events
                .send(Event::Fired(key, incarnation))
                .expect("test dropped the receiver"),

            Message::Ping => self
                .events
                .send(Event::Ping)
                .expect("test dropped the receiver"),

            Message::Fail => return Err(Failed),

            Message::Block { delay, release } => {
                context.start_timer("t", Message::Fired("t"), delay);
                self.events
                    .send(Event::Blocked)
                    .expect("test dropped the receiver");
                release.recv().expect("test dropped the release");
            }
        }
        Ok(Control::Continue(incarnation))
    }
}

/// Spawns a timed [Clock] and hands its reference out.
struct UntimedParent {
    events: mpsc::UnboundedSender<Event>,
    child_tx: mpsc::UnboundedSender<ActorRef<Message>>,
}

impl Actor for UntimedParent {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let child = context.spawn(Clock {
            events: self.events.clone(),
            incarnations: AtomicU32::new(0),
            init_timer: None,
        });
        self.child_tx
            .send(child)
            .expect("test dropped the receiver");
        Ok(())
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

/// Spawns an untimed [Echo] and hands its reference out; timed itself, with a timer no test waits
/// for.
struct TimedParent {
    echoes: mpsc::UnboundedSender<u32>,
    child_tx: mpsc::UnboundedSender<ActorRef<u32>>,
}

impl Actor for TimedParent {
    type Message = ();
    type Timers = Timed;
    type State = ();
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        context.start_timer("t", (), Duration::from_secs(3600));
        let child = context.spawn(Echo(self.echoes.clone()));
        self.child_tx
            .send(child)
            .expect("test dropped the receiver");
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message, Timed>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

/// Forwards every message it receives.
struct Echo(mpsc::UnboundedSender<u32>);

impl Actor for Echo {
    type Message = u32;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(n) = incoming {
            self.0.send(n).expect("test dropped the receiver");
        }
        Ok(Control::Continue(state))
    }
}

enum Message {
    Start {
        key: &'static str,
        delay: Duration,
    },
    Cancel(&'static str),
    Fired(&'static str),
    Ping,
    Fail,

    /// Starts the timer "t" with the delay, then blocks `receive` until released.
    Block {
        delay: Duration,
        release: sync::mpsc::Receiver<()>,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Fired(&'static str, u32),
    Ping,
    Blocked,
}

#[derive(Debug, Error)]
#[error("probe failed on purpose")]
struct Failed;

/// Starts a timer in `init` whose message panics when dropped, with a delay no test waits for.
struct PanickingTimer(Arc<AtomicBool>);

impl Actor for PanickingTimer {
    type Message = PanickingMessage;
    type Timers = Timed;
    type State = ();
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        context.start_timer(
            "t",
            PanickingMessage(self.0.clone()),
            Duration::from_secs(3600),
        );
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message, Timed>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}

/// Records that it was dropped, then panics.
struct PanickingMessage(Arc<AtomicBool>);

impl Drop for PanickingMessage {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
        panic!("timer message destructor panics on purpose");
    }
}

/// Starts a timer in `init` whose message panics when dropped, then replaces or cancels it on
/// command and answers a ping.
struct Discarder {
    dropped: Arc<AtomicBool>,
    pongs: mpsc::UnboundedSender<()>,
}

impl Actor for Discarder {
    type Message = Discard;
    type Timers = Timed;
    type State = ();
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        let held = Discard::Held {
            _message: PanickingMessage(self.dropped.clone()),
        };
        context.start_timer("t", held, Duration::from_secs(3600));
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the discarder watches no actor, hence never gets a terminated signal")
        };

        match message {
            Discard::Replace => context.start_timer("t", Discard::Ping, Duration::from_secs(3600)),

            Discard::Cancel => context.cancel_timer("t"),

            Discard::Ping => self.pongs.send(()).expect("test dropped the receiver"),

            Discard::Held { .. } => unreachable!("the held timer is never due in a test"),
        }
        Ok(Control::Continue(state))
    }
}

enum Discard {
    Replace,
    Cancel,
    Ping,
    Held { _message: PanickingMessage },
}

/// Holds the `ReplyTo` it receives in a timer no test waits for, and reports that it does.
struct Holder(mpsc::UnboundedSender<()>);

impl Actor for Holder {
    type Message = ReplyTo<u32>;
    type Timers = Timed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message, Timed>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        if let Incoming::Message(reply_to) = incoming {
            context.start_timer("t", reply_to, Duration::from_secs(3600));
            self.0.send(()).expect("test dropped the receiver");
        }
        Ok(Control::Continue(state))
    }
}

/// Floods its own mailbox with ticks, next to a zero-delay timer, until the timer's message arrives
/// or the bound is reached, and then reports how many ticks it has handled.
struct Flooder(mpsc::UnboundedSender<u32>);

impl Actor for Flooder {
    type Message = Flood;
    type Timers = Timed;
    type State = u32;
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        context.start_timer("t", Flood::Timeout, Duration::ZERO);
        context.self_ref().tell(Flood::Tick);
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        ticks: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the flooder watches no actor, hence never gets a terminated signal")
        };

        match message {
            Flood::Tick => {
                let ticks = ticks + 1;
                if ticks < FLOOD_BOUND {
                    context.self_ref().tell(Flood::Tick);
                    return Ok(Control::Continue(ticks));
                }
                self.0.send(ticks).expect("test dropped the receiver");
            }

            Flood::Timeout => self.0.send(ticks).expect("test dropped the receiver"),
        }
        Ok(Control::Stop)
    }
}

enum Flood {
    Tick,
    Timeout,
}

/// Tells itself a backlog of ticks and replaces its timer on every tick, so each time the timer
/// fires during the backlog, its next replacement leaves a stale timer behind. The last tick tells
/// it `End`, which hence lands behind all of these. Reports `End` once received.
struct Replacer(mpsc::UnboundedSender<()>);

impl Actor for Replacer {
    type Message = Backlog;
    type Timers = Timed;
    type State = u32;
    type Error = Infallible;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        for _ in 0..BACKLOG {
            context.self_ref().tell(Backlog::Tick);
        }
        context.start_timer("t", Backlog::Timeout, Duration::from_millis(1));
        Ok(0)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        ticks: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the replacer watches no actor, hence never gets a terminated signal")
        };

        match message {
            Backlog::Tick => {
                context.start_timer("t", Backlog::Timeout, Duration::from_millis(1));
                let ticks = ticks + 1;
                if ticks == BACKLOG {
                    context.self_ref().tell(Backlog::End);
                }
                Ok(Control::Continue(ticks))
            }

            Backlog::End => {
                self.0.send(()).expect("test dropped the receiver");
                Ok(Control::Continue(ticks))
            }

            Backlog::Timeout => Ok(Control::Continue(ticks)),
        }
    }
}

enum Backlog {
    Tick,
    End,
    Timeout,
}

/// Reports every event of a run loop dropping a stale timer.
struct StaleTimers(mpsc::UnboundedSender<()>);

impl<S> Layer<S> for StaleTimers
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut stale_timer = StaleTimer(false);
        event.record(&mut stale_timer);
        if stale_timer.0 {
            let _ = self.0.send(());
        }
    }
}

struct StaleTimer(bool);

impl Visit for StaleTimer {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" && format!("{value:?}") == "dropping stale timer" {
            self.0 = true;
        }
    }
}

/// A vending machine which sells the products of its inventory at their prices. A request for a
/// stocked product starts a wait for money, a timer of `MONEY_TIMEOUT`. Partial money restarts the
/// timer and delivery cancels it. If the timer expires first, the request times out and the money
/// inserted so far is given back.
struct VendingMachine;

impl Actor for VendingMachine {
    type Message = VendingMessage;
    type Timers = Timed;
    type State = Phase;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message, Timed>) -> Result<Self::State, Self::Error> {
        Ok(Phase::Idle)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        phase: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the machine watches no actor, hence never gets a terminated signal")
        };

        let next = match (phase, message) {
            (Phase::Idle, VendingMessage::Initialize { inventory, prices }) => {
                Phase::Operational { inventory, prices }
            }

            (Phase::Idle, VendingMessage::RequestProduct { customer, .. }) => {
                customer.tell(CustomerMessage::Error("MachineNotInitialized"));
                Phase::Idle
            }

            (
                Phase::Operational { inventory, prices },
                VendingMessage::RequestProduct { product, customer },
            ) => match inventory.get(&product) {
                None | Some(0) => {
                    customer.tell(CustomerMessage::Error("ProductNotAvailable"));
                    Phase::Operational { inventory, prices }
                }

                Some(_) => {
                    let price = prices[&product];
                    customer.tell(CustomerMessage::Instruction(format!(
                        "Please insert {price} dollars"
                    )));
                    context.start_timer("money", VendingMessage::MoneyTimeout, MONEY_TIMEOUT);
                    Phase::WaitForMoney {
                        inventory,
                        prices,
                        product,
                        money: 0,
                        customer,
                    }
                }
            },

            (
                Phase::WaitForMoney {
                    inventory,
                    prices,
                    money,
                    customer,
                    ..
                },
                VendingMessage::MoneyTimeout,
            ) => {
                customer.tell(CustomerMessage::Error("RequestTimedOut"));
                if money > 0 {
                    customer.tell(CustomerMessage::GiveBackChange(money));
                }
                Phase::Operational { inventory, prices }
            }

            (
                Phase::WaitForMoney {
                    mut inventory,
                    prices,
                    product,
                    money,
                    customer,
                },
                VendingMessage::ReceiveMoney(amount),
            ) => {
                let price = prices[&product];
                let total = money + amount;
                if total >= price {
                    context.cancel_timer("money");
                    customer.tell(CustomerMessage::Deliver(product.clone()));
                    let change = total - price;
                    if change > 0 {
                        customer.tell(CustomerMessage::GiveBackChange(change));
                    }
                    *inventory
                        .get_mut(&product)
                        .expect("a requested product is stocked") -= 1;
                    Phase::Operational { inventory, prices }
                } else {
                    let remaining = price - total;
                    customer.tell(CustomerMessage::Instruction(format!(
                        "Please insert {remaining} dollars"
                    )));
                    context.start_timer("money", VendingMessage::MoneyTimeout, MONEY_TIMEOUT);
                    Phase::WaitForMoney {
                        inventory,
                        prices,
                        product,
                        money: total,
                        customer,
                    }
                }
            }

            // Anything else is unhandled and dropped.
            (phase, _) => phase,
        };
        Ok(Control::Continue(next))
    }
}

enum VendingMessage {
    Initialize {
        inventory: HashMap<String, u32>,
        prices: HashMap<String, u32>,
    },

    RequestProduct {
        product: String,
        customer: ActorRef<CustomerMessage>,
    },

    ReceiveMoney(u32),

    /// From the timer "money": the wait for money has lasted too long.
    MoneyTimeout,
}

enum Phase {
    Idle,

    Operational {
        inventory: HashMap<String, u32>,
        prices: HashMap<String, u32>,
    },

    WaitForMoney {
        inventory: HashMap<String, u32>,
        prices: HashMap<String, u32>,
        product: String,
        money: u32,
        customer: ActorRef<CustomerMessage>,
    },
}

/// The customer: hands every message it gets to the test.
struct Customer(mpsc::UnboundedSender<CustomerMessage>);

impl Actor for Customer {
    type Message = CustomerMessage;
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the customer watches no actor, hence never gets a terminated signal")
        };

        self.0.send(message).expect("test dropped the receiver");
        Ok(Control::Continue(state))
    }
}

/// What the machine tells the customer.
#[derive(Debug, PartialEq, Eq)]
enum CustomerMessage {
    Instruction(String),
    Deliver(String),
    GiveBackChange(u32),
    Error(&'static str),
}

fn customer_system() -> (
    ActorSystem<CustomerMessage>,
    mpsc::UnboundedReceiver<CustomerMessage>,
) {
    let (messages_tx, messages_rx) = mpsc::unbounded_channel();
    (ActorSystem::new(Customer(messages_tx)), messages_rx)
}

fn stocked(cokes: u32, price: u32) -> VendingMessage {
    VendingMessage::Initialize {
        inventory: HashMap::from([("coke".to_string(), cokes)]),
        prices: HashMap::from([("coke".to_string(), price)]),
    }
}

fn request(customer: &ActorSystem<CustomerMessage>, product: &str) -> VendingMessage {
    VendingMessage::RequestProduct {
        product: product.to_string(),
        customer: customer.root().clone(),
    }
}

fn probe_system(
    config: ActorConfig,
    init_timer: Option<(u32, Duration)>,
) -> (ActorSystem<Message>, mpsc::UnboundedReceiver<Event>) {
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let timed = Clock {
        events: events_tx,
        incarnations: AtomicU32::new(0),
        init_timer,
    };
    (ActorSystem::with_config(timed, config), events_rx)
}

fn start(key: &'static str, delay: Duration) -> Message {
    Message::Start { key, delay }
}

/// Round-trip a `Ping`, so the probe has handled everything told before.
async fn ping(system: &ActorSystem<Message>, events: &mut mpsc::UnboundedReceiver<Event>) {
    system.root().tell(Message::Ping);
    assert_eq!(next_event(events).await, Event::Ping);
}

async fn next_event<T>(events: &mut mpsc::UnboundedReceiver<T>) -> T {
    timeout(TIMEOUT, events.recv())
        .await
        .expect("no event in time")
        .expect("probe dropped the sender")
}

async fn stop<M>(system: ActorSystem<M>)
where
    M: Send + 'static,
{
    system.stop();
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}

/// Replaces or cancels the [Discarder]'s timer, whose message panics when dropped, and checks
/// that the destructor ran and the actor still answers.
async fn assert_discarding_is_contained(discard: Discard) {
    let dropped = Arc::new(AtomicBool::new(false));
    let (pongs_tx, mut pongs) = mpsc::unbounded_channel();
    let system = ActorSystem::new(Discarder {
        dropped: dropped.clone(),
        pongs: pongs_tx,
    });

    system.root().tell(discard);
    system.root().tell(Discard::Ping);
    next_event(&mut pongs).await;
    assert!(dropped.load(SeqCst));
    stop(system).await;
}
