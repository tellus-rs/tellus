//! How-to example for retrying failed writes: a producer sends numbered entries to a journal which
//! writes them in batches to a store. A failed batch is retried with backoff through a timer. A
//! batch the journal gives up on is resent by the producer once its acknowledgment times out. The
//! keyed writes keep every entry in the store exactly once.

use anyhow::Context;
use std::{
    collections::{BTreeMap, VecDeque, btree_map::Entry as MapEntry},
    num::NonZeroU32,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, Backoff, Control, Incoming, ReplyTo,
    RestartPolicy, SupervisionStrategy, Timed, Unstashed,
};
use thiserror::Error;
use tokio::{
    task::{self, JoinError},
    time::sleep,
};

const TOTAL: u64 = 2_000;
const WINDOW: usize = 256;
const MAX_BATCH: usize = 100;
const MAX_ATTEMPTS: u32 = 3;
const MAX_RESTARTS: NonZeroU32 = NonZeroU32::new(2).unwrap();
const BACKOFF_MIN: Duration = Duration::from_millis(10);
const BACKOFF_MAX: Duration = Duration::from_millis(80);
// Longer than every batch ahead in the window retried MAX_ATTEMPTS times, plus one restart!
const ACK_TIMEOUT: Duration = Duration::from_millis(500);
const WRITE_LATENCY: Duration = Duration::from_millis(2);
const RESET_BEFORE_WRITE_SEQ: u64 = 301;
const RESET_AFTER_WRITE_SEQ: u64 = 901;
const GIVE_UP_SEQ: u64 = 1_501;

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Store::default();
    let backoff = Backoff::new(BACKOFF_MIN, BACKOFF_MAX)?;
    let system = ActorSystem::new(Producer {
        store: store.clone(),
        backoff,
    });
    system
        .terminated()
        .await
        .context("awaiting actor system termination")?;

    let table = store.table();
    println!(
        "{} rows written in {} batches, every key 1..={TOTAL} present once: {}, duplicate writes skipped: {}",
        table.rows.len(),
        table.writes,
        table.rows.keys().copied().eq(1..=TOTAL),
        table.skipped
    );
    Ok(())
}
// ANCHOR_END: main

// ANCHOR: producer
struct Producer {
    store: Store,
    backoff: Backoff,
}

impl Actor for Producer {
    type Message = ProducerMessage;
    type Timers = Timed;
    type Stash = Unstashed;
    type State = Sending;
    type Error = JournalStopped;

    fn init(
        &self,
        context: &ActorContext<Self::Message, Timed>,
    ) -> Result<Self::State, Self::Error> {
        let policy = RestartPolicy::new(MAX_RESTARTS).with_backoff(self.backoff);
        let config =
            ActorConfig::default().with_supervision_strategy(SupervisionStrategy::Restart(policy));
        let journal = context.spawn_with_config(
            Journal {
                store: self.store.clone(),
                backoff: self.backoff,
            },
            config,
        );
        context.watch(&journal);

        let mut sending = Sending {
            journal,
            next: 1,
            open: VecDeque::new(),
        };
        sending.fill_window(context);
        Ok(sending)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        mut sending: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(ProducerMessage::Acked(seq)) => {
                sending.acknowledge(seq, context);
                sending.fill_window(context);
                if sending.next > TOTAL && sending.open.is_empty() {
                    return Ok(Control::Stop);
                }
            }

            Incoming::Message(ProducerMessage::AckTimeout) => sending.resend_open(context),

            Incoming::Terminated(_) => {
                return Err(JournalStopped {
                    open: sending.open.len(),
                });
            }
        }
        Ok(Control::Continue(sending))
    }
}

enum ProducerMessage {
    Acked(u64),
    AckTimeout,
}

#[derive(Debug, Error)]
#[error("the journal stopped for good with {open} entries unacknowledged")]
struct JournalStopped {
    open: usize,
}

struct Sending {
    journal: ActorRef<JournalMessage>,
    next: u64,
    open: VecDeque<(u64, Instant)>,
}

impl Sending {
    // ANCHOR: window
    fn fill_window(&mut self, context: &ActorContext<ProducerMessage, Timed>) {
        while self.open.len() < WINDOW && self.next <= TOTAL {
            let seq = self.next;
            self.next += 1;
            self.append(seq, context);
            self.open.push_back((seq, Instant::now()));
        }
    }

    fn append(&self, seq: u64, context: &ActorContext<ProducerMessage, Timed>) {
        if self.open.is_empty() {
            context.start_timer("ack", ProducerMessage::AckTimeout, ACK_TIMEOUT);
        }
        self.journal.tell(JournalMessage::Append {
            seq,
            text: format!("entry {seq}"),
            reply_to: context.reply_to(move |()| ProducerMessage::Acked(seq)),
        });
    }
    // ANCHOR_END: window

    // ANCHOR: ack-timer
    fn acknowledge(&mut self, seq: u64, context: &ActorContext<ProducerMessage, Timed>) {
        // A resent append is acknowledged twice!
        let Some(position) = self.open.iter().position(|(open, _)| *open == seq) else {
            return;
        };
        self.open.remove(position);
        if position == 0 {
            match self.open.front() {
                Some((_, sent)) => {
                    let remaining = ACK_TIMEOUT.saturating_sub(sent.elapsed());
                    context.start_timer("ack", ProducerMessage::AckTimeout, remaining);
                }

                None => context.cancel_timer("ack"),
            }
        }
    }
    // ANCHOR_END: ack-timer

    // ANCHOR: resend
    fn resend_open(&mut self, context: &ActorContext<ProducerMessage, Timed>) {
        println!(
            "Resending {} entries after {} ms without an ack",
            self.open.len(),
            ACK_TIMEOUT.as_millis()
        );
        let now = Instant::now();
        for (seq, sent) in &mut self.open {
            let seq = *seq;
            self.journal.tell(JournalMessage::Append {
                seq,
                text: format!("entry {seq}"),
                reply_to: context.reply_to(move |()| ProducerMessage::Acked(seq)),
            });
            *sent = now;
        }
        context.start_timer("ack", ProducerMessage::AckTimeout, ACK_TIMEOUT);
    }
    // ANCHOR_END: resend
}
// ANCHOR_END: producer

// ANCHOR: journal
struct Journal {
    store: Store,
    backoff: Backoff,
}

impl Actor for Journal {
    type Message = JournalMessage;
    type Timers = Timed;
    type Stash = Unstashed;
    type State = Buffer;
    type Error = WriteFailed;

    fn init(&self, _: &ActorContext<Self::Message, Timed>) -> Result<Self::State, Self::Error> {
        println!("Journal started");
        Ok(Buffer {
            pending: Vec::new(),
            in_flight: Vec::new(),
            attempts: 0,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Timed>,
        incoming: Incoming<Self::Message>,
        mut buffer: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the journal watches no actor, hence never gets a terminated signal")
        };

        match message {
            JournalMessage::Append {
                seq,
                text,
                reply_to,
            } => buffer.pending.push(Entry {
                seq,
                text,
                reply_to,
            }),

            // ANCHOR: retry
            JournalMessage::Written(result) => match result? {
                Ok(()) => {
                    for entry in buffer.in_flight.drain(..) {
                        entry.reply_to.reply(());
                    }
                }

                Err(error) if buffer.attempts < MAX_ATTEMPTS => {
                    let delay = self.retry_delay(buffer.attempts);
                    println!(
                        "Retrying a batch of {} entries in {} ms: {error}",
                        buffer.in_flight.len(),
                        delay.as_millis()
                    );
                    context.start_timer("retry", JournalMessage::Retry, delay);
                }

                Err(error) => {
                    println!(
                        "Giving up on a batch of {} entries after {} attempts: {error}",
                        buffer.in_flight.len(),
                        buffer.attempts
                    );
                    return Err(WriteFailed::GaveUp {
                        attempts: buffer.attempts,
                        error,
                    });
                }
            },
            // ANCHOR_END: retry
            JournalMessage::Retry => buffer.write_in_flight(self, context),
        }

        buffer.write_if_due(self, context);

        Ok(Control::Continue(buffer))
    }
}

impl Journal {
    fn retry_delay(&self, attempts: u32) -> Duration {
        let factor = 1u32.checked_shl(attempts - 1).unwrap_or(u32::MAX);
        self.backoff
            .min()
            .saturating_mul(factor)
            .min(self.backoff.max())
    }
}

enum JournalMessage {
    Append {
        seq: u64,
        text: String,
        reply_to: ReplyTo<()>,
    },
    Written(Result<Result<(), ConnectionReset>, JoinError>),
    Retry,
}

#[derive(Debug, Error)]
enum WriteFailed {
    #[error("writing a batch failed after {attempts} attempts")]
    GaveUp {
        attempts: u32,
        #[source]
        error: ConnectionReset,
    },

    #[error("writing a batch panicked")]
    Panicked(#[from] JoinError),
}

struct Buffer {
    pending: Vec<Entry>,
    in_flight: Vec<Entry>,
    attempts: u32,
}

impl Buffer {
    fn write_if_due(&mut self, journal: &Journal, context: &ActorContext<JournalMessage, Timed>) {
        if self.in_flight.is_empty()
            && !self.pending.is_empty()
            && (context.queued() == 0 || self.pending.len() >= MAX_BATCH)
        {
            let len = self.pending.len().min(MAX_BATCH);
            self.in_flight = self.pending.drain(..len).collect();
            self.attempts = 0;
            self.write_in_flight(journal, context);
        }
    }

    fn write_in_flight(
        &mut self,
        journal: &Journal,
        context: &ActorContext<JournalMessage, Timed>,
    ) {
        self.attempts += 1;
        let batch = self
            .in_flight
            .iter()
            .map(|entry| (entry.seq, entry.text.clone()))
            .collect::<Vec<_>>();
        context.pipe_to_self(
            task::spawn(write(journal.store.clone(), batch)),
            JournalMessage::Written,
        );
    }
}

struct Entry {
    seq: u64,
    text: String,
    reply_to: ReplyTo<()>,
}
// ANCHOR_END: journal

// ANCHOR: write
#[derive(Clone, Default)]
struct Store(Arc<Mutex<Table>>);

impl Store {
    fn table(&self) -> MutexGuard<'_, Table> {
        self.0.lock().expect("the table lock is not poisoned")
    }
}

#[derive(Default)]
struct Table {
    rows: BTreeMap<u64, String>,
    writes: u64,
    skipped: u64,
    reset_before_write: bool,
    reset_after_write: bool,
    give_up_failures: u32,
}

impl Table {
    fn write(&mut self, batch: &[(u64, String)]) -> Result<(), ConnectionReset> {
        let contains = |wanted| batch.iter().any(|(seq, _)| *seq == wanted);
        if contains(RESET_BEFORE_WRITE_SEQ) && !self.reset_before_write {
            self.reset_before_write = true;
            return Err(ConnectionReset);
        }
        if contains(GIVE_UP_SEQ) && self.give_up_failures < MAX_ATTEMPTS {
            self.give_up_failures += 1;
            return Err(ConnectionReset);
        }

        let mut skipped = 0;
        for (seq, text) in batch {
            match self.rows.entry(*seq) {
                MapEntry::Vacant(vacant) => {
                    vacant.insert(text.clone());
                }

                MapEntry::Occupied(_) => skipped += 1,
            }
        }
        self.writes += 1;
        self.skipped += skipped;
        if skipped > 0 {
            println!("Skipped {skipped} duplicates");
        }

        if contains(RESET_AFTER_WRITE_SEQ) && !self.reset_after_write {
            self.reset_after_write = true;
            return Err(ConnectionReset);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Error)]
#[error("connection reset")]
struct ConnectionReset;

async fn write(store: Store, batch: Vec<(u64, String)>) -> Result<(), ConnectionReset> {
    sleep(WRITE_LATENCY).await;
    store.table().write(&batch)
}
// ANCHOR_END: write
