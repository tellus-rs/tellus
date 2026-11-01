//! How-to example for batching writes: a journal actor buffers entries and writes them in batches
//! with `ActorContext::pipe_to_self`, one write at a time, starting a write once its mailbox has
//! run empty or a full batch is buffered. A failed or panicking write fails the journal.

use anyhow::Context;
use std::{io, time::Duration};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, Unstashed, Untimed};
use thiserror::Error;
use tokio::{
    task::{self, JoinError},
    time::sleep,
};

const MAX_BATCH: usize = 100;

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Journal);
    let journal = system.root();

    let mut n = 0;
    for burst in [250, 30, 5] {
        for _ in 0..burst {
            n += 1;
            journal.tell(Message::Append(format!("entry {n}")));
        }
        sleep(Duration::from_millis(100)).await;
    }

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

// ANCHOR: journal
struct Journal;

impl Actor for Journal {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Buffer;
    type Error = WriteFailed;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(Buffer {
            entries: Vec::new(),
            writing: false,
        })
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut buffer: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the journal watches no actor, hence never gets a terminated signal")
        };

        match message {
            Message::Append(entry) => buffer.entries.push(entry),

            Message::Written(result) => {
                result??;
                buffer.writing = false;
            }
        }

        buffer.write_if_due(context);

        Ok(Control::Continue(buffer))
    }
}

enum Message {
    Append(String),
    Written(Result<io::Result<()>, JoinError>),
}

#[derive(Debug, Error)]
enum WriteFailed {
    #[error("writing a batch failed")]
    Io(#[from] io::Error),

    #[error("writing a batch panicked")]
    Panicked(#[from] JoinError),
}

struct Buffer {
    entries: Vec<String>,
    writing: bool,
}

// ANCHOR: write-if-due
impl Buffer {
    fn write_if_due(&mut self, context: &ActorContext<Message>) {
        if !self.writing
            && !self.entries.is_empty()
            && (context.queued() == 0 || self.entries.len() >= MAX_BATCH)
        {
            self.writing = true;
            let len = self.entries.len().min(MAX_BATCH);
            let batch = self.entries.drain(..len).collect::<Vec<_>>();
            context.pipe_to_self(task::spawn(write(batch)), Message::Written);
        }
    }
}
// ANCHOR_END: write-if-due

async fn write(batch: Vec<String>) -> io::Result<()> {
    sleep(Duration::from_millis(10)).await;
    println!("Wrote a batch of {} entries", batch.len());
    Ok(())
}
// ANCHOR_END: journal
