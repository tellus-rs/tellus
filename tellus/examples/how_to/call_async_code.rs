//! How-to example for calling async code from an actor: a directory actor looks up customers in a
//! simulated database with `ActorContext::pipe_to_self`, and runs a blocking computation the same
//! way via `tokio::task::spawn_blocking`.

use anyhow::Context;
use std::{convert::Infallible, time::Duration};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed};
use thiserror::Error;
use tokio::{
    task::{self, JoinError},
    time::sleep,
};

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let system = ActorSystem::new(Directory);
    let directory = system.root();

    for id in [1, 2, 3] {
        let customer = directory
            .ask(Duration::from_secs(1), |reply_to| Message::Lookup {
                id,
                reply_to,
            })
            .await
            .context("asking for a customer")?;
        match customer {
            Ok(name) => println!("Customer {id}: {name}"),
            Err(error) => println!("Customer {id}: {error}"),
        }
    }

    let checksum = directory
        .ask(Duration::from_secs(1), |reply_to| Message::Checksum {
            data: b"tellus".to_vec(),
            reply_to,
        })
        .await
        .context("asking for a checksum")?
        .context("computing the checksum")?;
    println!("Checksum: {checksum:x}");

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

struct Directory;

impl Actor for Directory {
    type Message = Message;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        _: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the directory watches no actor, hence never gets a terminated signal")
        };

        match message {
            // ANCHOR: lookup
            Message::Lookup { id, reply_to } => {
                context.pipe_to_self(find_customer(id), move |customer| Message::Found {
                    customer,
                    reply_to,
                });
            }

            Message::Found { customer, reply_to } => reply_to.reply(customer),
            // ANCHOR_END: lookup

            // ANCHOR: checksum
            Message::Checksum { data, reply_to } => {
                let computation = task::spawn_blocking(move || checksum(&data));
                context.pipe_to_self(computation, move |checksum| Message::Computed {
                    checksum,
                    reply_to,
                });
            }

            Message::Computed { checksum, reply_to } => reply_to.reply(checksum),
            // ANCHOR_END: checksum
        }
        Ok(Control::Continue(()))
    }
}

// ANCHOR: message
enum Message {
    Lookup {
        id: u64,
        reply_to: ReplyTo<Result<String, UnknownCustomer>>,
    },

    Found {
        customer: Result<String, UnknownCustomer>,
        reply_to: ReplyTo<Result<String, UnknownCustomer>>,
    },

    Checksum {
        data: Vec<u8>,
        reply_to: ReplyTo<Result<u64, JoinError>>,
    },

    Computed {
        checksum: Result<u64, JoinError>,
        reply_to: ReplyTo<Result<u64, JoinError>>,
    },
}
// ANCHOR_END: message

// ANCHOR: find-customer
async fn find_customer(id: u64) -> Result<String, UnknownCustomer> {
    sleep(Duration::from_millis(50)).await;

    match id {
        1 => Ok("Alice".to_string()),
        2 => Ok("Bob".to_string()),
        _ => Err(UnknownCustomer(id)),
    }
}
// ANCHOR_END: find-customer

fn checksum(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[derive(Debug, Error)]
#[error("unknown customer {0}")]
struct UnknownCustomer(u64);
