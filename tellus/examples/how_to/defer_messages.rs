//! How-to example for deferring messages while an actor waits: a directory actor loads its
//! customer table with `ActorContext::pipe_to_self`, stashes the lookups arriving meanwhile and
//! answers them in order once the table is in; a lookup the full stash hands back gets `Busy`.

use anyhow::Context;
use std::{collections::HashMap, convert::Infallible, num::NonZeroUsize, time::Duration};
use tellus::{
    Actor, ActorConfig, ActorContext, ActorRef, ActorSystem, AskError, Control, Incoming,
    MailboxCapacity, ReplyTo, Stashed, Untimed,
};
use thiserror::Error;
use tokio::{task::yield_now, time::sleep};

const MAILBOX_CAPACITY: NonZeroUsize = NonZeroUsize::new(3).unwrap();

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config =
        ActorConfig::default().with_mailbox_capacity(MailboxCapacity::Bounded(MAILBOX_CAPACITY));
    let system = ActorSystem::with_config(Directory, config);
    let directory = system.root();

    let (first, second, third, fourth) = tokio::join!(
        lookup(directory, 1),
        lookup(directory, 2),
        lookup(directory, 3),
        async {
            // Full until the directory has received one of the first three lookups!
            loop {
                match lookup(directory, 4).await {
                    Err(AskError::MailboxFull) => yield_now().await,
                    result => break result,
                }
            }
        },
    );
    for (id, customer) in [(1, first?), (2, second?), (3, third?), (4, fourth?)] {
        print_customer(id, customer);
    }

    print_customer(9, lookup(directory, 9).await?);

    system.stop();
    system
        .terminated()
        .await
        .context("awaiting actor system termination")
}
// ANCHOR_END: main

struct Directory;

impl Actor for Directory {
    // ANCHOR: types
    type Message = Message;
    type Timers = Untimed;
    type Stash = Stashed;
    type State = State;
    type Error = Infallible;
    // ANCHOR_END: types

    // ANCHOR: init
    fn init(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
    ) -> Result<Self::State, Self::Error> {
        context.pipe_to_self(load_customers(), Message::Loaded);
        Ok(State::Loading)
    }
    // ANCHOR_END: init

    fn receive(
        &self,
        context: &ActorContext<Self::Message, Untimed, Stashed>,
        incoming: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let state = match (state, incoming) {
            // ANCHOR: loading
            (State::Loading, Incoming::Message(Message::Loaded(customers))) => {
                println!("Loaded {} customers", customers.len());
                context.unstash_all();
                State::Ready { customers }
            }

            (State::Loading, incoming) => {
                match context.stash(incoming) {
                    Ok(()) => {}
                    Err(Incoming::Message(Message::Lookup { reply_to, .. })) => {
                        reply_to.reply(Err(LookupError::Busy));
                    }
                    Err(_) => unreachable!("only a lookup can be refused"),
                }
                State::Loading
            }
            // ANCHOR_END: loading

            // ANCHOR: ready
            (State::Ready { customers }, Incoming::Message(Message::Lookup { id, reply_to })) => {
                reply_to.reply(customers.get(&id).cloned().ok_or(LookupError::Unknown(id)));
                State::Ready { customers }
            }
            // ANCHOR_END: ready
            (State::Ready { .. }, Incoming::Message(Message::Loaded(_))) => {
                unreachable!("the table is loaded once")
            }

            (State::Ready { .. }, Incoming::Terminated(_)) => {
                unreachable!("the directory watches no actor, hence never gets a terminated signal")
            }
        };
        Ok(Control::Continue(state))
    }
}

// ANCHOR: message
enum Message {
    Lookup {
        id: u64,
        reply_to: ReplyTo<Result<String, LookupError>>,
    },

    Loaded(HashMap<u64, String>),
}
// ANCHOR_END: message

// ANCHOR: state
enum State {
    Loading,
    Ready { customers: HashMap<u64, String> },
}
// ANCHOR_END: state

async fn lookup(
    directory: &ActorRef<Message>,
    id: u64,
) -> Result<Result<String, LookupError>, AskError> {
    directory
        .ask(Duration::from_secs(1), |reply_to| Message::Lookup {
            id,
            reply_to,
        })
        .await
}

fn print_customer(id: u64, customer: Result<String, LookupError>) {
    match customer {
        Ok(name) => println!("Customer {id}: {name}"),
        Err(error) => println!("Customer {id}: {error}"),
    }
}

// ANCHOR: load-customers
async fn load_customers() -> HashMap<u64, String> {
    sleep(Duration::from_millis(300)).await;

    [(1, "Alice"), (2, "Bob"), (3, "Carol")]
        .into_iter()
        .map(|(id, name)| (id, name.to_string()))
        .collect()
}
// ANCHOR_END: load-customers

#[derive(Debug, Error)]
enum LookupError {
    #[error("busy, try again later")]
    Busy,

    #[error("unknown customer {0}")]
    Unknown(u64),
}
