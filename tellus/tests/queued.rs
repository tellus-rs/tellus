use std::{convert::Infallible, time::Duration};
use tellus::{Actor, ActorContext, ActorSystem, Control, Incoming};
use tokio::{sync::mpsc, time::timeout};

const TIMEOUT: Duration = Duration::from_secs(5);
const MESSAGES: usize = 1_000;

/// An actor which floods itself sees, on each of those messages, exactly the ones still queued
/// behind it: nothing else sends to it, and only the actor itself removes items, so the depth
/// counts down from all but the first to none.
#[tokio::test]
async fn an_actor_sees_what_is_queued_behind_the_current_message() {
    let (depths_tx, mut depths_rx) = mpsc::unbounded_channel();
    let system = ActorSystem::new(SelfFlooder { depths: depths_tx });

    system.root().tell(Message::Flood);

    for expected in (0..MESSAGES).rev() {
        let depth = timeout(TIMEOUT, depths_rx.recv())
            .await
            .expect("no depth in time")
            .expect("flooder dropped the sender");
        assert_eq!(depth, expected);
    }

    system.stop();
    timeout(TIMEOUT, system.terminated())
        .await
        .expect("actor system did not terminate")
        .expect("watching the root actor failed");
}

struct SelfFlooder {
    depths: mpsc::UnboundedSender<usize>,
}

impl Actor for SelfFlooder {
    type Message = Message;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        (): Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(Message::Flood) => {
                for _ in 0..MESSAGES {
                    context.self_ref().tell(Message::Tick);
                }
            }

            Incoming::Message(Message::Tick) => self
                .depths
                .send(context.queued())
                .expect("test dropped the receiver"),

            Incoming::Terminated(_) => {
                unreachable!("the flooder watches no actor, hence never gets a terminated signal")
            }
        }
        Ok(Control::Continue(()))
    }
}

enum Message {
    Flood,
    Tick,
}
