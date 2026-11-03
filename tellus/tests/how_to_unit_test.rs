#![cfg(feature = "test-util")]

use std::{assert_matches, collections::HashMap, convert::Infallible};
use tellus::{
    Actor, ActorContext, ActorRef, Control, Incoming, ReplyTo, Unstashed, Untimed,
    testing::TestContext,
};

// ANCHOR: record
/// A reading becomes the device's state, without any actor system running.
#[test]
fn a_reading_becomes_the_state() {
    let test_context = TestContext::new();

    let result = Device.receive(
        test_context.context(),
        Incoming::Message(DeviceMessage::Record(21.5)),
        None,
    );

    assert_matches!(result, Ok(Control::Continue(Some(21.5))));
}
// ANCHOR_END: record

// ANCHOR: reply
/// A read replies with the last reading: a second `TestContext` stands in for the asker and
/// receives the reply.
#[test]
fn a_read_replies_with_the_last_reading() {
    let test_context = TestContext::new();
    let mut asker = TestContext::<Option<f64>>::new();
    let reply_to = asker.context().reply_to(|last_reading| last_reading);

    let result = Device.receive(
        test_context.context(),
        Incoming::Message(DeviceMessage::Read(reply_to)),
        Some(21.5),
    );

    assert_matches!(result, Ok(Control::Continue(Some(21.5))));
    assert_eq!(asker.take_incoming(), [Incoming::Message(Some(21.5))]);
}
// ANCHOR_END: reply

// ANCHOR: children
/// A group spawns a device for an unknown name and forgets it once it has terminated. Spawning
/// starts a real child actor, hence the Tokio runtime.
#[tokio::test]
async fn a_group_spawns_and_forgets_devices() {
    let test_context = TestContext::new();

    let Control::Continue(devices) = Group
        .receive(
            test_context.context(),
            Incoming::Message(GroupMessage::Record {
                device_name: "left",
                degrees: 21.5,
            }),
            HashMap::new(),
        )
        .expect("the group cannot fail")
    else {
        panic!("the group stopped");
    };
    let device_id = devices["left"].actor_id();

    let Control::Continue(devices) = Group
        .receive(
            test_context.context(),
            Incoming::Terminated(device_id),
            devices,
        )
        .expect("the group cannot fail")
    else {
        panic!("the group stopped");
    };
    assert!(devices.is_empty());
}
// ANCHOR_END: children

struct Group;

impl Actor for Group {
    type Message = GroupMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = HashMap<&'static str, ActorRef<DeviceMessage>>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(HashMap::new())
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut devices: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(GroupMessage::Record {
                device_name,
                degrees,
            }) => {
                let device = devices.entry(device_name).or_insert_with(|| {
                    let device = context.spawn(Device);
                    context.watch(&device);
                    device
                });
                device.tell(DeviceMessage::Record(degrees));
                Ok(Control::Continue(devices))
            }

            Incoming::Terminated(device_id) => {
                devices.retain(|_, device| device.actor_id() != device_id);
                Ok(Control::Continue(devices))
            }
        }
    }
}

enum GroupMessage {
    Record {
        device_name: &'static str,
        degrees: f64,
    },
}

struct Device;

impl Actor for Device {
    type Message = DeviceMessage;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = Option<f64>;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(None)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        last_reading: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(message) = incoming else {
            unreachable!("the device watches no actor, hence never gets a terminated signal")
        };

        match message {
            DeviceMessage::Record(degrees) => Ok(Control::Continue(Some(degrees))),

            DeviceMessage::Read(reply_to) => {
                reply_to.reply(last_reading);
                Ok(Control::Continue(last_reading))
            }
        }
    }
}

enum DeviceMessage {
    Record(f64),
    Read(ReplyTo<Option<f64>>),
}
