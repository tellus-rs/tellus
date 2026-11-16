//! Captures the tracing events which name an actor, so a test can assert what tellus logged for
//! it. One global subscriber serves every test of a binary, each test receiving the events of the
//! actors it asked for.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Mutex, Once},
    time::Duration,
};
use tellus::ActorId;
use tokio::{sync::mpsc, time::timeout};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Layer, Registry,
    layer::{Context, SubscriberExt},
};

const TIMEOUT: Duration = Duration::from_secs(5);

static SUBSCRIBER: Once = Once::new();
static SINKS: Mutex<Vec<(String, mpsc::UnboundedSender<Fields>)>> = Mutex::new(Vec::new());

/// The fields of one event by name, its level under `level` and its text under `message`.
pub type Fields = HashMap<&'static str, String>;

/// The events logged with the `actor_id` field naming one actor, in the order they were logged.
pub struct Logs(mpsc::UnboundedReceiver<Fields>);

impl Logs {
    /// Receive the events logged for the given actor from now on.
    pub fn of(actor_id: ActorId) -> Self {
        SUBSCRIBER.call_once(|| {
            tracing::subscriber::set_global_default(Registry::default().with(Capture))
                .expect("no other global subscriber is set");
        });

        let (events_tx, events_rx) = mpsc::unbounded_channel();
        SINKS
            .lock()
            .expect("the sinks are not poisoned")
            .push((actor_id.to_string(), events_tx));
        Self(events_rx)
    }

    /// Every event up to and including the first one with the given message.
    pub async fn until(&mut self, message: &str) -> Vec<Fields> {
        let mut events = Vec::new();
        loop {
            let event = timeout(TIMEOUT, self.0.recv())
                .await
                .unwrap_or_else(|_| panic!("{message:?} was not logged in time: {events:?}"))
                .expect("the global subscriber keeps the sink");
            let found = event.get("message").is_some_and(|logged| logged == message);
            events.push(event);
            if found {
                return events;
            }
        }
    }
}

struct Capture;

impl<S> Layer<S> for Capture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::new();
        event.record(&mut Recorder(&mut fields));
        let Some(actor_id) = fields.get("actor_id").cloned() else {
            return;
        };
        fields.insert("level", event.metadata().level().to_string());

        let sinks = SINKS.lock().expect("the sinks are not poisoned");
        for (_, sink) in sinks.iter().filter(|(id, _)| *id == actor_id) {
            let _ = sink.send(fields.clone());
        }
    }
}

struct Recorder<'a>(&'a mut Fields);

impl Visit for Recorder<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}
