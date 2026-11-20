use crate::{config::Config, sse::SseParser};
use crossterm::event::KeyEvent;
use futures_util::{Stream, StreamExt};
use reqwest::{Client, header::ACCEPT};
use serde::de::DeserializeOwned;
use std::{fmt::Display, pin::pin, sync::Arc, time::Duration};
use tellus_cluster_demo::{ClusterView, NodeEvent, ProbeReport, VerifierEvent, VerifierStatus};
use tellus_cluster_inspect::{
    InspectEvent,
    events::{Cursor, Stamped},
};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
// Must exceed the servers' keep-alive interval of 15 seconds.
const STREAM_SILENCE: Duration = Duration::from_secs(45);
const CLUSTER_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
const STATUS_INTERVAL: Duration = Duration::from_secs(2);
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const LAST_EVENT_ID: &str = "last-event-id";

#[derive(Debug)]
pub enum Input {
    Key(KeyEvent),
    Tick,
    Node(usize, NodeInput),
    Verifier(VerifierInput),
}

#[derive(Debug)]
pub enum NodeInput {
    Stream(StreamInput<NodeEvent>),
    Inspect(StreamInput<InspectEvent>),
    Cluster(ClusterView),
    Probe(ProbeReport),
}

#[derive(Debug)]
pub enum VerifierInput {
    Stream(StreamInput<VerifierEvent>),
    Status(VerifierStatus),
}

#[derive(Debug)]
pub enum StreamInput<E> {
    Connected,
    Disconnected(String),
    Event(Stamped<E>, Option<Cursor>),
    Unreadable(String, Option<Cursor>),
}

pub fn spawn(config: &Config, tx: mpsc::Sender<Input>) {
    let client = Arc::new(client());
    for (index, url) in config.nodes.iter().enumerate() {
        tokio::spawn(stream(
            client.clone(),
            format!("{url}/events"),
            tx.clone(),
            move |input| Input::Node(index, NodeInput::Stream(input)),
        ));
        tokio::spawn(stream(
            client.clone(),
            format!("{url}/inspect/events"),
            tx.clone(),
            move |input| Input::Node(index, NodeInput::Inspect(input)),
        ));
        tokio::spawn(poll(
            client.clone(),
            format!("{url}/cluster"),
            CLUSTER_INTERVAL,
            HTTP_TIMEOUT,
            tx.clone(),
            move |view| Input::Node(index, NodeInput::Cluster(view)),
        ));
        tokio::spawn(poll(
            client.clone(),
            format!("{url}/probe"),
            PROBE_INTERVAL,
            PROBE_TIMEOUT,
            tx.clone(),
            move |report| Input::Node(index, NodeInput::Probe(report)),
        ));
    }
    tokio::spawn(stream(
        client.clone(),
        format!("{}/events", config.verifier),
        tx.clone(),
        |input| Input::Verifier(VerifierInput::Stream(input)),
    ));
    tokio::spawn(poll(
        client,
        format!("{}/status", config.verifier),
        STATUS_INTERVAL,
        HTTP_TIMEOUT,
        tx,
        |status| Input::Verifier(VerifierInput::Status(status)),
    ));
}

struct Position {
    last_id: Option<String>,
    backoff: Duration,
}

fn client() -> Client {
    Client::builder()
        .connect_timeout(HTTP_TIMEOUT)
        .build()
        .expect("HTTP client")
}

async fn stream<E, W>(client: Arc<Client>, url: String, tx: mpsc::Sender<Input>, wrap: W)
where
    E: DeserializeOwned,
    W: Fn(StreamInput<E>) -> Input,
{
    let mut position = Position {
        last_id: None,
        backoff: MIN_BACKOFF,
    };

    loop {
        let mut request = client.get(&url).header(ACCEPT, "text/event-stream");
        if let Some(id) = &position.last_id {
            request = request.header(LAST_EVENT_ID, id);
        }

        let reason = match timeout(HTTP_TIMEOUT, request.send()).await {
            Ok(Ok(response)) if response.status().is_success() => {
                if tx.send(wrap(StreamInput::Connected)).await.is_err() {
                    return;
                }
                let followed = follow(
                    response.bytes_stream(),
                    STREAM_SILENCE,
                    &mut position,
                    &tx,
                    &wrap,
                );
                match followed.await {
                    Some(reason) => reason,
                    None => return,
                }
            }

            Ok(Ok(response)) => format!("HTTP {}", response.status()),

            Ok(Err(error)) => error.to_string(),

            Err(_) => format!("no response within {HTTP_TIMEOUT:?}"),
        };

        if tx
            .send(wrap(StreamInput::Disconnected(reason)))
            .await
            .is_err()
        {
            return;
        }
        sleep(position.backoff).await;
        position.backoff = (position.backoff * 2).min(MAX_BACKOFF);
    }
}

async fn follow<S, B, X, E, W>(
    bytes: S,
    silence: Duration,
    position: &mut Position,
    tx: &mpsc::Sender<Input>,
    wrap: &W,
) -> Option<String>
where
    S: Stream<Item = Result<B, X>>,
    B: AsRef<[u8]>,
    X: Display,
    E: DeserializeOwned,
    W: Fn(StreamInput<E>) -> Input,
{
    let mut bytes = pin!(bytes);
    let mut parser = SseParser::default();

    loop {
        let chunk = match timeout(silence, bytes.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(error))) => return Some(error.to_string()),
            Ok(None) => return Some("stream ended".to_string()),
            Err(_) => return Some(format!("silent for {silence:?}")),
        };
        for message in parser.push(chunk.as_ref()) {
            let cursor = message.id.as_deref().map(str::parse::<Cursor>).transpose();
            let input = match (serde_json::from_str(&message.data), cursor) {
                (Ok(stamped), Ok(cursor)) => {
                    position.backoff = MIN_BACKOFF;
                    StreamInput::Event(stamped, cursor)
                }

                (Err(error), Ok(cursor)) => StreamInput::Unreadable(error.to_string(), cursor),

                (_, Err(error)) => StreamInput::Unreadable(
                    format!(
                        "id {:?}: {error}",
                        message.id.as_deref().unwrap_or_default()
                    ),
                    None,
                ),
            };
            if tx.send(wrap(input)).await.is_err() {
                return None;
            }
            // Acknowledged only once delivered, else a reconnect skips it.
            if let Some(id) = message.id {
                position.last_id = Some(id);
            }
        }
    }
}

async fn poll<T, W>(
    client: Arc<Client>,
    url: String,
    interval: Duration,
    request_timeout: Duration,
    tx: mpsc::Sender<Input>,
    wrap: W,
) where
    T: DeserializeOwned,
    W: Fn(T) -> Input,
{
    loop {
        // The UI owns the terminal: a failed poll shows as stale data, not a log line!
        if let Ok(response) = client.get(&url).timeout(request_timeout).send().await
            && let Ok(value) = response.json::<T>().await
            && tx.send(wrap(value)).await.is_err()
        {
            return;
        }
        sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use crate::sources::{Input, MIN_BACKOFF, Position, StreamInput, VerifierInput, follow};
    use futures_util::stream;
    use std::time::Duration;
    use tellus_cluster_demo::VerifierEvent;
    use tellus_cluster_inspect::events::Cursor;
    use tokio::sync::mpsc;

    const SILENCE: Duration = Duration::from_secs(45);

    fn wrap(input: StreamInput<VerifierEvent>) -> Input {
        Input::Verifier(VerifierInput::Stream(input))
    }

    fn position() -> Position {
        Position {
            last_id: None,
            backoff: Duration::from_secs(4),
        }
    }

    /// A peer which goes silent without closing ends the stream, so it is reconnected.
    #[tokio::test(start_paused = true)]
    async fn a_silent_stream_ends() {
        let (tx, _rx) = mpsc::channel(8);
        let mut position = position();

        let reason = follow(
            stream::pending::<Result<Vec<u8>, !>>(),
            SILENCE,
            &mut position,
            &tx,
            &wrap,
        )
        .await;

        assert_eq!(reason, Some("silent for 45s".to_string()));
    }

    /// An unreadable event is reported and skipped: the next one is delivered, the position moves
    /// past both, and only a delivered event resets the backoff.
    #[tokio::test]
    async fn an_unreadable_event_is_skipped_and_acknowledged() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut position = position();
        let unreadable = Ok::<_, !>(b"data: {\"at\":1}\nid: 7:1\n\n".to_vec());

        let reason = follow(
            stream::iter([unreadable]),
            SILENCE,
            &mut position,
            &tx,
            &wrap,
        )
        .await;
        assert_eq!(reason, Some("stream ended".to_string()));
        assert!(matches!(
            rx.try_recv(),
            Ok(Input::Verifier(VerifierInput::Stream(
                StreamInput::Unreadable(_, Some(Cursor { seq: 1, .. }))
            )))
        ));
        assert_eq!(position.last_id.as_deref(), Some("7:1"));
        assert_eq!(position.backoff, Duration::from_secs(4));

        let readable = Ok::<_, !>(
            b"data: {\"at\":2,\"event\":{\"kind\":\"chaos\",\"value\":\"quiet\"}}\nid: 7:2\n\n"
                .to_vec(),
        );
        follow(stream::iter([readable]), SILENCE, &mut position, &tx, &wrap).await;
        assert!(matches!(
            rx.try_recv(),
            Ok(Input::Verifier(VerifierInput::Stream(StreamInput::Event(
                _,
                Some(Cursor { seq: 2, .. })
            ))))
        ));
        assert_eq!(position.last_id.as_deref(), Some("7:2"));
        assert_eq!(position.backoff, MIN_BACKOFF);
    }
}
