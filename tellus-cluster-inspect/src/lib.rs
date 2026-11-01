//! An axum [Router] which lets an operator or a tool inspect the tellus cluster node it runs in:
//! `GET /cluster` answers a [Snapshot] of the node's view, `GET /events` streams every change of
//! it as server-sent events, resumable with `Last-Event-ID` from a bounded log. Mount it into the
//! application's own server, behind whatever protects that server, since it reveals the
//! cluster's topology:
//!
//! ```ignore
//! let app = Router::new().nest("/inspect", tellus_cluster_inspect::router(InspectConfig::default())?);
//! ```
//!
//! The stream is fed by [tellus::cluster::changes]: one whole [ClusterState] per version and the
//! receptionist's [Settlement] whenever it or the version it judges changes, with an explicit gap
//! where the node's own subscriber fell behind. Within the live part of a stream they keep their
//! publication order. Every message is a [Stamped](events::Stamped) event, and a client that cannot
//! resume gets the retained history and then the current state and verdict, so it is never left
//! without a state. That history can start at any retained item, and the pair after it repeats the
//! latest state and verdict. A client therefore derives no transitions from a state of the version
//! it already holds, but still takes that state as its baseline.

#![doc(
    html_logo_url = "https://tellus.rs/brand/logo-tile.svg",
    html_favicon_url = "https://tellus.rs/favicon.svg"
)]
#![warn(missing_docs, clippy::missing_errors_doc)]

pub mod events;

mod recorder;

use crate::recorder::Recorder;
use axum::{Json, Router, extract::State, http::HeaderMap, response::IntoResponse, routing::get};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::{num::NonZeroUsize, sync::Arc};
use tellus::cluster::{self, ClusterState, Lifecycle, receptionist::Settlement};
use thiserror::Error;

/// How the router behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InspectConfig {
    /// How many events the log retains for replay and resumption. Defaults to
    /// [InspectConfig::DEFAULT_EVENTS_KEPT].
    pub events_kept: NonZeroUsize,
}

impl InspectConfig {
    /// The `events_kept` of [InspectConfig::default].
    pub const DEFAULT_EVENTS_KEPT: NonZeroUsize =
        NonZeroUsize::new(1_024).expect("1024 is not zero");
}

impl Default for InspectConfig {
    fn default() -> Self {
        Self {
            events_kept: Self::DEFAULT_EVENTS_KEPT,
        }
    }
}

/// The router cannot be built.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InspectError {
    /// The remoting endpoint has not been started, see [tellus::cluster::start_endpoint].
    #[error("remoting endpoint not started")]
    EndpointNotStarted,
}

/// One message of `GET /events`. Only [InspectEvent::Hello] is never logged; every other kind
/// is a [tellus::cluster::Change] as recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InspectEvent {
    /// Opens every stream. A changed `started_at` on a reconnect means the node restarted;
    /// `resumed` is `false` when the stream does not continue right after the client's
    /// `Last-Event-ID`, in which case the retained history and the current state and verdict
    /// follow, and only the last of those carries an id.
    Hello {
        /// Unix milliseconds at which the log started, the first half of every id.
        started_at: u64,

        /// Whether the stream continues right after the client's `Last-Event-ID`.
        resumed: bool,
    },

    /// The node's view at a new version.
    State(ClusterState),

    /// The receptionist's verdict, sent whenever the verdict or the version it judges changes, or
    /// repeated after a gap.
    Settled(Settlement),

    /// The node's own subscriber fell behind: this many changes were overwritten. The current
    /// state and verdict follow.
    Gap {
        /// How many changes were overwritten.
        dropped: u64,
    },
}

/// What `GET /cluster` answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The node's current view of the cluster.
    pub state: ClusterState,

    /// Where the endpoint stands.
    pub lifecycle: Lifecycle,

    /// The receptionist's verdict, with the version it judged: at most `state`'s version, never
    /// newer.
    pub settlement: Settlement,
}

/// The router serving `/cluster` and `/events` for the endpoint of this process. It spawns the
/// task recording the endpoint's changes, so call it inside a Tokio runtime.
///
/// # Errors
/// Fails if the endpoint is not started.
pub fn router(config: InspectConfig) -> Result<Router, InspectError> {
    let changes = cluster::changes().map_err(|_| InspectError::EndpointNotStarted)?;
    let recorder = Arc::new(Recorder::new(config.events_kept));
    let changes = stream::unfold(changes, |mut changes| async move {
        let change = changes.next().await;
        Some((change, changes))
    });
    tokio::spawn(recorder.clone().record(changes));

    Ok(router_with(Arc::new(Endpoint { recorder })))
}

trait Sources: Send + Sync {
    fn snapshot(&self) -> Snapshot;
    fn recorder(&self) -> &Recorder;
}

struct Endpoint {
    recorder: Arc<Recorder>,
}

impl Sources for Endpoint {
    /// The verdict first, then the state: an update in between leaves the verdict older, never
    /// newer, than the state it is shown with.
    fn snapshot(&self) -> Snapshot {
        let settlement = cluster::receptionist::settlement().expect("remoting endpoint started");
        let state = cluster::cluster_state()
            .expect("remoting endpoint started")
            .borrow()
            .clone();
        Snapshot {
            state,
            lifecycle: cluster::lifecycle().expect("remoting endpoint started"),
            settlement,
        }
    }

    fn recorder(&self) -> &Recorder {
        &self.recorder
    }
}

fn router_with(sources: Arc<dyn Sources>) -> Router {
    Router::new()
        .route("/cluster", get(snapshot))
        .route("/events", get(events))
        .with_state(sources)
}

async fn snapshot(State(sources): State<Arc<dyn Sources>>) -> Json<Snapshot> {
    Json(sources.snapshot())
}

async fn events(State(sources): State<Arc<dyn Sources>>, headers: HeaderMap) -> impl IntoResponse {
    let recorder = sources.recorder();
    let capture = recorder.capture(events::last_event_id(&headers));
    let hello = InspectEvent::Hello {
        started_at: recorder.log().started_at(),
        resumed: capture.resumed,
    };

    events::sse(recorder.log(), capture.items, capture.anchor, hello)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Stamped;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::time::Duration;
    use tokio::time::timeout;
    use tower::ServiceExt;

    const INCARNATION: &str = "019a0000-0000-7000-8000-000000000001";
    const CLUSTER: &str = "019a0000-0000-7000-8000-0000000000c1";

    pub(crate) fn state(version: u64) -> ClusterState {
        serde_json::from_value(serde_json::json!({
            "version": version,
            "this": { "addr": "10.0.0.1:7878", "incarnation": INCARNATION },
            "cluster": CLUSTER,
            "members": [
                { "addr": "10.0.0.1:7878", "incarnation": INCARNATION, "state": "Up" }
            ],
            "unreachable": [],
            "broken_links": [],
            "foreign_clusters": []
        }))
        .expect("a cluster state deserializes")
    }

    pub(crate) fn settlement(version: u64, settled: bool) -> Settlement {
        serde_json::from_value(serde_json::json!({ "version": version, "settled": settled }))
            .expect("a settlement deserializes")
    }

    struct Fake {
        recorder: Arc<Recorder>,
    }

    impl Sources for Fake {
        fn snapshot(&self) -> Snapshot {
            Snapshot {
                state: state(3),
                lifecycle: Lifecycle::Formed,
                settlement: settlement(3, true),
            }
        }

        fn recorder(&self) -> &Recorder {
            &self.recorder
        }
    }

    fn app(events_kept: usize) -> (Router, Arc<Recorder>) {
        let recorder = Arc::new(Recorder::new(
            NonZeroUsize::new(events_kept).expect("non-zero"),
        ));
        let router = router_with(Arc::new(Fake {
            recorder: recorder.clone(),
        }));
        (router, recorder)
    }

    async fn prefix(body: Body, done: impl Fn(&str) -> bool) -> String {
        let mut body = body;
        let mut text = String::new();
        timeout(Duration::from_secs(5), async {
            while let Some(frame) = body.frame().await {
                let frame = frame.expect("a frame");
                if let Ok(data) = frame.into_data() {
                    text.push_str(&String::from_utf8_lossy(&data));
                    if done(&text) {
                        return;
                    }
                }
            }
        })
        .await
        .expect("the prefix arrives in time");
        text
    }

    fn messages(text: &str) -> Vec<(Option<String>, String)> {
        text.split("\n\n")
            .filter(|block| block.contains("data:"))
            .map(|block| {
                let id = block
                    .lines()
                    .find_map(|line| line.strip_prefix("id: "))
                    .map(str::to_string);
                let data = block
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .expect("a data line");
                let stamped = serde_json::from_str::<Stamped<InspectEvent>>(data)
                    .expect("a stamped inspect event");
                let kind = match stamped.event {
                    InspectEvent::Hello { resumed, .. } => format!("hello {resumed}"),
                    InspectEvent::State(state) => format!("state {}", state.version()),
                    InspectEvent::Settled(settlement) => {
                        format!("settled {} {}", settlement.version(), settlement.settled())
                    }
                    InspectEvent::Gap { dropped } => format!("gap {dropped}"),
                };
                (id, kind)
            })
            .collect()
    }

    async fn get(
        router: &Router,
        uri: &str,
        last_event_id: Option<&str>,
    ) -> axum::response::Response {
        let mut request = Request::builder().uri(uri);
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        router
            .clone()
            .oneshot(request.body(Body::empty()).expect("a request"))
            .await
            .expect("a response")
    }

    #[tokio::test]
    async fn the_snapshot_is_json() {
        let (router, _) = app(8);
        let response = get(&router, "/cluster", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        let value = serde_json::from_slice::<Value>(&body).expect("json");
        assert_eq!(value["state"]["version"], 3);
        assert_eq!(value["state"]["this"]["addr"], "10.0.0.1:7878");
        assert_eq!(value["lifecycle"], "formed");
        assert_eq!(
            value["settlement"],
            serde_json::json!({ "version": 3, "settled": true })
        );
        let snapshot = serde_json::from_slice::<Snapshot>(&body).expect("a snapshot");
        assert_eq!(snapshot.state.version(), 3);
    }

    #[tokio::test]
    async fn a_fresh_client_gets_the_history_and_the_pair() {
        let (router, recorder) = app(8);
        recorder.push(InspectEvent::State(state(1)));
        recorder.push(InspectEvent::Settled(settlement(1, false)));
        recorder.push(InspectEvent::State(state(2)));
        let started_at = recorder.log().started_at();

        let response = get(&router, "/events", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let text = prefix(response.into_body(), |text| {
            text.matches("data:").count() >= 6
        })
        .await;
        let anchor = format!("{started_at}:3");
        assert_eq!(
            messages(&text),
            vec![
                (None, "hello false".to_string()),
                (None, "state 1".to_string()),
                (None, "settled 1 false".to_string()),
                (None, "state 2".to_string()),
                (None, "state 2".to_string()),
                (Some(anchor), "settled 1 false".to_string()),
            ]
        );
        let hello = text
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("the hello");
        let hello = serde_json::from_str::<Stamped<InspectEvent>>(hello).expect("stamped");
        assert!(
            matches!(hello.event, InspectEvent::Hello { started_at: at, .. } if at == started_at)
        );
    }

    #[tokio::test]
    async fn a_resumable_client_continues_without_the_pair() {
        let (router, recorder) = app(8);
        recorder.push(InspectEvent::State(state(1)));
        recorder.push(InspectEvent::Settled(settlement(1, true)));
        recorder.push(InspectEvent::State(state(2)));
        let started_at = recorder.log().started_at();

        let response = get(&router, "/events", Some(&format!("{started_at}:1"))).await;
        let text = prefix(response.into_body(), |text| {
            text.matches("data:").count() >= 3
        })
        .await;
        assert_eq!(
            messages(&text),
            vec![
                (None, "hello true".to_string()),
                (
                    Some(format!("{started_at}:2")),
                    "settled 1 true".to_string()
                ),
                (Some(format!("{started_at}:3")), "state 2".to_string()),
            ]
        );

        let response = get(&router, "/events", Some("1:5")).await;
        let text = prefix(response.into_body(), |text| {
            text.matches("data:").count() >= 1
        })
        .await;
        assert_eq!(
            messages(&text).first().map(|(_, kind)| kind.as_str()),
            Some("hello false")
        );
    }

    #[tokio::test]
    async fn the_router_needs_a_started_endpoint() {
        assert_eq!(
            router(InspectConfig::default()).err(),
            Some(InspectError::EndpointNotStarted)
        );
    }
}
