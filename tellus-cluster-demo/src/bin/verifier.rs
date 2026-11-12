//! The verifier of the cluster demo: it polls every node and the load balancer. Once the chaos
//! agent has been quiet long enough for the cluster to have recovered, it asserts what the cluster
//! promises. No two members have disjoint Up sets (a split brain), every node is a member, and
//! every node sees every other as Up with none unreachable and no broken link. Every node's
//! receptionist resolves every worker and is settled. Every node resolves every subscriber of the
//! event topic and has heard from every node lately, and every node can still message every other.
//! A load balancer outage longer than the configured one is a violation at any time. Every node
//! reports the version of the application it runs: a verification seeing more than one is mixed,
//! and a uniform version replacing another is a violation unless every mixed cluster in between,
//! one to all but one node on the new version, was verified.
//!
//! Violations are logged, counted and served at `/violations`, which keeps the latest 200; the
//! process keeps running, since this is a forever test rather than a run to completion.

use anyhow::Context;
use axum::{Json, Router, extract::State, http::HeaderMap, response::IntoResponse, routing::get};
use reqwest::Client;
use std::{
    collections::BTreeSet,
    env, future,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tellus::cluster::AppVersion;
use tellus_cluster_demo::{
    ClusterView, Phase, ProbeOutcome, ProbeReport, VerifierEvent, VerifierStatus, Violation,
};
use tellus_cluster_inspect::events::{self, EventLog, now_millis};
use tokio::{
    fs,
    net::TcpListener,
    sync::Mutex,
    time::{Instant, sleep},
};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_PORT: u16 = 8080;
const CHECK_INTERVAL: Duration = Duration::from_secs(2);
const LB_INTERVAL: Duration = Duration::from_millis(500);
const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const VIOLATIONS_KEPT: usize = 200;
const EVENTS_KEPT: NonZeroUsize = NonZeroUsize::new(1024).unwrap();
const QUIET: &str = "quiet";
const UNKNOWN: &str = "unknown";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Arc::new(Config::from_env()?);
    let state = Arc::new(VerifierState::new(config.lb_outage));
    info!(nodes = ?config.nodes, lb = %config.lb, "verifying");

    tokio::spawn(serve_http(state.clone()));
    tokio::spawn(watch_lb(config.clone(), state.clone()));

    verify(config, state).await
}

/// The verifier's configuration, all of it from the environment.
struct Config {
    nodes: Vec<String>,
    lb: String,
    chaos_state: PathBuf,
    settle: Duration,
    lb_outage: Duration,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let nodes = var("TELLUS_NODES")?
            .split(',')
            .map(|node| node.trim().to_string())
            .filter(|node| !node.is_empty())
            .collect::<Vec<_>>();
        let lb = var("TELLUS_LB")?;
        let chaos_state = var("TELLUS_CHAOS_STATE")?.into();
        let settle = seconds("TELLUS_SETTLE_SECS")?;
        let lb_outage = seconds("TELLUS_LB_OUTAGE_SECS")?;

        Ok(Self {
            nodes,
            lb,
            chaos_state,
            settle,
            lb_outage,
        })
    }
}

/// Mutate and emit under the report lock, so the stream and the counters agree on the order.
struct VerifierState {
    report: Mutex<Report>,
    events: Arc<EventLog<VerifierEvent>>,
    started_at: u64,
    lb_outage: Duration,
}

impl VerifierState {
    fn new(lb_outage: Duration) -> Self {
        let started_at = now_millis();
        Self {
            report: Mutex::new(Report::new()),
            events: Arc::new(EventLog::new(started_at, EVENTS_KEPT)),
            started_at,
            lb_outage,
        }
    }

    /// How long the chaos agent has been quiet; `None` while it is not.
    async fn observe_chaos(&self, value: String) -> Option<Duration> {
        let now = Instant::now();
        let mut report = self.report.lock().await;
        if report.chaos != value {
            report.chaos = value.clone();
            self.events.push(VerifierEvent::Chaos { value });
        }
        if report.chaos == QUIET {
            let since = *report.quiet_since.get_or_insert(now);
            Some(now - since)
        } else {
            report.quiet_since = None;
            None
        }
    }

    /// `versions` is one application version per node, `None` when a node did not answer, which
    /// leaves the roll untouched: that window's cluster then counts as unverified.
    async fn verified(&self, mut violations: Vec<String>, versions: Option<Vec<String>>) {
        let mut report = self.report.lock().await;
        report.verifications += 1;
        let mixed = match versions {
            Some(versions) => {
                let (roll, mixed, violation) = roll_transition(report.roll.clone(), &versions);
                report.roll = roll;
                violations.extend(violation);
                if mixed {
                    report.mixed_verifications += 1;
                }
                mixed
            }

            None => false,
        };
        let quiet_for = report
            .quiet_since
            .map(|since| since.elapsed())
            .unwrap_or_default();
        if violations.is_empty() {
            info!(?quiet_for, mixed, "cluster verified");
        }
        let count = violations.len();
        for violation in violations {
            self.record_violation(&mut report, violation);
        }
        self.events.push(VerifierEvent::Verified {
            violations: count,
            mixed,
            quiet_for_millis: millis(quiet_for),
        });
    }

    async fn lb_observed(&self, available: bool) {
        let now = Instant::now();
        let mut report = self.report.lock().await;
        report.lb_requests += 1;
        if !available {
            report.lb_failures += 1;
        }
        // Measured before the transition, so the poll ending an outage counts its full length.
        if let LbState::Unavailable { since, .. } = report.lb {
            report.longest_lb_outage_millis = report
                .longest_lb_outage_millis
                .max((now - since).as_millis());
        }
        let (lb, event, violation) = lb_transition(report.lb, available, now, self.lb_outage);
        report.lb = lb;
        if let Some(event) = event {
            self.events.push(event);
        }
        if let Some(detail) = violation {
            self.record_violation(&mut report, detail);
        }
    }

    fn record_violation(&self, report: &mut Report, detail: String) {
        error!(detail, "VIOLATION");
        let at = now();
        report.violations += 1;
        report.recent_violations.push(Violation {
            at,
            detail: detail.clone(),
        });
        if report.recent_violations.len() > VIOLATIONS_KEPT {
            report.recent_violations.remove(0);
        }
        self.events.push(VerifierEvent::Violation { at, detail });
    }
}

struct Report {
    chaos: String,
    quiet_since: Option<Instant>,
    verifications: usize,
    mixed_verifications: usize,
    violations: usize,
    lb_requests: usize,
    lb_failures: usize,
    longest_lb_outage_millis: u128,
    lb: LbState,
    roll: Roll,
    recent_violations: Vec<Violation>,
}

impl Report {
    fn new() -> Self {
        Self {
            chaos: UNKNOWN.to_string(),
            quiet_since: None,
            verifications: 0,
            mixed_verifications: 0,
            violations: 0,
            lb_requests: 0,
            lb_failures: 0,
            longest_lb_outage_millis: 0,
            lb: LbState::Unknown,
            roll: Roll::new(),
            recent_violations: Vec::new(),
        }
    }

    fn status(&self) -> VerifierStatus {
        VerifierStatus {
            chaos: self.chaos.clone(),
            verifications: self.verifications,
            mixed_verifications: self.mixed_verifications,
            violations: self.violations,
            lb_requests: self.lb_requests,
            lb_failures: self.lb_failures,
            longest_lb_outage_millis: self.longest_lb_outage_millis,
            quiet_for_millis: self.quiet_since.map(|since| millis(since.elapsed())),
            lb_available: match self.lb {
                LbState::Unknown => None,
                LbState::Available => Some(true),
                LbState::Unavailable { .. } => Some(false),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LbState {
    Unknown,
    Available,
    Unavailable {
        since: Instant,
        last_violation_at: Instant,
    },
}

/// What the verifications since the last uniform application version have shown: that version,
/// and how many nodes were on another one in each mixed verification since.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Roll {
    last_uniform: Option<String>,
    rolled_counts: BTreeSet<usize>,
}

impl Roll {
    fn new() -> Self {
        Self {
            last_uniform: None,
            rolled_counts: BTreeSet::new(),
        }
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn var(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} is not set"))
}

fn seconds(name: &str) -> anyhow::Result<Duration> {
    let seconds = var(name)?
        .parse()
        .with_context(|| format!("{name} is not a number of seconds"))?;

    Ok(Duration::from_secs(seconds))
}

async fn serve_http(state: Arc<VerifierState>) {
    let router = Router::new()
        .route("/status", get(status))
        .route("/violations", get(violations))
        .route("/events", get(events))
        .with_state(state);

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, HTTP_PORT));
    match TcpListener::bind(addr).await {
        Ok(listener) => {
            if let Err(error) = axum::serve(listener, router).await {
                error!(%error, "HTTP API stopped");
            }
        }

        Err(error) => error!(%error, %addr, "cannot bind the HTTP API"),
    }
}

async fn status(State(state): State<Arc<VerifierState>>) -> Json<VerifierStatus> {
    Json(state.report.lock().await.status())
}

async fn violations(State(state): State<Arc<VerifierState>>) -> Json<Vec<Violation>> {
    Json(state.report.lock().await.recent_violations.clone())
}

async fn events(State(state): State<Arc<VerifierState>>, headers: HeaderMap) -> impl IntoResponse {
    let replay = state.events.replay(events::last_event_id(&headers));
    let hello = VerifierEvent::Hello {
        started_at: state.started_at,
        resumed: replay.resumed,
    };
    let anchor = replay.anchor;

    events::sse(
        &state.events,
        replay.into_items(state.started_at),
        anchor,
        hello,
        future::pending(),
    )
}

/// The claim the load balancer stands for: whatever the chaos agent does to the nodes behind it,
/// a request is answered. Every failed request is counted, and failures lasting beyond
/// [Config::lb_outage] are a violation.
async fn watch_lb(config: Arc<Config>, state: Arc<VerifierState>) {
    let client = client();
    let url = format!("{}/cluster", config.lb);

    loop {
        let available = client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        state.lb_observed(available).await;

        sleep(LB_INTERVAL).await;
    }
}

/// The next state, the event if availability changed, and the violation once another `lb_outage`
/// has passed.
fn lb_transition(
    lb: LbState,
    available: bool,
    now: Instant,
    lb_outage: Duration,
) -> (LbState, Option<VerifierEvent>, Option<String>) {
    match (lb, available) {
        (LbState::Available, true) => (lb, None, None),

        (LbState::Unknown, true) => (
            LbState::Available,
            Some(VerifierEvent::Lb {
                available: true,
                outage_millis: None,
            }),
            None,
        ),

        (LbState::Unavailable { since, .. }, true) => (
            LbState::Available,
            Some(VerifierEvent::Lb {
                available: true,
                outage_millis: Some(millis(now - since)),
            }),
            None,
        ),

        (LbState::Unknown | LbState::Available, false) => (
            LbState::Unavailable {
                since: now,
                last_violation_at: now,
            },
            Some(VerifierEvent::Lb {
                available: false,
                outage_millis: None,
            }),
            None,
        ),

        (
            LbState::Unavailable {
                since,
                last_violation_at,
            },
            false,
        ) => {
            if now - last_violation_at > lb_outage {
                let outage = now - since;
                (
                    LbState::Unavailable {
                        since,
                        last_violation_at: now,
                    },
                    None,
                    Some(format!("load balancer unavailable for {outage:?}")),
                )
            } else {
                (lb, None, None)
            }
        }
    }
}

/// The next state, whether this verification was mixed, and the violation when one version has
/// replaced the last uniform one without every mixed cluster in between, one to all but one node
/// on the new version, having been verified. `versions` is one application version per node.
fn roll_transition(roll: Roll, versions: &[String]) -> (Roll, bool, Option<String>) {
    let distinct = versions.iter().collect::<BTreeSet<_>>();
    if distinct.len() > 1 {
        let mut roll = roll;
        if let Some(previous) = &roll.last_uniform {
            let rolled = versions
                .iter()
                .filter(|version| *version != previous)
                .count();
            roll.rolled_counts.insert(rolled);
        }
        return (roll, true, None);
    }
    let Some(version) = distinct.into_iter().next() else {
        return (roll, false, None);
    };
    let violation = match &roll.last_uniform {
        Some(previous) if previous != version => {
            let mixed_clusters = 1..versions.len();
            let verified = mixed_clusters
                .clone()
                .filter(|count| roll.rolled_counts.contains(count))
                .count();
            (verified < mixed_clusters.len()).then(|| {
                format!(
                    "{} from {previous} to {version} with {verified} of {} mixed clusters verified",
                    direction(previous, version),
                    mixed_clusters.len()
                )
            })
        }

        _ => None,
    };
    let roll = Roll {
        last_uniform: Some(version.clone()),
        rolled_counts: BTreeSet::new(),
    };
    (roll, false, violation)
}

/// `upgraded` or `downgraded` by semver precedence, `changed` if either does not parse.
fn direction(previous: &str, current: &str) -> &'static str {
    match (
        previous.parse::<AppVersion>(),
        current.parse::<AppVersion>(),
    ) {
        (Ok(previous), Ok(current)) if current > previous => "upgraded",
        (Ok(_), Ok(_)) => "downgraded",
        _ => "changed",
    }
}

/// Verifies once per quiet window: the chaos agent's state file names the fault it is running,
/// so the cluster is only held to its promises once it has had [Config::settle] without one.
async fn verify(config: Arc<Config>, state: Arc<VerifierState>) -> anyhow::Result<()> {
    let client = client();
    let mut verified = false;

    loop {
        sleep(CHECK_INTERVAL).await;

        let chaos = chaos_state(&config.chaos_state).await;
        let Some(quiet_for) = state.observe_chaos(chaos).await else {
            verified = false;
            continue;
        };
        if verified || quiet_for < config.settle {
            continue;
        }

        let (violations, versions) = verify_cluster(&client, &config.nodes).await;
        state.verified(violations, versions).await;
        verified = true;
    }
}

/// An unreadable state file is not quiet: until the chaos agent says what it is doing, the
/// cluster is not held to anything.
async fn chaos_state(path: &Path) -> String {
    match fs::read_to_string(path).await {
        Ok(state) => state.trim().to_string(),

        Err(error) => {
            debug!(%error, "cannot read the chaos state");
            UNKNOWN.to_string()
        }
    }
}

/// The violations and, once every node answered, the application version of each: a
/// configuration fact every node reports in every phase, so a split brain still yields them.
async fn verify_cluster(client: &Client, nodes: &[String]) -> (Vec<String>, Option<Vec<String>>) {
    let mut violations = Vec::new();

    let mut views = Vec::with_capacity(nodes.len());
    for node in nodes {
        match cluster_view(client, node).await {
            Ok(view) => views.push(view),
            Err(error) => violations.push(format!("{node} did not answer: {error}")),
        }
    }
    if views.len() < nodes.len() {
        return (violations, None);
    }
    let versions = views
        .iter()
        .map(|view| view.app_version.clone())
        .collect::<Vec<_>>();

    if let Some(split) = split_brain(&views) {
        violations.push(split);
        return (violations, Some(versions));
    }

    let expected = views.iter().map(ClusterView::addr).collect::<BTreeSet<_>>();
    for view in &views {
        if view.phase != Phase::Member {
            violations.push(format!("{} is {:?}, not a member", view.name, view.phase));
        }

        let up = view.up_addrs();
        if up != expected {
            let missing = expected.difference(&up).collect::<Vec<_>>();
            violations.push(format!("{} does not see {missing:?} as Up", view.name));
        }

        if !view.state.unreachable().is_empty() {
            let unreachable = view
                .state
                .unreachable()
                .iter()
                .map(|member| member.addr())
                .collect::<Vec<_>>();
            violations.push(format!(
                "{} derives {unreachable:?} as unreachable",
                view.name
            ));
        }
        if !view.state.broken_links().is_empty() {
            let broken_links = view
                .state
                .broken_links()
                .iter()
                .map(|link| (link.observer().addr(), link.subject().addr()))
                .collect::<Vec<_>>();
            violations.push(format!(
                "{} derives {broken_links:?} as broken links",
                view.name
            ));
        }
        if view.workers != nodes.len() {
            violations.push(format!(
                "{} resolves {} workers, not {}",
                view.name,
                view.workers,
                nodes.len()
            ));
        }
        if !view.settlement.settled() || view.settlement.version() != view.state.version() {
            violations.push(format!(
                "{}'s receptionist is not settled at version {}: {:?}",
                view.name,
                view.state.version(),
                view.settlement
            ));
        }
        if view.subscribers != nodes.len() {
            violations.push(format!(
                "{} resolves {} subscribers, not {}",
                view.name,
                view.subscribers,
                nodes.len()
            ));
        }
        if view.publishers != expected {
            let missing = expected.difference(&view.publishers).collect::<Vec<_>>();
            violations.push(format!(
                "{} has not heard from {missing:?} on the topic",
                view.name
            ));
        }
    }

    for node in nodes {
        match probe_report(client, node).await {
            Ok(report) => {
                for probe in report.probes {
                    if let ProbeOutcome::Failed { error } = probe.outcome {
                        violations.push(format!(
                            "{} cannot message {}: {error}",
                            report.name, probe.addr
                        ));
                    }
                }
            }

            Err(error) => violations.push(format!("{node} did not answer a probe: {error}")),
        }
    }

    (violations, Some(versions))
}

/// Two members whose Up sets do not overlap are two clusters, which never merge on their own. It
/// is the one failure no single node's view shows, since each side looks healthy to itself. A
/// node still bootstrapping is no cluster, so its view of itself alone is not one side of one.
fn split_brain(views: &[ClusterView]) -> Option<String> {
    let members = views
        .iter()
        .filter(|view| view.phase == Phase::Member)
        .collect::<Vec<_>>();
    for (index, one) in members.iter().enumerate() {
        for other in &members[index + 1..] {
            if one.up_addrs().is_disjoint(&other.up_addrs()) {
                return Some(format!(
                    "split brain: {} sees {:?}, {} sees {:?}",
                    one.name,
                    one.up_addrs(),
                    other.name,
                    other.up_addrs()
                ));
            }
        }
    }

    None
}

async fn cluster_view(client: &Client, node: &str) -> anyhow::Result<ClusterView> {
    let view = client
        .get(format!("{node}/cluster"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(view)
}

async fn probe_report(client: &Client, node: &str) -> anyhow::Result<ProbeReport> {
    let report = client
        .get(format!("{node}/probe"))
        .timeout(PROBE_TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(report)
}

fn client() -> Client {
    Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("HTTP client")
}

/// Seconds since the epoch.
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs()
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("a duration fits in 64 bits of milliseconds")
}

#[cfg(test)]
mod tests {
    use crate::{LbState, Roll, VerifierState, lb_transition, roll_transition, split_brain};
    use serde_json::json;
    use std::{collections::BTreeSet, net::SocketAddr, time::Duration};
    use tellus_cluster_demo::{ClusterView, Phase, VerifierEvent};
    use tokio::time::Instant;

    const LB_OUTAGE: Duration = Duration::from_secs(5);
    const CLUSTER: &str = "019a0000-0000-7000-8000-0000000000c1";

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn incarnation(port: u16) -> String {
        format!("019a0000-0000-7000-8000-0000000{port:05}")
    }

    fn view(name: &str, port: u16, phase: Phase, up: &[u16]) -> ClusterView {
        let members = up
            .iter()
            .map(|port| {
                json!({ "addr": addr(*port), "incarnation": incarnation(*port), "state": "Up" })
            })
            .collect::<Vec<_>>();
        serde_json::from_value(json!({
            "name": name,
            "phase": phase,
            "app_version": "1.0.0",
            "state": {
                "version": 1,
                "this": { "addr": addr(port), "incarnation": incarnation(port) },
                "cluster": CLUSTER,
                "members": members,
                "unreachable": [],
                "broken_links": [],
                "foreign_clusters": [],
            },
            "settlement": { "version": 1, "settled": true },
            "workers": up.len(),
            "subscribers": up.len(),
            "publishers": up.iter().map(|port| addr(*port)).collect::<Vec<_>>(),
        }))
        .expect("a cluster view deserializes")
    }

    /// Two members which do not see each other are two clusters.
    #[test]
    fn two_disjoint_members_are_a_split_brain() {
        let views = [
            view("node1", 1, Phase::Member, &[1, 2]),
            view("node3", 3, Phase::Member, &[3, 4, 5]),
        ];

        assert!(split_brain(&views).is_some());
    }

    /// A node still bootstrapping lists only itself and is no cluster, so two of them are not two
    /// clusters, and neither is one beside a member.
    #[test]
    fn bootstrapping_nodes_are_not_a_split_brain() {
        let two_bootstrapping = [
            view("node1", 1, Phase::Bootstrapping, &[1]),
            view("node2", 2, Phase::Bootstrapping, &[2]),
        ];
        let one_beside_a_member = [
            view("node1", 1, Phase::Bootstrapping, &[1]),
            view("node3", 3, Phase::Member, &[3, 4, 5]),
        ];

        assert!(split_brain(&two_bootstrapping).is_none());
        assert!(split_brain(&one_beside_a_member).is_none());
    }

    #[test]
    fn the_first_lb_observation_is_an_event() {
        let now = Instant::now();

        let (lb, event, violation) = lb_transition(LbState::Unknown, true, now, LB_OUTAGE);
        assert_eq!(lb, LbState::Available);
        assert_eq!(
            event,
            Some(VerifierEvent::Lb {
                available: true,
                outage_millis: None
            })
        );
        assert_eq!(violation, None);

        let (lb, event, violation) = lb_transition(LbState::Unknown, false, now, LB_OUTAGE);
        assert_eq!(
            lb,
            LbState::Unavailable {
                since: now,
                last_violation_at: now
            }
        );
        assert_eq!(
            event,
            Some(VerifierEvent::Lb {
                available: false,
                outage_millis: None
            })
        );
        assert_eq!(violation, None);
    }

    #[test]
    fn an_unchanged_lb_availability_is_silent() {
        let now = Instant::now();
        assert_eq!(
            lb_transition(LbState::Available, true, now, LB_OUTAGE),
            (LbState::Available, None, None)
        );
        let outage = LbState::Unavailable {
            since: now,
            last_violation_at: now,
        };
        assert_eq!(
            lb_transition(outage, false, now + Duration::from_secs(1), LB_OUTAGE),
            (outage, None, None)
        );
    }

    #[test]
    fn an_outage_starts_with_an_event_and_repeats_its_violation() {
        let start = Instant::now();
        let (lb, event, violation) = lb_transition(LbState::Available, false, start, LB_OUTAGE);
        assert_eq!(
            lb,
            LbState::Unavailable {
                since: start,
                last_violation_at: start
            }
        );
        assert!(event.is_some());
        assert_eq!(violation, None);

        let first = start + LB_OUTAGE + Duration::from_secs(1);
        let (lb, event, violation) = lb_transition(lb, false, first, LB_OUTAGE);
        assert_eq!(
            lb,
            LbState::Unavailable {
                since: start,
                last_violation_at: first
            }
        );
        assert_eq!(event, None);
        assert_eq!(
            violation,
            Some("load balancer unavailable for 6s".to_string())
        );

        let second = first + LB_OUTAGE + Duration::from_secs(1);
        let (lb, event, violation) = lb_transition(lb, false, second, LB_OUTAGE);
        assert_eq!(
            lb,
            LbState::Unavailable {
                since: start,
                last_violation_at: second
            }
        );
        assert_eq!(event, None);
        assert_eq!(
            violation,
            Some("load balancer unavailable for 12s".to_string())
        );
    }

    #[test]
    fn a_recovery_reports_the_whole_outage() {
        let start = Instant::now();
        let outage = LbState::Unavailable {
            since: start,
            last_violation_at: start + LB_OUTAGE,
        };

        let (lb, event, violation) =
            lb_transition(outage, true, start + Duration::from_secs(7), LB_OUTAGE);
        assert_eq!(lb, LbState::Available);
        assert_eq!(
            event,
            Some(VerifierEvent::Lb {
                available: true,
                outage_millis: Some(7_000)
            })
        );
        assert_eq!(violation, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_recovered_outage_counts_its_full_length() {
        let state = VerifierState::new(LB_OUTAGE);
        state.lb_observed(true).await;
        state.lb_observed(false).await;
        tokio::time::advance(Duration::from_millis(700)).await;
        state.lb_observed(true).await;

        let status = state.report.lock().await.status();
        assert_eq!(status.longest_lb_outage_millis, 700);
        assert_eq!(status.lb_available, Some(true));
        assert_eq!(status.lb_requests, 3);
        assert_eq!(status.lb_failures, 1);
    }

    fn versions(old: usize, new: usize) -> Vec<String> {
        let old = (0..old).map(|_| "1.0.0".to_string());
        let new = (0..new).map(|_| "2.0.0".to_string());
        old.chain(new).collect()
    }

    fn uniform(version: &str) -> Roll {
        Roll {
            last_uniform: Some(version.to_string()),
            rolled_counts: BTreeSet::new(),
        }
    }

    #[test]
    fn a_first_uniform_verification_only_records() {
        let (roll, mixed, violation) = roll_transition(Roll::new(), &versions(5, 0));

        assert_eq!(roll, uniform("1.0.0"));
        assert!(!mixed);
        assert_eq!(violation, None);
    }

    /// Before a uniform version is known, old and new cannot be told apart, so nothing is
    /// recorded and the uniform version seen next does not count as a change.
    #[test]
    fn a_first_mixed_verification_records_no_count() {
        let (roll, mixed, violation) = roll_transition(Roll::new(), &versions(3, 2));
        assert_eq!(roll, Roll::new());
        assert!(mixed);
        assert_eq!(violation, None);

        let (roll, mixed, violation) = roll_transition(roll, &versions(0, 5));
        assert_eq!(roll, uniform("2.0.0"));
        assert!(!mixed);
        assert_eq!(violation, None);
    }

    #[test]
    fn a_mixed_verification_records_the_nodes_on_the_other_version() {
        let (roll, mixed, violation) = roll_transition(uniform("1.0.0"), &versions(3, 2));

        assert_eq!(roll.last_uniform.as_deref(), Some("1.0.0"));
        assert_eq!(roll.rolled_counts, BTreeSet::from([2]));
        assert!(mixed);
        assert_eq!(violation, None);
    }

    #[test]
    fn the_same_version_twice_is_silent_and_resets_the_counts() {
        let roll = Roll {
            last_uniform: Some("1.0.0".to_string()),
            rolled_counts: BTreeSet::from([1, 2]),
        };

        let (roll, mixed, violation) = roll_transition(roll, &versions(5, 0));

        assert_eq!(roll, uniform("1.0.0"));
        assert!(!mixed);
        assert_eq!(violation, None);
    }

    #[test]
    fn a_roll_back_to_the_last_uniform_version_is_silent() {
        let roll = Roll {
            last_uniform: Some("1.0.0".to_string()),
            rolled_counts: BTreeSet::from([1, 2]),
        };

        let (roll, mixed, violation) = roll_transition(roll, &versions(4, 1));
        assert!(mixed);
        assert_eq!(violation, None);
        assert_eq!(roll.rolled_counts, BTreeSet::from([1, 2]));

        let (roll, mixed, violation) = roll_transition(roll, &versions(5, 0));

        assert_eq!(roll, uniform("1.0.0"));
        assert!(!mixed);
        assert_eq!(violation, None);
    }

    #[test]
    fn a_change_after_every_mixed_cluster_is_fine() {
        let mut roll = uniform("1.0.0");
        for new in 1..5 {
            let (next, mixed, violation) = roll_transition(roll, &versions(5 - new, new));
            assert!(mixed);
            assert_eq!(violation, None);
            roll = next;
        }

        let (roll, mixed, violation) = roll_transition(roll, &versions(0, 5));

        assert_eq!(roll, uniform("2.0.0"));
        assert!(!mixed);
        assert_eq!(violation, None);
    }

    #[test]
    fn a_change_with_a_mixed_cluster_missing_is_a_violation() {
        let roll = Roll {
            last_uniform: Some("1.0.0".to_string()),
            rolled_counts: BTreeSet::from([1, 2, 4]),
        };

        let (roll, mixed, violation) = roll_transition(roll, &versions(0, 5));

        assert_eq!(roll, uniform("2.0.0"));
        assert!(!mixed);
        assert_eq!(
            violation,
            Some("upgraded from 1.0.0 to 2.0.0 with 3 of 4 mixed clusters verified".to_string())
        );
    }

    #[test]
    fn a_change_without_a_mixed_cluster_names_the_direction() {
        let (_, _, violation) = roll_transition(uniform("2.0.0"), &versions(5, 0));
        assert_eq!(
            violation,
            Some("downgraded from 2.0.0 to 1.0.0 with 0 of 4 mixed clusters verified".to_string())
        );

        let (_, _, violation) = roll_transition(uniform("1.0.0"), &vec!["two".to_string(); 3]);
        assert_eq!(
            violation,
            Some("changed from 1.0.0 to two with 0 of 2 mixed clusters verified".to_string())
        );
    }

    /// A verification a node did not answer leaves the roll alone, so the window it stood for is
    /// never counted as verified.
    #[tokio::test]
    async fn a_verification_without_versions_leaves_the_roll_untouched() {
        let state = VerifierState::new(LB_OUTAGE);
        state.verified(Vec::new(), Some(versions(5, 0))).await;
        state.verified(Vec::new(), Some(versions(4, 1))).await;
        state
            .verified(vec!["node3 did not answer".to_string()], None)
            .await;

        let report = state.report.lock().await;
        assert_eq!(report.verifications, 3);
        assert_eq!(report.mixed_verifications, 1);
        assert_eq!(report.violations, 1);
        assert_eq!(report.roll.last_uniform.as_deref(), Some("1.0.0"));
        assert_eq!(report.roll.rolled_counts, BTreeSet::from([1]));
    }
}
