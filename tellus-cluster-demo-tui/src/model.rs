use crate::{
    config::Config,
    sources::{Input, NodeInput, StreamInput, VerifierInput},
};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use std::{collections::BTreeSet, net::SocketAddr, time::Duration};
use tellus::cluster::{ClusterState, Member, MemberState, Transition, receptionist::Settlement};
use tellus_cluster_demo::{NodeEvent, Phase, ProbeReport, VerifierEvent, VerifierStatus};
use tellus_cluster_inspect::{InspectEvent, events::Stamped};

const TIMELINE_KEPT: usize = 1_000;

pub struct App {
    pub nodes: Vec<NodeSource>,
    pub verifier: VerifierSource,
    pub selected: usize,
    pub quit: bool,
    timeline: Vec<Entry>,
    frozen: Option<Vec<Entry>>,
    next_seq: u64,
}

impl App {
    pub fn new(config: &Config) -> Self {
        Self {
            nodes: config
                .nodes
                .iter()
                .map(|url| NodeSource::new(url))
                .collect(),
            verifier: VerifierSource::new(&config.verifier),
            selected: 0,
            quit: false,
            timeline: Vec::new(),
            frozen: None,
            next_seq: 0,
        }
    }

    /// `now` is unix milliseconds; it stamps only what the servers do not, connection changes.
    pub fn apply(&mut self, input: Input, now: u64) {
        match input {
            Input::Key(key) => self.key(key),
            Input::Tick => {}
            Input::Node(index, input) => self.node_input(index, input, now),
            Input::Verifier(input) => self.verifier_input(input, now),
        }
    }

    pub fn columns(&self) -> Vec<SocketAddr> {
        let mut columns = BTreeSet::new();
        for node in &self.nodes {
            columns.extend(node.addr);
            if let Some(snapshot) = &node.snapshot {
                columns.extend(snapshot.members().iter().map(Member::addr));
            }
        }
        columns.into_iter().collect()
    }

    pub fn name_of(&self, addr: SocketAddr) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.addr == Some(addr))
            .and_then(|node| node.name.as_deref())
    }

    pub fn cell(&self, index: usize, addr: SocketAddr) -> Cell {
        let node = &self.nodes[index];
        let Some(snapshot) = &node.snapshot else {
            return Cell::Unknown;
        };
        if snapshot.this_addr() == addr {
            return Cell::Me;
        }
        if snapshot
            .unreachable()
            .iter()
            .any(|member| member.addr() == addr)
        {
            return Cell::Unreachable;
        }
        let has = |state| {
            snapshot
                .members()
                .iter()
                .any(|member| member.addr() == addr && member.state() == state)
        };
        let dead = has(MemberState::Left) || has(MemberState::Down);
        match (has(MemberState::Up), dead) {
            (true, true) => Cell::UpAndDown,
            (true, false) => Cell::Up,
            (false, true) if has(MemberState::Down) => Cell::Down,
            (false, true) => Cell::Left,
            (false, false) => Cell::Unknown,
        }
    }

    pub fn timeline(&self) -> &[Entry] {
        self.frozen.as_deref().unwrap_or(&self.timeline)
    }

    pub fn paused(&self) -> bool {
        self.frozen.is_some()
    }

    fn key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.quit = true;
            }
            KeyCode::Down | KeyCode::Tab | KeyCode::Char('j') => {
                self.selected = (self.selected + 1) % self.nodes.len();
            }
            KeyCode::Up | KeyCode::BackTab | KeyCode::Char('k') => {
                self.selected = (self.selected + self.nodes.len() - 1) % self.nodes.len();
            }
            KeyCode::Char('p') => {
                self.frozen = match self.frozen {
                    Some(_) => None,
                    None => Some(self.timeline.clone()),
                };
            }
            KeyCode::Char('c') => {
                self.timeline.clear();
                if let Some(frozen) = &mut self.frozen {
                    frozen.clear();
                }
            }
            _ => {}
        }
    }

    fn node_input(&mut self, index: usize, input: NodeInput, now: u64) {
        let label = self.nodes[index].label().to_string();
        let source = Source::Node(index);
        match input {
            NodeInput::Stream(StreamInput::Connected) => {
                let node = &mut self.nodes[index];
                if !matches!(node.connection, Connection::Connected { .. }) {
                    node.connection = Connection::Connected { since: now };
                    self.record(
                        now,
                        source,
                        Severity::Info,
                        format!("{label} stream connected"),
                    );
                }
            }

            NodeInput::Stream(StreamInput::Disconnected(reason)) => {
                let node = &mut self.nodes[index];
                if matches!(node.connection, Connection::Connected { .. }) {
                    self.record(
                        now,
                        source,
                        Severity::Warn,
                        format!("{label} stream lost: {reason}"),
                    );
                }
                self.nodes[index].connection = Connection::Disconnected(reason);
            }

            NodeInput::Stream(StreamInput::Event(Stamped { at, event })) => {
                self.node_event(index, at, event);
            }

            NodeInput::Inspect(StreamInput::Connected) => {
                let node = &mut self.nodes[index];
                if !matches!(node.inspect_connection, Connection::Connected { .. }) {
                    node.inspect_connection = Connection::Connected { since: now };
                    node.stale_since = None;
                    self.record(
                        now,
                        source,
                        Severity::Info,
                        format!("{label} inspect stream connected"),
                    );
                }
            }

            NodeInput::Inspect(StreamInput::Disconnected(reason)) => {
                let node = &mut self.nodes[index];
                if matches!(node.inspect_connection, Connection::Connected { .. }) {
                    node.stale_since = Some(now);
                    self.record(
                        now,
                        source,
                        Severity::Warn,
                        format!("{label} inspect stream lost: {reason}"),
                    );
                }
                self.nodes[index].inspect_connection = Connection::Disconnected(reason);
            }

            NodeInput::Inspect(StreamInput::Event(Stamped { at, event })) => {
                self.inspect_event(index, at, event);
            }

            // The poll fills only what the streams have not said: theirs is the newer.
            NodeInput::Cluster(view) => {
                let node = &mut self.nodes[index];
                node.publishers = view.publishers;
                if node.phase.is_none() {
                    node.phase = Some(view.phase);
                }
                if node.app_version.is_none() {
                    node.app_version = Some(view.app_version);
                }
                if node.workers.is_none() {
                    node.workers = Some(view.workers);
                }
                if node.subscribers.is_none() {
                    node.subscribers = Some(view.subscribers);
                }
            }

            NodeInput::Probe(report) => self.nodes[index].probes = Some(report),
        }
    }

    fn node_event(&mut self, index: usize, at: u64, event: NodeEvent) {
        let source = Source::Node(index);
        match event {
            NodeEvent::Hello {
                name,
                addr,
                app_version,
                started_at,
                resumed,
            } => {
                let node = &mut self.nodes[index];
                node.name = Some(name);
                node.addr = Some(addr);
                match node.started_at {
                    Some(previous) if previous != started_at => {
                        node.started_at = Some(started_at);
                        node.reset_demo_facts();
                    }

                    Some(_) if !resumed => node.reset_demo_facts(),

                    Some(_) => {}

                    None => node.started_at = Some(started_at),
                }
                node.app_version = Some(app_version);
            }

            NodeEvent::Phase { phase } => {
                let node = &mut self.nodes[index];
                node.phase = Some(phase);
                let label = node.label().to_string();
                let (severity, text) = match phase {
                    Phase::Bootstrapping => (Severity::Info, format!("{label} bootstrapping")),
                    Phase::Member => (Severity::Info, format!("{label} is a member")),
                    Phase::Downed => (Severity::Error, format!("{label} downed")),
                    Phase::Leaving => (Severity::Info, format!("{label} leaving")),
                };
                self.record(at, source, severity, text);
            }

            NodeEvent::Receptionist {
                workers,
                subscribers,
            } => {
                let node = &mut self.nodes[index];
                node.workers = Some(workers);
                node.subscribers = Some(subscribers);
            }
        }
    }

    fn inspect_event(&mut self, index: usize, at: u64, event: InspectEvent) {
        let source = Source::Node(index);
        let label = self.nodes[index].label().to_string();
        match event {
            InspectEvent::Hello {
                started_at,
                resumed,
            } => {
                let node = &mut self.nodes[index];
                match node.inspect_started_at {
                    Some(previous) if previous != started_at => {
                        node.inspect_started_at = Some(started_at);
                        node.reset_cluster_facts();
                        self.record(at, source, Severity::Warn, format!("{label} restarted"));
                    }

                    Some(_) if !resumed => {
                        node.seeded = false;
                        self.record(
                            at,
                            source,
                            Severity::Warn,
                            format!("{label} history lost, replaying what is retained"),
                        );
                    }

                    Some(_) => {}

                    None => node.inspect_started_at = Some(started_at),
                }
            }

            InspectEvent::State(state) => {
                let node = &mut self.nodes[index];
                let displayed = node.snapshot.as_ref().map(ClusterState::version);
                match displayed {
                    // Older than what is shown: a duplicate, never a reverse transition.
                    Some(version) if state.version() < version => {}

                    // The recovery pair's state: re-arms the diff without transitions.
                    Some(version) if state.version() == version => {
                        node.snapshot = Some(state);
                        node.seeded = true;
                    }

                    _ => {
                        let transitions = match (&node.snapshot, node.seeded) {
                            (Some(previous), true) => state.diff(previous),
                            _ => Vec::new(),
                        };
                        let version = state.version();
                        node.snapshot = Some(state);
                        node.seeded = true;
                        for transition in transitions {
                            let (severity, text) = describe(&transition);
                            self.record(
                                at,
                                source,
                                severity,
                                format!("{label} v{version}: {text}"),
                            );
                        }
                    }
                }
            }

            InspectEvent::Settled(settlement) => {
                let node = &mut self.nodes[index];
                let flipped = node
                    .settlement
                    .is_none_or(|previous| previous != settlement);
                node.settlement = Some(settlement);
                if flipped {
                    self.record(
                        at,
                        source,
                        Severity::Info,
                        format!(
                            "{label} settled {} at v{}",
                            settlement.settled(),
                            settlement.version()
                        ),
                    );
                }
            }

            InspectEvent::Gap { dropped } => {
                self.nodes[index].seeded = false;
                self.record(
                    at,
                    source,
                    Severity::Warn,
                    format!("{label} fell behind by {dropped} changes"),
                );
            }
        }
    }

    fn verifier_input(&mut self, input: VerifierInput, now: u64) {
        match input {
            VerifierInput::Stream(StreamInput::Connected) => {
                if !matches!(self.verifier.connection, Connection::Connected { .. }) {
                    self.verifier.connection = Connection::Connected { since: now };
                    self.record(
                        now,
                        Source::Verifier,
                        Severity::Info,
                        "verifier stream connected".to_string(),
                    );
                }
            }

            VerifierInput::Stream(StreamInput::Disconnected(reason)) => {
                if matches!(self.verifier.connection, Connection::Connected { .. }) {
                    self.record(
                        now,
                        Source::Verifier,
                        Severity::Warn,
                        format!("verifier stream lost: {reason}"),
                    );
                }
                self.verifier.connection = Connection::Disconnected(reason);
            }

            VerifierInput::Stream(StreamInput::Event(Stamped { at, event })) => {
                self.verifier_event(at, event);
            }

            VerifierInput::Status(status) => self.verifier.status = Some(status),
        }
    }

    fn verifier_event(&mut self, at: u64, event: VerifierEvent) {
        let source = Source::Verifier;
        match event {
            VerifierEvent::Hello {
                started_at,
                resumed,
            } => match self.verifier.started_at {
                Some(previous) if previous != started_at => {
                    self.verifier.started_at = Some(started_at);
                    self.verifier.status = None;
                    self.verifier.chaos = None;
                    self.record(at, source, Severity::Warn, "verifier restarted".to_string());
                }

                Some(_) if !resumed => {
                    self.record(
                        at,
                        source,
                        Severity::Warn,
                        "verifier history lost, replaying what is retained".to_string(),
                    );
                }

                Some(_) => {}

                None => self.verifier.started_at = Some(started_at),
            },

            VerifierEvent::Chaos { value } => {
                self.record(at, source, Severity::Info, format!("chaos: {value}"));
                self.verifier.chaos = Some(value);
            }

            VerifierEvent::Verified {
                violations,
                mixed,
                quiet_for_millis,
            } => {
                let quiet_for = Duration::from_millis(quiet_for_millis);
                let (severity, text) = if violations == 0 {
                    let versions = if mixed { ", mixed versions" } else { "" };
                    (
                        Severity::Info,
                        format!("cluster verified after {quiet_for:.0?} quiet{versions}"),
                    )
                } else {
                    (
                        Severity::Error,
                        format!("verification found {violations} violations"),
                    )
                };
                self.record(at, source, severity, text);
            }

            VerifierEvent::Violation { detail, .. } => {
                self.record(at, source, Severity::Error, format!("violation: {detail}"));
            }

            VerifierEvent::Lb {
                available,
                outage_millis,
            } => {
                let (severity, text) = match (available, outage_millis) {
                    (false, _) => (Severity::Warn, "load balancer unavailable".to_string()),
                    (true, Some(millis)) => (
                        Severity::Info,
                        format!(
                            "load balancer back after {:.0?}",
                            Duration::from_millis(millis)
                        ),
                    ),
                    (true, None) => (Severity::Info, "load balancer available".to_string()),
                };
                self.record(at, source, severity, text);
            }
        }
    }

    /// Kept sorted by stamp, then source, then arrival: the sources deliver their replays in any
    /// order, and two sources can stamp the same millisecond.
    fn record(&mut self, at: u64, source: Source, severity: Severity, text: String) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let entry = Entry {
            at,
            source,
            severity,
            text,
            seq,
        };
        let position = self
            .timeline
            .partition_point(|other| other.order() <= entry.order());
        self.timeline.insert(position, entry);
        if self.timeline.len() > TIMELINE_KEPT {
            self.timeline.remove(0);
        }
    }
}

pub struct NodeSource {
    pub url: String,
    pub connection: Connection,
    pub inspect_connection: Connection,
    pub name: Option<String>,
    pub addr: Option<SocketAddr>,
    pub started_at: Option<u64>,
    pub inspect_started_at: Option<u64>,
    pub phase: Option<Phase>,
    pub app_version: Option<String>,
    pub snapshot: Option<ClusterState>,
    pub settlement: Option<Settlement>,
    pub workers: Option<usize>,
    pub subscribers: Option<usize>,
    pub publishers: BTreeSet<SocketAddr>,
    pub probes: Option<ProbeReport>,
    pub stale_since: Option<u64>,
    seeded: bool,
}

impl NodeSource {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            connection: Connection::Connecting,
            inspect_connection: Connection::Connecting,
            name: None,
            addr: None,
            started_at: None,
            inspect_started_at: None,
            phase: None,
            app_version: None,
            snapshot: None,
            settlement: None,
            workers: None,
            subscribers: None,
            publishers: BTreeSet::new(),
            probes: None,
            stale_since: None,
            seeded: false,
        }
    }

    pub fn label(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.url)
    }

    fn reset_demo_facts(&mut self) {
        self.phase = None;
        self.app_version = None;
        self.workers = None;
        self.subscribers = None;
        self.publishers.clear();
        self.probes = None;
    }

    fn reset_cluster_facts(&mut self) {
        self.snapshot = None;
        self.settlement = None;
        self.seeded = false;
    }
}

pub struct VerifierSource {
    pub url: String,
    pub connection: Connection,
    pub started_at: Option<u64>,
    pub status: Option<VerifierStatus>,
    pub chaos: Option<String>,
}

impl VerifierSource {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            connection: Connection::Connecting,
            started_at: None,
            status: None,
            chaos: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Connection {
    Connecting,
    Connected { since: u64 },
    Disconnected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub at: u64,
    pub source: Source,
    pub severity: Severity,
    pub text: String,
    seq: u64,
}

impl Entry {
    fn order(&self) -> (u64, usize, u64) {
        (self.at, self.source.order(), self.seq)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Node(usize),
    Verifier,
}

impl Source {
    fn order(self) -> usize {
        match self {
            Self::Node(index) => index,
            Self::Verifier => usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cell {
    Unknown,
    Up,
    UpAndDown,
    Left,
    Down,
    Unreachable,
    Me,
}

/// A member as the timeline names it: the address and the incarnation's last four hex digits,
/// so two incarnations at one address read apart.
pub fn short(member: &Member) -> String {
    let incarnation = member.incarnation().to_string();
    let suffix = &incarnation[incarnation.len().saturating_sub(4)..];
    format!("{}#{suffix}", member.addr())
}

fn describe(transition: &Transition) -> (Severity, String) {
    let member = short(transition.member());
    match transition {
        Transition::Up(_) => (Severity::Info, format!("{member} up")),
        Transition::Left(_) => (Severity::Info, format!("{member} left")),
        Transition::Down(_) => (Severity::Warn, format!("{member} down")),
        Transition::Forgotten(_) => (Severity::Info, format!("{member} forgotten")),
        Transition::Unreachable(_) => (Severity::Warn, format!("{member} unreachable")),
        Transition::Reachable(_) => (Severity::Info, format!("{member} reachable again")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tellus_cluster_demo::ClusterView;

    const NOW: u64 = 1_700_000_000_000;

    fn config() -> Config {
        Config {
            nodes: vec!["http://n1".to_string(), "http://n2".to_string()],
            verifier: "http://v".to_string(),
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("10.0.0.{}:{port}", port - 7000)
            .parse()
            .expect("valid address")
    }

    fn incarnation(port: u16, n: u8) -> String {
        format!("019a0000-0000-7000-8000-00000{port:05}{n:02}")
    }

    fn member(port: u16, n: u8, state: &str) -> Value {
        json!({ "addr": addr(port), "incarnation": incarnation(port, n), "state": state })
    }

    fn up(port: u16) -> Value {
        member(port, 1, "Up")
    }

    fn down(port: u16) -> Value {
        member(port, 1, "Down")
    }

    fn state(version: u64, members: Vec<Value>, unreachable: Vec<Value>) -> ClusterState {
        serde_json::from_value(json!({
            "version": version,
            "this": { "addr": addr(7001), "incarnation": incarnation(7001, 1) },
            "cluster": "019a0000-0000-7000-8000-0000000000c1",
            "members": members,
            "unreachable": unreachable,
            "broken_links": [],
            "foreign_clusters": [],
        }))
        .expect("a cluster state deserializes")
    }

    fn settlement(version: u64, settled: bool) -> Settlement {
        serde_json::from_value(json!({ "version": version, "settled": settled }))
            .expect("a settlement deserializes")
    }

    fn hello(started_at: u64, resumed: bool) -> NodeEvent {
        hello_of("1.0.0", started_at, resumed)
    }

    fn hello_of(app_version: &str, started_at: u64, resumed: bool) -> NodeEvent {
        NodeEvent::Hello {
            name: "node1".to_string(),
            addr: addr(7001),
            app_version: app_version.to_string(),
            started_at,
            resumed,
        }
    }

    fn inspect_hello(started_at: u64, resumed: bool) -> InspectEvent {
        InspectEvent::Hello {
            started_at,
            resumed,
        }
    }

    fn demo(app: &mut App, index: usize, at: u64, event: NodeEvent) {
        app.apply(
            Input::Node(
                index,
                NodeInput::Stream(StreamInput::Event(Stamped { at, event })),
            ),
            NOW,
        );
    }

    fn inspect(app: &mut App, index: usize, at: u64, event: InspectEvent) {
        app.apply(
            Input::Node(
                index,
                NodeInput::Inspect(StreamInput::Event(Stamped { at, event })),
            ),
            NOW,
        );
    }

    fn texts(app: &App) -> Vec<&str> {
        app.timeline()
            .iter()
            .map(|entry| entry.text.as_str())
            .collect()
    }

    fn node1(app: &mut App) {
        demo(app, 0, 1, hello(100, false));
        inspect(app, 0, 1, inspect_hello(100, false));
        inspect(
            app,
            0,
            2,
            InspectEvent::State(state(1, vec![up(7001), up(7002)], vec![])),
        );
    }

    #[test]
    fn a_member_going_down_is_one_transition_and_the_cell_follows() {
        let mut app = App::new(&config());
        node1(&mut app);
        assert_eq!(app.cell(0, addr(7002)), Cell::Up);
        assert_eq!(app.cell(0, addr(7001)), Cell::Me);

        inspect(
            &mut app,
            0,
            3,
            InspectEvent::State(state(2, vec![up(7001), down(7002)], vec![])),
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::Down);
        assert_eq!(texts(&app), vec!["node1 v2: 10.0.0.2:7002#0201 down"]);
    }

    #[test]
    fn a_member_leaving_is_one_transition_and_the_cell_follows() {
        let mut app = App::new(&config());
        node1(&mut app);

        inspect(
            &mut app,
            0,
            3,
            InspectEvent::State(state(2, vec![up(7001), member(7002, 1, "Left")], vec![])),
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::Left);
        assert_eq!(texts(&app), vec!["node1 v2: 10.0.0.2:7002#0201 left"]);
    }

    #[test]
    fn a_restart_names_both_incarnations() {
        let mut app = App::new(&config());
        node1(&mut app);
        inspect(
            &mut app,
            0,
            3,
            InspectEvent::State(state(
                2,
                vec![up(7001), down(7002), member(7002, 2, "Up")],
                vec![],
            )),
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::UpAndDown);
        assert_eq!(
            texts(&app),
            vec![
                "node1 v2: 10.0.0.2:7002#0201 down",
                "node1 v2: 10.0.0.2:7002#0202 up"
            ]
        );

        inspect(
            &mut app,
            0,
            4,
            InspectEvent::State(state(3, vec![up(7001), member(7002, 2, "Up")], vec![])),
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::Up);
        assert_eq!(
            texts(&app).last(),
            Some(&"node1 v3: 10.0.0.2:7002#0201 forgotten")
        );
    }

    #[test]
    fn an_unreachable_member_downed_is_not_reachable_again() {
        let mut app = App::new(&config());
        node1(&mut app);
        inspect(
            &mut app,
            0,
            3,
            InspectEvent::State(state(2, vec![up(7001), up(7002)], vec![up(7002)])),
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::Unreachable);
        inspect(
            &mut app,
            0,
            4,
            InspectEvent::State(state(3, vec![up(7001), down(7002)], vec![])),
        );
        assert_eq!(
            texts(&app),
            vec![
                "node1 v2: 10.0.0.2:7002#0201 unreachable",
                "node1 v3: 10.0.0.2:7002#0201 down"
            ]
        );
    }

    #[test]
    fn a_restart_of_the_node_clears_it_and_the_next_state_seeds_silently() {
        let mut app = App::new(&config());
        node1(&mut app);
        demo(
            &mut app,
            0,
            2,
            NodeEvent::Phase {
                phase: Phase::Member,
            },
        );

        inspect(&mut app, 0, 4, inspect_hello(200, false));
        demo(&mut app, 0, 4, hello(200, false));
        assert_eq!(app.nodes[0].phase, None);
        assert!(app.nodes[0].snapshot.is_none());
        assert_eq!(app.cell(0, addr(7002)), Cell::Unknown);

        inspect(
            &mut app,
            0,
            5,
            InspectEvent::State(state(0, vec![up(7001)], vec![])),
        );
        assert_eq!(texts(&app), vec!["node1 is a member", "node1 restarted"]);
    }

    #[test]
    fn lost_history_reseeds_without_transitions() {
        let mut app = App::new(&config());
        node1(&mut app);

        inspect(&mut app, 0, 3, inspect_hello(100, false));
        inspect(
            &mut app,
            0,
            4,
            InspectEvent::State(state(9, vec![up(7001), down(7002)], vec![])),
        );
        assert_eq!(
            texts(&app),
            vec!["node1 history lost, replaying what is retained"]
        );
        assert_eq!(app.cell(0, addr(7002)), Cell::Down);
    }

    /// After a gap or a lost history the recovery state equals the displayed one: it re-arms the
    /// diff, so the next real change is diffed instead of being swallowed as another seed.
    #[test]
    fn an_equal_recovery_state_rearms_the_diff() {
        for reset in [InspectEvent::Gap { dropped: 3 }, inspect_hello(100, false)] {
            let mut app = App::new(&config());
            node1(&mut app);
            inspect(&mut app, 0, 3, reset);
            let before = texts(&app).len();

            inspect(
                &mut app,
                0,
                4,
                InspectEvent::State(state(1, vec![up(7001), up(7002)], vec![])),
            );
            assert_eq!(texts(&app).len(), before);

            inspect(
                &mut app,
                0,
                5,
                InspectEvent::State(state(2, vec![up(7001), down(7002)], vec![])),
            );
            assert_eq!(
                texts(&app).last(),
                Some(&"node1 v2: 10.0.0.2:7002#0201 down")
            );
        }
    }

    #[test]
    fn an_older_state_is_ignored_and_does_not_rearm() {
        let mut app = App::new(&config());
        node1(&mut app);
        inspect(
            &mut app,
            0,
            3,
            InspectEvent::State(state(2, vec![up(7001), up(7002)], vec![])),
        );
        inspect(&mut app, 0, 4, InspectEvent::Gap { dropped: 1 });

        inspect(
            &mut app,
            0,
            5,
            InspectEvent::State(state(1, vec![up(7001)], vec![])),
        );
        assert_eq!(
            app.nodes[0].snapshot.as_ref().map(ClusterState::version),
            Some(2)
        );
        inspect(
            &mut app,
            0,
            6,
            InspectEvent::State(state(3, vec![up(7001), down(7002)], vec![])),
        );
        assert_eq!(texts(&app), vec!["node1 fell behind by 1 changes"]);
    }

    #[test]
    fn a_resumed_stream_keeps_diffing() {
        let mut app = App::new(&config());
        node1(&mut app);
        inspect(&mut app, 0, 3, inspect_hello(100, true));
        inspect(
            &mut app,
            0,
            4,
            InspectEvent::State(state(2, vec![up(7001), down(7002)], vec![])),
        );
        assert_eq!(texts(&app), vec!["node1 v2: 10.0.0.2:7002#0201 down"]);
    }

    #[test]
    fn a_verdict_is_shown_with_its_version_and_flips_make_entries() {
        let mut app = App::new(&config());
        node1(&mut app);
        inspect(&mut app, 0, 3, InspectEvent::Settled(settlement(1, false)));
        inspect(&mut app, 0, 4, InspectEvent::Settled(settlement(1, true)));
        inspect(&mut app, 0, 5, InspectEvent::Settled(settlement(1, true)));
        assert_eq!(app.nodes[0].settlement, Some(settlement(1, true)));
        assert_eq!(
            texts(&app),
            vec!["node1 settled false at v1", "node1 settled true at v1"]
        );
    }

    #[test]
    fn the_poll_supplies_what_the_streams_have_not_said() {
        let mut app = App::new(&config());
        node1(&mut app);
        assert_eq!(app.nodes[0].phase, None);

        let view = |phase: Phase| {
            serde_json::from_value::<ClusterView>(json!({
                "name": "node1",
                "phase": phase,
                "app_version": "2.0.0",
                "state": serde_json::to_value(state(1, vec![up(7001)], vec![])).expect("json"),
                "settlement": { "version": 1, "settled": true },
                "workers": 4,
                "subscribers": 5,
                "publishers": [addr(7002)],
            }))
            .expect("a view deserializes")
        };
        app.apply(Input::Node(0, NodeInput::Cluster(view(Phase::Member))), NOW);
        assert_eq!(app.nodes[0].phase, Some(Phase::Member));
        assert_eq!(
            (app.nodes[0].workers, app.nodes[0].subscribers),
            (Some(4), Some(5))
        );
        assert_eq!(app.nodes[0].publishers.len(), 1);
        assert!(texts(&app).is_empty());

        demo(
            &mut app,
            0,
            3,
            NodeEvent::Phase {
                phase: Phase::Downed,
            },
        );
        demo(
            &mut app,
            0,
            3,
            NodeEvent::Receptionist {
                workers: 1,
                subscribers: 1,
            },
        );
        app.apply(Input::Node(0, NodeInput::Cluster(view(Phase::Member))), NOW);
        assert_eq!(app.nodes[0].phase, Some(Phase::Downed));
        assert_eq!(app.nodes[0].workers, Some(1));
    }

    /// The version comes from the hello, which a restart with another version replaces; the poll
    /// fills it only while the stream has not said it.
    #[test]
    fn the_version_follows_the_hello_and_the_poll_fills_it_only_when_unknown() {
        let mut app = App::new(&config());
        let view = serde_json::from_value::<ClusterView>(json!({
            "name": "node1",
            "phase": Phase::Member,
            "app_version": "3.0.0",
            "state": serde_json::to_value(state(1, vec![up(7001)], vec![])).expect("json"),
            "settlement": { "version": 1, "settled": true },
            "workers": 1,
            "subscribers": 1,
            "publishers": [],
        }))
        .expect("a view deserializes");
        app.apply(Input::Node(0, NodeInput::Cluster(view.clone())), NOW);
        assert_eq!(app.nodes[0].app_version.as_deref(), Some("3.0.0"));

        demo(&mut app, 0, 1, hello_of("1.0.0", 100, false));
        assert_eq!(app.nodes[0].app_version.as_deref(), Some("1.0.0"));
        app.apply(Input::Node(0, NodeInput::Cluster(view)), NOW);
        assert_eq!(app.nodes[0].app_version.as_deref(), Some("1.0.0"));

        demo(&mut app, 0, 2, hello_of("2.0.0", 200, false));
        assert_eq!(app.nodes[0].app_version.as_deref(), Some("2.0.0"));
        assert_eq!(app.nodes[0].phase, None);
    }

    #[test]
    fn a_mixed_verification_says_so_on_the_timeline() {
        let mut app = App::new(&config());
        let verified = |mixed| {
            Input::Verifier(VerifierInput::Stream(StreamInput::Event(Stamped {
                at: 1,
                event: VerifierEvent::Verified {
                    violations: 0,
                    mixed,
                    quiet_for_millis: 30_000,
                },
            })))
        };
        app.apply(verified(false), NOW);
        app.apply(verified(true), NOW);
        assert_eq!(
            texts(&app),
            vec![
                "cluster verified after 30s quiet",
                "cluster verified after 30s quiet, mixed versions"
            ]
        );
    }

    #[test]
    fn entries_are_ordered_by_stamp_then_source_then_arrival() {
        let mut app = App::new(&config());
        demo(
            &mut app,
            1,
            50,
            NodeEvent::Phase {
                phase: Phase::Member,
            },
        );
        app.apply(
            Input::Verifier(VerifierInput::Stream(StreamInput::Event(Stamped {
                at: 20,
                event: VerifierEvent::Chaos {
                    value: "quiet".to_string(),
                },
            }))),
            NOW,
        );
        demo(
            &mut app,
            0,
            40,
            NodeEvent::Phase {
                phase: Phase::Downed,
            },
        );
        demo(
            &mut app,
            1,
            40,
            NodeEvent::Phase {
                phase: Phase::Bootstrapping,
            },
        );
        assert_eq!(
            texts(&app),
            vec![
                "chaos: quiet",
                "http://n1 downed",
                "http://n2 bootstrapping",
                "http://n2 is a member"
            ]
        );
    }

    #[test]
    fn a_verifier_restart_drops_the_cached_status() {
        let mut app = App::new(&config());
        let hello = |started_at| {
            Input::Verifier(VerifierInput::Stream(StreamInput::Event(Stamped {
                at: 1,
                event: VerifierEvent::Hello {
                    started_at,
                    resumed: false,
                },
            })))
        };
        app.apply(hello(100), NOW);
        app.apply(
            Input::Verifier(VerifierInput::Status(VerifierStatus {
                chaos: "quiet".to_string(),
                verifications: 3,
                mixed_verifications: 0,
                violations: 0,
                lb_requests: 10,
                lb_failures: 0,
                longest_lb_outage_millis: 0,
                quiet_for_millis: Some(1),
                lb_available: Some(true),
            })),
            NOW,
        );
        assert!(app.verifier.status.is_some());

        app.apply(hello(200), NOW);
        assert!(app.verifier.status.is_none());
        assert_eq!(texts(&app), vec!["verifier restarted"]);
    }

    #[test]
    fn pausing_freezes_the_visible_window_only() {
        let mut app = App::new(&config());
        node1(&mut app);
        demo(
            &mut app,
            0,
            2,
            NodeEvent::Phase {
                phase: Phase::Member,
            },
        );
        app.apply(Input::Key(KeyEvent::from(KeyCode::Char('p'))), NOW);
        demo(
            &mut app,
            0,
            3,
            NodeEvent::Phase {
                phase: Phase::Downed,
            },
        );
        // Replayed from another source with an older stamp: hidden while paused all the same.
        app.apply(
            Input::Verifier(VerifierInput::Stream(StreamInput::Event(Stamped {
                at: 1,
                event: VerifierEvent::Chaos {
                    value: "quiet".to_string(),
                },
            }))),
            NOW,
        );
        assert_eq!(texts(&app), vec!["node1 is a member"]);
        assert_eq!(app.nodes[0].phase, Some(Phase::Downed));

        app.apply(Input::Key(KeyEvent::from(KeyCode::Char('p'))), NOW);
        assert_eq!(
            texts(&app),
            vec!["chaos: quiet", "node1 is a member", "node1 downed"]
        );
    }

    #[test]
    fn the_two_streams_have_their_own_health_and_a_lost_inspect_stream_marks_the_row_stale() {
        let mut app = App::new(&config());
        let demo_stream = |input| Input::Node(0, NodeInput::Stream(input));
        let inspect_stream = |input| Input::Node(0, NodeInput::Inspect(input));
        app.apply(demo_stream(StreamInput::Connected), NOW);
        app.apply(inspect_stream(StreamInput::Connected), NOW);
        node1(&mut app);

        app.apply(
            inspect_stream(StreamInput::Disconnected("gone".to_string())),
            NOW + 5,
        );
        app.apply(
            inspect_stream(StreamInput::Disconnected("gone".to_string())),
            NOW + 6,
        );
        assert!(matches!(
            app.nodes[0].connection,
            Connection::Connected { .. }
        ));
        assert!(matches!(
            app.nodes[0].inspect_connection,
            Connection::Disconnected(_)
        ));
        assert_eq!(app.nodes[0].stale_since, Some(NOW + 5));
        assert!(app.nodes[0].snapshot.is_some());
        assert_eq!(
            texts(&app),
            vec![
                "http://n1 stream connected",
                "http://n1 inspect stream connected",
                "node1 inspect stream lost: gone"
            ]
        );

        app.apply(inspect_stream(StreamInput::Connected), NOW + 7);
        assert_eq!(app.nodes[0].stale_since, None);
    }
}
