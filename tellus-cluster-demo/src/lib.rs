//! The JSON a demo node's and the verifier's HTTP APIs answer. Every node also mounts
//! `tellus-cluster-inspect` at `/inspect`, which serves the cluster state as well.

#![warn(missing_docs, clippy::missing_errors_doc)]

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, net::SocketAddr};
use tellus::cluster::{ClusterState, receptionist::Settlement};

/// What one node sees of the cluster, answered by `GET /cluster`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterView {
    /// The node's name, e.g. `node1`.
    pub name: String,

    /// How far the node is along joining the cluster.
    pub phase: Phase,

    /// The node's view of the cluster, this node's identity included.
    pub state: ClusterState,

    /// The receptionist's verdict, with the version it judged, never newer than `state`.
    pub settlement: Settlement,

    /// How many workers this node's receptionist resolves under the worker key, its own included.
    pub workers: usize,

    /// How many subscribers of the event topic this node's receptionist resolves, its own
    /// included.
    pub subscribers: usize,

    /// The addresses this node received an event from within the freshness window.
    pub publishers: BTreeSet<SocketAddr>,
}

impl ClusterView {
    /// The address the node advertises to the cluster.
    pub fn addr(&self) -> SocketAddr {
        self.state.this_addr()
    }

    /// The addresses this node lists as Up.
    pub fn up_addrs(&self) -> BTreeSet<SocketAddr> {
        self.state.up().map(|member| member.addr()).collect()
    }
}

/// Where a node is in its membership of the cluster, from joining to leaving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// The endpoint is started and seed discovery is running, but this node is not a member yet.
    Bootstrapping,

    /// A member of the cluster, with the worker registered under its key.
    Member,

    /// Downed by the cluster: the process exits, so a restart rejoins with a fresh incarnation.
    Downed,

    /// Shutting down on request: the process announces its departure, then exits.
    Leaving,
}

/// What one node's messaging to every other member yields, answered by `GET /probe`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProbeReport {
    /// The probing node's name.
    pub name: String,

    /// One entry per Up member other than this node.
    pub probes: Vec<Probe>,
}

/// A message round trip to the worker of one member.
#[derive(Debug, Serialize, Deserialize)]
pub struct Probe {
    /// The probed member's address.
    pub addr: SocketAddr,

    /// What the round trip yielded.
    pub outcome: ProbeOutcome,
}

/// The result of one probe.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeOutcome {
    /// The worker answered.
    Ok {
        /// The round trip time in milliseconds, discovery included.
        millis: u128,

        /// How many members the answering node lists as Up.
        up_members: usize,
    },

    /// The worker could not be resolved or did not answer.
    Failed {
        /// What went wrong.
        error: String,
    },
}

/// One message of a node's own event stream, `GET /events`: the demo's facts about the node,
/// beside the cluster state `/inspect/events` streams. Only [NodeEvent::Hello] is not logged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeEvent {
    /// Opens every stream. A changed `started_at` on a reconnect means the node restarted;
    /// `resumed` is `false` when the stream does not continue where the client left off, so the
    /// replay that follows is the whole retained history rather than the missing part.
    Hello {
        /// The node's name, e.g. `node1`.
        name: String,

        /// The address the node advertises to the cluster.
        addr: SocketAddr,

        /// Unix milliseconds at which the process started, the first half of every event id.
        started_at: u64,

        /// Whether the stream continues right after the client's `Last-Event-ID`.
        resumed: bool,
    },

    /// The node's phase changed.
    Phase {
        /// The new phase.
        phase: Phase,
    },

    /// The receptionist's counts for the demo's keys changed.
    Receptionist {
        /// How many workers the receptionist resolves under the worker key.
        workers: usize,

        /// How many subscribers of the event topic the receptionist resolves.
        subscribers: usize,
    },
}

/// One message of the verifier's event stream, `GET /events`. Only [VerifierEvent::Hello] is
/// not logged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VerifierEvent {
    /// Opens every stream, see [NodeEvent::Hello].
    Hello {
        /// Unix milliseconds at which the process started, the first half of every event id.
        started_at: u64,

        /// Whether the stream continues right after the client's `Last-Event-ID`.
        resumed: bool,
    },

    /// The chaos agent's state file changed.
    Chaos {
        /// The new value, e.g. `quiet` or `kill tellus-demo-node3`.
        value: String,
    },

    /// A verification ran once the chaos agent had been quiet for the verifier's settle time.
    Verified {
        /// How many violations it found.
        violations: usize,

        /// How long the chaos agent had been quiet, in milliseconds.
        quiet_for_millis: u64,
    },

    /// The cluster failed to deliver one of its promises.
    Violation {
        /// Unix seconds at which the violation was recorded.
        at: u64,

        /// What went wrong.
        detail: String,
    },

    /// The load balancer's availability was first observed or changed.
    Lb {
        /// Whether the load balancer answers.
        available: bool,

        /// The length of the outage which just ended, in milliseconds, when `available` turned
        /// `true` after failures.
        outage_millis: Option<u64>,
    },
}

/// The verifier's counters, answered by `GET /status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifierStatus {
    /// The chaos agent's state as last read, `unknown` until read.
    pub chaos: String,

    /// How many verifications have run.
    pub verifications: usize,

    /// How many violations have been recorded.
    pub violations: usize,

    /// How many requests the verifier has sent through the load balancer.
    pub lb_requests: usize,

    /// How many of those failed.
    pub lb_failures: usize,

    /// The longest load balancer outage so far, in milliseconds.
    pub longest_lb_outage_millis: u128,

    /// How long the chaos agent has been quiet, in milliseconds; `None` while it is not.
    pub quiet_for_millis: Option<u64>,

    /// Whether the load balancer answered the last request; `None` before the first.
    pub lb_available: Option<bool>,
}

/// One violation, answered by `GET /violations`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// Unix seconds at which the violation was recorded.
    pub at: u64,

    /// What went wrong.
    pub detail: String,
}
