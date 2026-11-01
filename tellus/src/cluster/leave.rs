use crate::{ActorSystem, actor_system, cluster::endpoint};
use std::net::SocketAddr;
use thiserror::Error;

/// The report of a departure: the address of every peer connection [leave] drained. They are split
/// into those which acknowledged the drain and those which did not, e.g. since the connection ended
/// first or a stream failed. A third list holds those which had not acknowledged it within
/// [leave_timeout](crate::cluster::EndpointConfig::leave_timeout). Each list is in address order. A
/// peer acknowledges once every frame queued for it reached its delivery path, a local mailbox or a
/// logged dead letter; that does not mean any actor handled the frames.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Departure {
    pub(crate) acknowledged: Vec<SocketAddr>,
    pub(crate) unacknowledged: Vec<SocketAddr>,
    pub(crate) timed_out: Vec<SocketAddr>,
}

impl Departure {
    /// The peers which acknowledged that every frame queued for them, the departure included,
    /// reached their delivery path.
    pub fn acknowledged(&self) -> &[SocketAddr] {
        &self.acknowledged
    }

    /// The peers whose connection ended, or one of whose streams failed, before they acknowledged
    /// the drain.
    pub fn unacknowledged(&self) -> &[SocketAddr] {
        &self.unacknowledged
    }

    /// The peers which had not acknowledged the drain when the leave timeout passed.
    pub fn timed_out(&self) -> &[SocketAddr] {
        &self.timed_out
    }

    /// Whether every drained peer acknowledged the drain.
    pub fn is_acknowledged(&self) -> bool {
        self.unacknowledged.is_empty() && self.timed_out.is_empty()
    }

    pub(crate) fn sort(&mut self) {
        self.acknowledged.sort_unstable();
        self.unacknowledged.sort_unstable();
        self.timed_out.sort_unstable();
    }
}

/// The cluster cannot be left.
#[derive(Debug, Error)]
pub enum LeaveError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// This node was downed before it began to leave, so there is no departure to announce.
    #[error("this node has been downed")]
    Downed,

    /// The departure ended without a report, e.g. since it panicked. This node was downed and
    /// every connection severed before any caller learned of it. Whether the peers received what
    /// was queued for them is unknown.
    #[error("the departure did not complete, this node has been downed")]
    Incomplete,

    /// The actor system did not terminate cleanly; only [leave_on_terminated] returns this, and
    /// only after the departure was announced anyway.
    #[error(transparent)]
    Terminated(#[from] actor_system::TerminatedError),
}

/// Wait until the given actor system's root actor and all its descendants have terminated, then
/// [leave] the cluster: what a process calls instead of [ActorSystem::terminated] where that
/// system's lifetime is the node's lifetime.
///
/// Waiting first is what makes the real terminated signals the normal case. Once a root has
/// terminated, every local actor has run its destructors and every terminated signal owed to a
/// remote watcher is already queued. The departure is hence queued behind them, and the drain
/// covers both. See docs/cluster.md for the race which remains.
///
/// # Errors
/// Fails if the endpoint is not started, if this node was downed before it began to leave, if the
/// departure did not complete, or if the actor system did not terminate cleanly. In the last case
/// the departure was announced anyway.
pub async fn leave_on_terminated<M>(system: ActorSystem<M>) -> Result<Departure, LeaveError>
where
    M: Send + 'static,
{
    let terminated = system.terminated().await;
    let departure = leave().await?;
    terminated.map_err(LeaveError::from)?;
    Ok(departure)
}

/// Leave the cluster: gossip this node's own entry as [Left](crate::cluster::MemberState::Left),
/// drain every peer connection, then sever every connection, and report how each drain ended.
///
/// Draining a connection writes out everything queued for the peer, the departure included, and
/// finishes every data stream. Then a final control frame tells the peer how many data streams
/// there were. The peer acknowledges once exactly that many have ended and every frame they
/// carried was dispatched, and this node closes the connection.
///
/// A member learning of the departure, from this node or from any other member, treats this node
/// as dead at once: it sends nothing more to it and refuses it. If it has a connection from this
/// node, it keeps delivering what that connection still carries until every stream of it has
/// ended, or for at most its own
/// [leave_timeout](crate::cluster::EndpointConfig::leave_timeout). Then it runs the node death
/// sequence for this node, instead of waiting out failure detection plus downing as it does for a
/// node which merely fell silent. A downing of this node ends that wait at once.
///
/// Leaving is terminal for this process, exactly as being downed is: nothing is sent or admitted
/// afterward, [join](crate::cluster::join) fails as [Downed](crate::cluster::JoinError::Downed),
/// and only a restarted process, with a fresh incarnation, rejoins, so the caller is expected to
/// exit. A call made while another is leaving waits for that departure and returns its report,
/// as does leaving again afterward.
///
/// A departure which ends without a report, e.g. since it panicked, fails safe. This node is
/// downed and every connection severed, and only then does any caller learn of it. Every call,
/// concurrent or later, then fails as [Incomplete](LeaveError::Incomplete).
///
/// Nothing is enqueued once the departure is announced. An acknowledgment proves that the peer
/// dispatched every frame to its local mailboxes, or logged it as a dead letter there. It does not
/// prove that any actor handled them. The whole drain is bounded by one
/// [leave_timeout](crate::cluster::EndpointConfig::leave_timeout) deadline. A member which is gone
/// already costs that timeout and learns of the departure from another member's gossip, or, with
/// no live path at all, from its own failure detection, exactly as it would from a crash.
///
/// # Errors
/// Fails if the endpoint is not started, if this node was downed before it began to leave, or if
/// the departure did not complete.
pub async fn leave() -> Result<Departure, LeaveError> {
    let endpoint = endpoint::get().ok_or(LeaveError::EndpointNotStarted)?;
    endpoint.leave().await
}
