use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

/// Address plus per-process incarnation, so a restart is distinguishable from its predecessor.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[display("{addr}#{incarnation}")]
pub(crate) struct NodeId {
    addr: SocketAddr,
    incarnation: Incarnation,
}

impl NodeId {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            incarnation: Incarnation::new(),
        }
    }

    pub(crate) fn from_parts(addr: SocketAddr, incarnation: Incarnation) -> Self {
        Self { addr, incarnation }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    pub(crate) fn covers(&self, other: NodeId) -> bool {
        self.addr == other.addr && other.incarnation <= self.incarnation
    }
}

/// The identity of one process at an address, opaque: it can be compared, ordered, hashed,
/// printed and serialized, nothing else. Ordered, since UUIDv7 is time based: the younger
/// incarnation on a host compares greater, which is what resolves two incarnations at one address
/// toward the restart.
#[derive(
    Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Incarnation(Uuid);

impl Incarnation {
    fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

/// The identity of one cluster, minted when a node forms it and adopted by every node joining
/// it, opaque: it can be compared, ordered, hashed, printed and serialized, nothing else.
/// Ordered, since UUIDv7 is time based: a cluster formed later compares greater.
#[derive(
    Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ClusterId(Uuid);

impl ClusterId {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::node::{ClusterId, NodeId};
    use std::cmp::Ordering;

    #[test]
    fn a_cluster_formed_later_compares_greater() {
        let earlier = ClusterId::new();
        let later = ClusterId::new();

        assert!(earlier < later);
        assert_ne!(earlier.to_string(), later.to_string());
    }

    /// Supersession resolves toward the younger incarnation by comparing incarnations, so one
    /// minted later must compare greater.
    #[test]
    fn a_younger_incarnation_compares_greater() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let older = NodeId::new(addr).incarnation();
        let younger = NodeId::new(addr).incarnation();

        assert!(older < younger);
        assert!(younger > older);
        assert_eq!(older.cmp(&older), Ordering::Equal);
    }

    /// A node's textual identity is address plus incarnation, so two runs at the same address are
    /// distinguishable in a log line.
    #[test]
    fn a_node_displays_address_and_incarnation() {
        let addr = "127.0.0.1:1234".parse().expect("valid address");
        let node = NodeId::new(addr);
        let restarted = NodeId::new(addr);

        assert_eq!(
            node.to_string(),
            format!("127.0.0.1:1234#{}", node.incarnation())
        );
        assert_ne!(node.to_string(), restarted.to_string());
    }
}
