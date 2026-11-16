pub(crate) mod codec;
pub(crate) mod effect;
pub(crate) mod event_sourced;
#[cfg(feature = "persistence-in-memory")]
pub(crate) mod in_memory_store;
pub(crate) mod persistence_id;
pub(crate) mod seq_no;
pub(crate) mod spawn;
pub(crate) mod store;
pub(crate) mod versioned;

use crate::persistence::{
    codec::{Cbor, Codec},
    store::{EventStore, NoSnapshots, SnapshotStore},
};
use std::time::Duration;
use thiserror::Error;

/// The persistence wiring for an event-sourced actor: an event store, a snapshot store,
/// [NoSnapshots] unless one is set, a codec, [Cbor] unless one is set, and the timeout bounding
/// every store call, [Persistence::DEFAULT_TIMEOUT] unless one is set. The codec is part of the
/// stored format: it cannot be changed once events have been written.
#[derive(Debug, Clone)]
pub struct Persistence<E, S = NoSnapshots, C = Cbor> {
    pub(crate) event_store: E,
    pub(crate) snapshot_store: S,
    pub(crate) codec: C,
    pub(crate) timeout: Duration,
}

impl<E> Persistence<E>
where
    E: EventStore,
{
    /// Create the persistence wiring with the given event store, without a snapshot store, with
    /// the default [Cbor] codec and with [Persistence::DEFAULT_TIMEOUT].
    pub fn new(event_store: E) -> Self {
        Self {
            event_store,
            snapshot_store: NoSnapshots,
            codec: Cbor,
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }
}

impl<E, S, C> Persistence<E, S, C> {
    /// The timeout of a [Persistence] created by [Persistence::new].
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Use the given snapshot store.
    pub fn with_snapshot_store<T>(self, snapshot_store: T) -> Persistence<E, T, C>
    where
        T: SnapshotStore,
    {
        Persistence {
            event_store: self.event_store,
            snapshot_store,
            codec: self.codec,
            timeout: self.timeout,
        }
    }

    /// Use the given codec.
    pub fn with_codec<T>(self, codec: T) -> Persistence<E, S, T>
    where
        T: Codec,
    {
        Persistence {
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            codec,
            timeout: self.timeout,
        }
    }

    /// Bound every store call by the given timeout: the snapshot load and each read of a
    /// recovery, and the append and the snapshot save of a settlement. A load, read or append
    /// which exceeds it fails the actor into its supervision strategy, and a snapshot save which
    /// exceeds it is logged like a failed save. A call which exceeds it is dropped, which ends
    /// nothing the call has started in a server: a dropped append may still store its events.
    /// The timeout is hence a backstop, and a store bounds its own calls below it. A timeout the
    /// clock cannot represent, e.g. [Duration::MAX], is capped at about thirty years, as a
    /// timer's delay is.
    ///
    /// # Errors
    /// Fails if `timeout` is zero, which would fail every store call not answered at once.
    pub fn with_timeout(self, timeout: Duration) -> Result<Self, InvalidPersistence> {
        if timeout.is_zero() {
            return Err(InvalidPersistence::ZeroTimeout);
        }

        Ok(Self { timeout, ..self })
    }
}

/// The settings given to [Persistence] are invalid.
#[derive(Debug, Error)]
pub enum InvalidPersistence {
    /// The timeout given to [Persistence::with_timeout] is zero.
    #[error("store call timeout is zero")]
    ZeroTimeout,
}

#[cfg(all(test, feature = "persistence-in-memory"))]
mod tests {
    use crate::{InMemoryStore, InvalidPersistence, Persistence};
    use std::time::Duration;

    /// A new wiring bounds its store calls by the default timeout, and `with_timeout` replaces it.
    #[test]
    fn with_timeout_replaces_the_default_timeout() {
        let persistence = Persistence::new(InMemoryStore::default());
        assert_eq!(
            persistence.timeout,
            Persistence::<InMemoryStore>::DEFAULT_TIMEOUT
        );

        let persistence = persistence
            .with_timeout(Duration::from_secs(1))
            .expect("a nonzero timeout is valid");
        assert_eq!(persistence.timeout, Duration::from_secs(1));
    }

    /// A zero timeout is refused.
    #[test]
    fn with_timeout_refuses_zero() {
        let refused = Persistence::new(InMemoryStore::default()).with_timeout(Duration::ZERO);

        assert!(matches!(refused, Err(InvalidPersistence::ZeroTimeout)));
    }
}
