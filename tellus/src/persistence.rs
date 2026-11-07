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

/// The persistence wiring for an event-sourced actor: an event store, a snapshot store,
/// [NoSnapshots] unless one is set, and a codec, [Cbor] unless one is set. The codec is part of
/// the stored format: it cannot be changed once events have been written.
#[derive(Debug, Clone)]
pub struct Persistence<E, S = NoSnapshots, C = Cbor> {
    pub(crate) event_store: E,
    pub(crate) snapshot_store: S,
    pub(crate) codec: C,
}

impl<E> Persistence<E>
where
    E: EventStore,
{
    /// Create the persistence wiring with the given event store, without a snapshot store and
    /// with the default [Cbor] codec.
    pub fn new(event_store: E) -> Self {
        Self {
            event_store,
            snapshot_store: NoSnapshots,
            codec: Cbor,
        }
    }
}

impl<E, S, C> Persistence<E, S, C> {
    /// Use the given snapshot store.
    pub fn with_snapshot_store<T>(self, snapshot_store: T) -> Persistence<E, T, C>
    where
        T: SnapshotStore,
    {
        Persistence {
            event_store: self.event_store,
            snapshot_store,
            codec: self.codec,
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
        }
    }
}
