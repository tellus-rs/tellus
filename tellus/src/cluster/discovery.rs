use crate::{
    ActorRef,
    cluster::{
        endpoint,
        manifest::{self, Manifest},
        receptionist,
        registry::LimitExceeded,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    any,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
};
use thiserror::Error;

/// The name an actor is registered under for discovery, typed by the actor's message type. The
/// type's [Manifest] travels with the name, so a [lookup](receptionist::lookup) naming another
/// type is refused rather than resolved into a reference which drops every message it is told.
pub struct Key<M> {
    kind: KeyKind,
    name: String,
    message: PhantomData<fn() -> M>,
}

impl<M> Key<M>
where
    M: Manifest + 'static,
{
    /// A key under the given name.
    ///
    /// # Panics
    /// Panics in debug builds if another type of this process has claimed the same
    /// [Manifest::MANIFEST] as `M`.
    pub fn new<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        Self::make_key(KeyKind::Discovery, name)
    }

    pub(crate) fn make_topic<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        Self::make_key(KeyKind::Topic, name)
    }

    pub(crate) fn wire(&self) -> WireKey {
        WireKey::new::<M>(self.kind, self.name.clone())
    }

    fn make_key<N>(kind: KeyKind, name: N) -> Self
    where
        N: Into<String>,
    {
        manifest::claim::<M>();
        Self {
            kind,
            name: name.into(),
            message: PhantomData,
        }
    }
}

impl<M> Debug for Key<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Key")
            .field("kind", &self.kind)
            .field("name", &self.name)
            .field("message", &any::type_name::<M>())
            .finish()
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for Key<M> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            name: self.name.clone(),
            message: PhantomData,
        }
    }
}

/// An actor which cannot be registered for discovery.
#[derive(Debug, Error)]
pub enum RegisterError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// The reference names an actor on another node; only actors of this node can be registered
    /// here.
    #[error("reference names an actor on another node")]
    NotLocal,

    /// The key does not fit one frame of `max_frame_size` together with the actor's ID, so it
    /// could never be replicated to the other members; a shorter name is the remedy.
    #[error("registration of {len} bytes exceeds the {max} bytes a frame carries")]
    Oversize {
        /// The bytes the registration takes inside a frame.
        len: usize,

        /// The configured `max_frame_size`.
        max: usize,
    },

    /// This node already holds the most registrations every other member accepts from one
    /// node; a registration ends with its actor.
    #[error("this node already holds {max} registrations")]
    TooMany {
        /// The most registrations one node may hold.
        max: usize,
    },

    /// This node's registrations would take more bytes than every other member accepts from one
    /// node, counting each registration with its key; a registration ends with its actor.
    #[error("this node's registrations would exceed {max} bytes")]
    TooLarge {
        /// The most bytes the registrations of one node may take.
        max: usize,
    },
}

/// Register an actor of this node under a key, so other nodes can
/// [look it up](receptionist::lookup_at) by name and address
/// instead of being handed a serialized reference out of band.
///
/// More than one actor may be registered under one key; a lookup then answers with all of them.
/// The registration is dropped once the actor terminates, along with the reference binding it
/// shares with reference serialization. Registering the same actor again under the same key
/// changes nothing. Registering an actor which has terminated already registers nothing and
/// returns `Ok`.
///
/// # Errors
/// Fails if the endpoint is not started, if the reference names an actor on another node, or if
/// the registration does not fit one frame. Fails as well if this node already holds the most
/// registrations or bytes of registrations one node may.
pub fn register<M>(key: &Key<M>, actor_ref: &ActorRef<M>) -> Result<(), RegisterError>
where
    M: Manifest + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(RegisterError::EndpointNotStarted)?;
    if actor_ref.watcher_registry().is_none() {
        return Err(RegisterError::NotLocal);
    }
    let key = key.wire();
    let len = receptionist::framed_len(&key);
    let max = endpoint.config().max_frame_size.get();
    if len > max {
        return Err(RegisterError::Oversize { len, max });
    }

    endpoint
        .register_own(key, actor_ref)
        .map_err(|exceeded| match exceeded {
            LimitExceeded::Registrations => RegisterError::TooMany {
                max: receptionist::MAX_REGISTRATIONS,
            },

            LimitExceeded::Bytes => RegisterError::TooLarge {
                max: receptionist::MAX_SNAPSHOT_BYTES,
            },
        })
}

/// The manifest is always derived from the type, never passed in, so it cannot swap with the
/// name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WireKey {
    key_name: KeyName,
    manifest: String,
}

impl WireKey {
    pub(crate) fn new<M>(kind: KeyKind, name: impl Into<String>) -> Self
    where
        M: Manifest,
    {
        Self {
            key_name: KeyName {
                kind,
                name: name.into(),
            },
            manifest: M::MANIFEST.to_string(),
        }
    }

    pub(crate) fn key_name(&self) -> &KeyName {
        &self.key_name
    }

    pub(crate) fn manifest(&self) -> &str {
        &self.manifest
    }

    pub(crate) fn into_parts(self) -> (KeyName, String) {
        (self.key_name, self.manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct KeyName {
    kind: KeyKind,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum KeyKind {
    Discovery,
    Topic,
}
