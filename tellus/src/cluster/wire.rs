use crate::{
    ActorId, ActorRef,
    actor_ref::Sink,
    cluster::{
        codec::CodecError,
        endpoint::{self, EndpointInner},
        manifest::{self, Manifest},
        node::NodeId,
        registry::LocalRefError,
        schema::Schema,
        sink::RemoteSink,
    },
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser};
use thiserror::Error;

/// A reference which cannot be serialized or resolved out of band.
#[derive(Debug, Error)]
pub enum RefError {
    /// The remoting endpoint has not been started, see
    /// [start_endpoint](crate::cluster::start_endpoint).
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    /// The reference names another message type than the one it is resolved as, compared by
    /// [Manifest].
    #[error("reference of another message type")]
    TypeMismatch,

    /// The reference names an actor of this node which is not bound anymore, i.e. it has
    /// terminated.
    #[error("no actor bound under this reference")]
    Unbound,

    /// The reference bytes cannot be encoded or decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
}

impl From<LocalRefError> for RefError {
    fn from(error: LocalRefError) -> Self {
        match error {
            LocalRefError::Unbound => RefError::Unbound,
            LocalRefError::MessageType => RefError::TypeMismatch,
        }
    }
}

/// Serialize a reference for out of band exchange, e.g. via configuration or command line, using
/// the endpoint's codec. This is the bootstrap mechanism: any further reference travels inside a
/// message told to an already resolved one. The bytes carry the message type's [Manifest] next to
/// the reference, so [deserialize_ref] can refuse a resolution as another type.
///
/// # Errors
/// Fails if the endpoint is not started, or if the reference cannot be encoded.
///
/// # Panics
/// Panics in debug builds if another type of this process has claimed the same
/// [Manifest::MANIFEST] as `M`.
pub fn serialize_ref<M>(actor_ref: &ActorRef<M>) -> Result<Vec<u8>, RefError>
where
    M: Manifest + Send + 'static,
{
    manifest::claim::<M>();
    let endpoint = endpoint::get().ok_or(RefError::EndpointNotStarted)?;
    let bytes = endpoint.codec().encode(&(M::MANIFEST, actor_ref))?;
    Ok(bytes)
}

/// Resolve a reference serialized by [serialize_ref] on another node, using the endpoint's codec.
/// A reference serialized for another message type is refused rather than resolved into one which
/// drops every message told to it.
///
/// # Errors
/// Fails if the endpoint is not started, if the bytes cannot be decoded, if they name an actor of
/// this node which has terminated, or if they were serialized for another message type.
///
/// # Panics
/// Panics in debug builds if another type of this process has claimed the same
/// [Manifest::MANIFEST] as `M`.
pub fn deserialize_ref<M>(bytes: &[u8]) -> Result<ActorRef<M>, RefError>
where
    M: Manifest + Send + 'static,
{
    manifest::claim::<M>();
    let endpoint = endpoint::get().ok_or(RefError::EndpointNotStarted)?;

    let (manifest, WireRef { node, id }) = endpoint.codec().decode::<(String, WireRef)>(bytes)?;

    if manifest != M::MANIFEST {
        return Err(RefError::TypeMismatch);
    }

    let actor_ref = resolve(endpoint, node, id)?;
    Ok(actor_ref)
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<M> Serialize for ActorRef<M>
where
    M: Schema + Send + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(endpoint) = endpoint::get() else {
            return Err(ser::Error::custom(RefError::EndpointNotStarted));
        };

        let node = match self.sink() {
            Sink::Remote(remote_sink) => remote_sink.node(),

            Sink::Local(_) => {
                endpoint.registry().bind(self);
                endpoint.node()
            }
        };

        WireRef {
            node,
            id: self.actor_id(),
        }
        .serialize(serializer)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
impl<'de, M> Deserialize<'de> for ActorRef<M>
where
    M: Schema + Send + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let WireRef { node, id } = WireRef::deserialize(deserializer)?;

        let Some(endpoint) = endpoint::get() else {
            return Err(de::Error::custom(RefError::EndpointNotStarted));
        };

        resolve(endpoint, node, id).map_err(|error| de::Error::custom(RefError::from(error)))
    }
}

/// A reference to a local actor resolves to the local one, so a round trip comes home unchanged.
pub(crate) fn resolve<M>(
    endpoint: &EndpointInner,
    node: NodeId,
    id: ActorId,
) -> Result<ActorRef<M>, LocalRefError>
where
    M: Schema + Send + 'static,
{
    if node == endpoint.node() {
        endpoint.registry().local_ref::<M>(id)
    } else {
        Ok(ActorRef::remote(id, RemoteSink::new(node)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireRef {
    node: NodeId,
    id: ActorId,
}

#[cfg(test)]
mod tests {
    use crate::cluster::{codec::Codec, wire::WireRef};

    /// The bytes the trait object codec this one replaced encoded a manifest and a [WireRef]
    /// into.
    const WIRE_REF_PAYLOAD: &[u8] = &[
        19, 116, 101, 108, 108, 117, 115, 46, 116, 101, 115, 116, 46, 82, 101, 113, 117, 101, 115,
        116, 0, 127, 0, 0, 1, 248, 19, 16, 1, 161, 219, 7, 101, 218, 119, 87, 150, 37, 188, 240,
        109, 92, 4, 57, 16, 1, 161, 219, 7, 101, 218, 119, 87, 150, 37, 188, 241, 177, 116, 192,
        75,
    ];

    /// An actor reference serialized by the trait object codec decodes here, and encodes back
    /// into the same bytes.
    #[test]
    fn a_wire_ref_matches_the_trait_object_codec() {
        let (manifest, wire_ref) = Codec::Postcard
            .decode::<(String, WireRef)>(WIRE_REF_PAYLOAD)
            .expect("decodes");

        assert_eq!(manifest, "tellus.test.Request");
        assert_eq!(
            wire_ref.node.addr(),
            "127.0.0.1:2552".parse().expect("an address")
        );
        assert_eq!(
            Codec::Postcard
                .encode(&(manifest, wire_ref))
                .expect("encodes"),
            WIRE_REF_PAYLOAD
        );
    }
}
