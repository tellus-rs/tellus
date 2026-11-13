//! Schema evolution of message payloads: [Schema] is what every type crossing nodes implements,
//! versioning its payloads and choosing their shape per [Peer], see docs/cluster.md.

use crate::{
    ActorRef, ReplyTo, SchemaVersion,
    actor_context::PanicPayload,
    cluster::{
        codec::{Codec, CodecError},
        node::{Incarnation, NodeId},
    },
};
use semver::Version;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    any::type_name,
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
};
use thiserror::Error;
use tracing::error;

/// The version of the application a node runs, announced at its handshake: a semantic version,
/// ordered by semver precedence.
pub type AppVersion = Version;

/// A type whose values cross nodes as message payloads, versioned for schema evolution. Every
/// payload travels with the [schema version](Schema::VERSION) of the shape it was encoded in, so a
/// receiver decodes it with that shape or refuses it, never with another one. An older shape is
/// decoded by overriding [decode](Schema::decode), and encoded for a peer which still runs the
/// older application by overriding [encode](Schema::encode), which is what a rolling upgrade
/// needs.
///
/// tellus implements `Schema` for the types which have one shape forever: `bool`, `char`, the
/// integer and float primitives, `String`, `()`, and `Option` and `Vec` of each of those, plus
/// [ActorRef] and [ReplyTo]. A container of an application type, like a `Vec` of one, is not a
/// `Schema`: its elements' `encode` and `decode` would never run. Wrap it in a type of the
/// application, which is what makes it evolvable. A field of an application type inside a message
/// is the same case, so a change inside it bumps the enclosing type's `VERSION`, and `Result` is
/// no `Schema` either: a reply of `Result<T, E>` crosses nodes wrapped in a type of the
/// application.
pub trait Schema
where
    Self: Serialize + DeserializeOwned,
{
    /// The schema version of the shape this code encodes, sent alongside every payload: 0 unless
    /// overridden. It is bumped with every change to the serialized shape.
    const VERSION: SchemaVersion = SchemaVersion::new(0);

    /// Encode a value for the given peer, answering the payload and the schema version of its
    /// shape. The default encodes the current shape as [VERSION](Schema::VERSION); an override
    /// encodes an older shape for a peer whose [application version](Peer::app_version) predates
    /// the current one, typically through a view borrowing the fields of the old shape.
    ///
    /// # Errors
    /// Implementations fail if the value cannot be encoded in the codec's format, or in the shape
    /// the peer decodes.
    fn encode(
        &self,
        codec: &dyn Codec,
        peer: &Peer,
    ) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
        let _ = peer;
        let payload = codec.encode(self)?;
        Ok((Self::VERSION, payload))
    }

    /// Decode a payload sent with the given schema version. The default decodes
    /// [VERSION](Schema::VERSION) only; an override upcasts older versions, typically by decoding
    /// the old shape into its own type with the codec's `decode_to` and converting.
    ///
    /// # Errors
    /// Implementations fail if `schema_version` is one they cannot decode, with
    /// [UnsupportedSchemaVersion] as the source, or if the payload does not decode as that
    /// version.
    fn decode(
        codec: &dyn Codec,
        schema_version: SchemaVersion,
        payload: &[u8],
    ) -> Result<Self, CodecError> {
        if schema_version == Self::VERSION {
            codec.decode_to(payload)
        } else {
            Err(CodecError::decoding(UnsupportedSchemaVersion::new::<Self>(
                schema_version,
            )))
        }
    }
}

macro_rules! atoms {
    ($($atom:ty),* $(,)?) => {
        $(
            impl Schema for $atom {}

            impl Schema for Option<$atom> {}

            impl Schema for Vec<$atom> {}
        )*
    };
}

atoms!(
    bool,
    char,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    String,
    ()
);

impl<M> Schema for ActorRef<M> where M: Schema + Send + 'static {}

impl<R> Schema for ReplyTo<R> where R: Schema + Send + 'static {}

/// The node a payload is encoded for: its identity and, once a handshake with it has completed,
/// the application version it announced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    node: NodeId,
    app_version: Option<AppVersion>,
}

impl Peer {
    pub(crate) fn new(node: NodeId, app_version: Option<AppVersion>) -> Self {
        Self { node, app_version }
    }

    /// The address the node advertises.
    pub fn addr(&self) -> SocketAddr {
        self.node.addr()
    }

    /// The incarnation of the node at its address.
    pub fn incarnation(&self) -> Incarnation {
        self.node.incarnation()
    }

    /// The application version the node announced at its handshake, from the `app_version` of
    /// its endpoint configuration. `None` if it announced none, or if no handshake with it has
    /// completed yet, which a message enqueued before the first connection sees; an
    /// [encode](Schema::encode) override treats `None` as the oldest version it still encodes, so
    /// such a message is delivered in that shape.
    pub fn app_version(&self) -> Option<&AppVersion> {
        self.app_version.as_ref()
    }
}

/// A payload sent with a schema version the receiving type does not decode: the source of the
/// [CodecError::Decode] it is dead-lettered with.
#[derive(Debug, Error)]
#[error(
    "unsupported schema version {schema_version} of {type_name}, the current version is {current}"
)]
pub struct UnsupportedSchemaVersion {
    type_name: &'static str,
    schema_version: SchemaVersion,
    current: SchemaVersion,
}

impl UnsupportedSchemaVersion {
    /// The error for a payload of `T` sent with `schema_version`, for a [Schema::decode]
    /// override which does not decode that version.
    pub fn new<T>(schema_version: SchemaVersion) -> Self
    where
        T: Schema,
    {
        Self {
            type_name: type_name::<T>(),
            schema_version,
            current: T::VERSION,
        }
    }
}

pub(crate) fn catch_panic_and_log<T, U, F>(
    failure: &'static str,
    into_error: fn(&'static str) -> CodecError,
    f: F,
) -> Result<U, CodecError>
where
    T: Schema,
    F: FnOnce() -> Result<U, CodecError>,
{
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|panic| {
        error!(
            payload_type = type_name::<T>(),
            panic = %PanicPayload(panic.as_ref()),
            "{failure}"
        );
        Err(into_error(failure))
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        SchemaVersion,
        cluster::{
            codec::{Codec, CodecError, Postcard},
            node::NodeId,
            schema::{Peer, Schema, catch_panic_and_log},
        },
    };
    use serde::{Deserialize, Serialize};
    use std::error::Error;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Note {
        text: String,
        stars: u8,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Notes(Vec<Note>);

    impl Schema for Notes {
        const VERSION: SchemaVersion = SchemaVersion::new(1);

        fn encode(
            &self,
            codec: &dyn Codec,
            peer: &Peer,
        ) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
            if peer.app_version().is_none_or(|version| version.major < 2) {
                let texts = self.0.iter().map(|note| &note.text).collect::<Vec<_>>();
                return Ok((SchemaVersion::new(0), codec.encode(&texts)?));
            }
            Ok((Self::VERSION, codec.encode(self)?))
        }

        fn decode(
            codec: &dyn Codec,
            schema_version: SchemaVersion,
            payload: &[u8],
        ) -> Result<Self, CodecError> {
            if schema_version == SchemaVersion::new(0) {
                let texts = codec.decode_to::<Vec<String>>(payload)?;
                let notes = texts
                    .into_iter()
                    .map(|text| Note { text, stars: 0 })
                    .collect();
                return Ok(Notes(notes));
            }
            if schema_version == Self::VERSION {
                return codec.decode_to(payload);
            }
            Err(CodecError::decoding(
                super::UnsupportedSchemaVersion::new::<Self>(schema_version),
            ))
        }
    }

    const CODEC: &dyn Codec = &Postcard;

    fn peer(app_version: Option<&str>) -> Peer {
        let node = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));
        Peer::new(
            node,
            app_version.map(|version| version.parse().expect("a semantic version")),
        )
    }

    /// The provided types carry version 0 and encode as the codec encodes them; their payloads
    /// decode back as that version only.
    #[test]
    fn provided_types_are_version_zero() {
        assert_eq!(<Vec<u64> as Schema>::VERSION, SchemaVersion::new(0));
        assert_eq!(<Option<f64> as Schema>::VERSION, SchemaVersion::new(0));

        let value = Some(2.5_f64);
        let (schema_version, payload) = value.encode(CODEC, &peer(None)).expect("encodes");
        assert_eq!(schema_version, SchemaVersion::new(0));
        assert_eq!(payload, Postcard.encode(&value).expect("encodes"));
        assert_eq!(
            <Option<f64>>::decode(CODEC, SchemaVersion::new(0), &payload).expect("decodes"),
            value
        );
        assert!(matches!(
            <Option<f64>>::decode(CODEC, SchemaVersion::new(1), &payload),
            Err(CodecError::Decode(_))
        ));
    }

    /// A wrapper around a container of an application type evolves: the current shape round
    /// trips, an older peer gets the old shape, and the old shape is upcast on decode.
    #[test]
    fn a_wrapper_encodes_per_peer_and_upcasts() {
        let notes = Notes(vec![Note {
            text: "hello".to_string(),
            stars: 3,
        }]);

        let (schema_version, payload) = notes.encode(CODEC, &peer(Some("2.1.0"))).expect("encodes");
        assert_eq!(schema_version, SchemaVersion::new(1));
        assert_eq!(
            Notes::decode(CODEC, schema_version, &payload).expect("decodes"),
            notes
        );

        for older in [Some("1.9.0"), None] {
            let (schema_version, payload) = notes.encode(CODEC, &peer(older)).expect("encodes");
            assert_eq!(schema_version, SchemaVersion::new(0));
            assert_eq!(
                CODEC
                    .decode_to::<Vec<String>>(&payload)
                    .expect("the old shape"),
                ["hello"]
            );
            assert_eq!(
                Notes::decode(CODEC, schema_version, &payload).expect("upcasts"),
                Notes(vec![Note {
                    text: "hello".to_string(),
                    stars: 0,
                }])
            );
        }
    }

    /// An unsupported version is a decoding failure whose source names the type and both
    /// versions, which is what a dead letter logs.
    #[test]
    fn an_unsupported_version_names_the_type_and_both_versions() {
        let error = Notes::decode(CODEC, SchemaVersion::new(5), &[])
            .expect_err("version 5 is not decodable");

        assert!(matches!(error, CodecError::Decode(_)));
        let source = error
            .source()
            .expect("the version is the source")
            .to_string();
        assert!(
            source.contains("unsupported schema version 5 of ")
                && source.contains("Notes")
                && source.ends_with("the current version is 1"),
            "{source}"
        );
    }

    /// A panic is answered as the failure the caller names, an encoding or a decoding one as the
    /// caller chooses.
    #[test]
    fn a_panic_is_answered_as_the_given_failure() {
        let error =
            catch_panic_and_log::<Notes, Notes, _>("decode panicked", CodecError::decoding, || {
                panic!("a bug in a decode")
            })
            .expect_err("a panic is a failure");
        assert!(matches!(error, CodecError::Decode(_)));
        assert_eq!(
            error.source().expect("the failure").to_string(),
            "decode panicked"
        );

        let error = catch_panic_and_log::<Notes, (SchemaVersion, Vec<u8>), _>(
            "encode panicked",
            CodecError::encoding,
            || panic!("a bug in an encode"),
        )
        .expect_err("a panic is a failure");
        assert!(matches!(error, CodecError::Encode(_)));
        assert_eq!(
            error.source().expect("the failure").to_string(),
            "encode panicked"
        );
    }

    /// Without a panic the result comes back as it is, an error with its own source.
    #[test]
    fn a_result_without_a_panic_comes_back_unchanged() {
        let payload = Postcard.encode(&7_u64).expect("encodes");

        let decoded =
            catch_panic_and_log::<u64, _, _>("decode panicked", CodecError::decoding, || {
                u64::decode(CODEC, SchemaVersion::new(0), &payload)
            });
        assert_eq!(decoded.expect("decodes"), 7);

        let error =
            catch_panic_and_log::<u64, _, _>("decode panicked", CodecError::decoding, || {
                u64::decode(CODEC, SchemaVersion::new(1), &payload)
            })
            .expect_err("version 1 is not decodable");
        assert!(
            error
                .source()
                .expect("the version is the source")
                .to_string()
                .starts_with("unsupported schema version 1 of "),
            "{error:?}"
        );
    }
}
