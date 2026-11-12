use crate::{
    persistence::codec::{Codec, PayloadError},
    schema_version::SchemaVersion,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// A value stored durably, an event or a snapshot, named and versioned for schema evolution. Stored
/// payloads outlive the code which wrote them, so every payload carries a stable name and a schema
/// version outside of itself. Old versions can be upcast on read by overriding
/// [decode](Versioned::decode); the store is never rewritten. A version which is neither current
/// nor upcast is rejected on read.
pub trait Versioned
where
    Self: Serialize + DeserializeOwned,
{
    /// The stable name stored alongside every payload, independent of the Rust type: it must
    /// never change once payloads are stored, in particular not with a type rename or move. It must
    /// not hold a NUL.
    const MANIFEST: &'static str;

    /// The current schema version, stored alongside every payload written by the current code.
    const VERSION: SchemaVersion;

    /// Decode a stored payload with the given schema version. The default implementation decodes
    /// [VERSION](Self::VERSION) only; override it to upcast older versions, typically by decoding
    /// the old shape into its own type and converting.
    ///
    /// # Errors
    /// Implementations fail if `schema_version` is one they cannot decode, or if the payload does
    /// not decode as that version.
    fn decode<C>(
        codec: &C,
        schema_version: SchemaVersion,
        payload: &[u8],
    ) -> Result<Self, DecodeError>
    where
        C: Codec,
    {
        if schema_version == Self::VERSION {
            Ok(codec.decode(payload)?)
        } else {
            Err(DecodeError::UnsupportedSchemaVersion {
                manifest: Self::MANIFEST,
                schema_version,
            })
        }
    }
}

/// Errors of decoding a stored event or snapshot: the stored manifest names another type, or the
/// schema version or the payload is not decodable. The manifest is checked before
/// [Versioned::decode] is called.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The payload itself is undecodable; wraps the codec's failure.
    #[error(transparent)]
    Payload(#[from] PayloadError),

    /// The stored manifest names another type than the one being decoded.
    #[error("stored manifest {stored}, expected {expected}")]
    ManifestMismatch {
        /// The manifest read from the store.
        stored: String,

        /// The manifest of the type being decoded.
        expected: &'static str,
    },

    /// The stored schema version is not decodable by the current code; see [Versioned::decode]
    /// for upcasting.
    #[error("unsupported schema version {schema_version} for manifest {manifest}")]
    UnsupportedSchemaVersion {
        /// The manifest of the type being decoded.
        manifest: &'static str,

        /// The schema version read from the store.
        schema_version: SchemaVersion,
    },
}

pub(crate) fn decode_versioned<T, C>(
    codec: &C,
    manifest: &str,
    schema_version: SchemaVersion,
    payload: &[u8],
) -> Result<T, DecodeError>
where
    T: Versioned,
    C: Codec,
{
    if manifest != T::MANIFEST {
        return Err(DecodeError::ManifestMismatch {
            stored: manifest.to_string(),
            expected: T::MANIFEST,
        });
    }

    T::decode(codec, schema_version, payload)
}

#[cfg(test)]
mod tests {
    use crate::{
        Nothing,
        persistence::{
            codec::{Cbor, Codec, Json},
            versioned::{DecodeError, Versioned, decode_versioned},
        },
        schema_version::SchemaVersion,
    };
    use serde::{Deserialize, Serialize};
    use std::error::Error;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Event(u64);

    impl Versioned for Event {
        const MANIFEST: &'static str = "event";
        const VERSION: SchemaVersion = SchemaVersion::new(2);
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Upcast(u64);

    #[derive(Debug, Serialize, Deserialize)]
    struct UpcastV1(u32);

    impl Versioned for Upcast {
        const MANIFEST: &'static str = "upcast";
        const VERSION: SchemaVersion = SchemaVersion::new(2);

        fn decode<C>(
            codec: &C,
            schema_version: SchemaVersion,
            payload: &[u8],
        ) -> Result<Self, DecodeError>
        where
            C: Codec,
        {
            match schema_version.as_u16() {
                1 => Ok(Self(u64::from(codec.decode::<UpcastV1>(payload)?.0))),

                2 => Ok(codec.decode(payload)?),

                _ => Err(DecodeError::UnsupportedSchemaVersion {
                    manifest: Self::MANIFEST,
                    schema_version,
                }),
            }
        }
    }

    /// The default [Versioned::decode] decodes the current version and nothing else.
    #[test]
    fn the_current_version_decodes() {
        let payload = Cbor.encode(&Event(7)).expect("the event is encodable");

        assert_eq!(
            Event::decode(&Cbor, Event::VERSION, &payload).expect("the payload is decodable"),
            Event(7)
        );
    }

    /// The central schema evolution rule: a version the code does not know is rejected, never
    /// decoded with the current shape. Widening the version check would break this and nothing
    /// else in the crate would notice.
    #[test]
    fn an_unknown_version_is_rejected() {
        let payload = Cbor.encode(&Event(7)).expect("the event is encodable");

        for schema_version in [SchemaVersion::new(1), SchemaVersion::new(3)] {
            match Event::decode(&Cbor, schema_version, &payload) {
                Err(DecodeError::UnsupportedSchemaVersion {
                    manifest,
                    schema_version: rejected,
                }) => {
                    assert_eq!(manifest, "event");
                    assert_eq!(rejected, schema_version);
                }

                other => panic!("expected an unsupported schema version, got {other:?}"),
            }
        }
    }

    /// An undecodable payload surfaces as the codec's own failure, lifted into the wider error by
    /// `?` rather than by a hand-written conversion.
    #[test]
    fn an_undecodable_payload_fails_as_payload() {
        let error = Event::decode(&Cbor, Event::VERSION, &[])
            .expect_err("an empty payload must not decode");

        assert!(matches!(error, DecodeError::Payload(_)));
        assert_eq!(
            error.to_string(),
            "payload not decodable",
            "the transparent variant must render the payload error's own message"
        );
        assert!(
            error.source().is_some(),
            "the codec error must stay reachable through the transparent variant"
        );
    }

    /// Overriding [Versioned::decode] upcasts an older version on read, which is the documented
    /// alternative to rewriting the store.
    #[test]
    fn an_override_upcasts_an_older_version() {
        let payload = Cbor
            .encode(&UpcastV1(7))
            .expect("the old shape is encodable");

        assert_eq!(
            Upcast::decode(&Cbor, SchemaVersion::new(1), &payload).expect("version 1 must upcast"),
            Upcast(7)
        );

        let payload = Cbor
            .encode(&Upcast(7))
            .expect("the current shape is encodable");

        assert_eq!(
            Upcast::decode(&Cbor, SchemaVersion::new(2), &payload).expect("version 2 must decode"),
            Upcast(7)
        );
    }

    /// The manifest is checked before the payload is ever handed to the codec, so a payload
    /// stored under another type is rejected instead of being coerced into this one.
    #[test]
    fn a_foreign_manifest_is_rejected() {
        let payload = Cbor.encode(&Event(7)).expect("the event is encodable");

        match decode_versioned::<Event, _>(&Cbor, "other", Event::VERSION, &payload) {
            Err(DecodeError::ManifestMismatch { stored, expected }) => {
                assert_eq!(stored, "other");
                assert_eq!(expected, "event");
            }

            other => panic!("expected a manifest mismatch, got {other:?}"),
        }
    }

    /// The matching manifest passes through to [Versioned::decode].
    #[test]
    fn the_matching_manifest_decodes() {
        let payload = Cbor.encode(&Event(7)).expect("the event is encodable");

        assert_eq!(
            decode_versioned::<Event, _>(&Cbor, "event", Event::VERSION, &payload)
                .expect("the payload is decodable"),
            Event(7)
        );
    }

    /// [Nothing] is uninhabited, so its [Deserialize] impl must fail for every input rather than
    /// conjure a value; the actors without events or snapshots rely on it.
    #[test]
    fn nothing_never_deserializes() {
        assert!(Json.decode::<Nothing>(b"null").is_err());
        assert!(Cbor.decode::<Nothing>(&[0xF6]).is_err());
    }
}
