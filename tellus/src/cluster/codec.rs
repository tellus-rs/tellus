//! Encoding and decoding of message payloads on the wire: [Codec] is what
//! [EndpointConfig::codec](crate::cluster::EndpointConfig::codec) takes, the default is
//! [Postcard]. Every node of a cluster uses the same codec: its [name](Codec::name) is carried in
//! every handshake, and a peer naming another one is refused.

use erased_serde::{Deserializer, Serialize};
use postcard::Deserializer as PostcardDeserializer;
use serde::de::DeserializeOwned;
use std::error::Error;
#[cfg(feature = "serde")]
use std::sync::Arc;
use thiserror::Error as ThisError;

/// The receiving side of [Codec::decode], applying a payload's deserializer to the concrete
/// message type.
///
/// The deserializer's object lifetime is independent of the payload lifetime `'de`, so a codec can
/// reclaim its concrete deserializer after the call, e.g. to check for leftover payload bytes.
pub type DecodeVisitor<'a> = &'a mut dyn for<'de, 'b> FnMut(
    &'b mut (dyn Deserializer<'de> + 'b),
) -> Result<(), erased_serde::Error>;

/// Encodes and decodes message payloads via serde; the default is [Postcard].
pub trait Codec
where
    Self: Send + Sync + 'static,
{
    /// The name of this codec's format, carried in every handshake: a peer naming another codec
    /// is refused, since its payloads would not decode here. A provided codec is named as its
    /// configuration key, e.g. `"postcard"`.
    fn name(&self) -> &'static str;

    /// Encode a message into payload bytes.
    ///
    /// # Errors
    /// Implementations fail if the message cannot be serialized in this codec's format.
    fn encode(&self, message: &dyn Serialize) -> Result<Vec<u8>, CodecError>;

    /// Decode one message from the payload by applying `visit` to a deserializer over it: the
    /// deserializer borrows state living only for this call, hence it cannot be returned.
    ///
    /// # Errors
    /// Implementations fail if the payload is not one message in this codec's format, or if
    /// `visit` fails.
    fn decode(&self, payload: &[u8], visit: DecodeVisitor<'_>) -> Result<(), CodecError>;
}

impl dyn Codec {
    /// Decode one message of type `T` from the payload: the typed form of [Codec::decode], for a
    /// [Schema::decode](crate::cluster::Schema::decode) override decoding an older shape.
    ///
    /// # Errors
    /// Fails if the payload is not one message of type `T` in this codec's format.
    pub fn decode_to<T>(&self, payload: &[u8]) -> Result<T, CodecError>
    where
        T: DeserializeOwned,
    {
        let mut decoded = None;
        self.decode(payload, &mut |deserializer| {
            decoded = Some(erased_serde::deserialize::<T>(deserializer)?);
            Ok(())
        })?;

        decoded.ok_or_else(|| CodecError::decoding("codec did not decode a value"))
    }
}

/// The postcard wire format, the default [Codec].
#[derive(Debug, Default, Clone, Copy)]
pub struct Postcard;

impl Codec for Postcard {
    fn name(&self) -> &'static str {
        "postcard"
    }

    fn encode(&self, message: &dyn Serialize) -> Result<Vec<u8>, CodecError> {
        postcard::to_stdvec(message).map_err(CodecError::encoding)
    }

    fn decode(&self, payload: &[u8], visit: DecodeVisitor<'_>) -> Result<(), CodecError> {
        let mut deserializer = PostcardDeserializer::from_bytes(payload);

        {
            let mut erased = <dyn Deserializer>::erase(&mut deserializer);
            visit(&mut erased).map_err(CodecError::decoding)?;
        }

        let rest = deserializer.finalize().map_err(CodecError::decoding)?;
        if rest.is_empty() {
            Ok(())
        } else {
            Err(CodecError::decoding("trailing payload bytes"))
        }
    }
}

/// A message payload which cannot be encoded or decoded, naming which of the two failed.
#[derive(Debug, ThisError)]
pub enum CodecError {
    /// A message which cannot be encoded into payload bytes.
    #[error("cannot encode message")]
    Encode(#[source] Box<dyn Error + Send + Sync>),

    /// Payload bytes which cannot be decoded into a message.
    #[error("cannot decode message")]
    Decode(#[source] Box<dyn Error + Send + Sync>),
}

impl CodecError {
    /// Wrap any error as an encoding failure.
    pub fn encoding<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self::Encode(error.into())
    }

    /// Wrap any error as a decoding failure.
    pub fn decoding<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self::Decode(error.into())
    }
}

#[cfg(feature = "serde")]
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CodecConfig {
    Postcard,
}

#[cfg(feature = "serde")]
impl CodecConfig {
    pub(crate) fn codec(self) -> Arc<dyn Codec> {
        match self {
            Self::Postcard => Arc::new(Postcard),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::cluster::codec::CodecConfig;
    use crate::cluster::codec::{Codec, CodecError, Postcard};

    /// `<dyn Codec>::decode_to` is what every inbound payload goes through, and the only place
    /// the visitor's decoded value is captured; a codec which never calls the visitor fails it.
    #[test]
    fn decode_to_yields_the_value_and_propagates_a_rejection() {
        let codec: &dyn Codec = &Postcard;
        let payload = codec.encode(&"hello".to_string()).expect("encodes");

        assert_eq!(
            codec.decode_to::<String>(&payload).expect("decodes"),
            "hello"
        );
        assert!(matches!(
            codec.decode_to::<u64>(&payload),
            Err(CodecError::Decode(_))
        ));
    }

    /// Payload bytes which do not decode into the expected type are reported as a decoding
    /// failure, never as an encoding one.
    #[test]
    fn undecodable_payloads_name_the_direction() {
        let payload = Postcard.encode(&u64::MAX).expect("encodes");

        let error = Postcard
            .decode(&payload, &mut |deserializer| {
                erased_serde::deserialize::<bool>(deserializer)?;
                Ok(())
            })
            .expect_err("bool does not decode from a u64 payload");

        assert!(matches!(error, CodecError::Decode(_)));
    }

    /// The codec is chosen by name in a config file, the same name it announces at the
    /// handshake; the built codec is told apart by its bytes, since it is a trait object.
    #[cfg(feature = "serde")]
    #[test]
    fn a_codec_is_selected_by_name() {
        let config =
            serde_json::from_str::<CodecConfig>(r#""postcard""#).expect("the codec is valid");
        let message = "hello".to_string();

        let codec = config.codec();
        assert_eq!(codec.name(), "postcard");
        assert_eq!(codec.name(), Postcard.name());
        let payload = codec.encode(&message).expect("encodes");
        assert_eq!(payload, Postcard.encode(&message).expect("encodes"));

        assert!(serde_json::from_str::<CodecConfig>(r#""postcrad""#).is_err());
    }

    /// A payload with bytes left after the message is corrupt or from a mismatched type and must
    /// be rejected: postcard deserializes a prefix and would otherwise accept it silently.
    #[test]
    fn trailing_payload_bytes_are_rejected() {
        let mut payload = Postcard.encode(&u64::MAX).expect("encodes");
        payload.push(0);

        let error = Postcard
            .decode(&payload, &mut |deserializer| {
                erased_serde::deserialize::<u64>(deserializer)?;
                Ok(())
            })
            .expect_err("a payload with trailing bytes does not decode");

        assert!(matches!(error, CodecError::Decode(_)));
    }
}
