//! Encoding and decoding of message payloads on the wire: [Codec] is what
//! [EndpointConfig::codec](crate::cluster::EndpointConfig::codec) takes, the default is
//! [Codec::Postcard]. Every node of a cluster uses the same codec: its [name](Codec::name) is
//! carried in every handshake, and a peer naming another one is refused.

use serde::{Serialize, de::DeserializeOwned};
use std::error::Error;
use thiserror::Error as ThisError;

/// The wire format of message payloads, the same on every node of a cluster.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Codec {
    /// The postcard wire format, the default.
    #[default]
    Postcard,
}

impl Codec {
    /// The name of this codec's format, carried in every handshake: a peer naming another codec
    /// is refused, since its payloads would not decode here. It is the codec's configuration key,
    /// e.g. `"postcard"`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Postcard => "postcard",
        }
    }

    /// Encode a message into payload bytes.
    ///
    /// # Errors
    /// Fails if the message cannot be serialized in this codec's format.
    pub fn encode<T>(self, message: &T) -> Result<Vec<u8>, CodecError>
    where
        T: Serialize + ?Sized,
    {
        match self {
            Self::Postcard => postcard::to_stdvec(message).map_err(CodecError::encoding),
        }
    }

    /// Decode one message of type `T` from the payload.
    ///
    /// # Errors
    /// Fails if the payload is not exactly one message of type `T` in this codec's format.
    pub fn decode<T>(self, payload: &[u8]) -> Result<T, CodecError>
    where
        T: DeserializeOwned,
    {
        match self {
            Self::Postcard => {
                let (message, rest) =
                    postcard::take_from_bytes(payload).map_err(CodecError::decoding)?;
                if rest.is_empty() {
                    Ok(message)
                } else {
                    Err(CodecError::decoding("trailing payload bytes"))
                }
            }
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

#[cfg(test)]
mod tests {
    use crate::cluster::codec::{Codec, CodecError};
    use serde::{Deserialize, Serialize};
    use std::{collections::BTreeMap, net::SocketAddr};
    use uuid::Uuid;

    /// The bytes the trait object codec this one replaced encoded [sample] into.
    const SAMPLE_PAYLOAD: &[u8] = &[
        128, 2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68,
        69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91,
        92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
        112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
        130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147,
        148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165,
        166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183,
        184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201,
        202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
        220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237,
        238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
        7, 103, 114, 195, 188, 195, 159, 101, 1, 172, 2, 0, 13, 0, 0, 0, 0, 0, 0, 248, 191, 2, 206,
        187, 255, 215, 4, 0, 127, 0, 0, 1, 248, 19, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 249, 19, 16, 1, 35, 69, 103, 137, 171, 205, 239, 254, 220, 186, 152, 118, 84, 50, 16, 4,
        0, 1, 1, 2, 7, 5, 115, 101, 118, 101, 110, 3, 1, 1, 1, 255, 255, 255, 255, 15, 2, 1, 97, 1,
        1, 98, 255, 255, 255, 255, 15,
    ];

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Shape {
        Unit,
        Newtype(i32),
        Tuple(u8, String),
        Struct {
            flag: bool,
            nested: Option<Box<Shape>>,
        },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        payload: Vec<u8>,
        text: String,
        some: Option<u16>,
        none: Option<u16>,
        signed: i64,
        float: f64,
        letter: char,
        unit: (),
        pair: (u8, i16),
        v4: SocketAddr,
        v6: SocketAddr,
        id: Uuid,
        shapes: Vec<Shape>,
        counts: BTreeMap<String, u32>,
    }

    fn sample() -> Sample {
        Sample {
            payload: (0..=255).collect(),
            text: "grüße".to_string(),
            some: Some(300),
            none: None,
            signed: -7,
            float: -1.5,
            letter: 'λ',
            unit: (),
            pair: (255, -300),
            v4: "127.0.0.1:2552".parse().expect("an address"),
            v6: "[::1]:2553".parse().expect("an address"),
            id: Uuid::from_u128(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210),
            shapes: vec![
                Shape::Unit,
                Shape::Newtype(-1),
                Shape::Tuple(7, "seven".to_string()),
                Shape::Struct {
                    flag: true,
                    nested: Some(Box::new(Shape::Newtype(i32::MIN))),
                },
            ],
            counts: BTreeMap::from([("a".to_string(), 1), ("b".to_string(), u32::MAX)]),
        }
    }

    /// A typed decode yields the value, and a payload of another type is a decoding failure.
    #[test]
    fn decode_yields_the_value_and_rejects_another_type() {
        let payload = Codec::Postcard.encode("hello").expect("encodes");

        assert_eq!(
            Codec::Postcard.decode::<String>(&payload).expect("decodes"),
            "hello"
        );
        assert!(matches!(
            Codec::Postcard.decode::<u64>(&payload),
            Err(CodecError::Decode(_))
        ));
    }

    /// This codec and the trait object codec it replaced exchange the same postcard bytes, in
    /// both directions, across the serde shapes a message can take.
    #[test]
    fn payloads_match_the_trait_object_codec() {
        assert_eq!(
            Codec::Postcard.encode(&sample()).expect("encodes"),
            SAMPLE_PAYLOAD
        );
        assert_eq!(
            Codec::Postcard
                .decode::<Sample>(SAMPLE_PAYLOAD)
                .expect("decodes"),
            sample()
        );
    }

    /// Payload bytes which do not decode into the expected type are reported as a decoding
    /// failure, never as an encoding one.
    #[test]
    fn undecodable_payloads_name_the_direction() {
        let payload = Codec::Postcard.encode(&u64::MAX).expect("encodes");

        let error = Codec::Postcard
            .decode::<bool>(&payload)
            .expect_err("bool does not decode from a u64 payload");

        assert!(matches!(error, CodecError::Decode(_)));
    }

    /// The codec is chosen by name in a config file, the same name it announces at the
    /// handshake.
    #[cfg(feature = "serde")]
    #[test]
    fn a_codec_is_selected_by_name() {
        let codec = serde_json::from_str::<Codec>(r#""postcard""#).expect("the codec is valid");
        assert_eq!(codec, Codec::Postcard);
        assert_eq!(codec.name(), "postcard");

        assert!(serde_json::from_str::<Codec>(r#""postcrad""#).is_err());
    }

    /// A payload with bytes left after the message is corrupt or from a mismatched type and must
    /// be rejected: postcard deserializes a prefix and would otherwise accept it silently.
    #[test]
    fn trailing_payload_bytes_are_rejected() {
        let mut payload = Codec::Postcard.encode(&u64::MAX).expect("encodes");
        payload.push(0);

        let error = Codec::Postcard
            .decode::<u64>(&payload)
            .expect_err("a payload with trailing bytes does not decode");

        assert!(matches!(error, CodecError::Decode(_)));
    }
}
