use serde::{
    Serialize,
    de::DeserializeOwned,
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant, Serializer,
    },
};
use std::{error::Error, fmt::Display};
use thiserror::Error;

const CBOR_SCRATCH_LEN: usize = 4096;

/// Encodes events and snapshots into the payload bytes handed to the stores and decodes them
/// back. A codec must be self-describing enough that its output can be decoded without external
/// schema knowledge. No stored record names the codec which wrote its payload, so a codec cannot
/// be changed once events have been written.
pub trait Codec {
    /// Encode the given value into payload bytes.
    ///
    /// # Errors
    /// Implementations fail if the value cannot be encoded in this codec's format.
    fn encode<T>(&self, value: &T) -> Result<Vec<u8>, EncodeError>
    where
        T: Serialize;

    /// Decode a value from the given payload bytes.
    ///
    /// # Errors
    /// Implementations fail if the payload is not a value of the requested type in this codec's
    /// format.
    fn decode<T>(&self, payload: &[u8]) -> Result<T, PayloadError>
    where
        T: DeserializeOwned;
}

/// The default codec: CBOR, self-describing and compact.
#[derive(Debug, Default, Clone, Copy)]
pub struct Cbor;

impl Codec for Cbor {
    fn encode<T>(&self, value: &T) -> Result<Vec<u8>, EncodeError>
    where
        T: Serialize,
    {
        let mut payload = Vec::new();
        ciborium::into_writer(value, &mut payload).map_err(EncodeError::new)?;
        Ok(payload)
    }

    fn decode<T>(&self, payload: &[u8]) -> Result<T, PayloadError>
    where
        T: DeserializeOwned,
    {
        // Ciborium visits a text or byte string as a slice only if it fits the scratch buffer!
        let mut short_scratch;
        let mut long_scratch;
        let scratch = if payload.len() <= CBOR_SCRATCH_LEN {
            short_scratch = [0; CBOR_SCRATCH_LEN];
            short_scratch.as_mut_slice()
        } else {
            long_scratch = vec![0; payload.len()];
            long_scratch.as_mut_slice()
        };
        ciborium::from_reader_with_buffer(payload, scratch).map_err(PayloadError::new)
    }
}

/// An alternative codec: JSON, directly readable in the stores at the cost of size and speed. JSON
/// cannot represent a NaN or an infinity, so encoding a value which holds one fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct Json;

impl Codec for Json {
    fn encode<T>(&self, value: &T) -> Result<Vec<u8>, EncodeError>
    where
        T: Serialize,
    {
        // serde_json writes a non-finite float as `null`, which no float decodes from!
        serde_json::to_vec(&Finite(value)).map_err(EncodeError::new)
    }

    fn decode<T>(&self, payload: &[u8]) -> Result<T, PayloadError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(payload).map_err(PayloadError::new)
    }
}

/// The failure of [Codec::encode]; the codec's underlying error is its [source](Error::source).
#[derive(Debug, Error)]
#[error("value not encodable")]
pub struct EncodeError(#[source] Box<dyn Error + Send + Sync>);

impl EncodeError {
    /// Wrap the codec's own error as the [source](Error::source).
    pub fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self(error.into())
    }
}

/// The failure of [Codec::decode]; the codec's underlying error is its [source](Error::source).
/// An undecodable payload is the only failure a codec itself can produce. The manifest and
/// schema version checks around it fail with [DecodeError](crate::DecodeError) instead.
#[derive(Debug, Error)]
#[error("payload not decodable")]
pub struct PayloadError(#[source] Box<dyn Error + Send + Sync>);

impl PayloadError {
    /// Wrap the codec's own error as the [source](Error::source).
    pub fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self(error.into())
    }
}

macro_rules! delegate {
    ($($method:ident($($argument:ident: $type:ty),*)),* $(,)?) => {
        $(
            fn $method(self, $($argument: $type),*) -> Result<Self::Ok, Self::Error> {
                self.0.$method($($argument),*)
            }
        )*
    };
}

macro_rules! delegate_compound {
    ($($method:ident($($argument:ident: $type:ty),*) -> $compound:ident),* $(,)?) => {
        $(
            fn $method(self, $($argument: $type),*) -> Result<Self::$compound, Self::Error> {
                self.0.$method($($argument),*).map(Finite)
            }
        )*
    };
}

macro_rules! compound_impls {
    ($($compound:ident::$method:ident($($key:ident: $type:ty)?)),* $(,)?) => {
        $(
            impl<C> $compound for Finite<C>
            where
                C: $compound,
            {
                type Ok = C::Ok;
                type Error = C::Error;

                fn $method<T>(&mut self, $($key: $type,)? value: &T) -> Result<(), C::Error>
                where
                    T: Serialize + ?Sized,
                {
                    self.0.$method($($key,)? &Finite(value))
                }

                fn end(self) -> Result<C::Ok, C::Error> {
                    self.0.end()
                }
            }
        )*
    };
}

struct Finite<T>(T);

// Delegating `collect_seq` or `collect_map` would hand the elements over unchecked!
impl<S> Serializer for Finite<S>
where
    S: Serializer,
{
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Finite<S::SerializeSeq>;
    type SerializeTuple = Finite<S::SerializeTuple>;
    type SerializeTupleStruct = Finite<S::SerializeTupleStruct>;
    type SerializeTupleVariant = Finite<S::SerializeTupleVariant>;
    type SerializeMap = Finite<S::SerializeMap>;
    type SerializeStruct = Finite<S::SerializeStruct>;
    type SerializeStructVariant = Finite<S::SerializeStructVariant>;

    delegate! {
        serialize_bool(value: bool),
        serialize_i8(value: i8),
        serialize_i16(value: i16),
        serialize_i32(value: i32),
        serialize_i64(value: i64),
        serialize_i128(value: i128),
        serialize_u8(value: u8),
        serialize_u16(value: u16),
        serialize_u32(value: u32),
        serialize_u64(value: u64),
        serialize_u128(value: u128),
        serialize_char(value: char),
        serialize_str(value: &str),
        serialize_bytes(value: &[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(name: &'static str),
        serialize_unit_variant(name: &'static str, variant_index: u32, variant: &'static str),
    }

    delegate_compound! {
        serialize_seq(len: Option<usize>) -> SerializeSeq,
        serialize_tuple(len: usize) -> SerializeTuple,
        serialize_tuple_struct(name: &'static str, len: usize) -> SerializeTupleStruct,
        serialize_tuple_variant(
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize
        ) -> SerializeTupleVariant,
        serialize_map(len: Option<usize>) -> SerializeMap,
        serialize_struct(name: &'static str, len: usize) -> SerializeStruct,
        serialize_struct_variant(
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize
        ) -> SerializeStructVariant,
    }

    fn serialize_f32(self, value: f32) -> Result<S::Ok, S::Error> {
        if value.is_finite() {
            self.0.serialize_f32(value)
        } else {
            Err(ser::Error::custom(format_args!(
                "JSON cannot represent {value}"
            )))
        }
    }

    fn serialize_f64(self, value: f64) -> Result<S::Ok, S::Error> {
        if value.is_finite() {
            self.0.serialize_f64(value)
        } else {
            Err(ser::Error::custom(format_args!(
                "JSON cannot represent {value}"
            )))
        }
    }

    fn serialize_some<T>(self, value: &T) -> Result<S::Ok, S::Error>
    where
        T: Serialize + ?Sized,
    {
        self.0.serialize_some(&Finite(value))
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<S::Ok, S::Error>
    where
        T: Serialize + ?Sized,
    {
        self.0.serialize_newtype_struct(name, &Finite(value))
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error>
    where
        T: Serialize + ?Sized,
    {
        self.0
            .serialize_newtype_variant(name, variant_index, variant, &Finite(value))
    }

    fn collect_str<T>(self, value: &T) -> Result<S::Ok, S::Error>
    where
        T: Display + ?Sized,
    {
        self.0.collect_str(value)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

impl<T> Serialize for Finite<&T>
where
    T: Serialize + ?Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(Finite(serializer))
    }
}

compound_impls! {
    SerializeSeq::serialize_element(),
    SerializeTuple::serialize_element(),
    SerializeTupleStruct::serialize_field(),
    SerializeTupleVariant::serialize_field(),
    SerializeStruct::serialize_field(key: &'static str),
    SerializeStructVariant::serialize_field(key: &'static str),
}

impl<M> SerializeMap for Finite<M>
where
    M: SerializeMap,
{
    type Ok = M::Ok;
    type Error = M::Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), M::Error>
    where
        T: Serialize + ?Sized,
    {
        self.0.serialize_key(&Finite(key))
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), M::Error>
    where
        T: Serialize + ?Sized,
    {
        self.0.serialize_value(&Finite(value))
    }

    fn end(self) -> Result<M::Ok, M::Error> {
        self.0.end()
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::codec::{CBOR_SCRATCH_LEN, Cbor, Codec, Json};
    use serde::{
        Deserialize, Deserializer, Serialize, Serializer,
        de::{self, Visitor},
    };
    use std::{
        collections::BTreeMap,
        error::Error,
        fmt::{self, Formatter},
    };

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Value {
        name: String,
        count: u64,
    }

    #[derive(Debug, PartialEq, Eq, Serialize)]
    struct Text(String);

    impl<'de> Deserialize<'de> for Text {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(TextVisitor)
        }
    }

    struct TextVisitor;

    impl Visitor<'_> for TextVisitor {
        type Value = Text;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str("a text")
        }

        fn visit_str<E>(self, text: &str) -> Result<Text, E>
        where
            E: de::Error,
        {
            Ok(Text(text.to_owned()))
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Bytes(Vec<u8>);

    impl Serialize for Bytes {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0)
        }
    }

    impl<'de> Deserialize<'de> for Bytes {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_bytes(BytesVisitor)
        }
    }

    struct BytesVisitor;

    impl Visitor<'_> for BytesVisitor {
        type Value = Bytes;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str("a byte string")
        }

        fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Bytes, E>
        where
            E: de::Error,
        {
            Ok(Bytes(bytes.to_owned()))
        }
    }

    #[derive(Serialize)]
    struct Unit;

    #[derive(Serialize)]
    struct Newtype(f64);

    #[derive(Serialize)]
    struct Tuple(u8, f32);

    #[derive(Serialize)]
    struct Struct {
        float: f64,
    }

    #[derive(Serialize)]
    enum Enum {
        Unit,
        Newtype(f64),
        Tuple(u8, f64),
        Struct { float: f64 },
    }

    /// Both shipped codecs are self-describing as the trait promises: a payload decodes back into
    /// its own type with nothing but the bytes, no schema handed alongside.
    #[test]
    fn both_codecs_round_trip() {
        let value = value();

        let payload = Cbor.encode(&value).expect("the value is CBOR encodable");
        assert_eq!(
            Cbor.decode::<Value>(&payload)
                .expect("the payload is CBOR decodable"),
            value
        );

        let payload = Json.encode(&value).expect("the value is JSON encodable");
        assert_eq!(
            Json.decode::<Value>(&payload)
                .expect("the payload is JSON decodable"),
            value
        );
    }

    /// [Json] is documented as directly readable in the stores, which only holds if its output is
    /// text carrying the field names.
    #[test]
    fn json_encodes_readable_text() {
        let payload = Json.encode(&value()).expect("the value is JSON encodable");
        let text = String::from_utf8(payload).expect("JSON output is UTF-8");

        assert!(text.contains("\"name\""), "got {text}");
        assert!(text.contains("\"tellus\""), "got {text}");
    }

    /// Nothing names the codec which wrote a payload, so a payload from the other codec must
    /// fail rather than decode into a wrong value.
    #[test]
    fn a_payload_of_another_codec_does_not_decode() {
        let payload = Json.encode(&value()).expect("the value is JSON encodable");
        assert!(
            Cbor.decode::<Value>(&payload).is_err(),
            "CBOR must not decode a JSON payload"
        );

        let payload = Cbor.encode(&value()).expect("the value is CBOR encodable");
        assert!(
            Json.decode::<Value>(&payload).is_err(),
            "JSON must not decode a CBOR payload"
        );
    }

    /// A truncated payload is a decode failure, not a panic or a default value.
    #[test]
    fn an_empty_payload_does_not_decode() {
        assert!(Cbor.decode::<Value>(&[]).is_err());
        assert!(Json.decode::<Value>(&[]).is_err());
    }

    /// The codec's own error is reachable as the source, not merged into this error's message:
    /// a caller can walk the chain or downcast to the codec's concrete error type.
    #[test]
    fn the_codec_error_is_the_source() {
        let error = Cbor
            .decode::<Value>(&[])
            .expect_err("an empty payload must not decode");

        assert_eq!(error.to_string(), "payload not decodable");
        let source = error.source().expect("the codec error must be the source");
        assert_ne!(
            source.to_string(),
            error.to_string(),
            "the source must carry the codec's own text, not repeat this message"
        );
    }

    /// The default codec reads back a text of any length. Ciborium hands a text to a type which
    /// deserializes through `deserialize_str` only if it fits the scratch buffer. The first two
    /// lengths give the longest payload for the buffer on the stack and the shortest beyond it.
    #[test]
    fn cbor_reads_back_text_of_any_length() {
        for len in [
            CBOR_SCRATCH_LEN - 3,
            CBOR_SCRATCH_LEN - 2,
            CBOR_SCRATCH_LEN + 1,
            100_000,
        ] {
            let text = Text("x".repeat(len));

            let payload = Cbor.encode(&text).expect("the text is CBOR encodable");
            assert_eq!(
                Cbor.decode::<Text>(&payload)
                    .expect("the payload is CBOR decodable"),
                text
            );
        }
    }

    /// The default codec reads back a byte string of any length as well. Ciborium hands one to a
    /// type which deserializes through `deserialize_bytes` only if it fits the scratch buffer. The
    /// lengths are those of `cbor_reads_back_text_of_any_length`, since a byte string has a header
    /// of the same length.
    #[test]
    fn cbor_reads_back_bytes_of_any_length() {
        for len in [
            CBOR_SCRATCH_LEN - 3,
            CBOR_SCRATCH_LEN - 2,
            CBOR_SCRATCH_LEN + 1,
            100_000,
        ] {
            let bytes = Bytes(vec![7; len]);

            let payload = Cbor.encode(&bytes).expect("the bytes are CBOR encodable");
            assert_eq!(
                Cbor.decode::<Bytes>(&payload)
                    .expect("the payload is CBOR decodable"),
                bytes
            );
        }
    }

    /// Replay equals live execution only if a float is read back with the bits it was written with.
    #[test]
    fn json_reads_back_the_float_it_wrote() {
        for float in [
            51.248178375505404,
            962.6169236606465,
            f64::MIN_POSITIVE,
            f64::MAX,
        ] {
            let payload = Json.encode(&float).expect("the float is JSON encodable");
            let decoded = Json
                .decode::<f64>(&payload)
                .expect("the payload is JSON decodable");
            assert_eq!(
                decoded.to_bits(),
                float.to_bits(),
                "{float:?} was read back as {decoded:?}"
            );
        }
    }

    /// JSON cannot represent a NaN or an infinity. Encoding one fails, wherever it sits in the
    /// value, rather than writing a `null` which no float decodes from.
    #[test]
    fn json_refuses_a_non_finite_float() {
        for float in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = Json.encode(&float).expect_err("the float must be refused");
            let source = error
                .source()
                .and_then(|source| source.downcast_ref::<serde_json::Error>())
                .expect("the source must be the error of serde_json");
            assert_eq!(source.to_string(), format!("JSON cannot represent {float}"));
        }

        let nan = f64::NAN;
        assert!(Json.encode(&f32::NAN).is_err(), "an f32 NaN was encoded");
        assert!(
            Json.encode(&Some(nan)).is_err(),
            "a NaN was encoded in an option"
        );
        assert!(
            Json.encode(&Newtype(nan)).is_err(),
            "a NaN was encoded in a newtype struct"
        );
        assert!(
            Json.encode(&Tuple(1, f32::NAN)).is_err(),
            "a NaN was encoded in a tuple struct"
        );
        assert!(
            Json.encode(&Struct { float: nan }).is_err(),
            "a NaN was encoded in a struct"
        );
        assert!(
            Json.encode(&Enum::Newtype(nan)).is_err(),
            "a NaN was encoded in a newtype variant"
        );
        assert!(
            Json.encode(&Enum::Tuple(1, nan)).is_err(),
            "a NaN was encoded in a tuple variant"
        );
        assert!(
            Json.encode(&Enum::Struct { float: nan }).is_err(),
            "a NaN was encoded in a struct variant"
        );
        assert!(
            Json.encode(&vec![nan]).is_err(),
            "a NaN was encoded in a sequence"
        );
        assert!(
            Json.encode(&(1, nan)).is_err(),
            "a NaN was encoded in a tuple"
        );
        assert!(
            Json.encode(&BTreeMap::from([("key", nan)])).is_err(),
            "a NaN was encoded in a map"
        );
    }

    /// Refusing non-finite floats changes nothing else: a value without one is encoded to the very
    /// bytes serde_json writes for it.
    #[test]
    fn json_encodes_everything_else_as_serde_json_does() {
        let value = (
            (true, 'c', "text", Unit, (), Some(1_u8), None::<u8>),
            (i8::MIN, i16::MIN, i32::MIN, i64::MIN, i128::MIN),
            (u8::MAX, u16::MAX, u32::MAX, u64::MAX, u128::MAX),
            (
                Newtype(-0.0),
                Tuple(1, f32::MAX),
                Struct { float: f64::MAX },
            ),
            (
                Enum::Unit,
                Enum::Newtype(f64::MIN_POSITIVE),
                Enum::Tuple(1, 21.5),
                Enum::Struct { float: f64::MIN },
            ),
            (vec![1.5, 2.5], BTreeMap::from([("key", [3.5])])),
        );

        assert_eq!(
            Json.encode(&value).expect("the value is JSON encodable"),
            serde_json::to_vec(&value).expect("the value is JSON encodable")
        );
    }

    fn value() -> Value {
        Value {
            name: "tellus".to_string(),
            count: 42,
        }
    }
}
