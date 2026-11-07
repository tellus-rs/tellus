use derive_more::Display;
use serde::{Deserialize, Serialize};

/// The schema version of a payload, written or sent alongside it and checked on read: a payload
/// whose version the current code neither knows nor upcasts is rejected rather than decoded with
/// the current shape. It is a number of its own, never interchangeable with a sequence number or
/// any other value traveling with the same record. On the wire it is a varint of its number.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion(u16);

impl SchemaVersion {
    /// Create a schema version. It is `const` so it can define the `VERSION` of a versioned
    /// type.
    pub const fn new(schema_version: u16) -> Self {
        Self(schema_version)
    }

    /// This schema version as a `u16`, for stores mapping it onto a column type and for matching
    /// on it in a `decode` override.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::schema_version::SchemaVersion;

    /// A schema version round-trips through the `u16` a store column holds.
    #[test]
    fn new_and_as_u16_round_trip() {
        assert_eq!(SchemaVersion::new(7).as_u16(), 7);
    }

    /// A version renders as its bare number, which the unsupported-version error relies on.
    #[test]
    fn display_renders_the_number() {
        assert_eq!(SchemaVersion::new(7).to_string(), "7");
    }
}
