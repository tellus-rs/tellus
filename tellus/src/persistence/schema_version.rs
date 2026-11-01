use derive_more::Display;

/// The schema version of a stored payload, written alongside it and checked on read: a payload
/// whose version the current code neither knows nor upcasts is rejected rather than decoded with
/// the current shape. It is a number of its own, never interchangeable with a
/// [SeqNo](crate::SeqNo) or any other value traveling with the same record.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersion(u16);

impl SchemaVersion {
    /// Create a schema version. It is `const` so it can define
    /// [Versioned::VERSION](crate::Versioned::VERSION).
    pub const fn new(schema_version: u16) -> Self {
        Self(schema_version)
    }

    /// This schema version as a `u16`, for stores mapping it onto a column type and for matching
    /// on it in a [Versioned::decode](crate::Versioned::decode) override.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::schema_version::SchemaVersion;

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
