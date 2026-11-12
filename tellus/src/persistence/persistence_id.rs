use derive_more::Display;
use std::{str::FromStr, sync::Arc};
use thiserror::Error;

const MAX_SEGMENT_LEN: usize = 255;

/// The stable identity of an event stream, chosen by the application and unchanged across
/// incarnations: every spawn for the same entity names the same stream. It is unrelated to
/// [ActorId](crate::ActorId), which is fresh per spawn; over time many actor IDs serve one
/// persistence ID.
///
/// It pairs an entity type, e.g. "order", with an entity ID, e.g. "42". Both segments must be
/// non-empty, at most 255 bytes long and free of slashes and NULs; [Display] joins them as
/// `entity_type/entity_id` and [FromStr] parses that shape back.
#[derive(Debug, Display, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(into = "String", try_from = "String")
)]
#[display("{entity_type}/{entity_id}")]
pub struct PersistenceId {
    entity_type: Arc<str>,
    entity_id: Arc<str>,
}

impl PersistenceId {
    /// Create a persistence ID from the given entity type and entity ID.
    ///
    /// # Errors
    /// Fails if a segment is empty, longer than 255 bytes or contains a slash or a NUL.
    pub fn new<T, I>(entity_type: T, entity_id: I) -> Result<Self, InvalidPersistenceId>
    where
        T: AsRef<str>,
        I: AsRef<str>,
    {
        let entity_type = valid_segment(entity_type.as_ref(), PersistenceIdSegment::EntityType)?;
        let entity_id = valid_segment(entity_id.as_ref(), PersistenceIdSegment::EntityId)?;

        Ok(Self {
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
        })
    }

    /// The entity type segment.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// The entity ID segment.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }
}

impl FromStr for PersistenceId {
    type Err = InvalidPersistenceId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (entity_type, entity_id) = s
            .split_once('/')
            .ok_or(InvalidPersistenceId::MissingSeparator)?;
        Self::new(entity_type, entity_id)
    }
}

impl TryFrom<String> for PersistenceId {
    type Error = InvalidPersistenceId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<PersistenceId> for String {
    fn from(id: PersistenceId) -> Self {
        id.to_string()
    }
}

/// Which segment of a [PersistenceId] a validation failure refers to.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceIdSegment {
    /// The entity type, e.g. "order".
    #[display("entity type")]
    EntityType,

    /// The entity ID, e.g. "42".
    #[display("entity ID")]
    EntityId,
}

/// Errors possibly returned by [PersistenceId::new] and its [FromStr] implementation.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPersistenceId {
    /// A segment is empty.
    #[error("empty {segment}")]
    EmptySegment {
        /// The segment which is empty.
        segment: PersistenceIdSegment,
    },

    /// A segment is longer than 255 bytes.
    #[error("{segment} longer than {MAX_SEGMENT_LEN} bytes")]
    SegmentTooLong {
        /// The segment which is too long.
        segment: PersistenceIdSegment,
    },

    /// A segment contains a slash, which is reserved as the separator.
    #[error("{segment} contains a slash, the reserved separator")]
    SlashInSegment {
        /// The segment which contains a slash.
        segment: PersistenceIdSegment,
    },

    /// A segment contains a NUL.
    #[error("{segment} contains a NUL")]
    NulInSegment {
        /// The segment which contains a NUL.
        segment: PersistenceIdSegment,
    },

    /// The parsed string lacks the `entity_type/entity_id` separator.
    #[error("persistence ID without a slash separator")]
    MissingSeparator,
}

fn valid_segment(value: &str, segment: PersistenceIdSegment) -> Result<&str, InvalidPersistenceId> {
    if value.is_empty() {
        Err(InvalidPersistenceId::EmptySegment { segment })
    } else if value.len() > MAX_SEGMENT_LEN {
        Err(InvalidPersistenceId::SegmentTooLong { segment })
    } else if value.contains('/') {
        Err(InvalidPersistenceId::SlashInSegment { segment })
    } else if value.contains('\0') {
        Err(InvalidPersistenceId::NulInSegment { segment })
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::persistence_id::{
        InvalidPersistenceId, MAX_SEGMENT_LEN, PersistenceId, PersistenceIdSegment,
    };

    /// The display shape `entity_type/entity_id` parses back into the same ID, so the string form
    /// can travel through configuration and discovery without a bespoke format.
    #[test]
    fn display_and_from_str_round_trip() {
        let id = PersistenceId::new("order", "42").expect("the segments are valid");

        assert_eq!(id.to_string(), "order/42");
        assert_eq!("order/42".parse::<PersistenceId>().as_ref(), Ok(&id));
        assert_eq!(id.entity_type(), "order");
        assert_eq!(id.entity_id(), "42");
    }

    /// Each validation rule is enforced on both segments and names the one which failed, so a
    /// caller can tell an invalid entity type from an invalid entity ID; parsing requires the
    /// separator.
    #[test]
    fn invalid_segments_are_rejected() {
        assert_eq!(
            PersistenceId::new("", "42"),
            Err(InvalidPersistenceId::EmptySegment {
                segment: PersistenceIdSegment::EntityType
            })
        );
        assert_eq!(
            PersistenceId::new("order", ""),
            Err(InvalidPersistenceId::EmptySegment {
                segment: PersistenceIdSegment::EntityId
            })
        );
        assert_eq!(
            PersistenceId::new("a".repeat(MAX_SEGMENT_LEN + 1), "42"),
            Err(InvalidPersistenceId::SegmentTooLong {
                segment: PersistenceIdSegment::EntityType
            })
        );
        assert_eq!(
            PersistenceId::new("order", "a".repeat(MAX_SEGMENT_LEN + 1)),
            Err(InvalidPersistenceId::SegmentTooLong {
                segment: PersistenceIdSegment::EntityId
            })
        );
        assert_eq!(
            PersistenceId::new("or/der", "42"),
            Err(InvalidPersistenceId::SlashInSegment {
                segment: PersistenceIdSegment::EntityType
            })
        );
        assert_eq!(
            PersistenceId::new("order", "4/2"),
            Err(InvalidPersistenceId::SlashInSegment {
                segment: PersistenceIdSegment::EntityId
            })
        );
        assert_eq!(
            PersistenceId::new("or\0der", "42"),
            Err(InvalidPersistenceId::NulInSegment {
                segment: PersistenceIdSegment::EntityType
            })
        );
        assert_eq!(
            PersistenceId::new("order", "42\0"),
            Err(InvalidPersistenceId::NulInSegment {
                segment: PersistenceIdSegment::EntityId
            })
        );
        assert_eq!(
            "order/4\0".parse::<PersistenceId>(),
            Err(InvalidPersistenceId::NulInSegment {
                segment: PersistenceIdSegment::EntityId
            })
        );
        assert_eq!(
            "order-42".parse::<PersistenceId>(),
            Err(InvalidPersistenceId::MissingSeparator)
        );
    }

    /// The limit is inclusive: a segment of exactly the maximum length is accepted, so an
    /// off-by-one in the check is caught rather than only its rejecting side.
    #[test]
    fn a_segment_of_the_maximum_length_is_accepted() {
        assert!(PersistenceId::new("a".repeat(MAX_SEGMENT_LEN), "42").is_ok());
        assert!(PersistenceId::new("order", "a".repeat(MAX_SEGMENT_LEN)).is_ok());
    }

    /// The serde representation is the display string, not the struct: a persistence ID travels
    /// through configuration and messages as a plain string.
    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trips_as_a_string() {
        let id = PersistenceId::new("order", "42").expect("the segments are valid");
        let json = serde_json::to_string(&id).expect("the ID is serializable");

        assert_eq!(json, "\"order/42\"");
        assert_eq!(
            serde_json::from_str::<PersistenceId>(&json).expect("the string is deserializable"),
            id
        );
    }

    /// Deserialization enforces the constructor's rules, so an invalid ID cannot enter through a
    /// config file or a message.
    #[cfg(feature = "serde")]
    #[test]
    fn serde_rejects_an_invalid_string() {
        for invalid in ["\"order\"", "\"/42\"", "\"order/\"", "\"or\\u0000der/42\""] {
            assert!(
                serde_json::from_str::<PersistenceId>(invalid).is_err(),
                "{invalid} must not deserialize"
            );
        }
    }
}
