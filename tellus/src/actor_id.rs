use derive_more::{Display, Into};
use uuid::Uuid;

/// A unique actor ID: a UUID v7, so IDs are time-ordered by creation. With the `cluster` feature
/// it is serializable, e.g. to carry it in messages crossing node boundaries.
///
/// An ID is 16-byte aligned: an unaligned 16-byte copy of an ID can straddle a page boundary,
/// which is slow on aarch64.
#[derive(Debug, Display, Clone, Copy, Into, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "cluster", derive(serde::Serialize, serde::Deserialize))]
#[repr(align(16))]
pub struct ActorId(Uuid);

impl ActorId {
    pub(crate) fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

#[cfg(test)]
mod tests {
    use crate::ActorId;
    use uuid::Uuid;

    /// An ID converts into the UUID it wraps, and that UUID is what its `Display` shows.
    #[test]
    fn an_id_converts_into_its_uuid() {
        let actor_id = ActorId::new();

        assert_eq!(Uuid::from(actor_id).to_string(), actor_id.to_string());
    }

    /// An ID is as large as its UUID and 16-byte aligned.
    #[test]
    fn an_id_is_sixteen_bytes_and_sixteen_byte_aligned() {
        assert_eq!(size_of::<ActorId>(), 16);
        assert_eq!(align_of::<ActorId>(), 16);
    }
}
