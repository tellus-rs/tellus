use derive_more::Display;
use thiserror::Error;

/// A position in an event stream: either the sequence number of a stored event, gapless and
/// starting at 0, or the position at which the next event is appended, which is [SeqNo::ZERO] for
/// an empty stream. Replay resumes at a position, never after one, so no adjustment is needed
/// anywhere between a snapshot, a read and an append.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SeqNo(u64);

impl SeqNo {
    /// The position of an empty stream and the sequence number of the first event.
    pub const ZERO: Self = Self::new(0);

    /// Create a sequence number, e.g. from a store's column value.
    pub const fn new(seq_no: u64) -> Self {
        Self(seq_no)
    }

    /// The position directly after this one, or `None` if this one is `u64::MAX`, the last
    /// position.
    pub fn succ(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    /// The position after appending `count` events at this one, or `None` if it lies beyond
    /// `u64::MAX`, the last position.
    pub fn advanced_by(self, count: usize) -> Option<Self> {
        u64::try_from(count)
            .ok()
            .and_then(|count| self.0.checked_add(count))
            .map(Self)
    }

    /// This position as a `u64`, for stores mapping it onto a column type.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Appending `count` events at `seq_no` would advance a stream beyond its last position,
/// `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("sequence number {seq_no} cannot advance by {count}")]
pub struct SeqNoOverflow {
    /// The position the events would be appended at.
    pub seq_no: SeqNo,

    /// The number of events.
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use crate::persistence::seq_no::SeqNo;

    /// An empty stream's position is the number the first event will take.
    #[test]
    fn zero_is_the_first_position() {
        assert_eq!(SeqNo::ZERO, SeqNo::new(0));
        assert_eq!(SeqNo::ZERO.as_u64(), 0);
    }

    /// Advancing by one is the successor, and advancing by a count lands where that many
    /// successive appends would.
    #[test]
    fn succ_and_advanced_by_agree() {
        assert_eq!(SeqNo::ZERO.succ(), Some(SeqNo::new(1)));
        assert_eq!(SeqNo::ZERO.advanced_by(1), SeqNo::ZERO.succ());
        assert_eq!(SeqNo::new(2).advanced_by(3), Some(SeqNo::new(5)));
    }

    /// An effect without events settles at the same position it started at.
    #[test]
    fn advanced_by_zero_stays_put() {
        assert_eq!(SeqNo::new(7).advanced_by(0), Some(SeqNo::new(7)));
    }

    /// Positions order like their numbers, which a store's read filter relies on.
    #[test]
    fn positions_are_ordered_by_their_number() {
        assert!(SeqNo::ZERO < SeqNo::new(1));
        assert!(SeqNo::new(9) > SeqNo::new(8));
    }

    /// The last position has no successor.
    #[test]
    fn succ_of_the_last_position_is_none() {
        assert_eq!(SeqNo::new(u64::MAX - 1).succ(), Some(SeqNo::new(u64::MAX)));
        assert_eq!(SeqNo::new(u64::MAX).succ(), None);
    }

    /// Advancing by a count reaching beyond the last position is `None`, the last position itself
    /// is still reachable.
    #[test]
    fn advanced_by_beyond_the_last_position_is_none() {
        assert_eq!(
            SeqNo::new(u64::MAX - 3).advanced_by(3),
            Some(SeqNo::new(u64::MAX))
        );
        assert_eq!(SeqNo::new(u64::MAX - 3).advanced_by(4), None);
        assert_eq!(
            SeqNo::new(u64::MAX).advanced_by(0),
            Some(SeqNo::new(u64::MAX))
        );
    }

    /// A position renders as its bare number in logs and in the replay gap error.
    #[test]
    fn display_renders_the_number() {
        assert_eq!(SeqNo::new(42).to_string(), "42");
    }
}
