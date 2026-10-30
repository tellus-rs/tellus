use crate::{Incoming, MailboxCapacity};
use std::{collections::VecDeque, num::NonZeroUsize};

/// The items an actor has deferred, and those it has handed back to its run loop.
pub(crate) struct Stash<M> {
    capacity: Option<NonZeroUsize>,
    stashed: VecDeque<Incoming<M>>,
    unstashed: VecDeque<Incoming<M>>,
}

impl<M> Stash<M> {
    pub(crate) fn new(capacity: MailboxCapacity) -> Self {
        let capacity = match capacity {
            MailboxCapacity::Unbounded => None,
            MailboxCapacity::Bounded(capacity) => Some(capacity),
        };

        Self {
            capacity,
            stashed: VecDeque::new(),
            unstashed: VecDeque::new(),
        }
    }

    pub(crate) fn push(&mut self, incoming: Incoming<M>) -> Result<(), Incoming<M>> {
        let full = self
            .capacity
            .is_some_and(|capacity| self.stashed.len() + self.unstashed.len() >= capacity.get());
        if full {
            return Err(incoming);
        }

        self.stashed.push_back(incoming);
        Ok(())
    }

    pub(crate) fn unstash_all(&mut self) {
        self.unstashed.append(&mut self.stashed);
    }

    pub(crate) fn pop_unstashed(&mut self) -> Option<Incoming<M>> {
        self.unstashed.pop_front()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn take_stashed(&mut self) -> Vec<Incoming<M>> {
        self.stashed.drain(..).collect()
    }

    pub(crate) fn take_all(&mut self) -> Vec<Incoming<M>> {
        self.unstashed
            .drain(..)
            .chain(self.stashed.drain(..))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Incoming, MailboxCapacity, stash::Stash};
    use std::num::NonZeroUsize;

    /// A full stash hands the incoming back, counting the unstashed items which are not received
    /// yet.
    #[test]
    fn a_full_stash_hands_the_incoming_back() {
        let mut stash = Stash::new(MailboxCapacity::Bounded(
            NonZeroUsize::new(2).expect("two is non-zero"),
        ));

        assert!(stash.push(Incoming::Message(1)).is_ok());
        assert!(stash.push(Incoming::Message(2)).is_ok());
        assert_eq!(stash.push(Incoming::Message(3)), Err(Incoming::Message(3)));

        stash.unstash_all();
        assert_eq!(stash.push(Incoming::Message(3)), Err(Incoming::Message(3)));
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(stash.push(Incoming::Message(3)).is_ok());
    }

    /// Unstashing keeps the stash order, and an item stashed meanwhile waits behind the unstashed
    /// ones for the next call.
    #[test]
    fn unstashed_items_keep_their_order() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        stash.unstash_all();
        assert_eq!(stash.pop_unstashed(), None);

        assert!(stash.push(Incoming::Message(1)).is_ok());
        assert!(
            stash
                .push(Incoming::Terminated(crate::ActorId::new()))
                .is_ok()
        );
        stash.unstash_all();
        assert!(stash.push(Incoming::Message(3)).is_ok());

        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(1)));
        assert!(matches!(
            stash.pop_unstashed(),
            Some(Incoming::Terminated(_))
        ));
        assert_eq!(stash.pop_unstashed(), None);

        stash.unstash_all();
        assert_eq!(stash.pop_unstashed(), Some(Incoming::Message(3)));
    }

    /// Taking everything yields the unstashed items first, then the stashed ones.
    #[test]
    fn take_all_yields_unstashed_before_stashed() {
        let mut stash = Stash::new(MailboxCapacity::Unbounded);
        assert!(stash.push(Incoming::Message(1)).is_ok());
        stash.unstash_all();
        assert!(stash.push(Incoming::Message(2)).is_ok());

        assert_eq!(stash.take_stashed(), [Incoming::Message(2)]);
        assert!(stash.push(Incoming::Message(3)).is_ok());
        assert_eq!(
            stash.take_all(),
            [Incoming::Message(1), Incoming::Message(3)]
        );
        assert_eq!(stash.pop_unstashed(), None);
    }
}
