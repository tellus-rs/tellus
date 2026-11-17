use crate::{
    ActorId, Incoming,
    watch::{ActorTerminated, TerminatedHandler},
};
use flume::Sender;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;

pub(crate) struct CountedSender<T> {
    tx: Sender<T>,
    quota: Quota,
}

impl<T> CountedSender<T> {
    pub(crate) fn new(tx: Sender<T>, quota: Quota) -> Self {
        Self { tx, quota }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(crate) fn try_send_counted(&self, item: T) -> Result<(), CountedSendError> {
        match self.quota.reserve() {
            Ok(reservation) => {
                self.tx.send(item).map_err(|_| Disconnected)?;
                reservation.commit();
                Ok(())
            }

            // A full quota never drains after termination!
            Err(_) if self.tx.is_disconnected() => Err(Disconnected.into()),

            Err(full) => Err(full.into()),
        }
    }

    pub(crate) fn try_send_forced(&self, item: T) -> Result<(), Disconnected> {
        let reservation = self.quota.reserve_forced();
        self.tx.send(item).map_err(|_| Disconnected)?;
        reservation.commit();

        Ok(())
    }

    pub(crate) fn try_send_uncounted(&self, item: T) -> Result<(), Disconnected> {
        self.tx.send(item).map_err(|_| Disconnected)
    }

    pub(crate) fn len(&self) -> usize {
        self.tx.len()
    }
}

// A derived `Clone` would needlessly require `T: Clone`.
impl<T> Clone for CountedSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            quota: self.quota.clone(),
        }
    }
}

impl<M> TerminatedHandler for CountedSender<Incoming<M>>
where
    M: Send + 'static,
{
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.try_send_uncounted(Incoming::Terminated(actor_id))
            .map_err(|_| ActorTerminated)
    }
}

#[derive(Debug, Error)]
pub(crate) enum CountedSendError {
    #[error(transparent)]
    Full(#[from] Full),

    #[error(transparent)]
    Disconnected(#[from] Disconnected),
}

#[derive(Debug, Error)]
#[error("channel disconnected")]
pub(crate) struct Disconnected;

/// One [Quota::unreserve] per [Quota::reserve], else the count leaks or underflows.
#[derive(Clone)]
pub(crate) struct Quota(Repr);

impl Quota {
    pub(crate) fn unbounded() -> Self {
        Self(Repr::Unbounded)
    }

    pub(crate) fn bounded(capacity: NonZeroUsize) -> Self {
        Self(Repr::Bounded {
            capacity,
            count: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(crate) fn unreserve(&self) {
        if let Repr::Bounded { count, .. } = &self.0 {
            let previous = count.fetch_sub(1, Ordering::Relaxed);
            debug_assert!(previous > 0, "quota count underflow");
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn reserve(&self) -> Result<Reservation<'_>, Full> {
        let Repr::Bounded { capacity, count } = &self.0 else {
            return Ok(Reservation(Some(self)));
        };

        count
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < capacity.get()).then_some(current + 1)
            })
            .map(|_| Reservation(Some(self)))
            .map_err(|_| Full)
    }

    fn reserve_forced(&self) -> Reservation<'_> {
        if let Repr::Bounded { count, .. } = &self.0 {
            count.fetch_add(1, Ordering::Relaxed);
        }

        Reservation(Some(self))
    }
}

#[derive(Debug, Error)]
#[error("no capacity left")]
pub(crate) struct Full;

/// Unreserves on drop unless committed.
struct Reservation<'a>(Option<&'a Quota>);

impl Reservation<'_> {
    fn commit(mut self) {
        self.0 = None;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if let Some(quota) = self.0 {
            quota.unreserve();
        }
    }
}

#[derive(Clone)]
enum Repr {
    Unbounded,

    Bounded {
        capacity: NonZeroUsize,
        count: Arc<AtomicUsize>,
    },
}

#[cfg(test)]
mod tests {
    use crate::quota::{CountedSendError, CountedSender, Quota};
    use std::num::NonZeroUsize;

    /// A full quota whose receiver is gone reports the disconnect rather than the full quota: the
    /// reservations can never be released anymore, so a caller waiting for capacity would wait
    /// forever.
    #[test]
    fn disconnect_overrides_full() {
        let (item_tx, item_rx) = flume::unbounded();
        let item_tx = CountedSender::new(item_tx, Quota::bounded(NonZeroUsize::MIN));

        assert!(item_tx.try_send_counted(()).is_ok());
        drop(item_rx);

        assert!(matches!(
            item_tx.try_send_counted(()),
            Err(CountedSendError::Disconnected(_))
        ));
    }

    /// An unbounded quota reserves without limit and its counter short circuits, so unreserving
    /// without a matching reservation cannot underflow it.
    #[test]
    fn unbounded_never_fills() {
        let quota = Quota::unbounded();

        for _ in 0..1_000 {
            assert!(quota.reserve().is_ok());
        }
        quota.unreserve();
    }

    /// A bounded quota admits exactly `capacity` reservations, and releasing one frees exactly one
    /// slot.
    #[test]
    fn bounded_admits_exactly_capacity() {
        let capacity = NonZeroUsize::new(3).expect("3 is not zero");
        let quota = Quota::bounded(capacity);

        let mut reservations = (0..capacity.get())
            .map(|_| quota.reserve().expect("capacity is not exhausted yet"))
            .collect::<Vec<_>>();
        assert!(quota.reserve().is_err());

        reservations.pop();
        let _reservation = quota.reserve().expect("the released slot is available");
        assert!(quota.reserve().is_err());
    }

    /// A quota is shared by cloning it, so a reservation taken through one handle is visible
    /// through the other.
    #[test]
    fn clones_share_the_count() {
        let quota = Quota::bounded(NonZeroUsize::MIN);
        let clone = quota.clone();

        let reservation = quota.reserve().expect("the empty quota has capacity");
        assert!(clone.reserve().is_err());

        reservation.commit();
        clone.unreserve();
        assert!(quota.reserve().is_ok());
    }

    /// Cloning a sender shares its quota, so a clone cannot admit extra items past the capacity;
    /// this pins the manual [Clone] impl, which must clone the quota, never rebuild it.
    #[test]
    fn sender_clones_share_the_quota() {
        let (item_tx, _item_rx) = flume::unbounded();
        let item_tx = CountedSender::new(item_tx, Quota::bounded(NonZeroUsize::MIN));
        let clone = item_tx.clone();

        assert!(item_tx.try_send_counted(()).is_ok());
        assert!(matches!(
            clone.try_send_counted(()),
            Err(CountedSendError::Full(_))
        ));
    }

    /// A send failing once its reservation is taken releases that reservation, so a disconnect
    /// costs no capacity; committing only on success is what keeps the count off the failure path.
    #[test]
    fn a_failed_send_releases_its_reservation() {
        let (item_tx, item_rx) = flume::unbounded();
        let quota = Quota::bounded(NonZeroUsize::MIN);
        let item_tx = CountedSender::new(item_tx, quota.clone());

        drop(item_rx);
        assert!(matches!(
            item_tx.try_send_counted(()),
            Err(CountedSendError::Disconnected(_))
        ));

        assert!(quota.reserve().is_ok());
    }

    /// A forced item passes a full quota but is counted, so the receiver releases it like any
    /// other, without the count drifting.
    #[test]
    fn forced_reserves_past_a_full_quota() {
        let (item_tx, item_rx) = flume::unbounded();
        let quota = Quota::bounded(NonZeroUsize::MIN);
        let item_tx = CountedSender::new(item_tx, quota.clone());

        assert!(item_tx.try_send_counted(1).is_ok());
        assert!(matches!(
            item_tx.try_send_counted(2),
            Err(CountedSendError::Full(_))
        ));

        assert!(item_tx.try_send_forced(3).is_ok());
        assert!(item_tx.try_send_forced(4).is_ok());
        assert_eq!(item_rx.drain().collect::<Vec<_>>(), vec![1, 3, 4]);

        // Everything sent is still counted, so ordinary senders stay refused until the receiver
        // has released the excess.
        assert!(matches!(
            item_tx.try_send_counted(5),
            Err(CountedSendError::Full(_))
        ));
        for _ in 0..3 {
            quota.unreserve();
        }
        assert!(item_tx.try_send_counted(6).is_ok());
    }

    /// A forced send failing releases its reservation like a counted one, so a disconnect costs
    /// no capacity on this path either.
    #[test]
    fn a_failed_forced_send_releases_its_reservation() {
        let (item_tx, item_rx) = flume::unbounded();
        let quota = Quota::bounded(NonZeroUsize::MIN);
        let item_tx = CountedSender::new(item_tx, quota.clone());

        drop(item_rx);
        assert!(item_tx.try_send_forced(()).is_err());

        assert!(quota.reserve().is_ok());
    }

    /// An uncounted item passes a full quota and keeps its place behind the counted ones.
    #[test]
    fn uncounted_bypasses_a_full_quota() {
        let (item_tx, item_rx) = flume::unbounded();
        let item_tx = CountedSender::new(item_tx, Quota::bounded(NonZeroUsize::MIN));

        assert!(item_tx.try_send_counted(1).is_ok());
        assert!(matches!(
            item_tx.try_send_counted(2),
            Err(CountedSendError::Full(_))
        ));
        assert!(item_tx.try_send_uncounted(3).is_ok());

        assert_eq!(item_rx.drain().collect::<Vec<_>>(), vec![1, 3]);
    }
}
