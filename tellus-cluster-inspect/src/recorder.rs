use crate::{
    InspectEvent,
    events::{Cursor, EventLog, Stamped, now_millis},
};
use futures_util::{Stream, StreamExt};
use std::{
    num::NonZeroUsize,
    pin::pin,
    sync::{Arc, Mutex},
};
use tellus::cluster::Change;

/// The log plus the current state and verdict, guarded by one lock: a capture sees the ring and
/// the pair in agreement, so everything after its anchor is newer than the pair.
pub(crate) struct Recorder {
    log: Arc<EventLog<InspectEvent>>,
    record_lock: Mutex<Current>,
}

impl Recorder {
    pub(crate) fn new(events_kept: NonZeroUsize) -> Self {
        Self {
            log: Arc::new(EventLog::new(now_millis(), events_kept)),
            record_lock: Mutex::new(Current::default()),
        }
    }

    pub(crate) fn log(&self) -> &Arc<EventLog<InspectEvent>> {
        &self.log
    }

    /// A client's opening sequence: the entries after a resumable cursor, else the retained
    /// history without ids followed by the current pair, whose last item alone carries the
    /// anchor's id. A client which drops before that item holds no position and recovers again.
    pub(crate) fn capture(&self, after: Option<Cursor>) -> Capture {
        let current = self.record_lock.lock().expect("not poisoned");
        let replay = self.log.replay(after);
        let resumed = replay.resumed;
        let anchor = replay.anchor;
        let started_at = self.log.started_at();
        let mut items = replay
            .into_entries()
            .into_iter()
            .map(|(seq, stamped)| {
                let cursor = resumed.then_some(Cursor { started_at, seq });
                (cursor, stamped)
            })
            .collect::<Vec<_>>();
        if !resumed {
            let pair = [current.state.clone(), current.settled.clone()]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let last = pair.len().checked_sub(1);
            for (index, stamped) in pair.into_iter().enumerate() {
                let cursor = (Some(index) == last).then_some(Cursor {
                    started_at,
                    seq: anchor,
                });
                items.push((cursor, stamped));
            }
        }

        Capture {
            resumed,
            items,
            anchor,
        }
    }

    /// Records every change, in order, until the stream ends or `shutdown` resolves.
    pub(crate) async fn record<S, F>(self: Arc<Self>, changes: S, shutdown: F)
    where
        S: Stream<Item = Change>,
        F: Future<Output = ()>,
    {
        let mut changes = pin!(changes.take_until(shutdown));
        while let Some(change) = changes.next().await {
            self.push(match change {
                Change::State(state) => InspectEvent::State(state),
                Change::Settled(settlement) => InspectEvent::Settled(settlement),
                Change::Gap { dropped } => InspectEvent::Gap { dropped },
            });
        }
    }

    /// The pair first, then the log, under one lock: a capture never sees a pushed event the
    /// pair does not reflect.
    pub(crate) fn push(&self, event: InspectEvent) {
        let mut current = self.record_lock.lock().expect("not poisoned");
        let stamped = Stamped {
            at: now_millis(),
            event,
        };
        match &stamped.event {
            InspectEvent::State(_) => current.state = Some(stamped.clone()),
            InspectEvent::Settled(_) => current.settled = Some(stamped.clone()),
            InspectEvent::Hello { .. } | InspectEvent::Gap { .. } => {}
        }
        self.log.push_stamped(stamped);
    }
}

pub(crate) struct Capture {
    pub(crate) resumed: bool,
    pub(crate) items: Vec<(Option<Cursor>, Stamped<InspectEvent>)>,
    pub(crate) anchor: u64,
}

#[derive(Default)]
struct Current {
    state: Option<Stamped<InspectEvent>>,
    settled: Option<Stamped<InspectEvent>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{settlement, state};
    use futures_util::{FutureExt, stream};
    use std::{collections::VecDeque, future, time::Duration};
    use tokio::{
        sync::oneshot,
        time::{sleep, timeout},
    };

    fn recorder(events_kept: usize) -> Arc<Recorder> {
        Arc::new(Recorder::new(
            NonZeroUsize::new(events_kept).expect("non-zero"),
        ))
    }

    fn kinds(items: &[(Option<Cursor>, Stamped<InspectEvent>)]) -> Vec<(Option<u64>, String)> {
        items
            .iter()
            .map(|(cursor, stamped)| {
                let kind = match &stamped.event {
                    InspectEvent::Hello { .. } => "hello".to_string(),
                    InspectEvent::State(state) => format!("state {}", state.version()),
                    InspectEvent::Settled(settlement) => {
                        format!("settled {} {}", settlement.version(), settlement.settled())
                    }
                    InspectEvent::Gap { dropped } => format!("gap {dropped}"),
                };
                (cursor.map(|cursor| cursor.seq), kind)
            })
            .collect()
    }

    async fn recorded(recorder: &Recorder, count: usize) {
        timeout(Duration::from_secs(5), async {
            while recorder.log.replay(None).entries().count() < count {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("recorded in time");
    }

    #[tokio::test]
    async fn changes_are_recorded_in_order_and_the_pair_is_the_latest_of_each() {
        let recorder = recorder(16);
        let changes = stream::iter([
            Change::State(state(1)),
            Change::Settled(settlement(1, true)),
            Change::Gap { dropped: 2 },
            Change::State(state(2)),
            Change::Settled(settlement(1, true)),
        ])
        .chain(stream::pending());
        tokio::spawn(recorder.clone().record(changes, future::pending()));
        recorded(&recorder, 5).await;

        let capture = recorder.capture(None);
        assert!(!capture.resumed);
        assert_eq!(capture.anchor, 5);
        assert_eq!(
            kinds(&capture.items),
            vec![
                (None, "state 1".to_string()),
                (None, "settled 1 true".to_string()),
                (None, "gap 2".to_string()),
                (None, "state 2".to_string()),
                (None, "settled 1 true".to_string()),
                (None, "state 2".to_string()),
                (Some(5), "settled 1 true".to_string()),
            ]
        );
    }

    /// The production stream is an `unfold` over an async source, which is not `Unpin`.
    #[tokio::test]
    async fn an_unpinned_stream_is_accepted() {
        let recorder = recorder(16);
        let changes = stream::unfold(
            VecDeque::from([Change::State(state(1))]),
            |mut queue| async move {
                sleep(Duration::from_millis(1)).await;
                let change = queue.pop_front()?;
                Some((change, queue))
            },
        );
        recorder.clone().record(changes, future::pending()).await;

        assert_eq!(recorder.log.replay(None).entries().count(), 1);
    }

    /// With room for one event the verdict displaces the state; a fresh client and one with a
    /// lost position still get the state through the pair, and only the pair's last item is a
    /// position to resume from.
    #[tokio::test]
    async fn capacity_one_recovers_through_the_pair() {
        let recorder = recorder(1);
        recorder.push(InspectEvent::State(state(1)));
        recorder.push(InspectEvent::Settled(settlement(1, true)));

        let fresh = recorder.capture(None);
        assert!(!fresh.resumed);
        assert_eq!(
            kinds(&fresh.items),
            vec![
                (None, "settled 1 true".to_string()),
                (None, "state 1".to_string()),
                (Some(2), "settled 1 true".to_string()),
            ]
        );

        let again = recorder.capture(None);
        assert_eq!(kinds(&again.items), kinds(&fresh.items));

        let lost = recorder.capture(Some(Cursor {
            started_at: recorder.log.started_at(),
            seq: 0,
        }));
        assert!(!lost.resumed);
        assert_eq!(kinds(&lost.items), kinds(&fresh.items));

        let resumed = recorder.capture(Some(Cursor {
            started_at: recorder.log.started_at(),
            seq: fresh.anchor,
        }));
        assert!(resumed.resumed);
        assert!(resumed.items.is_empty());
    }

    /// Verdicts recorded after a capture follow the anchor, none of them older than the pair.
    #[tokio::test]
    async fn flips_around_the_cut_follow_the_pair() {
        let recorder = recorder(16);
        recorder.push(InspectEvent::State(state(1)));
        recorder.push(InspectEvent::Settled(settlement(1, false)));
        let capture = recorder.capture(None);
        assert_eq!(capture.anchor, 2);

        let flips = std::thread::spawn({
            let recorder = recorder.clone();
            move || {
                recorder.push(InspectEvent::Settled(settlement(1, true)));
                recorder.push(InspectEvent::Settled(settlement(1, false)));
            }
        });
        flips.join().expect("the flips complete");

        let mut live = Box::pin(recorder.log.follow_from(capture.anchor));
        let (first, _) = live.next().await.expect("an event");
        let (second, _) = live.next().await.expect("an event");
        assert_eq!((first.seq, second.seq), (3, 4));
        assert!(
            kinds(&capture.items)
                .last()
                .is_some_and(|(seq, kind)| *seq == Some(2) && kind == "settled 1 false")
        );
    }

    #[tokio::test]
    async fn recording_stops_once_the_shutdown_resolves() {
        let recorder = recorder(16);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let changes = stream::iter([Change::State(state(1))]).chain(stream::pending());
        let recording = tokio::spawn(recorder.clone().record(changes, shutdown_rx.map(drop)));
        recorded(&recorder, 1).await;
        assert!(!recording.is_finished());

        shutdown_tx
            .send(())
            .expect("the recording awaits the shutdown");
        timeout(Duration::from_secs(5), recording)
            .await
            .expect("the recording stops with the shutdown")
            .expect("the recording does not panic");
    }
}
