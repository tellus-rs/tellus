//! A bounded log of stamped events, replayed and followed over server-sent events. A client
//! reconnecting with `Last-Event-ID` continues where it left off while its position is still
//! retained; otherwise the hello says so and the caller decides what history to send first.

use axum::{
    http::HeaderMap,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures_util::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt::{self, Display, Formatter},
    num::NonZeroUsize,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::watch;

/// The retained events of one process, bounded to a capacity: pushing past it forgets the oldest.
pub struct EventLog<E> {
    started_at: u64,
    ring: Mutex<Ring<E>>,
    latest: watch::Sender<u64>,
}

impl<E> EventLog<E>
where
    E: Clone,
{
    /// An empty log which started at `started_at` unix milliseconds.
    pub fn new(started_at: u64, capacity: NonZeroUsize) -> Self {
        Self {
            started_at,
            ring: Mutex::new(Ring {
                entries: VecDeque::with_capacity(capacity.get()),
                capacity,
                next_seq: 1,
            }),
            latest: watch::Sender::new(0),
        }
    }

    /// Unix milliseconds at which the log started, the first half of every cursor.
    pub fn started_at(&self) -> u64 {
        self.started_at
    }

    /// Stamps and appends the event, forgetting the oldest one if the log is full.
    pub fn push(&self, event: E) -> Cursor {
        self.push_stamped(Stamped {
            at: now_millis(),
            event,
        })
    }

    /// Appends an event stamped by the caller, forgetting the oldest one if the log is full.
    pub fn push_stamped(&self, stamped: Stamped<E>) -> Cursor {
        let mut ring = self.ring.lock().expect("not poisoned");
        let seq = ring.next_seq;
        ring.next_seq += 1;
        if ring.entries.len() == ring.capacity.get() {
            ring.entries.pop_front();
        }
        ring.entries.push_back((seq, stamped));
        // Under the ring lock, so the latest seq never runs ahead of or behind the entries.
        self.latest.send_replace(seq);

        Cursor {
            started_at: self.started_at,
            seq,
        }
    }

    /// The retained events after the cursor, if the cursor comes from this log, does not lie
    /// ahead of its latest event and no event after it has been forgotten, else the whole
    /// retained history. Either way the replay carries the anchor to follow from, read together
    /// with the entries.
    pub fn replay(&self, after: Option<Cursor>) -> Replay<E> {
        let ring = self.ring.lock().expect("not poisoned");
        let anchor = ring.next_seq - 1;
        let oldest = ring.entries.front().map(|(seq, _)| *seq);
        let resumed = after.is_some_and(|cursor| {
            cursor.started_at == self.started_at
                && cursor.seq <= anchor
                && oldest.is_none_or(|oldest| cursor.seq + 1 >= oldest)
        });
        let after_seq = if resumed {
            after.map(|cursor| cursor.seq).unwrap_or_default()
        } else {
            0
        };
        let entries = ring
            .entries
            .iter()
            .filter(|(seq, _)| *seq > after_seq)
            .cloned()
            .collect::<Vec<_>>();

        Replay {
            resumed,
            entries,
            anchor,
        }
    }

    /// Every event pushed after the sequence number. Ends if that position falls out of the log,
    /// which a reconnect answers with a fresh replay.
    pub fn follow_from(
        self: &Arc<Self>,
        seq: u64,
    ) -> impl Stream<Item = (Cursor, Stamped<E>)> + Send + use<E>
    where
        E: Send + Sync + 'static,
    {
        // Subscribed before the first read, so a push between a read and the wait wakes it.
        let latest_rx = self.latest.subscribe();
        let follower = Follower {
            log: self.clone(),
            latest_rx,
            pending: VecDeque::new(),
            seq,
        };
        stream::unfold(follower, |mut follower| async move {
            loop {
                if let Some((seq, stamped)) = follower.pending.pop_front() {
                    follower.seq = seq;
                    let cursor = Cursor {
                        started_at: follower.log.started_at,
                        seq,
                    };
                    return Some(((cursor, stamped), follower));
                }
                match follower.log.read_after(follower.seq) {
                    Some(entries) if entries.is_empty() => {
                        if follower.latest_rx.changed().await.is_err() {
                            return None;
                        }
                    }
                    Some(entries) => follower.pending = entries,
                    None => return None,
                }
            }
        })
    }

    /// `None` if `seq + 1` was forgotten already.
    fn read_after(&self, seq: u64) -> Option<VecDeque<(u64, Stamped<E>)>> {
        let ring = self.ring.lock().expect("not poisoned");
        if ring
            .entries
            .front()
            .is_some_and(|(oldest, _)| seq + 1 < *oldest)
        {
            return None;
        }
        Some(
            ring.entries
                .iter()
                .filter(|(entry_seq, _)| *entry_seq > seq)
                .cloned()
                .collect(),
        )
    }
}

/// What [EventLog::replay] answers: the retained events to send first and the anchor to follow
/// from, read in one go.
pub struct Replay<E> {
    /// Whether the entries continue right after the cursor the client gave.
    pub resumed: bool,

    /// The sequence number the log stood at, which [EventLog::follow_from] continues after.
    pub anchor: u64,
    entries: Vec<(u64, Stamped<E>)>,
}

impl<E> Replay<E> {
    /// The replayed events with their sequence numbers, oldest first.
    pub fn entries(&self) -> impl Iterator<Item = (u64, &Stamped<E>)> {
        self.entries.iter().map(|(seq, stamped)| (*seq, stamped))
    }

    /// The replayed events, oldest first.
    pub fn into_entries(self) -> Vec<(u64, Stamped<E>)> {
        self.entries
    }

    /// The replayed events as the items [sse] sends, each with its cursor, for a stream which
    /// needs no recovery beyond the retained history.
    pub fn into_items(self, started_at: u64) -> Vec<(Option<Cursor>, Stamped<E>)> {
        self.entries
            .into_iter()
            .map(|(seq, stamped)| (Some(Cursor { started_at, seq }), stamped))
            .collect()
    }
}

/// What every message of an event stream carries, the hello included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stamped<E> {
    /// Unix milliseconds at which the event was recorded, or sent for a hello.
    pub at: u64,

    /// The event.
    pub event: E,
}

/// The id of a logged event, `{started_at}:{seq}`, so a cursor names the log it came from and a
/// restarted process never resumes a client at a stranger's position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// Unix milliseconds at which the log started.
    pub started_at: u64,

    /// The event's sequence number within that log, starting at one.
    pub seq: u64,
}

impl Display for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.started_at, self.seq)
    }
}

impl FromStr for Cursor {
    type Err = InvalidCursor;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (started_at, seq) = s.split_once(':').ok_or(InvalidCursor)?;
        let started_at = started_at.parse().map_err(|_| InvalidCursor)?;
        let seq = seq.parse().map_err(|_| InvalidCursor)?;

        Ok(Self { started_at, seq })
    }
}

/// A string which is no [Cursor].
#[derive(Debug, Error, PartialEq, Eq)]
#[error("a cursor is `{{started_at}}:{{seq}}`, two integers")]
pub struct InvalidCursor;

/// The cursor a client resumes from, `None` if the `Last-Event-ID` header is absent or is no
/// cursor, which both mean the client has no position to continue from.
pub fn last_event_id(headers: &HeaderMap) -> Option<Cursor> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

/// The stream a `GET /events` answers: the stamped hello, then `items` (each with its id, if it
/// has one), then every event pushed after `anchor`, each with its cursor as id, kept alive by
/// comments.
pub fn sse<E>(
    log: &Arc<EventLog<E>>,
    items: Vec<(Option<Cursor>, Stamped<E>)>,
    anchor: u64,
    hello: E,
) -> impl IntoResponse + use<E>
where
    E: Serialize + Clone + Send + Sync + 'static,
{
    let hello = Stamped {
        at: now_millis(),
        event: hello,
    };
    let opening = stream::iter(
        std::iter::once((None, hello))
            .chain(items)
            .map(|(cursor, stamped)| Ok::<_, Infallible>(event(cursor, &stamped))),
    );
    let live = log
        .follow_from(anchor)
        .map(|(cursor, stamped)| Ok(event(Some(cursor), &stamped)));

    Sse::new(opening.chain(live)).keep_alive(KeepAlive::default())
}

/// Unix milliseconds now.
pub fn now_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("now is after the epoch")
        .as_millis();
    u64::try_from(millis).expect("now fits in 64 bits of milliseconds")
}

struct Ring<E> {
    entries: VecDeque<(u64, Stamped<E>)>,
    capacity: NonZeroUsize,
    next_seq: u64,
}

struct Follower<E> {
    log: Arc<EventLog<E>>,
    latest_rx: watch::Receiver<u64>,
    pending: VecDeque<(u64, Stamped<E>)>,
    seq: u64,
}

fn event<E>(cursor: Option<Cursor>, stamped: &Stamped<E>) -> Event
where
    E: Serialize,
{
    let event = Event::default()
        .json_data(stamped)
        .expect("an event serializes");
    match cursor {
        Some(cursor) => event.id(cursor.to_string()),
        None => event,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    const STARTED_AT: u64 = 1_700_000_000_000;

    fn log(capacity: usize) -> Arc<EventLog<u32>> {
        Arc::new(EventLog::new(
            STARTED_AT,
            NonZeroUsize::new(capacity).expect("non-zero"),
        ))
    }

    fn cursor(seq: u64) -> Cursor {
        Cursor {
            started_at: STARTED_AT,
            seq,
        }
    }

    fn events<E>(replay: &Replay<E>) -> Vec<(u64, E)>
    where
        E: Clone,
    {
        replay
            .entries()
            .map(|(seq, stamped)| (seq, stamped.event.clone()))
            .collect()
    }

    #[test]
    fn pushes_are_numbered_from_one_and_replayed_in_order() {
        let log = log(8);
        assert_eq!(log.started_at(), STARTED_AT);
        assert_eq!(log.push(10), cursor(1));
        assert_eq!(log.push(20), cursor(2));
        assert_eq!(log.push(30), cursor(3));

        let replay = log.replay(None);
        assert!(!replay.resumed);
        assert_eq!(replay.anchor, 3);
        assert_eq!(events(&replay), vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn a_retained_cursor_resumes_right_after_it() {
        let log = log(8);
        for event in [10, 20, 30] {
            log.push(event);
        }

        let replay = log.replay(Some(cursor(1)));
        assert!(replay.resumed);
        assert_eq!(replay.anchor, 3);
        assert_eq!(events(&replay), vec![(2, 20), (3, 30)]);
    }

    #[test]
    fn the_latest_cursor_resumes_with_nothing_to_replay() {
        let log = log(8);
        for event in [10, 20, 30] {
            log.push(event);
        }

        let replay = log.replay(Some(cursor(3)));
        assert!(replay.resumed);
        assert!(events(&replay).is_empty());
    }

    #[test]
    fn the_cursor_before_the_oldest_retained_event_resumes() {
        let log = log(2);
        for event in [10, 20, 30] {
            log.push(event);
        }

        let replay = log.replay(Some(cursor(1)));
        assert!(replay.resumed);
        assert_eq!(events(&replay), vec![(2, 20), (3, 30)]);
    }

    #[test]
    fn a_forgotten_cursor_gets_the_whole_history() {
        let log = log(2);
        for event in [10, 20, 30, 40] {
            log.push(event);
        }

        let replay = log.replay(Some(cursor(1)));
        assert!(!replay.resumed);
        assert_eq!(events(&replay), vec![(3, 30), (4, 40)]);
    }

    #[test]
    fn a_cursor_beyond_the_latest_gets_the_whole_history() {
        let log = log(8);
        log.push(10);

        let replay = log.replay(Some(cursor(5)));
        assert!(!replay.resumed);
        assert_eq!(events(&replay), vec![(1, 10)]);
    }

    #[test]
    fn a_cursor_of_another_process_gets_the_whole_history() {
        let log = log(8);
        log.push(10);

        let replay = log.replay(Some(Cursor {
            started_at: STARTED_AT + 1,
            seq: 1,
        }));
        assert!(!replay.resumed);
        assert_eq!(events(&replay), vec![(1, 10)]);
    }

    #[test]
    fn an_empty_log_resumes_only_the_zero_cursor() {
        let log = log(8);

        let replay = log.replay(Some(cursor(0)));
        assert!(replay.resumed);
        assert!(events(&replay).is_empty());

        let replay = log.replay(Some(cursor(1)));
        assert!(!replay.resumed);
        assert!(events(&replay).is_empty());

        let replay = log.replay(None);
        assert!(!replay.resumed);
        assert_eq!(replay.anchor, 0);
        assert!(events(&replay).is_empty());
    }

    #[tokio::test]
    async fn following_yields_what_was_pushed_after_the_anchor() {
        let log = log(8);
        log.push(10);
        let replay = log.replay(None);
        // Pushed between the replay and the first poll, which the follower must not miss.
        log.push(20);
        let mut events = Box::pin(log.follow_from(replay.anchor));

        let (cursor_2, second) = events.next().await.expect("an event");
        assert_eq!((cursor_2, second.event), (cursor(2), 20));

        let pending = timeout(Duration::from_millis(50), events.next()).await;
        assert!(pending.is_err(), "nothing to yield yet");

        log.push(30);
        let (cursor_3, third) = timeout(Duration::from_secs(5), events.next())
            .await
            .expect("in time")
            .expect("an event");
        assert_eq!((cursor_3, third.event), (cursor(3), 30));
    }

    #[tokio::test]
    async fn a_follower_left_behind_ends() {
        let log = log(2);
        log.push(10);
        let mut events = Box::pin(log.follow_from(0));
        assert_eq!(events.next().await.expect("an event").0, cursor(1));

        for event in [20, 30, 40] {
            log.push(event);
        }

        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn concurrent_pushes_stay_gapless() {
        let log = log(1_024);
        let mut tasks = tokio::task::JoinSet::new();
        for task in 0..8u32 {
            let log = log.clone();
            tasks.spawn(async move {
                for n in 0..64 {
                    log.push(task * 64 + n);
                }
            });
        }
        tasks.join_all().await;

        let replay = log.replay(None);
        let seqs = replay.entries().map(|(seq, _)| seq).collect::<Vec<_>>();
        assert_eq!(seqs, (1..=512).collect::<Vec<_>>());
        assert_eq!(*log.latest.borrow(), 512);
    }

    #[test]
    fn a_cursor_round_trips_through_its_string_form() {
        let cursor = cursor(42);
        assert_eq!(cursor.to_string(), "1700000000000:42");
        assert_eq!("1700000000000:42".parse(), Ok(cursor));
    }

    #[test]
    fn a_malformed_cursor_is_rejected() {
        assert_eq!("42".parse::<Cursor>(), Err(InvalidCursor));
        assert_eq!("a:1".parse::<Cursor>(), Err(InvalidCursor));
        assert_eq!("1:".parse::<Cursor>(), Err(InvalidCursor));
        assert_eq!("".parse::<Cursor>(), Err(InvalidCursor));
    }
}
