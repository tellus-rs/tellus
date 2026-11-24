use crate::{quota::Queue, sync::CachePadded};
#[cfg(test)]
use std::iter;
use std::{
    collections::{VecDeque, vec_deque},
    future,
    iter::Chain,
    mem,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    pub(crate) fn send(&self, item: T) -> Result<(), T> {
        let mut state = self.shared.lock();
        // Under the lock, else an item can slip in after `close_and_drain`!
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(item);
        }
        state.queue.push_back(item);
        let waker = state.waker.take();
        drop(state);

        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    pub(crate) fn len(&self) -> usize {
        let state = self.shared.lock();
        state.queue.len() + self.shared.batch_len.load(Ordering::Relaxed)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            let waker = self.shared.lock().waker.take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl<T> Queue<T> for Sender<T> {
    fn push(&self, item: T) -> Result<(), T> {
        self.send(item)
    }

    fn is_closed(&self) -> bool {
        Sender::is_closed(self)
    }

    fn len(&self) -> usize {
        Sender::len(self)
    }
}

pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
    batch: VecDeque<T>,
}

impl<T> Receiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    #[cfg(any(test, feature = "test-util", feature = "cluster"))]
    pub(crate) fn try_recv(&mut self) -> Option<T> {
        if self.batch.is_empty() {
            let mut state = self.shared.lock();
            refill(&mut self.batch, &self.shared.batch_len, &mut state);
        }
        self.pop()
    }

    #[cfg(test)]
    pub(crate) fn try_iter(&mut self) -> impl Iterator<Item = T> {
        iter::from_fn(|| self.try_recv())
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn is_empty(&self) -> bool {
        self.batch.is_empty() && self.shared.lock().queue.is_empty()
    }

    pub(crate) fn close_and_drain(
        mut self,
    ) -> Chain<vec_deque::IntoIter<T>, vec_deque::IntoIter<T>> {
        let batch = mem::take(&mut self.batch);
        let (queued, waker) = self.close();
        drop(waker);

        batch.into_iter().chain(queued)
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if !self.batch.is_empty() {
            return Poll::Ready(self.pop());
        }

        let mut state = self.shared.lock();
        refill(&mut self.batch, &self.shared.batch_len, &mut state);
        if !self.batch.is_empty() {
            drop(state);
            return Poll::Ready(self.pop());
        }

        // Under the lock, else the last sender's wake could pass before the waker is stored!
        if self.shared.sender_count.load(Ordering::Acquire) == 0 {
            return Poll::Ready(None);
        }
        match &mut state.waker {
            Some(waker) => waker.clone_from(cx.waker()),
            None => state.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    fn close(&self) -> (VecDeque<T>, Option<Waker>) {
        let mut state = self.shared.lock();
        self.shared.closed.store(true, Ordering::Release);
        self.shared.batch_len.store(0, Ordering::Relaxed);
        (mem::take(&mut state.queue), state.waker.take())
    }

    fn pop(&mut self) -> Option<T> {
        let item = self.batch.pop_front();
        self.shared
            .batch_len
            .store(self.batch.len(), Ordering::Relaxed);
        item
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        self.shared.closed.store(true, Ordering::Release);
        let waker = state.waker.take();
        drop(state);
        drop(waker);
    }
}

pub(crate) fn make_channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            waker: None,
        }),
        closed: AtomicBool::new(false),
        sender_count: AtomicUsize::new(1),
        batch_len: CachePadded::new(AtomicUsize::new(0)),
    });

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver {
            shared,
            batch: VecDeque::new(),
        },
    )
}

// Only under the lock, else a sender's `len` can miss the items in transit!
fn refill<T>(batch: &mut VecDeque<T>, batch_len: &AtomicUsize, state: &mut State<T>) {
    mem::swap(batch, &mut state.queue);
    batch_len.store(batch.len(), Ordering::Relaxed);
}

struct Shared<T> {
    state: Mutex<State<T>>,
    closed: AtomicBool,
    sender_count: AtomicUsize,
    batch_len: CachePadded<AtomicUsize>,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct State<T> {
    queue: VecDeque<T>,
    waker: Option<Waker>,
}

#[cfg(test)]
mod tests {
    use crate::channel::make_channel;
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };
    use tokio::{runtime::Builder, time::timeout};

    #[tokio::test]
    async fn items_arrive_in_order_and_end_after_the_last_sender() {
        let (tx, mut rx) = make_channel();
        let clone = tx.clone();
        assert!(tx.send(1).is_ok());
        assert!(clone.send(2).is_ok());
        assert_eq!(tx.len(), 2);
        drop(tx);

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(clone.len(), 1);
        assert!(clone.send(3).is_ok());
        assert_eq!(clone.len(), 2);
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(clone.len(), 0);
        assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());

        drop(clone);
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn close_and_drain_takes_the_batch_and_the_queue_and_closes() {
        let (tx, mut rx) = make_channel();
        assert!(tx.send(1).is_ok());
        assert!(tx.send(2).is_ok());
        assert_eq!(rx.recv().await, Some(1));
        assert!(tx.send(3).is_ok());

        assert_eq!(rx.close_and_drain().collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(tx.len(), 0);
        assert_eq!(tx.shared.lock().queue.capacity(), 0);
        assert!(tx.is_closed());
        assert_eq!(tx.send(4), Err(4));
    }

    #[test]
    fn sends_fail_once_the_receiver_is_gone() {
        let (tx, rx) = make_channel();
        assert!(!tx.is_closed());
        drop(rx);

        assert!(tx.is_closed());
        assert_eq!(tx.send(1), Err(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_parked_receiver_gets_every_item_from_many_threads() {
        const SENDERS: usize = 8;
        const ITEMS: usize = 20_000;

        let (tx, mut rx) = make_channel();
        let tx = Arc::new(tx);
        let senders = (0..SENDERS)
            .map(|sender| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for item in 0..ITEMS {
                        assert!(tx.send((sender, item)).is_ok());
                        if item % 64 == 0 {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(tx);

        let mut next = [0; SENDERS];
        while let Some((sender, item)) = rx.recv().await {
            assert_eq!(next[sender], item);
            next[sender] += 1;
        }
        assert!(next.iter().all(|&count| count == ITEMS));
        for sender in senders {
            sender.join().expect("sender does not panic");
        }
    }

    #[test]
    fn senders_racing_the_closure_lose_nothing() {
        const ROUNDS: usize = 1_000;
        const SENDERS: usize = 4;
        const ITEMS: usize = 400;

        let runtime = Builder::new_current_thread()
            .build()
            .expect("runtime builds");
        for round in 0..ROUNDS {
            let (tx, mut rx) = make_channel();
            let barrier = Arc::new(Barrier::new(SENDERS + 1));
            let senders = (0..SENDERS)
                .map(|sender| {
                    let tx = tx.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        barrier.wait();
                        let mut accepted = 0;
                        for item in 0..ITEMS {
                            match tx.send((sender, item)) {
                                Ok(()) => {
                                    assert_eq!(accepted, item, "accepted after a refused send");
                                    accepted += 1;
                                }
                                Err(_) => assert!(tx.is_closed()),
                            }
                            if item % 32 == sender {
                                thread::yield_now();
                            }
                        }
                        accepted
                    })
                })
                .collect::<Vec<_>>();
            drop(tx);
            barrier.wait();

            let mut seen = vec![Vec::new(); SENDERS];
            runtime.block_on(async {
                for _ in 0..round % 257 {
                    match rx.recv().await {
                        Some((sender, item)) => seen[sender].push(item),
                        None => break,
                    }
                }
            });
            for (sender, item) in rx.close_and_drain() {
                seen[sender].push(item);
            }

            for (sender, handle) in senders.into_iter().enumerate() {
                let accepted = handle.join().expect("sender does not panic");
                assert_eq!(
                    seen[sender],
                    (0..accepted).collect::<Vec<_>>(),
                    "round {round}, sender {sender}"
                );
            }
        }
    }
}
