use crate::{Actor, ActorContext, ActorId, ActorRef, Control, Incoming};
use std::{
    any::type_name,
    collections::{HashMap, VecDeque, hash_map::Entry as MapEntry},
    convert::Infallible,
    hash::Hash,
    marker::PhantomData,
    num::NonZeroUsize,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep},
};
use tracing::{debug, warn};

/// Hosts entities: actors identified by a key, spawned on the first message for that key and, if
/// configured, passivated once they have not been messaged for a while.
///
/// A host is an ordinary actor whose messages are [HostEnvelope]s, so it is spawned like any other
/// and its entities are addressed through [ActorRef::tell] and [ActorRef::ask]. Its entities are
/// its children, hence stopping it stops them first.
///
/// Passivation frees the memory of entities nobody talks to, which is only safe where their state
/// is recoverable or disposable, e.g. for event sourced entities. A message arriving while an
/// entity stops is buffered and delivered to its next incarnation, so a passivation is invisible
/// to senders except in timing.
pub struct Host<K, M, S, P> {
    idle_timeout: Option<Duration>,
    buffer: NonZeroUsize,
    spawn: S,
    passivate: P,
    key: PhantomData<fn() -> K>,
    message: PhantomData<fn() -> M>,
}

impl<K, M, S, P> Host<K, M, S, P>
where
    K: Clone + Eq + Hash + Send + 'static,
    M: Send + 'static,
    S: Fn(&K, &ActorContext<HostEnvelope<K, M>>) -> ActorRef<M> + Send + 'static,
    P: Fn() -> M + Send + 'static,
{
    /// A host spawning its entities with `spawn` and passivating them with `passivate`.
    ///
    /// `spawn` must spawn a fresh child through the given context on every call, e.g. via
    /// [ActorContext::spawn] or [ActorContext::spawn_event_sourced]; it must not return an actor
    /// it has returned before, nor one from outside the host's subtree. One entity per key,
    /// stopping the entities with the host and the delivery of buffered messages all rest on
    /// that, and the signature cannot enforce it.
    ///
    /// `passivate` builds the message asking an entity to stop, since an actor is stopped by
    /// itself or by its parent alone; an entity which does not stop on that message is never
    /// passivated.
    ///
    /// [ActorContext::spawn_event_sourced]: crate::ActorContext::spawn_event_sourced
    pub fn new(config: HostConfig, spawn: S, passivate: P) -> Self {
        let HostConfig {
            idle_timeout,
            buffer,
        } = config;

        Self {
            idle_timeout,
            buffer,
            spawn,
            passivate,
            key: PhantomData,
            message: PhantomData,
        }
    }

    fn route(
        &self,
        context: &ActorContext<HostEnvelope<K, M>>,
        state: &mut State<K, M>,
        key: K,
        message: M,
    ) {
        let State { entries, keys, .. } = state;

        match entries.entry(key) {
            MapEntry::Occupied(mut occupied) => match occupied.get_mut() {
                Entry::Live {
                    actor,
                    last_message,
                } => {
                    actor.tell(message);
                    *last_message = Instant::now();
                }

                Entry::Stopping { actor_id, buffered } => {
                    if buffered.len() < self.buffer.get() {
                        buffered.push_back(message);
                    } else {
                        dead_letter::<M>(*actor_id, "host buffer full");
                    }
                }
            },

            MapEntry::Vacant(vacant) => {
                let actor = self.spawn_entity(context, vacant.key());
                actor.tell(message);
                remember(keys, &actor, vacant.key());
                vacant.insert(Entry::live(actor));
            }
        }
    }

    fn terminated(
        &self,
        context: &ActorContext<HostEnvelope<K, M>>,
        state: &mut State<K, M>,
        actor_id: ActorId,
    ) {
        if state.ticker == Some(actor_id) {
            self.passivate_idle(state);
            self.tick_later(context, state);
            return;
        }

        let Some(key) = state.keys.remove(&actor_id) else {
            return;
        };
        let Some(Entry::Stopping { buffered, .. }) = state.entries.remove(&key) else {
            return;
        };
        if buffered.is_empty() {
            return;
        }

        let actor = self.spawn_entity(context, &key);
        for message in buffered {
            if actor.try_tell_forced(message).is_err() {
                dead_letter::<M>(actor.actor_id(), "entity terminated");
            }
        }
        remember(&mut state.keys, &actor, &key);
        state.entries.insert(key, Entry::live(actor));
    }

    fn passivate_idle(&self, state: &mut State<K, M>) {
        let Some(idle_timeout) = self.idle_timeout else {
            return;
        };

        for entry in state.entries.values_mut() {
            match entry {
                Entry::Stopping { actor_id, .. } => debug!(%actor_id, "passivation pending"),

                Entry::Live {
                    actor,
                    last_message,
                } => {
                    if last_message.elapsed() < idle_timeout
                        || actor.try_tell((self.passivate)()).is_err()
                    {
                        continue;
                    }

                    let actor_id = actor.actor_id();
                    *entry = Entry::Stopping {
                        actor_id,
                        buffered: VecDeque::new(),
                    };
                }
            }
        }
    }

    fn tick_later(&self, context: &ActorContext<HostEnvelope<K, M>>, state: &mut State<K, M>) {
        state.ticker = self.idle_timeout.map(|idle_timeout| {
            let ticker = context.spawn(Ticker { idle_timeout });
            context.watch(&ticker);
            ticker.actor_id()
        });
    }

    fn spawn_entity(&self, context: &ActorContext<HostEnvelope<K, M>>, key: &K) -> ActorRef<M> {
        let entity = (self.spawn)(key, context);
        context.watch(&entity);
        entity
    }
}

impl<K, M, S, P> Actor for Host<K, M, S, P>
where
    K: Clone + Eq + Hash + Send + 'static,
    M: Send + 'static,
    S: Fn(&K, &ActorContext<HostEnvelope<K, M>>) -> ActorRef<M> + Send + 'static,
    P: Fn() -> M + Send + 'static,
{
    type Message = HostEnvelope<K, M>;
    type State = State<K, M>;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let mut state = State::default();
        self.tick_later(context, &mut state);

        Ok(state)
    }

    fn receive(
        &self,
        context: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        mut state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        match incoming {
            Incoming::Message(HostEnvelope { key, message }) => {
                self.route(context, &mut state, key, message)
            }

            Incoming::Terminated(actor_id) => self.terminated(context, &mut state, actor_id),
        }

        Ok(Control::Continue(state))
    }
}

/// A message for the entity under the given key: the message type of a [Host].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostEnvelope<K, M> {
    /// Which entity.
    pub key: K,

    /// What it is told.
    pub message: M,
}

/// Configuration for a [Host].
#[derive(Debug, Clone, Copy)]
pub struct HostConfig {
    idle_timeout: Option<Duration>,
    buffer: NonZeroUsize,
}

impl HostConfig {
    /// A configuration keeping every entity alive, buffering at most `buffer` messages per entity
    /// while it stops. Defaults to a buffer of 64.
    pub fn new(buffer: NonZeroUsize) -> Self {
        Self {
            idle_timeout: None,
            buffer,
        }
    }

    /// This configuration, passivating an entity which has not been messaged for `idle_timeout`.
    ///
    /// # Errors
    /// Fails for a zero duration, which would have the host sweeping without pause; leave the
    /// idle timeout unset to keep every entity alive instead.
    pub fn with_idle_timeout(self, idle_timeout: Duration) -> Result<Self, InvalidHostConfig> {
        if idle_timeout.is_zero() {
            return Err(InvalidHostConfig::ZeroIdleTimeout);
        }

        Ok(Self {
            idle_timeout: Some(idle_timeout),
            ..self
        })
    }
}

impl Default for HostConfig {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(64).expect("64 is not zero"))
    }
}

/// A [HostConfig] which cannot be used.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHostConfig {
    /// The idle timeout was zero.
    #[error("idle timeout must not be zero")]
    ZeroIdleTimeout,
}

/// The state of a [Host].
pub struct State<K, M> {
    entries: HashMap<K, Entry<M>>,
    keys: HashMap<ActorId, K>,
    ticker: Option<ActorId>,
}

// A derived `Default` would needlessly require `K: Default`/`M: Default`.
impl<K, M> Default for State<K, M> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            keys: HashMap::new(),
            ticker: None,
        }
    }
}

enum Entry<M> {
    Live {
        actor: ActorRef<M>,
        last_message: Instant,
    },

    Stopping {
        actor_id: ActorId,
        buffered: VecDeque<M>,
    },
}

impl<M> Entry<M> {
    fn live(actor: ActorRef<M>) -> Self {
        Self::Live {
            actor,
            last_message: Instant::now(),
        }
    }
}

/// Stops itself once its timeout has elapsed, so its watching host is woken by the terminated
/// signal: the host cannot wake itself with a timer, since its own message type is reserved for its
/// entities.
struct Ticker {
    idle_timeout: Duration,
}

impl Actor for Ticker {
    type Message = Tick;
    type State = AbortSleeping;
    type Error = Infallible;

    fn init(&self, context: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        let ticker = context.self_ref().clone();
        let idle_timeout = self.idle_timeout;

        let sleeping = tokio::spawn(async move {
            sleep(idle_timeout).await;
            ticker.tell(Tick);
        });

        Ok(AbortSleeping(sleeping))
    }

    fn receive(
        &self,
        _context: &ActorContext<Self::Message>,
        _incoming: Incoming<Self::Message>,
        _state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Stop)
    }
}

struct Tick;

struct AbortSleeping(JoinHandle<()>);

impl Drop for AbortSleeping {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn remember<K, M>(keys: &mut HashMap<ActorId, K>, entity: &ActorRef<M>, key: &K)
where
    K: Clone + Eq + Hash,
{
    let known = keys.insert(entity.actor_id(), key.clone());
    debug_assert!(
        known.is_none(),
        "the spawn function returned an already hosted actor; it must spawn a fresh child per call"
    );
}

fn dead_letter<M>(actor_id: ActorId, error: &'static str) {
    warn!(
        %actor_id,
        message_type = type_name::<M>(),
        error,
        "dead letter"
    );
}
