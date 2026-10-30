use crate::{
    ActorConfig, ActorContext, ActorId, ActorRef, ActorSystem, TimerMode,
    actor_context::{
        PanicPayload, STATE_FAILED_TO_DROP, catch_and_log, catch_panic_and_log,
        catch_panic_and_log_async, catch_unwind_async, dispose_error, drop_containing_panic,
        log_failure, next_incoming, next_incoming_timed, should_restart, terminate,
    },
    actor_ref::SelfRef,
    actor_system::watch_root,
    persistence::{
        Persistence,
        codec::{Codec, EncodeError},
        effect::Effect,
        event_sourced::EventSourced,
        persistence_id::PersistenceId,
        seq_no::SeqNo,
        store::{
            AppendError, EncodedEvent, EncodedSnapshot, EventStore, SnapshotStore, StoredEvent,
            StoredSnapshot,
        },
        versioned::{DecodeError, Versioned, decode_versioned},
    },
    timers::{Sealed, Timed, Token, Untimed},
};
use std::{
    any::Any,
    borrow::Cow,
    error::Error,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    select,
    sync::watch,
    task::{self, consume_budget},
    time::Instant,
};
use tracing::{debug, warn};

const FAILED_TO_RECOVER: &str = "actor failed to recover";
const FAILED_TO_APPEND: &str = "actor failed to append events";
const SNAPSHOT_NOT_SAVED: &str = "snapshot not saved";
const VALUES_FAILED_TO_DROP: &str = "actor values failed to drop";
const REPLAY_PAGE: NonZeroUsize = NonZeroUsize::new(512).unwrap();

impl<M> ActorSystem<M>
where
    M: Send + 'static,
{
    /// Create an actor system by giving the [EventSourced] actor and [Persistence] for the root
    /// actor, using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn event_sourced<A, E, S, C>(actor: A, persistence: Persistence<E, S, C>) -> Self
    where
        A: EventSourced<Command = M> + Send + Sync + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        Self::event_sourced_with_config(actor, persistence, ActorConfig::default())
    }

    /// Create an actor system by giving the [EventSourced] actor, [Persistence] and [ActorConfig]
    /// for the root actor.
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn event_sourced_with_config<A, E, S, C>(
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> Self
    where
        A: EventSourced<Command = M> + Send + Sync + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        let (stop_root_tx, stopped_by_parent_rx) = watch::channel(());
        let stop_root_tx = Arc::new(stop_root_tx);

        let root = spawn_event_sourced(stopped_by_parent_rx, actor, persistence, config);
        let terminated_rx = watch_root(&root, stop_root_tx.clone());

        Self::from_parts(root, stop_root_tx, terminated_rx)
    }
}

impl<M, T> ActorContext<M, T>
where
    T: TimerMode,
{
    /// Spawn an event-sourced child actor with the given [EventSourced] actor and [Persistence],
    /// using the default [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_event_sourced<A, E, S, C>(
        &self,
        actor: A,
        persistence: Persistence<E, S, C>,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        self.spawn_event_sourced_with_config(actor, persistence, ActorConfig::default())
    }

    /// Spawn an event-sourced child actor with the given [EventSourced] actor, [Persistence] and
    /// [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn spawn_event_sourced_with_config<A, E, S, C>(
        &self,
        actor: A,
        persistence: Persistence<E, S, C>,
        config: ActorConfig,
    ) -> ActorRef<A::Command>
    where
        A: EventSourced + Send + Sync + 'static,
        A::Command: Send + 'static,
        A::State: Send + 'static,
        A::Event: Send + 'static,
        A::Snapshot: Send + 'static,
        E: EventStore,
        S: SnapshotStore,
        C: Codec + Send + Sync + 'static,
    {
        spawn_event_sourced(self.stop_children_rx(), actor, persistence, config)
    }
}

struct Recovered<S> {
    id: PersistenceId,
    state: S,
    next_seq_no: SeqNo,
}

struct Settled<S> {
    state: S,
    next_seq_no: SeqNo,
    stop: bool,
}

#[derive(Debug, Error)]
enum ReplayError {
    #[error(transparent)]
    Decode(#[from] DecodeError),

    #[error("event stream gap: sequence number {seq_no}, expected {expected}")]
    Gap { seq_no: SeqNo, expected: SeqNo },
}

#[derive(Debug, Error)]
enum SnapshotError<E>
where
    E: Error,
{
    #[error(transparent)]
    Actor(E),

    #[error(transparent)]
    Encode(#[from] EncodeError),
}

/// The task future of the event-sourced run loop, expanded once per [TimerMode] with its wait
/// macro, so the untimed loop is exactly the loop without timers.
macro_rules! run_event_sourced {
    (
        $next_incoming:ident,
        $actor_id:ident,
        $self_ref:ident,
        $mailbox:ident,
        $actor:ident,
        $persistence:ident,
        $config:ident,
        $stopped_by_parent_rx:ident
    ) => {
        task::spawn(async move {
            let mut context = ActorContext::new($self_ref, $config.mailbox_capacity);

            let mut rx = $stopped_by_parent_rx.clone();
            let mut stopped_by_parent = pin!(rx.changed());

            let mut restarts = 0;

            'run: loop {
                let mut recovering =
                    Box::pin(recover_or_log_failure($actor_id, &$actor, &$persistence));
                let recovered = select! {
                    biased;

                    _ = &mut stopped_by_parent => {
                        debug!(
                            actor_id = %$actor_id,
                            "stopping, because parent stopped this actor"
                        );
                        drop_containing_panic($actor_id, VALUES_FAILED_TO_DROP, recovering);
                        break 'run;
                    }

                    recovered = &mut recovering => recovered,
                };
                drop(recovering);
                let mut up_since = None;

                if let Some(Recovered {
                    id,
                    state,
                    mut next_seq_no,
                }) = recovered
                {
                    let state = catch_and_log($actor_id, FAILED_TO_RECOVER, || {
                        $actor.recovered(&context, state)
                    });

                    if let Some(mut state) = state {
                        up_since = Some(Instant::now());

                        loop {
                            let incoming =
                                $next_incoming!($actor_id, $mailbox, context, stopped_by_parent);
                            let Some(incoming) = incoming else {
                                drop_containing_panic($actor_id, STATE_FAILED_TO_DROP, state);
                                break 'run;
                            };

                            let effect = catch_and_log($actor_id, "actor failed", || {
                                $actor.handle(&context, incoming, &state)
                            });
                            let Some(effect) = effect else {
                                drop_containing_panic($actor_id, STATE_FAILED_TO_DROP, state);
                                break;
                            };

                            match settle_or_log_failure(
                                $actor_id,
                                &$actor,
                                &$persistence,
                                &id,
                                state,
                                next_seq_no,
                                effect,
                            )
                            .await
                            {
                                Some(settled) => {
                                    state = settled.state;
                                    next_seq_no = settled.next_seq_no;

                                    if settled.stop {
                                        debug!(
                                            actor_id = %$actor_id,
                                            "stopping as decided by actor"
                                        );
                                        drop_containing_panic(
                                            $actor_id,
                                            STATE_FAILED_TO_DROP,
                                            state,
                                        );
                                        break 'run;
                                    }
                                }

                                None => break,
                            }
                        }
                    }
                }

                let restart = should_restart(
                    $actor_id,
                    $config.supervision_strategy,
                    up_since,
                    &mut restarts,
                    &$stopped_by_parent_rx,
                    &mut stopped_by_parent,
                    &mut context,
                )
                .await;
                if !restart {
                    break;
                }
            }

            terminate($actor, context, $mailbox).await;
        });
    };
}

fn spawn_event_sourced<A, E, S, C>(
    stopped_by_parent_rx: watch::Receiver<()>,
    actor: A,
    persistence: Persistence<E, S, C>,
    config: ActorConfig,
) -> ActorRef<A::Command>
where
    A: EventSourced + Send + Sync + 'static,
    A::Command: Send + 'static,
    A::State: Send + 'static,
    A::Event: Send + 'static,
    A::Snapshot: Send + 'static,
    E: EventStore,
    S: SnapshotStore,
    C: Codec + Send + Sync + 'static,
{
    <A::Timers as Sealed>::spawn_event_sourced(
        Token(()),
        stopped_by_parent_rx,
        actor,
        persistence,
        config,
    )
}

pub(crate) fn spawn_event_sourced_untimed<A, E, S, C>(
    stopped_by_parent_rx: watch::Receiver<()>,
    actor: A,
    persistence: Persistence<E, S, C>,
    config: ActorConfig,
) -> ActorRef<A::Command>
where
    A: EventSourced<Timers = Untimed> + Send + Sync + 'static,
    A::Command: Send + 'static,
    A::State: Send + 'static,
    A::Event: Send + 'static,
    A::Snapshot: Send + 'static,
    E: EventStore,
    S: SnapshotStore,
    C: Codec + Send + Sync + 'static,
{
    let actor_id = ActorId::new();
    let (self_ref, mut mailbox) = SelfRef::new(actor_id, config.mailbox_capacity);
    let actor_ref = self_ref.actor_ref().clone();

    run_event_sourced!(
        next_incoming,
        actor_id,
        self_ref,
        mailbox,
        actor,
        persistence,
        config,
        stopped_by_parent_rx
    );

    actor_ref
}

pub(crate) fn spawn_event_sourced_timed<A, E, S, C>(
    stopped_by_parent_rx: watch::Receiver<()>,
    actor: A,
    persistence: Persistence<E, S, C>,
    config: ActorConfig,
) -> ActorRef<A::Command>
where
    A: EventSourced<Timers = Timed> + Send + Sync + 'static,
    A::Command: Send + 'static,
    A::State: Send + 'static,
    A::Event: Send + 'static,
    A::Snapshot: Send + 'static,
    E: EventStore,
    S: SnapshotStore,
    C: Codec + Send + Sync + 'static,
{
    let actor_id = ActorId::new();
    let (self_ref, mut mailbox) = SelfRef::new(actor_id, config.mailbox_capacity);
    let actor_ref = self_ref.actor_ref().clone();

    run_event_sourced!(
        next_incoming_timed,
        actor_id,
        self_ref,
        mailbox,
        actor,
        persistence,
        config,
        stopped_by_parent_rx
    );

    actor_ref
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn recover_or_log_failure<A, E, S, C>(
    actor_id: ActorId,
    actor: &A,
    persistence: &Persistence<E, S, C>,
) -> Option<Recovered<A::State>>
where
    A: EventSourced,
    E: EventStore,
    S: SnapshotStore,
    C: Codec,
{
    let id = catch_panic_and_log(actor_id, FAILED_TO_RECOVER, || actor.persistence_id())?;

    let loaded = catch_panic_and_log_async(actor_id, FAILED_TO_RECOVER, async {
        persistence.snapshot_store.load(&id).await
    })
    .await;
    let snapshot = log_failure(actor_id, FAILED_TO_RECOVER, loaded)?;

    let decoded = snapshot.and_then(|stored| {
        let StoredSnapshot {
            next_seq_no,
            snapshot,
        } = stored;

        match catch_unwind(AssertUnwindSafe(|| {
            decode_versioned::<A::Snapshot, C>(
                &persistence.codec,
                &snapshot.manifest,
                snapshot.schema_version,
                &snapshot.payload,
            )
        })) {
            Ok(Ok(snapshot)) => Some((snapshot, next_seq_no)),

            Ok(Err(error)) => {
                dispose_error(actor_id, error, |error| {
                    warn!(
                        %actor_id,
                        %error,
                        source = error.source(),
                        "snapshot discarded, replaying in full"
                    );
                });
                None
            }

            Err(panic) => {
                warn!(
                    %actor_id,
                    panic = %PanicPayload(panic.as_ref()),
                    "snapshot discarded, replaying in full"
                );
                None
            }
        }
    });

    let (mut state, mut next_seq_no) = match decoded {
        Some((snapshot, next_seq_no)) => {
            let state = catch_and_log(actor_id, FAILED_TO_RECOVER, || {
                actor.init_from_snapshot(snapshot)
            })?;

            (state, next_seq_no)
        }

        None => {
            let state = catch_and_log(actor_id, FAILED_TO_RECOVER, || actor.init())?;

            (state, SeqNo::ZERO)
        }
    };

    loop {
        let page = catch_panic_and_log_async(
            actor_id,
            FAILED_TO_RECOVER,
            read_page(&persistence.event_store, &id, next_seq_no),
        )
        .await;
        let Some(page) = log_failure(actor_id, FAILED_TO_RECOVER, page) else {
            drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
            return None;
        };
        let page_len = page.len();

        (state, next_seq_no) = catch_and_log(actor_id, FAILED_TO_RECOVER, || {
            replay_page(actor, &persistence.codec, state, next_seq_no, page)
        })?;

        if page_len < REPLAY_PAGE.get() {
            break;
        }
    }

    Some(Recovered {
        id,
        state,
        next_seq_no,
    })
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn settle_or_log_failure<A, E, S, C>(
    actor_id: ActorId,
    actor: &A,
    persistence: &Persistence<E, S, C>,
    id: &PersistenceId,
    mut state: A::State,
    mut next_seq_no: SeqNo,
    effect: Effect<A>,
) -> Option<Settled<A::State>>
where
    A: EventSourced,
    E: EventStore,
    S: SnapshotStore,
    C: Codec,
{
    let Effect {
        events,
        stop,
        thens,
    } = effect;
    let appended = !events.is_empty();

    if appended {
        let encoded = catch_and_log(actor_id, "actor failed to encode events", || {
            encode_events::<A, C>(&persistence.codec, &events)
        });
        let Some(encoded) = encoded else {
            drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, (state, events, thens));
            return None;
        };

        let appended_events = catch_panic_and_log_async(
            actor_id,
            FAILED_TO_APPEND,
            append_events(&persistence.event_store, id, next_seq_no, encoded),
        )
        .await;
        if log_failure(actor_id, FAILED_TO_APPEND, appended_events).is_none() {
            drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, (state, events, thens));
            return None;
        }

        let appended_count = events.len();
        let applied = catch_panic_and_log(actor_id, "actor failed to apply events", || {
            apply_events(actor, state, events)
        });
        let Some(applied) = applied else {
            drop_containing_panic(actor_id, VALUES_FAILED_TO_DROP, thens);
            return None;
        };
        state = applied;
        next_seq_no = next_seq_no.advanced_by(appended_count);
    }

    let ran = catch_panic_and_log(actor_id, "actor continuation failed", || {
        for then in thens {
            then(&state);
        }
    });
    if ran.is_none() {
        drop_containing_panic(actor_id, STATE_FAILED_TO_DROP, state);
        return None;
    }

    if appended {
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            encode_snapshot(actor, &persistence.codec, &state)
        }));

        if let Some(Some(snapshot)) = warn_unless_snapshot_saved(actor_id, encoded) {
            let saved = catch_unwind_async(save_snapshot(
                &persistence.snapshot_store,
                id,
                next_seq_no,
                snapshot,
            ))
            .await;
            warn_unless_snapshot_saved(actor_id, saved);
        }
    }

    Some(Settled {
        state,
        next_seq_no,
        stop,
    })
}

fn warn_unless_snapshot_saved<T, E>(
    actor_id: ActorId,
    result: Result<Result<T, E>, Box<dyn Any + Send>>,
) -> Option<T>
where
    E: Error,
{
    match result {
        Ok(Ok(value)) => Some(value),

        Ok(Err(error)) => {
            dispose_error(actor_id, error, |error| {
                warn!(%actor_id, %error, source = error.source(), "{SNAPSHOT_NOT_SAVED}");
            });
            None
        }

        Err(panic) => {
            warn!(%actor_id, panic = %PanicPayload(panic.as_ref()), "{SNAPSHOT_NOT_SAVED}");
            None
        }
    }
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn read_page<E>(
    event_store: &E,
    id: &PersistenceId,
    from_seq_no: SeqNo,
) -> Result<Vec<StoredEvent>, E::Error>
where
    E: EventStore,
{
    event_store.read(id, from_seq_no, REPLAY_PAGE).await
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn replay_page<A, C>(
    actor: &A,
    codec: &C,
    state: A::State,
    next_seq_no: SeqNo,
    page: Vec<StoredEvent>,
) -> Result<(A::State, SeqNo), ReplayError>
where
    A: EventSourced,
    C: Codec,
{
    page.into_iter()
        .try_fold((state, next_seq_no), |(state, next_seq_no), stored| {
            if stored.seq_no != next_seq_no {
                return Err(ReplayError::Gap {
                    seq_no: stored.seq_no,
                    expected: next_seq_no,
                });
            }

            let event = decode_versioned::<A::Event, C>(
                codec,
                &stored.event.manifest,
                stored.event.schema_version,
                &stored.event.payload,
            )?;

            Ok((actor.apply(state, event), next_seq_no.succ()))
        })
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn encode_events<A, C>(codec: &C, events: &[A::Event]) -> Result<Vec<EncodedEvent>, EncodeError>
where
    A: EventSourced,
    C: Codec,
{
    events
        .iter()
        .map(|event| {
            let payload = codec.encode(event)?;

            Ok(EncodedEvent {
                manifest: Cow::Borrowed(A::Event::MANIFEST),
                schema_version: A::Event::VERSION,
                payload,
            })
        })
        .collect()
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn append_events<E>(
    event_store: &E,
    id: &PersistenceId,
    next_seq_no: SeqNo,
    events: Vec<EncodedEvent>,
) -> Result<(), AppendError<E::Error>>
where
    E: EventStore,
{
    event_store.append(id, next_seq_no, events).await
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) fn apply_events<A>(actor: &A, state: A::State, events: Vec<A::Event>) -> A::State
where
    A: EventSourced,
{
    events
        .into_iter()
        .fold(state, |state, event| actor.apply(state, event))
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn encode_snapshot<A, C>(
    actor: &A,
    codec: &C,
    state: &A::State,
) -> Result<Option<EncodedSnapshot>, SnapshotError<A::Error>>
where
    A: EventSourced,
    C: Codec,
{
    let Some(snapshot) = actor.snapshot(state).map_err(SnapshotError::Actor)? else {
        return Ok(None);
    };
    let payload = codec.encode(&snapshot)?;

    Ok(Some(EncodedSnapshot {
        manifest: Cow::Borrowed(A::Snapshot::MANIFEST),
        schema_version: A::Snapshot::VERSION,
        payload,
    }))
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn save_snapshot<S>(
    snapshot_store: &S,
    id: &PersistenceId,
    next_seq_no: SeqNo,
    snapshot: EncodedSnapshot,
) -> Result<(), S::Error>
where
    S: SnapshotStore,
{
    snapshot_store.save(id, next_seq_no, snapshot).await
}
