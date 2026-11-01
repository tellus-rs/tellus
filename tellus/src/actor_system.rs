use crate::{
    Actor, ActorConfig, ActorId, ActorRef,
    actor_context::spawn,
    sync::lock,
    watch::{ActorTerminated, TerminatedHandler, Watcher},
};
use derive_more::Debug;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::{oneshot, watch};

/// An actor system, hosting the tree of actors below its root actor.
///
/// Dropping an actor system does not stop its actors: the root actor stops on its own terms and
/// the tree keeps running detached. What is lost is the ability to call [ActorSystem::stop] and
/// [ActorSystem::root] and to await [ActorSystem::terminated].
#[must_use = "dropping an actor system does not stop its actors"]
#[derive(Debug)]
pub struct ActorSystem<M> {
    root: ActorRef<M>,

    #[debug(skip)]
    stop_root_tx: Arc<watch::Sender<()>>,

    #[debug(skip)]
    terminated_rx: oneshot::Receiver<()>,
}

impl<M> ActorSystem<M>
where
    M: Send + 'static,
{
    /// Create an actor system by giving the [Actor] for the root actor, using the default
    /// [ActorConfig].
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn new<A>(actor: A) -> Self
    where
        A: Actor<Message = M> + Send + 'static,
        A::State: Send + 'static,
    {
        Self::with_config(actor, ActorConfig::default())
    }

    /// Create an actor system by giving the [Actor] and [ActorConfig] for the root actor.
    ///
    /// # Panics
    /// Panics if called outside of a Tokio runtime.
    pub fn with_config<A>(actor: A, config: ActorConfig) -> Self
    where
        A: Actor<Message = M> + Send + 'static,
        A::State: Send + 'static,
    {
        let (root, stop_root_tx, terminated_rx) = spawn_root(actor, config);

        Self::from_parts(root, stop_root_tx, terminated_rx)
    }

    /// The reference for the root actor.
    pub fn root(&self) -> &ActorRef<M> {
        &self.root
    }

    /// Stop the root actor, and with it the whole tree, children first.
    ///
    /// The root stops between messages, never inside one, exactly as it would if a parent
    /// stopped it: the message it is handling is finished first, and for an event-sourced actor
    /// so is its settlement. Await [ActorSystem::terminated] for the tree to be gone.
    pub fn stop(&self) {
        self.stop_root_tx.send_replace(());
    }

    /// Wait until the root actor and all its descendants have terminated.
    ///
    /// # Errors
    /// Fails if the root's termination cannot be observed, which no ordinary termination causes.
    pub async fn terminated(self) -> Result<(), TerminatedError> {
        self.terminated_rx.await?;
        Ok(())
    }

    pub(crate) fn from_parts(
        root: ActorRef<M>,
        stop_root_tx: Arc<watch::Sender<()>>,
        terminated_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            root,
            stop_root_tx,
            terminated_rx,
        }
    }
}

/// Errors possibly returned by [ActorSystem::terminated].
#[derive(Debug, Error)]
pub enum TerminatedError {
    /// Watching the root actor failed unexpectedly.
    #[error("root watch failed unexpectedly")]
    WatchRoot(#[from] oneshot::error::RecvError),
}

pub(crate) fn watch_root<M>(
    root: &ActorRef<M>,
    stop_root_tx: Arc<watch::Sender<()>>,
) -> oneshot::Receiver<()> {
    let (terminated_tx, terminated_rx) = oneshot::channel();

    let handler = Arc::new(RootTerminatedHandler {
        terminated_tx: Mutex::new(Some(terminated_tx)),
        _stop_root_tx: stop_root_tx,
    });
    let registration = root
        .watcher_registry()
        .expect("the root actor is local")
        .add(Watcher::new(ActorId::new(), handler.clone()));
    if registration.is_err() {
        handler
            .handle_terminated(root.actor_id())
            .expect("a handler whose registration failed was never signaled");
    }

    terminated_rx
}

/// `_stop_root_tx` keeps the root actor running: living in the root's own watcher registry, it is
/// dropped only once termination has signaled the watchers. [ActorSystem] holds the other
/// reference, hence dropping a system stops nothing while [ActorSystem::stop] can still send.
struct RootTerminatedHandler {
    terminated_tx: Mutex<Option<oneshot::Sender<()>>>,
    _stop_root_tx: Arc<watch::Sender<()>>,
}

impl TerminatedHandler for RootTerminatedHandler {
    fn handle_terminated(&self, _actor_id: ActorId) -> Result<(), ActorTerminated> {
        let terminated_tx = lock(&self.terminated_tx).take().ok_or(ActorTerminated)?;
        let _ = terminated_tx.send(());

        Ok(())
    }
}

fn spawn_root<M, A>(
    root_actor: A,
    config: ActorConfig,
) -> (ActorRef<M>, Arc<watch::Sender<()>>, oneshot::Receiver<()>)
where
    M: Send + 'static,
    A: Actor<Message = M> + Send + 'static,
    A::State: Send + 'static,
{
    let (stop_root_tx, stopped_by_parent_rx) = watch::channel(());
    let stop_root_tx = Arc::new(stop_root_tx);

    let root = spawn(stopped_by_parent_rx, root_actor, config);
    let terminated_rx = watch_root(&root, stop_root_tx.clone());

    (root, stop_root_tx, terminated_rx)
}
