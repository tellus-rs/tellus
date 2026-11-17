use crate::{ActorId, sync::lock};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// Shared by the watched actor and by every watcher, which keeps a clone to deregister itself at
/// termination, with no reference to the watched actor left.
#[derive(Clone)]
pub(crate) struct WatcherRegistry(Arc<Mutex<Option<HashMap<ActorId, Watcher>>>>);

impl WatcherRegistry {
    pub(crate) fn new() -> (Self, WatcherRegistration) {
        let registry = Self(Arc::new(Mutex::new(Some(HashMap::new()))));

        (registry.clone(), WatcherRegistration(registry))
    }

    pub(crate) fn add(&self, watcher: Watcher) -> Result<(), ActorTerminated> {
        let mut registry = lock(&self.0);
        let watchers = registry.as_mut().ok_or(ActorTerminated)?;
        watchers.entry(watcher.watcher_id()).or_insert(watcher);

        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.0).is_none()
    }

    pub(crate) fn remove(&self, watcher_id: ActorId) {
        if let Some(watchers) = lock(&self.0).as_mut() {
            watchers.remove(&watcher_id);
        }
    }

    fn take(&self) -> Vec<Watcher> {
        lock(&self.0)
            .take()
            .map(|watchers| watchers.into_values().collect())
            .unwrap_or_default()
    }
}

pub(crate) struct WatcherRegistration(WatcherRegistry);

impl WatcherRegistration {
    pub(crate) fn close(self) -> Vec<Watcher> {
        self.0.take()
    }
}

pub(crate) struct Watcher {
    watcher_id: ActorId,
    terminated_handler: Arc<dyn TerminatedHandler>,
}

impl Watcher {
    pub(crate) fn new(watcher_id: ActorId, terminated_handler: Arc<dyn TerminatedHandler>) -> Self {
        Self {
            watcher_id,
            terminated_handler,
        }
    }

    pub(crate) fn watcher_id(&self) -> ActorId {
        self.watcher_id
    }

    pub(crate) fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        self.terminated_handler.handle_terminated(actor_id)
    }
}

pub(crate) trait TerminatedHandler
where
    Self: Send + Sync,
{
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated>;
}

#[derive(Debug, Error)]
#[error("actor terminated")]
pub(crate) struct ActorTerminated;

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        watch::{ActorTerminated, TerminatedHandler, Watcher, WatcherRegistry},
    };
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };

    /// Registering the same watcher twice signals once: a terminated signal only names the
    /// terminated actor, hence a second one would carry nothing.
    #[test]
    fn adding_a_watcher_twice_registers_once() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        for _ in 0..3 {
            assert!(registry.add(watcher(watcher_id)).is_ok());
        }

        assert_eq!(watchers.close().len(), 1);
    }

    /// Removing a watcher deregisters it, so no terminated signal is sent to it and no reference
    /// to it is held anymore.
    #[test]
    fn removing_a_watcher_deregisters_it() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        assert!(registry.add(watcher(watcher_id)).is_ok());
        registry.remove(watcher_id);

        assert!(watchers.close().is_empty());
    }

    /// Removing after registration has been closed has no effect, in particular it must not
    /// reopen registration.
    #[test]
    fn removing_after_take_is_a_noop() {
        let (registry, watchers) = WatcherRegistry::new();

        let watcher_id = ActorId::new();
        assert!(registry.add(watcher(watcher_id)).is_ok());
        assert_eq!(watchers.close().len(), 1);

        registry.remove(watcher_id);

        assert!(registry.add(watcher(watcher_id)).is_err());
    }

    /// A panic while the registry's lock is held, here in the destructor of a removed watcher's
    /// handler, poisons the lock but does not disable the registry: adding and closing still work.
    #[test]
    fn a_panic_under_the_lock_does_not_disable_the_registry() {
        let (registry, watchers) = WatcherRegistry::new();

        let panicking_id = ActorId::new();
        assert!(
            registry
                .add(Watcher::new(panicking_id, Arc::new(PanicOnDrop)))
                .is_ok()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| registry.remove(panicking_id))).is_err());

        let watcher_id = ActorId::new();
        assert!(registry.add(watcher(watcher_id)).is_ok());
        assert_eq!(watchers.close().len(), 1);
    }

    fn watcher(watcher_id: ActorId) -> Watcher {
        Watcher::new(watcher_id, Arc::new(Discard))
    }

    struct Discard;

    impl TerminatedHandler for Discard {
        fn handle_terminated(&self, _actor_id: ActorId) -> Result<(), ActorTerminated> {
            Ok(())
        }
    }

    struct PanicOnDrop;

    impl TerminatedHandler for PanicOnDrop {
        fn handle_terminated(&self, _actor_id: ActorId) -> Result<(), ActorTerminated> {
            Ok(())
        }
    }

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("the handler's destructor panics on purpose");
        }
    }
}
