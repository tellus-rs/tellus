use crate::{
    ActorId, ActorRef, SchemaVersion,
    cluster::{
        codec::{Codec, CodecError},
        discovery::WireKey,
        receptionist::{
            MAX_REGISTRATIONS, MAX_SNAPSHOT_BYTES, Registration, WireGroup, registration_len,
        },
        schema::Schema,
    },
    sync::{lock, read, write},
    watch::{ActorTerminated, TerminatedHandler, Watcher, WatcherRegistry},
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, hash_map::Entry},
    sync::{Arc, Mutex, RwLock, Weak},
};
use thiserror::Error;
use tokio::sync::watch;

/// Names are a second keyspace over the same bindings, so both are evicted together; the routes'
/// [RwLock] is read by every inbound delivery and written only by binding and eviction.
pub(crate) struct Registry {
    routes: RwLock<HashMap<ActorId, Arc<dyn InboundRoute>>>,
    names: Mutex<Names>,
    changes: watch::Sender<u64>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::new()),
            names: Mutex::new(Names::new()),
            changes: watch::Sender::new(0),
        }
    }

    /// Idempotent. Release the routes lock before registering the evictor, which retakes it.
    pub(crate) fn bind<M>(self: &Arc<Self>, actor_ref: &ActorRef<M>)
    where
        M: Schema + Send + 'static,
    {
        let id = actor_ref.actor_id();

        if read(&self.routes).contains_key(&id) {
            return;
        }

        let bound = match write(&self.routes).entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Route {
                    actor_ref: actor_ref.clone(),
                }));
                true
            }

            Entry::Occupied(_) => false,
        };
        if !bound {
            return;
        }

        let evictor = Watcher::new(
            ActorId::new(),
            Arc::new(Evictor {
                registry: Arc::downgrade(self),
            }),
        );
        let watcher_registry = actor_ref
            .watcher_registry()
            .expect("bound reference is local");
        if watcher_registry.add(evictor).is_err() {
            self.unbind(id);
        }
    }

    pub(crate) fn register<M>(
        self: &Arc<Self>,
        key: WireKey,
        actor_ref: &ActorRef<M>,
    ) -> Result<Registered, LimitExceeded>
    where
        M: Schema + Send + 'static,
    {
        let id = actor_ref.actor_id();
        if actor_ref
            .watcher_registry()
            .is_some_and(WatcherRegistry::is_closed)
        {
            return Ok(Registered::Unchanged);
        }

        let added = {
            let mut names = lock(&self.names);
            if !names.contains(&key, id) {
                if names.len >= MAX_REGISTRATIONS {
                    return Err(LimitExceeded::Registrations);
                }
                if names.bytes.saturating_add(registration_len(&key)) > MAX_SNAPSHOT_BYTES {
                    return Err(LimitExceeded::Bytes);
                }
            }
            names.add(key.clone(), id).then_some(names.version)
        };
        self.bind(actor_ref);
        self.publish();

        let Some(version) = added else {
            return Ok(Registered::Unchanged);
        };
        if lock(&self.names).version != version {
            return Ok(Registered::Changed);
        }
        Ok(Registered::Added {
            version,
            registration: Registration { key, id },
        })
    }

    pub(crate) fn registrations(&self) -> Registrations {
        let names = lock(&self.names);
        Registrations {
            version: names.version,
            groups: names.groups(),
        }
    }

    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(crate) fn deliver(
        &self,
        target: ActorId,
        schema_version: SchemaVersion,
        payload: &[u8],
        codec: &dyn Codec,
    ) -> Result<(), DeliverError> {
        let route = read(&self.routes).get(&target).cloned();

        route
            .ok_or(DeliverError::UnknownTarget)?
            .deliver(schema_version, payload, codec)?;

        Ok(())
    }

    pub(crate) fn local_ref<M>(&self, id: ActorId) -> Result<ActorRef<M>, LocalRefError>
    where
        M: Send + 'static,
    {
        let route = read(&self.routes)
            .get(&id)
            .cloned()
            .ok_or(LocalRefError::Unbound)?;

        route
            .as_any()
            .downcast_ref::<Route<M>>()
            .map(|route| route.actor_ref.clone())
            .ok_or(LocalRefError::MessageType)
    }

    pub(crate) fn watcher_registry(&self, id: ActorId) -> Option<WatcherRegistry> {
        let route = read(&self.routes).get(&id).cloned()?;
        Some(route.watcher_registry())
    }

    /// The two locks are taken one after the other, never nested, else they could deadlock.
    pub(crate) fn unbind(&self, id: ActorId) {
        write(&self.routes).remove(&id);
        if lock(&self.names).remove(id) {
            self.publish();
        }
    }

    fn publish(&self) {
        let version = lock(&self.names).version;
        self.changes.send_if_modified(|published| {
            let modified = *published != version;
            *published = version;
            modified
        });
    }
}

#[derive(Debug, Error)]
pub(crate) enum LimitExceeded {
    #[error("this node already holds {MAX_REGISTRATIONS} registrations")]
    Registrations,

    #[error("this node's registrations would exceed {MAX_SNAPSHOT_BYTES} bytes")]
    Bytes,
}

pub(crate) enum Registered {
    Unchanged,
    Added {
        version: u64,
        registration: Registration,
    },
    Changed,
}

#[derive(Debug, Error)]
pub(crate) enum DeliverError {
    #[error("unknown target actor")]
    UnknownTarget,

    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[derive(Debug, Error)]
pub(crate) enum LocalRefError {
    #[error("no actor bound under this ID")]
    Unbound,

    #[error("actor is bound for a different message type")]
    MessageType,
}

pub(crate) struct Registrations {
    pub(crate) version: u64,
    pub(crate) groups: Vec<WireGroup>,
}

/// Keyed both ways: by key to list the registrations, by actor to evict without a scan.
struct Names {
    by_key: BTreeMap<WireKey, BTreeSet<ActorId>>,
    by_id: HashMap<ActorId, HashSet<WireKey>>,
    len: usize,
    bytes: usize,
    version: u64,
}

impl Names {
    fn new() -> Self {
        Self {
            by_key: BTreeMap::new(),
            by_id: HashMap::new(),
            len: 0,
            bytes: 0,
            version: 0,
        }
    }

    fn contains(&self, key: &WireKey, id: ActorId) -> bool {
        self.by_key
            .get(key)
            .is_some_and(|registered| registered.contains(&id))
    }

    fn add(&mut self, key: WireKey, id: ActorId) -> bool {
        let added = self.by_key.entry(key.clone()).or_default().insert(id);
        if added {
            self.len += 1;
            self.bytes += registration_len(&key);
            self.version += 1;
        }
        self.by_id.entry(id).or_default().insert(key);
        added
    }

    fn groups(&self) -> Vec<WireGroup> {
        self.by_key
            .iter()
            .map(|(key, ids)| WireGroup {
                key: key.clone(),
                ids: ids.iter().copied().collect(),
            })
            .collect()
    }

    fn remove(&mut self, id: ActorId) -> bool {
        let Some(keys) = self.by_id.remove(&id) else {
            return false;
        };
        self.version += 1;

        for key in keys {
            let Some(registered) = self.by_key.get_mut(&key) else {
                continue;
            };

            if registered.remove(&id) {
                self.len -= 1;
                self.bytes -= registration_len(&key);
            }
            if registered.is_empty() {
                self.by_key.remove(&key);
            }
        }
        true
    }
}

trait InboundRoute
where
    Self: Send + Sync,
{
    fn deliver(
        &self,
        schema_version: SchemaVersion,
        payload: &[u8],
        codec: &dyn Codec,
    ) -> Result<(), CodecError>;

    fn watcher_registry(&self) -> WatcherRegistry;

    fn as_any(&self) -> &dyn Any;
}

struct Route<M> {
    actor_ref: ActorRef<M>,
}

impl<M> InboundRoute for Route<M>
where
    M: Schema + Send + 'static,
{
    fn deliver(
        &self,
        schema_version: SchemaVersion,
        payload: &[u8],
        codec: &dyn Codec,
    ) -> Result<(), CodecError> {
        let message = M::decode(codec, schema_version, payload)?;
        self.actor_ref.tell(message);
        Ok(())
    }

    fn watcher_registry(&self) -> WatcherRegistry {
        self.actor_ref
            .watcher_registry()
            .expect("route target is local")
            .clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Registered on the very actor it is signaled about, hence the signaled ID is the one to unbind.
struct Evictor {
    registry: Weak<Registry>,
}

impl TerminatedHandler for Evictor {
    fn handle_terminated(&self, actor_id: ActorId) -> Result<(), ActorTerminated> {
        if let Some(registry) = self.registry.upgrade() {
            registry.unbind(actor_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity, SchemaVersion,
        actor_ref::SelfRef,
        cluster::{
            codec::{Codec, CodecError, Postcard},
            discovery::{KeyKind, WireKey},
            manifest::Manifest,
            receptionist::{MAX_REGISTRATIONS, MAX_SNAPSHOT_BYTES, WireGroup},
            registry::{DeliverError, LimitExceeded, LocalRefError, Registry},
            schema::{Schema, UnsupportedSchemaVersion},
        },
        sync::lock,
    };
    use serde::{Deserialize, Serialize};
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn key<M>(name: &str) -> WireKey
    where
        M: Manifest,
    {
        WireKey::new::<M>(KeyKind::Discovery, name)
    }

    /// Binding an actor which has already terminated reverts the binding, which takes the
    /// registry lock again: the bind path must have released it, else the endpoint deadlocks.
    /// Run on its own thread, so a regression fails here instead of hanging the whole suite.
    #[test]
    fn binding_a_terminated_actor_does_not_deadlock() {
        let (done_tx, done_rx) = mpsc::channel();

        thread::spawn(move || {
            let registry = Arc::new(Registry::new());

            let id = ActorId::new();
            let (self_ref, mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
            let actor_ref = self_ref.actor_ref().clone();

            mailbox.split().1.close();

            registry.bind(&actor_ref);
            let _ = done_tx.send(matches!(
                registry.local_ref::<u32>(id),
                Err(LocalRefError::Unbound)
            ));
        });

        match done_rx.recv_timeout(TIMEOUT) {
            Ok(reverted) => assert!(reverted, "binding a terminated actor was not reverted"),
            Err(_) => panic!("binding a terminated actor deadlocked"),
        }
    }

    /// Binding twice registers once and keeps the first route, so a second serialization of the
    /// same reference does not replace a live route.
    #[test]
    fn binding_twice_is_idempotent() {
        let registry = Arc::new(Registry::new());

        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        let actor_ref = self_ref.actor_ref().clone();

        registry.bind(&actor_ref);
        registry.bind(&actor_ref);

        assert!(registry.local_ref::<u32>(id).is_ok());
        assert_eq!(mailbox.split().1.close().len(), 1);
    }

    /// A bound reference is only handed back for the message type it was bound with: the downcast
    /// is what keeps a type mismatch from delivering garbage to an actor.
    #[test]
    fn local_ref_requires_the_bound_message_type() {
        let registry = Arc::new(Registry::new());

        let id = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        let actor_ref = self_ref.actor_ref().clone();

        registry.bind(&actor_ref);

        assert!(registry.local_ref::<u32>(id).is_ok());
        assert!(matches!(
            registry.local_ref::<String>(id),
            Err(LocalRefError::MessageType)
        ));
        assert!(matches!(
            registry.local_ref::<u32>(ActorId::new()),
            Err(LocalRefError::Unbound)
        ));
    }

    /// Every observable change of the names bumps the version once: a repeated registration of
    /// one actor under one name and the unbinding of an unnamed actor change nothing.
    #[test]
    fn the_version_counts_name_changes_only() {
        let registry = Arc::new(Registry::new());
        let mut changes = registry.changes();

        let id = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");
        assert_eq!(*changes.borrow_and_update(), 1);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");
        assert!(!changes.has_changed().expect("sender alive"));
        registry
            .register(key::<u32>("other"), self_ref.actor_ref())
            .expect("registers");
        assert_eq!(*changes.borrow_and_update(), 2);

        let unnamed = ActorId::new();
        let (unnamed_ref, _mailbox) = SelfRef::<u32>::new(unnamed, MailboxCapacity::Unbounded);
        registry.bind(unnamed_ref.actor_ref());
        registry.unbind(unnamed);
        assert!(!changes.has_changed().expect("sender alive"));

        registry.unbind(id);
        assert_eq!(*changes.borrow_and_update(), 3);
    }

    #[test]
    fn registering_a_terminated_actor_is_a_no_op() {
        let registry = Arc::new(Registry::new());
        let changes = registry.changes();

        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        let (_incoming_rx, watcher_registration) = mailbox.split();
        drop(watcher_registration.close());

        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");

        assert!(!changes.has_changed().expect("sender alive"));
        assert!(registry.registrations().groups.is_empty());
    }

    #[test]
    fn registrations_are_capped_per_node() {
        let registry = Arc::new(Registry::new());
        let id = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");
        lock(&registry.names).len = MAX_REGISTRATIONS;

        assert!(matches!(
            registry.register(key::<u32>("other"), self_ref.actor_ref()),
            Err(LimitExceeded::Registrations)
        ));
        assert!(
            registry
                .register(key::<u32>("pool"), self_ref.actor_ref())
                .is_ok()
        );
    }

    /// The bytes are capped like the count, and a registration already held passes the cap.
    #[test]
    fn registration_bytes_are_capped_per_node() {
        let registry = Arc::new(Registry::new());
        let id = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");
        lock(&registry.names).bytes = MAX_SNAPSHOT_BYTES;

        assert!(matches!(
            registry.register(key::<u32>("other"), self_ref.actor_ref()),
            Err(LimitExceeded::Bytes)
        ));
        assert!(
            registry
                .register(key::<u32>("pool"), self_ref.actor_ref())
                .is_ok()
        );
    }

    /// The bytes follow the registrations both ways, so an eviction frees what it took.
    #[test]
    fn registration_bytes_follow_the_registrations() {
        let registry = Arc::new(Registry::new());
        let id = ActorId::new();
        let (self_ref, _mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");
        registry
            .register(key::<u32>("other"), self_ref.actor_ref())
            .expect("registers");
        assert!(lock(&registry.names).bytes > 0);

        registry.unbind(id);

        assert_eq!(lock(&registry.names).bytes, 0);
    }

    /// The snapshot lists every key once, with every actor registered under it, keys and actors
    /// in order.
    #[test]
    fn registrations_group_every_actor_under_its_key() {
        let registry = Arc::new(Registry::new());
        let mut ids = [ActorId::new(), ActorId::new(), ActorId::new()];
        ids.sort_unstable();
        let [first, second, third] = ids;
        let (first_ref, _mailbox) = SelfRef::<u32>::new(first, MailboxCapacity::Unbounded);
        let (second_ref, _mailbox) = SelfRef::<u64>::new(second, MailboxCapacity::Unbounded);
        let (third_ref, _mailbox) = SelfRef::<u32>::new(third, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), third_ref.actor_ref())
            .expect("registers");
        registry
            .register(key::<u32>("pool"), first_ref.actor_ref())
            .expect("registers");
        registry
            .register(key::<u32>("other"), first_ref.actor_ref())
            .expect("registers");
        registry
            .register(key::<u64>("pool"), second_ref.actor_ref())
            .expect("registers");
        registry
            .register(
                WireKey::new::<u32>(KeyKind::Topic, "pool"),
                first_ref.actor_ref(),
            )
            .expect("registers");

        let registrations = registry.registrations();

        assert_eq!(registrations.version, 5);
        let mut expected = vec![
            WireGroup {
                key: key::<u32>("pool"),
                ids: vec![first, third],
            },
            WireGroup {
                key: key::<u32>("other"),
                ids: vec![first],
            },
            WireGroup {
                key: key::<u64>("pool"),
                ids: vec![second],
            },
            WireGroup {
                key: WireKey::new::<u32>(KeyKind::Topic, "pool"),
                ids: vec![first],
            },
        ];
        expected.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(registrations.groups, expected);
    }

    /// Termination evicts through the watcher mechanism itself: taking the watchers and signaling
    /// them, as the run loop does, must remove both the route and the name.
    #[test]
    fn termination_evicts_route_and_name() {
        let registry = Arc::new(Registry::new());

        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<u32>::new(id, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), self_ref.actor_ref())
            .expect("registers");

        for watcher in mailbox.split().1.close() {
            watcher
                .handle_terminated(id)
                .expect("the evictor accepts the signal");
        }

        assert!(matches!(
            registry.local_ref::<u32>(id),
            Err(LocalRefError::Unbound)
        ));
        assert!(registry.registrations().groups.is_empty());
    }

    /// More than one actor may answer to a name, and losing one leaves the others discoverable:
    /// the name keyspace already holds several actors under one name. That is the input a
    /// receptionist listing would read.
    #[test]
    fn a_name_takes_more_than_one_actor() {
        let registry = Arc::new(Registry::new());

        let (first, second) = (ActorId::new(), ActorId::new());
        let (first_ref, _first_mailbox) = SelfRef::<u32>::new(first, MailboxCapacity::Unbounded);
        let (second_ref, _second_mailbox) = SelfRef::<u32>::new(second, MailboxCapacity::Unbounded);
        registry
            .register(key::<u32>("pool"), first_ref.actor_ref())
            .expect("registers");
        registry
            .register(key::<u32>("pool"), second_ref.actor_ref())
            .expect("registers");

        registry.unbind(first);

        assert_eq!(
            registry.registrations().groups,
            vec![WireGroup {
                key: key::<u32>("pool"),
                ids: vec![second],
            }]
        );
    }

    /// A payload is decoded with the schema version it carries: one the route's type upcasts is
    /// delivered, one it does not decode is a decoding failure whose source names the type and
    /// both versions, and nothing of it reaches the actor.
    #[test]
    fn a_route_decodes_the_schema_versions_its_type_declares() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Upcast(u64);

        impl Schema for Upcast {
            const VERSION: SchemaVersion = SchemaVersion::new(1);

            fn decode(
                codec: &dyn Codec,
                schema_version: SchemaVersion,
                payload: &[u8],
            ) -> Result<Self, CodecError> {
                if schema_version == SchemaVersion::new(0) {
                    let narrow = codec.decode_to::<u32>(payload)?;
                    return Ok(Upcast(u64::from(narrow)));
                }
                if schema_version == Self::VERSION {
                    return codec.decode_to(payload);
                }
                Err(CodecError::decoding(UnsupportedSchemaVersion::new::<Self>(
                    schema_version,
                )))
            }
        }

        let registry = Arc::new(Registry::new());
        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<Upcast>::new(id, MailboxCapacity::Unbounded);
        registry.bind(self_ref.actor_ref());
        let codec: &dyn Codec = &Postcard;

        registry
            .deliver(
                id,
                SchemaVersion::new(0),
                &codec.encode(&7_u32).expect("encodes"),
                codec,
            )
            .expect("version 0 is upcast");
        registry
            .deliver(
                id,
                SchemaVersion::new(1),
                &codec.encode(&Upcast(8)).expect("encodes"),
                codec,
            )
            .expect("version 1 is current");
        let error = registry
            .deliver(id, SchemaVersion::new(2), &[], codec)
            .expect_err("version 2 is not decodable");

        let DeliverError::Codec(CodecError::Decode(source)) = &error else {
            panic!("not a decoding failure: {error:?}");
        };
        let source = source.to_string();
        assert!(
            source.contains("unsupported schema version 2 of ")
                && source.ends_with("Upcast, the current version is 1"),
            "{source}"
        );
        let received = mailbox
            .split()
            .0
            .drain()
            .filter_map(|incoming| match incoming {
                Incoming::Message(message) => Some(message),
                Incoming::Terminated(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(received, [Upcast(7), Upcast(8)]);
    }
}
