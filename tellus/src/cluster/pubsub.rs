//! Publish-subscribe over the [receptionist](crate::cluster::receptionist): a [Topic] is a [Key]
//! under another name, [subscribe] registers a local actor under it and [publish] tells every
//! subscriber the receptionist resolves, see docs/cluster.md.

use crate::{
    ActorId, ActorRef, SchemaVersion,
    actor_ref::Sink,
    cluster::{
        codec::CodecError,
        discovery::{Key, RegisterError, register},
        endpoint::{self, EndpointInner},
        frame::Frame,
        manifest::Manifest,
        node::NodeId,
        receptionist::{ReceptionistError, resolve_key},
        reply,
        schema::Schema,
        sink::{self, RemoteSendError, admit_frame},
    },
};
use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
};

/// A topic, i.e. a [Key] under another name: [subscribe] registers under it, [publish] resolves
/// it. A topic and a discovery key of the same name are distinct keys, so the subscribers of a
/// topic are listed under its [key](Topic::key) only, never under [Key::new] of its name.
pub struct Topic<M> {
    key: Key<M>,
}

impl<M> Topic<M>
where
    M: Manifest + 'static,
{
    /// A topic under the given name.
    ///
    /// # Panics
    /// Panics in debug builds if another type of this process has claimed the same
    /// [Manifest::MANIFEST] as `M`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            key: Key::topic(name),
        }
    }

    /// The key the topic's subscribers are registered under, for
    /// [receptionist::lookup](crate::cluster::receptionist::lookup) and
    /// [receptionist::subscribe](crate::cluster::receptionist::subscribe) on the subscriber set.
    pub fn key(&self) -> &Key<M> {
        &self.key
    }
}

impl<M> Debug for Topic<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topic").field("key", &self.key).finish()
    }
}

// A derived `Clone` would needlessly require `M: Clone`.
impl<M> Clone for Topic<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
        }
    }
}

/// Subscribe an actor of this node to a topic by registering it under the topic's key: a
/// member's [publish] tells it once the registration has replicated there, not before.
///
/// There is no unsubscribe: a subscription lives as long as its actor, exactly like a
/// [registration](crate::cluster::register). An actor which leaves a topic while running
/// subscribes a child it can stop instead.
///
/// # Errors
/// Fails if the endpoint is not started, if the reference names an actor on another node, or if
/// the registration does not fit one frame. Fails as well if this node already holds the most
/// registrations or bytes of registrations one node may.
pub fn subscribe<M>(topic: &Topic<M>, actor_ref: &ActorRef<M>) -> Result<(), RegisterError>
where
    M: Manifest + Send + 'static,
{
    register(topic.key(), actor_ref)
}

/// Tell the message to every subscriber of the topic the receptionist currently resolves, this
/// node's own subscribers included, and answer how many were told.
///
/// This node's own subscribers are told first, each a clone of the message. The message is
/// encoded only if a subscriber lives on another node, and then once per such node, through
/// [Schema::encode] for that node. One frame carries it to all
/// subscribers of one node which share a stream toward it. That frame is split into several only
/// to stay within the maximum frame size. A reply destination is serialized only once: the node
/// whose encoding takes a [ReplyTo](crate::ReplyTo) gets it in a frame to its first subscriber
/// alone, every further subscriber of that node is told separately and gets a dead letter, and a
/// node whose encoding leaves the destination alone gets its multicast as usual.
///
/// [ActorRef::tell]'s contract holds per subscriber: a full outbound queue or an unreachable node
/// makes the message a dead letter for each subscriber its frame names. The answer counts the
/// subscribers told, not the messages delivered. A subscriber whose registration has not reached
/// this node yet is not told. [receptionist::settled] says whether every Up member has delivered
/// a first snapshot at all. A later registration still in transit is invisible to it and to this
/// answer alike.
///
/// # Errors
/// Fails if the endpoint is not started, or if no Up member has the topic subscribed for `M` but
/// one has it subscribed for another message type.
///
/// [ActorRef::tell]: crate::ActorRef::tell
/// [receptionist::settled]: crate::cluster::receptionist::settled
pub fn publish<M>(topic: &Topic<M>, message: M) -> Result<usize, ReceptionistError>
where
    M: Manifest + Clone + Send + 'static,
{
    let endpoint = endpoint::get().ok_or(ReceptionistError::EndpointNotStarted)?;
    publish_at(endpoint, topic, message)
}

fn publish_at<M>(
    endpoint: &EndpointInner,
    topic: &Topic<M>,
    message: M,
) -> Result<usize, ReceptionistError>
where
    M: Manifest + Clone + Send + 'static,
{
    let subscribers = resolve_key(endpoint, topic.key())?;
    let count = subscribers.len();

    let mut local = Vec::new();
    let mut remote = Vec::new();
    for subscriber in subscribers {
        match subscriber.sink() {
            Sink::Local(_) => local.push(subscriber),
            Sink::Remote(remote_sink) => remote.push((remote_sink.node(), subscriber)),
        }
    }

    if remote.is_empty() {
        tell_each(&local, message);
    } else {
        for subscriber in &local {
            subscriber.tell(message.clone());
        }
        multicast(endpoint, &remote, message);
    }
    Ok(count)
}

fn tell_each<'a, M, I>(subscribers: I, message: M)
where
    M: Clone + 'a,
    I: IntoIterator<Item = &'a ActorRef<M>>,
{
    let mut subscribers = subscribers.into_iter().peekable();
    while let Some(subscriber) = subscribers.next() {
        if subscribers.peek().is_none() {
            subscriber.tell(message);
            return;
        }
        subscriber.tell(message.clone());
    }
}

/// Encodes once per destination node, since the shape may differ per peer, and decides per
/// encoding: serializing a [ReplyTo](crate::ReplyTo) moves its destination away and mints reply
/// tags, which only a message frame carries.
fn multicast<M>(endpoint: &EndpointInner, subscribers: &[(NodeId, ActorRef<M>)], message: M)
where
    M: Clone + Schema + Send + 'static,
{
    for (node, node_subscribers) in node_groups(subscribers) {
        let peer = endpoint.peer(node);
        let (encoded, reply_tags) =
            reply::record_minted(|| message.encode(endpoint.codec(), &peer));

        if !reply_tags.is_empty() {
            let (first, rest) = node_subscribers
                .split_first()
                .expect("a group has a subscriber");
            let target = first.actor_id();
            let sent = sink::send_encoded(
                endpoint,
                node,
                encoded,
                reply_tags,
                |schema_version, payload, reply_tags| Frame::Message {
                    target,
                    reply_tags: reply_tags.to_vec(),
                    schema_version,
                    payload: Cow::Owned(payload),
                },
            );
            if let Err(error) = sent {
                first.dead_letter(&error);
            }
            tell_each(rest.iter().copied(), message.clone());
            continue;
        }

        match encoded {
            Ok((schema_version, payload)) => {
                for group in stream_groups(endpoint, &node_subscribers) {
                    send_multicast(endpoint, node, &group, schema_version, &payload);
                }
            }

            Err(error) => {
                for subscriber in node_subscribers {
                    subscriber.dead_letter(&error);
                }
            }
        }
    }
}

fn node_groups<M>(subscribers: &[(NodeId, ActorRef<M>)]) -> Vec<(NodeId, Vec<&ActorRef<M>>)> {
    let mut groups = Vec::<(NodeId, Vec<&ActorRef<M>>)>::new();
    for (node, subscriber) in subscribers {
        match groups.iter_mut().find(|(group_node, _)| group_node == node) {
            Some((_, group)) => group.push(subscriber),
            None => groups.push((*node, vec![subscriber])),
        }
    }
    groups
}

fn stream_groups<'a, M>(
    endpoint: &EndpointInner,
    subscribers: &[&'a ActorRef<M>],
) -> Vec<Vec<&'a ActorRef<M>>> {
    let mut groups = Vec::<(Option<usize>, Vec<&ActorRef<M>>)>::new();
    for subscriber in subscribers {
        let stream = endpoint.data_stream(subscriber.actor_id());
        match groups
            .iter_mut()
            .find(|(group_stream, _)| *group_stream == stream)
        {
            Some((_, group)) => group.push(subscriber),
            None => groups.push((stream, vec![subscriber])),
        }
    }
    groups.into_iter().map(|(_, group)| group).collect()
}

/// The frames must be sent in order, else they overtake each other on the group's stream.
fn send_multicast<M>(
    endpoint: &EndpointInner,
    node: NodeId,
    subscribers: &[&ActorRef<M>],
    schema_version: SchemaVersion,
    payload: &[u8],
) {
    let max_frame_size = endpoint.config().max_frame_size.get();
    let targets_per_frame = match targets_per_frame(schema_version, payload, max_frame_size) {
        Ok(targets) => targets.max(1),

        Err(error) => {
            let error = RemoteSendError::Codec(CodecError::encoding(error));
            for subscriber in subscribers {
                subscriber.dead_letter(&error);
            }
            return;
        }
    };

    for chunk in subscribers.chunks(targets_per_frame) {
        let frame = Frame::Multicast {
            targets: chunk
                .iter()
                .map(|subscriber| subscriber.actor_id())
                .collect(),
            schema_version,
            payload: Cow::Owned(payload.to_vec()),
        };
        let sent = admit_frame(frame, max_frame_size)
            .and_then(|frame| endpoint.send(node, frame).map_err(RemoteSendError::from));
        if let Err(error) = sent {
            for subscriber in chunk {
                subscriber.dead_letter(&error);
            }
        }
    }
}

/// The most targets a multicast of the payload holds within the maximum frame size, zero if not
/// even one fits.
fn targets_per_frame(
    schema_version: SchemaVersion,
    payload: &[u8],
    max_frame_size: usize,
) -> Result<usize, postcard::Error> {
    let len = |targets| {
        Frame::Multicast {
            targets,
            schema_version,
            payload: Cow::Borrowed(payload),
        }
        .encoded_len()
    };
    let empty = len(Vec::new())?;
    let per_target = len(vec![ActorId::new()])? - empty;
    let fixed = empty - varint_len(0);

    let mut targets = max_frame_size
        .saturating_sub(fixed)
        .checked_div(per_target)
        .unwrap_or(0);
    while targets > 0 && fixed + varint_len(targets) + targets * per_target > max_frame_size {
        targets -= 1;
    }
    Ok(targets)
}

/// Must match postcard's encoding of a sequence length.
fn varint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, Incoming, MailboxCapacity, ReplyTo, SchemaVersion,
        actor_ref::SelfRef,
        cluster::{
            codec::{Codec, CodecError},
            discovery::Key,
            endpoint::{self, EndpointConfig, EndpointInner},
            frame::Frame,
            manifest::Manifest,
            node::NodeId,
            peer::DialRequest,
            pubsub::{Topic, publish_at},
            receptionist::{ReceptionistError, WireGroup},
            reply,
            schema::{Peer, Schema},
            sink::RemoteSink,
        },
        mailbox::Mailbox,
        sync::lock,
    };
    use flume::Receiver;
    use semver::Version;
    use serde::{
        Deserialize, Deserializer, Serialize, Serializer, de,
        ser::{self, SerializeTuple},
    };
    use std::{
        cell::Cell,
        collections::{HashMap, HashSet},
        net::SocketAddr,
        num::NonZeroUsize,
        sync::{Arc, Mutex, mpsc},
    };

    thread_local! {
        static SERIALIZATIONS: Cell<usize> = const { Cell::new(0) };
    }

    /// Counts its serializations on this thread.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Counted(u64);

    impl Serialize for Counted {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            SERIALIZATIONS.set(SERIALIZATIONS.get() + 1);
            self.0.serialize(serializer)
        }
    }

    impl Schema for Counted {}

    impl Manifest for Counted {
        const MANIFEST: &'static str = "counted";
    }

    /// A user-written cloneable wrapper sharing one reply destination between its clones.
    #[derive(Clone)]
    struct SharedReply(Arc<Mutex<ReplyTo<u64>>>);

    impl SharedReply {
        fn new() -> (Self, mpsc::Receiver<u64>) {
            let (reply_tx, reply_rx) = mpsc::channel();
            let reply_to = ReplyTo::new(move |reply| {
                reply_tx.send(reply).expect("the test awaits the reply");
            });
            (Self(Arc::new(Mutex::new(reply_to))), reply_rx)
        }
    }

    impl Serialize for SharedReply {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            lock(&self.0).serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for SharedReply {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            Err(de::Error::custom("never deserialized"))
        }
    }

    impl Schema for SharedReply {}

    impl Manifest for SharedReply {
        const MANIFEST: &'static str = "shared-reply";
    }

    /// Serializes its reply destination and then fails.
    #[derive(Clone)]
    struct PoisonedReply(Arc<Mutex<ReplyTo<u64>>>);

    impl Serialize for PoisonedReply {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(2)?;
            tuple.serialize_element(&*lock(&self.0))?;
            Err(ser::Error::custom("poisoned"))
        }
    }

    impl<'de> Deserialize<'de> for PoisonedReply {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            Err(de::Error::custom("never deserialized"))
        }
    }

    impl Schema for PoisonedReply {}

    impl Manifest for PoisonedReply {
        const MANIFEST: &'static str = "poisoned-reply";
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("valid address")
    }

    fn endpoint() -> &'static EndpointInner {
        EndpointInner::for_tests(EndpointConfig::new(addr(1))).0
    }

    fn subscriber<M>(endpoint: &EndpointInner, name: &str) -> (ActorId, Mailbox<M>)
    where
        M: Manifest + Send + 'static,
    {
        let id = ActorId::new();
        let (self_ref, mailbox) = SelfRef::<M>::new(id, MailboxCapacity::Unbounded);
        endpoint
            .registry()
            .register(Topic::<M>::new(name).key().wire(), self_ref.actor_ref())
            .expect("registers");
        replicate(endpoint);
        (id, mailbox)
    }

    fn replicate(endpoint: &EndpointInner) {
        let registrations = endpoint.registry().registrations();
        endpoint.receptionist().apply(
            endpoint.node(),
            registrations.version,
            registrations.groups,
            true,
        );
    }

    /// Registers `count` subscribers on an Up remote node, in resolution order.
    fn remote_subscribers<M>(
        endpoint: &EndpointInner,
        node: NodeId,
        name: &str,
        count: usize,
    ) -> Vec<ActorId>
    where
        M: Manifest + 'static,
    {
        let mut ids = (0..count).map(|_| ActorId::new()).collect::<Vec<_>>();
        ids.sort_unstable();
        endpoint.update(|membership, _| membership.add_up(node));
        let groups = vec![WireGroup {
            key: Topic::<M>::new(name).key().wire(),
            ids: ids.clone(),
        }];
        assert!(endpoint.membership().while_up(node, |up| {
            endpoint.receptionist().apply(node, 0, groups, up)
        }));
        ids
    }

    /// Every frame queued so far, with the address it is queued toward and its data stream.
    fn queued(
        dial_requests: &Receiver<DialRequest>,
    ) -> Vec<(SocketAddr, Option<usize>, Frame<'static>)> {
        let mut frames = Vec::new();
        for lane in dial_requests.try_iter() {
            frames.extend(
                lane.control_rx
                    .try_iter()
                    .map(|frame| (lane.addr, None, frame)),
            );
            for (stream, data_rx) in lane.data_rx.iter().enumerate() {
                frames.extend(
                    data_rx
                        .try_iter()
                        .map(|frame| (lane.addr, Some(stream), frame)),
                );
            }
        }
        frames
    }

    fn received<M>(mailbox: Mailbox<M>) -> Vec<M> {
        mailbox
            .split()
            .0
            .drain()
            .filter_map(|incoming| match incoming {
                Incoming::Message(message) => Some(message),
                Incoming::Terminated(_) => None,
            })
            .collect()
    }

    /// Every subscriber is told once, and the count is what the publisher resolved.
    #[test]
    fn a_publish_tells_every_subscriber_once() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");
        let (_, first) = subscriber::<u64>(endpoint, "notes");
        let (_, second) = subscriber::<u64>(endpoint, "notes");

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 2);

        assert_eq!(received(first), vec![42]);
        assert_eq!(received(second), vec![42]);
    }

    /// A topic nobody subscribed to is not an error: it resolves to nobody.
    #[test]
    fn a_topic_without_subscribers_tells_nobody() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 0);
    }

    /// A topic of another message type is refused rather than answered as an empty set, and the
    /// subscribers of the matching type are told whatever else the name holds.
    #[test]
    fn a_topic_of_another_message_type_is_refused() {
        let endpoint = endpoint();
        let (_, mailbox) = subscriber::<u64>(endpoint, "notes");

        assert!(matches!(
            publish_at(endpoint, &Topic::<String>::new("notes"), "note".to_string()),
            Err(ReceptionistError::TypeMismatch)
        ));
        assert_eq!(
            publish_at(endpoint, &Topic::<u64>::new("notes"), 42).expect("resolves"),
            1
        );
        assert_eq!(received(mailbox), vec![42]);
    }

    /// An actor registered for discovery under the topic's name is no subscriber.
    #[test]
    fn a_discovery_registration_of_the_topic_name_is_not_told() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");
        let (self_ref, mailbox) = SelfRef::<u64>::new(ActorId::new(), MailboxCapacity::Unbounded);
        endpoint
            .registry()
            .register(Key::<u64>::new("notes").wire(), self_ref.actor_ref())
            .expect("registers");
        replicate(endpoint);

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 0);
        assert!(received(mailbox).is_empty());
    }

    /// A subscription ends with its actor: the registry evicts the name on termination and the
    /// next replication takes it out of the set a publish resolves.
    #[test]
    fn a_terminated_subscriber_is_not_told() {
        let endpoint = endpoint();
        let topic = Topic::<u64>::new("notes");
        let (_, first) = subscriber::<u64>(endpoint, "notes");
        let (leaving, second) = subscriber::<u64>(endpoint, "notes");
        // Take the receiver before the eviction: one taken after it starts at the evicted
        // version and waits for a change which never comes.
        let mut changes = endpoint.registry().changes();
        let version = *changes.borrow_and_update();

        for watcher in second.split().1.close() {
            watcher.handle_terminated(leaving).expect("watcher alive");
        }
        assert!(*changes.borrow_and_update() > version);
        replicate(endpoint);

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 1);
        assert_eq!(received(first), vec![42]);
    }

    /// Remote subscribers sharing a node and a stream share one frame, whichever order they
    /// resolve in, and a frame names its targets in resolution order.
    #[test]
    fn a_publish_sends_one_frame_per_node_and_stream() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests_with_data_streams(EndpointConfig::new(addr(1)), 4);
        let topic = Topic::<u64>::new("notes");
        let subscribers = [addr(2), addr(3)]
            .into_iter()
            .map(|addr| {
                let node = NodeId::new(addr);
                (addr, remote_subscribers::<u64>(endpoint, node, "notes", 8))
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(publish_at(endpoint, &topic, 42).expect("resolves"), 16);

        let frames = queued(&dial_requests);
        let mut seen = HashSet::new();
        let mut told = HashMap::<SocketAddr, Vec<ActorId>>::new();
        for (addr, stream, frame) in frames {
            let Frame::Multicast {
                targets,
                schema_version: _,
                payload,
            } = frame
            else {
                panic!("not a multicast frame: {frame:?}");
            };
            assert!(stream.is_some(), "a multicast rides a data stream");
            assert!(seen.insert((addr, stream)), "one frame per node and stream");
            assert!(
                targets
                    .iter()
                    .all(|target| endpoint.data_stream(*target) == stream)
            );
            assert!(targets.is_sorted());
            assert_eq!(
                endpoint.codec().decode::<u64>(&payload).expect("decodes"),
                42
            );
            told.entry(addr).or_default().extend(targets);
        }
        for (addr, ids) in subscribers {
            let expected_streams = ids
                .iter()
                .map(|id| endpoint.data_stream(*id))
                .collect::<HashSet<_>>();
            assert_eq!(
                seen.iter()
                    .filter(|(seen_addr, _)| *seen_addr == addr)
                    .count(),
                expected_streams.len()
            );
            let mut told = told.remove(&addr).expect("the node is told");
            told.sort_unstable();
            assert_eq!(told, ids);
        }
    }

    /// Subscribers too many for one frame are split over several, filled to the maximum frame
    /// size and queued in resolution order on one stream.
    #[test]
    fn a_publish_splits_its_frames_at_the_maximum_frame_size() {
        let mut config = EndpointConfig::new(addr(1));
        // Discriminant, target count, payload length and the payload, plus 17 bytes per target:
        // two targets fit, three do not.
        config.max_frame_size = NonZeroUsize::new(4 + 2 * 17 + 16).expect("not zero");
        let (endpoint, dial_requests, _join_requests) = EndpointInner::for_tests(config);
        let ids = remote_subscribers::<u64>(endpoint, NodeId::new(addr(2)), "notes", 5);

        assert_eq!(
            publish_at(endpoint, &Topic::<u64>::new("notes"), 42).expect("resolves"),
            5
        );

        let frames = queued(&dial_requests);
        let mut told = Vec::new();
        let mut sizes = Vec::new();
        for (_, stream, frame) in frames {
            assert_eq!(stream, None);
            assert!(
                frame.encoded_len().expect("frame size") <= endpoint.config().max_frame_size.get()
            );
            let Frame::Multicast { targets, .. } = frame else {
                panic!("not a multicast frame: {frame:?}");
            };
            sizes.push(targets.len());
            told.extend(targets);
        }
        assert_eq!(sizes, [2, 2, 1]);
        assert_eq!(told, ids);
    }

    /// Neither an empty topic nor one with subscribers on this node only serializes the message,
    /// and one with remote subscribers on several nodes serializes it once per node, since the
    /// shape may differ per peer.
    #[test]
    fn a_publish_serializes_only_for_remote_subscribers_and_once_per_node() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let topic = Topic::<Counted>::new("counted");
        SERIALIZATIONS.set(0);

        assert_eq!(
            publish_at(endpoint, &topic, Counted(1)).expect("resolves"),
            0
        );
        assert_eq!(SERIALIZATIONS.get(), 0);

        let (_, first) = subscriber::<Counted>(endpoint, "counted");
        let (_, second) = subscriber::<Counted>(endpoint, "counted");
        assert_eq!(
            publish_at(endpoint, &topic, Counted(2)).expect("resolves"),
            2
        );
        assert_eq!(SERIALIZATIONS.get(), 0);
        assert_eq!(received(first), vec![Counted(2)]);
        assert_eq!(received(second), vec![Counted(2)]);

        remote_subscribers::<Counted>(endpoint, NodeId::new(addr(2)), "counted", 3);
        remote_subscribers::<Counted>(endpoint, NodeId::new(addr(3)), "counted", 3);
        assert_eq!(
            publish_at(endpoint, &topic, Counted(3)).expect("resolves"),
            8
        );
        assert_eq!(SERIALIZATIONS.get(), 2);
        assert_eq!(queued(&dial_requests).len(), 2);
    }

    /// A reply destination shared by a cloneable wrapper is serialized once, for the first remote
    /// subscriber alone: its frame carries the one pending reply, which answers the asker, while
    /// the second subscriber's tell finds the destination taken and dead-letters.
    #[test]
    fn a_shared_reply_destination_reaches_one_remote_subscriber() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let first_node = NodeId::new(addr(2));
        let [first] = remote_subscribers::<SharedReply>(endpoint, first_node, "replies", 1)[..]
        else {
            panic!("not one subscriber");
        };
        remote_subscribers::<SharedReply>(endpoint, NodeId::new(addr(3)), "replies", 1);
        let (shared, reply_rx) = SharedReply::new();

        let told = endpoint::with_current(endpoint, || {
            publish_at(endpoint, &Topic::new("replies"), shared)
        });

        assert_eq!(told.expect("resolves"), 2);
        assert_eq!(endpoint.pending_replies().len(), 1);
        let frames = queued(&dial_requests);
        let [
            (
                addr,
                _,
                Frame::Message {
                    target, reply_tags, ..
                },
            ),
        ] = &frames[..]
        else {
            panic!("not one message frame: {frames:?}");
        };
        assert_eq!((*addr, *target), (first_node.addr(), first));
        let [tag] = reply_tags[..] else {
            panic!("not one reply tag: {reply_tags:?}");
        };

        let reply = endpoint.codec().encode(&7_u64).expect("encodes");
        reply::on_reply(
            endpoint,
            first_node,
            tag.nonce,
            SchemaVersion::new(0),
            &reply,
        );
        assert_eq!(reply_rx.try_recv(), Ok(7));
        assert_eq!(endpoint.pending_replies().len(), 0);
    }

    /// Reply tags minted by a payload which then fails to encode are discarded, so no pending
    /// reply is left behind for a frame never sent.
    #[test]
    fn a_failed_encoding_leaves_no_pending_reply() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        remote_subscribers::<PoisonedReply>(endpoint, NodeId::new(addr(2)), "replies", 1);
        remote_subscribers::<PoisonedReply>(endpoint, NodeId::new(addr(3)), "replies", 1);
        let (SharedReply(reply_to), _reply_rx) = SharedReply::new();

        let told = endpoint::with_current(endpoint, || {
            publish_at(endpoint, &Topic::new("replies"), PoisonedReply(reply_to))
        });

        assert_eq!(told.expect("resolves"), 2);
        assert_eq!(endpoint.pending_replies().len(), 0);
        assert!(queued(&dial_requests).is_empty());
    }

    /// Reply tags minted for a frame refused at admission or at the lane are discarded too.
    #[test]
    fn a_failed_send_leaves_no_pending_reply() {
        let mut oversize = EndpointConfig::new(addr(1));
        oversize.max_frame_size = NonZeroUsize::new(16).expect("not zero");
        let refusing = EndpointConfig::new(addr(1));

        for (config, refuse) in [(oversize, false), (refusing, true)] {
            let (endpoint, dial_requests, _join_requests) = EndpointInner::for_tests(config);
            let first_node = NodeId::new(addr(2));
            remote_subscribers::<SharedReply>(endpoint, first_node, "replies", 1);
            remote_subscribers::<SharedReply>(endpoint, NodeId::new(addr(3)), "replies", 1);
            if refuse {
                endpoint.refuse(first_node.addr());
            }
            let (shared, _reply_rx) = SharedReply::new();

            let told = endpoint::with_current(endpoint, || {
                publish_at(endpoint, &Topic::new("replies"), shared)
            });

            assert_eq!(told.expect("resolves"), 2);
            assert_eq!(endpoint.pending_replies().len(), 0);
            assert!(queued(&dial_requests).is_empty());
        }
    }

    /// Encodes the bare number for a peer below application version 2 and the number with a
    /// flag, as version 1, for a newer one.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Shaped(u64);

    impl Schema for Shaped {
        const VERSION: SchemaVersion = SchemaVersion::new(1);

        fn encode(
            &self,
            codec: Codec,
            peer: &Peer,
        ) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
            if peer.app_version().is_none_or(|version| version.major < 2) {
                return Ok((SchemaVersion::new(0), codec.encode(&self.0)?));
            }
            Ok((Self::VERSION, codec.encode(&(self.0, true))?))
        }
    }

    impl Manifest for Shaped {
        const MANIFEST: &'static str = "shaped";
    }

    /// Serializes its reply destination for a peer of application version 2 or later only.
    #[derive(Clone)]
    struct GatedReply(SharedReply);

    impl Serialize for GatedReply {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.0.serialize(serializer)
        }
    }

    impl<'de> Deserialize<'de> for GatedReply {
        fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            Err(de::Error::custom("never deserialized"))
        }
    }

    impl Schema for GatedReply {
        fn encode(
            &self,
            codec: Codec,
            peer: &Peer,
        ) -> Result<(SchemaVersion, Vec<u8>), CodecError> {
            if peer.app_version().is_some_and(|version| version.major >= 2) {
                return Ok((Self::VERSION, codec.encode(&self.0)?));
            }
            Ok((Self::VERSION, codec.encode(&0_u64)?))
        }
    }

    impl Manifest for GatedReply {
        const MANIFEST: &'static str = "gated-reply";
    }

    /// A publish encodes per destination node with the shape that node's application version
    /// takes: a node which announced none, or an older one, gets the old shape, a newer one the
    /// new, and every frame carries its schema version.
    #[test]
    fn a_publish_encodes_the_shape_each_peer_understands() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let unknown = NodeId::new(addr(2));
        let older = NodeId::new(addr(3));
        let newer = NodeId::new(addr(4));
        for node in [unknown, older, newer] {
            remote_subscribers::<Shaped>(endpoint, node, "shaped", 1);
        }
        endpoint.record_peer(older, Some(Version::new(1, 9, 0)));
        endpoint.record_peer(newer, Some(Version::new(2, 0, 0)));

        let told = publish_at(endpoint, &Topic::new("shaped"), Shaped(5));

        assert_eq!(told.expect("resolves"), 3);
        let mut shapes = queued(&dial_requests)
            .into_iter()
            .map(|(addr, _, frame)| match frame {
                Frame::Multicast {
                    schema_version,
                    payload,
                    ..
                } => (addr, schema_version, payload.into_owned()),
                frame => panic!("not a multicast: {frame:?}"),
            })
            .collect::<Vec<_>>();
        shapes.sort_by_key(|(addr, _, _)| *addr);
        let old = Codec::Postcard.encode(&5_u64).expect("encodes");
        let new = Codec::Postcard.encode(&(5_u64, true)).expect("encodes");
        assert_eq!(
            shapes,
            [
                (unknown.addr(), SchemaVersion::new(0), old.clone()),
                (older.addr(), SchemaVersion::new(0), old),
                (newer.addr(), SchemaVersion::new(1), new),
            ]
        );
    }

    /// A tell through a remote reference encodes for the peer's application version the same
    /// way: with no information before the handshake, with the version afterward.
    #[test]
    fn a_tell_encodes_the_shape_the_peer_understands() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let node = NodeId::new(addr(2));
        let target = ActorId::new();
        let sink = RemoteSink::<Shaped>::new(node);

        endpoint::with_current(endpoint, || sink.try_send_message(target, Shaped(5)))
            .expect("sends");
        endpoint.record_peer(node, Some(Version::new(2, 0, 0)));
        endpoint::with_current(endpoint, || sink.try_send_message(target, Shaped(6)))
            .expect("sends");

        let shapes = queued(&dial_requests)
            .into_iter()
            .map(|(_, _, frame)| match frame {
                Frame::Message {
                    schema_version,
                    payload,
                    ..
                } => (schema_version, payload.into_owned()),
                frame => panic!("not a message: {frame:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            shapes,
            [
                (
                    SchemaVersion::new(0),
                    Codec::Postcard.encode(&5_u64).expect("encodes")
                ),
                (
                    SchemaVersion::new(1),
                    Codec::Postcard.encode(&(6_u64, true)).expect("encodes")
                ),
            ]
        );
    }

    /// Whether reply tags were minted is decided per destination encoding: the peer for which the
    /// destination is omitted gets a multicast, the newer peer for which it is serialized gets a
    /// message frame carrying the pending reply, which that peer's reply settles.
    #[test]
    fn a_destination_serialized_for_one_peer_rides_that_peers_message_frame() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let older = NodeId::new(addr(2));
        let newer = NodeId::new(addr(3));
        remote_subscribers::<GatedReply>(endpoint, older, "gated", 1);
        let [target] = remote_subscribers::<GatedReply>(endpoint, newer, "gated", 1)[..] else {
            panic!("not one subscriber");
        };
        endpoint.record_peer(older, Some(Version::new(1, 0, 0)));
        endpoint.record_peer(newer, Some(Version::new(2, 0, 0)));
        let (shared, reply_rx) = SharedReply::new();

        let told = endpoint::with_current(endpoint, || {
            publish_at(endpoint, &Topic::new("gated"), GatedReply(shared))
        });

        assert_eq!(told.expect("resolves"), 2);
        assert_eq!(endpoint.pending_replies().len(), 1);
        let mut frames = queued(&dial_requests);
        frames.sort_by_key(|(addr, _, _)| *addr);
        let [
            (older_addr, _, Frame::Multicast { .. }),
            (
                newer_addr,
                _,
                Frame::Message {
                    target: message_target,
                    reply_tags,
                    ..
                },
            ),
        ] = &frames[..]
        else {
            panic!("not a multicast and a message: {frames:?}");
        };
        assert_eq!(
            (*older_addr, *newer_addr, *message_target),
            (older.addr(), newer.addr(), target)
        );
        let [tag] = reply_tags[..] else {
            panic!("not one reply tag: {reply_tags:?}");
        };

        let reply = endpoint.codec().encode(&7_u64).expect("encodes");
        reply::on_reply(endpoint, newer, tag.nonce, SchemaVersion::new(0), &reply);
        assert_eq!(reply_rx.try_recv(), Ok(7));
        assert_eq!(endpoint.pending_replies().len(), 0);
    }

    /// A reply of a schema version the asker does not decode is a dead letter which drops the
    /// destination, so the ask fails as `NoReply` rather than hanging.
    #[test]
    fn a_reply_of_an_unsupported_version_drops_the_destination() {
        let (endpoint, dial_requests, _join_requests) =
            EndpointInner::for_tests(EndpointConfig::new(addr(1)));
        let node = NodeId::new(addr(2));
        remote_subscribers::<SharedReply>(endpoint, node, "replies", 1);
        let (shared, reply_rx) = SharedReply::new();

        let told = endpoint::with_current(endpoint, || {
            publish_at(endpoint, &Topic::new("replies"), shared)
        });

        assert_eq!(told.expect("resolves"), 1);
        let frames = queued(&dial_requests);
        let [(_, _, Frame::Message { reply_tags, .. })] = &frames[..] else {
            panic!("not one message frame: {frames:?}");
        };
        let [tag] = reply_tags[..] else {
            panic!("not one reply tag: {reply_tags:?}");
        };

        let reply = endpoint.codec().encode(&7_u64).expect("encodes");
        reply::on_reply(endpoint, node, tag.nonce, SchemaVersion::new(3), &reply);
        assert_eq!(reply_rx.try_recv(), Err(mpsc::TryRecvError::Disconnected));
        assert_eq!(endpoint.pending_replies().len(), 0);
    }
}
