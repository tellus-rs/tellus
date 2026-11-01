use crate::{
    ActorId,
    cluster::{
        discovery::Nonce,
        membership::Member,
        node::{ClusterId, NodeId},
        reachability::WireReachability,
        receptionist::WireGroup,
        reply::ReplyTag,
    },
};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, ops::RangeInclusive};
use thiserror::Error;

const PROTOCOL_MAGIC: u32 = 0x574C_545A;

/// The protocol versions this build speaks. Two nodes speak the newest version both ranges
/// hold, negotiated through the handshake envelope, whose layout is frozen: the `Frame`
/// discriminant 0, the magic, the oldest and the newest version. A version extends the previous
/// one's handshake body only by appending fields, so the body of every version decodes as the
/// body of any older one. Every new frame and every new field is gated on the negotiated
/// version: a peer which negotiated an older one is never sent it.
const PROTOCOL_VERSIONS: RangeInclusive<u16> = 1..=1;

const HANDSHAKE_DISCRIMINANT: u8 = 0;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Frame<'a> {
    Handshake(Handshake),

    Message {
        target: ActorId,
        reply_tags: Vec<ReplyTag>,
        #[serde(borrow, with = "payload_bytes")]
        payload: Cow<'a, [u8]>,
    },

    Watch {
        target: ActorId,
        watcher: ActorId,
    },

    Unwatch {
        target: ActorId,
        watcher: ActorId,
    },

    Terminated {
        target: ActorId,
        watcher: ActorId,
    },

    Reply {
        nonce: Nonce,
        recipient: Option<ActorId>,
        #[serde(borrow, with = "payload_bytes")]
        payload: Cow<'a, [u8]>,
    },

    ReplyDropped {
        nonce: Nonce,
        recipient: Option<ActorId>,
    },

    Gossip {
        members: Vec<Member>,
        more: bool,
    },

    Refused {
        reason: RefusalReason,
    },

    Reachability {
        observations: Vec<WireReachability>,
    },

    Registrations {
        version: u64,
        chunk: u32,
        chunks: u32,
        groups: Vec<WireGroup>,
    },

    ReplyCancelled {
        nonce: Nonce,
        stream: Option<StreamKey>,
    },

    // Every target must map to the stream of the first one, which the frame rides.
    Multicast {
        targets: Vec<ActorId>,
        #[serde(borrow, with = "payload_bytes")]
        payload: Cow<'a, [u8]>,
    },

    // A departing dialer's final control frame, sent only once its data streams are finished.
    Drained {
        data_streams: u32,
    },

    // The acceptor's answer to `Drained`, on the reply direction of the control stream.
    DrainAck,
}

mod payload_bytes {
    use serde::{
        Deserializer, Serializer,
        de::{Error, Visitor},
    };
    use std::{borrow::Cow, fmt::Formatter};

    pub(super) fn serialize<S>(payload: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(payload)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Cow<'de, [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BytesVisitor)
    }

    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Cow<'de, [u8]>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a byte payload")
        }

        fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Borrowed(value))
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Owned(value.to_vec()))
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Cow::Owned(value))
        }
    }
}

impl<'a> Frame<'a> {
    pub(crate) fn encode_into(&self, mut buffer: Vec<u8>) -> Result<Vec<u8>, postcard::Error> {
        buffer.clear();
        postcard::to_extend(self, buffer)
    }

    pub(crate) fn encoded_len(&self) -> Result<usize, postcard::Error> {
        postcard::experimental::serialized_size(self)
    }

    pub(crate) fn from_bytes(bytes: &'a [u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }

    pub(crate) fn is_counted(&self) -> bool {
        match self {
            Frame::Message { .. } | Frame::Reply { .. } | Frame::Multicast { .. } => true,

            Frame::Handshake(_)
            | Frame::Watch { .. }
            | Frame::Unwatch { .. }
            | Frame::Terminated { .. }
            | Frame::ReplyDropped { .. }
            | Frame::Gossip { .. }
            | Frame::Refused { .. }
            | Frame::Reachability { .. }
            | Frame::Registrations { .. }
            | Frame::ReplyCancelled { .. }
            | Frame::Drained { .. }
            | Frame::DrainAck => false,
        }
    }

    pub(crate) fn stream_key(&self) -> Option<StreamKey> {
        match self {
            Frame::Message { target, .. } => Some(StreamKey::Actor(*target)),

            Frame::Terminated { watcher, .. } => Some(StreamKey::Actor(*watcher)),

            Frame::Reply {
                nonce, recipient, ..
            }
            | Frame::ReplyDropped { nonce, recipient } => {
                Some(recipient.map_or(StreamKey::Nonce(*nonce), StreamKey::Actor))
            }

            Frame::Handshake(_)
            | Frame::Watch { .. }
            | Frame::Unwatch { .. }
            | Frame::Gossip { .. }
            | Frame::Refused { .. }
            | Frame::Reachability { .. }
            | Frame::Registrations { .. }
            | Frame::Drained { .. }
            | Frame::DrainAck => None,

            Frame::ReplyCancelled { stream, .. } => *stream,

            Frame::Multicast { targets, .. } => targets.first().copied().map(StreamKey::Actor),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum StreamKey {
    Actor(ActorId),
    Nonce(Nonce),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Handshake {
    envelope: HandshakeEnvelope,
    body: HandshakeV1,
}

impl Handshake {
    pub(crate) fn new(body: HandshakeV1) -> Self {
        Self {
            envelope: HandshakeEnvelope::this_build(),
            body,
        }
    }
}

/// Frozen for every version: whatever a peer speaks, this is how both learn it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HandshakeEnvelope {
    magic: u32,
    min_version: u16,
    max_version: u16,
}

impl HandshakeEnvelope {
    pub(crate) fn this_build() -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            min_version: *PROTOCOL_VERSIONS.start(),
            max_version: *PROTOCOL_VERSIONS.end(),
        }
    }

    /// `None` for any frame but a handshake; the body behind the envelope is not read.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, postcard::Error> {
        if bytes.first() != Some(&HANDSHAKE_DISCRIMINANT) {
            return Ok(None);
        }

        let HandshakeFrame::Handshake(envelope, ()) = postcard::from_bytes(bytes)?;
        Ok(Some(envelope))
    }

    /// The envelope alone, without a body.
    pub(crate) fn encode(self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(&HandshakeFrame::Handshake(self, ()))
    }

    /// The newest version both ranges hold.
    pub(crate) fn negotiate(self, theirs: HandshakeEnvelope) -> Result<u16, HandshakeError> {
        if theirs.magic != PROTOCOL_MAGIC {
            return Err(HandshakeError::Magic(theirs.magic));
        }

        let version = self.max_version.min(theirs.max_version);
        if version < self.min_version.max(theirs.min_version) {
            return Err(HandshakeError::Incompatible {
                ours: self.versions(),
                theirs: theirs.versions(),
            });
        }
        Ok(version)
    }

    fn versions(self) -> RangeInclusive<u16> {
        self.min_version..=self.max_version
    }
}

/// The body of protocol version 1. A later version's body appends to it, so this decodes the
/// handshake of any version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HandshakeV1 {
    pub(crate) node: NodeId,
    pub(crate) intent: HandshakeIntent,
    pub(crate) cluster: Option<ClusterId>,
}

impl HandshakeV1 {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, postcard::Error> {
        let HandshakeFrame::Handshake(_, body) =
            postcard::from_bytes::<HandshakeFrame<Self>>(bytes)?;
        Ok(body)
    }
}

/// A `Probe` is answered with the reply handshake and never admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HandshakeIntent {
    Member,
    Join,
    Probe,
}

/// [RefusalReason::UnknownMember], [RefusalReason::NoCluster] and [RefusalReason::JoinsHeld] are
/// worth retrying.
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RefusalReason {
    #[display("not a member of this cluster")]
    UnknownMember,

    #[display("dead node incarnation")]
    Down,

    #[display("not a member of any cluster")]
    NoCluster,

    #[display("a member of another cluster")]
    ForeignCluster,

    #[display("joins held back while a member is unreachable")]
    JoinsHeld,
}

#[derive(Debug, Error)]
pub(crate) enum HandshakeError {
    #[error("unexpected protocol magic {0:#010x}")]
    Magic(u32),

    #[error("no common protocol version, this node speaks {ours:?}, the peer {theirs:?}")]
    Incompatible {
        ours: RangeInclusive<u16>,
        theirs: RangeInclusive<u16>,
    },
}

/// The frame a handshake rides, generic over its body: `()` is the bare envelope, which is all a
/// node sends back when the ranges do not overlap.
#[derive(Serialize, Deserialize)]
enum HandshakeFrame<B> {
    Handshake(HandshakeEnvelope, B),
}

/// A peer of another protocol version: its handshake bodies and its encoder and decoder, which
/// read and write the body of whichever version was negotiated.
#[cfg(test)]
pub(crate) mod synthetic {
    use crate::cluster::{
        frame::{HandshakeEnvelope, HandshakeFrame, HandshakeIntent, HandshakeV1, PROTOCOL_MAGIC},
        node::{ClusterId, NodeId},
    };
    use serde::{Deserialize, Serialize};

    /// Version 2 appends one field to the version 1 body.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct HandshakeV2 {
        pub(crate) node: NodeId,
        pub(crate) intent: HandshakeIntent,
        pub(crate) cluster: Option<ClusterId>,
        pub(crate) appended: u64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Body {
        V1(HandshakeV1),
        V2(HandshakeV2),
    }

    pub(crate) fn envelope(min_version: u16, max_version: u16) -> HandshakeEnvelope {
        HandshakeEnvelope {
            magic: PROTOCOL_MAGIC,
            min_version,
            max_version,
        }
    }

    pub(crate) fn encode(envelope: HandshakeEnvelope, body: Body) -> Vec<u8> {
        match body {
            Body::V1(body) => postcard::to_allocvec(&HandshakeFrame::Handshake(envelope, body)),
            Body::V2(body) => postcard::to_allocvec(&HandshakeFrame::Handshake(envelope, body)),
        }
        .expect("handshake encodes")
    }

    pub(crate) fn decode(bytes: &[u8], version: u16) -> Body {
        match version {
            1 => Body::V1(HandshakeV1::decode(bytes).expect("a version 1 body")),

            2 => {
                let HandshakeFrame::Handshake(_, body) =
                    postcard::from_bytes::<HandshakeFrame<HandshakeV2>>(bytes)
                        .expect("a version 2 body");
                Body::V2(body)
            }

            _ => panic!("no body schema for version {version}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId,
        cluster::{
            discovery::{KeyKind, Nonce, WireKey},
            frame::{
                Frame, Handshake, HandshakeEnvelope, HandshakeError, HandshakeIntent, HandshakeV1,
                PROTOCOL_MAGIC, RefusalReason, StreamKey,
                synthetic::{self, Body, HandshakeV2},
            },
            membership::{Member, MemberState},
            node::{ClusterId, NodeId},
            reachability::WireReachability,
            receptionist::WireGroup,
            reply::ReplyTag,
        },
    };
    use std::borrow::Cow;

    /// A terminated signal rides the watcher's stream: routing it by target would break its
    /// ordering behind the messages the terminated actor sent to that watcher. A reply rides the
    /// stream of the actor it is delivered to, for the same reason.
    #[test]
    fn frames_key_on_the_actor_they_are_delivered_to() {
        let target = ActorId::new();
        let watcher = ActorId::new();

        let message = Frame::Message {
            target,
            reply_tags: Vec::new(),
            payload: Cow::Borrowed(&[]),
        };
        assert_eq!(message.stream_key(), Some(StreamKey::Actor(target)));
        assert_eq!(
            Frame::Terminated { target, watcher }.stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(
            Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            }
            .stream_key(),
            Some(StreamKey::Actor(watcher))
        );
        assert_eq!(Frame::Watch { target, watcher }.stream_key(), None);
        assert_eq!(Frame::Drained { data_streams: 2 }.stream_key(), None);
        assert_eq!(Frame::DrainAck.stream_key(), None);
        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .stream_key(),
            None
        );
        assert_eq!(
            Frame::Refused {
                reason: RefusalReason::UnknownMember
            }
            .stream_key(),
            None
        );
    }

    /// A reply no actor awaits keys on its nonce, not on nothing: falling back to the control
    /// stream would put a user payload in front of gossip and watch frames.
    #[test]
    fn a_reply_without_a_recipient_keys_on_its_nonce() {
        let nonce = Nonce::first();

        assert_eq!(
            Frame::Reply {
                nonce,
                recipient: None,
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            Some(StreamKey::Nonce(nonce))
        );
        assert_eq!(
            Frame::ReplyDropped {
                nonce,
                recipient: None,
            }
            .stream_key(),
            Some(StreamKey::Nonce(nonce))
        );
    }

    /// A cancellation rides the stream of the request it cancels, so it never overtakes that
    /// request; one without a stream rides the control stream.
    #[test]
    fn a_reply_cancellation_rides_the_stream_it_carries() {
        let nonce = Nonce::first();

        for stream in [
            Some(StreamKey::Actor(ActorId::new())),
            Some(StreamKey::Nonce(nonce)),
            None,
        ] {
            assert_eq!(Frame::ReplyCancelled { nonce, stream }.stream_key(), stream);
        }
    }

    /// A multicast rides the stream of its first target, which every other target shares; one
    /// without targets has no stream to pick and rides the control stream.
    #[test]
    fn a_multicast_keys_on_its_first_target() {
        let (first, second) = (ActorId::new(), ActorId::new());

        assert_eq!(
            Frame::Multicast {
                targets: vec![first, second],
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            Some(StreamKey::Actor(first))
        );
        assert_eq!(
            Frame::Multicast {
                targets: Vec::new(),
                payload: Cow::Borrowed(&[]),
            }
            .stream_key(),
            None
        );
    }

    /// Protocol version 1 pins the frame discriminants to hardcoded wire bytes: a round
    /// trip cannot catch a format break, since reordered variants change both directions at once.
    #[test]
    fn gossip_matches_its_pinned_wire_bytes() {
        let bytes = [7, 0, 0];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        assert!(matches!(frame, Frame::Gossip { members, more } if members.is_empty() && !more));

        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            bytes
        );

        assert_eq!(
            Frame::Gossip {
                members: Vec::new(),
                more: true,
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            [7, 0, 1]
        );
    }

    /// A gossiped member is its node followed by its state, so the member type can change
    /// without changing a byte on the wire. The state's byte is its rank in the lattice
    /// `Up < Left < Down`.
    #[test]
    fn a_gossiped_member_encodes_as_its_node_and_state() {
        let node = NodeId::new("127.0.0.1:2552".parse().expect("valid address"));
        let leaver = NodeId::new("127.0.0.1:2553".parse().expect("valid address"));
        let other = NodeId::new("127.0.0.1:2554".parse().expect("valid address"));
        let frame = Frame::Gossip {
            members: vec![
                Member::new(node, MemberState::Up),
                Member::new(leaver, MemberState::Left),
                Member::new(other, MemberState::Down),
            ],
            more: true,
        };

        let mut expected = vec![7, 3];
        expected.extend(postcard::to_allocvec(&node).expect("node encodes"));
        expected.push(0);
        expected.extend(postcard::to_allocvec(&leaver).expect("node encodes"));
        expected.push(1);
        expected.extend(postcard::to_allocvec(&other).expect("node encodes"));
        expected.push(2);
        expected.push(1);
        assert_eq!(
            frame.encode_into(Vec::new()).expect("frame encodes"),
            expected
        );
        assert_eq!(Frame::from_bytes(&expected).expect("frame decodes"), frame);
        assert!(MemberState::Up < MemberState::Left && MemberState::Left < MemberState::Down);
    }

    /// The frames delivered to an actor are pinned to hardcoded wire bytes too, and these are the
    /// ones that matter. Their field layout carries the ordering guarantees across the wire, since
    /// a terminated signal names its watcher and a reply its asker to ride the right stream.
    /// Decoding the IDs back out of the bytes they were built from catches a transposed `target`
    /// and `watcher`, which a round trip alone cannot see.
    #[test]
    fn delivered_frames_match_their_pinned_wire_bytes() {
        const TARGET: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
        const RECIPIENT: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";

        let bytes = [
            1, // Frame::Message
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target
            0,  // no reply tags
            3, 1, 2, 3, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Message {
            target,
            reply_tags,
            payload,
        } = &frame
        else {
            panic!("not a message frame");
        };
        assert_eq!(target.to_string(), TARGET);
        assert!(reply_tags.is_empty());
        assert_eq!(payload.as_ref(), &[1, 2, 3][..]);
        assert!(matches!(payload, Cow::Borrowed(_)));
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            4, // Frame::Terminated
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // watcher
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Terminated { target, watcher } = &frame else {
            panic!("not a terminated frame");
        };
        assert_eq!(target.to_string(), TARGET);
        assert_eq!(watcher.to_string(), RECIPIENT);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            5, // Frame::Reply
            0, // nonce
            1, // recipient: Some
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // recipient
            2, 4, 5, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Reply {
            nonce,
            recipient,
            payload,
        } = &frame
        else {
            panic!("not a reply frame");
        };
        assert_eq!(*nonce, Nonce::first());
        assert_eq!(
            recipient.expect("the reply names a recipient").to_string(),
            RECIPIENT
        );
        assert_eq!(payload.as_ref(), &[4, 5][..]);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let bytes = [
            5, // Frame::Reply
            0, // nonce
            0, // recipient: None
            2, 4, 5, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Reply { recipient, .. } = &frame else {
            panic!("not a reply frame");
        };
        assert_eq!(*recipient, None);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);
    }

    /// A refusal's reason is a discriminant on the wire, so a variant may be appended but never
    /// reordered: a peer of another build reads these bytes, not the names.
    #[test]
    fn refusals_match_their_pinned_wire_bytes() {
        let reasons = [
            (RefusalReason::UnknownMember, 0),
            (RefusalReason::Down, 1),
            (RefusalReason::NoCluster, 2),
            (RefusalReason::ForeignCluster, 3),
            (RefusalReason::JoinsHeld, 4),
        ];

        for (reason, discriminant) in reasons {
            let bytes = [
                8, // Frame::Refused
                discriminant,
            ];

            let frame = Frame::from_bytes(&bytes).expect("frame decodes");
            let Frame::Refused { reason: decoded } = &frame else {
                panic!("not a refused frame");
            };
            assert_eq!(*decoded, reason);
            assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);
        }
    }

    /// Every frame survives a round trip through the wire format; a change to the variant order
    /// or to a field breaks this, which is what makes the format explicit rather than incidental.
    #[test]
    fn frames_round_trip() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let other = NodeId::new("127.0.0.1:5678".parse().expect("valid address"));
        let target = ActorId::new();
        let watcher = ActorId::new();

        let frames = [
            Frame::Handshake(Handshake::new(HandshakeV1 {
                node,
                intent: HandshakeIntent::Join,
                cluster: Some(ClusterId::new()),
            })),
            Frame::Message {
                target,
                reply_tags: vec![ReplyTag {
                    nonce: Nonce::first(),
                    recipient: Some(watcher),
                }],
                payload: Cow::Borrowed(&[1, 2, 3]),
            },
            Frame::Watch { target, watcher },
            Frame::Unwatch { target, watcher },
            Frame::Terminated { target, watcher },
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[4, 5, 6]),
            },
            Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            },
            Frame::Gossip {
                members: vec![
                    Member::new(node, MemberState::Up),
                    Member::new(other, MemberState::Left),
                    Member::new(other, MemberState::Down),
                ],
                more: true,
            },
            Frame::Refused {
                reason: RefusalReason::UnknownMember,
            },
            Frame::Reachability {
                observations: vec![WireReachability {
                    observer: node,
                    subject: other,
                    version: 1,
                    reachable: false,
                }],
            },
            Frame::Registrations {
                version: 7,
                chunk: 1,
                chunks: 2,
                groups: vec![WireGroup {
                    key: WireKey::new::<u64>(KeyKind::Discovery, "worker-pool"),
                    ids: vec![target, watcher],
                }],
            },
            Frame::ReplyCancelled {
                nonce: Nonce::first(),
                stream: Some(StreamKey::Actor(watcher)),
            },
            Frame::Multicast {
                targets: vec![target, watcher],
                payload: Cow::Borrowed(&[7, 8, 9]),
            },
            Frame::Drained { data_streams: 4 },
            Frame::DrainAck,
        ];

        for frame in frames {
            let bytes = frame.encode_into(Vec::new()).expect("frame encodes");
            let decoded = Frame::from_bytes(&bytes).expect("frame decodes");
            assert_eq!(decoded, frame);
        }
    }

    /// Only message, reply and multicast frames count against the outbound capacity, a multicast
    /// as one slot however many targets it names; system frames bypass it, since a terminated
    /// signal, like a reply-dropped notification, must never be dropped.
    #[test]
    fn only_message_reply_and_multicast_frames_are_counted() {
        let target = ActorId::new();
        let watcher = ActorId::new();

        assert!(
            Frame::Message {
                target,
                reply_tags: Vec::new(),
                payload: Cow::Borrowed(&[])
            }
            .is_counted()
        );
        assert!(
            Frame::Reply {
                nonce: Nonce::first(),
                recipient: Some(watcher),
                payload: Cow::Borrowed(&[]),
            }
            .is_counted()
        );
        assert!(
            Frame::Multicast {
                targets: vec![target, watcher],
                payload: Cow::Borrowed(&[]),
            }
            .is_counted()
        );
        assert!(
            !Frame::ReplyDropped {
                nonce: Nonce::first(),
                recipient: Some(watcher),
            }
            .is_counted()
        );
        assert!(!Frame::Watch { target, watcher }.is_counted());
        assert!(!Frame::Unwatch { target, watcher }.is_counted());
        assert!(!Frame::Terminated { target, watcher }.is_counted());
        assert!(
            !Frame::Gossip {
                members: Vec::new(),
                more: false,
            }
            .is_counted()
        );
        assert!(
            !Frame::Refused {
                reason: RefusalReason::Down
            }
            .is_counted()
        );
        assert!(
            !Frame::ReplyCancelled {
                nonce: Nonce::first(),
                stream: Some(StreamKey::Actor(target)),
            }
            .is_counted()
        );
        assert!(!Frame::Drained { data_streams: 1 }.is_counted());
        assert!(!Frame::DrainAck.is_counted());
    }

    /// The registrations variant was appended at discriminant 10, pinned like the others.
    #[test]
    fn registrations_match_their_pinned_wire_bytes() {
        let bytes = [10, 0, 0, 1, 0];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        assert!(matches!(
            frame,
            Frame::Registrations { version: 0, chunk: 0, chunks: 1, ref groups } if groups.is_empty()
        ));
        assert_eq!(
            Frame::Registrations {
                version: 0,
                chunk: 0,
                chunks: 1,
                groups: Vec::new(),
            }
            .encode_into(Vec::new())
            .expect("frame encodes"),
            bytes
        );
    }

    /// One group of two IDs under one key: the kind, the name and the manifest of the key, then
    /// the IDs. Decoding them back out of hardcoded bytes catches a transposed field, which a round
    /// trip cannot.
    #[test]
    fn a_registrations_group_matches_its_pinned_wire_bytes() {
        const FIRST: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
        const SECOND: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";

        let mut bytes = vec![
            10, // Frame::Registrations
            7,  // version
            1,  // chunk
            2,  // chunks
            1,  // groups: 1
            0,  // kind: Discovery
            4, 112, 111, 111, 108, // name "pool"
            3, 117, 54, 52, // manifest "u64"
            2,  // ids: 2
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // first id
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // second id
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Registrations {
            version,
            chunk,
            chunks,
            groups,
        } = &frame
        else {
            panic!("not a registrations frame");
        };
        assert_eq!((*version, *chunk, *chunks), (7, 1, 2));
        let [WireGroup { key, ids }] = groups.as_slice() else {
            panic!("not one group: {groups:?}");
        };
        assert_eq!(*key, WireKey::new::<u64>(KeyKind::Discovery, "pool"));
        assert_eq!(
            ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [FIRST, SECOND]
        );
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        bytes[5] = 1; // kind: Topic
        let Frame::Registrations { groups, .. } = Frame::from_bytes(&bytes).expect("frame decodes")
        else {
            panic!("not a registrations frame");
        };
        assert_eq!(groups[0].key, WireKey::new::<u64>(KeyKind::Topic, "pool"));
    }

    /// The reply cancellation was appended at discriminant 11, pinned like the others, with and
    /// without the stream it rides.
    #[test]
    fn reply_cancellations_match_their_pinned_wire_bytes() {
        for (stream, bytes) in [
            (None, vec![11, 0, 0]),
            (Some(StreamKey::Nonce(Nonce::first())), vec![11, 0, 1, 1, 0]),
        ] {
            let frame = Frame::from_bytes(&bytes).expect("frame decodes");
            assert!(matches!(
                frame,
                Frame::ReplyCancelled { nonce, stream: decoded }
                    if nonce == Nonce::first() && decoded == stream
            ));
            assert_eq!(
                Frame::ReplyCancelled {
                    nonce: Nonce::first(),
                    stream,
                }
                .encode_into(Vec::new())
                .expect("frame encodes"),
                bytes
            );
        }
    }

    /// The multicast was appended at discriminant 12: its targets in order, then the payload.
    /// Decoding the IDs back out of hardcoded bytes catches a transposed field, which a round trip
    /// cannot.
    #[test]
    fn a_multicast_matches_its_pinned_wire_bytes() {
        const FIRST: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
        const SECOND: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";

        let bytes = [
            12, // Frame::Multicast
            2,  // targets: 2
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // first target
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
            31, // second target
            3, 1, 2, 3, // payload
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Multicast { targets, payload } = &frame else {
            panic!("not a multicast frame");
        };
        assert_eq!(
            targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [FIRST, SECOND]
        );
        assert_eq!(payload.as_ref(), &[1, 2, 3][..]);
        assert!(matches!(payload, Cow::Borrowed(_)));
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);
    }

    /// The drain marker was appended at discriminant 13 with the number of data streams it
    /// covers, its acknowledgment at 14 without a field.
    #[test]
    fn the_drain_frames_match_their_pinned_wire_bytes() {
        let drained = [13, 3];
        let frame = Frame::from_bytes(&drained).expect("frame decodes");
        assert_eq!(frame, Frame::Drained { data_streams: 3 });
        assert_eq!(
            frame.encode_into(Vec::new()).expect("frame encodes"),
            drained
        );

        let wide = [13, 128, 2];
        assert_eq!(
            Frame::from_bytes(&wide).expect("frame decodes"),
            Frame::Drained { data_streams: 256 }
        );

        let drain_ack = [14];
        let frame = Frame::from_bytes(&drain_ack).expect("frame decodes");
        assert_eq!(frame, Frame::DrainAck);
        assert_eq!(
            frame.encode_into(Vec::new()).expect("frame encodes"),
            drain_ack
        );
    }

    /// The envelope is frozen across every version, so these bytes must never change: they are
    /// how two builds of any version learn whether they share one.
    #[test]
    fn the_handshake_envelope_matches_its_pinned_wire_bytes() {
        let bytes = [
            0, // Frame::Handshake
            218, 168, 177, 186, 5, // magic
            1, // min_version
            1, // max_version
        ];

        assert_eq!(
            HandshakeEnvelope::decode(&bytes).expect("envelope decodes"),
            Some(HandshakeEnvelope::this_build())
        );
        assert_eq!(
            HandshakeEnvelope::this_build()
                .encode()
                .expect("envelope encodes"),
            bytes
        );
    }

    /// The version 1 body follows the envelope: the node, the intent, the cluster ID. Decoding
    /// the IDs back out of hardcoded bytes catches a transposed field, which a round trip cannot.
    #[test]
    fn a_v1_handshake_matches_its_pinned_wire_bytes() {
        const INCARNATION: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
        const CLUSTER: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";

        let mut bytes = vec![
            0, // Frame::Handshake
            218, 168, 177, 186, 5, // magic
            1, // min_version
            1, // max_version
            0, 127, 0, 0, 1, 248, 19, // node address 127.0.0.1:2552
            16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // node incarnation
            1,  // intent: Join
            1,  // cluster: Some
            16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // cluster
        ];

        let frame = Frame::from_bytes(&bytes).expect("frame decodes");
        let Frame::Handshake(handshake) = &frame else {
            panic!("not a handshake frame");
        };
        assert_eq!(handshake.envelope, HandshakeEnvelope::this_build());
        let body = handshake.body;
        assert_eq!(body.node.addr().to_string(), "127.0.0.1:2552");
        assert_eq!(body.node.incarnation().to_string(), INCARNATION);
        assert_eq!(body.intent, HandshakeIntent::Join);
        assert_eq!(
            body.cluster
                .expect("the handshake names a cluster")
                .to_string(),
            CLUSTER
        );
        assert_eq!(HandshakeV1::decode(&bytes).expect("body decodes"), body);
        assert_eq!(frame.encode_into(Vec::new()).expect("frame encodes"), bytes);

        let intents = [
            (HandshakeIntent::Member, 0),
            (HandshakeIntent::Join, 1),
            (HandshakeIntent::Probe, 2),
        ];
        let intent_at = bytes.len() - 19;
        bytes.truncate(intent_at + 1);
        bytes.push(0); // cluster: None
        for (intent, discriminant) in intents {
            bytes[intent_at] = discriminant;
            let body = HandshakeV1::decode(&bytes).expect("body decodes");
            assert_eq!(body.intent, intent);
            assert_eq!(body.cluster, None);
        }
    }

    /// The newest version both ranges hold wins; ranges which do not overlap have none, and an
    /// inverted range is empty.
    #[test]
    fn negotiation_picks_the_newest_common_version() {
        let cases = [
            ((1, 1), (1, 1), Some(1)),
            ((1, 2), (1, 1), Some(1)),
            ((1, 1), (1, 2), Some(1)),
            ((1, 3), (2, 5), Some(3)),
            ((2, 5), (1, 3), Some(3)),
            ((1, 1), (2, 3), None),
            ((2, 3), (1, 1), None),
            ((1, 1), (0, 0), None),
            ((1, 3), (3, 2), None),
        ];

        for ((min, max), (their_min, their_max), expected) in cases {
            let ours = synthetic::envelope(min, max);
            let theirs = synthetic::envelope(their_min, their_max);
            match (ours.negotiate(theirs), expected) {
                (Ok(version), Some(expected)) => assert_eq!(version, expected),

                (Err(HandshakeError::Incompatible { ours, theirs }), None) => {
                    assert_eq!(ours, min..=max);
                    assert_eq!(theirs, their_min..=their_max);
                }

                (negotiated, expected) => {
                    panic!(
                        "{min}..={max} with {their_min}..={their_max}: {negotiated:?}, not {expected:?}"
                    )
                }
            }
        }
    }

    #[test]
    fn handshake_rejects_alien_magic() {
        let alien = HandshakeEnvelope {
            magic: !PROTOCOL_MAGIC,
            ..HandshakeEnvelope::this_build()
        };

        assert!(matches!(
            HandshakeEnvelope::this_build().negotiate(alien),
            Err(HandshakeError::Magic(magic)) if magic == !PROTOCOL_MAGIC
        ));
    }

    /// A frame other than a handshake has no envelope, and its bytes are not read as one.
    #[test]
    fn only_a_handshake_has_an_envelope() {
        let gossip = Frame::Gossip {
            members: Vec::new(),
            more: false,
        }
        .encode_into(Vec::new())
        .expect("frame encodes");

        assert_eq!(HandshakeEnvelope::decode(&gossip).expect("no error"), None);
        assert_eq!(HandshakeEnvelope::decode(&[]).expect("no error"), None);
    }

    /// A version 2 handshake decodes as a version 1 one, its appended field ignored, and a
    /// version 2 build reads a version 1 handshake with the schema of the version negotiated.
    #[test]
    fn a_handshake_body_extends_its_predecessor_by_appending() {
        let node = NodeId::new("127.0.0.1:1234".parse().expect("valid address"));
        let cluster = Some(ClusterId::new());
        let v2 = HandshakeV2 {
            node,
            intent: HandshakeIntent::Join,
            cluster,
            appended: 7,
        };
        let v1 = HandshakeV1 {
            node,
            intent: HandshakeIntent::Join,
            cluster,
        };

        let bytes = synthetic::encode(synthetic::envelope(1, 2), Body::V2(v2));
        let envelope = HandshakeEnvelope::decode(&bytes)
            .expect("envelope decodes")
            .expect("a handshake");
        assert_eq!(
            HandshakeEnvelope::this_build().negotiate(envelope).ok(),
            Some(1)
        );
        assert_eq!(HandshakeV1::decode(&bytes).expect("body decodes"), v1);
        assert_eq!(synthetic::decode(&bytes, 2), Body::V2(v2));

        let bytes = Frame::Handshake(Handshake::new(v1))
            .encode_into(Vec::new())
            .expect("frame encodes");
        let envelope = HandshakeEnvelope::decode(&bytes)
            .expect("envelope decodes")
            .expect("a handshake");
        let version = synthetic::envelope(1, 2)
            .negotiate(envelope)
            .expect("the ranges overlap");
        assert_eq!(version, 1);
        assert_eq!(synthetic::decode(&bytes, version), Body::V1(v1));
    }
}
