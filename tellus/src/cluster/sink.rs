use crate::{
    ActorId, AskError, SchemaVersion,
    cluster::{
        codec::{Codec, CodecError},
        endpoint::{self, EndpointInner},
        frame::Frame,
        lanes::LaneError,
        node::NodeId,
        reply::{self, ReplyTag},
        schema::{Peer, Schema},
    },
};
use derive_more::Debug;
use std::borrow::Cow;
use thiserror::Error;

type Encode<M> = fn(&M, &dyn Codec, &Peer) -> Result<(SchemaVersion, Vec<u8>), CodecError>;

#[derive(Debug)]
pub(crate) struct RemoteSink<M> {
    node: NodeId,
    #[debug(skip)]
    encode: Encode<M>,
}

impl<M> RemoteSink<M> {
    pub(crate) fn new(node: NodeId) -> Self
    where
        M: Schema,
    {
        Self {
            node,
            encode: M::encode,
        }
    }

    pub(crate) fn node(&self) -> NodeId {
        self.node
    }

    /// The minted reply tags are returned, so an ask giving up can release what it carried.
    pub(crate) fn try_send_message(
        &self,
        target: ActorId,
        message: M,
    ) -> Result<Vec<ReplyTag>, RemoteSendError> {
        let endpoint = endpoint::get().ok_or(RemoteSendError::EndpointNotStarted)?;
        let peer = endpoint.peer(self.node);

        encode_and_send(
            endpoint,
            self.node,
            || (self.encode)(&message, endpoint.codec(), &peer),
            |schema_version, payload, reply_tags| Frame::Message {
                target,
                reply_tags: reply_tags.to_vec(),
                schema_version,
                payload: Cow::Owned(payload),
            },
        )
    }
}

impl<M> Clone for RemoteSink<M> {
    fn clone(&self) -> Self {
        Self {
            node: self.node,
            encode: self.encode,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum RemoteSendError {
    #[error("remoting endpoint not started")]
    EndpointNotStarted,

    #[error(transparent)]
    Lane(#[from] LaneError),

    #[error(transparent)]
    Codec(#[from] CodecError),

    #[error("encoded frame of {len} bytes exceeds the maximum frame size of {max} bytes")]
    FrameTooLarge { len: usize, max: usize },
}

impl From<RemoteSendError> for AskError {
    fn from(error: RemoteSendError) -> Self {
        match error {
            RemoteSendError::Lane(LaneError::OutboundQueueFull(_)) => Self::MailboxFull,

            RemoteSendError::Lane(LaneError::NodeUnreachable(_)) => Self::ActorTerminated,

            RemoteSendError::EndpointNotStarted => Self::EndpointNotStarted,

            RemoteSendError::Codec(error) => Self::NotEncodable(error),

            RemoteSendError::FrameTooLarge { len, max } => Self::TooLarge { len, max },
        }
    }
}

pub(crate) fn encode_and_send<E, F>(
    endpoint: &EndpointInner,
    peer: NodeId,
    encode: E,
    into_frame: F,
) -> Result<Vec<ReplyTag>, RemoteSendError>
where
    E: FnOnce() -> Result<(SchemaVersion, Vec<u8>), CodecError>,
    F: FnOnce(SchemaVersion, Vec<u8>, &[ReplyTag]) -> Frame<'static>,
{
    let (encoded, reply_tags) = reply::record_minted(encode);
    send_encoded(endpoint, peer, encoded, reply_tags, into_frame)
}

/// The reply tags are the ones minted encoding the payload, discarded unless the frame is queued.
/// Stamping precedes queueing, else a reply arriving first finds its entry unstamped.
pub(crate) fn send_encoded<F>(
    endpoint: &EndpointInner,
    peer: NodeId,
    encoded: Result<(SchemaVersion, Vec<u8>), CodecError>,
    reply_tags: Vec<ReplyTag>,
    into_frame: F,
) -> Result<Vec<ReplyTag>, RemoteSendError>
where
    F: FnOnce(SchemaVersion, Vec<u8>, &[ReplyTag]) -> Frame<'static>,
{
    let frame = encoded
        .map_err(RemoteSendError::from)
        .map(|(schema_version, payload)| into_frame(schema_version, payload, &reply_tags));

    let stream = frame.as_ref().ok().and_then(Frame::stream_key);
    let mut cancellations = endpoint.pending_replies().stamp(&reply_tags, peer, stream);

    let sent = frame
        .and_then(|frame| admit_frame(frame, endpoint.config().max_frame_size.get()))
        .and_then(|frame| endpoint.send(peer, frame).map_err(RemoteSendError::from));

    match sent {
        Ok(()) => {
            cancellations.extend(endpoint.pending_replies().confirm(&reply_tags));
            reply::send_cancellations(endpoint, cancellations);
            Ok(reply_tags)
        }

        Err(error) => {
            endpoint.pending_replies().discard(&reply_tags);
            Err(error)
        }
    }
}

/// Synchronous, so an oversize ask fails at the send rather than as `NoReply` at the writer.
pub(crate) fn admit_frame(
    frame: Frame<'static>,
    max_frame_size: usize,
) -> Result<Frame<'static>, RemoteSendError> {
    let len = frame
        .encoded_len()
        .map_err(CodecError::encoding)
        .map_err(RemoteSendError::Codec)?;
    if len > max_frame_size {
        return Err(RemoteSendError::FrameTooLarge {
            len,
            max: max_frame_size,
        });
    }

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use crate::{
        ActorId, SchemaVersion,
        cluster::{
            frame::Frame,
            sink::{RemoteSendError, admit_frame},
        },
    };
    use std::borrow::Cow;

    /// Admission measures the whole frame, envelope included, not just the payload.
    #[test]
    fn admission_uses_the_exact_encoded_frame_size() {
        let frame = || Frame::Message {
            target: ActorId::new(),
            reply_tags: Vec::new(),
            schema_version: SchemaVersion::new(0),
            payload: Cow::Owned(vec![0; 31]),
        };
        let len = frame().encoded_len().expect("frame size");

        assert!(admit_frame(frame(), len).is_ok());

        assert!(matches!(
            admit_frame(frame(), len - 1),
            Err(RemoteSendError::FrameTooLarge { len: actual, max })
                if actual == len && max == len - 1
        ));
        assert!(31 < len);
        assert!(matches!(
            admit_frame(frame(), 32),
            Err(RemoteSendError::FrameTooLarge { len: actual, max: 32 }) if actual == len
        ));
    }
}
