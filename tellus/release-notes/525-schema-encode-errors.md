### `Schema::encode` fails only where the codec fails

An `encode` override is no longer meant to refuse a value which the shape it encodes for a peer
cannot hold, and `CodecError::make_encoding` is no longer public. An override which returned an
error of its own for such a value maps the value to a representation the older peer understands
instead: an old variant, a default, or the value without the data the old shape cannot represent.
It passes that representation to `Codec::encode`, propagates any error `Codec::encode` returns, and
returns the encoded payload on success. tellus still treats any error an override returns as
before. A tell is a dead letter, and the request of an ask fails the ask as `NotEncodable`. A reply
is logged as a dead letter, and its proxy attempts `ReplyDropped`.
