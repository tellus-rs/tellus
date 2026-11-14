### The payload codec is an enum, and a custom codec is no longer possible

`cluster::codec::Codec` is now an enum with one variant, `Codec::Postcard`, instead of a trait
implemented by `Postcard`. The trait had to be object safe, so every payload was serialized
through `erased_serde`, one dynamic call per field and per element. A message carrying a
`Vec<u8>` of 64 KiB decoded at 21 MiB/s on Linux. Without the trait, encoding and decoding are
static calls into postcard. On a 10-core arm64 machine, bulk transfer went from 97 to 713 MiB/s
and pipelined round trips from 534,000 to 1,462,000 per second. The bytes on the wire are
unchanged.

`EndpointConfig::codec` is a `Codec` instead of an `Arc<dyn Codec>`. Code assigning
`Arc::new(Postcard)` assigns `Codec::Postcard`, which is also the default. In a config file,
`codec: postcard` stays as it is. `Postcard`, `DecodeVisitor` and
`InvalidEndpointConfig::CodecNameTooLong` are gone, and an application's own `impl Codec` has no
replacement. Another format is a new variant of `Codec`; open an issue for the one you need.

A `Schema` override receives the codec by value, as `codec: Codec`, and decodes an older shape
with `codec.decode::<OldShape>(payload)`.
