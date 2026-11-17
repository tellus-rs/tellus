### cluster-dev adds only the dev transport

The `cluster-dev` feature now adds `QuicTransport::dev` and nothing else.
`cluster::sever_connections`, `cluster::gossip_bytes` and `cluster::drop_terminated_frames` are no
longer part of it. They moved to `__internal`, tellus's own test and bench support, which is not
public API.
