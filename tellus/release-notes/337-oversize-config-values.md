### Oversize cluster config values are capped or refused

A duration in `EndpointConfig` or `BootstrapConfig` which the clock cannot represent, e.g.
`Duration::MAX`, is now capped at about thirty years, as a timer's delay is. It used to panic: in
`start_endpoint` and in deserialization for `heartbeat_interval` and `watch_refresh_interval`, in
`leave` and on every member learning of a departure for `leave_timeout`, on an ambiguous join for
`ambiguous_join_bound`, and in the probe task for `probe_interval`. With the default failure
detector, `start_endpoint` now refuses such a `heartbeat_interval` or `watch_refresh_interval` with
`InvalidEndpointConfig::FailureDetectorTooEager`.

`change_capacity` is now refused above 65536 with `InvalidEndpointConfig::ChangeCapacity`. The
channel allocates every entry when the endpoint starts, and the old bound, half the address space,
let `start_endpoint` panic allocating it, for example at 2^62.

`PhiAccrual::with_max_sample_size` now refuses a window above `PhiAccrual::MAX_MAX_SAMPLE_SIZE`,
10000, with the new variant `InvalidPhiAccrual::MaxSampleSizeTooLarge`, and so does
deserialization. The window grows by one interval of eight bytes per inbound frame and peer up to
that size, so a very large value let it grow for as long as the endpoint ran. A `match` on
`InvalidPhiAccrual` has to handle the new variant.

Where `change_capacity` or `max_sample_size` is above its new bound, lower it, or leave the key out
for its default.

`QuicTransport::new` takes the idle timeout as a new last parameter and sets it on the given
`TransportConfig`, replacing whatever that held. The idle timeout is what bounds a handshake toward
a silent address, and a config without one left such a handshake unbounded. `new` refuses an idle
timeout which is not at least one whole millisecond above the 10 s keep-alive it sets, since an idle
connection would then close between two keep-alives, with the new variant
`QuicTransportError::IdleTimeoutTooShort`. It refuses one QUIC cannot encode, above 2^62
milliseconds, with `QuicTransportError::IdleTimeoutTooLong`. `mutual_tls`, `from_config` and `dev`
use `QuicTransport::DEFAULT_IDLE_TIMEOUT`, 30 s, which is quinn's default and hence what they used
before. A caller of `new` passes `QuicTransport::DEFAULT_IDLE_TIMEOUT` to keep the previous
behavior, and a `match` on `QuicTransportError` has to handle the two new variants.