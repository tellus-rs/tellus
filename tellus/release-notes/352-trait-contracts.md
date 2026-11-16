### What implementations of the store, connection and discovery traits must guarantee

The documentation of `EventStore`, `SnapshotStore`, `Connection` and `SeedDiscovery` now states
guarantees tellus relies on, and a third-party implementation has to meet them.

A store call may be dropped before it completes: at the timeout of its `Persistence`, and for `read`
and `load` also when the actor is stopped during recovery. A dropped call must not panic or corrupt
the store, and the store must stay usable for later calls. Dropping a call does not end what it
started in a server, so a store bounds its own calls below the `Persistence` timeout.
`EventStore::append` no longer promises that a failed append leaves the stream untouched; only a
conflict does. Any other failure may be ambiguous, like a dropped append, and replay reconciles it
as docs/persistence.md describes under "Store timeout".

The stream halves of a `Connection` must keep the connection open and stay usable after the
`Connection` value is dropped. Dropping the last half must close the connection.
`Connection::accept_data` must be cancel safe, so a dropped future must not lose a stream.
`FrameReceiver::recv` returns `None` once the peer has closed the stream, or once the connection
was closed between two frames and what arrived before has been received. A connection which fails,
or closes in the middle of a frame, makes `recv` fail.

`SeedDiscovery::resolve` must resolve one address per node and must bound itself. A source yielding
no address at all must fail rather than return an empty list. `FixedSeeds::new` hence refuses an
empty list with the new error `NoSeeds`, and so does deserializing `FixedSeeds`. An empty list used
to let a node with `min_nodes` of one form a cluster alone. Code calling `FixedSeeds::new` has to
handle the `Result`.
