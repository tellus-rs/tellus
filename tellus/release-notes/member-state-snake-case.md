### MemberState serializes in snake_case

`cluster::MemberState` used to serialize as `"Up"`, `"Left"` and `"Down"`. It now serializes as
`"up"`, `"left"` and `"down"`, like `Lifecycle`, like the tags of `Transition` and `Change` and like
its own `Display`. The `state` of a serialized `Member` changes with it, and with that the
serialized form of every type which holds a `Member`: `ClusterState`, `BrokenLink`, `Transition` and
`Change`.

Code which reads the JSON of these types without them has to match the new strings. Code which
deserializes into them reads only what 0.6 or later has serialized, and code built against an
earlier release does not read what 0.6 serializes. The same holds for every other format which
writes the name of a variant, such as CBOR. Gossip is not affected: a frame carries the state as the
index of its variant.
