### A cluster of 0.5 nodes cannot be upgraded node by node

The cluster protocol is now version 2, and a 0.6 node speaks nothing else. A 0.5 node and a 0.6
node refuse each other at the handshake, so they never form one cluster. To upgrade, stop every
node of the 0.5 cluster and start the 0.6 nodes as a new cluster. Rolling upgrades work from 0.6
on, which is what version 2 is for: every payload now carries the schema version of its type.

Every type which crosses nodes, as a message, a reply or a published item, implements
`cluster::Schema`, which `Manifest` now requires. For a type which keeps its shape, the defaults are
enough:

```rust
use tellus::cluster::Schema;

impl Schema for Greet {}
```

tellus implements `Schema` for the primitives, `String`, `()`, `Option` and `Vec` of those, and for
`ActorRef` and `ReplyTo`, but not for a container of an application type such as
`Vec<Greet>` or `Result<Greet, Error>`. Such a message crosses nodes in a type of the
application. How to change a type while old and new nodes run side by side is described in
[Rolling upgrades](https://github.com/tellus-rs/tellus/blob/main/docs/cluster.md#rolling-upgrades).
