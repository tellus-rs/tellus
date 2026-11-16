### Bootstrap bounds every join attempt and decides formation on a fresh view

`BootstrapConfig` has a new field, `join_attempt_timeout`, 5 s by default
(`BootstrapConfig::DEFAULT_JOIN_ATTEMPT_TIMEOUT`), with the YAML key of the same name. A join round
used to wait for each address until the transport gave up, which takes 30 s for QUIC towards a
silent address. Two silent addresses below a live member hence used up the 60 s timeout the
bootstrap READMEs recommend, so a restarted node could keep timing out without ever asking a
healthy member. Now a round goes on to the next address once an attempt has run for
`join_attempt_timeout`, and counts that address as unavailable. An attempt which has sent its `Join`
handshake still runs to its conclusion, so an admission or an ambiguous join is recorded as before.
A zero timeout is refused with the new variant `InvalidBootstrapConfig::ZeroJoinAttemptTimeout`.

Bootstrap also resolves the discovery again after a round it would decide formation on, and decides
only if that resolution shows the same settled universe. Before, it decided on the universe resolved
before the round. This node could hence form a cluster while a lower address which registered during
a long round formed a second one.

A `BootstrapConfig` built with a struct literal needs the new field, e.g. through
`..BootstrapConfig::new()`, and a `match` on `InvalidBootstrapConfig` has to handle the new variant.
