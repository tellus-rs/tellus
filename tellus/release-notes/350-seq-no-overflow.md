### Sequence numbers refuse to overflow instead of saturating

`SeqNo::succ` and `SeqNo::advanced_by` now return an `Option<SeqNo>`, which is `None` beyond
`u64::MAX`. They used to saturate there, so two events could end up at the same position.

A store which calls either function handles `None` by failing the append, for example with
`AppendError::Store` and an error of its own, and never stores a position twice. The new
`SeqNoOverflow` error describes such an append. `InMemoryStore` returns it, so its `EventStore::Error`
is now `SeqNoOverflow` instead of `Infallible`. Code which matched that error as uninhabited, with
`match error {}`, now has to handle it.

The event-sourced run loop checks the position before it appends. A command whose events would
advance the stream beyond `u64::MAX` fails the actor under its supervision, and nothing is appended.
Recovery fails at a stored event at `u64::MAX`.
