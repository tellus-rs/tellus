### Bound the pool's calls below tellus's store timeout

tellus now bounds every store call by the timeout of `Persistence`, 10 s by default, and gives up
on a call which exceeds it by dropping it. A dropped call does not end a statement which has
reached PostgreSQL. sqlx sends no cancel request, so the statement runs on and keeps its pool
connection, and a dropped append can still commit after the actor's next incarnation has replayed.
The next append of that actor then conflicts and fails its command.

Configure the pool to bound its calls itself: a `statement_timeout` on its connections, for example
through `PgConnectOptions::options`, and an acquire timeout through
`PgPoolOptions::acquire_timeout`. Keep the acquire timeout plus twice the statement timeout below
the `Persistence` timeout, with room for the network round trips. The statement timeout does not
count the wait for a connection, and it can count twice. sqlx prepares a statement on its first use
on a connection in a separate round trip which ends with a Sync. PostgreSQL cancels the statement
timeout when that Sync completes and starts it again with the Bind which follows. sqlx's default
acquire timeout, 30 s, is above tellus's default. Neither the statement timeout nor
`transaction_timeout` covers the commit which ends a statement, so an append which fails while it
commits stays ambiguous. The README shows the configuration under
[Usage](https://github.com/tellus-rs/tellus/blob/main/tellus-persistence-postgres/README.md#usage),
and its new section
[Timeouts](https://github.com/tellus-rs/tellus/blob/main/tellus-persistence-postgres/README.md#timeouts)
explains it and what stays ambiguous.

A shutdown of an event-sourced actor now waits for an append under way at most the `Persistence`
timeout, where it used to wait until the append settled.
