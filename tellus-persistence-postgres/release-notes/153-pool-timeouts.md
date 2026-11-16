### Bound the pool's calls below tellus's store timeout

tellus now bounds every store call by the timeout of `Persistence`, 10 s by default, and gives up
on a call which exceeds it by dropping it. A dropped call does not end a statement which has
reached PostgreSQL. sqlx sends no cancel request, so the statement runs on and keeps its pool
connection, and a dropped append can still commit after the actor's next incarnation has replayed.
The next append of that actor then conflicts and fails its command.

Configure the pool to bound its calls itself: a `statement_timeout` on its connections, for example
through `PgConnectOptions::options`, and an acquire timeout through
`PgPoolOptions::acquire_timeout`. Keep their sum below the `Persistence` timeout, since the
statement timeout does not count the wait for a connection. sqlx's default acquire timeout, 30 s, is
above tellus's default. Neither the statement timeout nor `transaction_timeout` covers the commit
which ends a statement, so an append which fails while it commits stays ambiguous. The new section
[Timeouts](https://github.com/tellus-rs/tellus/blob/main/tellus-persistence-postgres/README.md#timeouts)
of the README shows the configuration and what stays ambiguous.

A shutdown of an event-sourced actor now waits for an append under way at most the `Persistence`
timeout, where it used to wait until the append settled.
