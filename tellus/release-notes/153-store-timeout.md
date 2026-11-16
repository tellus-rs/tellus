### Every store call of an event-sourced actor is bounded by a timeout

`Persistence` now bounds every store call by a timeout, `Persistence::DEFAULT_TIMEOUT`, 10 s, unless
`Persistence::with_timeout` sets another. `with_timeout` refuses zero with the new error
`InvalidPersistence::ZeroTimeout`. The timeout bounds the snapshot load and every page read of a
recovery, and the append and the snapshot save of a settlement, each on its own. A load, read or
append which exceeds it fails the actor into its supervision strategy, and a snapshot save which
exceeds it is logged like a failed save. A store which never answered used to keep the actor, and
every ancestor waiting for it, from terminating.

The stop guarantee changes. A shutdown used to wait for an append under way until it settled, so it
never left an append unsettled. It now waits at most the timeout for the append, and at most the
timeout again for a due snapshot save. An append which exceeds the timeout is ambiguous, and the
logged error says so: its events may have been stored. The replay of the next incarnation picks them
up if they have landed by then. An append which lands only after that replay makes the next append
conflict, and the command of that next append fails into supervision.

The timeout gives up on a call by dropping it, which does not end what the call started in a server.
A store hence bounds its own calls below the timeout. For `PostgresStore`, the README of
tellus-persistence-postgres says how. A store whose calls can take longer than 10 s, for example a
replay page of very large events over a slow link, raises the timeout with `with_timeout`.
