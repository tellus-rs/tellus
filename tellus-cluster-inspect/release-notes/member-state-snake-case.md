### A member's state is written in snake_case

`GET /cluster` and the `state` events of `GET /events` used to write the state of a member as
`"Up"`, `"Left"` or `"Down"`, beside tags and field names in snake_case. They now write `"up"`,
`"left"` or `"down"`.

A client which reads the JSON without the types of this crate has to match the new strings. A client
which deserializes into them has to be built against this release to read a node of this release,
and it then no longer reads a node of an earlier one.
