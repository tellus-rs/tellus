### Leaving an endpoint which never formed or joined a cluster fails

`cluster::leave` and `cluster::leave_on_terminated` on an endpoint which has neither formed nor
joined a cluster now fail with the new `LeaveError::NotAMember` and leave the endpoint as it was, so
it can still `form` or `join` a cluster. They used to succeed with an empty `Departure` and latch the
endpoint downed for good, so a later `form` or `join` failed. A join in progress does not count as a
membership.

A match on `LeaveError` handles the new variant. Code which leaves only once it has formed or joined
needs no other change.
