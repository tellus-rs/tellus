### Directory reads fail once this node has left or is down

`receptionist::lookup`, `receptionist::lookup_at`, `Subscription::current` and `pubsub::publish`
now fail with the new `ReceptionistError::NoLongerMember` once this node's own member entry is
Left or Down, because it left the cluster, was downed or downed itself. Before, they answered an
empty list or `Ok(0)`, as if nobody were registered, since the directory lists the registrations
of Up members only and so left out this node's own. A node which is leaving answers as usual until
its entry is Left, and a node which never formed or joined a cluster is unaffected.

`ReceptionistError` has a new variant, so an exhaustive match on it needs an arm for
`NoLongerMember`. A caller which treated an empty answer after `cluster::leave` or a downing as
"nobody registered" handles the error instead, usually by stopping, since only a restarted process
can rejoin.
