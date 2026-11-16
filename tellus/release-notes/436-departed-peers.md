### A departure lists the peers which left themselves

`Departure` has a fourth list, `departed`: the peers which left the cluster themselves before they
acknowledged this node's drain. Two nodes leaving at the same time used to report each other as
unacknowledged, or as timed out after `leave_timeout`. Now each one's `DrainAck` still counts after
the other's departure has been seen, and a drain towards a peer which has left is reported as
departed, since whether it got what was queued for it is unknown.

`Departure::is_acknowledged` stays strict and is false while `departed` is not empty, although
`unacknowledged` and `timed_out` may both be empty then. Code which checks only those two lists to
decide whether a departure failed should use the new `Departure::is_complete` instead, which accepts
acknowledged and departed peers alike.

A departure no longer waits for a node which is Down when it begins, nor lists it. Its death
already turned what was queued for it into dead letters or failed asks, so a leave right after a
node died used to wait out `leave_timeout` for nothing.
