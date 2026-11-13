### Cluster durations which switch off a safeguard at zero are refused

A zero `down_retention`, `leave_timeout` or `ambiguous_join_bound` in `EndpointConfig` is now
refused, by `start_endpoint` and by deserialization, with `InvalidEndpointConfig::ZeroDownRetention`,
`ZeroLeaveTimeout` or `ZeroAmbiguousJoinBound`. Each of them used to switch off what the setting is
for: a zero `leave_timeout`, for example, severed a departing node's connections without waiting for
their drain.

`PhiAccrual::with_warmup_deadline` now returns a `Result` and refuses zero with
`InvalidPhiAccrual::WarmupDeadline`. A zero warmup deadline declared a peer unavailable one tick
after its first heartbeat. A config file with `warmup_deadline: 0s` fails to deserialize.

Where one of these four is zero, set a positive duration, or leave the key out for its default.

Zero stays valid for `keep_majority.after`, `down_after_deadline.after`, `settle` and
`majority_with_fallback.after`, and for `acceptable_pause` in `PhiAccrual`. An endpoint refuses a
zero `acceptable_pause` with the default intervals, though, unless `min_std_deviation` is raised.
`docs/cluster.md` says what zero means for each.
