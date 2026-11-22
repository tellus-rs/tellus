### Termination closes the mailbox

A message sent while an actor terminated could slip into its mailbox after the mailbox had been
drained. It was never received, not logged, and it stayed in memory, together with everything it
captured, while any `ActorRef` to the terminated actor lived. An `ask` whose request it was could
time out, unless every `ActorRef` was dropped before its deadline.

Termination now closes and drains the mailbox in one step. A message still queued at that point is
dropped during termination, so a request's `ReplyTo` resolves its ask as `NoReply`, unless the ask's
deadline expires first. A local send after the closure is refused: `tell` logs a dead letter and
`ask` returns `AskError::ActorTerminated`. A request from another node can still end as `NoReply` or
`Timeout`, since it is refused only once it arrives. Nothing needs to change in code using tellus.
