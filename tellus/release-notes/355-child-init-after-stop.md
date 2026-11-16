### A child stopped before it ran no longer runs `init`

A child whose parent stops it before the child's task has run now terminates without running
`init`. It used to run `init` first, with every send, spawn and connection made there, and only then
stopped. A child spawned in the same `receive` which returns `Control::Stop` is the common case. The
same holds for a root whose `ActorSystem::stop` comes before the root ran. An event-sourced actor
stopped before it ran already skipped recovery, which is now documented: the store sees no load and
no read, and neither `init`, `init_from_snapshot` nor `recovered` runs.

Such an actor still runs the termination sequence. Its watchers get an ordinary terminated signal,
supervision is not consulted, and its parent terminates only behind it. Code which relied on `init`
running in this case, for example to hand out a reference or to acknowledge a start, must not count
on it: wait for the actor to be up before stopping its parent.
