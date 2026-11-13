### Every actor declares its stash mode

`Actor` and `EventSourced` have a new associated type, `Stash`, next to `Timers`. It decides whether
an actor can stash: `Stashed` gives its context `stash` and `unstash_all`, `Unstashed` gives it
neither, and its run loop holds no stash code. An actor which does not stash declares `Unstashed`:

```rust
use tellus::{Actor, Unstashed, Untimed};

impl Actor for Greeter {
    type Message = Greet;
    type Timers = Untimed;
    type Stash = Unstashed;
    // ...
}
```

Nothing else changes for such an actor. `ActorContext` takes the stash mode as a third type
parameter, which defaults to `Unstashed`, so a signature naming `ActorContext<Self::Message,
Self::Timers>` still compiles. The stash itself is described in
[Stashing](https://github.com/tellus-rs/tellus/blob/main/docs/actors.md#stashing).
