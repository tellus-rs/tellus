### The Json codec refuses NaN and infinite floats

`Json` used to write a NaN or an infinite float as `null`, and read `Some(f64::NAN)` back as `None`.
It now refuses to encode such a value. A command whose event holds one fails the actor under its
supervision, and nothing is appended. A snapshot of a state which holds one is skipped with a
warning.

A `null` which an earlier release stored for a float stays in the store, and the type which wrote it
cannot read it back. To recover such a stream, override `Versioned::decode` and decode a shape with
an `Option<f64>` in that place. Which non-finite value it was is lost.
