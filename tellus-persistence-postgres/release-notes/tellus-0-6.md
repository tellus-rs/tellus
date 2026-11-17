### This release requires tellus 0.6

`PostgresStore` implements `tellus::EventStore` and `tellus::SnapshotStore`, so the tellus it is
built against is part of this crate's API. This release depends on tellus 0.6, and its
`PostgresStore` cannot back an event-sourced actor of tellus 0.5. Upgrade tellus to 0.6 together
with this crate.
