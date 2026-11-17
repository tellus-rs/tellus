### This release requires tellus 0.6

The router reads the cluster state of the tellus endpoint it runs in and serves tellus types, so
the tellus it is built against is part of this crate's API. This release depends on tellus 0.6 and
does not work with an endpoint of tellus 0.5. Upgrade tellus to 0.6 together with this crate.
