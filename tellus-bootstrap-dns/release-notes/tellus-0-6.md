### This release requires tellus 0.6

`DnsSeeds` implements `tellus::cluster::SeedDiscovery`, so the tellus it is built against is part of
this crate's API. This release depends on tellus 0.6, and its `DnsSeeds` cannot be passed to the
`cluster::bootstrap` of tellus 0.5. Upgrade tellus to 0.6 together with this crate.
