### A blank or malformed query is refused, and a failing SRV target is logged

`DnsSeeds::new`, `DnsSeeds::srv`, `DnsSeeds::ip` and the deserialization of `Query` refuse a blank
name and one which is no valid DNS name, such as a label over 63 bytes or an empty label. Before,
either failed on every resolve while bootstrap retried it forever. The refusals are the new
`DnsSeedsError::Config(InvalidQuery::BlankName)` and `InvalidQuery::MalformedName`. `DnsSeedsError`
is therefore an enum now, and a resolution failure is its `Resolve` variant. Code matching on
`DnsSeedsError` or naming its tuple field has to match the variants instead.

`with_resolver` is now a constructor taking the `Query` and the resolver, so a host without a
system resolver configuration can build a `DnsSeeds` as well. Replace
`DnsSeeds::srv(name)?.with_resolver(resolver)` by
`DnsSeeds::with_resolver(Query::Srv(name), resolver)?`.

An SRV target which fails to resolve while another target resolves is logged at warn level, naming
the target and the error. It is still skipped, so the resolved view is partial, and the log now
shows it.
