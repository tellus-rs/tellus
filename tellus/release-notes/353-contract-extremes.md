### The store contract suite checks persistence IDs and read limits at their extremes

`persistence_tests` has five new checks, and `event_store_contract` and `snapshot_store_contract`
run them. A store which passed the suite before can fail it now.

`events_round_trip_under_persistence_ids_at_their_extremes` and
`snapshots_round_trip_under_persistence_ids_at_their_extremes` use persistence IDs whose entity
type or entity ID is exactly 255 bytes long, in ASCII and in multi-byte characters. They also use
segments holding non-ASCII characters, `%`, `_`, single and double quotes, a backslash and spaces.
A store with keys shorter than 255 bytes, or with a character set short of four-byte characters,
fails them.

`streams_are_isolated_between_lookalike_ids` and `snapshots_are_isolated_between_lookalike_ids`
require two streams and two snapshots for IDs which differ only in case, in Unicode normalization,
in a trailing space or in the last byte of a 255-byte segment. The same holds for an ID holding `%`,
`_` or a backslash and an ID it matches as a `LIKE` pattern, and for an ID and its prefix. A store
with a case-insensitive or padding collation, a normalizing or truncating key, a `LIKE` match or a
prefix match fails them.

`reads_honor_limits_at_their_extremes` reads with limit 1 and with limit `usize::MAX`. The latter
must return the whole rest of the stream. A store which casts the limit to a narrower integer
without clamping it fails this check.
