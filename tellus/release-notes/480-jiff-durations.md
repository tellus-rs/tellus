### Config durations are parsed by jiff

With the `serde` feature, the durations in a config are now parsed by jiff's friendly format
instead of humantime, which is unmaintained (RUSTSEC-2026-0542). Seconds, minutes, hours and their
fractions parse as before, so `10s`, `500ms` and `1h 30m` keep their meaning.

Days, weeks, months and years no longer parse, and neither does a bare `0`. A config using them
fails to load: write `24h` instead of `1d`, and `0s` instead of `0`. ISO 8601 durations such as
`PT10S` parse now.
