### `migrate` records its migrations in a table of its own

`PostgresStore::migrate` records what it has applied in `_tellus_persistence_migrations` instead of
sqlx's shared `_sqlx_migrations`, so the store and an application's own sqlx migrations can share a
database. In a database which 0.2.14 or earlier has migrated, the old row stays in
`_sqlx_migrations`. It does no harm: `migrate` applies the schema once more, which changes nothing.

If the application adds its own sqlx migrator to such a database, remove that row first, or the
migrator fails with `VersionMissing(1)` or `VersionMismatch(1)`. The statement which removes it, and
when it is safe to run, are under
[Usage](https://github.com/tellus-rs/tellus/blob/main/tellus-persistence-postgres/README.md#usage).
