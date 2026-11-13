### The store's tables are now `tellus_events` and `tellus_snapshots`

The tables were called `events` and `snapshots`. An application sharing the database may have a
table of such a name itself. `migrate` then left that table as it was and reported success, and the
store failed at its first statement against that table. With the new names `migrate` also no longer
adopts a table: it fails if `tellus_events` or `tellus_snapshots` exists and it has not created it.

0.3 does not read the old tables. In a database which 0.2.14 or earlier has migrated, the events
and snapshots stay in `events` and `snapshots` until they are moved. To move them, stop every
process which uses the store, those of the earlier release too. Then call `migrate` of 0.3, which
creates the two new tables next to the old ones, and run these statements before any process uses
the store again:

```sql
BEGIN;
INSERT INTO tellus_events SELECT * FROM events;
DROP TABLE events;
INSERT INTO tellus_snapshots SELECT * FROM snapshots;
DROP TABLE snapshots;
COMMIT;
```

An application which calls `migrate` at its start needs a run which stops after it. Started without
the move, it finds every entity empty. What it then persists for an entity of the old tables makes
the copy fail, and the old tables stay as they are.

An application which created the two tables itself renames them with the tool which manages its
schema. It no longer calls `migrate`, which had adopted them. The definition with the new names is
under
[Schema](https://github.com/tellus-rs/tellus/blob/main/tellus-persistence-postgres/README.md#schema)
in the README.
