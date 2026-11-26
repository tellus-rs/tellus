# Changelog

## [0.3.1](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.3.0...tellus-persistence-postgres-v0.3.1) - 2026-11-26

### Bug Fixes

- *(postgres)* Answer Conflict to a stale writer at the BIGINT limit ([#562](https://github.com/tellus-rs/tellus/pull/562))

### Documentation

- *(persistence)* Document the BIGINT limit and failure handling ([#567](https://github.com/tellus-rs/tellus/pull/567))


## [0.3.0](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.14...tellus-persistence-postgres-v0.3.0) - 2026-11-20

### Features

- *(persistence)* [**breaking**] Bound every store call by a timeout ([#409](https://github.com/tellus-rs/tellus/pull/409))
- Add stashing, opt-in per actor via Actor::Stash ([#201](https://github.com/tellus-rs/tellus/pull/201))

### Bug Fixes

- *(postgres)* [**breaking**] Rename the store's tables so they cannot collide ([#328](https://github.com/tellus-rs/tellus/pull/328))
- [**breaking**] Refuse a NUL in a persistence ID, map positions beyond BIGINT ([#308](https://github.com/tellus-rs/tellus/pull/308))
- Record the PostgreSQL store's migrations in a table of its own ([#305](https://github.com/tellus-rs/tellus/pull/305))

### Refactor

- Replace Infallible with ! and require Rust 1.100 ([#516](https://github.com/tellus-rs/tellus/pull/516))

### Documentation

- *(postgres)* Count the statement timeout twice in the pool bounds ([#507](https://github.com/tellus-rs/tellus/pull/507))
- Regenerate changelogs with the unified config ([#505](https://github.com/tellus-rs/tellus/pull/505))
- Qualify the replay claim in the postgres readme and the roadmap ([#501](https://github.com/tellus-rs/tellus/pull/501))
- Clarify cluster guarantees and complete the READMEs ([#475](https://github.com/tellus-rs/tellus/pull/475))
- Fix review findings in READMEs, roadmap, demo and examples ([#477](https://github.com/tellus-rs/tellus/pull/477))
- Fix the review's core and persistence doc findings ([#476](https://github.com/tellus-rs/tellus/pull/476))
- Add release notes for the breaking changes since 0.5.3 ([#321](https://github.com/tellus-rs/tellus/pull/321))
- Give the PostgreSQL store's schema as the supported definition ([#315](https://github.com/tellus-rs/tellus/pull/315))

### Styling

- Trim example comments, rename the manager's message, per review ([#474](https://github.com/tellus-rs/tellus/pull/474))
- *(persistence)* Order items, narrow fields, shorten paths per review ([#469](https://github.com/tellus-rs/tellus/pull/469))

### Miscellaneous Tasks

- Rename docker-compose.yaml to compose.yaml ([#241](https://github.com/tellus-rs/tellus/pull/241))


## [0.2.14](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.13...tellus-persistence-postgres-v0.2.14) - 2026-11-01

### Documentation

- Correct and complete the documentation ([#237](https://github.com/tellus-rs/tellus/pull/237))

## [0.2.13](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.12...tellus-persistence-postgres-v0.2.13) - 2026-11-01

### Features

- Re-export sqlx from tellus-persistence-postgres ([#227](https://github.com/tellus-rs/tellus/pull/227))

### Documentation

- Fix the findings of the documentation review ([#225](https://github.com/tellus-rs/tellus/pull/225))

## [0.2.12](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.11...tellus-persistence-postgres-v0.2.12) - 2026-10-29

### Other

- updated the following local packages: tellus

## [0.2.11](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.10...tellus-persistence-postgres-v0.2.11) - 2026-10-28

### Features

- Add timers, opt-in per actor via Actor::Timers ([#185](https://github.com/tellus-rs/tellus/pull/185))

## [0.2.10](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.9...tellus-persistence-postgres-v0.2.10) - 2026-10-21

### Features

- Add remoting and cluster support ([#1](https://github.com/tellus-rs/tellus/pull/1))

## [0.2.9](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.8...tellus-persistence-postgres-v0.2.9) - 2026-10-19

### Other

- updated the following local packages: tellus

## [0.2.8](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.7...tellus-persistence-postgres-v0.2.8) - 2026-10-19

### Other

- updated the following local packages: tellus

## [0.2.7](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.6...tellus-persistence-postgres-v0.2.7) - 2026-10-19

### Miscellaneous Tasks

- Add crate keywords and categories for search ([#143](https://github.com/tellus-rs/tellus/pull/143))

## [0.2.6](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.5...tellus-persistence-postgres-v0.2.6) - 2026-10-19

### Documentation

- Add event-sourced tutorial, how-tos and explanation examples ([#142](https://github.com/tellus-rs/tellus/pull/142))

## [0.2.5](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.4...tellus-persistence-postgres-v0.2.5) - 2026-10-18

### Other

- updated the following local packages: tellus

## [0.2.4](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.3...tellus-persistence-postgres-v0.2.4) - 2026-10-18

### Styling

- Drop blank lines before a closing Ok ([#133](https://github.com/tellus-rs/tellus/pull/133))

## [0.2.3](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.2...tellus-persistence-postgres-v0.2.3) - 2026-10-18

### Documentation

- Unify the examples into a getting started tutorial ([#130](https://github.com/tellus-rs/tellus/pull/130))
- Point links and crate homepage to tellus.rs ([#128](https://github.com/tellus-rs/tellus/pull/128))

## [0.2.2](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.1...tellus-persistence-postgres-v0.2.2) - 2026-10-17

### Miscellaneous Tasks

- Drop redundant homepage from manifests ([#118](https://github.com/tellus-rs/tellus/pull/118))

## [0.2.1](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.2.0...tellus-persistence-postgres-v0.2.1) - 2026-10-12

### Other

- updated the following local packages: tellus

## [0.2.0](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.1.1...tellus-persistence-postgres-v0.2.0) - 2026-09-13

### Other

- updated the following local packages: tellus, tellus

## [0.1.1](https://github.com/tellus-rs/tellus/compare/tellus-persistence-postgres-v0.1.0...tellus-persistence-postgres-v0.1.1) - 2026-08-30

### Bug Fixes

- Harden backoff config, recovery drop and bench sizing ([#63](https://github.com/tellus-rs/tellus/pull/63))

### Refactor

- Harden core and persistence guarantees ([#62](https://github.com/tellus-rs/tellus/pull/62))

## [0.1.0](https://github.com/tellus-rs/tellus/releases/tag/tellus-persistence-postgres-v0.1.0) - 2026-08-27

### Features

- Add event-sourced persistence ([#21](https://github.com/tellus-rs/tellus/pull/21))

### Documentation

- Install via cargo add instead of pinned versions ([#51](https://github.com/tellus-rs/tellus/pull/51))
- Refresh docs after persistence landed ([#47](https://github.com/tellus-rs/tellus/pull/47))
