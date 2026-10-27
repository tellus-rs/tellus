# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1](https://github.com/tellus-rs/tellus/compare/tellus-v0.4.0...tellus-v0.4.1) - 2026-10-27

### Added

- let an actor read its own mailbox depth

### Fixed

- *(cluster)* drain a departure over a connection still being set up

### Other

- *(cluster)* recover from ports taken before a node binds them

## [0.4.0](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.9...tellus-v0.4.0) - 2026-10-21

### Added

- add remoting and cluster support

### Changed

- add shared, versioned remoting reachability observations
- borrow static persistence manifests through `Cow<'static, str>`

### Fixed

- fail fenced lookups and clean up frames cancelled with a lost connection
- cache final outbound admission failures instead of redialing per message
- tolerate stale SRV targets when another target resolves

### Performance

- serialize frame payloads as byte slices and remove avoidable remoting hot-path locking and hashing

## [0.3.9](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.8...tellus-v0.3.9) - 2026-10-19

### Other

- describe restarts by their count instead of a streak

## [0.3.8](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.7...tellus-v0.3.8) - 2026-10-19

### Other

- add example anchors for the site and rename the cellar device

## [0.3.7](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.6...tellus-v0.3.7) - 2026-10-19

### Other

- add crate keywords and categories for search

## [0.3.6](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.5...tellus-v0.3.6) - 2026-10-19

### Other

- add event-sourced tutorial, how-tos and explanation examples
- fix flaky error formatting assertions with a global subscriber

## [0.3.5](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.4...tellus-v0.3.5) - 2026-10-18

### Fixed

- yield the worker from busy actors via the cooperative budget
- contain panics in logged and dropped errors

## [0.3.4](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.3...tellus-v0.3.4) - 2026-10-18

### Added

- add ActorContext::pipe_to_self

### Other

- drop blank lines before a closing Ok

## [0.3.3](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.2...tellus-v0.3.3) - 2026-10-18

### Other

- unify the examples into a getting started tutorial
- point links and crate homepage to tellus.rs

## [0.3.2](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.1...tellus-v0.3.2) - 2026-10-17

### Other

- move repo links and code owners to tellus-rs
- drop redundant homepage from manifests

## [0.3.1](https://github.com/hseeberger/tellus/compare/tellus-v0.3.0...tellus-v0.3.1) - 2026-10-12

### Added

- add TestContext behind test-util for unit testing actors

### Other

- migrate profiling to hotpath cloud
- replace TestStore with FaultyStore over InMemoryStore

## [0.3.0](https://github.com/hseeberger/tellus/compare/tellus-v0.2.1...tellus-v0.3.0) - 2026-09-13

### Added

- add hosts, spawning/passivating managed entities
- add in-memory store behind persistence-in-memory feature
- stop an actor system from the outside

## [0.2.1](https://github.com/hseeberger/tellus/compare/tellus-v0.2.0...tellus-v0.2.1) - 2026-08-30

### Fixed

- harden backoff config, recovery drop and bench sizing

### Other

- harden core and persistence guarantees
- cut nesting and duplication in core and persistence
- pin bench runtime threads and raise sampling

## [0.2.0](https://github.com/hseeberger/tellus/compare/tellus-v0.1.0...tellus-v0.2.0) - 2026-08-27

### Added

- add event-sourced persistence
- add hotpath-based profiling behind hotpath feature

### Other

- use independent versions per crate
- install via cargo add instead of pinned versions
- refresh docs after persistence landed

## [0.1.0](https://github.com/hseeberger/tellus/releases/tag/tellus-v0.1.0) - 2026-08-24

### Other

- rename ferrier to tellus
