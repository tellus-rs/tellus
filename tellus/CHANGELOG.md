# Changelog

## [0.5.3](https://github.com/tellus-rs/tellus/compare/tellus-v0.5.2...tellus-v0.5.3) - 2026-11-01

### Documentation

- Correct and complete the documentation ([#237](https://github.com/tellus-rs/tellus/pull/237))

## [0.5.2](https://github.com/tellus-rs/tellus/compare/tellus-v0.5.1...tellus-v0.5.2) - 2026-11-01

### Bug Fixes

- Pass the crypto provider explicitly in QuicTransport ([#229](https://github.com/tellus-rs/tellus/pull/229))
- Require full pages from EventStore::read ([#228](https://github.com/tellus-rs/tellus/pull/228))

### Refactor

- Rename identifiers with British spellings ([#226](https://github.com/tellus-rs/tellus/pull/226))

### Documentation

- Fix the findings of the documentation review ([#225](https://github.com/tellus-rs/tellus/pull/225))

### Miscellaneous Tasks

- Use hotpath criterion adapter for benchmarks ([#199](https://github.com/tellus-rs/tellus/pull/199))
- Run the profile flood on a current-thread runtime ([#211](https://github.com/tellus-rs/tellus/pull/211))

## [0.5.1](https://github.com/tellus-rs/tellus/compare/tellus-v0.5.0...tellus-v0.5.1) - 2026-10-29

### Features

- Deliver a reply_to reply past a full mailbox ([#197](https://github.com/tellus-rs/tellus/pull/197))

### Bug Fixes

- Deliver a piped future's output past a full mailbox ([#196](https://github.com/tellus-rs/tellus/pull/196))

### Miscellaneous Tasks

- Use callgrind merged mode for hotpath benchmarks ([#179](https://github.com/tellus-rs/tellus/pull/179))

## [0.5.0](https://github.com/tellus-rs/tellus/compare/tellus-v0.4.0...tellus-v0.5.0) - 2026-10-28

### Features

- Add timers, opt-in per actor via Actor::Timers ([#185](https://github.com/tellus-rs/tellus/pull/185))
- Let an actor read its own mailbox depth ([#162](https://github.com/tellus-rs/tellus/pull/162))

### Bug Fixes

- *(cluster)* Drain a departure over a connection still being set up ([#170](https://github.com/tellus-rs/tellus/pull/170))

### Documentation

- Add a stale readings tutorial step and a batch writes example ([#191](https://github.com/tellus-rs/tellus/pull/191))

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

### Documentation

- Describe restarts by their count instead of a streak ([#147](https://github.com/tellus-rs/tellus/pull/147))

## [0.3.8](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.7...tellus-v0.3.8) - 2026-10-19

### Documentation

- Add example anchors for the site and rename the cellar device ([#145](https://github.com/tellus-rs/tellus/pull/145))

## [0.3.7](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.6...tellus-v0.3.7) - 2026-10-19

### Miscellaneous Tasks

- Add crate keywords and categories for search ([#143](https://github.com/tellus-rs/tellus/pull/143))

## [0.3.6](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.5...tellus-v0.3.6) - 2026-10-19

### Documentation

- Add event-sourced tutorial, how-tos and explanation examples ([#142](https://github.com/tellus-rs/tellus/pull/142))

## [0.3.5](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.4...tellus-v0.3.5) - 2026-10-18

### Bug Fixes

- Yield the worker from busy actors via the cooperative budget ([#138](https://github.com/tellus-rs/tellus/pull/138))
- Contain panics in logged and dropped errors ([#136](https://github.com/tellus-rs/tellus/pull/136))

## [0.3.4](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.3...tellus-v0.3.4) - 2026-10-18

### Features

- Add ActorContext::pipe_to_self ([#135](https://github.com/tellus-rs/tellus/pull/135))

### Styling

- Drop blank lines before a closing Ok ([#133](https://github.com/tellus-rs/tellus/pull/133))

## [0.3.3](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.2...tellus-v0.3.3) - 2026-10-18

### Documentation

- Unify the examples into a getting started tutorial ([#130](https://github.com/tellus-rs/tellus/pull/130))
- Point links and crate homepage to tellus.rs ([#128](https://github.com/tellus-rs/tellus/pull/128))

## [0.3.2](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.1...tellus-v0.3.2) - 2026-10-17

### Miscellaneous Tasks

- Move repo links and code owners to tellus-rs ([#126](https://github.com/tellus-rs/tellus/pull/126))
- Drop redundant homepage from manifests ([#118](https://github.com/tellus-rs/tellus/pull/118))

## [0.3.1](https://github.com/tellus-rs/tellus/compare/tellus-v0.3.0...tellus-v0.3.1) - 2026-10-12

### Features

- Add TestContext behind test-util for unit testing actors ([#103](https://github.com/tellus-rs/tellus/pull/103))

## [0.3.0](https://github.com/tellus-rs/tellus/compare/tellus-v0.2.1...tellus-v0.3.0) - 2026-09-13

### Features

- Add hosts, spawning/passivating managed entities ([#73](https://github.com/tellus-rs/tellus/pull/73))
- Add in-memory store behind persistence-in-memory feature ([#74](https://github.com/tellus-rs/tellus/pull/74))
- Stop an actor system from the outside ([#72](https://github.com/tellus-rs/tellus/pull/72))

## [0.2.1](https://github.com/tellus-rs/tellus/compare/tellus-v0.2.0...tellus-v0.2.1) - 2026-08-30

### Bug Fixes

- Harden backoff config, recovery drop and bench sizing ([#63](https://github.com/tellus-rs/tellus/pull/63))

### Refactor

- Harden core and persistence guarantees ([#62](https://github.com/tellus-rs/tellus/pull/62))
- Cut nesting and duplication in core and persistence ([#61](https://github.com/tellus-rs/tellus/pull/61))

### Miscellaneous Tasks

- Pin bench runtime threads and raise sampling ([#55](https://github.com/tellus-rs/tellus/pull/55))

## [0.2.0](https://github.com/tellus-rs/tellus/compare/tellus-v0.1.0...tellus-v0.2.0) - 2026-08-27

### Features

- Add event-sourced persistence ([#21](https://github.com/tellus-rs/tellus/pull/21))
- Add hotpath-based profiling behind hotpath feature ([#38](https://github.com/tellus-rs/tellus/pull/38))

### Documentation

- Install via cargo add instead of pinned versions ([#51](https://github.com/tellus-rs/tellus/pull/51))
- Refresh docs after persistence landed ([#47](https://github.com/tellus-rs/tellus/pull/47))

## [0.1.0](https://github.com/tellus-rs/tellus/releases/tag/tellus-v0.1.0) - 2026-08-24

### Miscellaneous Tasks

- Rename ferrier to tellus ([#34](https://github.com/tellus-rs/tellus/pull/34))
