# Changelog

## [0.6.0](https://github.com/tellus-rs/tellus/compare/tellus-v0.5.3...tellus-v0.6.0) - 2026-11-20

### Features

- *(persistence)* [**breaking**] Bound every store call by a timeout ([#409](https://github.com/tellus-rs/tellus/pull/409))
- *(host)* Make HostConfig deserializable ([#392](https://github.com/tellus-rs/tellus/pull/392))
- Check more fencing, isolation and extremes in the store contract ([#288](https://github.com/tellus-rs/tellus/pull/288))
- [**breaking**] Replace the passivate function of Host with Hosted<M> ([#265](https://github.com/tellus-rs/tellus/pull/265))
- *(cluster)* [**breaking**] Version message payloads for rolling upgrades ([#262](https://github.com/tellus-rs/tellus/pull/262))
- Add stashing, opt-in per actor via Actor::Stash ([#201](https://github.com/tellus-rs/tellus/pull/201))

### Bug Fixes

- *(cluster)* Drain lanes toward unknown and restarted nodes on leave ([#506](https://github.com/tellus-rs/tellus/pull/506))
- *(cluster)* Resolve concurrent departures by incarnation ([#453](https://github.com/tellus-rs/tellus/pull/453))
- *(examples)* Wait for every view before leaving, sort seed addresses ([#454](https://github.com/tellus-rs/tellus/pull/454))
- *(cluster)* End a QUIC stream at a graceful close between frames ([#451](https://github.com/tellus-rs/tellus/pull/451))
- *(cluster)* Refuse joins as joining while pinned to a cluster ([#448](https://github.com/tellus-rs/tellus/pull/448))
- *(cluster)* Await abandoned join attempts before deciding formation ([#450](https://github.com/tellus-rs/tellus/pull/450))
- *(cluster)* Judge receptionist settled by the current cluster state ([#447](https://github.com/tellus-rs/tellus/pull/447))
- *(persistence)* Settle stash requests once the events are durable ([#446](https://github.com/tellus-rs/tellus/pull/446))
- *(persistence)* Stop before recovered when stopped during recovery ([#444](https://github.com/tellus-rs/tellus/pull/444))
- *(examples)* Wait for the passivation instead of sleeping ([#435](https://github.com/tellus-rs/tellus/pull/435))
- *(examples)* Bound every wait and check every exit status ([#424](https://github.com/tellus-rs/tellus/pull/424))
- [**breaking**] Never run init for a child its parent stopped before it ran ([#414](https://github.com/tellus-rs/tellus/pull/414))
- *(cluster)* [**breaking**] Refuse empty FixedSeeds and state the trait contracts ([#415](https://github.com/tellus-rs/tellus/pull/415))
- *(cluster)* [**breaking**] Bound bootstrap join attempts, decide on a fresh view ([#410](https://github.com/tellus-rs/tellus/pull/410))
- *(persistence)* [**breaking**] Refuse a sequence number overflow instead of saturating ([#395](https://github.com/tellus-rs/tellus/pull/395))
- *(cluster)* [**breaking**] Refuse to leave an endpoint which never formed or joined ([#400](https://github.com/tellus-rs/tellus/pull/400))
- *(cluster)* [**breaking**] Cap derived deadlines and bound oversize config values ([#397](https://github.com/tellus-rs/tellus/pull/397))
- *(cluster)* Keep a self-down from settling watchers and asks ([#399](https://github.com/tellus-rs/tellus/pull/399))
- *(cluster)* [**breaking**] Fail directory reads once this node has left or is down ([#394](https://github.com/tellus-rs/tellus/pull/394))
- *(cluster)* Send toward a peer at the smaller frame limit of both ([#389](https://github.com/tellus-rs/tellus/pull/389))
- *(cluster)* Release the reply destinations of a dead-lettered reply ([#388](https://github.com/tellus-rs/tellus/pull/388))
- *(cluster)* Name the asking incarnation in reply frames ([#383](https://github.com/tellus-rs/tellus/pull/383))
- *(cluster)* Refuse sends toward a dead incarnation through any lane ([#382](https://github.com/tellus-rs/tellus/pull/382))
- *(cluster)* [**breaking**] Serialize MemberState in snake_case ([#339](https://github.com/tellus-rs/tellus/pull/339))
- *(cluster)* [**breaking**] Refuse zero durations which switch off a safeguard ([#332](https://github.com/tellus-rs/tellus/pull/332))
- *(cluster)* Refuse a send toward another incarnation of this node ([#331](https://github.com/tellus-rs/tellus/pull/331))
- *(cluster)* Keep a panicking decode from closing the connection ([#327](https://github.com/tellus-rs/tellus/pull/327))
- *(cluster)* Keep a panicking reply encode from failing the responder ([#316](https://github.com/tellus-rs/tellus/pull/316))
- *(inspect)* [**breaking**] End event streams and the recorder on a shutdown future ([#307](https://github.com/tellus-rs/tellus/pull/307))
- [**breaking**] Refuse a NUL in a persistence ID, map positions beyond BIGINT ([#308](https://github.com/tellus-rs/tellus/pull/308))
- Keep a panicking reply_to conversion from failing the responder ([#306](https://github.com/tellus-rs/tellus/pull/306))
- *(cluster)* [**breaking**] Return a ClusterStateSubscription from cluster_state ([#304](https://github.com/tellus-rs/tellus/pull/304))
- Decode long CBOR text and exact JSON floats, refuse non-finite ones ([#303](https://github.com/tellus-rs/tellus/pull/303))
- *(cluster)* Warn of an overdue departure only if it was awaited ([#287](https://github.com/tellus-rs/tellus/pull/287))
- *(cluster)* Ask every seed before forming once a pin is released ([#286](https://github.com/tellus-rs/tellus/pull/286))
- *(cluster)* Hold back joins while this node is leaving ([#285](https://github.com/tellus-rs/tellus/pull/285))
- *(cluster)* Drop removed reply entries outside the pending lock ([#274](https://github.com/tellus-rs/tellus/pull/274))
- *(cluster)* Stop a lane from dialing once its endpoint is downed ([#273](https://github.com/tellus-rs/tellus/pull/273))

### Refactor

- Replace Infallible with ! and require Rust 1.100 ([#516](https://github.com/tellus-rs/tellus/pull/516))
- *(cluster)* [**breaking**] Move test support from cluster-dev to __internal ([#457](https://github.com/tellus-rs/tellus/pull/457))
- *(cluster)* [**breaking**] Rename bootstrap fields, Formation and codec makers ([#396](https://github.com/tellus-rs/tellus/pull/396))

### Documentation

- Regenerate changelogs with the unified config ([#505](https://github.com/tellus-rs/tellus/pull/505))
- *(examples)* Call the device manager's message DeviceManagerMessage ([#502](https://github.com/tellus-rs/tellus/pull/502))
- Correct statements found in review of the 0.6 docs ([#483](https://github.com/tellus-rs/tellus/pull/483))
- Clarify cluster guarantees and complete the READMEs ([#475](https://github.com/tellus-rs/tellus/pull/475))
- Fix review findings in READMEs, roadmap, demo and examples ([#477](https://github.com/tellus-rs/tellus/pull/477))
- Fix the review's core and persistence doc findings ([#476](https://github.com/tellus-rs/tellus/pull/476))
- *(cluster)* Name every node in FixedSeeds to keep a minority out ([#460](https://github.com/tellus-rs/tellus/pull/460))
- *(cluster)* State the order of gate and check in read_frames ([#455](https://github.com/tellus-rs/tellus/pull/455))
- *(examples)* Show pubsub, downing, rolling upgrades and formation ([#431](https://github.com/tellus-rs/tellus/pull/431))
- *(examples)* Show schema upcasting and event-sourced children ([#429](https://github.com/tellus-rs/tellus/pull/429))
- *(examples)* Show Host and passivation in a how-to example ([#428](https://github.com/tellus-rs/tellus/pull/428))
- *(cluster)* Limit the release of a lost frame to one not yet sent ([#406](https://github.com/tellus-rs/tellus/pull/406))
- *(cluster)* Say what releases a dead-lettered reply's destinations ([#403](https://github.com/tellus-rs/tellus/pull/403))
- Add release notes for the breaking changes since 0.5.3 ([#321](https://github.com/tellus-rs/tellus/pull/321))
- State the codecs' limits on nesting and on Some(None) ([#313](https://github.com/tellus-rs/tellus/pull/313))
- Add a how-to example for retrying failed writes ([#270](https://github.com/tellus-rs/tellus/pull/270))
- Retry the fourth lookup of the stash how-to instead of sleeping ([#269](https://github.com/tellus-rs/tellus/pull/269))
- Add a how-to example for stashing ([#255](https://github.com/tellus-rs/tellus/pull/255))
- State the stash exception to the unwatch contract ([#257](https://github.com/tellus-rs/tellus/pull/257))
- Mention clustering in the crate description ([#245](https://github.com/tellus-rs/tellus/pull/245))
- Show the build badge for main only ([#244](https://github.com/tellus-rs/tellus/pull/244))

### Performance

- *(cluster)* [**breaking**] Make the payload codec a closed enum ([#340](https://github.com/tellus-rs/tellus/pull/340))
- Encode events in a plain loop ([#236](https://github.com/tellus-rs/tellus/pull/236))

### Styling

- Trim example comments, rename the manager's message, per review ([#474](https://github.com/tellus-rs/tellus/pull/474))
- *(cluster)* Shorten paths and place docs in the remoting test ([#473](https://github.com/tellus-rs/tellus/pull/473))
- *(cluster)* Order and shorten failure, reachability, bootstrap, QUIC ([#472](https://github.com/tellus-rs/tellus/pull/472))
- *(cluster)* Order, scope and name the directory and reply code ([#471](https://github.com/tellus-rs/tellus/pull/471))
- *(cluster)* Order, scope and name the endpoint and membership code ([#468](https://github.com/tellus-rs/tellus/pull/468))
- *(tests)* Unify paths and helpers' names, regroup the budget test ([#470](https://github.com/tellus-rs/tellus/pull/470))
- *(persistence)* Order items, narrow fields, shorten paths per review ([#469](https://github.com/tellus-rs/tellus/pull/469))
- *(examples)* Paths, item order, uniform constants and contexts ([#467](https://github.com/tellus-rs/tellus/pull/467))
- *(core)* Narrow visibility, order items, trim docs per review ([#464](https://github.com/tellus-rs/tellus/pull/464))
- *(cluster)* Order, name and shorten the peer and frame code ([#463](https://github.com/tellus-rs/tellus/pull/463))

### Miscellaneous Tasks

- Add the stash bench as a binary of its own ([#252](https://github.com/tellus-rs/tellus/pull/252))
- Rename docker-compose.yaml to compose.yaml ([#241](https://github.com/tellus-rs/tellus/pull/241))

### Other

- *(persistence)* [**breaking**] Check IDs and read limits at their extremes ([#412](https://github.com/tellus-rs/tellus/pull/412))


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
