# Changelog

## [0.2.1](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.2.0...tellus-cluster-inspect-v0.2.1) - 2026-11-26

### Documentation

- Fill gaps and fix small errors in the READMEs ([#566](https://github.com/tellus-rs/tellus/pull/566))


## [0.2.0](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.1.4...tellus-cluster-inspect-v0.2.0) - 2026-11-20

### Bug Fixes

- *(inspect)* Clamp resumed event IDs, make InspectConfig deserializable ([#391](https://github.com/tellus-rs/tellus/pull/391))
- *(cluster)* [**breaking**] Serialize MemberState in snake_case ([#339](https://github.com/tellus-rs/tellus/pull/339))
- *(inspect)* [**breaking**] End event streams and the recorder on a shutdown future ([#307](https://github.com/tellus-rs/tellus/pull/307))
- *(cluster)* [**breaking**] Return a ClusterStateSubscription from cluster_state ([#304](https://github.com/tellus-rs/tellus/pull/304))

### Refactor

- Replace Infallible with ! and require Rust 1.100 ([#516](https://github.com/tellus-rs/tellus/pull/516))

### Documentation

- Regenerate changelogs with the unified config ([#505](https://github.com/tellus-rs/tellus/pull/505))
- Clarify cluster guarantees and complete the READMEs ([#475](https://github.com/tellus-rs/tellus/pull/475))
- Fix review findings in READMEs, roadmap, demo and examples ([#477](https://github.com/tellus-rs/tellus/pull/477))

### Styling

- *(inspect,demo,tui)* Imports, item order, test-only deps ([#466](https://github.com/tellus-rs/tellus/pull/466))


## [0.1.4](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.1.3...tellus-cluster-inspect-v0.1.4) - 2026-11-01

### Documentation

- Correct and complete the documentation ([#237](https://github.com/tellus-rs/tellus/pull/237))
- Describe when the inspect stream sends a verdict ([#233](https://github.com/tellus-rs/tellus/pull/233))

## [0.1.3](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.1.2...tellus-cluster-inspect-v0.1.3) - 2026-11-01

### Documentation

- Fix the findings of the documentation review ([#225](https://github.com/tellus-rs/tellus/pull/225))

## [0.1.2](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.1.1...tellus-cluster-inspect-v0.1.2) - 2026-10-29

### Other

- updated the following local packages: tellus

## [0.1.1](https://github.com/tellus-rs/tellus/compare/tellus-cluster-inspect-v0.1.0...tellus-cluster-inspect-v0.1.1) - 2026-10-28

### Other

- updated the following local packages: tellus

## [0.1.0](https://github.com/tellus-rs/tellus/releases/tag/tellus-cluster-inspect-v0.1.0) - 2026-10-21

### Features

- Add remoting and cluster support ([#1](https://github.com/tellus-rs/tellus/pull/1))
