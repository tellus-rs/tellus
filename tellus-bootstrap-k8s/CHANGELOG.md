# Changelog

## [0.3.0](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.2.0...tellus-bootstrap-k8s-v0.3.0) - 2026-11-26

### Documentation

- Fill gaps and fix small errors in the READMEs ([#566](https://github.com/tellus-rs/tellus/pull/566))

### Dependencies

- [**breaking**] Replace humantime-serde with jiff ([#551](https://github.com/tellus-rs/tellus/pull/551))


## [0.2.0](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.1.4...tellus-bootstrap-k8s-v0.2.0) - 2026-11-20

### Features

- Add stashing, opt-in per actor via Actor::Stash ([#201](https://github.com/tellus-rs/tellus/pull/201))

### Bug Fixes

- *(cluster)* [**breaking**] Bound bootstrap join attempts, decide on a fresh view ([#410](https://github.com/tellus-rs/tellus/pull/410))
- *(bootstrap)* [**breaking**] Validate DNS queries and bound the Kubernetes list call ([#393](https://github.com/tellus-rs/tellus/pull/393))
- *(cluster)* [**breaking**] Return a ClusterStateSubscription from cluster_state ([#304](https://github.com/tellus-rs/tellus/pull/304))

### Refactor

- Replace Infallible with ! and require Rust 1.100 ([#516](https://github.com/tellus-rs/tellus/pull/516))
- *(cluster)* [**breaking**] Rename bootstrap fields, Formation and codec makers ([#396](https://github.com/tellus-rs/tellus/pull/396))

### Documentation

- *(bootstrap-k8s)* Name k8s-openapi 0.29 in the README ([#514](https://github.com/tellus-rs/tellus/pull/514))
- Regenerate changelogs with the unified config ([#505](https://github.com/tellus-rs/tellus/pull/505))
- *(bootstrap-k8s)* Note that this release requires kube 5 ([#481](https://github.com/tellus-rs/tellus/pull/481))
- Fix review findings in READMEs, roadmap, demo and examples ([#477](https://github.com/tellus-rs/tellus/pull/477))

### Styling

- *(bootstrap)* Drop blank lines before Ok and bound names in where ([#465](https://github.com/tellus-rs/tellus/pull/465))


## [0.1.4](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.1.3...tellus-bootstrap-k8s-v0.1.4) - 2026-11-01

### Documentation

- Correct and complete the documentation ([#237](https://github.com/tellus-rs/tellus/pull/237))

## [0.1.3](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.1.2...tellus-bootstrap-k8s-v0.1.3) - 2026-11-01

### Documentation

- Fix the findings of the documentation review ([#225](https://github.com/tellus-rs/tellus/pull/225))

## [0.1.2](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.1.1...tellus-bootstrap-k8s-v0.1.2) - 2026-10-29

### Other

- updated the following local packages: tellus

## [0.1.1](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-k8s-v0.1.0...tellus-bootstrap-k8s-v0.1.1) - 2026-10-28

### Features

- Add timers, opt-in per actor via Actor::Timers ([#185](https://github.com/tellus-rs/tellus/pull/185))

## [0.1.0](https://github.com/tellus-rs/tellus/releases/tag/tellus-bootstrap-k8s-v0.1.0) - 2026-10-21

### Features

- Add remoting and cluster support ([#1](https://github.com/tellus-rs/tellus/pull/1))
