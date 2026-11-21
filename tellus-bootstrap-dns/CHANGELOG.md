# Changelog

## [0.2.1](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.2.0...tellus-bootstrap-dns-v0.2.1) - 2026-11-21

### Miscellaneous Tasks

- Updated the following local packages: tellus


## [0.2.0](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.1.4...tellus-bootstrap-dns-v0.2.0) - 2026-11-20

### Bug Fixes

- *(cluster)* [**breaking**] Bound bootstrap join attempts, decide on a fresh view ([#410](https://github.com/tellus-rs/tellus/pull/410))
- *(bootstrap)* [**breaking**] Validate DNS queries and bound the Kubernetes list call ([#393](https://github.com/tellus-rs/tellus/pull/393))

### Refactor

- Replace Infallible with ! and require Rust 1.100 ([#516](https://github.com/tellus-rs/tellus/pull/516))
- *(cluster)* [**breaking**] Rename bootstrap fields, Formation and codec makers ([#396](https://github.com/tellus-rs/tellus/pull/396))

### Documentation

- Regenerate changelogs with the unified config ([#505](https://github.com/tellus-rs/tellus/pull/505))
- Fix review findings in READMEs, roadmap, demo and examples ([#477](https://github.com/tellus-rs/tellus/pull/477))
- *(bootstrap-dns)* [**breaking**] State that this release requires tellus 0.6 ([#334](https://github.com/tellus-rs/tellus/pull/334))

### Styling

- *(bootstrap)* Drop blank lines before Ok and bound names in where ([#465](https://github.com/tellus-rs/tellus/pull/465))


## [0.1.4](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.1.3...tellus-bootstrap-dns-v0.1.4) - 2026-11-01

### Documentation

- Correct and complete the documentation ([#237](https://github.com/tellus-rs/tellus/pull/237))

## [0.1.3](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.1.2...tellus-bootstrap-dns-v0.1.3) - 2026-11-01

### Documentation

- Fix the findings of the documentation review ([#225](https://github.com/tellus-rs/tellus/pull/225))
- Correct stale statements on inspect, min_peers, caching and lb ([#216](https://github.com/tellus-rs/tellus/pull/216))

## [0.1.2](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.1.1...tellus-bootstrap-dns-v0.1.2) - 2026-10-29

### Other

- updated the following local packages: tellus

## [0.1.1](https://github.com/tellus-rs/tellus/compare/tellus-bootstrap-dns-v0.1.0...tellus-bootstrap-dns-v0.1.1) - 2026-10-28

### Other

- updated the following local packages: tellus

## [0.1.0](https://github.com/tellus-rs/tellus/releases/tag/tellus-bootstrap-dns-v0.1.0) - 2026-10-21

### Features

- Add remoting and cluster support ([#1](https://github.com/tellus-rs/tellus/pull/1))
