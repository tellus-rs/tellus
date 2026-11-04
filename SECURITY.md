# Security policy

## Reporting a vulnerability

Please do not report a vulnerability in a public issue, a pull request or a discussion. Report it
privately through GitHub's
[vulnerability report form](https://github.com/tellus-rs/tellus/security/advisories/new) instead.
Only the maintainers see the report and the conversation which follows.

A useful report names the crate and its version, the features enabled and what an attacker can
achieve. A reproduction, even a rough one, helps most.

You will get an answer as soon as possible, usually within a few days. Once the vulnerability is confirmed, the fix is developed
in a private fork attached to the advisory. The advisory is published together with the release
containing the fix, credits you unless you prefer otherwise, and is submitted to the
[RustSec advisory database](https://rustsec.org).

## Supported versions

tellus is 0.x, so fixes land in the latest release of each crate only:

- `tellus`
- `tellus-bootstrap-dns`
- `tellus-bootstrap-k8s`
- `tellus-cluster-inspect`
- `tellus-persistence-postgres`

The unpublished crates of this repository, `tellus-cluster-demo`, `tellus-cluster-demo-tui` and
`tellus-comparison`, are out of scope.

## Scope

The parts of tellus most exposed to an attacker are those reading input from the network, behind
the `cluster` feature: frame decoding, the handshake, peer admission under mutual TLS and the
gossiped membership. A remote peer crashing a node, admitting itself without a valid certificate,
or delivering a message to an actor it could not otherwise reach is a vulnerability.

The router of `tellus-cluster-inspect` reveals the cluster's topology by design, and an
application mounts it behind its own authentication. Reaching it without that authentication is
not a vulnerability of tellus.
