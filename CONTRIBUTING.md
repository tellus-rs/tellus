# Contributing to tellus

Thank you for considering a contribution. Bug reports, fixes, documentation and new features are
all welcome.

For anything beyond a small fix, please open an issue first and describe what you want to change.
The core, persistence and the cluster are held together by guarantees spanning several files, so
agreeing on the approach before the code saves both of us a rewrite.

A vulnerability is the one thing never to report in a public issue; see the
[security policy](SECURITY.md).

## Building and testing

The [README](README.md#development) lists the tools you need: `just`, `cargo-hack`, `taplo`, `jq`,
the nightly toolchain matching the pinned stable one, and Docker for the PostgreSQL tests. Then
run the full local gate before you open a pull request:

```sh
just all
```

`just all` checks and lints every feature combination, runs the tests, builds the documentation and
formats Rust and TOML. Use `just fmt` rather than `cargo fmt`, since the rustfmt configuration
needs nightly.

## Commits and pull requests

A pull request consists of exactly one commit, which CI enforces. Squash your changes and force
push when you address review comments.

The commit message is a single subject line of at most 72 characters, without a body, following
[Conventional Commits](https://www.conventionalcommits.org), for example `fix(cluster): refuse a
zero leave_timeout`. release-plz builds the changelog from these subjects, so the subject is what
users will read.

Sign off your commit with `git commit -s`. The `Signed-off-by` trailer certifies that you may
contribute the change under the project's license, as the
[Developer Certificate of Origin](https://developercertificate.org) states.

If your change makes a statement on [tellus.rs](https://tellus.rs) false, say so in the pull
request's description. The website is updated after each release, and the hint makes sure that
statement gets fixed.

## Code and documentation

The [conventions](AGENTS.md#conventions) cover the code style, item order, comments and writing
style. Two of them catch most first contributions: use US English spelling, in identifiers too, and
no em or en dashes as punctuation.

The documents in [docs](docs) are the authoritative description of how tellus works:
[actors.md](docs/actors.md) for the core, [persistence.md](docs/persistence.md) for event sourcing
and [cluster.md](docs/cluster.md) for the cluster. Read the relevant one before changing the code
it describes, and keep it consistent with your change. The integration tests in
[tellus/tests](tellus/tests) encode the same guarantees.

A change to the cluster's wire format updates the pinned-byte tests in
[frame.rs](tellus/src/cluster/frame.rs).

## Performance

The per pull request benchmark comparison is a hint and never a verdict, since a single run varies
by tens of percent on identical code. A change to code the benchmarks run is judged by the
[stack sweep](.github/stack-sweep/README.md) instead, which a maintainer starts by labeling your
pull request.

## License

tellus is licensed under the [Apache 2.0 License](LICENSE). Every contribution submitted for
inclusion is licensed under the same terms, as section 5 of the license states.
