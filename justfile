set shell := ["bash", "-uc"]

nightly := `rustc --version | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sed 's/^/nightly-/'`

rust_version := `grep channel rust-toolchain.toml | sed -r 's/channel = "(.*)"/\1/'`

export RUST_VERSION := rust_version

# A benchmark counts as regressed once we are 95% confident it is at least this fraction slower;
# bench-report flags it.
bench_regression_threshold := "0.15"

# The feature powerset for `tellus`, minus what cargo-hack cannot know: `hotpath` is orthogonal
# instrumentation. Do not exclude a feature against one which implies it, neither
# `persistence`/`persistence-tests` nor `cluster`/`cluster-dev`: cargo-hack resolves a group, so
# that also drops `persistence-tests,persistence-in-memory`, the one combination which compiles
# tests/persistence_tests.rs. Implications need no group anyway, since cargo-hack drops a
# combination whose features resolve to the same set.
powerset := "--feature-powerset --exclude-features hotpath,hotpath-alloc,hotpath-callgrind,hotpath-cloud"

check:
    cargo hack check -p tellus                      --all-targets {{ powerset }}
    cargo check      -p tellus                      --all-targets --features hotpath
    cargo check      -p tellus                      --all-targets --all-features
    cargo hack check -p tellus-bootstrap-dns        --all-targets --feature-powerset
    cargo hack check -p tellus-bootstrap-k8s        --all-targets --feature-powerset
    cargo check      -p tellus-persistence-postgres --all-targets
    cargo check      -p tellus-cluster-inspect      --all-targets
    cargo check      -p tellus-cluster-demo         --all-targets
    cargo check      -p tellus-cluster-demo-tui     --all-targets

fix:
    cargo fix --all-targets --allow-dirty --allow-staged

fmt:
    cargo +{{ nightly }} fmt
    RUST_LOG=error taplo fmt

fmt-check:
    cargo +{{ nightly }} fmt --check

lint:
    cargo hack clippy -p tellus                      --all-targets --no-deps {{ powerset }}     -- -D warnings
    cargo clippy      -p tellus                      --all-targets --no-deps --features hotpath -- -D warnings
    cargo clippy      -p tellus                      --all-targets --no-deps --all-features     -- -D warnings
    cargo hack clippy -p tellus-bootstrap-dns        --all-targets --no-deps --feature-powerset -- -D warnings
    cargo hack clippy -p tellus-bootstrap-k8s        --all-targets --no-deps --feature-powerset -- -D warnings
    cargo clippy      -p tellus-persistence-postgres --all-targets --no-deps                    -- -D warnings
    cargo clippy      -p tellus-cluster-inspect      --all-targets --no-deps                    -- -D warnings
    cargo clippy      -p tellus-cluster-demo         --all-targets --no-deps                    -- -D warnings
    cargo clippy      -p tellus-cluster-demo-tui     --all-targets --no-deps                    -- -D warnings

lint-fix:
    cargo clippy --all-targets --no-deps --allow-dirty --allow-staged --fix

test:
    cargo test      -p tellus
    cargo test      -p tellus                      --features persistence-tests,persistence-in-memory,cluster-dev,serde,test-util
    cargo hack test -p tellus                      --doc {{ powerset }}
    cargo test      -p tellus-bootstrap-dns        --all-features
    cargo test      -p tellus-bootstrap-k8s        --all-features
    cargo test      -p tellus-persistence-postgres
    cargo test      -p tellus-cluster-inspect
    cargo test      -p tellus-cluster-demo
    cargo test      -p tellus-cluster-demo-tui

doc:
    RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +{{ nightly }} doc --no-deps --all-features

all: check fmt lint test doc

bench:
    cargo bench -p tellus --bench messaging
    cargo bench -p tellus --features "persistence,persistence-in-memory" --bench persistence

bench-save baseline:
    cargo bench -p tellus --bench messaging -- --save-baseline {{ baseline }}

bench-compare baseline:
    cargo bench -p tellus --bench messaging -- --baseline-lenient {{ baseline }}

bench-persistence-save baseline:
    cargo bench -p tellus --features "persistence,persistence-in-memory" --bench persistence -- --save-baseline {{ baseline }}

bench-persistence-compare baseline:
    cargo bench -p tellus --features "persistence,persistence-in-memory" --bench persistence -- --baseline-lenient {{ baseline }}

bench-bencher:
    cargo bench -p tellus --bench messaging -- --output-format bencher

bench-persistence-bencher:
    cargo bench -p tellus --features "persistence,persistence-in-memory" --bench persistence -- --output-format bencher

bench-gossip size="5":
    TELLUS_REMOTING_ROLE=bench-gossip TELLUS_REMOTING_CLUSTER_SIZE={{ size }} cargo test -p tellus --test cluster --features cluster-dev --release

bench-streams streams="16":
    TELLUS_REMOTING_ROLE=bench-streams TELLUS_REMOTING_STREAMS={{ streams }} cargo test -p tellus --test cluster --features cluster-dev --release

bench-cluster:
    TELLUS_REMOTING_ROLE=bench cargo test -p tellus --test cluster --features cluster-dev --release

bench-report:
    #!/usr/bin/env bash
    set -euo pipefail
    threshold={{ bench_regression_threshold }}
    threshold_pct=$(awk -v t="$threshold" 'BEGIN { printf "%.0f", t * 100 }')
    printf '### Benchmark comparison\n\n'
    printf '| Benchmark | Time | Change | Verdict |\n'
    printf '| --- | --- | --- | --- |\n'
    regressed=0
    while IFS= read -r change; do
        dir=${change%/change/estimates.json}
        id=${dir#target/criterion/}
        time=$(jq -r '.mean.point_estimate' "$dir/new/estimates.json")
        read -r pct lower < <(jq -r '.mean.point_estimate, .mean.confidence_interval.lower_bound' "$change" | paste -sd' ')
        verdict=$(awk -v l="$lower" -v t="$threshold" 'BEGIN { print (l > t) ? "⚠️ regressed" : "ok" }')
        [[ $verdict == ok ]] || regressed=1
        awk -v id="$id" -v t="$time" -v p="$pct" -v v="$verdict" \
            'BEGIN { printf "| %s | %.3f ms | %+.1f%% | %s |\n", id, t / 1e6, p * 100, v }'
    done < <(find target/criterion -path '*/change/estimates.json' | sort)
    printf '\n'
    if [[ $regressed -ne 0 ]]; then
        printf '_Regression: 95%% confident a benchmark is at least %s%% slower._\n\n' "$threshold_pct"
    fi
    printf '_One stack position, with the head always running second: a hint, not a verdict. A change to code the benches run gets the stack sweep, label `sweep` or `sweep-persistence` ([protocol](https://github.com/tellus-rs/tellus/blob/main/.github/stack-sweep/README.md))._\n'

profile:
    cargo run --release -p tellus --example profile --features hotpath

profile-alloc *features:
    cargo run --release -p tellus --example profile --features "hotpath-alloc {{ features }}"

profile-persistence:
    cargo run --release -p tellus --example profile_persistence --features "hotpath,persistence,persistence-in-memory"

profile-persistence-alloc *features:
    cargo run --release -p tellus --example profile_persistence --features "hotpath-alloc,persistence,persistence-in-memory {{ features }}"

# One Criterion benchmark as a hotpath report: the routine of `id` runs exactly `iterations` times.
# Example: just criterion-profile messaging flood/bounded 10
criterion-profile bench id iterations *features:
    HOTPATH_CRITERION_ITERATIONS={{ iterations }} cargo bench -p tellus --bench {{ bench }} --features "hotpath-alloc {{ features }}" -- --test --exact "{{ id }}"

profile-alloc-gate out="target/hotpath/profile.json":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "$(dirname "{{ out }}")"
    HOTPATH_OUTPUT_FORMAT=json HOTPATH_OUTPUT_PATH="{{ out }}" just profile-alloc
    just profile-alloc-check "{{ out }}"

# The steady-state messaging path must not allocate per message; "0 B" is exact, not rounded.
profile-alloc-check file:
    just _alloc-check "{{ file }}" tellus::actor_ref::tell tellus::quota::reserve tellus::actor_context::receive_incoming

profile-persistence-alloc-gate out="target/hotpath/profile_persistence.json":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "$(dirname "{{ out }}")"
    HOTPATH_OUTPUT_FORMAT=json HOTPATH_OUTPUT_PATH="{{ out }}" just profile-persistence-alloc
    just profile-persistence-alloc-check "{{ out }}"

# Applying and replaying must not allocate per event beyond the event's own content.
profile-persistence-alloc-check file:
    just _alloc-check "{{ file }}" tellus::persistence::spawn::apply_events tellus::persistence::spawn::replay_page

# Every named function must show up in the report having allocated exactly "0 B", not rounded.
_alloc-check file +names:
    #!/usr/bin/env bash
    set -euo pipefail
    failed=0
    for name in {{ names }}; do
        if jq -e --arg name "$name" \
            '[.functions_alloc.data[] | select(.name == $name)] | length == 1 and .[0].total == "0 B"' \
            "{{ file }}" > /dev/null; then
            echo "ok: $name allocated 0 B"
        else
            echo "FAIL: $name allocated memory or is missing from {{ file }}"
            failed=1
        fi
    done
    exit $failed

bench-comparison:
    cargo bench -p tellus-comparison --bench frameworks

bench-comparison-report tag="local":
    cargo run -p tellus-comparison --bin report -- --tag {{ tag }}

check-comparison:
    cargo check -p tellus-comparison --all-targets

lint-comparison:
    cargo clippy -p tellus-comparison --all-targets --no-deps -- -D warnings

start-cluster-demo-compose:
    docker compose -f tellus-cluster-demo/docker-compose.yaml up -d --build

stop-cluster-demo-compose:
    docker compose -f tellus-cluster-demo/docker-compose.yaml down -v

logs-cluster-demo-compose *args:
    docker compose -f tellus-cluster-demo/docker-compose.yaml logs -f {{ args }}

start-cluster-demo-k8s discovery="dns":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{ discovery }}" in
        dns|k8s) ;;
        *) echo "discovery must be dns or k8s, not '{{ discovery }}'" >&2; exit 1 ;;
    esac
    for tool in kind kubectl docker; do
        command -v "$tool" > /dev/null || { echo "$tool is not installed" >&2; exit 1; }
    done
    # The kubelet backoff field and kindnetd's policy enforcement need a current kind.
    kind version | grep -qE 'v0\.(3[3-9]|[4-9][0-9])' || { echo "kind v0.33 or later is required" >&2; exit 1; }
    kind create cluster --config tellus-cluster-demo/k8s/kind.yaml
    docker build --build-arg RUST_VERSION={{ rust_version }} \
        -t tellus-cluster-demo:latest -f tellus-cluster-demo/Dockerfile .
    kind load docker-image tellus-cluster-demo:latest --name tellus-cluster-demo
    kubectl --context kind-tellus-cluster-demo create configmap tellus-discovery \
        --from-literal=CONFIG_OVERLAYS={{ discovery }}
    kubectl --context kind-tellus-cluster-demo create configmap tellus-chaos-script \
        --from-file=tellus-cluster-demo/k8s/chaos.sh
    kubectl --context kind-tellus-cluster-demo apply \
        -f tellus-cluster-demo/k8s/rbac.yaml \
        -f tellus-cluster-demo/k8s/nodes.yaml \
        -f tellus-cluster-demo/k8s/lb.yaml \
        -f tellus-cluster-demo/k8s/node-ports.yaml \
        -f tellus-cluster-demo/k8s/partition.yaml \
        -f tellus-cluster-demo/k8s/flush.yaml \
        -f tellus-cluster-demo/k8s/verifier.yaml

stop-cluster-demo-k8s:
    kind delete cluster --name tellus-cluster-demo

# Without a target the whole demo is followed; with one, e.g. `tellus-2`, only that.
logs-cluster-demo-k8s *args:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{ args }}" ]; then
        kubectl --context kind-tellus-cluster-demo logs -f \
            -l app.kubernetes.io/part-of=tellus-cluster-demo \
            --all-containers --prefix --max-log-requests 10
    else
        kubectl --context kind-tellus-cluster-demo logs -f --all-containers --prefix {{ args }}
    fi

status-cluster-demo:
    curl -s localhost:8080/cluster | jq
    curl -s localhost:8080/inspect/cluster | jq
    curl -s localhost:8081/status | jq

violations-cluster-demo:
    curl -s localhost:8081/violations | jq

run-cluster-demo-tui:
    cargo run -p tellus-cluster-demo-tui
