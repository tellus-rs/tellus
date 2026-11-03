#!/usr/bin/env bash
# Usage: codegen.sh [--bench messaging|persistence|stash] BASE CANDIDATE [--rename REGEX=REPLACEMENT]...
# Builds the bench of both commits with v0 symbol mangling and compares their code: fncounts.py over
# all functions, fndiff.py over the per-message path below. A diagnostic, never a performance verdict.
# TARGET=x86_64-unknown-linux-gnu cross-builds from aarch64 with the x86_64 GNU toolchain.
set -euo pipefail

bench=messaging
if [ "${1:-}" = --bench ]; then
  bench=$2
  shift 2
fi
[ $# -ge 2 ] || { sed -n 2,5p "$0" >&2; exit 2; }
base=$(git rev-parse --verify "$1^{commit}")
candidate=$(git rev-parse --verify "$2^{commit}")
shift 2
renames=("$@")

# A selection for reading, not a coverage claim; every pattern must match in both binaries.
patterns=(
  '^<tellus::actor_ref::ActorRef<[^>]*>>::tell$'
  '^<flume::Shared<tellus::actor::Incoming<[^>]*>>>::(send|recv)::<'
)
case $bench in
  messaging)
    features=()
    patterns+=(
      '^tellus::actor_context::spawn(_untimed|_timed)?(_unstashed|_stashed)?::<[^>]*>::\{closure#0\}$'
      '^<core::future::poll_fn::PollFn<tellus::actor_context::spawn(_untimed|_timed)?(_unstashed|_stashed)?<[^>]*>::\{closure#0\}::\{closure#[0-9]+\}> as core::future::future::Future>::poll$'
      '^<tellus::mailbox::Mailbox<[^>]*>>::recv::\{closure#0\}$'
      '^tellus::actor_context::receive_incoming::<'
      '^tellus::actor_context::should_restart::<'
    )
    ;;
  persistence)
    features=(--features "persistence,persistence-in-memory")
    patterns+=(
      '^tellus::persistence::spawn::spawn_event_sourced(_untimed|_timed)?(_unstashed|_stashed)?::<.*>::\{closure#0\}$'
      '^<core::future::poll_fn::PollFn<tellus::persistence::spawn::spawn_event_sourced(_untimed|_timed)?(_unstashed|_stashed)?<.*> as core::future::future::Future>::poll$'
      '^tellus::actor_context::catch_(panic_)?and_log::<.*tellus::persistence::spawn::settle_or_log_failure<'
      '^<core::future::poll_fn::PollFn<tellus::actor_context::catch_unwind_async<.*tellus::persistence::spawn::(append_events|save_snapshot|read_page)<'
      '^tellus::persistence::spawn::encode_events::<'
      '^tellus::persistence::spawn::replay_page::<'
    )
    ;;
  stash)
    features=(--features "persistence,persistence-in-memory")
    patterns+=(
      '^tellus::actor_context::spawn(_untimed|_timed)?_stashed::<[^>]*>::\{closure#0\}$'
      '^<core::future::poll_fn::PollFn<tellus::actor_context::spawn(_untimed|_timed)?_stashed<[^>]*>::\{closure#0\}::\{closure#[0-9]+\}> as core::future::future::Future>::poll$'
      '^tellus::actor_context::receive_incoming::<'
      '^<tellus::stash::Stash<[^>]*>>::apply_requests$'
      '^tellus::persistence::spawn::spawn_event_sourced(_untimed|_timed)?_stashed::<.*>::\{closure#0\}$'
      '^<core::future::poll_fn::PollFn<tellus::persistence::spawn::spawn_event_sourced(_untimed|_timed)?_stashed<.*> as core::future::future::Future>::poll$'
      '^tellus::actor_context::catch_(panic_)?and_log::<.*tellus::persistence::spawn::settle_or_log_failure<'
      '^<core::future::poll_fn::PollFn<tellus::actor_context::catch_unwind_async<.*tellus::persistence::spawn::(append_events|save_snapshot|read_page)<'
      '^tellus::persistence::spawn::encode_events::<'
      '^tellus::persistence::spawn::replay_page::<'
    )
    ;;
  *) echo "unknown bench $bench" >&2; exit 2 ;;
esac

target=()
if [ -n "${TARGET:-}" ]; then
  target=(--target "$TARGET")
  if [ "$TARGET" = x86_64-unknown-linux-gnu ] && [ "$(uname -m)" != x86_64 ]; then
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER:-x86_64-linux-gnu-gcc}
    export CC_x86_64_unknown_linux_gnu=${CC_x86_64_unknown_linux_gnu:-x86_64-linux-gnu-gcc}
    export OBJDUMP=${OBJDUMP:-x86_64-linux-gnu-objdump}
  fi
fi

scripts=$(cd "$(dirname "$0")" && pwd)
# Under target/, since /tmp may be a small tmpfs; one target directory per tree, since Cargo reuses
# an output whose sources look older than it.
mkdir -p "$(git rev-parse --show-toplevel)/target"
work=$(mktemp -d "$(git rev-parse --show-toplevel)/target/codegen.XXXXXX")
cleanup() {
  git worktree remove --force "$work/base" 2>/dev/null || true
  git worktree remove --force "$work/candidate" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

for tree in base candidate; do
  git worktree add --quiet --detach "$work/$tree" "${!tree}"
  (cd "$work/$tree" && RUSTFLAGS=-Csymbol-mangling-version=v0 CARGO_TARGET_DIR="$work/target-$tree" \
    cargo bench -p tellus --bench "$bench" "${features[@]}" "${target[@]}" --no-run --locked \
      --message-format=json) \
    | jq -r "select(.reason == \"compiler-artifact\" and .target.name == \"$bench\" and .executable != null) | .executable" \
    > "$work/$tree.path"
  test "$(wc -l < "$work/$tree.path")" -eq 1
  cp "$(cat "$work/$tree.path")" "$work/$tree.bin"
done

status=0
echo "## fncounts.py, $bench, ${base:0:7} against ${candidate:0:7}"
python3 -I "$scripts/fncounts.py" "$work/base.bin" "$work/candidate.bin" "${renames[@]}" || status=1
echo
echo "## fndiff.py, the per-message path"
python3 -I "$scripts/fndiff.py" "$work/base.bin" "$work/candidate.bin" "${patterns[@]}" "${renames[@]}" || status=1
exit $status
