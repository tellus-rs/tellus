#!/usr/bin/env bash
# Usage: probe.sh <stack_shift.so> <probe>; fails unless worker stacks move by exactly the shift.
set -euo pipefail
preload=$(realpath "$1")
probe=$(realpath "$2")
declare -A offsets
for shift in 0 16 256 4080; do
  seen=()
  for _ in 1 2 3 4 5; do
    seen+=("$(cd / && env -i PATH=/usr/bin:/bin LD_PRELOAD="$preload" STACK_SHIFT="$shift" "$probe" 2>/dev/null)")
  done
  if [ "$(printf '%s\n' "${seen[@]}" | sort -u | wc -l)" -ne 1 ]; then
    echo "unstable offset at shift $shift: ${seen[*]}" >&2
    exit 1
  fi
  offsets[$shift]=${seen[0]}
  echo "shift=$shift offset=${seen[0]}"
done
for shift in 16 256 4080; do
  expected=$(( (offsets[0] - shift + 4096) % 4096 ))
  if [ "${offsets[$shift]}" -ne "$expected" ]; then
    echo "shift $shift: offset ${offsets[$shift]}, expected $expected" >&2
    exit 1
  fi
done
