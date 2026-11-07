#!/bin/sh
# The chaos agent: every cycle it kills one node, partitions two nodes off the other three, stops
# one node or rolls all five to the other application version, and names what it is doing in the
# state file the verifier reads, so the cluster is only held to its promises once it has had a
# quiet window to recover.

set -eu

NODE_COUNT=${NODE_COUNT:-5}
POD_PREFIX=${POD_PREFIX:-tellus-}
SELECTOR=${SELECTOR:-app=tellus}
STATEFULSET=${STATEFULSET:-tellus}
VERSION_ENV=CFG__ENDPOINT__APP_VERSION
OLD_VERSION=${OLD_VERSION:-1.0.0}
NEW_VERSION=${NEW_VERSION:-2.0.0}
FLUSH_SELECTOR=${FLUSH_SELECTOR:-app=tellus-conntrack}
FLUSH_SETTLE_SECS=${FLUSH_SETTLE_SECS:-5}
STATE_FILE=${STATE_FILE:-/chaos/state}
STARTUP_SECS=${STARTUP_SECS:-60}
QUIET_SECS=${QUIET_SECS:-45}
# The quiet window between two pods of one roll; the windows before and after the roll are the
# cycle's own. Below the verifier's settle time no mixed cluster is verified, which it reports.
ROLL_QUIET_SECS=${ROLL_QUIET_SECS:-$QUIET_SECS}
PARTITION_SECS=${PARTITION_SECS:-75}
RECOVERY_SECS=${RECOVERY_SECS:-25}
ROLLOUT_TIMEOUT_SECS=${ROLLOUT_TIMEOUT_SECS:-120}
RETRY_SECS=${RETRY_SECS:-10}

state() {
    echo "$1" > "$STATE_FILE"
    echo "chaos: $1"
}

pod() {
    echo "${POD_PREFIX}$1"
}

# The partition policies are static and label driven, so an agent which died mid partition would
# leave the cluster split for good; this runs at startup and from the trap.
heal() {
    kubectl label pod -l "$SELECTOR" side- > /dev/null 2>&1 || true
}

# Deleting the pod is the only crash reachable from inside the cluster: a process cannot SIGKILL
# PID 1 of its own namespace, and SIGTERM is the departure below. The replacement carries a new
# address, so the crashed member is gone for good and has to be detected.
kill_node() {
    state "kill $(pod "$1")"
    kubectl delete pod "$(pod "$1")" --force --grace-period=0 || echo "kubectl delete failed"
    sleep "$RECOVERY_SECS"
}

# The same command with its grace period intact: the kubelet sends SIGTERM, the node announces
# its departure and the cluster downs it within a gossip round instead of detecting its silence.
leave_node() {
    state "leave $(pod "$1")"
    kubectl delete pod "$(pod "$1")" || echo "kubectl delete failed"
    sleep "$RECOVERY_SECS"
}

# Runs a command until it succeeds. The state file keeps naming the fault meanwhile, so a step
# which cannot complete never lets the verifier take a half-rolled cluster for a quiet one.
until_done() {
    until "$@"; do
        echo "chaos: '$*' failed, retrying in ${RETRY_SECS}s" >&2
        sleep "$RETRY_SECS"
    done
}

# Rolls every pod to the other application version, highest ordinal first as the StatefulSet
# controller does, with the partition stepped down one ordinal at a time and a quiet window after
# each, so the verifier checks every mixed cluster. Re-asserting the partition above the replica
# count before the template change is what keeps that change from updating every pod at once.
roll_nodes() {
    if [ "$version" = "$OLD_VERSION" ]; then
        target=$NEW_VERSION
        verb=upgrade
    else
        target=$OLD_VERSION
        verb=downgrade
    fi

    state "$verb to $target"
    until_done partition_at "$NODE_COUNT"
    until_done kubectl set env "statefulset/$STATEFULSET" "$VERSION_ENV=$target" > /dev/null
    until_done template_observed
    for ordinal in $(seq $((NODE_COUNT - 1)) -1 0); do
        if [ "$ordinal" -ne $((NODE_COUNT - 1)) ]; then
            state quiet
            sleep "$ROLL_QUIET_SECS"
        fi
        roll_node "$ordinal" "$target" "$verb"
    done
    version=$target
}

# The kubelet's SIGTERM within the grace period, as leave_node, but issued by the StatefulSet
# controller, which recreates the pod from the updated template. The wait is on this pod's own
# revision: the updated count is at the partition's threshold before the pod is replaced when an
# interrupted roll left pods of the other direction at the target already.
roll_node() {
    state "$3 $(pod "$1") to $2"
    until_done partition_at "$1"
    until_done pod_updated "$1"
    sleep "$RECOVERY_SECS"
}

partition_at() {
    kubectl patch statefulset "$STATEFULSET" \
        -p "{\"spec\":{\"updateStrategy\":{\"rollingUpdate\":{\"partition\":$1}}}}" > /dev/null
}

# The update revision is the controller's, so it names the new template only once the controller
# has observed the generation the template change made.
template_observed() {
    generations=$(kubectl get statefulset "$STATEFULSET" \
        -o jsonpath='{.metadata.generation} {.status.observedGeneration}') || return 1
    [ "${generations% *}" = "${generations#* }" ]
}

pod_updated() {
    revision=$(kubectl get statefulset "$STATEFULSET" -o jsonpath='{.status.updateRevision}') ||
        return 1
    [ -n "$revision" ] || return 1
    hash=$(kubectl get pod "$(pod "$1")" \
        -o jsonpath='{.metadata.labels.controller-revision-hash}') || return 1
    [ "$hash" = "$revision" ] || return 1
    kubectl wait --for=condition=Ready --timeout="${ROLLOUT_TIMEOUT_SECS}s" pod "$(pod "$1")" \
        > /dev/null
}

current_version() {
    kubectl get statefulset "$STATEFULSET" \
        -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name==\"$VERSION_ENV\")].value}" \
        2> /dev/null
}

# Labeling is the whole fault: partition.yaml denies each side ingress from the other, and a
# recreated pod comes back without a label, so nothing has to be undone by hand after a crash.
partition() {
    minority="$*"
    majority=
    for node in $(seq 0 $((NODE_COUNT - 1))); do
        case " $minority " in
            *" $node "*) ;;
            *) majority="$majority $node" ;;
        esac
    done

    majority="${majority# }"

    state "partition [$minority] from [$majority]"
    # shellcheck disable=SC2086
    label_side a $minority
    # shellcheck disable=SC2086
    label_side b $majority
    # The policy engine has to see the labels before the flush, or the flows it forgets are
    # re-admitted under the old view and pinned for the rest of the window. The second pass
    # catches whatever slipped through between the two.
    sleep "$FLUSH_SETTLE_SECS"
    flush_flows
    sleep "$FLUSH_SETTLE_SECS"
    flush_flows
    sleep "$PARTITION_SECS"
    heal
    sleep "$RECOVERY_SECS"
}

# A NetworkPolicy is evaluated for new connections only, and the cluster's QUIC flows never end,
# so the labels above cut nothing until conntrack forgets them. The helper runs in the node's
# network namespace, which is the table the policy hooks see.
flush_flows() {
    helper=$(kubectl get pod -l "$FLUSH_SELECTOR" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -z "$helper" ]; then
        echo "no conntrack helper, the partition will not cut established flows"
        return
    fi

    ips=$(kubectl get pod -l "$SELECTOR" \
        -o jsonpath='{range .items[*]}{.status.podIP}{" "}{end}' 2>/dev/null)

    # By address rather than by port: conntrack's port filters match the original direction only,
    # and every flow of every node has to go, not just the ones which happen to be listed that way.
    kubectl exec "$helper" -- nsenter -t 1 -m -n -- sh -c \
        "for ip in $ips; do conntrack -D -p udp -s \$ip; conntrack -D -p udp -d \$ip; done" \
        > /dev/null 2>&1 || true
}

label_side() {
    side=$1
    shift

    for node in "$@"; do
        kubectl label --overwrite pod "$(pod "$node")" "side=$side" > /dev/null ||
            echo "cannot label $(pod "$node")"
    done
}

trap 'heal; exit 0' INT TERM

heal

# The version the cluster runs, so an agent restarted mid roll rolls away from it, not toward it.
version=$(current_version)
[ -n "$version" ] || version=$OLD_VERSION

state startup
kubectl wait --for=condition=Ready --timeout="${STARTUP_SECS}s" pod -l "$SELECTOR" ||
    echo "not every pod became ready"
echo "waiting ${STARTUP_SECS}s for the cluster to form"
sleep "$STARTUP_SECS"

cycle=0
while true; do
    state quiet
    sleep "$QUIET_SECS"

    cycle=$((cycle + 1))
    node=$(((cycle / 4) % NODE_COUNT))
    case $((cycle % 4)) in
        1) kill_node "$node" ;;
        2) partition "$node" "$(((node + 1) % NODE_COUNT))" ;;
        3) leave_node "$node" ;;
        0) roll_nodes ;;
    esac
done
