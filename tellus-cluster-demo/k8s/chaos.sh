#!/bin/sh
# The chaos agent: every cycle it kills one node, stops one node or partitions two nodes off the
# other three, and names what it is doing in the state file the verifier reads, so the cluster is
# only held to its promises once it has had a quiet window to recover.

set -eu

NODE_COUNT=${NODE_COUNT:-5}
POD_PREFIX=${POD_PREFIX:-tellus-}
SELECTOR=${SELECTOR:-app=tellus}
FLUSH_SELECTOR=${FLUSH_SELECTOR:-app=tellus-conntrack}
FLUSH_SETTLE_SECS=${FLUSH_SETTLE_SECS:-5}
STATE_FILE=${STATE_FILE:-/chaos/state}
STARTUP_SECS=${STARTUP_SECS:-60}
QUIET_SECS=${QUIET_SECS:-45}
PARTITION_SECS=${PARTITION_SECS:-75}
RECOVERY_SECS=${RECOVERY_SECS:-25}

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
    node=$(((cycle / 3) % NODE_COUNT))
    case $((cycle % 3)) in
        1) kill_node "$node" ;;
        2) partition "$node" "$(((node + 1) % NODE_COUNT))" ;;
        0) leave_node "$node" ;;
    esac
done
