#!/bin/sh
# The chaos agent: every cycle it kills one node, stops one node or partitions two nodes off the
# other three, and names what it is doing in the state file the verifier reads, so the cluster is
# only held to its promises once it has had a quiet window to recover.

set -eu

NODE_COUNT=${NODE_COUNT:-5}
CONTAINER_PREFIX=${CONTAINER_PREFIX:-tellus-demo-node}
IP_PREFIX=${IP_PREFIX:-172.28.0.1}
INTERFACES=${INTERFACES:-eth0 eth1}
PUMBA_IMAGE=${PUMBA_IMAGE:-gaiaadm/pumba:1.2.1}
STATE_FILE=${STATE_FILE:-/chaos/state}
STARTUP_SECS=${STARTUP_SECS:-45}
DEAD_SECS=${DEAD_SECS:-20}
QUIET_SECS=${QUIET_SECS:-45}
PARTITION_SECS=${PARTITION_SECS:-30}
RECOVERY_SECS=${RECOVERY_SECS:-25}

state() {
    echo "$1" > "$STATE_FILE"
    echo "chaos: $1"
}

pumba() {
    docker run --rm -v /var/run/docker.sock:/var/run/docker.sock "$PUMBA_IMAGE" "$@"
}

container() {
    echo "${CONTAINER_PREFIX}$1"
}

ip_of() {
    echo "${IP_PREFIX}$1"
}

# Kills a node outright, so the cluster detects its silence rather than a departure, and only
# the fresh incarnation of the started container can rejoin.
kill_node() {
    state "kill $(container "$1")"
    pumba kill --signal SIGKILL "$(container "$1")" || echo "pumba kill failed"
    sleep "$DEAD_SECS"
    revive "$1"
}

# Terminates a node, so it announces its departure and the cluster downs it within a gossip
# round instead of detecting its silence.
leave_node() {
    state "leave $(container "$1")"
    docker kill --signal SIGTERM "$(container "$1")" || echo "docker kill failed"
    sleep "$DEAD_SECS"
    revive "$1"
}

# A container signaled through the API counts as manually stopped, which no restart policy
# undoes; only a node exiting on its own, e.g. once the cluster has downed it, is restarted.
revive() {
    docker start "$(container "$1")" || echo "docker start failed"
    sleep "$RECOVERY_SECS"
}

# Drops everything between the two sides in both directions, which is what makes it a partition
# rather than one silent node: the majority downs the minority, the minority downs itself.
partition() {
    minority="$*"
    majority=
    for node in $(seq 1 "$NODE_COUNT"); do
        case " $minority " in
            *" $node "*) ;;
            *) majority="$majority $node" ;;
        esac
    done

    majority="${majority# }"

    state "partition [$minority] from [$majority]"
    netem_side "$(ips $majority)" "$(containers $minority)"
    netem_side "$(ips $minority)" "$(containers $majority)"
    wait || true
    sleep "$RECOVERY_SECS"
}

# The cluster network's interface is not named the same way in every container, so the rules go
# on all of them; on the wrong one they match nothing.
netem_side() {
    targets=$1
    side=$2

    for interface in $INTERFACES; do
        args=
        for target in $targets; do
            args="$args --target $target"
        done

        # shellcheck disable=SC2086
        pumba netem --duration "${PARTITION_SECS}s" --interface "$interface" $args \
            loss --percent 100 $side &
    done
}

ips() {
    for node in "$@"; do
        ip_of "$node"
    done
}

containers() {
    for node in "$@"; do
        container "$node"
    done
}

docker pull "$PUMBA_IMAGE" || echo "cannot pull $PUMBA_IMAGE, chaos actions will fail"

# An agent which died mid fault leaves a node stopped, so nothing is decided before every node
# is running again.
for node in $(seq 1 "$NODE_COUNT"); do
    docker start "$(container "$node")" > /dev/null 2>&1 || true
done

state startup
echo "waiting ${STARTUP_SECS}s for the cluster to form"
sleep "$STARTUP_SECS"

cycle=0
while true; do
    state quiet
    sleep "$QUIET_SECS"

    cycle=$((cycle + 1))
    node=$(((cycle / 3) % NODE_COUNT + 1))
    case $((cycle % 3)) in
        1) kill_node "$node" ;;
        2) partition "$node" "$((node % NODE_COUNT + 1))" ;;
        0) leave_node "$node" ;;
    esac
done
