#!/bin/sh
# The chaos agent: every cycle it kills one node, partitions two nodes off the other three, stops
# one node or rolls all five to the other application version, and names what it is doing in the
# state file the verifier reads, so the cluster is only held to its promises once it has had a
# quiet window to recover.

set -eu

NODE_COUNT=${NODE_COUNT:-5}
CONTAINER_PREFIX=${CONTAINER_PREFIX:-tellus-demo-node}
IP_PREFIX=${IP_PREFIX:-172.28.0.1}
INTERFACES=${INTERFACES:-eth0 eth1}
PUMBA_IMAGE=${PUMBA_IMAGE:-gaiaadm/pumba:1.2.1}
STATE_FILE=${STATE_FILE:-/chaos/state}
VERSION_FILE=${VERSION_FILE:-/chaos/version}
OLD_VERSION=${OLD_VERSION:-1.0.0}
NEW_VERSION=${NEW_VERSION:-2.0.0}
STARTUP_SECS=${STARTUP_SECS:-45}
DEAD_SECS=${DEAD_SECS:-20}
QUIET_SECS=${QUIET_SECS:-45}
# The quiet window between two nodes of one roll; the windows before and after the roll are the
# cycle's own. Below the verifier's settle time no mixed cluster is verified, which it reports.
ROLL_QUIET_SECS=${ROLL_QUIET_SECS:-$QUIET_SECS}
PARTITION_SECS=${PARTITION_SECS:-30}
RECOVERY_SECS=${RECOVERY_SECS:-25}
RETRY_SECS=${RETRY_SECS:-10}
STOP_WAIT_SECS=${STOP_WAIT_SECS:-30}

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

# Runs a command until it succeeds. The state file keeps naming the fault meanwhile, so a step
# which cannot complete never lets the verifier take a half-rolled cluster for a quiet one.
until_done() {
    until "$@"; do
        echo "chaos: '$*' failed, retrying in ${RETRY_SECS}s" >&2
        sleep "$RETRY_SECS"
    done
}

# Stops a node with SIGTERM. A node already stopped counts as stopped: a kill whose answer was
# lost, or a node which exited on its own, must not be retried forever. The wait is bounded and
# follows a delivered signal only, so every path returns to the inspection.
stop_node() {
    while true; do
        case $(docker inspect -f '{{.State.Running}}' "$1" 2> /dev/null) in
            false) return 0 ;;
            true)
                if docker kill --signal SIGTERM "$1" > /dev/null; then
                    timeout "$STOP_WAIT_SECS" docker wait "$1" > /dev/null || true
                else
                    echo "chaos: cannot signal $1, retrying in ${RETRY_SECS}s"
                    sleep "$RETRY_SECS"
                fi
                ;;
            *)
                echo "chaos: cannot inspect $1, retrying in ${RETRY_SECS}s"
                sleep "$RETRY_SECS"
                ;;
        esac
    done
}

# Rolls every node to the other application version, one at a time with a quiet window after
# each restart, so the verifier checks every mixed cluster. Every container start reads the
# version file, which is also what brings a node killed later back at the version the cluster
# runs.
roll_nodes() {
    if [ "$version" = "$OLD_VERSION" ]; then
        target=$NEW_VERSION
        verb=upgrade
    else
        target=$OLD_VERSION
        verb=downgrade
    fi

    state "$verb to $target"
    echo "$target" > "$VERSION_FILE"
    for node in $(seq "$NODE_COUNT" -1 1); do
        if [ "$node" -ne "$NODE_COUNT" ]; then
            state quiet
            sleep "$ROLL_QUIET_SECS"
        fi
        roll_node "$node" "$target" "$verb"
    done
    version=$target
}

# A departure as leave_node does it, but restarted as soon as the node has exited, like a
# deploy, and only once each step has completed: a node which did not restart would otherwise
# be followed by the next node's restart, two nodes down at once and a mixed cluster the verifier
# never saw.
roll_node() {
    state "$3 $(container "$1") to $2"
    stop_node "$(container "$1")"
    until_done docker start "$(container "$1")" > /dev/null
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

# The version the cluster runs, so an agent restarted mid roll rolls away from it, not toward it.
if [ -r "$VERSION_FILE" ]; then
    version=$(cat "$VERSION_FILE")
else
    version=$OLD_VERSION
fi

state startup
echo "waiting ${STARTUP_SECS}s for the cluster to form"
sleep "$STARTUP_SECS"

cycle=0
while true; do
    state quiet
    sleep "$QUIET_SECS"

    cycle=$((cycle + 1))
    node=$(((cycle / 4) % NODE_COUNT + 1))
    case $((cycle % 4)) in
        1) kill_node "$node" ;;
        2) partition "$node" "$((node % NODE_COUNT + 1))" ;;
        3) leave_node "$node" ;;
        0) roll_nodes ;;
    esac
done
