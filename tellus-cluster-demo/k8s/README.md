# The cluster demo on Kubernetes

The same five nodes, the same verifier and the same guarantees as the Compose stack, in a
[kind](https://kind.sigs.k8s.io) cluster of its own. Read the [demo README](../README.md) first;
this explains only what the manifests do and why.

Everything is scoped to a cluster named `tellus-cluster-demo` and the context
`kind-tellus-cluster-demo`, so no recipe here can touch another kind cluster of yours.

## Why a StatefulSet

A Deployment is the natural default, and it is what an elastic cluster would use, which is what
the Kubernetes discovery backend is really for. This demo needs stable names instead, because
three things address nodes individually:

- the verifier is configured with one URL per node and checks every node's view against every
  other's, reporting by name,
- the chaos agent picks its victim by number, so the numbering must mean the same thing all run,
- `http://localhost:8091` through `8095` are how you watch a partition from both sides, which is
  the only way to see the two halves disagree and then agree again.

A Deployment gives random pod names and no per-pod DNS, so all three would have to list pods
through the API on every use. `tellus-0.tellus` through `tellus-4.tellus` cost one field.

`podManagementPolicy: Parallel` is deliberate: the default starts pods one at a time, and with
`min_peers` at five every node waits for the last one anyway, so ordering would only delay
formation and hide the simultaneous start bootstrap is meant to survive.

## Why no Caddy

Under Compose, Caddy health checks the five nodes so a downed one leaves the rotation before it
stops answering. A Kubernetes service does that natively: `tellus-lb` selects the same pods, and
the readiness probe on `/health` takes an unready node out. The Caddyfile has no counterpart here.

## The partition

[`partition.yaml`](partition.yaml) holds two NetworkPolicies which are applied with everything
else and do nothing until the chaos agent labels pods, since a policy selecting no pod selects
nothing. Each denies its side ingress from the other:

- `NotIn` matches pods with the other side's label absent, **including pods with no `side` label
  at all**, which is what keeps the verifier talking to both halves while they cannot talk to
  each other.
- The `ipBlock` admits every source which is not a pod, which is what lets NodePort traffic from
  your machine reach a partitioned node. That half is a kind arrangement rather than a Kubernetes
  guarantee: where service rewriting happens relative to policy evaluation is left to the
  implementation, so it is worth re-checking all seven exposed ports after a kind upgrade.
- Only ingress is restricted, so egress to CoreDNS and to the API server keeps working and a node
  which restarts mid-partition can still bootstrap.

Blocking ingress on both sides cuts both directions, which is what the Compose stack's two pumba
rules do. Unlike those rules, the label survives a container restart, so a node self-downing
mid-partition stays cut off; that retires a Compose caveat rather than inheriting it. A pod which
is deleted and recreated does come back unlabeled, which is fine, because that is the crash
fault and not a partition.

**The labels alone cut nothing**, which is the one thing about this fault worth knowing. A
NetworkPolicy is evaluated when a connection is new: kindnetd queues the first packet of a flow
to its policy engine, and once the flow is admitted it carries a conntrack label which the
`established,related` rule accepts directly from then on. The cluster's QUIC connections are long
lived and its one second heartbeats keep the conntrack entries fresh indefinitely, so a policy
applied to a running cluster never sees a packet it could drop. Labeling five pods and waiting
changes nothing at all; every node keeps seeing every other as Up.

[`flush.yaml`](flush.yaml) is what closes that gap: a DaemonSet holding nothing but a sleeping
container, which the agent execs into after labeling to delete every conntrack entry of every
node. The flows are forgotten, their next packets count as new, and the policy finally applies.
With the flush the minority self-downs and the majority keeps quorum; without it the partition is
a no-op which still looks like a perfectly healthy run.

Two details of that flush are not optional, and both were found the hard way:

- **It comes after the labels have settled, and it runs twice.** kindnetd's policy engine learns
  the labels through an informer, and a flush racing ahead of it deletes flows which are then
  re-admitted under the old view and pinned for the rest of the window. The agent waits, flushes,
  waits again and flushes again, which also clears anything admitted in between.
- **It deletes by address, not by port.** `conntrack -D -p udp --dport 7878` matches the original
  direction only and leaves most of the cluster's entries in place; deleting `-s` and `-d` for
  each node's address is what actually empties them.

`PARTITION_SECS` is also longer here than under Compose, because a policy cut has to be noticed by
the failure detector and then acted on by the downing provider, where pumba's packet loss is felt
at once. Too short a window and the fault heals before the cluster has reacted, which again looks
like a healthy run.

That helper is privileged and sits in the host's PID and network namespaces, and reaches the
node's conntrack table through `nsenter` rather than shipping a `conntrack` of its own. The table
the policy hooks consult is the node's, so the flush has to happen there; keeping the tool on the
node also keeps the demo image free of it. It is node level access, which is why it is a separate
pod with one job rather than a capability handed to the chaos agent.

This is worth remembering beyond the demo: a NetworkPolicy is not a way to interrupt traffic
which is already flowing, whatever the protocol, and a cluster held together by long lived
connections will not notice one.

## The faults

[`chaos.sh`](chaos.sh) keeps the Compose agent's cycle of faults and the state file the verifier
reads, and expresses each fault in kubectl:

| Fault | Command |
|---|---|
| crash | `kubectl delete pod tellus-N --force --grace-period=0` |
| departure | `kubectl delete pod tellus-N`, the kubelet's SIGTERM within the grace period |
| partition | `kubectl label --overwrite pod ... side=a` / `side=b`, then `kubectl label pod -l app=tellus side-` |

The two ways out of a cluster are the same command with and without its grace period, which reads
better than the Compose pair.

**Why force deletion.** It is the only crash reachable from inside the cluster. A process cannot
SIGKILL PID 1 of its own PID namespace, so `kubectl exec -- kill -KILL 1` returns success and does
nothing, and SIGTERM is the departure fault rather than a crash. Kubernetes warns against force
deleting StatefulSet pods because it frees the name before the kubelet confirms the process is
gone, which breaks applications needing at most one instance per identity. That does not apply
here: these pods hold no volume, identity in tellus is the advertised address together with the
incarnation, so a replacement is a distinct member even at an address its predecessor held, and
stale members are exactly what incarnations and downing exist for. A duplicate which did survive
would show up as a sixth Up member and fail the verifier's next check.

**Why there is no dead window.** The StatefulSet replaces a deleted pod at once, so nothing keeps
a node down for a fixed time, and the Compose agent's `DEAD_SECS` has no counterpart. It is not
needed: the replacement normally comes up on a different address, so the crashed member's address
never returns and the survivors have to detect and down it before any check can pass. Compose
needs the window because its containers always return at the same static address.

**Recovering the agent.** The policies are static and label driven, so an agent which died in the
middle of a partition would leave the cluster split for good. It therefore clears every `side`
label at startup and from a `TERM` trap, and always labels with `--overwrite`.

## RBAC

[`rbac.yaml`](rbac.yaml) grants two service accounts the least it can:

- `tellus`, which the nodes run as, gets `pods: list` in this namespace. That is exactly what
  `K8sSeeds` calls and nothing else. Under DNS discovery it is unused.
- `tellus-chaos`, which the verifier pod runs as, gets `pods: get, list, watch, patch, delete`
  plus `pods/exec: create`. `patch` is what `kubectl label` uses, `watch` what `kubectl wait`
  needs at startup, and `exec` is how it reaches the conntrack helper. It has no permission on
  networkpolicies, because it never creates one.

A Role and its binding are namespaced, so both live beside the pods. Listing another namespace
would mean binding there instead and naming this namespace in the subject.

## Versions

`kindest/node` and `alpine/k8s` are pinned to a full patch version and a digest rather than the
`major.minor` used elsewhere in this repository. Neither publishes a rolling minor tag, and kind
instructs pinning its node image by digest because even its tags are not guaranteed to be the
image built for a given release. The kubelet's `crashLoopBackOff.maxContainerRestartPeriod`
requires Kubernetes 1.35 or later, where its feature gate is on by default, and the partition
depends on kindnetd enforcing NetworkPolicy; the pin is what makes both true rather than hoped
for.
