# tellus-bootstrap-k8s

Kubernetes seed discovery for [tellus](../tellus) cluster bootstrap: `K8sSeeds` implements
`SeedDiscovery`, listing the pods matching a label selector through the Kubernetes API and pairing
each pod's address with a port, either one number for all of them or the number a named container
port carries in each pod. Unlike a headless service, which publishes ready pods only, this sees a
pod as soon as it has an address, so a readiness probe may gate on cluster membership.

A pod contributes exactly one address, the primary one Kubernetes reports as `status.podIP` and
repeats as the first entry of `status.podIPs`. Bootstrap counts addresses, so a dual stack pod
counted twice would inflate both `min_peers` and the majority the formation provider counts, and
only the primary address is the one a node advertises and is hence admitted at. Terminating and
finished pods are left out, since they would inflate that count as well, while a pod which is
merely still starting stays in: an unreachable seed is retried, a missing one shrinks the universe.

For how bootstrap joins and when it forms a new cluster, from the settle window to the
lowest-address formation rule, see the Bootstrap paragraph of the Membership section in
[docs/cluster.md](../docs/cluster.md#membership).

tellus-bootstrap-k8s is available on [crates.io](https://crates.io/crates/tellus-bootstrap-k8s):

```sh
cargo add tellus --features cluster
cargo add tellus-bootstrap-k8s
cargo add k8s-openapi --features v1_33
```

The last one is not optional. `k8s-openapi` requires the minimum Kubernetes version to be chosen
by a feature, and only the binary may choose it, so a library crate cannot do it for you: without
it your build fails with "None of the v1_* features are enabled".

Start the endpoint, then bootstrap through the discovered seeds, bounded by a timeout of your
choosing:

```rust
cluster::start_endpoint(EndpointConfig::new(addr), transport)?;
let seeds = K8sSeeds::new(Pods {
    namespace: None,
    label_selector: "app=tellus".to_string(),
    port: Port::Number(NonZeroU16::new(7878).expect("7878 is not zero")),
})
.await?;
timeout(Duration::from_secs(60), bootstrap(seeds, BootstrapConfig::new())).await??;
```

`K8sSeeds::new` takes the service account mounted into the pod, or the kubeconfig outside a
cluster; `with_client` takes a client of your own. Both refuse a blank selector, which would match
every pod in the namespace, as well as a blank namespace or port name.

With the `serde` feature `Pods` is deserializable, so it can come from a config file next to
tellus's own configuration:

```yaml
seeds:
  label_selector: app=tellus
  port:
    number: 7878
```

```yaml
seeds:
  namespace: tellus
  label_selector: app=tellus
  port:
    name: tellus
```

The named form takes the number of that container port from each pod's own specification, so nodes
can advertise differing ports. Without a namespace the client's own is listed, which in a pod is
the namespace it runs in.

A listing which yields no address at all is a resolve failure rather than an empty seed list, so
bootstrap retries it and its settle window starts over rather than this node settling on itself.
That happens while the matching pods have no address yet, when all of them are terminating or
finished, when none of them carries the configured port name, and of course when the selector
matches nothing.

## Deploying

Listing pods is a permission, so the pods run as a service account of their own:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tellus
```

named by the pod template as `serviceAccountName: tellus`, and granted the one verb this crate
uses:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: tellus-seeds
rules:
  - apiGroups: [ "" ]
    resources: [ pods ]
    verbs: [ list ]
```

bound to that service account:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: tellus-seeds
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: tellus-seeds
subjects:
  - kind: ServiceAccount
    name: tellus
    namespace: tellus
```

A Role grants access to its own namespace, so the Role and the RoleBinding belong to the namespace
being listed, which is the `namespace` of the configuration or, without one, the namespace the
pods run in. The subject's `namespace` is the service account's, which is what lets pods list a
namespace other than their own: bind there, and name the namespace the pods run in. It defaults to
the binding's namespace, so it may be left out when everything is in one namespace, but spelling
it out keeps the two apart.

The address a node advertises comes from the downward API, paired with the same port the
discovery is configured with:

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: CFG__ENDPOINT__ADVERTISED_ADDR
    value: "$(POD_IP):7878"
```

Kubernetes expands `$(VAR)` in an environment value referring to an earlier entry. On an IPv6
cluster the address needs the bracketed form, `"[$(POD_IP)]:7878"`, since that is how a socket
address with an IPv6 host parses. The variable name is the one
[configured](https://github.com/hseeberger/configured) reads for `EndpointConfig::advertised_addr`;
another loader wants its own.

Size `bootstrap.min_peers` to the replica count for a fixed size cluster and lower for an elastic
one. Whether the pods come from a StatefulSet or a Deployment makes no difference to discovery.

## A working deployment

[`tellus-cluster-demo`](../tellus-cluster-demo) runs five nodes on Kubernetes under continuous
chaos, and `just start-cluster-demo-k8s k8s` starts them with this crate's discovery. Its
[manifests](../tellus-cluster-demo/k8s) are the excerpts above as a deployable set, in the
`default` namespace instead of `tellus`.

[`examples/how_to_run_on_kubernetes.rs`](examples/how_to_run_on_kubernetes.rs) with
[its manifests](examples/how_to_run_on_kubernetes.yaml) is a minimal deployment: each node reports
itself ready once it is a member and leaves the cluster on SIGTERM. Build it with
`cargo build -p tellus-bootstrap-k8s --example how_to_run_on_kubernetes`.

## Tests

[`tests/k8s_seeds.rs`](tests/k8s_seeds.rs) answers the client from an in-process
[tower-test](https://crates.io/crates/tower-test) mock service, so the tests never touch an API
server and need no cluster.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
