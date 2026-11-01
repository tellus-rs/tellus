# tellus-bootstrap-dns

DNS seed discovery for [tellus](../tellus) cluster bootstrap: `DnsSeeds` implements
`SeedDiscovery`, resolving seed addresses from SRV records, which carry address and port (e.g. a
Kubernetes headless service), or from A/AAAA records paired with a configured port.
SRV targets are resolved concurrently and independently: a failing target is ignored while any
other target resolves, and a view resolving no address at all remains an error for bootstrap to
retry. A stale target and a transient failure are indistinguishable to the resolver and are hence
skipped alike. A partial view does settle, so a target failing throughout the settle window shrinks
the universe the formation provider counts its majority against.

The default resolver keeps no response cache: bootstrap polls DNS to see nodes come and go, and a
cached answer would hide a node registered after it for the record's TTL, e.g. a sibling restarting
a moment later than this one. A resolver given via `with_resolver` is used as configured.

For how bootstrap joins and when it forms a new cluster, from the settle window to the
lowest-address formation rule, see the Bootstrap paragraph of the Membership section in
[docs/cluster.md](../docs/cluster.md#membership).

tellus-bootstrap-dns is available on [crates.io](https://crates.io/crates/tellus-bootstrap-dns):

```sh
cargo add tellus --features cluster
cargo add tellus-bootstrap-dns
```

Start the endpoint, then bootstrap through the discovered seeds, bounded by a timeout of your
choosing:

```rust
cluster::start_endpoint(EndpointConfig::new(addr), transport)?;
let seeds = DnsSeeds::srv("_tellus._udp.tellus.default.svc.cluster.local")?;
timeout(Duration::from_secs(60), bootstrap(seeds, BootstrapConfig::new())).await??;
```

With the `serde` feature the `Query` deciding what to resolve is deserializable, so it can come
from a config file next to tellus's own configuration, and `DnsSeeds::new` takes it:

```yaml
seeds:
  srv: _tellus._udp.tellus.default.svc.cluster.local
```

```yaml
seeds:
  ip:
    name: tellus
    port: 7878
```

That is the normalized map form a loader like [config](https://crates.io/crates/config) produces,
which is the one tellus's own configuration assumes; `serde_yaml` expects a YAML tag (`seeds:
!srv ...`) instead, see the configuration section of the [tellus README](../tellus/README.md).

## Kubernetes

A headless service over the node pods is all this crate needs, so nothing but the service name is
configured:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: tellus
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ipFamilyPolicy: SingleStack
  ipFamilies: [ IPv4 ]
  selector:
    app: tellus
  ports:
    - name: tellus
      protocol: UDP
      port: 7878
      targetPort: tellus
```

The service name resolves to every pod's address, which is the `ip` form paired with the well
known port. The named port additionally yields one SRV record per pod at
`_tellus._udp.tellus.<namespace>.svc.cluster.local`, which is the `srv` form. Pods advertise
differing ports only if `targetPort` names a container port instead of giving a number, so each
pod contributes the port its own container declares:

```yaml
ports:
  - name: tellus
    containerPort: 7878
    protocol: UDP
```

Two of the service's settings must be exactly as shown:

- `publishNotReadyAddresses: true`, because a headless service publishes ready pods only. A
  readiness probe gated on cluster membership would hide every pod from every other one, so no
  node could ever settle: bootstrap would be waiting for itself.
- `ipFamilyPolicy: SingleStack` with `ipFamilies: [ IPv4 ]`, because a dual stack service answers
  both A and AAAA for every pod. Bootstrap counts addresses, so each node would count twice
  towards `min_peers` and towards the formation provider's majority, and only one of the two
  addresses is the one the node advertises; a peer dialed at any other address is refused.

The address a node advertises comes from the downward API, paired with the same port:

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
one; the Bootstrap paragraph of [docs/cluster.md](../docs/cluster.md#membership) spells out what each choice
costs.
Whether the pods come from a StatefulSet or a Deployment makes no difference to discovery.

Where the readiness rule is in the way, [tellus-bootstrap-k8s](../tellus-bootstrap-k8s) lists the
pods through the Kubernetes API instead, which sees a pod as soon as it has an address.

## A working deployment

[`tellus-cluster-demo`](../tellus-cluster-demo) runs five nodes on Kubernetes under continuous
chaos, and `just start-cluster-demo-k8s` starts them with this crate's discovery. Its
[manifests](../tellus-cluster-demo/k8s) are the excerpts above as a deployable set.

## Tests

[`tests/dns_seeds.rs`](tests/dns_seeds.rs) resolves against an in-process
[hickory-server](https://crates.io/crates/hickory-server) on an ephemeral local port, so the tests
never touch real DNS.

## License

This code is open source software licensed under the
[Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).
