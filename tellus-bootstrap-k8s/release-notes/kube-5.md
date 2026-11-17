### This release requires kube 5 and k8s-openapi 0.29

`K8sSeeds::with_client` takes a `kube::Client`, and `K8sSeedsError::Client` holds a `kube::Error`,
so the kube this crate is built against is part of its API. This release depends on kube 5.0 and
k8s-openapi 0.29. An application which builds its own client upgrades both together with this
crate. A binary which enables a Kubernetes version feature of k8s-openapi enables it on 0.29.
