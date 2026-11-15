### Each listing is bounded by `list_timeout`

The Kubernetes client has no response timeout, so a listing on a connection which went silent never
returned, and bootstrap or its probe of foreign clusters hung in it. `Pods` has a new field,
`list_timeout`, and a listing not answered within it fails with the new
`K8sSeedsError::ListTimeout`, which bootstrap retries like any other resolve failure. A zero
`list_timeout` is refused as `InvalidPods::ZeroListTimeout`.

A `Pods` built in code needs the new field; `Pods::DEFAULT_LIST_TIMEOUT`, 5 seconds, is what a
deserialized `Pods` defaults to. In a config file the key is optional and takes a duration such as
`list_timeout: 2s`.
