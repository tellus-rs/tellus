use anyhow::Context;
use http::{Request, Response, StatusCode};
use kube::{Client, client::Body};
use serde_json::{Value, json};
use std::{net::SocketAddr, num::NonZeroU16, pin::pin};
use tellus::cluster::SeedDiscovery;
use tellus_bootstrap_k8s::{InvalidPods, K8sSeeds, K8sSeedsError, Pods, Port};
use tokio::task::JoinHandle;
use tower_test::mock;

const NAMESPACE: &str = "tellus";
const SELECTOR: &str = "app=tellus";
const PORT: NonZeroU16 = NonZeroU16::new(7_878).expect("7878 is not zero");

/// Every pod which is running and has an address contributes it, paired with the one configured
/// port. A terminating and a finished pod contribute nothing, since counting them would inflate
/// the universe bootstrap decides over, and a pending pod has no address to contribute yet.
#[tokio::test]
async fn test_number_port() -> anyhow::Result<()> {
    let pods = pod_list(vec![
        running("node-0", "10.0.0.1"),
        terminating("node-1", "10.0.0.2"),
        succeeded("node-2", "10.0.0.3"),
        pending("node-3"),
        running("node-4", "10.0.0.5"),
    ]);
    let mut seeds = seeds(Port::Number(PORT), None, StatusCode::OK, pods)?;

    let resolved = seeds.resolve().await.context("resolving pods")?;

    assert_eq!(
        resolved,
        vec![addr("10.0.0.1", 7_878), addr("10.0.0.5", 7_878)]
    );
    Ok(())
}

/// A pod contributes its primary address only: a dual stack pod counted twice would inflate
/// `min_peers` and the formation majority, and only the primary address is the one the node
/// advertises. Without `podIP` the first `podIPs` entry is that primary address.
#[tokio::test]
async fn test_primary_address_only() -> anyhow::Result<()> {
    let mut dual_stack = running("node-0", "10.0.0.1");
    dual_stack["status"]["podIPs"] = json!([{ "ip": "10.0.0.1" }, { "ip": "fd00::1" }]);
    let mut ips_only = running("node-1", "10.0.0.2");
    ips_only["status"]["podIP"] = Value::Null;
    ips_only["status"]["podIPs"] = json!([{ "ip": "fd00::2" }, { "ip": "10.0.0.2" }]);

    let pods = pod_list(vec![dual_stack, ips_only]);
    let mut seeds = seeds(Port::Number(PORT), None, StatusCode::OK, pods)?;

    let resolved = seeds.resolve().await.context("resolving pods")?;

    assert_eq!(
        resolved,
        vec![addr("10.0.0.1", 7_878), addr("fd00::2", 7_878)]
    );
    Ok(())
}

/// A named port is taken from each pod's own specification, so nodes can advertise differing
/// ports. A pod which does not carry that name, or carries a number outside the port range, is
/// skipped instead of contributing an address nothing listens at.
#[tokio::test]
async fn test_named_port() -> anyhow::Result<()> {
    let mut other_name = running("node-2", "10.0.0.3");
    other_name["spec"]["containers"][0]["ports"] = json!([{ "name": "http", "containerPort": 80 }]);
    let mut zero = running("node-3", "10.0.0.4");
    zero["spec"]["containers"][0]["ports"] = json!([{ "name": "tellus", "containerPort": 0 }]);
    let mut too_large = running("node-4", "10.0.0.5");
    too_large["spec"]["containers"][0]["ports"] =
        json!([{ "name": "tellus", "containerPort": 70_000 }]);

    let pods = pod_list(vec![
        with_named_port(running("node-0", "10.0.0.1"), 7_878),
        with_named_port(running("node-1", "10.0.0.2"), 8_888),
        other_name,
        zero,
        too_large,
    ]);
    let mut seeds = seeds(Port::Name("tellus".to_string()), None, StatusCode::OK, pods)?;

    let resolved = seeds.resolve().await.context("resolving pods")?;

    assert_eq!(
        resolved,
        vec![addr("10.0.0.1", 7_878), addr("10.0.0.2", 8_888)]
    );
    Ok(())
}

/// An address which is no address is skipped, not fatal: the other pods still seed bootstrap.
#[tokio::test]
async fn test_unparsable_address() -> anyhow::Result<()> {
    let pods = pod_list(vec![
        running("node-0", "not-an-address"),
        running("node-1", "10.0.0.2"),
    ]);
    let mut seeds = seeds(Port::Number(PORT), None, StatusCode::OK, pods)?;

    let resolved = seeds.resolve().await.context("resolving pods")?;

    assert_eq!(resolved, vec![addr("10.0.0.2", 7_878)]);
    Ok(())
}

/// A listing yielding no address is a resolve failure, not an empty seed list: bootstrap must
/// keep retrying it instead of settling on this node alone.
#[tokio::test]
async fn test_no_addresses() -> anyhow::Result<()> {
    let mut seeds = seeds(Port::Number(PORT), None, StatusCode::OK, pod_list(vec![]))?;

    let error = seeds.resolve().await.expect_err("no pod contributes");

    assert!(matches!(error, K8sSeedsError::NoAddresses { .. }));
    Ok(())
}

/// The listing goes to the configured namespace, falling back to the client's own, and carries
/// the label selector, so pods of other clusters in the same namespace are never seeds.
#[tokio::test]
async fn test_request() -> anyhow::Result<()> {
    let pods = pod_list(vec![running("node-0", "10.0.0.1")]);
    let (client, request) = client_answering(StatusCode::OK, pods.clone());
    let mut seeds =
        K8sSeeds::with_client(pods_config(Port::Number(PORT), Some(NAMESPACE)), client)?;
    seeds.resolve().await.context("resolving pods")?;

    let request = request.await.context("awaiting the request")?;
    let uri = request.uri().to_string();
    assert!(uri.starts_with("/api/v1/namespaces/tellus/pods?"), "{uri}");
    assert!(uri.contains("labelSelector=app%3Dtellus"), "{uri}");

    let (client, request) = client_answering(StatusCode::OK, pods);
    let mut seeds = K8sSeeds::with_client(pods_config(Port::Number(PORT), None), client)?;
    seeds.resolve().await.context("resolving pods")?;

    let request = request.await.context("awaiting the request")?;
    let uri = request.uri().to_string();
    assert!(uri.starts_with("/api/v1/namespaces/default/pods?"), "{uri}");
    Ok(())
}

/// An API server which refuses the listing, e.g. for want of the permission to list pods, is a
/// resolve failure bootstrap retries, not a panic and not an empty seed list.
#[tokio::test]
async fn test_api_error() -> anyhow::Result<()> {
    let status = json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "message": "pods is forbidden",
        "reason": "Forbidden",
        "code": 403,
    });
    let mut seeds = seeds(Port::Number(PORT), None, StatusCode::FORBIDDEN, status)?;

    let error = seeds.resolve().await.expect_err("the API server refuses");

    assert!(matches!(error, K8sSeedsError::Client(_)));
    Ok(())
}

/// A blank selector matches every pod in the namespace, so it is refused when the discovery is
/// built rather than seeding the cluster from strangers; a blank namespace or port name cannot
/// name anything either.
#[tokio::test]
async fn test_invalid_pods() -> anyhow::Result<()> {
    let blank_selector = Pods {
        namespace: None,
        label_selector: " ".to_string(),
        port: Port::Number(PORT),
    };
    let blank_namespace = Pods {
        namespace: Some("".to_string()),
        ..pods_config(Port::Number(PORT), None)
    };
    let blank_port_name = pods_config(Port::Name("".to_string()), None);

    for (pods, expected) in [
        (blank_selector, InvalidPods::BlankLabelSelector),
        (blank_namespace, InvalidPods::BlankNamespace),
        (blank_port_name, InvalidPods::BlankPortName),
    ] {
        let error =
            K8sSeeds::with_client(pods, disconnected_client()).expect_err("invalid pods refused");

        assert!(
            matches!(error, K8sSeedsError::Config(error) if error.to_string() == expected.to_string())
        );
    }

    Ok(())
}

/// Invalid pods are refused before a client is built, so a blank selector is a configuration
/// error whether or not this process has a usable Kubernetes configuration at all.
#[tokio::test]
async fn test_invalid_pods_before_client() -> anyhow::Result<()> {
    let pods = Pods {
        namespace: None,
        label_selector: " ".to_string(),
        port: Port::Number(PORT),
    };

    let error = K8sSeeds::new(pods).await.expect_err("invalid pods refused");

    assert!(matches!(
        error,
        K8sSeedsError::Config(InvalidPods::BlankLabelSelector)
    ));
    Ok(())
}

/// Discovery answered by an in-process mock service, so the tests never touch an API server.
fn seeds(
    port: Port,
    namespace: Option<&str>,
    status: StatusCode,
    body: Value,
) -> anyhow::Result<K8sSeeds> {
    let (client, _) = client_answering(status, body);
    let seeds = K8sSeeds::with_client(pods_config(port, namespace), client)?;

    Ok(seeds)
}

/// A client whose one request is answered with the given status and body, plus the request it
/// received, so a test can assert what was asked of the API server.
fn client_answering(status: StatusCode, body: Value) -> (Client, JoinHandle<Request<Body>>) {
    let (service, handle) = mock::pair::<Request<Body>, Response<Body>>();

    let request = tokio::spawn(async move {
        let mut handle = pin!(handle);
        let (request, send) = handle
            .next_request()
            .await
            .expect("the service is called once");
        let body = serde_json::to_vec(&body).expect("the body serializes");
        let response = Response::builder()
            .status(status)
            .body(Body::from(body))
            .expect("the response builds");
        send.send_response(response);

        request
    });

    (Client::new(service, "default"), request)
}

fn disconnected_client() -> Client {
    let (service, _) = mock::pair::<Request<Body>, Response<Body>>();

    Client::new(service, "default")
}

fn pods_config(port: Port, namespace: Option<&str>) -> Pods {
    Pods {
        namespace: namespace.map(ToString::to_string),
        label_selector: SELECTOR.to_string(),
        port,
    }
}

fn pod_list(pods: Vec<Value>) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {},
        "items": pods,
    })
}

fn running(name: &str, ip: &str) -> Value {
    json!({
        "metadata": { "name": name },
        "spec": { "containers": [{ "name": "node", "image": "tellus" }] },
        "status": { "phase": "Running", "podIP": ip, "podIPs": [{ "ip": ip }] },
    })
}

fn terminating(name: &str, ip: &str) -> Value {
    let mut pod = running(name, ip);
    pod["metadata"]["deletionTimestamp"] = json!("2026-09-04T10:00:00Z");

    pod
}

fn succeeded(name: &str, ip: &str) -> Value {
    let mut pod = running(name, ip);
    pod["status"]["phase"] = json!("Succeeded");

    pod
}

fn pending(name: &str) -> Value {
    json!({
        "metadata": { "name": name },
        "spec": { "containers": [{ "name": "node", "image": "tellus" }] },
        "status": { "phase": "Pending" },
    })
}

fn with_named_port(mut pod: Value, port: u16) -> Value {
    pod["spec"]["containers"][0]["ports"] =
        json!([{ "name": "tellus", "containerPort": port, "protocol": "UDP" }]);

    pod
}

fn addr(ip: &str, port: u16) -> SocketAddr {
    SocketAddr::new(ip.parse().expect("valid IP"), port)
}
