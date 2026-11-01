//! Kubernetes seed discovery for tellus cluster bootstrap: [K8sSeeds] implements [SeedDiscovery],
//! listing the pods matching a label selector through the Kubernetes API and pairing each pod's
//! address with a port. That port is either one number for all pods or the number a named container
//! port carries in each pod. Pass it to [bootstrap](tellus::cluster::bootstrap), which lists the
//! pods every `resolve_interval` and joins through the listed addresses on every listing, retrying
//! failures, e.g. pods still starting while an orchestrator rolls out the others. Only forming a
//! new cluster waits for the view to settle. Unlike a headless service, which publishes ready
//! pods only, this sees a pod as soon as it has an address, so a readiness probe may gate on
//! cluster membership.
//!
//! A pod contributes exactly one address, the primary one Kubernetes reports as `status.podIP`
//! and repeats as the first entry of `status.podIPs`. Bootstrap counts addresses, so a dual stack
//! pod counted twice would inflate both `min_peers` and the majority a
//! [FormationProvider](tellus::cluster::formation::FormationProvider) counts. And only the primary
//! address is the one a node advertises, hence the one it is admitted at. Terminating and finished
//! pods are left out, since they would inflate that count as well. A pod which is merely still
//! starting stays in: an unreachable seed is retried, a missing one shrinks the universe. An
//! unparsable address and a missing or unusable named port skip that pod and are logged; a pod
//! without an address yet is skipped silently. A listing yielding no address at all remains an
//! error, so bootstrap does not settle on this node alone.

#![warn(missing_docs, clippy::missing_errors_doc)]

use derive_more::Debug;
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, api::ListParams};
use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU16,
};
use tellus::cluster::SeedDiscovery;
use thiserror::Error;
use tracing::warn;

/// Seed discovery via the Kubernetes API, the [SeedDiscovery] for clusters whose nodes are pods
/// carrying a common label.
#[derive(Debug)]
pub struct K8sSeeds {
    #[debug(skip)]
    api: Api<Pod>,

    pods: Pods,
}

impl K8sSeeds {
    /// Discovery of the given [Pods] through the default client: the service account mounted into
    /// a pod, or the kubeconfig outside a cluster. Unless the [Pods] name a namespace, the
    /// client's own is listed, which in a pod is the namespace it runs in.
    ///
    /// # Errors
    /// Fails if the given [Pods] carry a blank selector, namespace or port name, which is
    /// decided before any client is built, or if there is no usable client configuration.
    pub async fn new(pods: Pods) -> Result<Self, K8sSeedsError> {
        validate(&pods)?;

        let client = Client::try_default().await?;
        Self::with_client(pods, client)
    }

    /// Discovery through the given client, e.g. one pointed at a specific API server.
    ///
    /// # Errors
    /// Fails if the given [Pods] carry a blank selector, namespace or port name.
    pub fn with_client(pods: Pods, client: Client) -> Result<Self, K8sSeedsError> {
        validate(&pods)?;

        let api = match &pods.namespace {
            Some(namespace) => Api::namespaced(client, namespace),
            None => Api::default_namespaced(client),
        };

        Ok(Self { api, pods })
    }
}

impl SeedDiscovery for K8sSeeds {
    type Error = K8sSeedsError;

    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, K8sSeedsError> {
        let params = ListParams::default().labels(&self.pods.label_selector);
        let pods = self.api.list(&params).await?;

        let addrs = pods
            .items
            .iter()
            .filter(|pod| live(pod))
            .filter_map(|pod| addr(pod, &self.pods.port))
            .collect::<Vec<_>>();

        if addrs.is_empty() {
            Err(K8sSeedsError::NoAddresses {
                selector: self.pods.label_selector.clone(),
            })
        } else {
            Ok(addrs)
        }
    }
}

/// Which pods [K8sSeeds] lists and which port each one's address is paired with, deserializable
/// with the `serde` feature: a config file names the selector, the port as a single key and
/// optionally the namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedPods")
)]
pub struct Pods {
    /// The namespace to list; defaults to the client's own, which in a pod is the namespace it
    /// runs in. Must not be blank.
    pub namespace: Option<String>,

    /// The label selector the listed pods must match, e.g. `app=tellus`. Must not be blank: an
    /// empty selector matches every pod in the namespace.
    pub label_selector: String,

    /// The port each listed pod's address is paired with.
    pub port: Port,
}

/// Which port a pod's address is paired with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(rename_all = "snake_case", deny_unknown_fields)
)]
pub enum Port {
    /// The one port every pod's address is paired with: for clusters whose nodes all advertise
    /// one well known port.
    Number(NonZeroU16),

    /// The number of the container port of this name, taken from each pod's own specification, so
    /// nodes can advertise differing ports. Must not be blank.
    Name(String),
}

/// The [Pods] given to [K8sSeeds] are invalid.
#[derive(Debug, Error)]
pub enum InvalidPods {
    /// The configured `label_selector` is blank, which would match every pod in the namespace.
    #[error("label_selector is blank")]
    BlankLabelSelector,

    /// The configured `namespace` is blank.
    #[error("namespace is blank")]
    BlankNamespace,

    /// The configured port name is blank.
    #[error("port name is blank")]
    BlankPortName,
}

/// The seeds cannot be discovered. [bootstrap](tellus::cluster::bootstrap) logs and retries the
/// failures; [InvalidPods] cannot occur there, since both constructors refuse it.
#[derive(Debug, Error)]
pub enum K8sSeedsError {
    /// The given [Pods] are invalid.
    #[error(transparent)]
    Config(#[from] InvalidPods),

    /// The Kubernetes API could not be reached or refused the request, e.g. for want of the
    /// permission to list pods.
    #[error(transparent)]
    Client(#[from] kube::Error),

    /// No pod contributed an address: none matched the selector, or every one is terminating or
    /// finished, has no address yet, has an unparsable one, or lacks a usable container port of the
    /// configured name.
    #[error("no pod address for selector {selector}")]
    NoAddresses {
        /// The selector which was listed.
        selector: String,
    },
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct UncheckedPods {
    #[serde(default)]
    namespace: Option<String>,

    label_selector: String,

    port: Port,
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedPods> for Pods {
    type Error = InvalidPods;

    fn try_from(unchecked: UncheckedPods) -> Result<Self, Self::Error> {
        let pods = Self {
            namespace: unchecked.namespace,
            label_selector: unchecked.label_selector,
            port: unchecked.port,
        };
        validate(&pods)?;

        Ok(pods)
    }
}

fn validate(pods: &Pods) -> Result<(), InvalidPods> {
    if pods.label_selector.trim().is_empty() {
        return Err(InvalidPods::BlankLabelSelector);
    }
    if pods
        .namespace
        .as_ref()
        .is_some_and(|namespace| namespace.trim().is_empty())
    {
        return Err(InvalidPods::BlankNamespace);
    }
    if matches!(&pods.port, Port::Name(name) if name.trim().is_empty()) {
        return Err(InvalidPods::BlankPortName);
    }

    Ok(())
}

fn live(pod: &Pod) -> bool {
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());

    pod.metadata.deletion_timestamp.is_none() && !matches!(phase, Some("Succeeded" | "Failed"))
}

fn addr(pod: &Pod, port: &Port) -> Option<SocketAddr> {
    let ip = ip(pod)?;

    let port = match port {
        Port::Number(number) => *number,
        Port::Name(name) => container_port(pod, name)?,
    };

    Some(SocketAddr::new(ip, port.get()))
}

fn ip(pod: &Pod) -> Option<IpAddr> {
    let status = pod.status.as_ref()?;
    let ip = status.pod_ip.as_deref().or_else(|| {
        status
            .pod_ips
            .as_ref()
            .and_then(|ips| ips.first())
            .map(|ip| ip.ip.as_str())
    })?;

    match ip.parse() {
        Ok(ip) => Some(ip),

        Err(error) => {
            warn!(pod = name(pod), ip, %error, "cannot parse the pod IP");
            None
        }
    }
}

fn container_port(pod: &Pod, port_name: &str) -> Option<NonZeroU16> {
    let port = pod
        .spec
        .as_ref()
        .into_iter()
        .flat_map(|spec| spec.containers.iter())
        .flat_map(|container| container.ports.iter().flatten())
        .find(|port| port.name.as_deref() == Some(port_name));

    match port {
        Some(port) => {
            let number = u16::try_from(port.container_port)
                .ok()
                .and_then(NonZeroU16::new);
            if number.is_none() {
                warn!(
                    pod = name(pod),
                    port_name,
                    container_port = port.container_port,
                    "container port out of range"
                );
            }

            number
        }

        None => {
            warn!(pod = name(pod), port_name, "no container port of this name");
            None
        }
    }
}

fn name(pod: &Pod) -> &str {
    pod.metadata.name.as_deref().unwrap_or("<unnamed>")
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use crate::{Pods, Port};
    use std::num::NonZeroU16;

    /// The documented config form, which a config file provides: either port is one key. A zero
    /// port is unrepresentable rather than resolving to unusable addresses, and a selector
    /// matching every pod in the namespace is refused rather than seeding from strangers.
    #[test]
    fn pods_deserialize_from_their_documented_form() {
        let pods = serde_json::from_str::<Pods>(
            r#"{ "label_selector": "app=tellus", "port": { "number": 7878 } }"#,
        )
        .expect("the numbered port form deserializes");
        assert_eq!(
            pods,
            Pods {
                namespace: None,
                label_selector: "app=tellus".to_string(),
                port: Port::Number(NonZeroU16::new(7_878).expect("7878 is not zero")),
            }
        );

        let pods = serde_json::from_str::<Pods>(
            r#"{ "namespace": "tellus", "label_selector": "app=tellus", "port": { "name": "tellus" } }"#,
        )
        .expect("the named port form deserializes");
        assert_eq!(
            pods,
            Pods {
                namespace: Some("tellus".to_string()),
                label_selector: "app=tellus".to_string(),
                port: Port::Name("tellus".to_string()),
            }
        );

        assert!(
            serde_json::from_str::<Pods>(
                r#"{ "label_selector": "app=tellus", "port": { "number": 0 } }"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Pods>(
                r#"{ "label_selector": " ", "port": { "number": 7878 } }"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Pods>(
                r#"{ "namespace": "", "label_selector": "app=tellus", "port": { "number": 7878 } }"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Pods>(
                r#"{ "label_selector": "app=tellus", "port": { "name": "" } }"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<Pods>(
                r#"{ "lable_selector": "app=tellus", "port": { "number": 7878 } }"#
            )
            .is_err()
        );
    }
}
