//! DNS seed discovery for tellus cluster bootstrap: [DnsSeeds] implements
//! [SeedDiscovery], resolving seed addresses from SRV records,
//! which carry address and port (e.g. a Kubernetes headless service), or from A/AAAA records
//! paired with a configured port. Pass it to [bootstrap](tellus::cluster::bootstrap), which
//! resolves it before every join round, `resolve_interval` apart, and joins through the resolved
//! addresses on every resolution, retrying resolve failures, e.g. records still appearing while an
//! orchestrator starts the other nodes. Only forming a new cluster waits for the view to settle.
//! SRV targets resolve concurrently; a failing target is logged at warn level and skipped while
//! any other resolves, and a set resolving no address at all remains an error so bootstrap does
//! not settle on it. Nothing here distinguishes a stale target from a transient failure, because
//! the resolver does not either, so both are skipped alike. A partial view does settle, so a
//! target failing throughout the settle window shrinks the universe a
//! [FormationProvider](tellus::cluster::formation::FormationProvider) counts its majority against.
//!
//! Whatever is queried must resolve to one address per node, since bootstrap counts addresses and
//! a peer is only admitted at the address it advertises; on Kubernetes that makes the headless
//! service single stack, see this crate's README.
//!
//! The default resolver keeps no response cache. Bootstrap polls DNS to see nodes come and go, and
//! a cached answer would hide a node registered after it for the record's TTL, e.g. a sibling
//! restarting a moment later than this one. A resolver given via [DnsSeeds::with_resolver] is
//! used as configured.

#![warn(missing_docs, clippy::missing_errors_doc)]

use futures_util::future::join_all;
use hickory_resolver::{
    TokioResolver,
    net::NetError,
    proto::{
        ProtoError,
        rr::{Name, RData},
    },
};
use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU16,
};
use tellus::cluster::SeedDiscovery;
use thiserror::Error;
use tracing::warn;

/// Seed discovery via DNS, the [SeedDiscovery] for clusters whose nodes are named by DNS
/// records instead of configured addresses.
#[derive(Debug)]
pub struct DnsSeeds {
    resolver: TokioResolver,
    query: Query,
}

impl DnsSeeds {
    /// Discovery via the given [Query], which a config file can provide. Resolves with the
    /// system resolver configuration; see [with_resolver](DnsSeeds::with_resolver).
    ///
    /// # Errors
    /// Fails if the given [Query] carries a blank name, which is decided before the system
    /// resolver configuration is read, or if that configuration cannot be read.
    pub fn new(query: Query) -> Result<Self, DnsSeedsError> {
        validate(&query)?;

        Self::with_resolver(query, system_resolver()?)
    }

    /// Discovery via the SRV records at the given name, e.g.
    /// `_tellus._udp.svc.cluster.example`: every record contributes its port paired with each
    /// address its target resolves to, so nodes can advertise differing ports. Resolves with
    /// the system resolver configuration; see [with_resolver](DnsSeeds::with_resolver).
    ///
    /// # Errors
    /// Fails if the given name is blank, or if the system resolver configuration cannot be read.
    pub fn srv(name: impl Into<String>) -> Result<Self, DnsSeedsError> {
        Self::new(Query::Srv(name.into()))
    }

    /// Discovery via the A/AAAA records at the given name, every address paired with the given
    /// port: for clusters whose nodes all advertise one well known port. Resolves with the
    /// system resolver configuration; see [with_resolver](DnsSeeds::with_resolver).
    ///
    /// # Errors
    /// Fails if the given name is blank, or if the system resolver configuration cannot be read.
    pub fn ip(name: impl Into<String>, port: NonZeroU16) -> Result<Self, DnsSeedsError> {
        Self::new(Query::Ip {
            name: name.into(),
            port,
        })
    }

    /// Discovery via the given [Query] through the given resolver, e.g. one pointed at specific
    /// name servers, without reading the system resolver configuration. The resolver
    /// [DnsSeeds::new] builds keeps no response cache. A resolver which does returns the same
    /// addresses until its cached answer expires, so bootstrap sees nodes come and go only that
    /// late.
    ///
    /// # Errors
    /// Fails if the given [Query] carries a blank name.
    pub fn with_resolver(query: Query, resolver: TokioResolver) -> Result<Self, DnsSeedsError> {
        validate(&query)?;

        Ok(Self { resolver, query })
    }
}

impl SeedDiscovery for DnsSeeds {
    type Error = DnsSeedsError;

    async fn resolve(&mut self) -> Result<Vec<SocketAddr>, DnsSeedsError> {
        match &self.query {
            Query::Srv(name) => {
                let records = self.resolver.srv_lookup(name.as_str()).await?;
                let lookups = records
                    .answers()
                    .iter()
                    .filter_map(|record| match &record.data {
                        RData::SRV(srv) => Some(srv),
                        _ => None,
                    })
                    .map(|srv| {
                        let resolver = self.resolver.clone();
                        let target = srv.target.clone();
                        let port = srv.port;
                        async move {
                            let ips = resolver.lookup_ip(target.clone()).await;
                            (target, port, ips)
                        }
                    });
                let mut addrs = Vec::new();
                let mut first_error = None;
                for (target, port, ips) in join_all(lookups).await {
                    match ips {
                        Ok(ips) => {
                            addrs.extend(ips.iter().map(|ip| SocketAddr::new(ip, port)));
                        }

                        Err(error) => {
                            warn!(srv_target = %target, port, %error, "cannot resolve SRV target");
                            first_error.get_or_insert(error);
                        }
                    }
                }

                match first_error {
                    Some(error) if addrs.is_empty() => Err(error.into()),
                    _ => Ok(addrs),
                }
            }

            Query::Ip { name, port } => {
                let ips = self.resolver.lookup_ip(name.as_str()).await?;
                let port = port.get();
                Ok(ips.iter().map(|ip| SocketAddr::new(ip, port)).collect())
            }
        }
    }
}

/// What [DnsSeeds] resolves, deserializable with the `serde` feature: a config file names either
/// form as a single key, `srv: _tellus._udp.example` or `ip: { name: nodes.example, port: 7000 }`.
///
/// That is the normalized map form a loader like [config](https://crates.io/crates/config)
/// produces; `serde_yaml` deserializes the same enum from a YAML tag (`!srv`) instead, as the
/// tellus README's configuration section explains.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(try_from = "UncheckedQuery")
)]
pub enum Query {
    /// The SRV records at the given name, every record's port paired with each address its
    /// target resolves to. The name must not be blank.
    Srv(String),

    /// The A/AAAA records at the given name, every address paired with the given port.
    Ip {
        /// The name to resolve. Must be a valid DNS name, not blank.
        name: String,

        /// The port every resolved address is paired with.
        port: NonZeroU16,
    },
}

/// The [Query] given to [DnsSeeds] is invalid.
#[derive(Debug, Error)]
pub enum InvalidQuery {
    /// The configured name is blank.
    #[error("name is blank")]
    BlankName,

    /// The configured name is no valid DNS name, e.g. it holds a label longer than 63 bytes or an
    /// empty label. An IP query also accepts an IP address literal.
    #[error("{name} is no valid DNS name")]
    MalformedName {
        /// The name as configured.
        name: String,

        /// Why the name is malformed.
        #[source]
        source: ProtoError,
    },
}

/// The seeds cannot be discovered. [bootstrap](tellus::cluster::bootstrap) logs and retries the
/// failures; [InvalidQuery] cannot occur there, since every constructor refuses it.
#[derive(Debug, Error)]
pub enum DnsSeedsError {
    /// The given [Query] is invalid.
    #[error(transparent)]
    Config(#[from] InvalidQuery),

    /// DNS resolution failed, or the system resolver configuration cannot be read.
    #[error(transparent)]
    Resolve(#[from] NetError),
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum UncheckedQuery {
    Srv(String),

    Ip { name: String, port: NonZeroU16 },
}

#[cfg(feature = "serde")]
impl TryFrom<UncheckedQuery> for Query {
    type Error = InvalidQuery;

    fn try_from(unchecked: UncheckedQuery) -> Result<Self, Self::Error> {
        let query = match unchecked {
            UncheckedQuery::Srv(name) => Self::Srv(name),
            UncheckedQuery::Ip { name, port } => Self::Ip { name, port },
        };
        validate(&query)?;

        Ok(query)
    }
}

fn validate(query: &Query) -> Result<(), InvalidQuery> {
    let (name, literal_allowed) = match query {
        Query::Srv(name) => (name, false),
        Query::Ip { name, .. } => (name, true),
    };
    if name.trim().is_empty() {
        return Err(InvalidQuery::BlankName);
    }
    if literal_allowed && name.parse::<IpAddr>().is_ok() {
        return Ok(());
    }
    if let Err(source) = Name::from_utf8(name) {
        return Err(InvalidQuery::MalformedName {
            name: name.clone(),
            source,
        });
    }

    Ok(())
}

fn system_resolver() -> Result<TokioResolver, DnsSeedsError> {
    let mut builder = TokioResolver::builder_tokio()?;
    // No response cache: bootstrap polls to see nodes come and go, which a cached answer hides.
    builder.options_mut().cache_size = 0;
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde")]
    use crate::Query;
    use crate::system_resolver;
    #[cfg(feature = "serde")]
    use std::num::NonZeroU16;

    /// Bootstrap polls DNS to see nodes come and go, which a response cache would hide.
    #[test]
    fn the_system_resolver_keeps_no_response_cache() {
        let resolver = system_resolver().expect("the system resolver is built");
        assert_eq!(resolver.options().cache_size, 0);
    }

    /// The documented config form, which a config file provides: either query is one key. A zero
    /// port is unrepresentable rather than resolving to unusable addresses, and a blank or
    /// malformed name is refused rather than failing on every resolve.
    #[cfg(feature = "serde")]
    #[test]
    fn a_query_deserializes_from_its_documented_form() {
        let query = serde_json::from_str::<Query>(r#"{ "srv": "_tellus._udp.example" }"#)
            .expect("the SRV form deserializes");
        assert_eq!(query, Query::Srv("_tellus._udp.example".to_string()));

        let query =
            serde_json::from_str::<Query>(r#"{ "ip": { "name": "nodes.example", "port": 7000 } }"#)
                .expect("the A/AAAA form deserializes");
        assert_eq!(
            query,
            Query::Ip {
                name: "nodes.example".to_string(),
                port: NonZeroU16::new(7_000).expect("7000 is not zero"),
            }
        );

        assert!(
            serde_json::from_str::<Query>(r#"{ "ip": { "name": "nodes.example", "port": 0 } }"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<Query>(r#"{ "ip": { "nmae": "nodes.example", "port": 7000 } }"#)
                .is_err()
        );
        assert!(serde_json::from_str::<Query>(r#"{ "svr": "_tellus._udp.example" }"#).is_err());
        assert!(serde_json::from_str::<Query>(r#"{ "srv": " " }"#).is_err());
        assert!(
            serde_json::from_str::<Query>(r#"{ "ip": { "name": "", "port": 7000 } }"#).is_err()
        );
        assert!(serde_json::from_str::<Query>(r#"{ "srv": "node..example" }"#).is_err());
    }
}
