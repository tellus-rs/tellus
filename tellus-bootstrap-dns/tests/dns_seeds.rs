use anyhow::Context;
use hickory_resolver::{
    Resolver, TokioResolver,
    config::{ConnectionConfig, NameServerConfig, ResolverConfig},
    net::runtime::TokioRuntimeProvider,
};
use hickory_server::{
    Server,
    proto::rr::{Name, RData, Record, rdata},
    store::in_memory::InMemoryZoneHandler,
    zone_handler::{AxfrPolicy, Catalog, ZoneHandler, ZoneType},
};
use std::{
    cell::RefCell,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    str::FromStr,
    sync::{Arc, Once},
};
use tellus::cluster::SeedDiscovery;
use tellus_bootstrap_dns::{DnsSeeds, DnsSeedsError, InvalidQuery, Query};
use tokio::net::UdpSocket;
use tracing::Level;

const NODE_A_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const NODE_B_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
const NODE_A_PORT: u16 = 4_001;
const NODE_B_PORT: u16 = 4_002;
const STALE_PORT: u16 = 4_003;
const SHARED_PORT: NonZeroU16 = NonZeroU16::new(4_242).expect("4242 is not zero");

thread_local! {
    static LOG: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Each SRV record contributes its own port, paired with the addresses its target resolves to; a
/// target which does not resolve is skipped while the others resolve, and logged at warn level
/// with its name, since the resolved view is partial.
#[tokio::test]
async fn test_srv() -> anyhow::Result<()> {
    capture_logs();
    let dns_addr = spawn_dns().await?;
    let mut seeds = DnsSeeds::with_resolver(
        Query::Srv("_tellus._udp.tellus.test.".to_string()),
        resolver_at(dns_addr)?,
    )?;

    let mut resolved = seeds.resolve().await.context("resolving SRV seeds")?;
    resolved.sort();
    let expected = vec![
        SocketAddr::new(IpAddr::V4(NODE_A_IP), NODE_A_PORT),
        SocketAddr::new(IpAddr::V4(NODE_B_IP), NODE_B_PORT),
    ];
    assert_eq!(resolved, expected);

    let log = logged();
    assert!(log.contains("WARN"), "{log}");
    assert!(log.contains("cannot resolve SRV target"), "{log}");
    assert!(log.contains("srv_target=stale.tellus.test."), "{log}");
    assert!(!log.contains("node-a.tellus.test."), "{log}");
    Ok(())
}

/// If every SRV target is stale, resolution still fails so bootstrap retries instead of settling
/// on an empty universe.
#[tokio::test]
async fn test_all_srv_targets_stale() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds = DnsSeeds::with_resolver(
        Query::Srv("_stale._udp.tellus.test.".to_string()),
        resolver_at(dns_addr)?,
    )?;

    assert!(seeds.resolve().await.is_err());
    Ok(())
}

/// A/AAAA records pair every resolved address with the one configured port.
#[tokio::test]
async fn test_ip() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds =
        DnsSeeds::with_resolver(ip_query("nodes.tellus.test."), resolver_at(dns_addr)?)?;

    let mut resolved = seeds.resolve().await.context("resolving A seeds")?;
    resolved.sort();
    let expected = vec![
        SocketAddr::new(IpAddr::V4(NODE_A_IP), SHARED_PORT.get()),
        SocketAddr::new(IpAddr::V4(NODE_B_IP), SHARED_PORT.get()),
    ];
    assert_eq!(resolved, expected);
    Ok(())
}

/// A name without records is a resolve failure, not an empty seed list: bootstrap must keep
/// retrying it instead of settling on nothing.
#[tokio::test]
async fn test_missing_name() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds =
        DnsSeeds::with_resolver(ip_query("missing.tellus.test."), resolver_at(dns_addr)?)?;

    assert!(seeds.resolve().await.is_err());
    Ok(())
}

/// A blank name fails on every resolve, so it is refused when the discovery is built rather than
/// retried by bootstrap forever, and before the system resolver configuration is read.
#[tokio::test]
async fn test_blank_query() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;

    for query in [Query::Srv("".to_string()), ip_query(" \t")] {
        let error = DnsSeeds::with_resolver(query.clone(), resolver_at(dns_addr)?)
            .expect_err("a blank name is refused");
        assert!(matches!(
            error,
            DnsSeedsError::Config(InvalidQuery::BlankName)
        ));

        let error = DnsSeeds::new(query).expect_err("a blank name is refused");
        assert!(matches!(
            error,
            DnsSeedsError::Config(InvalidQuery::BlankName)
        ));
    }

    let error = DnsSeeds::srv(" ").expect_err("a blank name is refused");
    assert!(matches!(
        error,
        DnsSeedsError::Config(InvalidQuery::BlankName)
    ));
    let error = DnsSeeds::ip("", SHARED_PORT).expect_err("a blank name is refused");
    assert!(matches!(
        error,
        DnsSeedsError::Config(InvalidQuery::BlankName)
    ));
    Ok(())
}

/// A name which is no valid DNS name, such as one with a label over 63 bytes or an empty label,
/// is refused by every constructor rather than failing on every resolve. An IP query still accepts
/// an IP address literal, which the resolver answers without a lookup.
#[tokio::test]
async fn test_malformed_query() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let long_label = format!("{}.tellus.test.", "a".repeat(64));

    for name in [long_label.as_str(), "node..tellus.test."] {
        for query in [Query::Srv(name.to_string()), ip_query(name)] {
            let error = DnsSeeds::with_resolver(query.clone(), resolver_at(dns_addr)?)
                .expect_err("a malformed name is refused");
            assert!(
                matches!(
                    &error,
                    DnsSeedsError::Config(InvalidQuery::MalformedName { name: refused, .. })
                        if refused == name
                ),
                "{name} was not refused as malformed: {error:?}"
            );
        }
        assert!(matches!(
            DnsSeeds::srv(name),
            Err(DnsSeedsError::Config(InvalidQuery::MalformedName { .. }))
        ));
        assert!(matches!(
            DnsSeeds::ip(name, SHARED_PORT),
            Err(DnsSeedsError::Config(InvalidQuery::MalformedName { .. }))
        ));
    }

    for literal in ["127.0.0.1", "::1"] {
        DnsSeeds::with_resolver(ip_query(literal), resolver_at(dns_addr)?)
            .expect("an IP address literal is a valid IP query");
    }
    Ok(())
}

/// Installs, once per test binary, a subscriber writing every warning into a buffer of the thread
/// logging it.
fn capture_logs() {
    static INSTALLED: Once = Once::new();

    INSTALLED.call_once(|| {
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_ansi(false)
            .with_writer(|| ThreadLog)
            .init();
    });
}

/// What this thread has logged since [capture_logs].
fn logged() -> String {
    LOG.with_borrow(|bytes| String::from_utf8_lossy(bytes).into_owned())
}

struct ThreadLog;

impl io::Write for ThreadLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        LOG.with_borrow_mut(|bytes| bytes.extend_from_slice(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// An in-process DNS server on an ephemeral UDP port, authoritative for `tellus.test.`. It serves
/// three SRV records under `_tellus._udp`, two with per-node ports and resolvable targets and one
/// with a target without records. It also serves one SRV record under `_stale._udp` with only that
/// target, A records for the resolvable targets, and a two address A record set for the fixed port
/// mode.
async fn spawn_dns() -> anyhow::Result<SocketAddr> {
    let origin = Name::from_str("tellus.test.")?;
    let authority = InMemoryZoneHandler::<TokioRuntimeProvider>::empty(
        origin.clone(),
        ZoneType::Primary,
        AxfrPolicy::Deny,
    );

    let srv_name = Name::from_str("_tellus._udp.tellus.test.")?;
    let node_a = Name::from_str("node-a.tellus.test.")?;
    let node_b = Name::from_str("node-b.tellus.test.")?;
    let stale = Name::from_str("stale.tellus.test.")?;
    let all_stale = Name::from_str("_stale._udp.tellus.test.")?;
    let nodes = Name::from_str("nodes.tellus.test.")?;
    let records = [
        Record::from_rdata(
            srv_name.clone(),
            60,
            RData::SRV(rdata::SRV::new(0, 0, NODE_A_PORT, node_a.clone())),
        ),
        Record::from_rdata(
            srv_name,
            60,
            RData::SRV(rdata::SRV::new(0, 0, NODE_B_PORT, node_b.clone())),
        ),
        Record::from_rdata(
            Name::from_str("_tellus._udp.tellus.test.")?,
            60,
            RData::SRV(rdata::SRV::new(0, 0, STALE_PORT, stale.clone())),
        ),
        Record::from_rdata(
            all_stale,
            60,
            RData::SRV(rdata::SRV::new(0, 0, STALE_PORT, stale)),
        ),
        Record::from_rdata(node_a, 60, RData::A(rdata::A(NODE_A_IP))),
        Record::from_rdata(node_b, 60, RData::A(rdata::A(NODE_B_IP))),
        Record::from_rdata(nodes.clone(), 60, RData::A(rdata::A(NODE_A_IP))),
        Record::from_rdata(nodes, 60, RData::A(rdata::A(NODE_B_IP))),
    ];
    for record in records {
        authority.upsert(record, 0).await;
    }

    let mut catalog = Catalog::new();
    catalog.upsert(
        origin.into(),
        vec![Arc::new(authority) as Arc<dyn ZoneHandler>],
    );

    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let addr = socket.local_addr()?;
    let mut server = Server::new(catalog);
    server.register_socket(socket);
    tokio::spawn(async move {
        let _ = server.block_until_done().await;
    });
    Ok(addr)
}

fn ip_query(name: &str) -> Query {
    Query::Ip {
        name: name.to_string(),
        port: SHARED_PORT,
    }
}

/// A resolver asking only the given name server, so the tests never touch real DNS.
fn resolver_at(addr: SocketAddr) -> anyhow::Result<TokioResolver> {
    let mut connection = ConnectionConfig::udp();
    connection.port = addr.port();
    let mut name_server = NameServerConfig::udp(addr.ip());
    name_server.connections = vec![connection];
    let mut config = ResolverConfig::default();
    config.add_name_server(name_server);

    let resolver =
        Resolver::builder_with_config(config, TokioRuntimeProvider::default()).build()?;
    Ok(resolver)
}
