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
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    str::FromStr,
    sync::Arc,
};
use tellus::cluster::SeedDiscovery;
use tellus_bootstrap_dns::DnsSeeds;
use tokio::net::UdpSocket;

const NODE_A_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const NODE_B_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
const NODE_A_PORT: u16 = 4_001;
const NODE_B_PORT: u16 = 4_002;
const STALE_PORT: u16 = 4_003;
const SHARED_PORT: NonZeroU16 = NonZeroU16::new(4_242).expect("4242 is not zero");

/// Each SRV record contributes its own port, paired with the addresses its target resolves to; a
/// target which does not resolve is skipped while the others resolve.
#[tokio::test]
async fn test_srv() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds =
        DnsSeeds::srv("_tellus._udp.tellus.test.")?.with_resolver(resolver_at(dns_addr)?);

    let mut resolved = seeds.resolve().await.context("resolving SRV seeds")?;
    resolved.sort();
    let expected = vec![
        SocketAddr::new(IpAddr::V4(NODE_A_IP), NODE_A_PORT),
        SocketAddr::new(IpAddr::V4(NODE_B_IP), NODE_B_PORT),
    ];
    assert_eq!(resolved, expected);
    Ok(())
}

/// If every SRV target is stale, resolution still fails so bootstrap retries instead of settling
/// on an empty universe.
#[tokio::test]
async fn test_all_srv_targets_stale() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds =
        DnsSeeds::srv("_stale._udp.tellus.test.")?.with_resolver(resolver_at(dns_addr)?);

    assert!(seeds.resolve().await.is_err());
    Ok(())
}

/// A/AAAA records pair every resolved address with the one configured port.
#[tokio::test]
async fn test_ip() -> anyhow::Result<()> {
    let dns_addr = spawn_dns().await?;
    let mut seeds =
        DnsSeeds::ip("nodes.tellus.test.", SHARED_PORT)?.with_resolver(resolver_at(dns_addr)?);

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
        DnsSeeds::ip("missing.tellus.test.", SHARED_PORT)?.with_resolver(resolver_at(dns_addr)?);

    assert!(seeds.resolve().await.is_err());
    Ok(())
}

/// An in-process DNS server on an ephemeral UDP port, authoritative for `tellus.test.`: three SRV
/// records under `_tellus._udp`, two with per-node ports and resolvable targets and one with a
/// target without records, one SRV record under `_stale._udp` with only that target, A records
/// for the resolvable targets, and a two address A record set for the fixed port mode.
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
