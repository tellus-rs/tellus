//! How-to example: secure a cluster with mutual TLS. The first node creates a certificate
//! authority and a certificate for each node, bound to its address by a `tellus://` URI SAN. It
//! builds its own transport in code and starts the second node with a config file naming the PEM
//! files, the way a deployed node reads them.
//!
//! The first node prints to stdout; tellus logs to stderr, configured via `RUST_LOG`, e.g.
//! `RUST_LOG=tellus=debug cargo run --quiet -p tellus --features cluster,serde --example
//! how_to_secure_with_mutual_tls`.

use anyhow::Context;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, Issuer, KeyPair, SanType};
use rustls::{
    RootCertStore,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};
use serde::Deserialize;
use std::{
    env, fs, future, io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    path::{Path, PathBuf},
    time::Duration,
};
use tellus::cluster::{
    self, BootstrapConfig, EndpointConfig, FixedSeeds,
    transport::{QuicConfig, QuicTransport},
};
use tokio::{
    process::{self, Child},
    time::timeout,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const SECOND_NODE_ARG: &str = "second-node";
const SERVER_NAME: &str = "tellus";
const NODES: usize = 2;
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match env::args().nth(1).as_deref() {
        Some(SECOND_NODE_ARG) => second_node().await,
        _ => first_node().await,
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(io::stderr),
        )
        .init();
}

// ANCHOR: first-node
async fn first_node() -> anyhow::Result<()> {
    let seeds = reserved_addrs(NODES)?;
    let ca = CertificateAuthority::new()?;
    let dir = tempfile::tempdir().context("creating a directory for the PEM files")?;
    let config = write_node_config(dir.path(), &ca, seeds[1], &seeds)?;
    let _second_node = spawn_second_node(&config)?;

    let (cert, key) = ca.issue(seeds[0])?;
    let mut roots = RootCertStore::empty();
    roots
        .add(ca.cert.der().clone())
        .context("adding the certificate authority")?;
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.serialize_der()));
    let transport =
        QuicTransport::mutual_tls(seeds[0], vec![cert.der().clone()], key, roots, SERVER_NAME)
            .context("mutual TLS transport")?;
    join_cluster(seeds[0], seeds, transport).await?;

    let mut state = cluster::cluster_state().context("subscribing to the cluster state")?;
    let state = timeout(
        BOOTSTRAP_TIMEOUT,
        state.wait_for(|state| state.up().count() == NODES),
    )
    .await
    .context("the second node did not come up within the timeout")?;
    println!("Members up over mutual TLS: {}", state.up().count());
    Ok(())
}
// ANCHOR_END: first-node

// ANCHOR: second-node
async fn second_node() -> anyhow::Result<()> {
    let path = env::args().nth(2).context("config path argument")?;
    let config = config::Config::builder()
        .add_source(config::File::with_name(&path))
        .build()
        .context("loading the configuration")?
        .try_deserialize::<NodeConfig>()
        .context("deserializing the configuration")?;

    let transport =
        QuicTransport::from_config(&config.transport).context("mutual TLS transport")?;
    join_cluster(config.transport.bind_addr, config.seeds, transport).await?;

    future::pending().await
}

#[derive(Deserialize)]
struct NodeConfig {
    seeds: Vec<SocketAddr>,
    transport: QuicConfig,
}
// ANCHOR_END: second-node

// ANCHOR: certificate-authority
struct CertificateAuthority {
    cert: Certificate,
    issuer: Issuer<'static, KeyPair>,
}

impl CertificateAuthority {
    fn new() -> anyhow::Result<Self> {
        let key = KeyPair::generate().context("generating the CA key")?;
        let mut params = CertificateParams::new(Vec::new()).context("CA parameters")?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let cert = params
            .self_signed(&key)
            .context("signing the CA certificate")?;
        let issuer = Issuer::new(params, key);
        Ok(Self { cert, issuer })
    }

    fn issue(&self, addr: SocketAddr) -> anyhow::Result<(Certificate, KeyPair)> {
        let key = KeyPair::generate().context("generating a node key")?;
        let mut params =
            CertificateParams::new(vec![SERVER_NAME.to_string()]).context("node parameters")?;
        let uri = format!("tellus://{addr}");
        params.subject_alt_names.push(SanType::URI(
            uri.try_into().context("the URI SAN is not ASCII")?,
        ));
        let cert = params
            .signed_by(&key, &self.issuer)
            .context("signing a node certificate")?;
        Ok((cert, key))
    }
}
// ANCHOR_END: certificate-authority

// ANCHOR: write-node-config
fn write_node_config(
    dir: &Path,
    ca: &CertificateAuthority,
    addr: SocketAddr,
    seeds: &[SocketAddr],
) -> anyhow::Result<PathBuf> {
    let (cert, key) = ca.issue(addr)?;
    let roots = dir.join("roots.pem");
    let cert_chain = dir.join("node-cert.pem");
    let key_path = dir.join("node-key.pem");
    fs::write(&roots, ca.cert.pem()).context("writing the roots")?;
    fs::write(&cert_chain, cert.pem()).context("writing the certificate")?;
    fs::write(&key_path, key.serialize_pem()).context("writing the key")?;

    let seeds = seeds
        .iter()
        .map(|seed| format!("\"{seed}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let config = format!(
        r#"seeds = [{seeds}]

[transport]
bind_addr   = "{addr}"
cert_chain  = "{}"
key         = "{}"
roots       = "{}"
server_name = "{SERVER_NAME}"
"#,
        cert_chain.display(),
        key_path.display(),
        roots.display()
    );
    let path = dir.join("node.toml");
    fs::write(&path, config).context("writing the configuration")?;
    Ok(path)
}
// ANCHOR_END: write-node-config

async fn join_cluster(
    addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    transport: QuicTransport,
) -> anyhow::Result<()> {
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")?;
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(FixedSeeds::new(seeds), BootstrapConfig::new()),
    )
    .await
    .context("no cluster within the timeout")?
    .context("bootstrapping the cluster")
}

fn reserved_addrs(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let sockets = (0..count)
        .map(|_| UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))))
        .collect::<Result<Vec<_>, _>>()
        .context("reserving loopback ports")?;
    sockets
        .iter()
        .map(|socket| socket.local_addr().context("reserved local address"))
        .collect()
}

fn spawn_second_node(config: &Path) -> anyhow::Result<Child> {
    process::Command::new(env::current_exe()?)
        .arg(SECOND_NODE_ARG)
        .arg(config)
        .kill_on_drop(true)
        .spawn()
        .context("spawning the second node process")
}
