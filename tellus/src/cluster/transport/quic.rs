//! The provided [Transport]: QUIC via quinn, with one stream per queue of a lane.

use crate::cluster::transport::{
    ConnectedControl, Connection, FrameReceiver, FrameSender, PeerIdentity, Transport,
    TransportError,
};
use quinn::{
    ClientConfig, ConnectionError, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
    VarInt,
    crypto::rustls::{NoInitialCipherSuite, QuicClientConfig, QuicServerConfig},
};
#[cfg(feature = "cluster-dev")]
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::UnixTime,
};
use rustls::{
    RootCertStore,
    crypto::{CryptoProvider, ring},
    pki_types::{
        CertificateDer, InvalidDnsNameError, PrivateKeyDer, ServerName,
        pem::{self, PemObject},
    },
    server::WebPkiClientVerifier,
    version::TLS13,
};
use std::{
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{select, sync::Mutex, task::JoinSet};
use tracing::{debug, warn};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

#[cfg(feature = "cluster-dev")]
const DEV_SERVER_NAME: &str = "tellus";

/// Must stay below quinn's default 30 s idle timeout, else idle connections die between pings.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Beyond this size the copy into one assembled write outweighs the saved write call.
const ASSEMBLED_WRITE_LIMIT: usize = 4 * 1_024;

/// A QUIC [Transport] backed by quinn. TLS is mandatory for QUIC; use [QuicTransport::mutual_tls]
/// for production, [QuicTransport::new] for custom TLS configurations and `QuicTransport::dev`,
/// added by the `cluster-dev` feature, for development and tests.
#[derive(Debug)]
pub struct QuicTransport {
    endpoint: Endpoint,
    server_name: String,
    handshakes: Mutex<JoinSet<Result<quinn::Connection, ConnectionError>>>,
}

impl QuicTransport {
    /// The number of data streams per connection, set as the concurrent unidirectional stream
    /// limit on the transport config and reported by [Transport::data_streams], so the endpoint
    /// never opens more than a peer configured the same way admits.
    pub const DATA_STREAMS: NonZeroUsize = NonZeroUsize::new(100).expect("100 is not zero");

    /// A transport bound to the given address with the given TLS configurations, validating the
    /// certificates of the nodes it connects to against `server_name`, which hence must be the
    /// name those certificates are issued for. A server config without client certificate
    /// verification accepts every dialer; see [QuicTransport::mutual_tls] and docs/cluster.md
    /// on what an unauthenticated dialer can do.
    ///
    /// The transport config is used for both directions. A keep-alive below QUIC's default idle
    /// timeout is set on it, so a lane between tellus nodes is not silently closed between
    /// messages, and so is a concurrent unidirectional stream limit of
    /// [QuicTransport::DATA_STREAMS].
    ///
    /// # Errors
    /// Fails if `server_name` is neither a valid DNS name nor an IP address, if the UDP socket
    /// cannot be bound to `bind_addr`, or if no Tokio runtime is running.
    pub fn new(
        bind_addr: SocketAddr,
        mut server_config: ServerConfig,
        mut client_config: ClientConfig,
        server_name: impl Into<String>,
        mut transport_config: TransportConfig,
    ) -> Result<Self, QuicTransportError> {
        let server_name = server_name.into();
        ServerName::try_from(server_name.as_str()).map_err(|source| {
            QuicTransportError::ServerName {
                name: server_name.clone(),
                source,
            }
        })?;

        // A keep-alive from either side resets the idle timers of both sides!
        transport_config.keep_alive_interval(Some(KEEP_ALIVE_INTERVAL));
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(
            u32::try_from(Self::DATA_STREAMS.get()).expect("DATA_STREAMS fits in u32"),
        ));

        let transport_config = Arc::new(transport_config);
        server_config.transport = transport_config.clone();
        client_config.transport_config(transport_config);

        let mut endpoint = Endpoint::server(server_config, bind_addr).map_err(|source| {
            QuicTransportError::Bind {
                addr: bind_addr,
                source,
            }
        })?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint,
            server_name,
            handshakes: Mutex::new(JoinSet::new()),
        })
    }

    /// A transport for mutual TLS from a [QuicConfig], the production configuration read from a
    /// config file. The certificate chain, the private key and the roots are read as PEM from the
    /// paths it names, then given to [QuicTransport::mutual_tls], which also chooses the crypto
    /// provider. A relative path resolves against the process working directory.
    ///
    /// This is where a [QuicConfig] is validated, its server name included: the config itself is
    /// plain data, since a path can only be checked by reading it.
    ///
    /// The files are read once: renewing the certificates takes a restart.
    ///
    /// # Errors
    /// Fails if a PEM file cannot be read, holds no item or holds something other than what it
    /// must, if a root certificate cannot be added, or if [QuicTransport::mutual_tls] fails on
    /// what was read.
    pub fn from_config(config: &QuicConfig) -> Result<Self, QuicTransportError> {
        let cert_chain = certs(&config.cert_chain)?;
        let key = PrivateKeyDer::from_pem_file(&config.key).map_err(|source| {
            QuicTransportError::Pem {
                path: config.key.clone(),
                source,
            }
        })?;

        let mut roots = RootCertStore::empty();
        for root in certs(&config.roots)? {
            roots.add(root)?;
        }

        Self::mutual_tls(
            config.bind_addr,
            cert_chain,
            key,
            roots,
            config.server_name.clone(),
        )
    }

    /// A transport for mutual TLS, the production configuration: the node presents `cert_chain`
    /// and `key` as both its server and its client identity, and only accepts peers whose
    /// certificates verify against `roots`, which hence must hold the cluster's certificate
    /// authority. A dialer without such a certificate cannot complete a connection, so it reaches
    /// neither the protocol nor the failure detection of any node.
    ///
    /// The certificates are fixed for the transport's lifetime: renewing them takes a restart.
    ///
    /// The TLS configuration uses rustls's process default [CryptoProvider] if the application has
    /// installed one, otherwise ring's default provider; it never installs a process default
    /// itself. QUIC needs TLS 1.3 with the `TLS13_AES_128_GCM_SHA256` cipher suite. An installed
    /// provider lacking either makes this fail; it does not fall back to ring.
    ///
    /// # Errors
    /// Fails if the client certificate verifier cannot be built from `roots`, or if the crypto
    /// provider offers no TLS 1.3 cipher suite. Fails as well if `cert_chain` and `key` do not make
    /// a valid TLS configuration, if that configuration cannot back QUIC, or if
    /// [QuicTransport::new] fails.
    pub fn mutual_tls(
        bind_addr: SocketAddr,
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        roots: RootCertStore,
        server_name: impl Into<String>,
    ) -> Result<Self, QuicTransportError> {
        let provider = crypto_provider();
        let roots = Arc::new(roots);
        let verifier =
            WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone()).build()?;
        let tls_config = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&TLS13])?
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain.clone(), key.clone_key())?;
        let tls_config = QuicServerConfig::try_from(tls_config)?;
        let server_config = ServerConfig::with_crypto(Arc::new(tls_config));

        let tls_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&TLS13])?
            .with_root_certificates(roots)
            .with_client_auth_cert(cert_chain, key)?;
        let tls_config = QuicClientConfig::try_from(tls_config)?;
        let client_config = ClientConfig::new(Arc::new(tls_config));

        Self::new(
            bind_addr,
            server_config,
            client_config,
            server_name,
            TransportConfig::default(),
        )
    }

    /// A transport for development and tests only: a self signed certificate on the server side
    /// and no certificate verification on the client side. Never use this on untrusted networks!
    ///
    /// The certificate carries the bind address's IP, so the dialer's identity check accepts a
    /// node advertising the address it is bound to.
    ///
    /// Only available with the `cluster-dev` feature, so it cannot reach a production build
    /// which does not ask for it.
    ///
    /// The client side chooses its crypto provider like [QuicTransport::mutual_tls], while the
    /// server side always uses ring's default provider.
    ///
    /// # Errors
    /// Fails if the self signed certificate cannot be generated, if it does not make a valid TLS
    /// configuration, if that configuration cannot back QUIC, or if [QuicTransport::new] fails.
    #[cfg(feature = "cluster-dev")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-dev")))]
    pub fn dev(bind_addr: SocketAddr) -> Result<Self, QuicTransportError> {
        let certified_key = rcgen::generate_simple_self_signed(vec![
            DEV_SERVER_NAME.to_string(),
            bind_addr.ip().to_string(),
        ])?;
        let cert = certified_key.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(certified_key.signing_key.serialize_der().into());
        let server_config = ServerConfig::with_single_cert(vec![cert], key)?;

        let provider = crypto_provider();
        let tls_config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(provider)))
            .with_no_client_auth();
        let tls_config = QuicClientConfig::try_from(tls_config)?;
        let client_config = ClientConfig::new(Arc::new(tls_config));

        Self::new(
            bind_addr,
            server_config,
            client_config,
            DEV_SERVER_NAME,
            TransportConfig::default(),
        )
    }

    /// The actually bound local address, e.g. for advertising a port chosen by the OS.
    ///
    /// # Errors
    /// Fails if the bound socket cannot report its address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }
}

impl Transport for QuicTransport {
    type Connection = QuicConnection;

    fn data_streams(&self) -> Option<NonZeroUsize> {
        Some(Self::DATA_STREAMS)
    }

    async fn connect(
        &self,
        addr: SocketAddr,
        max_frame_size: usize,
    ) -> Result<ConnectedControl<QuicConnection>, TransportError> {
        let connecting = self
            .endpoint
            .connect(addr, &self.server_name)
            .map_err(TransportError::other)?;
        let connection = connecting.await.map_err(TransportError::other)?;
        let (stream_sender, stream_receiver) =
            connection.open_bi().await.map_err(TransportError::other)?;

        let connection = QuicConnection {
            connection,
            max_frame_size,
        };
        let control_tx = connection.sender(stream_sender);
        let control_rx = connection.receiver(stream_receiver);
        Ok(ConnectedControl {
            connection,
            control_tx,
            control_rx,
        })
    }

    async fn accept(&self, max_frame_size: usize) -> Result<QuicConnection, TransportError> {
        let mut handshakes = self.handshakes.lock().await;

        loop {
            select! {
                incoming = self.endpoint.accept() => {
                    let incoming =
                        incoming.ok_or_else(|| TransportError::other("QUIC endpoint closed"))?;
                    handshakes.spawn(incoming.into_future());
                }

                Some(handshake) = handshakes.join_next() => match handshake {
                    Ok(Ok(connection)) => {
                        return Ok(QuicConnection {
                            connection,
                            max_frame_size,
                        });
                    }

                    Ok(Err(error)) => debug!(%error, "cannot establish inbound connection"),

                    Err(error) => warn!(%error, "inbound handshake task failed"),
                },
            }
        }
    }
}

/// Configuration for [QuicTransport::from_config], deserializable with the `serde` feature: the
/// mutual TLS configuration named by the paths of its PEM files, which are read and validated by
/// [QuicTransport::from_config] rather than here.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct QuicConfig {
    /// The address to bind the UDP socket to; `0` as its port lets the OS choose one, which
    /// [QuicTransport::local_addr] then reports.
    pub bind_addr: SocketAddr,

    /// The path of the PEM file holding this node's certificate chain, presented as both its
    /// server and its client identity.
    pub cert_chain: PathBuf,

    /// The path of the PEM file holding the private key of the certificate chain.
    pub key: PathBuf,

    /// The path of the PEM file holding the certificate authority every peer's certificate must
    /// verify against, which hence must be the cluster's own.
    pub roots: PathBuf,

    /// The name the peers' certificates are issued for, a DNS name or an IP address; every node
    /// of the cluster validates its peers against it.
    pub server_name: String,
}

/// A [QuicTransport] which cannot be constructed. The variants separate what an operator can act
/// on: a taken port is a deployment problem, an invalid TLS configuration a configuration one.
#[derive(Debug, Error)]
pub enum QuicTransportError {
    /// The client certificate verifier cannot be built from the given roots.
    #[error("cannot build the client certificate verifier")]
    ClientVerifier(#[from] rustls::server::VerifierBuilderError),

    /// The TLS configuration is invalid, e.g. the key does not match the certificate.
    #[error("invalid TLS configuration")]
    Tls(#[from] rustls::Error),

    /// The TLS configuration cannot back QUIC, which requires TLS 1.3.
    #[error("TLS configuration not usable for QUIC")]
    Quic(#[from] NoInitialCipherSuite),

    /// The UDP socket cannot be bound.
    #[error("cannot bind to {addr}")]
    Bind {
        /// The address the transport was to be bound to.
        addr: SocketAddr,

        /// The bind failure.
        source: io::Error,
    },

    /// The server name is neither a DNS name nor an IP address, so no certificate could be
    /// issued for it.
    #[error("invalid server name {name}")]
    ServerName {
        /// The name which was given.
        name: String,

        /// Why it is not a valid name.
        source: InvalidDnsNameError,
    },

    /// A PEM file cannot be read, holds no item, or holds something other than what it must.
    #[error("cannot read PEM file {}", path.display())]
    Pem {
        /// The path of the file.
        path: PathBuf,

        /// The read failure.
        source: pem::Error,
    },

    /// The self signed development certificate cannot be generated.
    #[cfg(feature = "cluster-dev")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-dev")))]
    #[error("cannot generate the dev certificate")]
    DevCertificate(#[from] rcgen::Error),
}

/// A connection produced by [QuicTransport]: one bidirectional QUIC stream as the control stream
/// plus unidirectional QUIC streams as data streams, all carrying length delimited frames.
#[derive(Debug)]
pub struct QuicConnection {
    connection: quinn::Connection,
    max_frame_size: usize,
}

impl QuicConnection {
    fn sender(&self, stream: SendStream) -> QuicFrameSender {
        QuicFrameSender {
            stream,
            buffer: Vec::new(),
            _connection: self.connection.clone(),
        }
    }

    fn receiver(&self, stream: RecvStream) -> QuicFrameReceiver {
        QuicFrameReceiver {
            stream,
            max_frame_size: self.max_frame_size,
            buffer: Vec::new(),
            _connection: self.connection.clone(),
        }
    }
}

impl Connection for QuicConnection {
    type Sender = QuicFrameSender;
    type Receiver = QuicFrameReceiver;

    async fn accept_control(&self) -> Result<(QuicFrameSender, QuicFrameReceiver), TransportError> {
        let (stream_sender, stream_receiver) = self
            .connection
            .accept_bi()
            .await
            .map_err(TransportError::other)?;

        Ok((self.sender(stream_sender), self.receiver(stream_receiver)))
    }

    async fn open_data(&self) -> Result<QuicFrameSender, TransportError> {
        let stream = self
            .connection
            .open_uni()
            .await
            .map_err(TransportError::other)?;
        Ok(self.sender(stream))
    }

    async fn accept_data(&self) -> Result<Option<QuicFrameReceiver>, TransportError> {
        match self.connection.accept_uni().await {
            Ok(stream) => Ok(Some(self.receiver(stream))),

            Err(ConnectionError::ApplicationClosed(_) | ConnectionError::LocallyClosed) => Ok(None),

            Err(error) => Err(TransportError::other(error)),
        }
    }

    fn peer_identity(&self) -> Option<PeerIdentity> {
        let identity = self.connection.peer_identity()?;

        match identity.downcast::<Vec<CertificateDer<'static>>>() {
            Ok(certificates) => Some(parse_identity(&certificates)),

            Err(_) => {
                warn!("peer identity is not a certificate chain");
                Some(PeerIdentity {
                    dns_names: Vec::new(),
                    ip_addresses: Vec::new(),
                    uris: Vec::new(),
                })
            }
        }
    }
}

/// The sending half of a [QuicConnection].
#[derive(Debug)]
pub struct QuicFrameSender {
    stream: SendStream,
    buffer: Vec<u8>,

    /// Keeps the connection alive: quinn closes a connection once its last handle is dropped.
    _connection: quinn::Connection,
}

impl FrameSender for QuicFrameSender {
    async fn send(&mut self, frame: &[u8]) -> Result<(), TransportError> {
        let len = u32::try_from(frame.len()).map_err(TransportError::other)?;

        if frame.len() <= ASSEMBLED_WRITE_LIMIT {
            self.buffer.clear();
            self.buffer.extend_from_slice(&len.to_be_bytes());
            self.buffer.extend_from_slice(frame);
            self.stream
                .write_all(&self.buffer)
                .await
                .map_err(TransportError::other)
        } else {
            self.stream
                .write_all(&len.to_be_bytes())
                .await
                .map_err(TransportError::other)?;
            self.stream
                .write_all(frame)
                .await
                .map_err(TransportError::other)
        }
    }

    async fn send_batch(&mut self, frames: &[Vec<u8>]) -> Result<(), TransportError> {
        if let [frame] = frames {
            return self.send(frame).await;
        }

        self.buffer.clear();
        for frame in frames {
            let len = u32::try_from(frame.len()).map_err(TransportError::other)?;
            self.buffer.extend_from_slice(&len.to_be_bytes());
            self.buffer.extend_from_slice(frame);
        }
        self.stream
            .write_all(&self.buffer)
            .await
            .map_err(TransportError::other)
    }

    async fn finish(&mut self) -> Result<(), TransportError> {
        self.stream.finish().map_err(TransportError::other)?;
        match self.stream.stopped().await {
            Ok(None) => Ok(()),

            Ok(Some(code)) => Err(TransportError::other(format!(
                "peer stopped the stream with code {code}"
            ))),

            Err(error) => Err(TransportError::other(error)),
        }
    }
}

/// The receiving half of a [QuicConnection].
#[derive(Debug)]
pub struct QuicFrameReceiver {
    stream: RecvStream,
    max_frame_size: usize,
    buffer: Vec<u8>,

    /// Keeps the connection alive: quinn closes a connection once its last handle is dropped, so
    /// both halves must hold one to stay usable independently.
    _connection: quinn::Connection,
}

impl FrameReceiver for QuicFrameReceiver {
    async fn recv(&mut self) -> Result<Option<&[u8]>, TransportError> {
        let mut len = [0; 4];
        match self.stream.read_exact(&mut len).await {
            Ok(()) => {}
            Err(quinn::ReadExactError::FinishedEarly(0)) => return Ok(None),
            Err(error) => return Err(TransportError::other(error)),
        }

        let len = usize::try_from(u32::from_be_bytes(len)).map_err(TransportError::other)?;
        if len > self.max_frame_size {
            return Err(TransportError::FrameTooLarge {
                len,
                max: self.max_frame_size,
            });
        }

        self.buffer.resize(len, 0);
        self.stream
            .read_exact(&mut self.buffer)
            .await
            .map_err(TransportError::other)?;
        Ok(Some(&self.buffer))
    }
}

#[cfg(feature = "cluster-dev")]
#[derive(Debug)]
struct AcceptAnyServerCert(Arc<CryptoProvider>);

#[cfg(feature = "cluster-dev")]
impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, QuicTransportError> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|source| QuicTransportError::Pem {
            path: path.to_path_buf(),
            source,
        })?;

    if certs.is_empty() {
        return Err(QuicTransportError::Pem {
            path: path.to_path_buf(),
            source: pem::Error::NoItemsFound,
        });
    }

    Ok(certs)
}

fn crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(ring::default_provider()))
}

/// An unparsable certificate yields an empty identity, never a missing one, so it is refused.
fn parse_identity(certificates: &[CertificateDer<'_>]) -> PeerIdentity {
    let mut identity = PeerIdentity {
        dns_names: Vec::new(),
        ip_addresses: Vec::new(),
        uris: Vec::new(),
    };

    let Some(end_entity) = certificates.first() else {
        return identity;
    };
    let Ok((_, certificate)) = X509Certificate::from_der(end_entity) else {
        warn!("cannot parse the peer's certificate");
        return identity;
    };
    let Ok(Some(san)) = certificate.subject_alternative_name() else {
        return identity;
    };

    for name in &san.value.general_names {
        match name {
            GeneralName::DNSName(name) => identity.dns_names.push((*name).to_string()),

            GeneralName::IPAddress(bytes) => {
                let addr = match bytes.len() {
                    4 => <[u8; 4]>::try_from(*bytes).ok().map(IpAddr::from),
                    16 => <[u8; 16]>::try_from(*bytes).ok().map(IpAddr::from),
                    _ => None,
                };
                identity.ip_addresses.extend(addr);
            }

            GeneralName::URI(uri) => identity.uris.push((*uri).to_string()),

            _ => {}
        }
    }
    identity
}

#[cfg(all(test, feature = "cluster-dev"))]
mod tests {
    use crate::cluster::transport::{
        Connection, FrameReceiver, FrameSender, Transport,
        quic::{
            ASSEMBLED_WRITE_LIMIT, QuicConfig, QuicTransport, QuicTransportError, certs,
            parse_identity,
        },
    };
    use rustls::{
        RootCertStore,
        crypto::CryptoProvider,
        pki_types::{PrivateKeyDer, pem::PemObject},
    };
    use std::{
        fs,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        path::Path,
        time::Duration,
    };
    use tokio::{net::UdpSocket, time::timeout};

    const MAX_FRAME_SIZE: usize = 64;
    const TIMEOUT: Duration = Duration::from_secs(10);

    /// A connected pair of framing halves plus the dialer's own receiver, which the caller must
    /// keep alive: it holds the dialer's connection, and dropping the last handle would close the
    /// connection instead of finishing the stream.
    ///
    /// The halves only pair up once the dialer has written, since a QUIC stream is invisible to
    /// the acceptor until then.
    async fn connected(
        max_frame_size: usize,
        first: &[u8],
    ) -> anyhow::Result<(impl FrameSender, impl FrameReceiver, impl FrameReceiver)> {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let server = QuicTransport::dev(loopback)?;
        let addr = server.local_addr()?;
        let client = QuicTransport::dev(loopback)?;

        let accepting = tokio::spawn(async move {
            let connection = server.accept(max_frame_size).await.expect("accepts");
            connection.accept_control().await.expect("accepts control")
        });

        let connected = client
            .connect(addr, max_frame_size)
            .await
            .expect("connects");
        let mut sender = connected.control_tx;
        let keep_alive = connected.control_rx;
        sender.send(first).await.expect("sends the first frame");

        let (_sender, receiver) = accepting.await.expect("accept task");
        Ok((sender, keep_alive, receiver))
    }

    /// Frames survive the length delimited framing intact and in order.
    #[tokio::test]
    async fn frames_round_trip_in_order() {
        let (mut sender, _keep_alive, mut receiver) =
            timeout(TIMEOUT, connected(MAX_FRAME_SIZE, b"first"))
                .await
                .expect("connects in time")
                .expect("connects");

        sender.send(b"second").await.expect("sends");

        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            Some(b"first".as_slice())
        );
        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            Some(b"second".as_slice())
        );
    }

    /// A frame past the assembled write limit takes the split write path, which must produce the
    /// same framing: a desynchronization here would corrupt every later frame on the stream.
    #[tokio::test]
    async fn a_large_frame_round_trips() {
        let frame = vec![0xAB; ASSEMBLED_WRITE_LIMIT + 1];
        let (_sender, _keep_alive, mut receiver) =
            timeout(TIMEOUT, connected(ASSEMBLED_WRITE_LIMIT * 2, &frame))
                .await
                .expect("connects in time")
                .expect("connects");

        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            Some(frame.as_slice())
        );
    }

    /// A peer closing the stream ends the frames rather than failing: the receiver reports the
    /// end of stream once the sender is gone.
    #[tokio::test]
    async fn a_closed_stream_ends_the_frames() {
        let (sender, _keep_alive, mut receiver) =
            timeout(TIMEOUT, connected(MAX_FRAME_SIZE, b"only"))
                .await
                .expect("connects in time")
                .expect("connects");

        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            Some(b"only".as_slice())
        );

        drop(sender);

        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            None
        );
    }

    /// A data stream carries frames one way and in order, next to the control stream and without
    /// disturbing it: this is what per-target streams are built on.
    #[tokio::test]
    async fn a_data_stream_carries_frames_one_way() {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let server = QuicTransport::dev(loopback).expect("server transport");
        let addr = server.local_addr().expect("local address");
        let client = QuicTransport::dev(loopback).expect("client transport");

        let accepting = tokio::spawn(async move {
            let connection = server.accept(MAX_FRAME_SIZE).await.expect("accepts");
            let (_sender, mut control) = connection
                .accept_control()
                .await
                .expect("accepts the control stream");
            let mut data = connection
                .accept_data()
                .await
                .expect("accepts a data stream")
                .expect("the connection is alive");

            let control = control
                .recv()
                .await
                .expect("receives on the control stream")
                .map(<[u8]>::to_vec);
            let first = data.recv().await.expect("receives").map(<[u8]>::to_vec);
            let second = data.recv().await.expect("receives").map(<[u8]>::to_vec);
            (control, first, second)
        });

        let connected = client
            .connect(addr, MAX_FRAME_SIZE)
            .await
            .expect("connects");
        let mut control = connected.control_tx;
        let _keep_alive = connected.control_rx;
        control.send(b"control").await.expect("sends");

        let mut data = connected
            .connection
            .open_data()
            .await
            .expect("opens a data stream");
        data.send(b"first").await.expect("sends");
        data.send(b"second").await.expect("sends");

        let received = timeout(TIMEOUT, accepting)
            .await
            .expect("in time")
            .expect("accept task");
        assert_eq!(
            received,
            (
                Some(b"control".to_vec()),
                Some(b"first".to_vec()),
                Some(b"second".to_vec())
            )
        );
    }

    /// A frame beyond the connection's maximum is refused instead of allocating for it, which is
    /// what keeps a peer from naming an arbitrary length.
    #[tokio::test]
    async fn an_oversize_frame_is_refused() {
        let (mut sender, _keep_alive, mut receiver) =
            timeout(TIMEOUT, connected(MAX_FRAME_SIZE, b"small"))
                .await
                .expect("connects in time")
                .expect("connects");

        assert_eq!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .expect("receives"),
            Some(b"small".as_slice())
        );

        sender.send(&[0; MAX_FRAME_SIZE + 1]).await.expect("sends");

        assert!(
            timeout(TIMEOUT, receiver.recv())
                .await
                .expect("in time")
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_stalled_handshake_does_not_block_the_next_dialer() {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let server = QuicTransport::dev(loopback).expect("server binds");
        let server_addr = server.local_addr().expect("server address");

        // Relays the stalled dialer's first datagram to the server and drops everything else.
        let relay = UdpSocket::bind(loopback).await.expect("relay binds");
        let relay_addr = relay.local_addr().expect("relay address");
        let relaying = tokio::spawn(async move {
            let mut datagram = vec![0; 65_536];
            let (len, _) = relay
                .recv_from(&mut datagram)
                .await
                .expect("relay receives");
            relay
                .send_to(&datagram[..len], server_addr)
                .await
                .expect("relay forwards");
            loop {
                if relay.recv_from(&mut datagram).await.is_err() {
                    break;
                }
            }
        });

        let stalled = QuicTransport::dev(loopback).expect("stalled dialer binds");
        let stalling = tokio::spawn(async move {
            let _ = stalled.connect(relay_addr, MAX_FRAME_SIZE).await;
        });

        let accepting = tokio::spawn(async move {
            let connection = server.accept(MAX_FRAME_SIZE).await.expect("accepts");
            (connection.connection.remote_address(), server)
        });
        // The stalled handshake reaches the server first.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let dialer = QuicTransport::dev(loopback).expect("dialer binds");
        let dialer_addr = dialer.local_addr().expect("dialer address");
        let _connected = timeout(TIMEOUT, dialer.connect(server_addr, MAX_FRAME_SIZE))
            .await
            .expect("connects in time")
            .expect("connects");

        let (accepted_from, _server) = timeout(TIMEOUT, accepting)
            .await
            .expect("accepted in time")
            .expect("accept task");
        assert_eq!(accepted_from, dialer_addr);

        stalling.abort();
        relaying.abort();
    }

    #[tokio::test]
    async fn the_reported_data_streams_are_what_a_peer_admits() {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let server = QuicTransport::dev(loopback).expect("server binds");
        let server_addr = server.local_addr().expect("server address");
        let client = QuicTransport::dev(loopback).expect("client binds");

        let accepting = tokio::spawn(async move {
            let connection = server.accept(MAX_FRAME_SIZE).await.expect("accepts");
            (connection, server)
        });
        let connected = timeout(TIMEOUT, client.connect(server_addr, MAX_FRAME_SIZE))
            .await
            .expect("connects in time")
            .expect("connects");

        let streams = client.data_streams().expect("QUIC has data streams").get();
        assert_eq!(streams, QuicTransport::DATA_STREAMS.get());

        let mut senders = Vec::with_capacity(streams);
        for _ in 0..streams {
            senders.push(
                timeout(TIMEOUT, connected.connection.open_data())
                    .await
                    .expect("opens in time")
                    .expect("opens"),
            );
        }
        assert!(
            timeout(Duration::from_millis(200), connected.connection.open_data())
                .await
                .is_err(),
            "one stream more than reported waits for the peer"
        );

        drop(accepting);
    }

    /// A mutual TLS pair with certificates from one authority round trips, a dialer without a
    /// certificate is refused during the handshake, and the server keeps accepting afterward.
    #[tokio::test]
    async fn mutual_tls_refuses_strangers_and_round_trips() {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));

        let ca_key = rcgen::KeyPair::generate().expect("ca key");
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).expect("ca parameters");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca certificate");
        let ca = rcgen::Issuer::from_params(&ca_params, &ca_key);

        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_cert.der().clone()).expect("ca root");

        let node = |roots: rustls::RootCertStore| {
            let key = rcgen::KeyPair::generate().expect("node key");
            let params =
                rcgen::CertificateParams::new(vec!["tellus".to_string()]).expect("node parameters");
            let cert = params.signed_by(&key, &ca).expect("node certificate");

            QuicTransport::mutual_tls(
                loopback,
                vec![cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
                roots,
                "tellus",
            )
            .expect("mutual TLS transport")
        };
        let server = node(roots.clone());
        let addr = server.local_addr().expect("local address");
        let client = node(roots);

        let accepting = tokio::spawn(async move {
            let connection = server.accept(MAX_FRAME_SIZE).await.expect("accepts");
            let (_sender, mut receiver) = connection.accept_control().await.expect("accepts");
            receiver.recv().await.expect("receives").map(<[u8]>::to_vec)
        });

        let stranger = QuicTransport::dev(loopback).expect("stranger transport");
        let refused = timeout(TIMEOUT, async {
            // TLS 1.3 refuses the missing client certificate only after the dialer's own
            // handshake has completed, so the refusal may also show as the connection dying!
            let Ok(connected) = stranger.connect(addr, MAX_FRAME_SIZE).await else {
                return true;
            };
            let mut receiver = connected.control_rx;
            receiver.recv().await.is_err()
        })
        .await
        .expect("in time");
        assert!(refused, "a certificate-less dialer was not refused");

        let connected = timeout(TIMEOUT, client.connect(addr, MAX_FRAME_SIZE))
            .await
            .expect("in time")
            .expect("connects");
        let mut sender = connected.control_tx;
        let _keep_alive = connected.control_rx;
        sender.send(b"hello").await.expect("sends");

        let received = timeout(TIMEOUT, accepting)
            .await
            .expect("in time")
            .expect("accept task");
        assert_eq!(received, Some(b"hello".to_vec()));
    }

    /// A certificate authority and one PEM triple per node in the given directory, all nodes
    /// verifying against the same roots file.
    fn mtls_configs(dir: &Path, count: usize) -> Vec<QuicConfig> {
        let ca_key = rcgen::KeyPair::generate().expect("ca key");
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).expect("ca parameters");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca certificate");
        let ca = rcgen::Issuer::from_params(&ca_params, &ca_key);

        let roots = dir.join("roots.pem");
        fs::write(&roots, ca_cert.pem()).expect("writes the roots");

        (0..count)
            .map(|n| {
                let key = rcgen::KeyPair::generate().expect("node key");
                let params = rcgen::CertificateParams::new(vec!["tellus".to_string()])
                    .expect("node parameters");
                let cert = params.signed_by(&key, &ca).expect("node certificate");

                let cert_chain = dir.join(format!("node-{n}-cert.pem"));
                let key_path = dir.join(format!("node-{n}-key.pem"));
                fs::write(&cert_chain, cert.pem()).expect("writes the certificate");
                fs::write(&key_path, key.serialize_pem()).expect("writes the key");

                QuicConfig {
                    bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                    cert_chain,
                    key: key_path,
                    roots: roots.clone(),
                    server_name: "tellus".to_string(),
                }
            })
            .collect()
    }

    /// The production configuration read from a config file: the PEM files the config names build
    /// a mutual TLS transport on both sides, which then connect and carry a frame.
    #[tokio::test]
    async fn a_config_builds_a_mutual_tls_transport() {
        let dir = tempfile::tempdir().expect("temp dir");
        let configs = mtls_configs(dir.path(), 2);

        let server = QuicTransport::from_config(&configs[0]).expect("server transport");
        let addr = server.local_addr().expect("local address");
        let client = QuicTransport::from_config(&configs[1]).expect("client transport");

        let accepting = tokio::spawn(async move {
            let connection = server.accept(MAX_FRAME_SIZE).await.expect("accepts");
            let (_sender, mut receiver) = connection.accept_control().await.expect("accepts");
            receiver.recv().await.expect("receives").map(<[u8]>::to_vec)
        });

        let connected = timeout(TIMEOUT, client.connect(addr, MAX_FRAME_SIZE))
            .await
            .expect("in time")
            .expect("connects");
        let mut sender = connected.control_tx;
        let _keep_alive = connected.control_rx;
        sender.send(b"hello").await.expect("sends");

        let received = timeout(TIMEOUT, accepting)
            .await
            .expect("in time")
            .expect("accept task");
        assert_eq!(received, Some(b"hello".to_vec()));
    }

    /// Without a process default crypto provider, mutual TLS and dev transports build and leave
    /// the process default unset, which is the application's to choose.
    #[tokio::test]
    async fn transports_build_without_installing_a_crypto_provider() {
        assert!(
            CryptoProvider::get_default().is_none(),
            "a process default crypto provider is installed"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let config = mtls_configs(dir.path(), 1).remove(0);
        QuicTransport::from_config(&config).expect("mutual TLS transport");
        QuicTransport::dev(config.bind_addr).expect("dev transport");

        assert!(
            CryptoProvider::get_default().is_none(),
            "building a transport installed a process default crypto provider"
        );
    }

    /// An unreadable PEM file names the file which failed, and a server name no certificate could
    /// be issued for is refused by [QuicTransport::new], which every constructor goes through,
    /// rather than at the first connection attempt.
    #[test]
    fn an_invalid_config_names_what_failed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let config = mtls_configs(dir.path(), 1).remove(0);

        let missing = dir.path().join("missing.pem");
        let mut broken = config.clone();
        broken.key = missing.clone();
        assert!(matches!(
            QuicTransport::from_config(&broken),
            Err(QuicTransportError::Pem { path, .. }) if path == missing
        ));

        let empty = dir.path().join("empty.pem");
        fs::write(&empty, "").expect("writes the empty file");
        let mut broken = config.clone();
        broken.cert_chain = empty.clone();
        assert!(matches!(
            QuicTransport::from_config(&broken),
            Err(QuicTransportError::Pem { path, .. }) if path == empty
        ));

        let mut broken = config.clone();
        broken.server_name = "not a name".to_string();
        assert!(matches!(
            QuicTransport::from_config(&broken),
            Err(QuicTransportError::ServerName { name, .. }) if name == "not a name"
        ));

        let cert_chain = certs(&config.cert_chain).expect("reads the certificate");
        let key = PrivateKeyDer::from_pem_file(&config.key).expect("reads the key");
        let mut roots = RootCertStore::empty();
        for root in certs(&config.roots).expect("reads the roots") {
            roots.add(root).expect("adds the root");
        }
        assert!(matches!(
            QuicTransport::mutual_tls(config.bind_addr, cert_chain, key, roots, "not a name"),
            Err(QuicTransportError::ServerName { name, .. }) if name == "not a name"
        ));
    }

    /// The documented config form, which a config file provides.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<QuicConfig>(
            r#"{
                "bind_addr": "0.0.0.0:2552",
                "cert_chain": "/etc/tellus/tls/cert.pem",
                "key": "/etc/tellus/tls/key.pem",
                "roots": "/etc/tellus/tls/ca.pem",
                "server_name": "tellus"
            }"#,
        )
        .expect("the documented config form deserializes");

        assert_eq!(config.bind_addr, SocketAddr::from(([0, 0, 0, 0], 2552)));
        assert_eq!(config.cert_chain, Path::new("/etc/tellus/tls/cert.pem"));
        assert_eq!(config.key, Path::new("/etc/tellus/tls/key.pem"));
        assert_eq!(config.roots, Path::new("/etc/tellus/tls/ca.pem"));
        assert_eq!(config.server_name, "tellus");

        assert!(
            serde_json::from_str::<QuicConfig>(
                r#"{ "bind_addr": "0.0.0.0:2552", "cert_chian": "cert.pem" }"#
            )
            .is_err()
        );
    }

    /// The identity check reads the certificate's subject alternative names: DNS names and both
    /// IP address families must come out as what they are.
    #[test]
    fn an_identity_parses_dns_and_ip_sans() {
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec![
            "tellus".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ])
        .expect("certificate parameters");
        let cert = params.self_signed(&key).expect("certificate");

        let identity = parse_identity(&[cert.der().clone()]);

        assert_eq!(identity.dns_names, vec!["tellus".to_string()]);
        assert_eq!(
            identity.ip_addresses,
            vec![
                IpAddr::from(Ipv4Addr::LOCALHOST),
                IpAddr::from(Ipv6Addr::LOCALHOST)
            ]
        );
    }

    /// URI SANs come out verbatim, whatever their scheme, so the identity check can pick the
    /// `tellus://` ones.
    #[test]
    fn an_identity_parses_uri_sans() {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params =
            rcgen::CertificateParams::new(vec!["tellus".to_string()]).expect("parameters");
        for uri in ["tellus://[::1]:1234", "spiffe://example.org/node"] {
            params
                .subject_alt_names
                .push(rcgen::SanType::URI(uri.try_into().expect("ia5 string")));
        }
        let cert = params.self_signed(&key).expect("certificate");

        let identity = parse_identity(&[cert.der().clone()]);

        assert_eq!(
            identity.uris,
            vec![
                "tellus://[::1]:1234".to_string(),
                "spiffe://example.org/node".to_string()
            ]
        );
        assert!(identity.ip_addresses.is_empty());
    }

    /// A certificate without IP addresses proves no address, so a node presenting it is refused
    /// by the identity check rather than admitted unchecked.
    #[test]
    fn an_identity_without_ip_sans_is_empty_of_addresses() {
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec!["tellus".to_string()]).expect("parameters");
        let cert = params.self_signed(&key).expect("certificate");

        let identity = parse_identity(&[cert.der().clone()]);

        assert_eq!(identity.dns_names, vec!["tellus".to_string()]);
        assert!(identity.ip_addresses.is_empty());
    }

    /// An empty or unparsable chain yields an empty identity, never a missing one: proving
    /// nothing must not admit a peer unchecked.
    #[test]
    fn a_broken_chain_yields_an_empty_identity() {
        let identity = parse_identity(&[]);
        assert!(identity.dns_names.is_empty());
        assert!(identity.ip_addresses.is_empty());

        let garbage = rustls::pki_types::CertificateDer::from(vec![0xff; 16]);
        let identity = parse_identity(&[garbage]);
        assert!(identity.dns_names.is_empty());
        assert!(identity.ip_addresses.is_empty());
    }
}
