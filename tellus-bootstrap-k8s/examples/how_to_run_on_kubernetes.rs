//! How-to example: run a cluster node on Kubernetes. The node takes its advertised address from
//! the downward API, its certificate from a mounted volume, finds its peers by label selector,
//! reports itself ready once it is a member and leaves the cluster when Kubernetes terminates the
//! pod.
//!
//! It is meant to run in a pod with the manifests of the how-to guide, not on a developer machine;
//! `cargo build -p tellus-bootstrap-k8s --example how_to_run_on_kubernetes` builds it.

use anyhow::Context;
use axum::{Router, extract::State, http::StatusCode, routing::get};
use std::{
    convert::Infallible,
    env,
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU16, NonZeroUsize},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorSystem, Control, Incoming, Untimed,
    cluster::{
        self, BootstrapConfig, EndpointConfig, MemberState,
        transport::{QuicConfig, QuicTransport},
    },
};
use tellus_bootstrap_k8s::{K8sSeeds, Pods, Port};
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    time::{sleep, timeout},
};

const PORT: NonZeroU16 = NonZeroU16::new(7878).expect("7878 is not zero");
const HTTP_PORT: u16 = 8080;
const TLS_DIR: &str = "/etc/tellus/tls";
const SERVER_NAME: &str = "tellus";
// Must match the manifest's `replicas`.
const REPLICAS: NonZeroUsize = NonZeroUsize::new(3).expect("3 is not zero");
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(60);
// Must cover the propagation of the endpoint removal to kube-proxy, ingresses and load balancers.
const UNREADY_LINGER: Duration = Duration::from_secs(5);
const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: transport
    let addr = env::var("ADVERTISED_ADDR")
        .context("ADVERTISED_ADDR is not set")?
        .parse::<SocketAddr>()
        .context("ADVERTISED_ADDR is no socket address")?;
    let tls = Path::new(TLS_DIR);
    let transport = QuicTransport::from_config(&QuicConfig {
        bind_addr: SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT.get())),
        cert_chain: tls.join("tls.crt"),
        key: tls.join("tls.key"),
        roots: tls.join("ca.crt"),
        server_name: SERVER_NAME.to_string(),
    })
    .context("mutual TLS transport")?;
    let config = EndpointConfig::new(addr);
    let leave_timeout = config.leave_timeout;
    cluster::start_endpoint(config, transport).context("remoting endpoint")?;
    // ANCHOR_END: transport

    // ANCHOR: ready
    let ready = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, HTTP_PORT)))
        .await
        .context("binding the HTTP port")?;
    let router = Router::new()
        .route("/ready", get(readiness))
        .with_state(ready.clone());
    let mut server = tokio::spawn(async { axum::serve(listener, router).await });
    // ANCHOR_END: ready

    // ANCHOR: seeds
    let seeds = K8sSeeds::new(Pods {
        namespace: None,
        label_selector: "app=tellus".to_string(),
        port: Port::Number(PORT),
    })
    .await
    .context("Kubernetes seed discovery")?;
    let bootstrap_config = BootstrapConfig {
        min_peers: REPLICAS,
        ..BootstrapConfig::new()
    };
    timeout(
        BOOTSTRAP_TIMEOUT,
        cluster::bootstrap(seeds, bootstrap_config),
    )
    .await
    .context("no cluster within the timeout")?
    .context("bootstrapping the cluster")?;
    // ANCHOR_END: seeds

    // ANCHOR: lifecycle
    let system = ActorSystem::new(Idle);
    ready.store(true, Ordering::Relaxed);

    let mut terminate = signal(SignalKind::terminate()).context("SIGTERM handler")?;
    tokio::select! {
        _ = terminate.recv() => (),
        downed = await_downed() => {
            ready.store(false, Ordering::Relaxed);
            downed?;
            anyhow::bail!("downed by the cluster, exiting so a restart rejoins");
        }
        served = &mut server => {
            served.context("the HTTP server panicked")?.context("serving HTTP")?;
            anyhow::bail!("the HTTP server stopped");
        }
    }
    ready.store(false, Ordering::Relaxed);
    sleep(UNREADY_LINGER).await;
    system.stop();
    timeout(
        TERMINATION_TIMEOUT + leave_timeout,
        cluster::leave_on_terminated(system),
    )
    .await
    .context("the actors did not stop and leave within the timeout")?
    .context("leaving the cluster")?;
    // ANCHOR_END: lifecycle
    Ok(())
}
// ANCHOR_END: main

// ANCHOR: readiness
async fn readiness(State(ready): State<Arc<AtomicBool>>) -> StatusCode {
    if ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
// ANCHOR_END: readiness

// ANCHOR: await-downed
async fn await_downed() -> anyhow::Result<()> {
    let mut state = cluster::cluster_state().context("subscribing to the cluster state")?;
    state
        .wait_for(|state| {
            state
                .this_member()
                .is_none_or(|member| member.state() == MemberState::Down)
        })
        .await
        .context("the cluster state stopped")?;
    Ok(())
}
// ANCHOR_END: await-downed

struct Idle;

impl Actor for Idle {
    type Message = ();
    type Timers = Untimed;
    type State = ();
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(())
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        _: Incoming<Self::Message>,
        state: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        Ok(Control::Continue(state))
    }
}
