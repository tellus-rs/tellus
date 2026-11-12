//! How-to example: serve a node's view of its cluster over HTTP. The node forms a cluster of one
//! and mounts the inspect router at `/inspect` on port 8080.
//!
//! Run it with `cargo run --quiet -p tellus-cluster-inspect --example how_to_serve_cluster_state`,
//! then `curl localhost:8080/inspect/cluster` for a snapshot or `curl -N
//! localhost:8080/inspect/events` for the stream of changes, and press Ctrl-C to stop.

use anyhow::Context;
use axum::Router;
use std::{
    future,
    net::{Ipv4Addr, SocketAddr},
};
use tellus::cluster::{self, EndpointConfig, transport::QuicTransport};
use tellus_cluster_inspect::InspectConfig;
use tokio::{net::TcpListener, signal};

const HTTP_PORT: u16 = 8080;

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = QuicTransport::dev(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .context("dev QUIC transport")?;
    let addr = transport.local_addr().context("local transport address")?;
    cluster::start_endpoint(EndpointConfig::new(addr), transport).context("remoting endpoint")?;
    cluster::form().context("forming the cluster")?;

    let inspect = tellus_cluster_inspect::router(InspectConfig::default(), future::pending())
        .context("inspect router")?;
    let app = Router::new().nest("/inspect", inspect);
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, HTTP_PORT)))
        .await
        .context("binding the HTTP port")?;
    tokio::select! {
        served = async { axum::serve(listener, app).await } => served.context("serving HTTP"),
        interrupt = signal::ctrl_c() => interrupt.context("Ctrl-C handler"),
    }
}
// ANCHOR_END: main
