//! How-to example: shut down a cluster node gracefully. On SIGTERM or Ctrl-C the node reports
//! itself unhealthy and keeps serving requests while a load balancer takes it out of rotation.
//! Then it drains the requests still in flight, stops its actors and leaves the cluster.
//!
//! Run it with `cargo run --quiet -p tellus --features cluster-dev --example
//! how_to_shut_down_gracefully`, try `curl -i localhost:8080/health` and `curl
//! localhost:8080/count`, and press Ctrl-C.

use anyhow::Context;
use axum::{Router, extract::State, http::StatusCode, routing::get};
use std::{
    convert::Infallible,
    future::IntoFuture,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tellus::{
    Actor, ActorContext, ActorRef, ActorSystem, Control, Incoming, ReplyTo, Unstashed, Untimed,
    cluster::{self, EndpointConfig, transport::QuicTransport},
};
use tokio::{
    net::TcpListener,
    signal::{
        self,
        unix::{SignalKind, signal},
    },
    sync::Notify,
    time::{sleep, timeout},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const HTTP_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
// Must exceed the time a load balancer takes to drop a node that fails its health check.
const UNHEALTHY_LINGER: Duration = Duration::from_secs(5);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);
const ASK_TIMEOUT: Duration = Duration::from_secs(2);

// ANCHOR: main
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let leave_timeout = start_endpoint()?;
    cluster::form().context("forming the cluster")?;

    let system = ActorSystem::new(Counter);
    let app = Arc::new(App {
        healthy: AtomicBool::new(true),
        counter: system.root().clone(),
    });
    let listener = TcpListener::bind(HTTP_ADDR)
        .await
        .context("binding the HTTP port")?;
    let drain = Arc::new(Notify::new());
    let drained = drain.clone();
    let server = tokio::spawn(
        axum::serve(listener, router(app.clone()))
            .with_graceful_shutdown(async move { drained.notified().await })
            .into_future(),
    );
    println!("Serving, press Ctrl-C to shut down");

    shutdown_signal().await?;
    shut_down(&app, &drain, server, system, leave_timeout).await
}
// ANCHOR_END: main

// ANCHOR: shut-down
async fn shut_down(
    app: &App,
    drain: &Notify,
    server: tokio::task::JoinHandle<io::Result<()>>,
    system: ActorSystem<Count>,
    leave_timeout: Duration,
) -> anyhow::Result<()> {
    app.healthy.store(false, Ordering::Relaxed);
    println!("Unhealthy, still serving");
    sleep(UNHEALTHY_LINGER).await;

    drain.notify_one();
    timeout(DRAIN_TIMEOUT, server)
        .await
        .context("the requests in flight did not drain within the timeout")?
        .context("the HTTP server panicked")?
        .context("serving HTTP")?;
    println!("Drained, leaving the cluster");

    system.stop();
    let departure = timeout(
        TERMINATION_TIMEOUT + leave_timeout,
        cluster::leave_on_terminated(system),
    )
    .await
    .context("the actors did not stop and leave within the timeout")?
    .context("leaving the cluster")?;
    if departure.is_acknowledged() {
        println!("Left the cluster");
    } else {
        println!(
            "Left the cluster, unacknowledged by {:?}, timed out for {:?}",
            departure.unacknowledged(),
            departure.timed_out()
        );
    }
    Ok(())
}
// ANCHOR_END: shut-down

// ANCHOR: app
struct App {
    healthy: AtomicBool,
    counter: ActorRef<Count>,
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/count", get(count))
        .with_state(app)
}

async fn health(State(app): State<Arc<App>>) -> StatusCode {
    if app.healthy.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn count(State(app): State<Arc<App>>) -> Result<String, StatusCode> {
    app.counter
        .ask(ASK_TIMEOUT, Count)
        .await
        .map(|count| format!("{count}\n"))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}
// ANCHOR_END: app

// ANCHOR: shutdown-signal
async fn shutdown_signal() -> anyhow::Result<()> {
    let mut terminate = signal(SignalKind::terminate()).context("SIGTERM handler")?;
    tokio::select! {
        _ = terminate.recv() => Ok(()),
        interrupt = signal::ctrl_c() => interrupt.context("Ctrl-C handler"),
    }
}
// ANCHOR_END: shutdown-signal

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

fn start_endpoint() -> anyhow::Result<Duration> {
    let transport = QuicTransport::dev(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .context("dev QUIC transport")?;
    let addr = transport.local_addr().context("local transport address")?;
    let config = EndpointConfig::new(addr);
    let leave_timeout = config.leave_timeout;
    cluster::start_endpoint(config, transport).context("remoting endpoint")?;
    Ok(leave_timeout)
}

struct Counter;

impl Actor for Counter {
    type Message = Count;
    type Timers = Untimed;
    type Stash = Unstashed;
    type State = u64;
    type Error = Infallible;

    fn init(&self, _: &ActorContext<Self::Message>) -> Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn receive(
        &self,
        _: &ActorContext<Self::Message>,
        incoming: Incoming<Self::Message>,
        count: Self::State,
    ) -> Result<Control<Self::State>, Self::Error> {
        let Incoming::Message(Count(reply_to)) = incoming else {
            unreachable!("the counter watches no actor, hence never gets a terminated signal")
        };

        let count = count + 1;
        reply_to.reply(count);
        Ok(Control::Continue(count))
    }
}

struct Count(ReplyTo<u64>);
