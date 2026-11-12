//! The router of a started endpoint, served the way an application serves it. It lives in a test
//! binary of its own, since the endpoint is one per process.

use axum::Router;
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tellus::cluster::{self, EndpointConfig, transport::QuicTransport};
use tellus_cluster_inspect::InspectConfig;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};

/// One signal shuts a server down gracefully while a client follows `/inspect/events`. The server
/// and the router each take it once, and then the stream ends and the server completes.
#[tokio::test]
async fn a_graceful_shutdown_completes_with_a_client_on_events() {
    let transport = QuicTransport::dev(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("the dev transport is built");
    let addr = transport.local_addr().expect("the transport is bound");
    cluster::start_endpoint(EndpointConfig::new(addr), transport).expect("the endpoint starts");
    cluster::form().expect("the cluster forms");

    let drain = Arc::new(Notify::new());
    let inspect =
        tellus_cluster_inspect::router(InspectConfig::default(), drain.clone().notified_owned())
            .expect("the endpoint is started");
    let app = Router::new().nest("/inspect", inspect);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let http_addr = listener.local_addr().expect("the local address");
    let server = tokio::spawn(
        axum::serve(listener, app)
            .with_graceful_shutdown(drain.clone().notified_owned())
            .into_future(),
    );

    let client = TcpStream::connect(http_addr)
        .await
        .expect("the client connects");
    client.writable().await.expect("the socket is writable");
    client
        .try_write(b"GET /inspect/events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("the request is written");
    let opening = read_until(&client, |text| text.contains(r#""kind":"hello""#)).await;
    assert!(
        opening.starts_with("HTTP/1.1 200 OK"),
        "the client is not on the stream: {opening}"
    );

    drain.notify_waiters();
    timeout(Duration::from_secs(5), server)
        .await
        .expect("the server still waits for the client on /inspect/events")
        .expect("the server task does not panic")
        .expect("serving does not fail");
    let rest = read_until(&client, |_| false).await;
    assert!(
        rest.ends_with("0\r\n\r\n"),
        "the stream did not end: {rest}"
    );
}

async fn read_until(client: &TcpStream, done: impl Fn(&str) -> bool) -> String {
    let mut text = String::new();
    timeout(Duration::from_secs(5), async {
        while !done(&text) {
            client.readable().await.expect("the socket is readable");
            let mut chunk = [0; 4096];
            match client.try_read(&mut chunk) {
                Ok(0) => break,
                Ok(len) => text.push_str(&String::from_utf8_lossy(&chunk[..len])),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("reading the response failed: {error}"),
            }
        }
    })
    .await
    .expect("the response arrives in time");
    text
}
