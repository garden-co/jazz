use std::net::SocketAddr;

use axum::serve::{Listener, ListenerExt};
use tokio::net::{TcpListener, TcpStream};

/// Configure each connection once, before HTTP or WebSocket traffic is served.
/// Small multiplexed wire frames must not wait for the peer's delayed TCP ACK.
pub(crate) fn low_latency_listener(
    listener: TcpListener,
) -> impl Listener<Io = TcpStream, Addr = SocketAddr> {
    listener.tap_io(|stream| {
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%error, "failed to disable TCP buffering on accepted connection");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Logical query results cannot detect this socket policy. Check real accepted
    // sockets directly instead of using a timing threshold that flakes under load.
    #[tokio::test]
    async fn accepted_connections_disable_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut listener = low_latency_listener(listener);
        for _ in 0..2 {
            let (client, (server, _)) =
                tokio::join!(TcpStream::connect(address), listener.accept());
            client.unwrap();
            assert!(server.nodelay().unwrap());
        }
    }
}
