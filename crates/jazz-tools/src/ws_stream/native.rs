use crate::transport_manager::StreamAdapter;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub struct NativeWsStream {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl StreamAdapter for NativeWsStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (ws, _) = connect_async(url).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.inner.send(Message::Binary(data.to_owned())).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Binary(b))) => return Ok(Some(b)),
                // Ping/Pong are handled automatically by tokio-tungstenite before
                // reaching this layer; these arms are kept for defensive completeness.
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Text(_))) => {
                    tracing::warn!(
                        "received unexpected text frame on binary-only WS connection; ignoring"
                    );
                    continue;
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close(None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport_manager::StreamAdapter;
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn native_ws_stream_send_recv_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            // Echo server
            while let Some(Ok(msg)) = ws.next().await {
                ws.send(msg).await.unwrap();
            }
        });

        let url = format!("ws://{addr}");
        let mut stream = NativeWsStream::connect(&url).await.unwrap();
        let msg = b"hello ws";
        stream.send(msg).await.unwrap();
        let recv = stream.recv().await.unwrap().unwrap();
        assert_eq!(recv, msg);
    }

    #[tokio::test]
    async fn native_ws_stream_server_close_yields_none_on_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            // Server sends proper WS close frame.
            let _ = ws.close(None).await;
        });

        let url = format!("ws://{addr}");
        let mut stream = NativeWsStream::connect(&url).await.unwrap();
        // Server closed with proper handshake; recv should return None.
        let result = stream.recv().await.unwrap();
        assert!(result.is_none());
    }
}
