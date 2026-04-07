//! Native WebSocket adapter using `tokio-tungstenite`.
//!
//! Shared by NAPI, React Native, server, and integration tests.
//! WASM uses a separate adapter via `web-sys::WebSocket`.

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::transport_ws::StreamAdapter;

/// Native WebSocket connection wrapping `tokio-tungstenite`.
pub struct NativeWsStream {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl StreamAdapter for NativeWsStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (ws, _response) = connect_async(url).await?;
        Ok(Self { ws })
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.ws.send(Message::Binary(data.into())).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(data))) => return Ok(Some(data.to_vec())),
                Some(Ok(Message::Text(text))) => return Ok(Some(text.into_bytes())),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Frame(_))) => continue,
                Some(Err(e)) => return Err(e),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.ws.close(None).await;
    }
}
