use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use jazz::ids::AuthorId;
use jazz::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, TransportError, WIRE_PROTOCOL_VERSION,
    WireError, WireFrame, WireHello, WirePeerRole, WireTransport, decode_frame, encode_frame,
    negotiate_wire,
};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::AppId;
use crate::websocket_prelude_auth::AuthConfig;

const WS_CLIENT_SUPPORTED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;
const WS_CLIENT_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const WS_CLIENT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum WebSocketClientError {
    Connect(tokio_tungstenite::tungstenite::Error),
    Send(tokio_tungstenite::tungstenite::Error),
    Receive(tokio_tungstenite::tungstenite::Error),
    ClosedDuringHandshake,
    HandshakeTimeout,
    UnexpectedHandshakeMessage,
    EncodePrelude(serde_json::Error),
    EncodeHello(postcard::Error),
    DecodeBatch(postcard::Error),
    DecodeFrame(postcard::Error),
    Negotiation(WireError),
    ServerRejected(String),
}

impl fmt::Display for WebSocketClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "failed to connect websocket: {error}"),
            Self::Send(error) => write!(f, "failed to send websocket frame: {error}"),
            Self::Receive(error) => write!(f, "failed to receive websocket frame: {error}"),
            Self::ClosedDuringHandshake => {
                write!(f, "websocket closed during handshake")
            }
            Self::HandshakeTimeout => write!(f, "websocket handshake timed out"),
            Self::UnexpectedHandshakeMessage => {
                write!(f, "websocket returned an unexpected handshake message")
            }
            Self::EncodePrelude(error) => write!(f, "failed to encode websocket prelude: {error}"),
            Self::EncodeHello(error) => write!(f, "failed to encode websocket hello: {error}"),
            Self::DecodeBatch(error) => write!(f, "failed to decode frame batch: {error}"),
            Self::DecodeFrame(error) => write!(f, "failed to decode frame: {error}"),
            Self::Negotiation(error) => write!(f, "websocket negotiation failed: {error:?}"),
            Self::ServerRejected(reason) => write!(f, "websocket rejected: {reason}"),
        }
    }
}

impl std::error::Error for WebSocketClientError {}

pub struct WebSocketTransport {
    inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    wake: Arc<dyn Fn() + Send + Sync>,
    task: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for WebSocketTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketTransport")
            .field("inbound", &self.inbound)
            .field("outbound", &self.outbound)
            .field("task", &self.task)
            .finish_non_exhaustive()
    }
}

impl WebSocketTransport {
    pub async fn connect(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorId,
        auth: AuthConfig,
    ) -> Result<Self, WebSocketClientError> {
        Self::connect_with_wake(base_url, app_id, peer_identity, auth, Arc::new(|| {})).await
    }

    pub async fn connect_with_wake(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorId,
        auth: AuthConfig,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Self, WebSocketClientError> {
        let url = ws_url(base_url.as_ref(), app_id);
        let (mut ws, _) = connect_async(url)
            .await
            .map_err(WebSocketClientError::Connect)?;

        let prelude = serde_json::to_vec(&WebSocketClientPrelude {
            peer_identity: hex::encode(peer_identity.as_bytes()),
            auth,
        })
        .map_err(WebSocketClientError::EncodePrelude)?;
        ws.send(Message::Binary(prelude))
            .await
            .map_err(WebSocketClientError::Send)?;

        let hello = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            WS_CLIENT_SUPPORTED_FEATURES,
        ));
        let encoded_hello = encode_frame(&hello).map_err(WebSocketClientError::EncodeHello)?;
        let batch = postcard::to_allocvec(&vec![encoded_hello])
            .map_err(WebSocketClientError::EncodeHello)?;
        ws.send(Message::Binary(batch))
            .await
            .map_err(WebSocketClientError::Send)?;

        let server_hello = receive_server_hello(&mut ws).await?;
        let negotiated = negotiate_wire(
            &server_hello,
            WIRE_PROTOCOL_VERSION,
            WIRE_PROTOCOL_VERSION,
            WS_CLIENT_SUPPORTED_FEATURES,
        )
        .map_err(WebSocketClientError::Negotiation)?;
        if negotiated.features & WS_CLIENT_REQUIRED_FEATURES != WS_CLIENT_REQUIRED_FEATURES {
            return Err(WebSocketClientError::ServerRejected(
                "server did not negotiate sync message payload frames".to_owned(),
            ));
        }

        let inbound = Arc::new(Mutex::new(VecDeque::new()));
        let (outbound, outbound_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_ws_pump(
            ws,
            inbound.clone(),
            outbound_rx,
            Arc::clone(&wake),
        ));

        Ok(Self {
            inbound,
            outbound,
            wake,
            task,
        })
    }
}

impl Drop for WebSocketTransport {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WireTransport for WebSocketTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        #[cfg(feature = "sync-autopsy")]
        jazz::db::sync_autopsy::record(format!(
            "client websocket queue outbound frame bytes={}",
            frame.len()
        ));
        self.outbound
            .send(frame)
            .map_err(|_| TransportError::Failed("websocket pump is closed".to_owned()))?;
        (self.wake)();
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        let mut inbound = self.inbound.lock().ok()?;
        let before = inbound.len();
        let frame = inbound.pop_front();
        if let Some(frame) = &frame {
            #[cfg(feature = "sync-autopsy")]
            jazz::db::sync_autopsy::record(format!(
                "client websocket pop inbound before={before} after={} bytes={}",
                inbound.len(),
                frame.len()
            ));
        }
        frame
    }
}

#[derive(serde::Serialize)]
struct WebSocketClientPrelude {
    peer_identity: String,
    auth: AuthConfig,
}

fn ws_url(base_url: &str, app_id: AppId) -> String {
    let base = base_url
        .replace("http://", "ws://")
        .replace("https://", "wss://")
        .trim_end_matches('/')
        .to_owned();
    format!("{base}/apps/{app_id}/ws")
}

async fn receive_server_hello(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<WireHello, WebSocketClientError> {
    let message = tokio::time::timeout(WS_CLIENT_HANDSHAKE_TIMEOUT, ws.next())
        .await
        .map_err(|_| WebSocketClientError::HandshakeTimeout)?
        .ok_or(WebSocketClientError::ClosedDuringHandshake)?
        .map_err(WebSocketClientError::Receive)?;

    let Message::Binary(bytes) = message else {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    };
    let encoded: Vec<Vec<u8>> =
        postcard::from_bytes(&bytes).map_err(WebSocketClientError::DecodeBatch)?;
    if encoded.len() != 1 {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    let frame = decode_frame(&encoded[0]).map_err(WebSocketClientError::DecodeFrame)?;
    let WireFrame::Hello(hello) = frame else {
        if let WireFrame::Error(error) = frame {
            return Err(WebSocketClientError::ServerRejected(format!(
                "{:?}: {}",
                error.code, error.message
            )));
        }
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    };
    if hello.role != WirePeerRole::Core {
        return Err(WebSocketClientError::UnexpectedHandshakeMessage);
    }
    Ok(hello)
}

async fn run_ws_pump(
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    mut outbound: mpsc::UnboundedReceiver<Vec<u8>>,
    wake: Arc<dyn Fn() + Send + Sync>,
) {
    loop {
        tokio::select! {
            maybe_frame = outbound.recv() => {
                let Some(first_frame) = maybe_frame else {
                    let _ = ws.close(None).await;
                    return;
                };
                let mut batch = vec![first_frame];
                while let Ok(frame) = outbound.try_recv() {
                    batch.push(frame);
                }
                let Ok(bytes) = postcard::to_allocvec(&batch) else {
                    continue;
                };
                #[cfg(feature = "sync-autopsy")]
                jazz::db::sync_autopsy::record(format!(
                    "client websocket send batch frames={} bytes={}",
                    batch.len(),
                    bytes.len()
                ));
                if ws.send(Message::Binary(bytes)).await.is_err() {
                    return;
                }
            }
            message = ws.next() => {
                let Some(Ok(Message::Binary(bytes))) = message else {
                    return;
                };
                let Ok(frames) = postcard::from_bytes::<Vec<Vec<u8>>>(&bytes) else {
                    return;
                };
                let Ok(mut queue) = inbound.lock() else {
                    return;
                };
                let before = queue.len();
                let frame_count = frames.len();
                queue.extend(frames);
                #[cfg(feature = "sync-autopsy")]
                jazz::db::sync_autopsy::record(format!(
                    "client websocket received batch frames={frame_count} inbound_before={before} inbound_after={} bytes={}",
                    queue.len(),
                    bytes.len()
                ));
                drop(queue);
                wake();
            }
        }
    }
}
