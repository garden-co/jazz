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

use crate::schema_manager::AppId;
use crate::transport_auth::AuthConfig;

const DIRECT_CLIENT_SUPPORTED_FEATURES: u64 =
    FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;
const DIRECT_CLIENT_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const DIRECT_CLIENT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum DirectCoreWebSocketClientError {
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

impl fmt::Display for DirectCoreWebSocketClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "failed to connect direct websocket: {error}"),
            Self::Send(error) => write!(f, "failed to send direct websocket frame: {error}"),
            Self::Receive(error) => write!(f, "failed to receive direct websocket frame: {error}"),
            Self::ClosedDuringHandshake => {
                write!(f, "direct websocket closed during handshake")
            }
            Self::HandshakeTimeout => write!(f, "direct websocket handshake timed out"),
            Self::UnexpectedHandshakeMessage => {
                write!(
                    f,
                    "direct websocket returned an unexpected handshake message"
                )
            }
            Self::EncodePrelude(error) => write!(f, "failed to encode direct prelude: {error}"),
            Self::EncodeHello(error) => write!(f, "failed to encode direct hello: {error}"),
            Self::DecodeBatch(error) => write!(f, "failed to decode direct frame batch: {error}"),
            Self::DecodeFrame(error) => write!(f, "failed to decode direct frame: {error}"),
            Self::Negotiation(error) => write!(f, "direct websocket negotiation failed: {error:?}"),
            Self::ServerRejected(reason) => write!(f, "direct websocket rejected: {reason}"),
        }
    }
}

impl std::error::Error for DirectCoreWebSocketClientError {}

#[derive(Debug)]
pub struct DirectCoreWebSocketTransport {
    inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    task: tokio::task::JoinHandle<()>,
}

impl DirectCoreWebSocketTransport {
    pub async fn connect(
        base_url: impl AsRef<str>,
        app_id: AppId,
        peer_identity: AuthorId,
        auth: AuthConfig,
    ) -> Result<Self, DirectCoreWebSocketClientError> {
        let url = direct_ws_url(base_url.as_ref(), app_id);
        let (mut ws, _) = connect_async(url)
            .await
            .map_err(DirectCoreWebSocketClientError::Connect)?;

        let prelude = serde_json::to_vec(&DirectWsClientPrelude {
            peer_identity: hex::encode(peer_identity.as_bytes()),
            auth,
        })
        .map_err(DirectCoreWebSocketClientError::EncodePrelude)?;
        ws.send(Message::Binary(prelude.into()))
            .await
            .map_err(DirectCoreWebSocketClientError::Send)?;

        let hello = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            DIRECT_CLIENT_SUPPORTED_FEATURES,
        ));
        let encoded_hello =
            encode_frame(&hello).map_err(DirectCoreWebSocketClientError::EncodeHello)?;
        let batch = postcard::to_allocvec(&vec![encoded_hello])
            .map_err(DirectCoreWebSocketClientError::EncodeHello)?;
        ws.send(Message::Binary(batch.into()))
            .await
            .map_err(DirectCoreWebSocketClientError::Send)?;

        let server_hello = receive_server_hello(&mut ws).await?;
        let negotiated = negotiate_wire(
            &server_hello,
            WIRE_PROTOCOL_VERSION,
            WIRE_PROTOCOL_VERSION,
            DIRECT_CLIENT_SUPPORTED_FEATURES,
        )
        .map_err(DirectCoreWebSocketClientError::Negotiation)?;
        if negotiated.features & DIRECT_CLIENT_REQUIRED_FEATURES != DIRECT_CLIENT_REQUIRED_FEATURES
        {
            return Err(DirectCoreWebSocketClientError::ServerRejected(
                "server did not negotiate sync message payload frames".to_owned(),
            ));
        }

        let inbound = Arc::new(Mutex::new(VecDeque::new()));
        let (outbound, outbound_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_direct_ws_pump(ws, inbound.clone(), outbound_rx));

        Ok(Self {
            inbound,
            outbound,
            task,
        })
    }
}

impl Drop for DirectCoreWebSocketTransport {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WireTransport for DirectCoreWebSocketTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound
            .send(frame)
            .map_err(|_| TransportError::Failed("direct websocket pump is closed".to_owned()))
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.lock().ok()?.pop_front()
    }
}

#[derive(serde::Serialize)]
struct DirectWsClientPrelude {
    peer_identity: String,
    auth: AuthConfig,
}

fn direct_ws_url(base_url: &str, app_id: AppId) -> String {
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
) -> Result<WireHello, DirectCoreWebSocketClientError> {
    let message = tokio::time::timeout(DIRECT_CLIENT_HANDSHAKE_TIMEOUT, ws.next())
        .await
        .map_err(|_| DirectCoreWebSocketClientError::HandshakeTimeout)?
        .ok_or(DirectCoreWebSocketClientError::ClosedDuringHandshake)?
        .map_err(DirectCoreWebSocketClientError::Receive)?;

    let Message::Binary(bytes) = message else {
        return Err(DirectCoreWebSocketClientError::UnexpectedHandshakeMessage);
    };
    let encoded: Vec<Vec<u8>> =
        postcard::from_bytes(&bytes).map_err(DirectCoreWebSocketClientError::DecodeBatch)?;
    if encoded.len() != 1 {
        return Err(DirectCoreWebSocketClientError::UnexpectedHandshakeMessage);
    }
    let frame = decode_frame(&encoded[0]).map_err(DirectCoreWebSocketClientError::DecodeFrame)?;
    let WireFrame::Hello(hello) = frame else {
        if let WireFrame::Error(error) = frame {
            return Err(DirectCoreWebSocketClientError::ServerRejected(format!(
                "{:?}: {}",
                error.code, error.message
            )));
        }
        return Err(DirectCoreWebSocketClientError::UnexpectedHandshakeMessage);
    };
    if hello.role != WirePeerRole::Core {
        return Err(DirectCoreWebSocketClientError::UnexpectedHandshakeMessage);
    }
    Ok(hello)
}

async fn run_direct_ws_pump(
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
    mut outbound: mpsc::UnboundedReceiver<Vec<u8>>,
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
                if ws.send(Message::Binary(bytes.into())).await.is_err() {
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
                queue.extend(frames);
            }
        }
    }
}
