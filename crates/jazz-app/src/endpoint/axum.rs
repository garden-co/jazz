//! [`Endpoint::router`]: the endpoint as an Axum router (feature `axum`).

use axum::extract::ws::{CloseFrame, Message, WebSocketUpgrade};
use axum::extract::{FromRequestParts as _, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::IntoResponse as _;
use futures_util::{SinkExt as _, StreamExt as _};

use super::{Endpoint, Request};

/// The largest request body the account routes read.
const MAX_BODY: usize = 64 * 1024;

impl Endpoint {
    /// The endpoint as an Axum router, serving everything under its
    /// [`Endpoint::path`]: merge it into the app's router.
    ///
    /// ```ignore
    /// let app = axum::Router::new().route("/", get(home)).merge(endpoint.router());
    /// ```
    pub fn router(&self) -> axum::Router {
        let base = self.path().to_owned();
        axum::Router::new()
            .route(&format!("{base}/{{*rest}}"), axum::routing::any(serve))
            .with_state(self.clone())
    }
}

async fn serve(
    State(endpoint): State<Endpoint>,
    request: axum::extract::Request,
) -> axum::response::Response {
    let (mut parts, body) = request.into_parts();
    let head = Request {
        method: parts.method.to_string(),
        path: parts.uri.path_and_query().map_or_else(
            || parts.uri.path().to_owned(),
            |path| path.as_str().to_owned(),
        ),
        headers: parts
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())))
            .collect(),
        body: Vec::new(),
    };
    if endpoint.is_socket(&head.path) {
        let Ok(upgrade) = WebSocketUpgrade::from_request_parts(&mut parts, &()).await else {
            return endpoint.handle(head).await.into_axum();
        };
        return upgrade.on_upgrade(move |socket| async move {
            let (mut sink, stream) = socket.split();
            // A close or an error ends the client's stream; text, ping and
            // pong frames carry nothing for the core.
            let incoming = Box::pin(
                stream
                    .take_while(|message| {
                        std::future::ready(
                            matches!(message, Ok(m) if !matches!(m, Message::Close(_))),
                        )
                    })
                    .filter_map(|message| {
                        std::future::ready(match message {
                            Ok(Message::Binary(data)) => Some(data.to_vec()),
                            _ => None,
                        })
                    }),
            );
            let (to_client, mut from_relay) = futures_channel_pair();
            let relay = endpoint.serve_socket(head, incoming, to_client);
            let forward = async {
                while let Some(data) = from_relay.recv().await {
                    if sink.send(Message::Binary(data.into())).await.is_err() {
                        break;
                    }
                }
            };
            let (closed, ()) = futures_util::future::join(relay, forward).await;
            let _ = sink
                .send(Message::Close(Some(CloseFrame {
                    code: closed.code,
                    reason: closed.reason.into(),
                })))
                .await;
        });
    }
    let Ok(body) = axum::body::to_bytes(body, MAX_BODY).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    endpoint
        .handle(Request {
            body: body.to_vec(),
            ..head
        })
        .await
        .into_axum()
}

/// A sink the relay writes the client's messages to, drained into the
/// WebSocket by the caller: the relay's end closes when it is done.
fn futures_channel_pair() -> (
    impl futures_util::Sink<Vec<u8>> + Unpin + Send,
    tokio::sync::mpsc::Receiver<Vec<u8>>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    let sink = futures_util::sink::unfold(sender, |sender, data: Vec<u8>| async move {
        sender.send(data).await.map(|()| sender)
    });
    (Box::pin(sink), receiver)
}

impl super::Response {
    fn into_axum(self) -> axum::response::Response {
        let mut response = (
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            self.body,
        )
            .into_response();
        for (name, value) in self.headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::try_from(value.as_str()),
            ) {
                response.headers_mut().insert(name, value);
            }
        }
        response
    }
}
