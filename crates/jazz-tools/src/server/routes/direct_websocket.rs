//! Direct jazz_core WebSocket boundary.
//!
//! This route intentionally does not share the alpha `/ws` transport framing.
//! It accepts postcard-encoded batches of raw `jazz::wire::WireFrame` bytes,
//! matching the direct core binding/server carrier shape.

use std::sync::Arc;

use axum::{
    extract::State,
    extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    response::{IntoResponse, Response},
};
use jazz::wire::{
    FEATURE_STRUCTURED_ERRORS, FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireError,
    WireErrorCode, WireFrame, WireHello, WirePeerRole, WireRetry, encode_frame, negotiate_wire,
};

use crate::server::ServerState;

const DIRECT_WS_REQUIRED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD;
const DIRECT_WS_SUPPORTED_FEATURES: u64 = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;

/// Direct jazz_core websocket endpoint.
///
/// This is a protocol boundary, not a compatibility shim for the alpha
/// `SyncPayload` websocket. The semantic `SyncMessage` loop is deliberately
/// gated on the server owning the state needed to open a real direct
/// `jazz::Db` peer.
pub(super) async fn direct_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
) -> Response {
    if state.shutdown.is_shutting_down() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(crate::jazz_transport::ErrorResponse::internal(
                "server is shutting down".to_string(),
            )),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| handle_direct_ws_connection(socket, state))
}

async fn handle_direct_ws_connection(mut socket: WebSocket, state: Arc<ServerState>) {
    let mut shutdown_rx = state.shutdown.subscribe();
    let Some(_websocket_guard) = state.shutdown.try_enter_websocket() else {
        close_direct_ws_for_shutdown(&mut socket).await;
        return;
    };

    let first = tokio::select! {
        msg = socket.recv() => match msg {
            Some(Ok(Message::Binary(bytes))) => bytes,
            _ => {
                let _ = socket.close().await;
                return;
            }
        },
        changed = shutdown_rx.changed() => {
            if changed.is_ok() && state.shutdown.is_shutting_down() {
                close_direct_ws_for_shutdown(&mut socket).await;
            } else {
                let _ = socket.close().await;
            }
            return;
        }
    };

    let Some(WireFrame::Hello(remote_hello)) = decode_single_direct_frame(&first).ok() else {
        send_direct_wire_error(
            &mut socket,
            WireError::new(
                WireErrorCode::MalformedFrame,
                WireRetry::Never,
                "direct websocket expects first frame to be WireFrame::Hello",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };

    let negotiated = match negotiate_wire(
        &remote_hello,
        WIRE_PROTOCOL_VERSION,
        WIRE_PROTOCOL_VERSION,
        DIRECT_WS_SUPPORTED_FEATURES,
    ) {
        Ok(negotiated) if negotiated.features & DIRECT_WS_REQUIRED_FEATURES != 0 => negotiated,
        Ok(_) => {
            send_direct_wire_error(
                &mut socket,
                WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::Never,
                    "direct websocket requires sync message payload frames",
                ),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        Err(error) => {
            send_direct_wire_error(&mut socket, error).await;
            let _ = socket.close().await;
            return;
        }
    };

    let server_hello = WireFrame::Hello(WireHello {
        min_protocol_version: negotiated.protocol_version,
        max_protocol_version: negotiated.protocol_version,
        features: negotiated.features,
        role: WirePeerRole::Core,
    });
    if send_direct_wire_frames(&mut socket, &[server_hello])
        .await
        .is_err()
    {
        return;
    }

    tracing::info!(
        protocol_version = negotiated.protocol_version,
        features = negotiated.features,
        "direct jazz_core ws negotiated"
    );

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && state.shutdown.is_shutting_down() {
                    close_direct_ws_for_shutdown(&mut socket).await;
                    break;
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    let frames = match decode_direct_frame_batch(&bytes) {
                        Ok(frames) => frames,
                        Err(_) => {
                            send_direct_wire_error(
                                &mut socket,
                                WireError::new(
                                    WireErrorCode::MalformedFrame,
                                    WireRetry::Never,
                                    "failed to decode direct websocket frame batch",
                                ),
                            )
                            .await;
                            break;
                        }
                    };
                    if frames.iter().any(|frame| matches!(frame, WireFrame::Message(_))) {
                        send_direct_wire_error(&mut socket, direct_peer_loop_unavailable_error())
                            .await;
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    let _ = socket.close().await;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MissingDirectPeerLoopState {
    fields: &'static [&'static str],
}

impl MissingDirectPeerLoopState {
    fn current_server_state() -> Self {
        Self {
            fields: &[
                "history-complete jazz::Db stored on ServerState",
                "jazz::schema::JazzSchema for the direct core node",
                "groove::storage::OrderedKvStorage + ReopenableStorage for direct core history",
                "jazz::db::DbIdentity for the direct core node",
                "authenticated client AuthorId/session admission for accept_subscriber",
            ],
        }
    }

    fn diagnostic(&self) -> String {
        format!(
            "direct websocket negotiated jazz_core wire frames, but ServerState cannot start a server-side peer loop yet; missing: {}. TODO: add a ServerState-owned history-complete jazz::Db and call Db::accept_subscriber through WireTransportAdapter instead of routing through the legacy SyncPayload runtime.",
            self.fields.join(", ")
        )
    }
}

fn direct_peer_loop_unavailable_error() -> WireError {
    WireError::new(
        WireErrorCode::Internal,
        WireRetry::Never,
        MissingDirectPeerLoopState::current_server_state().diagnostic(),
    )
}

fn decode_single_direct_frame(bytes: &[u8]) -> Result<WireFrame, postcard::Error> {
    let mut frames = decode_direct_frame_batch(bytes)?;
    if frames.len() == 1 {
        Ok(frames.remove(0))
    } else {
        postcard::from_bytes(bytes)
    }
}

fn decode_direct_frame_batch(bytes: &[u8]) -> Result<Vec<WireFrame>, postcard::Error> {
    let encoded_frames = postcard::from_bytes::<Vec<Vec<u8>>>(bytes)?;
    encoded_frames
        .iter()
        .map(|frame| jazz::wire::decode_frame(frame))
        .collect()
}

async fn send_direct_wire_error(socket: &mut WebSocket, error: WireError) {
    let _ = send_direct_wire_frames(socket, &[WireFrame::Error(error)]).await;
}

async fn send_direct_wire_frames(
    socket: &mut WebSocket,
    frames: &[WireFrame],
) -> Result<(), axum::Error> {
    let encoded = frames
        .iter()
        .map(encode_frame)
        .collect::<Result<Vec<_>, _>>()
        .map_err(axum::Error::new)?;
    let batch = postcard::to_allocvec(&encoded).map_err(axum::Error::new)?;
    socket.send(Message::Binary(batch)).await
}

async fn close_direct_ws_for_shutdown(socket: &mut WebSocket) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::RESTART,
            reason: "server shutting down".into(),
        })))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_frame_batch_round_trips_wire_frames() {
        let frames = vec![WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            DIRECT_WS_SUPPORTED_FEATURES,
        ))];
        let encoded = frames
            .iter()
            .map(encode_frame)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let batch = postcard::to_allocvec(&encoded).unwrap();

        assert_eq!(decode_direct_frame_batch(&batch).unwrap(), frames);
    }

    #[test]
    fn direct_peer_loop_error_documents_missing_server_state() {
        // Internal test: the missing direct jazz_core Db is not observable
        // through public client APIs until ServerState can own one.
        let error = direct_peer_loop_unavailable_error();

        assert_eq!(error.code, WireErrorCode::Internal);
        assert_eq!(error.retry, WireRetry::Never);
        assert!(
            error
                .message
                .contains("history-complete jazz::Db stored on ServerState")
        );
        assert!(error.message.contains("jazz::schema::JazzSchema"));
        assert!(
            error
                .message
                .contains("OrderedKvStorage + ReopenableStorage")
        );
        assert!(error.message.contains("jazz::db::DbIdentity"));
        assert!(
            error
                .message
                .contains("authenticated client AuthorId/session admission")
        );
        assert!(
            error
                .message
                .contains("Db::accept_subscriber through WireTransportAdapter")
        );
        assert!(
            !error
                .message
                .contains("SyncPayload websocket compatibility")
        );
    }
}
