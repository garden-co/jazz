//! Direct jazz_core WebSocket boundary.
//!
//! This route intentionally does not share the alpha `/ws` transport framing.
//! It accepts postcard-encoded batches of raw `jazz::wire::WireFrame` bytes,
//! matching the direct core binding/server carrier shape.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::{
    extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    extract::{Query, State},
    response::{IntoResponse, Response},
};
use jazz::ids::AuthorId;
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
    Query(params): Query<HashMap<String, String>>,
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

    let identity = match direct_ws_identity(&params) {
        Ok(identity) => identity,
        Err(error) => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(crate::jazz_transport::ErrorResponse::unauthorized(error)),
            )
                .into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_direct_ws_connection(socket, state, identity))
}

async fn handle_direct_ws_connection(
    mut socket: WebSocket,
    state: Arc<ServerState>,
    identity: AuthorId,
) {
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

    let Some(direct_core) = state.direct_core.clone() else {
        send_direct_wire_error(
            &mut socket,
            WireError::new(
                WireErrorCode::Internal,
                WireRetry::Never,
                "direct websocket requires a fixed schema server",
            ),
        )
        .await;
        let _ = socket.close().await;
        return;
    };
    let session = match direct_core.open(identity, BTreeMap::new()).await {
        Ok(session) => session,
        Err(error) => {
            send_direct_wire_error(
                &mut socket,
                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };

    tracing::info!(
        protocol_version = negotiated.protocol_version,
        features = negotiated.features,
        ?identity,
        "direct jazz_core ws negotiated"
    );

    let mut outbound_tick = tokio::time::interval(std::time::Duration::from_millis(5));
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
                    let frames = match decode_direct_encoded_frame_batch(&bytes) {
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
                    let outbound = match direct_core.receive_tick_take(session, frames).await {
                        Ok(frames) => frames,
                        Err(error) => {
                            send_direct_wire_error(
                                &mut socket,
                                WireError::new(WireErrorCode::Internal, WireRetry::Later, error),
                            )
                            .await;
                            break;
                        }
                    };
                    if !outbound.is_empty() && send_direct_encoded_frames(&mut socket, &outbound).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                _ => {}
            },
            _ = outbound_tick.tick() => {
                let outbound = match direct_core.tick_take(session).await {
                    Ok(frames) => frames,
                    Err(_) => break,
                };
                if !outbound.is_empty() && send_direct_encoded_frames(&mut socket, &outbound).await.is_err() {
                    break;
                }
            }
        }
    }

    direct_core.close(session);
    let _ = socket.close().await;
}

fn direct_ws_identity(params: &HashMap<String, String>) -> Result<AuthorId, String> {
    let Some(identity) = params.get("identity") else {
        return Err("direct websocket requires identity".to_owned());
    };
    if identity.len() != 32 {
        return Err("identity must be 32 hex characters".to_owned());
    }
    let bytes: [u8; 16] = hex::decode(identity)
        .map_err(|_| "identity contains non-hex digit".to_owned())?
        .try_into()
        .map_err(|_| "identity must be 32 hex characters".to_owned())?;
    Ok(AuthorId::from_bytes(bytes))
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
    let encoded_frames = decode_direct_encoded_frame_batch(bytes)?;
    encoded_frames
        .iter()
        .map(|frame| jazz::wire::decode_frame(frame))
        .collect()
}

fn decode_direct_encoded_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    postcard::from_bytes::<Vec<Vec<u8>>>(bytes)
}

async fn send_direct_encoded_frames(
    socket: &mut WebSocket,
    frames: &[Vec<u8>],
) -> Result<(), axum::Error> {
    let batch = postcard::to_allocvec(frames).map_err(axum::Error::new)?;
    socket.send(Message::Binary(batch)).await
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
    fn direct_ws_identity_requires_hex_author() {
        let mut params = HashMap::new();
        params.insert(
            "identity".to_owned(),
            "0102030405060708090a0b0c0d0e0f10".to_owned(),
        );

        assert_eq!(
            direct_ws_identity(&params).unwrap(),
            AuthorId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        );

        params.insert("identity".to_owned(), "not-hex".to_owned());
        assert!(direct_ws_identity(&params).is_err());
    }
}
