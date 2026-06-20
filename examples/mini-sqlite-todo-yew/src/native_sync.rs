use mini_jazz_sqlite::protocol::{ClientMessage, ServerMessage};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeClientFrame {
    pub client_messages: Vec<ClientMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeServerFrame {
    pub server_messages: Vec<ServerMessage>,
}

pub fn encode_client_frame(client_messages: Vec<ClientMessage>) -> Result<String, String> {
    serde_json::to_string(&NativeClientFrame { client_messages }).map_err(|error| error.to_string())
}

pub fn decode_client_frame(encoded: &str) -> Result<NativeClientFrame, String> {
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

pub fn encode_server_frame(server_messages: Vec<ServerMessage>) -> Result<String, String> {
    serde_json::to_string(&NativeServerFrame { server_messages }).map_err(|error| error.to_string())
}

pub fn decode_server_frame(encoded: &str) -> Result<NativeServerFrame, String> {
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_jazz_sqlite::protocol::{ClientMessage, CloseReason, ServerMessage};

    #[test]
    fn native_sync_frames_round_trip_through_json() {
        let client_messages = vec![ClientMessage::Close(CloseReason::ClientClosed)];
        let encoded = encode_client_frame(client_messages.clone()).unwrap();
        let decoded = decode_client_frame(&encoded).unwrap();

        assert!(matches!(
            decoded.client_messages.as_slice(),
            [ClientMessage::Close(CloseReason::ClientClosed)]
        ));

        let server_messages = vec![ServerMessage::Close(CloseReason::ClientClosed)];
        let encoded = encode_server_frame(server_messages.clone()).unwrap();
        let decoded = decode_server_frame(&encoded).unwrap();

        assert!(matches!(
            decoded.server_messages.as_slice(),
            [ServerMessage::Close(CloseReason::ClientClosed)]
        ));
    }
}
