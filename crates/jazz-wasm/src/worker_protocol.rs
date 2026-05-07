//! Worker bridge protocol — binary postcard envelopes.
//!
//! Every `postMessage` between main and worker carries a single `Uint8Array`
//! of postcard-encoded enum bytes, with the underlying `ArrayBuffer`
//! transferred. The only exception is the **init** message, which stays as a
//! JS object so the worker's JS shim can consume `runtimeSources` (bundler-
//! resolved JS module/blob refs) before handing off to Rust. The shim also
//! posts a JS-object `{type:"ready"}` once WASM is loaded.
//!
//! Variant fields use `serde_bytes::ByteBuf` for binary payloads so postcard
//! serialises them as length-prefixed bytes rather than Vec<u8>'s default
//! sequence-of-u8s. Heterogeneous JS-shaped fields (`LocalBatchRecord`,
//! `DebugSchemaState`) ride as JSON strings inside the binary envelope and
//! are `JSON.parse`-d on the JS side — it's the cheapest way to preserve
//! the existing TS listener shapes without re-serialising via
//! `serde-wasm-bindgen` on every receive.

#![allow(dead_code)]

use js_sys::{Array, Reflect, Uint8Array};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

// =============================================================================
// Lifecycle event
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerLifecycleEvent {
    VisibilityHidden,
    VisibilityVisible,
    Pagehide,
    Freeze,
    Resume,
}

// =============================================================================
// Init payload (JS-object special case)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitPayloadFields {
    pub schema_json: String,
    pub app_id: String,
    pub env: String,
    pub user_branch: String,
    pub db_name: String,
    pub client_id: String,
    pub server_url: Option<String>,
    pub jwt_token: Option<String>,
    pub admin_secret: Option<String>,
    pub fallback_wasm_url: Option<String>,
    pub log_level: Option<String>,
    pub telemetry_collector_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InitPayload {
    pub fields: InitPayloadFields,
    /// Opaque bundler-resolved JS source references (Uint8Array / module / URL).
    /// Stays as a `JsValue` because there's no clean Rust shape for it.
    pub runtime_sources: JsValue,
}

// =============================================================================
// Worker → main `Sync` entry
// =============================================================================

/// Per-entry shape inside a worker → main `sync` batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncEntry {
    BareBytes(ByteBuf),
    BareString(String),
    SequencedBytes { payload: ByteBuf, sequence: u64 },
    SequencedString { payload: String, sequence: u64 },
}

// =============================================================================
// Wire enums (postcard-encoded)
// =============================================================================

/// Wire-only Main → Worker variants. The `Init` message is *not* in this enum
/// — it stays as a JS object so the worker's JS shim can pull
/// `runtimeSources` out before handoff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MainToWorkerWire {
    Sync {
        payloads: Vec<ByteBuf>,
    },
    PeerOpen {
        peer_id: String,
    },
    PeerSync {
        peer_id: String,
        term: u32,
        payloads: Vec<ByteBuf>,
    },
    PeerClose {
        peer_id: String,
    },
    LifecycleHint {
        event: WorkerLifecycleEvent,
        sent_at_ms: f64,
    },
    UpdateAuth {
        jwt_token: Option<String>,
    },
    DisconnectUpstream,
    ReconnectUpstream,
    Shutdown,
    AcknowledgeRejectedBatch {
        batch_id: String,
    },
    SimulateCrash,
    DebugSchemaState,
    DebugSeedLiveSchema {
        schema_json: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerToMainWire {
    InitOk {
        client_id: String,
    },
    UpstreamConnected,
    UpstreamDisconnected,
    Sync {
        payloads: Vec<SyncEntry>,
    },
    PeerSync {
        peer_id: String,
        term: u32,
        payloads: Vec<ByteBuf>,
    },
    /// `Vec<LocalBatchRecord>` serialised as JSON. JS side does `JSON.parse`.
    LocalBatchRecordsSync {
        batches_json: String,
    },
    /// `LocalBatchRecord` serialised as JSON.
    MutationErrorReplay {
        batch_json: String,
    },
    Error {
        message: String,
    },
    AuthFailed {
        reason: String,
    },
    ShutdownOk,
    /// `DebugSchemaState` serialised as JSON.
    DebugSchemaStateOk {
        state_json: String,
    },
    DebugSeedLiveSchemaOk,
}

// =============================================================================
// In-process Rust enum used by the host's dispatch loop
// =============================================================================

#[derive(Debug, Clone)]
pub enum MainToWorkerMessage {
    Init(Box<InitPayload>),
    Wire(MainToWorkerWire),
    /// Fallback for unrecognised JS-object messages. Worker host responds
    /// with an `Error`.
    Unknown(String),
}

// =============================================================================
// Read path (Main → Worker)
// =============================================================================

/// Parse an inbound `MessageEvent.data`. Init is JS-object; everything else is
/// a `Uint8Array` of postcard-encoded `MainToWorkerWire` bytes.
pub fn parse_main_to_worker(value: &JsValue) -> Result<MainToWorkerMessage, String> {
    // Init special case (JS object with `type === "init"`).
    if let Some(type_str) = Reflect::get(value, &JsValue::from_str("type"))
        .ok()
        .and_then(|v| v.as_string())
    {
        if type_str == "init" {
            let runtime_sources = Reflect::get(value, &JsValue::from_str("runtimeSources"))
                .unwrap_or(JsValue::UNDEFINED);
            let fields: InitPayloadFields = serde_wasm_bindgen::from_value(value.clone())
                .map_err(|e| format!("init payload: {e}"))?;
            return Ok(MainToWorkerMessage::Init(Box::new(InitPayload {
                fields,
                runtime_sources,
            })));
        }
        return Ok(MainToWorkerMessage::Unknown(type_str));
    }

    // Binary path.
    if let Some(arr) = value.dyn_ref::<Uint8Array>() {
        let bytes = arr.to_vec();
        let wire: MainToWorkerWire =
            postcard::from_bytes(&bytes).map_err(|e| format!("postcard decode: {e}"))?;
        return Ok(MainToWorkerMessage::Wire(wire));
    }

    Err("expected Uint8Array (binary) or `init` JS object".to_string())
}

// =============================================================================
// Read path (Worker → Main)
// =============================================================================

/// Decode a worker → main message. Returns `None` for the JS-object `ready`
/// message (posted by the worker's JS shim before Rust takes over).
pub fn parse_worker_to_main(value: &JsValue) -> ParsedWorkerToMain {
    if let Some(type_str) = Reflect::get(value, &JsValue::from_str("type"))
        .ok()
        .and_then(|v| v.as_string())
    {
        return match type_str.as_str() {
            "ready" => ParsedWorkerToMain::Ready,
            other => ParsedWorkerToMain::UnknownJsObject(other.to_string()),
        };
    }

    if let Some(arr) = value.dyn_ref::<Uint8Array>() {
        let bytes = arr.to_vec();
        return match postcard::from_bytes::<WorkerToMainWire>(&bytes) {
            Ok(wire) => ParsedWorkerToMain::Wire(wire),
            Err(e) => ParsedWorkerToMain::DecodeError(format!("postcard decode: {e}")),
        };
    }

    ParsedWorkerToMain::Malformed
}

#[derive(Debug)]
pub enum ParsedWorkerToMain {
    Ready,
    Wire(WorkerToMainWire),
    UnknownJsObject(String),
    DecodeError(String),
    Malformed,
}

// =============================================================================
// Encode path
// =============================================================================

pub fn encode_main_to_worker(msg: &MainToWorkerWire) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(msg)
}

pub fn encode_worker_to_main(msg: &WorkerToMainWire) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(msg)
}

/// Build a JS-owned `Uint8Array` containing the postcard-encoded message
/// alongside a transfer list with its `ArrayBuffer`. Caller passes both to
/// `target.postMessage(message, transfer)`.
pub fn encode_to_uint8array_with_transfer(bytes: &[u8]) -> (JsValue, Array) {
    let arr = Uint8Array::from(bytes);
    let transfer = Array::new();
    transfer.push(&arr.buffer().into());
    (arr.into(), transfer)
}

// =============================================================================
// Helpers shared with the worker host's outbound builders
// =============================================================================

/// Convenience: serialise a `WorkerToMainWire` and produce the
/// `(message, transfer)` pair ready for `postMessage`.
pub fn worker_to_main_post(msg: &WorkerToMainWire) -> Result<(JsValue, Array), postcard::Error> {
    let bytes = encode_worker_to_main(msg)?;
    Ok(encode_to_uint8array_with_transfer(&bytes))
}

pub fn main_to_worker_post(msg: &MainToWorkerWire) -> Result<(JsValue, Array), postcard::Error> {
    let bytes = encode_main_to_worker(msg)?;
    Ok(encode_to_uint8array_with_transfer(&bytes))
}

#[cfg(test)]
mod tests {
    //! Postcard round-trip tests for the wire enums. These guard the protocol
    //! from a regression where the receiver expects postcard but the sender
    //! emits a JS object — the silent-drop class of bug.
    use super::*;

    fn rt_main(msg: &MainToWorkerWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let decoded: MainToWorkerWire = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
    }

    fn rt_worker(msg: &WorkerToMainWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode");
        let decoded: WorkerToMainWire = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
    }

    #[test]
    fn main_to_worker_round_trips() {
        rt_main(&MainToWorkerWire::Sync {
            payloads: vec![ByteBuf::from(vec![1, 2, 3]), ByteBuf::from(vec![4, 5])],
        });
        rt_main(&MainToWorkerWire::PeerOpen {
            peer_id: "tab-a".into(),
        });
        rt_main(&MainToWorkerWire::PeerSync {
            peer_id: "tab-b".into(),
            term: 7,
            payloads: vec![ByteBuf::from(vec![9, 8, 7])],
        });
        rt_main(&MainToWorkerWire::PeerClose {
            peer_id: "tab-c".into(),
        });
        rt_main(&MainToWorkerWire::LifecycleHint {
            event: WorkerLifecycleEvent::VisibilityHidden,
            sent_at_ms: 1_700_000_000_000.0,
        });
        rt_main(&MainToWorkerWire::UpdateAuth {
            jwt_token: Some("jwt".into()),
        });
        rt_main(&MainToWorkerWire::UpdateAuth { jwt_token: None });
        rt_main(&MainToWorkerWire::DisconnectUpstream);
        rt_main(&MainToWorkerWire::ReconnectUpstream);
        rt_main(&MainToWorkerWire::Shutdown);
        rt_main(&MainToWorkerWire::AcknowledgeRejectedBatch {
            batch_id: "b1".into(),
        });
        rt_main(&MainToWorkerWire::SimulateCrash);
        rt_main(&MainToWorkerWire::DebugSchemaState);
        rt_main(&MainToWorkerWire::DebugSeedLiveSchema {
            schema_json: "{}".into(),
        });
    }

    #[test]
    fn worker_to_main_round_trips() {
        rt_worker(&WorkerToMainWire::InitOk {
            client_id: "c1".into(),
        });
        rt_worker(&WorkerToMainWire::UpstreamConnected);
        rt_worker(&WorkerToMainWire::UpstreamDisconnected);
        rt_worker(&WorkerToMainWire::Sync {
            payloads: vec![
                SyncEntry::BareBytes(ByteBuf::from(vec![1, 2])),
                SyncEntry::BareString("hi".into()),
                SyncEntry::SequencedBytes {
                    payload: ByteBuf::from(vec![9]),
                    sequence: 42,
                },
                SyncEntry::SequencedString {
                    payload: "x".into(),
                    sequence: 99,
                },
            ],
        });
        rt_worker(&WorkerToMainWire::PeerSync {
            peer_id: "p".into(),
            term: 1,
            payloads: vec![ByteBuf::from(vec![0xff])],
        });
        rt_worker(&WorkerToMainWire::LocalBatchRecordsSync {
            batches_json: "[]".into(),
        });
        rt_worker(&WorkerToMainWire::MutationErrorReplay {
            batch_json: "{}".into(),
        });
        rt_worker(&WorkerToMainWire::Error {
            message: "oops".into(),
        });
        rt_worker(&WorkerToMainWire::AuthFailed {
            reason: "expired".into(),
        });
        rt_worker(&WorkerToMainWire::ShutdownOk);
        rt_worker(&WorkerToMainWire::DebugSchemaStateOk {
            state_json: "{}".into(),
        });
        rt_worker(&WorkerToMainWire::DebugSeedLiveSchemaOk);
    }
}
