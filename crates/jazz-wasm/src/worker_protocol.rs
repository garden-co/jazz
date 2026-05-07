//! Worker bridge protocol — message shapes exchanged between the main thread
//! and the dedicated worker over `postMessage`.
//!
//! ## Encoding strategy
//!
//! - **Scalar variants** go through `serde-wasm-bindgen` end-to-end with
//!   `tag = "type"`, `rename_all = "kebab-case"` for the discriminator, and
//!   `rename_all = "camelCase"` on inner field names — matching the existing
//!   TS protocol.
//! - **Binary-payload variants** (`Sync`, `PeerSync` in both directions, and
//!   `Init`'s `runtimeSources` field) are hand-rolled. `serde-wasm-bindgen`'s
//!   `Vec<u8>` path allocates a JS array of numbers; we want JS-owned
//!   `Uint8Array`s so the transferable list stays intact.
//!
//! ## Read path
//!
//! `parse_main_to_worker(value)` dispatches on the JS `type` field and reads
//! the rest of the payload via `Reflect::get` for binary variants or
//! `serde-wasm-bindgen` for scalars.
//!
//! ## Write path
//!
//! Each write function returns `(JsValue /* the message */, js_sys::Array /*
//! transferable buffers */)`. Callers pass the pair to
//! `target.postMessage(message, transferables)`.

#![allow(dead_code)]

use js_sys::{Array, Object, Reflect, Uint8Array};
use serde::{Deserialize, Serialize};
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
// Init payload
// =============================================================================

/// Init payload. Everything except `runtime_sources` round-trips through serde.
/// `runtime_sources` is opaque (bundler-resolved JS module references / blobs)
/// and is attached/extracted via `Reflect`.
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
// Sequenced sync payload (worker → main, heterogeneous)
// =============================================================================

/// One entry of a worker→main `sync` message. The worker emits three shapes:
/// - bare `Uint8Array` (binary, unsequenced)
/// - bare `string` (JSON, unsequenced — server-bound traffic forwarded out of
///   bootstrap-catalogue mode)
/// - `{payload, sequence}` envelope (sequenced, payload is `Uint8Array | string`)
#[derive(Debug, Clone)]
pub enum SyncEnvelopeFromWorker {
    BareBytes(Vec<u8>),
    BareString(String),
    SequencedBytes { payload: Vec<u8>, sequence: u64 },
    SequencedString { payload: String, sequence: u64 },
}

// =============================================================================
// Main → Worker messages
// =============================================================================

#[derive(Debug, Clone)]
pub enum MainToWorkerMessage {
    Init(Box<InitPayload>),
    Sync {
        /// Already postcard-encoded sync payload bytes. The main → worker direction
        /// is always client-bound binary, so each entry is bare bytes.
        payloads: Vec<Vec<u8>>,
    },
    PeerOpen {
        peer_id: String,
    },
    PeerSync {
        peer_id: String,
        term: u32,
        payloads: Vec<Vec<u8>>,
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
    /// Unknown / malformed message. Worker host responds with an `Error`.
    Unknown(String),
}

pub fn parse_main_to_worker(value: &JsValue) -> Result<MainToWorkerMessage, String> {
    let type_value = Reflect::get(value, &JsValue::from_str("type"))
        .map_err(|_| "missing `type` field".to_string())?;
    let type_str = type_value
        .as_string()
        .ok_or_else(|| "`type` is not a string".to_string())?;

    match type_str.as_str() {
        "init" => {
            let runtime_sources = Reflect::get(value, &JsValue::from_str("runtimeSources"))
                .unwrap_or(JsValue::UNDEFINED);
            // serde-wasm-bindgen ignores extra fields by default, so the
            // `type` and `runtimeSources` fields on the JS object are skipped
            // automatically. No clone/strip needed.
            let fields: InitPayloadFields = serde_wasm_bindgen::from_value(value.clone())
                .map_err(|e| format!("init payload: {e}"))?;
            Ok(MainToWorkerMessage::Init(Box::new(InitPayload {
                fields,
                runtime_sources,
            })))
        }
        "sync" => {
            let payload_arr = Reflect::get(value, &JsValue::from_str("payload"))
                .map_err(|_| "missing payload".to_string())?;
            let arr = payload_arr
                .dyn_into::<Array>()
                .map_err(|_| "sync payload is not an array".to_string())?;
            let mut payloads = Vec::with_capacity(arr.length() as usize);
            for entry in arr.iter() {
                let bytes = decode_uint8_entry(&entry)?;
                payloads.push(bytes);
            }
            Ok(MainToWorkerMessage::Sync { payloads })
        }
        "peer-open" => {
            let peer_id = read_string_field(value, "peerId")?;
            Ok(MainToWorkerMessage::PeerOpen { peer_id })
        }
        "peer-sync" => {
            let peer_id = read_string_field(value, "peerId")?;
            let term = read_u32_field(value, "term")?;
            let payload_arr = Reflect::get(value, &JsValue::from_str("payload"))
                .map_err(|_| "missing payload".to_string())?
                .dyn_into::<Array>()
                .map_err(|_| "peer-sync payload is not an array".to_string())?;
            let mut payloads = Vec::with_capacity(payload_arr.length() as usize);
            for entry in payload_arr.iter() {
                payloads.push(decode_uint8_entry(&entry)?);
            }
            Ok(MainToWorkerMessage::PeerSync {
                peer_id,
                term,
                payloads,
            })
        }
        "peer-close" => {
            let peer_id = read_string_field(value, "peerId")?;
            Ok(MainToWorkerMessage::PeerClose { peer_id })
        }
        "lifecycle-hint" => {
            let event_str = read_string_field(value, "event")?;
            let event = parse_lifecycle_event(&event_str)?;
            let sent_at_ms = Reflect::get(value, &JsValue::from_str("sentAtMs"))
                .map_err(|_| "missing sentAtMs".to_string())?
                .as_f64()
                .unwrap_or(0.0);
            Ok(MainToWorkerMessage::LifecycleHint { event, sent_at_ms })
        }
        "update-auth" => {
            let jwt_token = Reflect::get(value, &JsValue::from_str("jwtToken"))
                .ok()
                .and_then(|v| v.as_string());
            Ok(MainToWorkerMessage::UpdateAuth { jwt_token })
        }
        "disconnect-upstream" => Ok(MainToWorkerMessage::DisconnectUpstream),
        "reconnect-upstream" => Ok(MainToWorkerMessage::ReconnectUpstream),
        "shutdown" => Ok(MainToWorkerMessage::Shutdown),
        "acknowledge-rejected-batch" => {
            let batch_id = read_string_field(value, "batchId")?;
            Ok(MainToWorkerMessage::AcknowledgeRejectedBatch { batch_id })
        }
        "simulate-crash" => Ok(MainToWorkerMessage::SimulateCrash),
        "debug-schema-state" => Ok(MainToWorkerMessage::DebugSchemaState),
        "debug-seed-live-schema" => {
            let schema_json = read_string_field(value, "schemaJson")?;
            Ok(MainToWorkerMessage::DebugSeedLiveSchema { schema_json })
        }
        other => Ok(MainToWorkerMessage::Unknown(other.to_string())),
    }
}

fn parse_lifecycle_event(s: &str) -> Result<WorkerLifecycleEvent, String> {
    Ok(match s {
        "visibility-hidden" => WorkerLifecycleEvent::VisibilityHidden,
        "visibility-visible" => WorkerLifecycleEvent::VisibilityVisible,
        "pagehide" => WorkerLifecycleEvent::Pagehide,
        "freeze" => WorkerLifecycleEvent::Freeze,
        "resume" => WorkerLifecycleEvent::Resume,
        other => return Err(format!("unknown lifecycle event {other}")),
    })
}

fn read_string_field(value: &JsValue, name: &str) -> Result<String, String> {
    Reflect::get(value, &JsValue::from_str(name))
        .map_err(|_| format!("missing field {name}"))?
        .as_string()
        .ok_or_else(|| format!("field {name} is not a string"))
}

fn read_u32_field(value: &JsValue, name: &str) -> Result<u32, String> {
    let v = Reflect::get(value, &JsValue::from_str(name))
        .map_err(|_| format!("missing field {name}"))?;
    v.as_f64()
        .map(|f| f as u32)
        .ok_or_else(|| format!("field {name} is not a number"))
}

fn decode_uint8_entry(entry: &JsValue) -> Result<Vec<u8>, String> {
    if let Some(arr) = entry.dyn_ref::<Uint8Array>() {
        Ok(arr.to_vec())
    } else {
        Err("sync payload entry is not a Uint8Array".to_string())
    }
}

// =============================================================================
// Worker → Main message builders
// =============================================================================

/// `{type:"ready"}`
pub fn build_ready() -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"ready".into());
    obj.into()
}

/// `{type:"init-ok", clientId}`
pub fn build_init_ok(client_id: &str) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"init-ok".into());
    let _ = Reflect::set(&obj, &"clientId".into(), &JsValue::from_str(client_id));
    obj.into()
}

/// `{type:"upstream-connected"}`
pub fn build_upstream_connected() -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"upstream-connected".into());
    obj.into()
}

/// `{type:"upstream-disconnected"}`
pub fn build_upstream_disconnected() -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"upstream-disconnected".into());
    obj.into()
}

/// `{type:"shutdown-ok"}`
pub fn build_shutdown_ok() -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"shutdown-ok".into());
    obj.into()
}

/// `{type:"error", message}`
pub fn build_error(message: &str) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"error".into());
    let _ = Reflect::set(&obj, &"message".into(), &JsValue::from_str(message));
    obj.into()
}

/// `{type:"auth-failed", reason}`
pub fn build_auth_failed(reason: &str) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"auth-failed".into());
    let _ = Reflect::set(&obj, &"reason".into(), &JsValue::from_str(reason));
    obj.into()
}

/// `{type:"local-batch-records-sync", batches}` where `batches` is whatever
/// shape `runtime.loadLocalBatchRecords` returned (already a JS array).
pub fn build_local_batch_records_sync(batches: &JsValue) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"local-batch-records-sync".into());
    let _ = Reflect::set(&obj, &"batches".into(), batches);
    obj.into()
}

/// `{type:"mutation-error-replay", batch}`. `batch` is the JS-shaped
/// `LocalBatchRecord` from `runtime.loadLocalBatchRecord`.
pub fn build_mutation_error_replay(batch: &JsValue) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"mutation-error-replay".into());
    let _ = Reflect::set(&obj, &"batch".into(), batch);
    obj.into()
}

/// `{type:"debug-schema-state-ok", state}`
pub fn build_debug_schema_state_ok(state: &JsValue) -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"debug-schema-state-ok".into());
    let _ = Reflect::set(&obj, &"state".into(), state);
    obj.into()
}

/// `{type:"debug-seed-live-schema-ok"}`
pub fn build_debug_seed_live_schema_ok() -> JsValue {
    let obj = Object::new();
    let _ = Reflect::set(&obj, &"type".into(), &"debug-seed-live-schema-ok".into());
    obj.into()
}
