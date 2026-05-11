//! Wire protocol for the dedicated-worker bridge.
//!
//! The bridge speaks two distinct envelopes:
//!
//! * **JS objects** for the init/ready/error handshake — `runtimeSources` cannot
//!   ride on a postcard byte stream (it is a bundler-resolved JS value), so the
//!   `init` envelope and the worker-side `ready`/`error` pings stay as JS
//!   objects with a string `type` tag.
//! * **Postcard-encoded `Uint8Array`** for every other message after the
//!   handshake (one direction at a time, then the buffer is transferred so the
//!   browser detaches it without a copy).
//!
//! The wire-format invariants live here. Receivers are `parse_main_to_worker`
//! (worker side) and `parse_worker_to_main` (bridge side).

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[cfg(target_arch = "wasm32")]
use js_sys::{Array, Object, Reflect, Uint8Array};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;

// ---------------------------------------------------------------------------
// 6.2 — lifecycle hint enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerLifecycleEvent {
    VisibilityHidden,
    VisibilityVisible,
    Pagehide,
    Freeze,
    Resume,
}

/// Parse the kebab-case string the TS adapter forwards into the enum variant.
pub fn parse_lifecycle_event(s: &str) -> Option<WorkerLifecycleEvent> {
    match s {
        "visibility-hidden" => Some(WorkerLifecycleEvent::VisibilityHidden),
        "visibility-visible" => Some(WorkerLifecycleEvent::VisibilityVisible),
        "pagehide" => Some(WorkerLifecycleEvent::Pagehide),
        "freeze" => Some(WorkerLifecycleEvent::Freeze),
        "resume" => Some(WorkerLifecycleEvent::Resume),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 6.3 — init payload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitPayloadFields {
    pub schema_json: String,
    pub app_id: String,
    pub env: String,
    pub user_branch: String,
    pub db_name: String,
    #[serde(default)]
    pub client_id: String,
    pub server_url: Option<String>,
    pub jwt_token: Option<String>,
    pub admin_secret: Option<String>,
    pub fallback_wasm_url: Option<String>,
    pub log_level: Option<String>,
    pub telemetry_collector_url: Option<String>,
}

#[cfg(target_arch = "wasm32")]
pub struct InitPayload {
    pub fields: InitPayloadFields,
    pub runtime_sources: JsValue,
}

// ---------------------------------------------------------------------------
// 6.4 — heterogeneous sync entries (worker → main)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncEntry {
    BareBytes(ByteBuf),
    BareString(String),
    SequencedBytes { payload: ByteBuf, sequence: u64 },
    SequencedString { payload: String, sequence: u64 },
}

// ---------------------------------------------------------------------------
// 6.5 — main → worker wire enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

// ---------------------------------------------------------------------------
// 6.6 — worker → main wire enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    LocalBatchRecordsSync {
        batches_json: String,
    },
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
    DebugSchemaStateOk {
        state_json: String,
    },
    DebugSeedLiveSchemaOk,
}

// ---------------------------------------------------------------------------
// 6.7 — in-process Rust dispatch enum (worker host)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
pub enum MainToWorkerMessage {
    Init(Box<InitPayload>),
    Wire(MainToWorkerWire),
    Unknown(String),
}

#[cfg(target_arch = "wasm32")]
pub enum ParsedWorkerToMain {
    Ready,
    Wire(WorkerToMainWire),
    UnknownJsObject(String),
    DecodeError(String),
    Malformed,
}

// ---------------------------------------------------------------------------
// 6.8 — parse functions
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
fn js_value_type_string(value: &JsValue) -> Option<String> {
    if !value.is_object() || value.is_null() {
        return None;
    }
    let ty = Reflect::get(value, &JsValue::from_str("type")).ok()?;
    ty.as_string()
}

#[cfg(target_arch = "wasm32")]
fn is_uint8_array(value: &JsValue) -> bool {
    value.is_instance_of::<Uint8Array>()
}

#[cfg(target_arch = "wasm32")]
pub fn parse_main_to_worker(value: &JsValue) -> Result<MainToWorkerMessage, String> {
    if let Some(ty) = js_value_type_string(value) {
        if ty == "init" {
            let runtime_sources = Reflect::get(value, &JsValue::from_str("runtimeSources"))
                .unwrap_or(JsValue::UNDEFINED);
            let fields: InitPayloadFields = serde_wasm_bindgen::from_value(value.clone())
                .map_err(|e| format!("init parse error: {e}"))?;
            return Ok(MainToWorkerMessage::Init(Box::new(InitPayload {
                fields,
                runtime_sources,
            })));
        }
        return Ok(MainToWorkerMessage::Unknown(ty));
    }
    if is_uint8_array(value) {
        let arr = Uint8Array::from(value.clone());
        let bytes = arr.to_vec();
        let wire: MainToWorkerWire =
            postcard::from_bytes(&bytes).map_err(|e| format!("postcard decode: {e}"))?;
        return Ok(MainToWorkerMessage::Wire(wire));
    }
    Err("expected Uint8Array (binary) or `init` JS object".to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn parse_worker_to_main(value: &JsValue) -> ParsedWorkerToMain {
    if let Some(ty) = js_value_type_string(value) {
        match ty.as_str() {
            "ready" => return ParsedWorkerToMain::Ready,
            "error" => {
                let message = Reflect::get(value, &JsValue::from_str("message"))
                    .ok()
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                return ParsedWorkerToMain::Wire(WorkerToMainWire::Error { message });
            }
            other => return ParsedWorkerToMain::UnknownJsObject(other.to_string()),
        }
    }
    if is_uint8_array(value) {
        let arr = Uint8Array::from(value.clone());
        let bytes = arr.to_vec();
        return match postcard::from_bytes::<WorkerToMainWire>(&bytes) {
            Ok(w) => ParsedWorkerToMain::Wire(w),
            Err(e) => ParsedWorkerToMain::DecodeError(format!("postcard decode: {e}")),
        };
    }
    ParsedWorkerToMain::Malformed
}

// ---------------------------------------------------------------------------
// 6.9 — encode helpers
// ---------------------------------------------------------------------------

pub fn encode_main_to_worker(msg: &MainToWorkerWire) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(msg)
}

pub fn encode_worker_to_main(msg: &WorkerToMainWire) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(msg)
}

#[cfg(target_arch = "wasm32")]
pub fn encode_to_uint8array_with_transfer(bytes: &[u8]) -> (JsValue, Array) {
    let arr = Uint8Array::from(bytes);
    let transfer = Array::new();
    transfer.push(&arr.buffer().into());
    (arr.into(), transfer)
}

#[cfg(target_arch = "wasm32")]
pub fn main_to_worker_post(msg: &MainToWorkerWire) -> Result<(JsValue, Array), postcard::Error> {
    let bytes = encode_main_to_worker(msg)?;
    Ok(encode_to_uint8array_with_transfer(&bytes))
}

#[cfg(target_arch = "wasm32")]
pub fn worker_to_main_post(msg: &WorkerToMainWire) -> Result<(JsValue, Array), postcard::Error> {
    let bytes = encode_worker_to_main(msg)?;
    Ok(encode_to_uint8array_with_transfer(&bytes))
}

// ---------------------------------------------------------------------------
// 6.10 — JS-callable test helpers (encode/decode via kebab-case JS-object form)
// ---------------------------------------------------------------------------
//
// These mirror the legacy TS protocol shape — `{ type: "kebab-case", …fields }`.
// They only cover the variants used by browser tests; unsupported variants
// return Err so a typo surfaces loudly rather than silently producing an
// unexpected wire.

#[cfg(target_arch = "wasm32")]
fn js_set(obj: &JsValue, key: &str, value: &JsValue) {
    let _ = Reflect::set(obj, &JsValue::from_str(key), value);
}

#[cfg(target_arch = "wasm32")]
fn js_obj_with_type(ty: &str) -> Object {
    let obj = Object::new();
    js_set(obj.as_ref(), "type", &JsValue::from_str(ty));
    obj
}

#[cfg(target_arch = "wasm32")]
fn lifecycle_to_kebab(e: WorkerLifecycleEvent) -> &'static str {
    match e {
        WorkerLifecycleEvent::VisibilityHidden => "visibility-hidden",
        WorkerLifecycleEvent::VisibilityVisible => "visibility-visible",
        WorkerLifecycleEvent::Pagehide => "pagehide",
        WorkerLifecycleEvent::Freeze => "freeze",
        WorkerLifecycleEvent::Resume => "resume",
    }
}

#[cfg(target_arch = "wasm32")]
fn bytebuf_to_uint8array(buf: &ByteBuf) -> Uint8Array {
    Uint8Array::from(buf.as_ref())
}

#[cfg(target_arch = "wasm32")]
fn js_object_get_string(obj: &JsValue, key: &str) -> Option<String> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_string())
}

#[cfg(target_arch = "wasm32")]
fn js_object_get_f64(obj: &JsValue, key: &str) -> Option<f64> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_f64())
}

#[cfg(target_arch = "wasm32")]
fn js_object_get(obj: &JsValue, key: &str) -> Option<JsValue> {
    Reflect::get(obj, &JsValue::from_str(key)).ok()
}

#[cfg(target_arch = "wasm32")]
fn payload_array_to_byte_bufs(value: &JsValue) -> Result<Vec<ByteBuf>, JsError> {
    if value.is_undefined() || value.is_null() {
        return Ok(Vec::new());
    }
    let arr: &Array = value
        .dyn_ref::<Array>()
        .ok_or_else(|| JsError::new("payload must be an Array of Uint8Array"))?;
    let mut out = Vec::with_capacity(arr.length() as usize);
    for entry in arr.iter() {
        let bytes = entry
            .dyn_ref::<Uint8Array>()
            .ok_or_else(|| JsError::new("payload entry must be Uint8Array"))?
            .to_vec();
        out.push(ByteBuf::from(bytes));
    }
    Ok(out)
}

#[cfg(target_arch = "wasm32")]
fn parse_lifecycle_required(value: &JsValue) -> Result<WorkerLifecycleEvent, JsError> {
    let s = js_object_get_string(value, "event")
        .ok_or_else(|| JsError::new("lifecycle-hint: missing `event`"))?;
    parse_lifecycle_event(&s).ok_or_else(|| JsError::new("lifecycle-hint: unknown event"))
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = encodeMainToWorkerJs)]
pub fn encode_main_to_worker_js(value: JsValue) -> Result<Uint8Array, JsError> {
    let ty = js_object_get_string(&value, "type").ok_or_else(|| JsError::new("missing `type`"))?;
    let wire: MainToWorkerWire = match ty.as_str() {
        "sync" => {
            let payloads = payload_array_to_byte_bufs(
                &js_object_get(&value, "payload").unwrap_or(JsValue::UNDEFINED),
            )?;
            MainToWorkerWire::Sync { payloads }
        }
        "peer-open" => {
            let peer_id = js_object_get_string(&value, "peerId")
                .ok_or_else(|| JsError::new("peer-open: missing `peerId`"))?;
            MainToWorkerWire::PeerOpen { peer_id }
        }
        "peer-sync" => {
            let peer_id = js_object_get_string(&value, "peerId")
                .ok_or_else(|| JsError::new("peer-sync: missing `peerId`"))?;
            let term = js_object_get_f64(&value, "term")
                .ok_or_else(|| JsError::new("peer-sync: missing `term`"))?;
            let payloads = payload_array_to_byte_bufs(
                &js_object_get(&value, "payload").unwrap_or(JsValue::UNDEFINED),
            )?;
            MainToWorkerWire::PeerSync {
                peer_id,
                term: term as u32,
                payloads,
            }
        }
        "peer-close" => {
            let peer_id = js_object_get_string(&value, "peerId")
                .ok_or_else(|| JsError::new("peer-close: missing `peerId`"))?;
            MainToWorkerWire::PeerClose { peer_id }
        }
        "lifecycle-hint" => {
            let event = parse_lifecycle_required(&value)?;
            let sent_at_ms = js_object_get_f64(&value, "sentAtMs").unwrap_or(0.0);
            MainToWorkerWire::LifecycleHint { event, sent_at_ms }
        }
        "update-auth" => {
            let jwt_token = js_object_get_string(&value, "jwtToken");
            MainToWorkerWire::UpdateAuth { jwt_token }
        }
        "disconnect-upstream" => MainToWorkerWire::DisconnectUpstream,
        "reconnect-upstream" => MainToWorkerWire::ReconnectUpstream,
        "shutdown" => MainToWorkerWire::Shutdown,
        "simulate-crash" => MainToWorkerWire::SimulateCrash,
        "acknowledge-rejected-batch" => {
            let batch_id = js_object_get_string(&value, "batchId")
                .ok_or_else(|| JsError::new("acknowledge-rejected-batch: missing `batchId`"))?;
            MainToWorkerWire::AcknowledgeRejectedBatch { batch_id }
        }
        "debug-schema-state" => MainToWorkerWire::DebugSchemaState,
        "debug-seed-live-schema" => {
            let schema_json = js_object_get_string(&value, "schemaJson")
                .ok_or_else(|| JsError::new("debug-seed-live-schema: missing `schemaJson`"))?;
            MainToWorkerWire::DebugSeedLiveSchema { schema_json }
        }
        other => {
            return Err(JsError::new(&format!(
                "unsupported main→worker type `{other}`"
            )))
        }
    };
    let bytes =
        encode_main_to_worker(&wire).map_err(|e| JsError::new(&format!("postcard encode: {e}")))?;
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[cfg(target_arch = "wasm32")]
fn sync_entry_to_js(entry: &SyncEntry) -> JsValue {
    let obj = Object::new();
    let obj_ref: &JsValue = obj.as_ref();
    match entry {
        SyncEntry::BareBytes(b) => {
            js_set(obj_ref, "payload", &bytebuf_to_uint8array(b).into());
        }
        SyncEntry::BareString(s) => {
            js_set(obj_ref, "payload", &JsValue::from_str(s));
        }
        SyncEntry::SequencedBytes { payload, sequence } => {
            js_set(obj_ref, "payload", &bytebuf_to_uint8array(payload).into());
            js_set(obj_ref, "sequence", &JsValue::from_f64(*sequence as f64));
        }
        SyncEntry::SequencedString { payload, sequence } => {
            js_set(obj_ref, "payload", &JsValue::from_str(payload));
            js_set(obj_ref, "sequence", &JsValue::from_f64(*sequence as f64));
        }
    }
    obj.into()
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = encodeWorkerToMainJs)]
pub fn encode_worker_to_main_js(value: JsValue) -> Result<Uint8Array, JsError> {
    let ty = js_object_get_string(&value, "type").ok_or_else(|| JsError::new("missing `type`"))?;
    let wire: WorkerToMainWire = match ty.as_str() {
        "init-ok" => WorkerToMainWire::InitOk {
            client_id: js_object_get_string(&value, "clientId")
                .ok_or_else(|| JsError::new("init-ok: missing `clientId`"))?,
        },
        "upstream-connected" => WorkerToMainWire::UpstreamConnected,
        "upstream-disconnected" => WorkerToMainWire::UpstreamDisconnected,
        "shutdown-ok" => WorkerToMainWire::ShutdownOk,
        "error" => WorkerToMainWire::Error {
            message: js_object_get_string(&value, "message").unwrap_or_default(),
        },
        "auth-failed" => WorkerToMainWire::AuthFailed {
            reason: js_object_get_string(&value, "reason").unwrap_or_default(),
        },
        "peer-sync" => WorkerToMainWire::PeerSync {
            peer_id: js_object_get_string(&value, "peerId")
                .ok_or_else(|| JsError::new("peer-sync: missing `peerId`"))?,
            term: js_object_get_f64(&value, "term")
                .ok_or_else(|| JsError::new("peer-sync: missing `term`"))? as u32,
            payloads: payload_array_to_byte_bufs(
                &js_object_get(&value, "payload").unwrap_or(JsValue::UNDEFINED),
            )?,
        },
        "local-batch-records-sync" => WorkerToMainWire::LocalBatchRecordsSync {
            batches_json: js_object_get_string(&value, "batchesJson").unwrap_or_default(),
        },
        "mutation-error-replay" => WorkerToMainWire::MutationErrorReplay {
            batch_json: js_object_get_string(&value, "batchJson").unwrap_or_default(),
        },
        "debug-schema-state-ok" => WorkerToMainWire::DebugSchemaStateOk {
            state_json: js_object_get_string(&value, "stateJson").unwrap_or_default(),
        },
        "debug-seed-live-schema-ok" => WorkerToMainWire::DebugSeedLiveSchemaOk,
        other => {
            return Err(JsError::new(&format!(
                "unsupported worker→main type `{other}`"
            )))
        }
    };
    let bytes =
        encode_worker_to_main(&wire).map_err(|e| JsError::new(&format!("postcard encode: {e}")))?;
    Ok(Uint8Array::from(bytes.as_slice()))
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = decodeMainToWorkerJs)]
pub fn decode_main_to_worker_js(bytes: &Uint8Array) -> Result<JsValue, JsValue> {
    let vec = bytes.to_vec();
    let wire: MainToWorkerWire = postcard::from_bytes(&vec)
        .map_err(|e| JsValue::from_str(&format!("postcard decode: {e}")))?;
    let obj = match wire {
        MainToWorkerWire::Sync { payloads } => {
            let obj = js_obj_with_type("sync");
            let arr = Array::new();
            for b in &payloads {
                arr.push(&bytebuf_to_uint8array(b).into());
            }
            Reflect::set(&obj, &JsValue::from_str("payload"), &arr.into())?;
            obj
        }
        MainToWorkerWire::PeerOpen { peer_id } => {
            let obj = js_obj_with_type("peer-open");
            Reflect::set(
                &obj,
                &JsValue::from_str("peerId"),
                &JsValue::from_str(&peer_id),
            )?;
            obj
        }
        MainToWorkerWire::PeerSync {
            peer_id,
            term,
            payloads,
        } => {
            let obj = js_obj_with_type("peer-sync");
            Reflect::set(
                &obj,
                &JsValue::from_str("peerId"),
                &JsValue::from_str(&peer_id),
            )?;
            Reflect::set(
                &obj,
                &JsValue::from_str("term"),
                &JsValue::from_f64(term as f64),
            )?;
            let arr = Array::new();
            for b in &payloads {
                arr.push(&bytebuf_to_uint8array(b).into());
            }
            Reflect::set(&obj, &JsValue::from_str("payload"), &arr.into())?;
            obj
        }
        MainToWorkerWire::PeerClose { peer_id } => {
            let obj = js_obj_with_type("peer-close");
            Reflect::set(
                &obj,
                &JsValue::from_str("peerId"),
                &JsValue::from_str(&peer_id),
            )?;
            obj
        }
        MainToWorkerWire::LifecycleHint { event, sent_at_ms } => {
            let obj = js_obj_with_type("lifecycle-hint");
            Reflect::set(
                &obj,
                &JsValue::from_str("event"),
                &JsValue::from_str(lifecycle_to_kebab(event)),
            )?;
            Reflect::set(
                &obj,
                &JsValue::from_str("sentAtMs"),
                &JsValue::from_f64(sent_at_ms),
            )?;
            obj
        }
        MainToWorkerWire::UpdateAuth { jwt_token } => {
            let obj = js_obj_with_type("update-auth");
            if let Some(t) = jwt_token {
                Reflect::set(&obj, &JsValue::from_str("jwtToken"), &JsValue::from_str(&t))?;
            }
            obj
        }
        MainToWorkerWire::DisconnectUpstream => js_obj_with_type("disconnect-upstream"),
        MainToWorkerWire::ReconnectUpstream => js_obj_with_type("reconnect-upstream"),
        MainToWorkerWire::Shutdown => js_obj_with_type("shutdown"),
        MainToWorkerWire::SimulateCrash => js_obj_with_type("simulate-crash"),
        MainToWorkerWire::AcknowledgeRejectedBatch { batch_id } => {
            let obj = js_obj_with_type("acknowledge-rejected-batch");
            Reflect::set(
                &obj,
                &JsValue::from_str("batchId"),
                &JsValue::from_str(&batch_id),
            )?;
            obj
        }
        MainToWorkerWire::DebugSchemaState => js_obj_with_type("debug-schema-state"),
        MainToWorkerWire::DebugSeedLiveSchema { schema_json } => {
            let obj = js_obj_with_type("debug-seed-live-schema");
            Reflect::set(
                &obj,
                &JsValue::from_str("schemaJson"),
                &JsValue::from_str(&schema_json),
            )?;
            obj
        }
    };
    Ok(obj.into())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = decodeWorkerToMainJs)]
pub fn decode_worker_to_main_js(bytes: &Uint8Array) -> Result<JsValue, JsValue> {
    let vec = bytes.to_vec();
    let wire: WorkerToMainWire = postcard::from_bytes(&vec)
        .map_err(|e| JsValue::from_str(&format!("postcard decode: {e}")))?;
    let obj = match wire {
        WorkerToMainWire::InitOk { client_id } => {
            let obj = js_obj_with_type("init-ok");
            Reflect::set(
                &obj,
                &JsValue::from_str("clientId"),
                &JsValue::from_str(&client_id),
            )?;
            obj
        }
        WorkerToMainWire::UpstreamConnected => js_obj_with_type("upstream-connected"),
        WorkerToMainWire::UpstreamDisconnected => js_obj_with_type("upstream-disconnected"),
        WorkerToMainWire::ShutdownOk => js_obj_with_type("shutdown-ok"),
        WorkerToMainWire::Error { message } => {
            let obj = js_obj_with_type("error");
            Reflect::set(
                &obj,
                &JsValue::from_str("message"),
                &JsValue::from_str(&message),
            )?;
            obj
        }
        WorkerToMainWire::AuthFailed { reason } => {
            let obj = js_obj_with_type("auth-failed");
            Reflect::set(
                &obj,
                &JsValue::from_str("reason"),
                &JsValue::from_str(&reason),
            )?;
            obj
        }
        WorkerToMainWire::Sync { payloads } => {
            let obj = js_obj_with_type("sync");
            let arr = Array::new();
            for entry in &payloads {
                arr.push(&sync_entry_to_js(entry));
            }
            Reflect::set(&obj, &JsValue::from_str("payload"), &arr.into())?;
            obj
        }
        WorkerToMainWire::PeerSync {
            peer_id,
            term,
            payloads,
        } => {
            let obj = js_obj_with_type("peer-sync");
            Reflect::set(
                &obj,
                &JsValue::from_str("peerId"),
                &JsValue::from_str(&peer_id),
            )?;
            Reflect::set(
                &obj,
                &JsValue::from_str("term"),
                &JsValue::from_f64(term as f64),
            )?;
            let arr = Array::new();
            for b in &payloads {
                arr.push(&bytebuf_to_uint8array(b).into());
            }
            Reflect::set(&obj, &JsValue::from_str("payload"), &arr.into())?;
            obj
        }
        WorkerToMainWire::LocalBatchRecordsSync { batches_json } => {
            let obj = js_obj_with_type("local-batch-records-sync");
            Reflect::set(
                &obj,
                &JsValue::from_str("batchesJson"),
                &JsValue::from_str(&batches_json),
            )?;
            obj
        }
        WorkerToMainWire::MutationErrorReplay { batch_json } => {
            let obj = js_obj_with_type("mutation-error-replay");
            Reflect::set(
                &obj,
                &JsValue::from_str("batchJson"),
                &JsValue::from_str(&batch_json),
            )?;
            obj
        }
        WorkerToMainWire::DebugSchemaStateOk { state_json } => {
            let obj = js_obj_with_type("debug-schema-state-ok");
            Reflect::set(
                &obj,
                &JsValue::from_str("stateJson"),
                &JsValue::from_str(&state_json),
            )?;
            obj
        }
        WorkerToMainWire::DebugSeedLiveSchemaOk => js_obj_with_type("debug-seed-live-schema-ok"),
    };
    Ok(obj.into())
}

// ---------------------------------------------------------------------------
// 6.11 — in-source round-trip tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn rt_main(msg: &MainToWorkerWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode main→worker");
        let decoded: MainToWorkerWire = postcard::from_bytes(&bytes).expect("decode main→worker");
        assert_eq!(format!("{msg:?}"), format!("{decoded:?}"));
    }

    fn rt_worker(msg: &WorkerToMainWire) {
        let bytes = postcard::to_allocvec(msg).expect("encode worker→main");
        let decoded: WorkerToMainWire = postcard::from_bytes(&bytes).expect("decode worker→main");
        assert_eq!(format!("{msg:?}"), format!("{decoded:?}"));
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
        rt_main(&MainToWorkerWire::SimulateCrash);
        rt_main(&MainToWorkerWire::DebugSchemaState);
        rt_main(&MainToWorkerWire::AcknowledgeRejectedBatch {
            batch_id: "b1".into(),
        });
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
                SyncEntry::BareBytes(ByteBuf::from(vec![1, 2, 3])),
                SyncEntry::BareString("hello".into()),
                SyncEntry::SequencedBytes {
                    payload: ByteBuf::from(vec![9]),
                    sequence: 1,
                },
                SyncEntry::SequencedString {
                    payload: "world".into(),
                    sequence: 2,
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
