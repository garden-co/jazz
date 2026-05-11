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
use js_sys::{Array, Reflect, Uint8Array};
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
        /// JSON-encoded array of batch records (`batchId`, `mode`, `sealed`,
        /// `latestSettlement`). Parsed and handed to the main-thread listener.
        batches_json: String,
        /// Parallel to the JSON array: optional encoded storage rows
        /// (`LocalBatchRecord::encode_storage_row`). The main bridge attaches
        /// each present entry as a `Uint8Array` on the matching JS object so
        /// the main runtime can hydrate the optimistic row on restart.
        encoded_records: Vec<Option<ByteBuf>>,
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
    /// JS-object form of a wire command — accepted only for a small set of
    /// commands that browser test harnesses send directly via `postMessage`
    /// (bypassing the production bridge). The dispatcher uses this to also
    /// emit the response in JS-object form so the harness's `event.data.type`
    /// listener wakes up.
    WireJsObject(MainToWorkerWire),
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
        if let Some(wire) = parse_js_object_test_command(value, &ty)? {
            return Ok(MainToWorkerMessage::WireJsObject(wire));
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

/// Recognise the JS-object form of the small set of commands that browser
/// test harnesses send directly via `worker.postMessage({type: ...})`.
/// Production code always sends binary postcard.
#[cfg(target_arch = "wasm32")]
fn parse_js_object_test_command(
    value: &JsValue,
    ty: &str,
) -> Result<Option<MainToWorkerWire>, String> {
    match ty {
        "simulate-crash" => Ok(Some(MainToWorkerWire::SimulateCrash)),
        "shutdown" => Ok(Some(MainToWorkerWire::Shutdown)),
        "debug-schema-state" => Ok(Some(MainToWorkerWire::DebugSchemaState)),
        "debug-seed-live-schema" => {
            let schema_json = Reflect::get(value, &JsValue::from_str("schemaJson"))
                .ok()
                .and_then(|v| v.as_string())
                .ok_or_else(|| "debug-seed-live-schema: missing schemaJson".to_string())?;
            Ok(Some(MainToWorkerWire::DebugSeedLiveSchema { schema_json }))
        }
        _ => Ok(None),
    }
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
fn js_set_in(obj: &js_sys::Object, key: &str, value: &JsValue) {
    let _ = js_sys::Reflect::set(obj.as_ref(), &JsValue::from_str(key), value);
}

/// Decode a postcard-encoded `MainToWorkerWire` payload back into a JS object
/// of the shape `{ type: "kebab-case", ...fields }`. Exposed as a test helper
/// for harnesses that intercept the bridge's outbound `Uint8Array` traffic and
/// need to assert against the original wire variant (the production protocol
/// is binary-only after `init`).
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = decodeMainToWorkerJs)]
pub fn decode_main_to_worker_js(bytes: &Uint8Array) -> Result<JsValue, JsValue> {
    let vec = bytes.to_vec();
    let wire: MainToWorkerWire = postcard::from_bytes(&vec)
        .map_err(|e| JsValue::from_str(&format!("postcard decode: {e}")))?;
    let obj = js_sys::Object::new();
    let payload_for_arr = |bufs: &[ByteBuf]| -> JsValue {
        let arr = Array::new();
        for b in bufs {
            arr.push(&Uint8Array::from(b.as_ref()).into());
        }
        arr.into()
    };
    match wire {
        MainToWorkerWire::Sync { payloads } => {
            js_set_in(&obj, "type", &JsValue::from_str("sync"));
            js_set_in(&obj, "payload", &payload_for_arr(&payloads));
        }
        MainToWorkerWire::PeerOpen { peer_id } => {
            js_set_in(&obj, "type", &JsValue::from_str("peer-open"));
            js_set_in(&obj, "peerId", &JsValue::from_str(&peer_id));
        }
        MainToWorkerWire::PeerSync {
            peer_id,
            term,
            payloads,
        } => {
            js_set_in(&obj, "type", &JsValue::from_str("peer-sync"));
            js_set_in(&obj, "peerId", &JsValue::from_str(&peer_id));
            js_set_in(&obj, "term", &JsValue::from_f64(term as f64));
            js_set_in(&obj, "payload", &payload_for_arr(&payloads));
        }
        MainToWorkerWire::PeerClose { peer_id } => {
            js_set_in(&obj, "type", &JsValue::from_str("peer-close"));
            js_set_in(&obj, "peerId", &JsValue::from_str(&peer_id));
        }
        MainToWorkerWire::LifecycleHint { event, sent_at_ms } => {
            js_set_in(&obj, "type", &JsValue::from_str("lifecycle-hint"));
            let kebab = match event {
                WorkerLifecycleEvent::VisibilityHidden => "visibility-hidden",
                WorkerLifecycleEvent::VisibilityVisible => "visibility-visible",
                WorkerLifecycleEvent::Pagehide => "pagehide",
                WorkerLifecycleEvent::Freeze => "freeze",
                WorkerLifecycleEvent::Resume => "resume",
            };
            js_set_in(&obj, "event", &JsValue::from_str(kebab));
            js_set_in(&obj, "sentAtMs", &JsValue::from_f64(sent_at_ms));
        }
        MainToWorkerWire::UpdateAuth { jwt_token } => {
            js_set_in(&obj, "type", &JsValue::from_str("update-auth"));
            if let Some(t) = jwt_token {
                js_set_in(&obj, "jwtToken", &JsValue::from_str(&t));
            }
        }
        MainToWorkerWire::DisconnectUpstream => {
            js_set_in(&obj, "type", &JsValue::from_str("disconnect-upstream"));
        }
        MainToWorkerWire::ReconnectUpstream => {
            js_set_in(&obj, "type", &JsValue::from_str("reconnect-upstream"));
        }
        MainToWorkerWire::Shutdown => {
            js_set_in(&obj, "type", &JsValue::from_str("shutdown"));
        }
        MainToWorkerWire::SimulateCrash => {
            js_set_in(&obj, "type", &JsValue::from_str("simulate-crash"));
        }
        MainToWorkerWire::AcknowledgeRejectedBatch { batch_id } => {
            js_set_in(
                &obj,
                "type",
                &JsValue::from_str("acknowledge-rejected-batch"),
            );
            js_set_in(&obj, "batchId", &JsValue::from_str(&batch_id));
        }
        MainToWorkerWire::DebugSchemaState => {
            js_set_in(&obj, "type", &JsValue::from_str("debug-schema-state"));
        }
        MainToWorkerWire::DebugSeedLiveSchema { schema_json } => {
            js_set_in(&obj, "type", &JsValue::from_str("debug-seed-live-schema"));
            js_set_in(&obj, "schemaJson", &JsValue::from_str(&schema_json));
        }
    }
    Ok(obj.into())
}

#[cfg(target_arch = "wasm32")]
pub fn worker_to_main_post(msg: &WorkerToMainWire) -> Result<(JsValue, Array), postcard::Error> {
    let bytes = encode_worker_to_main(msg)?;
    Ok(encode_to_uint8array_with_transfer(&bytes))
}

// ---------------------------------------------------------------------------
// 6.10 — in-source round-trip tests
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
            encoded_records: vec![],
        });
        rt_worker(&WorkerToMainWire::LocalBatchRecordsSync {
            batches_json:
                r#"[{"batchId":"a","mode":"buffered","sealed":false,"latestSettlement":null}]"#
                    .into(),
            encoded_records: vec![Some(ByteBuf::from(vec![1, 2, 3]))],
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
