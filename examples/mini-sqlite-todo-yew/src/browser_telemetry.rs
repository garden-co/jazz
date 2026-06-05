use crate::native_sync::{NativeSyncLogContext, NativeSyncProbe, SyncLogRecord};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const LOG_SCOPE_NAME: &str = "mini_sqlite_todo_yew::browser_telemetry";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTelemetryConfig {
    pub endpoint: String,
    pub service_name: String,
    pub service_version: String,
    pub browser_instance_id: String,
    pub deployment_environment: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowserTelemetryLog {
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
    event_name: String,
    body: String,
    attributes: BTreeMap<String, String>,
}

impl BrowserTelemetryLog {
    pub fn new<const N: usize>(
        event_name: &str,
        time_unix_nano: u64,
        sync_context: Option<&NativeSyncLogContext>,
        body: &str,
        attributes: [(&str, &str); N],
    ) -> Self {
        let mut log_attributes = BTreeMap::new();
        record_context_attributes(&mut log_attributes, sync_context);
        for (key, value) in attributes {
            log_attributes.insert(key.to_owned(), value.to_owned());
        }

        Self {
            time_unix_nano,
            observed_time_unix_nano: time_unix_nano,
            event_name: event_name.to_owned(),
            body: body.to_owned(),
            attributes: log_attributes,
        }
    }

    pub fn from_sync_record(time_unix_nano: u64, record: &SyncLogRecord) -> Self {
        Self {
            time_unix_nano,
            observed_time_unix_nano: time_unix_nano,
            event_name: record.event_name.to_owned(),
            body: record.body.clone(),
            attributes: record.attributes.clone(),
        }
    }
}

pub fn otlp_log_payload(config: &BrowserTelemetryConfig, log: BrowserTelemetryLog) -> Value {
    json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    string_attribute("service.name", &config.service_name),
                    string_attribute("service.version", &config.service_version),
                    string_attribute("deployment.environment.name", &config.deployment_environment),
                    string_attribute("service.instance.id", &config.browser_instance_id),
                    string_attribute("browser.instance.id", &config.browser_instance_id),
                ]
            },
            "scopeLogs": [{
                "scope": {
                    "name": LOG_SCOPE_NAME,
                },
                "logRecords": [{
                    "timeUnixNano": log.time_unix_nano.to_string(),
                    "observedTimeUnixNano": log.observed_time_unix_nano.to_string(),
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "eventName": log.event_name,
                    "body": {
                        "stringValue": log.body,
                    },
                    "attributes": log.attributes
                        .iter()
                        .map(|(key, value)| string_attribute(key, value))
                        .collect::<Vec<_>>(),
                }]
            }]
        }]
    })
}

fn record_context_attributes(
    attributes: &mut BTreeMap<String, String>,
    sync_context: Option<&NativeSyncLogContext>,
) {
    let Some(sync_context) = sync_context else {
        return;
    };
    if let Some(session_id) = &sync_context.session_id {
        attributes.insert("sync.session_id".to_owned(), session_id.clone());
    }
    if let Some(probe) = &sync_context.probe {
        record_probe_attributes(attributes, probe);
    }
}

fn record_probe_attributes(attributes: &mut BTreeMap<String, String>, probe: &NativeSyncProbe) {
    attributes.insert("sync.probe.id".to_owned(), probe.probe_id.clone());
    attributes.insert("sync.operation".to_owned(), probe.operation.clone());
    attributes.insert("sync.table".to_owned(), probe.table.clone());
    attributes.insert("sync.row_id".to_owned(), probe.row_id.clone());
    attributes.insert(
        "sync.origin_browser_id".to_owned(),
        probe.origin_browser_id.clone(),
    );
}

fn string_attribute(key: &str, value: &str) -> Value {
    json!({
        "key": key,
        "value": {
            "stringValue": value,
        },
    })
}

pub fn new_sync_log_context(probe: NativeSyncProbe) -> NativeSyncLogContext {
    NativeSyncLogContext {
        session_id: None,
        probe: Some(probe),
    }
}

#[cfg(target_arch = "wasm32")]
pub fn unix_nano_now() -> u64 {
    (js_sys::Date::now() * 1_000_000.0) as u64
}

#[cfg(target_arch = "wasm32")]
pub fn emit_log<const N: usize>(
    config: Option<&BrowserTelemetryConfig>,
    event_name: &str,
    sync_context: Option<&NativeSyncLogContext>,
    body: &str,
    attributes: [(&str, &str); N],
) {
    let Some(config) = config else {
        return;
    };
    let log = BrowserTelemetryLog::new(event_name, unix_nano_now(), sync_context, body, attributes);
    send_otlp_log(config, log);
}

#[cfg(target_arch = "wasm32")]
pub fn emit_sync_log_records(config: Option<&BrowserTelemetryConfig>, records: &[SyncLogRecord]) {
    let Some(config) = config else {
        return;
    };
    for record in records {
        send_otlp_log(
            config,
            BrowserTelemetryLog::from_sync_record(unix_nano_now(), record),
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn emit_log<const N: usize>(
    _config: Option<&BrowserTelemetryConfig>,
    _event_name: &str,
    _sync_context: Option<&NativeSyncLogContext>,
    _body: &str,
    _attributes: [(&str, &str); N],
) {
}

#[cfg(not(target_arch = "wasm32"))]
pub fn emit_sync_log_records(_config: Option<&BrowserTelemetryConfig>, _records: &[SyncLogRecord]) {
}

#[cfg(target_arch = "wasm32")]
fn send_otlp_log(config: &BrowserTelemetryConfig, log: BrowserTelemetryLog) {
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::{JsCast, JsValue};

    let payload = otlp_log_payload(config, log).to_string();
    let init = Object::new();
    let headers = Object::new();
    let _ = Reflect::set(
        &headers,
        &JsValue::from_str("content-type"),
        &JsValue::from_str("application/json"),
    );
    let _ = Reflect::set(
        &init,
        &JsValue::from_str("method"),
        &JsValue::from_str("POST"),
    );
    let _ = Reflect::set(&init, &JsValue::from_str("headers"), &headers);
    let _ = Reflect::set(
        &init,
        &JsValue::from_str("body"),
        &JsValue::from_str(&payload),
    );

    let global = js_sys::global();
    let Ok(fetch) = Reflect::get(&global, &JsValue::from_str("fetch")) else {
        return;
    };
    let Some(fetch) = fetch.dyn_ref::<Function>() else {
        return;
    };
    let _ = fetch.call2(
        &global,
        &JsValue::from_str(&logs_endpoint(&config.endpoint)),
        &init,
    );
}

#[cfg(target_arch = "wasm32")]
fn logs_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.ends_with("/v1/logs") {
        endpoint.to_owned()
    } else if let Some(base) = endpoint.strip_suffix("/v1/traces") {
        format!("{base}/v1/logs")
    } else {
        format!("{endpoint}/v1/logs")
    }
}
