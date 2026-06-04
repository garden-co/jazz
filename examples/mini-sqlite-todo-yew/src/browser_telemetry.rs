use crate::native_sync::{NativeSyncProbe, NativeTraceContext};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const TRACE_SCOPE_NAME: &str = "mini_sqlite_todo_yew::browser_telemetry";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserTelemetryConfig {
    pub endpoint: String,
    pub service_name: String,
    pub service_version: String,
    pub browser_instance_id: String,
    pub deployment_environment: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
}

impl SpanKind {
    fn otlp_value(self) -> i64 {
        match self {
            Self::Internal => 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrowserTelemetrySpan {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    kind: SpanKind,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: BTreeMap<String, String>,
}

impl BrowserTelemetrySpan {
    pub fn from_trace_context<const N: usize>(
        name: &str,
        kind: SpanKind,
        start_time_unix_nano: u64,
        end_time_unix_nano: u64,
        trace_context: &NativeTraceContext,
        attributes: [(&str, &str); N],
    ) -> Option<Self> {
        let parsed = ParsedTraceparent::parse(&trace_context.traceparent)?;
        let mut span_attributes = BTreeMap::new();
        if let Some(probe) = &trace_context.probe {
            record_probe_attributes(&mut span_attributes, probe);
        }
        for (key, value) in attributes {
            span_attributes.insert(key.to_owned(), value.to_owned());
        }

        Some(Self {
            trace_id: parsed.trace_id,
            span_id: parsed.span_id,
            parent_span_id: parsed.parent_span_id,
            name: name.to_owned(),
            kind,
            start_time_unix_nano,
            end_time_unix_nano,
            attributes: span_attributes,
        })
    }
}

pub fn otlp_trace_payload(config: &BrowserTelemetryConfig, span: BrowserTelemetrySpan) -> Value {
    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    string_attribute("service.name", &config.service_name),
                    string_attribute("service.version", &config.service_version),
                    string_attribute("deployment.environment.name", &config.deployment_environment),
                    string_attribute("service.instance.id", &config.browser_instance_id),
                    string_attribute("browser.instance.id", &config.browser_instance_id),
                ]
            },
            "scopeSpans": [{
                "scope": {
                    "name": TRACE_SCOPE_NAME,
                },
                "spans": [{
                    "traceId": span.trace_id,
                    "spanId": span.span_id,
                    "parentSpanId": span.parent_span_id,
                    "name": span.name,
                    "kind": span.kind.otlp_value(),
                    "startTimeUnixNano": span.start_time_unix_nano.to_string(),
                    "endTimeUnixNano": span.end_time_unix_nano.to_string(),
                    "attributes": span.attributes
                        .iter()
                        .map(|(key, value)| string_attribute(key, value))
                        .collect::<Vec<_>>(),
                }]
            }]
        }]
    })
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

struct ParsedTraceparent {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
}

impl ParsedTraceparent {
    fn parse(traceparent: &str) -> Option<Self> {
        let mut parts = traceparent.split('-');
        let version = parts.next()?;
        let trace_id = parts.next()?;
        let parent_span_id = parts.next()?;
        let trace_flags = parts.next()?;
        if parts.next().is_some()
            || version != "00"
            || trace_id.len() != 32
            || parent_span_id.len() != 16
            || trace_flags.len() != 2
            || trace_id == "00000000000000000000000000000000"
            || parent_span_id == "0000000000000000"
            || !trace_id.chars().all(|value| value.is_ascii_hexdigit())
            || !parent_span_id
                .chars()
                .all(|value| value.is_ascii_hexdigit())
            || !trace_flags.chars().all(|value| value.is_ascii_hexdigit())
        {
            return None;
        }

        Some(Self {
            trace_id: trace_id.to_owned(),
            span_id: new_span_id(),
            parent_span_id: parent_span_id.to_owned(),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn new_span_id() -> String {
    "0000000000000001".to_owned()
}

#[cfg(target_arch = "wasm32")]
fn new_span_id() -> String {
    let random = (js_sys::Math::random() * u64::MAX as f64) as u64;
    let span_id = format!("{random:016x}");
    if span_id == "0000000000000000" {
        "0000000000000001".to_owned()
    } else {
        span_id
    }
}

#[cfg(target_arch = "wasm32")]
pub fn unix_nano_now() -> u64 {
    (js_sys::Date::now() * 1_000_000.0) as u64
}

#[cfg(target_arch = "wasm32")]
pub fn new_trace_context(probe: NativeSyncProbe) -> NativeTraceContext {
    NativeTraceContext {
        traceparent: format!("00-{}-{}-01", new_trace_id(), new_span_id()),
        probe: Some(probe),
    }
}

#[cfg(target_arch = "wasm32")]
fn new_trace_id() -> String {
    let high = js_sys::Date::now() as u64;
    let low = (js_sys::Math::random() * u64::MAX as f64) as u64;
    let trace_id = format!("{high:016x}{low:016x}");
    if trace_id == "00000000000000000000000000000000" {
        "00000000000000000000000000000001".to_owned()
    } else {
        trace_id
    }
}

#[cfg(target_arch = "wasm32")]
pub fn emit_span<const N: usize>(
    config: Option<&BrowserTelemetryConfig>,
    name: &str,
    trace_context: Option<&NativeTraceContext>,
    attributes: [(&str, &str); N],
) {
    let Some(config) = config else {
        return;
    };
    let Some(trace_context) = trace_context else {
        return;
    };
    let start = unix_nano_now();
    let Some(span) = BrowserTelemetrySpan::from_trace_context(
        name,
        SpanKind::Internal,
        start,
        start.saturating_add(1_000_000),
        trace_context,
        attributes,
    ) else {
        return;
    };
    send_otlp_trace(config, span);
}

#[cfg(target_arch = "wasm32")]
fn send_otlp_trace(config: &BrowserTelemetryConfig, span: BrowserTelemetrySpan) {
    use js_sys::{Function, Object, Reflect};
    use wasm_bindgen::{JsCast, JsValue};

    let payload = otlp_trace_payload(config, span).to_string();
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
    let Ok(fetch) = fetch.dyn_into::<Function>() else {
        return;
    };
    let _ = fetch.call2(&global, &JsValue::from_str(&trace_endpoint(config)), &init);
}

#[cfg(target_arch = "wasm32")]
fn trace_endpoint(config: &BrowserTelemetryConfig) -> String {
    let endpoint = config.endpoint.trim().trim_end_matches('/');
    if endpoint.ends_with("/v1/traces") {
        endpoint.to_owned()
    } else if let Some(base) = endpoint.strip_suffix("/v1/logs") {
        format!("{base}/v1/traces")
    } else {
        format!("{endpoint}/v1/traces")
    }
}
