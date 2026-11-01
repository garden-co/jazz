//! Stateless anchors and ranges into text values (`jazz-text`), for editors
//! and presence. Values cross as plain objects: `{ version?, position?,
//! side, before, after }` and `{ version?, start?, end?, before, matched,
//! after }`. Versions are optional: a resolve takes the position as is only
//! when the anchor's version is the value's.

use jazz_text::{Anchor, Range, Side};
use napi_derive::napi;
use serde_json::Value as JsonValue;

fn from_json<T: serde::de::DeserializeOwned>(value: JsonValue) -> napi::Result<T> {
    serde_json::from_value(value).map_err(|error| napi::Error::from_reason(error.to_string()))
}

fn to_json(value: &impl serde::Serialize) -> napi::Result<JsonValue> {
    serde_json::to_value(value).map_err(|error| napi::Error::from_reason(error.to_string()))
}

/// The anchor at `position` (UTF-16 code units) of `text`.
#[napi(js_name = "captureTextAnchor")]
pub fn capture_text_anchor(
    text: String,
    version: Option<f64>,
    position: u32,
    side: JsonValue,
) -> napi::Result<JsonValue> {
    let side: Side = from_json(side)?;
    to_json(&Anchor::capture(
        &text,
        version.map(|v| v as u64),
        position as usize,
        side,
    ))
}

/// Where `anchor` lands in `text`: `{ position, confidence, ambiguous }`.
#[napi(js_name = "resolveTextAnchor")]
pub fn resolve_text_anchor(
    anchor: JsonValue,
    text: String,
    version: Option<f64>,
) -> napi::Result<JsonValue> {
    let anchor: Anchor = from_json(anchor)?;
    to_json(&anchor.resolve(&text, version.map(|v| v as u64)))
}

/// The range `start..end` (UTF-16 code units) of `text`.
#[napi(js_name = "captureTextRange")]
pub fn capture_text_range(
    text: String,
    version: Option<f64>,
    start: u32,
    end: u32,
) -> napi::Result<JsonValue> {
    to_json(&Range::capture(
        &text,
        version.map(|v| v as u64),
        start as usize,
        end as usize,
    ))
}

/// Where `range` lands in `text`: `{ start, end, confidence, ambiguous }`.
#[napi(js_name = "resolveTextRange")]
pub fn resolve_text_range(
    range: JsonValue,
    text: String,
    version: Option<f64>,
) -> napi::Result<JsonValue> {
    let range: Range = from_json(range)?;
    to_json(&range.resolve(&text, version.map(|v| v as u64)))
}
