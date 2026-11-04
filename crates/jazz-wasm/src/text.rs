//! Stateless anchors and ranges into text values (`jazz-text`), for editors
//! and presence. Values cross as plain objects: `{ version?, position?,
//! side, before, after }` and `{ version?, start?, end?, before, matched,
//! after }`. Versions are optional: a resolve takes the position as is only
//! when the anchor's version is the value's.

use jazz_text::{Anchor, Range, Side};
use wasm_bindgen::prelude::*;

fn from_js<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn to_js(value: &impl serde::Serialize) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

/// The anchor at `position` (UTF-16 code units) of `text`.
#[wasm_bindgen(js_name = captureTextAnchor)]
pub fn capture_text_anchor(
    text: &str,
    version: Option<f64>,
    position: u32,
    side: JsValue,
) -> Result<JsValue, JsValue> {
    let side: Side = from_js(side)?;
    to_js(&Anchor::capture(
        text,
        version.map(|v| v as u64),
        position as usize,
        side,
    ))
}

/// Where `anchor` lands in `text`: `{ position, confidence, ambiguous }`.
#[wasm_bindgen(js_name = resolveTextAnchor)]
pub fn resolve_text_anchor(
    anchor: JsValue,
    text: &str,
    version: Option<f64>,
) -> Result<JsValue, JsValue> {
    let anchor: Anchor = from_js(anchor)?;
    to_js(&anchor.resolve(text, version.map(|v| v as u64)))
}

/// The range `start..end` (UTF-16 code units) of `text`.
#[wasm_bindgen(js_name = captureTextRange)]
pub fn capture_text_range(
    text: &str,
    version: Option<f64>,
    start: u32,
    end: u32,
) -> Result<JsValue, JsValue> {
    to_js(&Range::capture(
        text,
        version.map(|v| v as u64),
        start as usize,
        end as usize,
    ))
}

/// Where `range` lands in `text`: `{ start, end, confidence, ambiguous }`.
#[wasm_bindgen(js_name = resolveTextRange)]
pub fn resolve_text_range(
    range: JsValue,
    text: &str,
    version: Option<f64>,
) -> Result<JsValue, JsValue> {
    let range: Range = from_js(range)?;
    to_js(&range.resolve(text, version.map(|v| v as u64)))
}
