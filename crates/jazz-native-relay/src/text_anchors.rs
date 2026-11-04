//! Stateless anchors and ranges into text values (`jazz-text`), the same
//! resolver WASM and NAPI export. One JSON request in, one JSON result out,
//! so the C ABI stays a single entry point as operations grow.

use crate::{
    JazzNativeRelayBytes, JazzNativeRelayStatus, bounded_bytes_arg, ffi_call, reset_out,
    write_bytes_out,
};
use jazz_text::{Anchor, Range, Side};
use serde::Deserialize;

/// Text values can be large values; the cap only bounds a malformed call.
const MAX_REQUEST_BYTES: usize = 256 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Request {
    CaptureAnchor {
        text: String,
        version: Option<u64>,
        position: usize,
        side: Side,
    },
    ResolveAnchor {
        anchor: Anchor,
        text: String,
        version: Option<u64>,
    },
    CaptureRange {
        text: String,
        version: Option<u64>,
        start: usize,
        end: usize,
    },
    ResolveRange {
        range: Range,
        text: String,
        version: Option<u64>,
    },
}

fn handle(request: Request) -> serde_json::Result<Vec<u8>> {
    match request {
        Request::CaptureAnchor {
            text,
            version,
            position,
            side,
        } => serde_json::to_vec(&Anchor::capture(&text, version, position, side)),
        Request::ResolveAnchor {
            anchor,
            text,
            version,
        } => serde_json::to_vec(&anchor.resolve(&text, version)),
        Request::CaptureRange {
            text,
            version,
            start,
            end,
        } => serde_json::to_vec(&Range::capture(&text, version, start, end)),
        Request::ResolveRange {
            range,
            text,
            version,
        } => serde_json::to_vec(&range.resolve(&text, version)),
    }
}

/// Run one text-anchor operation: `request` is UTF-8 JSON
/// `{ "op": "captureAnchor" | "resolveAnchor" | "captureRange" |
/// "resolveRange", ... }` and `out` receives the result as UTF-8 JSON.
///
/// # Safety
/// `request` must be readable for `request_len` bytes and `out` writable;
/// they must not alias. Release a successful `out` with
/// `jazz_native_relay_bytes_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_text_anchors(
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let request = unsafe { bounded_bytes_arg(request, request_len, MAX_REQUEST_BYTES)? };
        let request: Request =
            serde_json::from_slice(request).map_err(|_| JazzNativeRelayStatus::InvalidArgument)?;
        let result = handle(request).map_err(|_| JazzNativeRelayStatus::InvalidArgument)?;
        unsafe { write_bytes_out(out, result) };
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jazz_native_relay_bytes_free;

    fn call(request: &str) -> (JazzNativeRelayStatus, String) {
        let mut out = JazzNativeRelayBytes::EMPTY;
        let status =
            unsafe { jazz_native_relay_text_anchors(request.as_ptr(), request.len(), &mut out) };
        let text = if out.data.is_null() {
            String::new()
        } else {
            let bytes = unsafe { std::slice::from_raw_parts(out.data, out.len) };
            String::from_utf8(bytes.to_vec()).unwrap()
        };
        unsafe { jazz_native_relay_bytes_free(&mut out) };
        (status, text)
    }

    #[test]
    fn captures_and_resolves_an_anchor_as_json() {
        let (status, anchor) = call(
            r#"{"op":"captureAnchor","text":"Doors open at 8pm.","version":1,"position":14,"side":"after"}"#,
        );
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let request = format!(
            r#"{{"op":"resolveAnchor","anchor":{anchor},"text":"Doors now open at 8pm.","version":2}}"#
        );
        let (status, found) = call(&request);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let found: serde_json::Value = serde_json::from_str(&found).unwrap();
        assert_eq!(found["position"], 18);
        assert_eq!(found["ambiguous"], false);
    }

    #[test]
    fn resolves_an_authored_range() {
        let (status, found) = call(
            r#"{"op":"resolveRange","range":{"before":"at ","matched":"the studio"},"text":"Rehearsal at the studio."}"#,
        );
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let found: serde_json::Value = serde_json::from_str(&found).unwrap();
        assert_eq!(
            (found["start"].clone(), found["end"].clone()),
            (13.into(), 23.into())
        );
    }

    #[test]
    fn rejects_a_malformed_request() {
        assert_eq!(
            call(r#"{"op":"nope"}"#).0,
            JazzNativeRelayStatus::InvalidArgument
        );
    }
}
