//! Platform codec implementations used by Jazz transport envelopes.
//!
//! Wire negotiation, feature bits, and logical message limits remain owned by
//! `jazz`; this crate owns only codec-specific dependencies and byte transforms.

#[cfg(all(not(feature = "zstd"), feature = "ruzstd"))]
use ruzstd::io::Read as _;

#[cfg(feature = "lz4")]
pub fn compress_lz4(payload: &[u8]) -> Result<Vec<u8>, String> {
    Ok(lz4_flex::compress_prepend_size(payload))
}

#[cfg(not(feature = "lz4"))]
pub fn compress_lz4(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "lz4")]
pub fn decompress_lz4(payload: &[u8], max_decoded_len: usize) -> Result<Vec<u8>, String> {
    let advertised = payload
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| "lz4 payload is missing its decoded-length prefix".to_owned())?
        as usize;
    validate_decoded_len(advertised, max_decoded_len)?;
    let decoded = lz4_flex::decompress_size_prepended(payload)
        .map_err(|error| format!("failed to decompress lz4 payload: {error}"))?;
    validate_decoded_len(decoded.len(), max_decoded_len)?;
    Ok(decoded)
}

#[cfg(not(feature = "lz4"))]
pub fn decompress_lz4(_payload: &[u8], _max_decoded_len: usize) -> Result<Vec<u8>, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "zstd")]
pub fn compress_zstd(payload: &[u8]) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(payload, 3)
        .map_err(|error| format!("failed to compress zstd payload: {error}"))
}

#[cfg(not(feature = "zstd"))]
pub fn compress_zstd(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "zstd")]
pub fn decompress_zstd(payload: &[u8], max_decoded_len: usize) -> Result<Vec<u8>, String> {
    zstd::bulk::decompress(payload, max_decoded_len)
        .map_err(|error| format!("failed to decompress zstd payload: {error}"))
}

#[cfg(all(not(feature = "zstd"), feature = "ruzstd"))]
pub fn decompress_zstd(payload: &[u8], max_decoded_len: usize) -> Result<Vec<u8>, String> {
    let decoder = ruzstd::decoding::StreamingDecoder::new(payload)
        .map_err(|error| format!("failed to initialize ruzstd payload: {error}"))?;
    let mut output = Vec::new();
    decoder
        .take(max_decoded_len.saturating_add(1) as u64)
        .read_to_end(&mut output)
        .map_err(|error| format!("failed to decompress ruzstd payload: {error}"))?;
    validate_decoded_len(output.len(), max_decoded_len)?;
    Ok(output)
}

#[cfg(not(any(feature = "zstd", feature = "ruzstd")))]
pub fn decompress_zstd(_payload: &[u8], _max_decoded_len: usize) -> Result<Vec<u8>, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
}

#[cfg(any(feature = "lz4", all(not(feature = "zstd"), feature = "ruzstd")))]
fn validate_decoded_len(len: usize, max_decoded_len: usize) -> Result<(), String> {
    if len > max_decoded_len {
        return Err(format!(
            "logical message payload size {len} exceeds max {max_decoded_len}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_round_trips_with_a_decoded_size_limit() {
        let payload = b"canonical transport payload".repeat(32);
        let compressed = super::compress_lz4(&payload).expect("compress lz4");
        assert_eq!(
            super::decompress_lz4(&compressed, payload.len()).expect("decompress lz4"),
            payload
        );
        assert!(super::decompress_lz4(&compressed, payload.len() - 1).is_err());
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn native_zstd_round_trips_with_a_decoded_size_limit() {
        let payload = b"canonical transport payload".repeat(32);
        let compressed = super::compress_zstd(&payload).expect("compress zstd");
        assert_eq!(
            super::decompress_zstd(&compressed, payload.len()).expect("decompress zstd"),
            payload
        );
        assert!(super::decompress_zstd(&compressed, payload.len() - 1).is_err());
    }

    #[cfg(all(feature = "ruzstd", not(feature = "zstd")))]
    #[test]
    fn pure_rust_zstd_decoder_reads_native_zstd_frames() {
        let payload = b"canonical transport payload".repeat(32);
        let compressed = zstd::bulk::compress(&payload, 3).expect("compress fixture");
        assert_eq!(
            super::decompress_zstd(&compressed, payload.len()).expect("decompress ruzstd"),
            payload
        );
        assert!(super::decompress_zstd(&compressed, payload.len() - 1).is_err());
    }
}
