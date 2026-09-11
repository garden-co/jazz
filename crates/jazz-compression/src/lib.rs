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
    // The stable bulk API otherwise reserves the entire caller-supplied limit.
    // A frame content size applies to one frame, not a concatenated stream.
    // Keep the existing bounded decoder path when that size is unavailable.
    let capacity =
        if zstd::zstd_safe::find_frame_compressed_size(payload).ok() == Some(payload.len()) {
            zstd::zstd_safe::get_frame_content_size(payload)
                .ok()
                .flatten()
                .and_then(|size| usize::try_from(size).ok())
                .unwrap_or(max_decoded_len)
                .min(max_decoded_len)
        } else {
            max_decoded_len
        };
    zstd::bulk::decompress(payload, capacity)
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

    #[cfg(feature = "zstd")]
    #[test]
    fn known_zstd_frame_reserves_its_content_size_not_the_message_limit() {
        // Capacity is the behavior under test: value equality alone cannot
        // detect reserving a maximum-sized buffer for each small message.
        for payload in [Vec::new(), b"small frame".repeat(64)] {
            let compressed = super::compress_zstd(&payload).unwrap();
            let decoded = super::decompress_zstd(&compressed, 4 * 1024 * 1024).unwrap();
            assert_eq!(decoded, payload);
            assert_eq!(decoded.capacity(), payload.len());
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn native_zstd_preserves_unknown_size_and_concatenated_frames() {
        let first = b"first frame".repeat(64);
        let second = b"second frame".repeat(32);
        let unknown_size = zstd::stream::encode_all(first.as_slice(), 3).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&unknown_size).unwrap(),
            None
        );
        assert_eq!(
            super::decompress_zstd(&unknown_size, first.len()).unwrap(),
            first
        );
        assert!(super::decompress_zstd(&unknown_size, first.len() - 1).is_err());

        let mut concatenated = super::compress_zstd(&first).unwrap();
        concatenated.extend(super::compress_zstd(&second).unwrap());
        let mut expected = first;
        expected.extend(second);
        assert_eq!(
            super::decompress_zstd(&concatenated, expected.len()).unwrap(),
            expected
        );
        assert!(super::decompress_zstd(&concatenated, expected.len() - 1).is_err());
        assert!(super::decompress_zstd(b"not a zstd frame", 1024).is_err());
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
