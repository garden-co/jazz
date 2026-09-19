use jazz_compression::stream::{Codec, MAX_STREAM_CHUNK_BYTES, StreamDecoder, StreamEncoder};

fn encode(encoder: &mut StreamEncoder, input: &[u8], finish: bool, output_size: usize) -> Vec<u8> {
    let mut input = input;
    let mut result = Vec::new();
    let mut output = vec![0; output_size];
    while !input.is_empty() {
        let p = encoder.encode(input, &mut output).unwrap();
        assert!(p.consumed + p.written > 0);
        input = &input[p.consumed..];
        result.extend_from_slice(&output[..p.written]);
    }
    loop {
        let p = if finish {
            encoder.finish(&mut output)
        } else {
            encoder.flush(&mut output)
        }
        .unwrap();
        result.extend_from_slice(&output[..p.written]);
        if p.finished {
            break;
        }
        // A completed LZ4 drain may require one final flush call.
    }
    result
}
fn decode(decoder: &mut StreamDecoder, input: &[u8], output_size: usize) -> Vec<u8> {
    let mut result = Vec::new();
    let mut output = vec![0; output_size];
    for fragment in input.chunks(1) {
        let mut fragment = fragment;
        loop {
            let p = decoder.decode(fragment, &mut output).unwrap();
            fragment = &fragment[p.consumed..];
            result.extend_from_slice(&output[..p.written]);
            if fragment.is_empty() && p.written < output_size {
                break;
            }
            assert!(p.consumed + p.written > 0 || p.finished);
        }
    }
    result
}
fn payload() -> Vec<u8> {
    let mut state = 12345_u32;
    (0..8192)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}
fn exercise(codec: Codec) {
    let mut encoder = StreamEncoder::new(codec).unwrap();
    let mut decoder = StreamDecoder::new(codec).unwrap();
    let payload = payload();
    let first = encode(&mut encoder, &payload, false, 7);
    assert_eq!(
        decode(&mut decoder, &first, 3),
        payload,
        "flushed bytes must be readable before END, even below window size"
    );
    assert!(decoder.finish().is_err());
    for _ in 0..40 {
        let compressed = encode(&mut encoder, &payload, false, 13);
        assert!(
            compressed.len() < first.len() / 4,
            "cross-message dictionary must compress repeated incompressible input: {} vs {}",
            compressed.len(),
            first.len()
        );
        assert_eq!(decode(&mut decoder, &compressed, 11), payload);
    }
    let last = encode(&mut encoder, b"tail", true, 1);
    assert_eq!(decode(&mut decoder, &last, 1), b"tail");
    decoder.finish().unwrap();
    assert!(decoder.decode(b"extra", &mut [0; 1]).is_err());
    assert!(encoder.encode(b"extra", &mut [0; 1]).is_err());
    let mut fresh = StreamEncoder::new(codec).unwrap();
    assert_eq!(encode(&mut fresh, &payload, false, 7), first);
}
#[cfg(feature = "lz4")]
#[test]
fn lz4_persistent_fragmented_stream() {
    exercise(Codec::Lz4);
}
#[cfg(feature = "zstd")]
#[test]
fn native_zstd_persistent_fragmented_stream() {
    exercise(Codec::Zstd);
}

#[cfg(feature = "ruzstd")]
#[test]
fn pure_rust_reads_flushed_native_zstd_with_bounded_output() {
    use std::io::Write;
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    encoder.window_log(16).unwrap();
    let mut decoder = StreamDecoder::new(Codec::Zstd).unwrap();
    let mut cursor = 0;
    let data = payload();
    for _ in 0..40 {
        encoder.write_all(&data).unwrap();
        encoder.flush().unwrap();
        let compressed = &encoder.get_ref()[cursor..];
        assert_eq!(decode(&mut decoder, compressed, 5), data);
        cursor = encoder.get_ref().len();
    }
    let all = encoder.finish().unwrap();
    assert!(decode(&mut decoder, &all[cursor..], 1).is_empty());
    decoder.finish().unwrap();
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_profile_is_standard_linked_frame() {
    use std::io::Read;
    let data = payload();
    let mut encoder = StreamEncoder::new(Codec::Lz4).unwrap();
    let mut bytes = encode(&mut encoder, &data, false, 100);
    assert_eq!(&bytes[..7], &[4, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0]);
    bytes.extend(encode(&mut encoder, &data, true, 100));
    let mut decoded = Vec::new();
    lz4_flex::frame::FrameDecoder::new(bytes.as_slice())
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, [data.as_slice(), data.as_slice()].concat());
}

#[test]
fn codec_limits_are_pinned() {
    assert_eq!(MAX_STREAM_CHUNK_BYTES, 65536);
}

#[test]
fn codec_states_are_send() {
    fn is_send<T: Send>() {}
    is_send::<StreamEncoder>();
    is_send::<StreamDecoder>();
}

fn available_codecs() -> Vec<Codec> {
    #[allow(unused_mut)]
    let mut codecs = Vec::new();
    #[cfg(feature = "lz4")]
    codecs.push(Codec::Lz4);
    #[cfg(feature = "zstd")]
    codecs.push(Codec::Zstd);
    codecs
}

#[test]
fn empty_output_preserves_input_and_tiny_buffers_resume_exactly() {
    for codec in available_codecs() {
        let mut encoder = StreamEncoder::new(codec).unwrap();
        let p = encoder.encode(b"message", &mut []).unwrap();
        assert_eq!((p.consumed, p.written), (0, 0));
        assert!(!encoder.flush(&mut []).unwrap().finished);
        let compressed = encode(&mut encoder, b"message", true, 1);
        let mut decoder = StreamDecoder::new(codec).unwrap();
        let p = decoder.decode(&compressed, &mut []).unwrap();
        assert_eq!((p.consumed, p.written), (0, 0));
        assert_eq!(decode(&mut decoder, &compressed, 1), b"message");
        decoder.finish().unwrap();
    }
}

#[test]
fn every_truncated_prefix_fails_explicit_finish() {
    for codec in available_codecs() {
        let compressed = encode(
            &mut StreamEncoder::new(codec).unwrap(),
            b"message",
            true,
            100,
        );
        for len in 0..compressed.len() {
            let mut decoder = StreamDecoder::new(codec).unwrap();
            decode(&mut decoder, &compressed[..len], 2);
            assert!(
                decoder.finish().is_err(),
                "prefix {len}/{}",
                compressed.len()
            );
        }
    }
}

#[test]
fn multiple_blocks_and_more_than_one_window_round_trip() {
    for codec in available_codecs() {
        let data: Vec<u8> = payload()
            .into_iter()
            .cycle()
            .take(MAX_STREAM_CHUNK_BYTES * 5 + 17)
            .collect();
        let compressed = encode(&mut StreamEncoder::new(codec).unwrap(), &data, true, 1024);
        let mut decoder = StreamDecoder::new(codec).unwrap();
        assert_eq!(decode(&mut decoder, &compressed, 17), data);
        decoder.finish().unwrap();
    }
}

fn reject(codec: Codec, bytes: &[u8], expected: &str) {
    let mut d = StreamDecoder::new(codec).unwrap();
    let error = d.decode(bytes, &mut [0; 65536]).unwrap_err();
    assert!(error.contains(expected), "unexpected error: {error}");
    assert!(d.decode(&[], &mut [0; 1]).unwrap_err().contains("poisoned"));
    assert!(d.finish().is_err());
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_rejects_profile_and_expansion_violations() {
    let header = [4, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0];
    for index in 0..header.len() {
        let mut bad = header;
        bad[index] ^= 1;
        reject(Codec::Lz4, &bad, "header/window");
    }
    let mut large = header.to_vec();
    large.extend_from_slice(&65537_u32.to_le_bytes());
    reject(Codec::Lz4, &large, "exceeds 64KiB");
    let bomb = lz4_flex::compress(&vec![42; 65537]);
    let mut large = header.to_vec();
    large.extend_from_slice(&(bomb.len() as u32).to_le_bytes());
    large.extend(bomb);
    reject(Codec::Lz4, &large, "channel codec:");
}

#[cfg(any(feature = "zstd", feature = "ruzstd"))]
#[test]
fn zstd_rejects_profile_and_expansion_violations() {
    use std::io::Write;
    let header = [0x28, 0xb5, 0x2f, 0xfd, 0, 0x30];
    for (index, value) in [(0, 0), (4, 1), (4, 4), (4, 0x20), (5, 0x31), (5, 0xff)] {
        let mut bad = header;
        bad[index] = value;
        reject(Codec::Zstd, &bad, "header/window");
    }
    let mut large = header.to_vec();
    large.extend_from_slice(&(65537_u32 << 3).to_le_bytes()[..3]);
    reject(Codec::Zstd, &large, "exceeds 64KiB");
    let mut reserved = header.to_vec();
    reserved.extend_from_slice(&[6, 0, 0]);
    reject(Codec::Zstd, &reserved, "reserved");
    // The encoded payload is small, but its match sequence expands beyond the
    // negotiated 64KiB block/window. Header rewriting models an untrusted peer.
    let mut native = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    native.window_log(17).unwrap();
    native.write_all(&vec![42; 65537]).unwrap();
    native.flush().unwrap();
    let mut bomb = native.get_ref().clone();
    assert_eq!(&bomb[..5], &header[..5]);
    bomb[5] = 0x30;
    reject(Codec::Zstd, &bomb, "channel codec:");
}
