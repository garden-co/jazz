//! Public codec compatibility for the frozen V1 leaf layout.
use groove::large_values::{
    ChunkNode, Error, LEAF_MAX_BYTES, LargeValueKind, decode_node, encode_node, object_hash,
};
use groove::records::{RecordDescriptor, Value, ValueType, encode_variant_record};

fn ordinary_record_leaf(format: u8, kind: u8, bytes: &[u8]) -> Vec<u8> {
    // The private raw-bytes schema type is deliberately not exposed publicly.
    // Build the fixed fields through the public record API, then append the
    // sole raw payload. Valid cases also compare this fixture byte-for-byte
    // with encode_node, whose ordinary enum/record encoder is unchanged.
    let descriptor = RecordDescriptor::new([("format", ValueType::U8), ("kind", ValueType::U8)]);
    let mut payload = descriptor
        .create(&[Value::U8(format), Value::U8(kind)])
        .unwrap();
    assert_eq!(payload.len(), 2);
    payload.extend_from_slice(bytes);
    encode_variant_record(0, &payload)
}

#[test]
fn leaves_match_the_ordinary_record_codec_for_every_kind_and_boundary() {
    for (kind, tag) in [
        (LargeValueKind::Bytes, 0),
        (LargeValueKind::String, 1),
        (LargeValueKind::Json, 2),
    ] {
        for size in [0, 1, 2, 3, 31, 255, 256, 4096, LEAF_MAX_BYTES] {
            let bytes = (0..size)
                .map(|index| {
                    if kind == LargeValueKind::Bytes {
                        (index.wrapping_mul(73).wrapping_add(19) % 256) as u8
                    } else {
                        b'a' + (index % 26) as u8
                    }
                })
                .collect::<Vec<_>>();
            let node = ChunkNode::Leaf {
                format: 1,
                kind,
                bytes: bytes.clone(),
            };
            let encoded = ordinary_record_leaf(1, tag, &bytes);
            assert_eq!(encode_node(&node).unwrap(), encoded);
            assert_eq!(
                decode_node(kind, object_hash(&encoded), &encoded).unwrap(),
                node
            );
        }
    }
    for bytes in ["é🙂\0".as_bytes(), br#"{"n":-0,"a":[true,null]}"#] {
        for (kind, tag) in [(LargeValueKind::String, 1), (LargeValueKind::Json, 2)] {
            let encoded = ordinary_record_leaf(1, tag, bytes);
            assert_eq!(
                decode_node(kind, object_hash(&encoded), &encoded).unwrap(),
                ChunkNode::Leaf {
                    format: 1,
                    kind,
                    bytes: bytes.to_vec()
                }
            );
        }
    }
}

#[test]
fn leaf_dispatch_and_kind_checks_reject_every_unknown_header() {
    for format in 0..=u8::MAX {
        if format == 1 {
            continue;
        }
        let encoded = ordinary_record_leaf(format, 0, b"payload");
        assert_eq!(
            decode_node(LargeValueKind::Bytes, object_hash(&encoded), &encoded),
            Err(Error::UnsupportedFormat(format))
        );
    }
    for kind_tag in 3..=u8::MAX {
        let encoded = ordinary_record_leaf(1, kind_tag, b"payload");
        assert_eq!(
            decode_node(LargeValueKind::Bytes, object_hash(&encoded), &encoded),
            Err(Error::MalformedNode)
        );
    }
    for (actual, tag) in [
        (LargeValueKind::Bytes, 0),
        (LargeValueKind::String, 1),
        (LargeValueKind::Json, 2),
    ] {
        let encoded = ordinary_record_leaf(1, tag, b"null");
        for expected in [
            LargeValueKind::Bytes,
            LargeValueKind::String,
            LargeValueKind::Json,
        ] {
            if actual != expected {
                assert_eq!(
                    decode_node(expected, object_hash(&encoded), &encoded),
                    Err(Error::DescriptorMismatch)
                );
            }
        }
    }
}

#[test]
fn malformed_leaf_framing_utf8_and_size_still_fail() {
    let malformed: &[&[u8]] = &[
        &[],
        &[0],
        &[0, 1],
        &[0x80, 0, 1, 0],
        &[0x80, 0x80, 0, 1, 0],
        &[0x80, 0x80, 0x80, 0x80, 0x10, 1, 0],
        &[2, 1, 0],
    ];
    for encoded in malformed {
        assert_eq!(
            decode_node(LargeValueKind::Bytes, object_hash(encoded), encoded),
            Err(Error::MalformedNode)
        );
    }
    for (kind, tag) in [(LargeValueKind::String, 1), (LargeValueKind::Json, 2)] {
        let encoded = ordinary_record_leaf(1, tag, &[0xf0, 0x9f, 0x99]);
        assert_eq!(
            decode_node(kind, object_hash(&encoded), &encoded),
            Err(Error::InvalidUtf8)
        );
    }
    let encoded = ordinary_record_leaf(1, 0, &vec![0; LEAF_MAX_BYTES + 1]);
    assert_eq!(
        decode_node(LargeValueKind::Bytes, object_hash(&encoded), &encoded),
        Err(Error::MalformedNode)
    );
}

#[test]
fn raw_leaf_suffix_is_payload_and_remains_object_authenticated() {
    let mut encoded = ordinary_record_leaf(1, 0, b"payload");
    let original_hash = object_hash(&encoded);
    encoded.push(0);
    assert_eq!(
        decode_node(LargeValueKind::Bytes, original_hash, &encoded),
        Err(Error::ObjectHashMismatch)
    );
    assert_eq!(
        decode_node(LargeValueKind::Bytes, object_hash(&encoded), &encoded).unwrap(),
        ChunkNode::Leaf {
            format: 1,
            kind: LargeValueKind::Bytes,
            bytes: b"payload\0".to_vec()
        }
    );
}
