//! Byte-level checks distinguish a structured rejection from an older payload
//! with the same tag. Row-only tests cannot prove this compatibility boundary.
use jazz::db::{
    CorpusReader, SyncMessage, WireError, decode_message, encode_message,
    validate_frame_batch_for_corpus,
};
use jazz::wire::{current_wire_features, encode_websocket_frame_batch};
use serde_json::Value;
use std::path::Path;

fn fixture(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn bytes(value: &Value) -> Vec<u8> {
    hex::decode(value.as_str().unwrap()).unwrap()
}

fn reader(value: &Value) -> CorpusReader {
    match value.as_str().unwrap() {
        "server-opening" => CorpusReader::ServerOpening,
        "server" => CorpusReader::Server,
        "client" => CorpusReader::Client,
        other => panic!("unknown reader {other}"),
    }
}

fn assert_frame(value: &Value, frame: &[u8]) {
    let decoded = decode_message(frame).unwrap_or_else(|error| panic!("{value}: {error}"));
    assert_eq!(encode_message(&decoded).unwrap(), frame, "{value}");
    assert_eq!(
        serde_json::to_value(jazz::sync_codec::decode_sync_message(frame).unwrap()).unwrap(),
        serde_json::to_value(&decoded).unwrap(),
    );
}

/// Alice and Bob retain all 47 message tags and the existing receipt/evidence
/// forms. The carrier accepts their current frames and refuses malformed ones.
#[test]
fn every_v3_tag_extended_form_and_carrier_case_has_pinned_bytes() {
    let corpus = fixture("sqlite_sync_wire_v3.json");
    assert_eq!(corpus["format"], "jazz-sqlite-sync-wire-corpus-v3");
    let messages = corpus["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 47);
    for (tag, message) in messages.iter().enumerate() {
        let frame = bytes(&message["frame_hex"]);
        assert_eq!(frame[5], tag as u8);
        let decoded = serde_json::to_value(decode_message(&frame).unwrap()).unwrap();
        assert!(decoded.get(message["variant"].as_str().unwrap()).is_some());
    }
    for message in messages
        .iter()
        .chain(corpus["examples"].as_array().unwrap())
        .chain(corpus["fates"].as_array().unwrap())
    {
        let frame = bytes(&message["frame_hex"]);
        assert_frame(message, &frame);
        let batch = encode_websocket_frame_batch(&[frame]).unwrap();
        validate_frame_batch_for_corpus(
            &batch,
            reader(&message["reader"]),
            current_wire_features(),
        )
        .unwrap_or_else(|error| panic!("{message}: {error}"));
    }
    for (key, accepted) in [("frames", true), ("rejections", false)] {
        for case in corpus[key].as_array().unwrap() {
            assert_eq!(
                validate_frame_batch_for_corpus(
                    &bytes(&case["batch_hex"]),
                    reader(&case["reader"]),
                    current_wire_features()
                )
                .is_ok(),
                accepted,
                "{case}"
            );
        }
    }
}

/// Bob's authority receipts preserve named and unnamed rejection reasons for
/// Alice. Even an unknown reason name retains its message and exact wire bytes.
#[test]
fn v3_rejections_match_independent_fields_in_every_receipt_envelope() {
    let fixtures = fixture("sqlite-sync-v3/error-cases.json");
    assert_eq!(fixtures["cases"].as_array().unwrap().len(), 7);
    for case in fixtures["cases"].as_array().unwrap() {
        let frame = bytes(&case["frame_hex"]);
        let message: SyncMessage = serde_json::from_value(case["message"].clone()).unwrap();
        assert_eq!(encode_message(&message).unwrap(), frame, "{}", case["name"]);
        assert_eq!(
            serde_json::to_value(decode_message(&frame).unwrap()).unwrap(),
            case["message"]
        );
        assert_frame(case, &frame);
    }
}

/// Alice's durable fate is encoded independently of its outer receipt. The
/// format-3 file stamp protects this new raw value layout on reopen.
#[test]
fn durable_v2_rejections_have_independent_byte_fixtures() {
    let fixture = fixture("sqlite_write_fate_v2.json");
    assert_eq!(fixture["role"], "jazz.sqlite.write-fate.v2");
    let cases = [
        ("accepted", Ok(())),
        ("conflict", Err(WireError::Conflict("no".into()))),
        ("other", Err(WireError::Other("no".into()))),
        (
            "rejected",
            Err(WireError::Rejected {
                reason: None,
                message: "no".into(),
            }),
        ),
        (
            "rejected_reason",
            Err(WireError::Rejected {
                reason: Some("client_clock_too_far_ahead".into()),
                message: "no".into(),
            }),
        ),
        (
            "rejected_unknown_reason",
            Err(WireError::Rejected {
                reason: Some("future_reason".into()),
                message: "no".into(),
            }),
        ),
    ];
    for (name, value) in cases {
        let expected = bytes(&fixture[name]);
        assert_eq!(postcard::to_allocvec(&value).unwrap(), expected, "{name}");
        let decoded: Result<(), WireError> = postcard::from_bytes(&expected).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(value).unwrap()
        );
    }
}

/// Mallory cannot label an old v1/v2 frame as the current protocol by sending
/// it to a different reader. Both public semantic boundaries reject it first.
#[test]
fn historical_v1_v2_frames_are_refused_on_both_sides() {
    for name in ["sqlite_sync_wire_v1.json", "sqlite_sync_wire_v2.json"] {
        let corpus = fixture(name);
        for case in corpus["messages"]
            .as_array()
            .unwrap()
            .iter()
            .chain(corpus["examples"].as_array().into_iter().flatten())
        {
            let frame = bytes(&case["frame_hex"]);
            assert!(decode_message(&frame).is_err(), "{name}: {case}");
            assert!(
                jazz::sync_codec::decode_sync_message(&frame).is_err(),
                "{name}: {case}"
            );
            let batch = encode_websocket_frame_batch(&[frame]).unwrap();
            for side in [CorpusReader::Client, CorpusReader::Server] {
                assert!(
                    validate_frame_batch_for_corpus(&batch, side, current_wire_features()).is_err()
                );
            }
        }
    }
}

fn check_pins(value: &Value, count: &mut usize) {
    match value {
        Value::String(text) if text.starts_with("4a53514c03") && text.len() > 10 => {
            assert_frame(value, &hex::decode(text).unwrap());
            *count += 1;
        }
        Value::Object(object) => object.values().for_each(|value| check_pins(value, count)),
        Value::Array(array) => array.iter().for_each(|value| check_pins(value, count)),
        _ => {}
    }
}

/// Alice's catalogue, owner, evidence and transaction fixtures retain their
/// canonical body fields under the new header, alongside the new error cases.
#[test]
fn every_independent_v3_field_fixture_decodes_exactly() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/sqlite-sync-v3");
    let mut count = 0;
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            check_pins(&value, &mut count);
        }
    }
    assert!(
        count > 70,
        "all field-fixture frames must be exercised: {count}"
    );
    let artifact = fixture("sqlite_sync_artifact_corpus_v3.json");
    assert_eq!(artifact["role"], "jazz.sqlite.sync-artifact-corpus.v3");
    for case in artifact["base_messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(artifact["existing_messages"].as_array().unwrap())
    {
        assert_frame(case, &bytes(&case["frame_hex"]));
    }
}
