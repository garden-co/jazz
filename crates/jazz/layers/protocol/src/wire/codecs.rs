//! Sync-wire codecs for transport frames: the layout postcard has always
//! given these types, written out once per type.

use super::channels::{ChannelClass, ChannelFrame};
use super::{
    WireAuthorityEndpoint, WireChannelCredit, WireChannelEnvelope, WireCreditKind, WireError,
    WireErrorCode, WireFrame, WireHello, WirePeerRole, WireRetry, WireSession,
};
use crate::codec::Bytes;

crate::wire_enum! {
    for WireFrame {
        0 => Hello(hello),
        1 => Error(error),
        2 => Channel(envelope),
        3 => ChannelCredit(credit),
    }
}

crate::wire_enum! {
    for WireCreditKind {
        0 => Frames,
        1 => Messages { count, bulk },
    }
}

crate::wire_struct! {
    for WireChannelCredit {
        protocol_version,
        features,
        session,
        class,
        sequence,
        consumed_bytes,
        kind,
    }
}

crate::wire_struct! {
    for WireChannelEnvelope {
        protocol_version,
        features,
        session,
        extent,
    }
}

crate::wire_enum! {
    for WirePeerRole {
        0 => Client,
        1 => Core,
        2 => Relay,
    }
}

crate::wire_struct! {
    for WireHello {
        min_protocol_version,
        max_protocol_version,
        features,
        role,
        authority,
    }
}

crate::wire_struct! {
    for WireAuthorityEndpoint {
        node,
        epoch,
    }
}

crate::wire_struct! {
    for WireSession {
        session_id,
        epoch,
        identity,
    }
}

crate::wire_enum! {
    for WireErrorCode {
        0 => UnsupportedProtocolVersion,
        1 => UnsupportedFeature,
        2 => MalformedFrame,
        3 => AuthFailed,
        4 => Backpressure,
        5 => Internal,
        6 => NotReady,
    }
}

crate::wire_enum! {
    for WireRetry {
        0 => Never,
        1 => AfterAuth,
        2 => AfterResume,
        3 => Later,
    }
}

crate::wire_struct! {
    for WireError {
        code,
        retry,
        message,
    }
}

// Serde spells the class by variant index, which equals its `repr(u8)`
// discriminant.
crate::wire_enum! {
    for ChannelClass {
        0 => Control,
        1 => Requests,
        2 => Delivery,
        3 => Writes,
        4 => LargeValue,
        5 => Auxiliary,
        6 => Progress,
    }
}

crate::wire_struct! {
    for ChannelFrame {
        channel,
        generation,
        sequence,
        class,
        first,
        last,
        message_len,
        decoded_len,
        payload: Bytes,
    }
}

#[cfg(test)]
mod tests {
    //! Differential tests against postcard, the layout these codecs must
    //! reproduce byte for byte. They are white-box because the contract is
    //! the byte format itself, before any public entry point switches over.

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::*;
    use crate::codec::{WireCodec, decode_exact, encode};
    use crate::ids::{AuthorSubject, NodeUuid};
    use crate::wire::{
        WIRE_FRAME_ARTIFACT_CORPUS, decode_postcard_exact, decode_websocket_frame_batch,
        encode_websocket_frame_batch,
    };

    #[track_caller]
    fn same<T>(value: T)
    where
        T: WireCodec + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert_eq!(encode(&value), bytes, "{value:?}");
        assert_eq!(decode_exact::<T>(&bytes).unwrap(), value);
    }

    fn sessions() -> Vec<Option<WireSession>> {
        let node = NodeUuid::from_bytes([7; 16]);
        vec![
            None,
            Some(WireSession {
                session_id: String::new(),
                epoch: 0,
                identity: None,
            }),
            Some(WireSession {
                session_id: "s-é".repeat(50),
                epoch: u64::MAX,
                identity: Some(AuthorSubject::authenticated("urn:jazz:test", "alice").unwrap()),
            }),
            Some(WireSession {
                session_id: "s".into(),
                epoch: 128,
                identity: Some(AuthorSubject::system_at(node)),
            }),
            Some(WireSession {
                session_id: "s".into(),
                epoch: 1,
                identity: Some(AuthorSubject::System),
            }),
        ]
    }

    const CLASSES: [ChannelClass; 7] = [
        ChannelClass::Control,
        ChannelClass::Requests,
        ChannelClass::Delivery,
        ChannelClass::Writes,
        ChannelClass::LargeValue,
        ChannelClass::Auxiliary,
        ChannelClass::Progress,
    ];

    fn extents() -> Vec<ChannelFrame> {
        CLASSES
            .iter()
            .enumerate()
            .map(|(i, &class)| ChannelFrame {
                channel: [0, 1, 127, 128, u16::MAX, 62, 63][i],
                generation: [0, 1, u64::MAX, 300, 0, 9, 1 << 40][i],
                sequence: [0, u64::MAX, 1, 2, 3, 4, 5][i],
                class,
                first: i % 2 == 0,
                last: i % 3 == 0,
                message_len: [0, 1, u32::MAX, 128, 7, 0, 65536][i],
                decoded_len: [u32::MAX, 0, 1, 2, 3, 127, 128][i],
                payload: match i {
                    0 => Vec::new(),
                    1 => vec![0],
                    2 => vec![0xff; 300],
                    _ => (0..=i as u8).collect(),
                },
            })
            .collect()
    }

    fn frames() -> Vec<WireFrame> {
        let mut frames = Vec::new();
        let node = NodeUuid::from_bytes([0x5e; 16]);
        for role in [
            WirePeerRole::Client,
            WirePeerRole::Core,
            WirePeerRole::Relay,
        ] {
            for authority in [
                None,
                Some(WireAuthorityEndpoint { node, epoch: 300 }),
                Some(WireAuthorityEndpoint {
                    node: NodeUuid::from_bytes([0; 16]),
                    epoch: u64::MAX,
                }),
            ] {
                frames.push(WireFrame::Hello(WireHello {
                    min_protocol_version: 0,
                    max_protocol_version: u16::MAX,
                    features: u64::MAX,
                    role,
                    authority,
                }));
            }
        }
        let codes = [
            WireErrorCode::UnsupportedProtocolVersion,
            WireErrorCode::UnsupportedFeature,
            WireErrorCode::MalformedFrame,
            WireErrorCode::AuthFailed,
            WireErrorCode::Backpressure,
            WireErrorCode::Internal,
            WireErrorCode::NotReady,
        ];
        let retries = [
            WireRetry::Never,
            WireRetry::AfterAuth,
            WireRetry::AfterResume,
            WireRetry::Later,
        ];
        for (i, code) in codes.into_iter().enumerate() {
            frames.push(WireFrame::Error(WireError::new(
                code,
                retries[i % retries.len()],
                "x".repeat(i * 40),
            )));
        }
        for session in sessions() {
            for extent in extents() {
                frames.push(WireFrame::Channel(WireChannelEnvelope {
                    protocol_version: 6,
                    features: 1 << 4,
                    session: session.clone(),
                    extent,
                }));
            }
            for (i, class) in CLASSES.into_iter().enumerate() {
                let kind = match i % 3 {
                    0 => WireCreditKind::Frames,
                    1 => WireCreditKind::Messages {
                        count: u32::MAX,
                        bulk: true,
                    },
                    _ => WireCreditKind::Messages {
                        count: 0,
                        bulk: false,
                    },
                };
                frames.push(WireFrame::ChannelCredit(WireChannelCredit {
                    protocol_version: u16::MAX,
                    features: 0,
                    session: session.clone(),
                    class,
                    sequence: i as u64 * 1000,
                    consumed_bytes: u64::MAX - i as u64,
                    kind,
                }));
            }
        }
        frames
    }

    /// Every variant of every frame type encodes to postcard's bytes and
    /// decodes back from them.
    #[test]
    fn frame_codecs_match_postcard_for_every_variant() {
        for frame in frames() {
            same(frame);
        }
        for session in sessions() {
            same(session);
        }
        for extent in extents() {
            same(extent);
        }
    }

    /// Both decoders reach the same verdict and the same value on `bytes`, and
    /// an accepted frame re-encodes to `bytes`.
    #[track_caller]
    fn decoders_agree(bytes: &[u8]) {
        let ours = decode_exact::<WireFrame>(bytes);
        let theirs = decode_postcard_exact::<WireFrame>(bytes);
        match (&ours, &theirs) {
            (Ok(ours), Ok(theirs)) => {
                assert_eq!(ours, theirs, "{}", hex::encode(bytes));
                assert_eq!(encode(ours), bytes);
            }
            (Err(_), Err(_)) => {}
            _ => panic!(
                "decoders disagree on {}: ours {ours:?}, postcard {theirs:?}",
                hex::encode(bytes)
            ),
        }
    }

    /// Hex strings under `*frame_hex` / `*batch_hex` keys anywhere in `json`.
    fn hex_fields(json: &serde_json::Value, key_suffix: &str, out: &mut Vec<Vec<u8>>) {
        match json {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    match value.as_str() {
                        Some(text) if key.ends_with(key_suffix) => {
                            out.push(hex::decode(text).unwrap())
                        }
                        _ => hex_fields(value, key_suffix, out),
                    }
                }
            }
            serde_json::Value::Array(items) => {
                items.iter().for_each(|v| hex_fields(v, key_suffix, out))
            }
            _ => {}
        }
    }

    fn fixture_frames() -> Vec<Vec<u8>> {
        let sources = [
            WIRE_FRAME_ARTIFACT_CORPUS,
            include_str!("../../../../fixtures/wire_frame_artifact_corpus.json"),
            include_str!("../../../../fixtures/wire_frame_artifact_corpus_v5.json"),
            include_str!("../../../../fixtures/wire_hello_frames.json"),
            include_str!("../../../../fixtures/wire_hello_frames_v5.json"),
            include_str!("../../../../fixtures/wire_hello_frames_v6.json"),
            include_str!("../../../../fixtures/wire_message_frames.json"),
            include_str!("../../../../fixtures/wire_message_frames_v5.json"),
            include_str!("../../../../fixtures/wire_message_frames_v6.json"),
        ];
        let mut frames = Vec::new();
        let mut batches = Vec::new();
        for source in sources {
            let json: serde_json::Value = serde_json::from_str(source).unwrap();
            hex_fields(&json, "frame_hex", &mut frames);
            hex_fields(&json, "batch_hex", &mut batches);
        }
        assert!(!batches.is_empty());
        for batch in batches {
            let theirs = postcard::from_bytes::<Vec<Vec<u8>>>(&batch);
            let ours = decode_exact::<Vec<Vec<u8>>>(&batch);
            assert_eq!(ours.as_ref().ok(), theirs.as_ref().ok());
            let Ok(batch_frames) = decode_websocket_frame_batch(&batch) else {
                continue;
            };
            assert_eq!(ours.unwrap(), batch_frames);
            assert_eq!(encode(&batch_frames), batch);
            frames.extend(batch_frames);
        }
        frames
    }

    /// Every frame of the frozen fixtures, current and retired, gets the same
    /// verdict and value from both decoders, and so does every truncation
    /// and single-byte corruption of it.
    #[test]
    fn fixture_frames_decode_like_postcard() {
        let frames = fixture_frames();
        assert!(frames.len() > 20, "{}", frames.len());
        let mut accepted = 0;
        for frame in &frames {
            accepted += usize::from(decode_postcard_exact::<WireFrame>(frame).is_ok());
            decoders_agree(frame);
            for end in 0..frame.len() {
                decoders_agree(&frame[..end]);
            }
            let mut padded = frame.clone();
            padded.push(0);
            decoders_agree(&padded);
            for at in 0..frame.len() {
                for byte in [0, 1, 2, 0x7f, 0x80, 0xff, frame[at] ^ 1] {
                    let mut mutated = frame.clone();
                    mutated[at] = byte;
                    decoders_agree(&mutated);
                }
            }
        }
        assert!(accepted > 10, "{accepted}");
    }

    /// The WebSocket carrier is postcard's `Vec<Vec<u8>>`; the generic
    /// sequence codec spells it identically.
    #[test]
    fn websocket_batches_match_postcard() {
        let batches: [Vec<Vec<u8>>; 3] = [
            vec![vec![]],
            vec![vec![0; 127], vec![1; 128], vec![2; 20_000]],
            frames().iter().map(encode).collect(),
        ];
        for batch in batches {
            let bytes = postcard::to_allocvec(&batch).unwrap();
            assert_eq!(encode(&batch), bytes);
            assert_eq!(decode_exact::<Vec<Vec<u8>>>(&bytes).unwrap(), batch);
            if let Ok(carrier) = encode_websocket_frame_batch(&batch) {
                assert_eq!(carrier, bytes);
            }
        }
        same(Vec::<Vec<u8>>::new());
    }
}
