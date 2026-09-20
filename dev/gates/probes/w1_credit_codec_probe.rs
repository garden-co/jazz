//! The grant's serde field shape, independent of Jazz runtime and author interning.
//! Unused frame variants only preserve the real ChannelCredit variant index.
use super::{ChannelClass, WireCreditKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireSession {
    session_id: String,
    epoch: u64,
    // Jazz's AuthorSubject serializer emits this canonical string. This probe
    // deliberately has no interned author implementation or storage dependency.
    identity: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WireChannelCredit {
    protocol_version: u16,
    features: u64,
    session: Option<WireSession>,
    class: ChannelClass,
    sequence: u64,
    consumed_bytes: u64,
    kind: WireCreditKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WireFrame {
    Hello,
    Message,
    Error,
    MessageFragment,
    Channel,
    ChannelCredit(WireChannelCredit),
}

pub(super) fn check((index, amount, class, kind): (usize, usize, ChannelClass, WireCreditKind)) {
    for session in [
        None,
        Some(WireSession {
            session_id: "w1-resume-benchmark".to_owned(),
            epoch: 1,
            identity: Some(r#"["urn:jazz:system","system"]"#.to_owned()),
        }),
    ] {
        let frame = WireFrame::ChannelCredit(WireChannelCredit {
            protocol_version: 3,
            features: 39,
            session,
            class,
            sequence: 0,
            consumed_bytes: amount as u64,
            kind,
        });
        let bytes = postcard::to_allocvec(&frame).unwrap();
        let (decoded, remaining): (WireFrame, _) = postcard::take_from_bytes(&bytes).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
        assert_eq!(decoded, frame);
        let WireFrame::ChannelCredit(decoded) = decoded else {
            unreachable!()
        };
        println!(
            "codec bucket={index} session={} class={:?} raw_class={} kind={:?} amount={}",
            decoded.session.is_some(),
            decoded.class,
            decoded.class as u8,
            decoded.kind,
            decoded.consumed_bytes
        );
        assert_eq!(decoded.class, class);
        assert_eq!(decoded.kind, kind);
    }
}
