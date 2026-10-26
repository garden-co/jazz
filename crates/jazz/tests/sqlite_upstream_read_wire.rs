use jazz::db::{ReadTier, SyncMessage, decode_message, encode_message};
use jazz::ids::NodeUuid;
use jazz::protocol::{
    BranchColumnValue, BranchSelector, BranchViewBase, ReadViewSourceSpec, SnapshotRef,
};
use jazz::query::Query;
use jazz::records::Value;
use jazz::time::{GlobalTime, TxTime};
use jazz::tx::TxId;
use std::collections::BTreeMap;

/// Alice's reopened tab names its exact frozen view, while Bob's ordinary
/// worker reports local storage separately. Public codec byte checks are needed
/// because runtime reads alone cannot pin the new field and appended tag.
#[test]
fn alice_fetches_exact_views_and_bob_reports_stored_with_independent_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v2/fetch-view.json")).unwrap();
    let branch = |value| BranchSelector {
        values: BTreeMap::from([("branch".into(), BranchColumnValue(vec![1, 0, value]))]),
    };
    let snapshot = SnapshotRef {
        owner: NodeUuid::from_bytes([0x11; 16]),
        global_base: GlobalTime(127),
        local_base: TxTime(0),
        dots: vec![TxId {
            time: TxTime(128),
            node: NodeUuid::from_bytes([0x22; 16]),
        }],
    };
    let views = [
        ReadViewSourceSpec::Current,
        ReadViewSourceSpec::BranchView {
            head: branch(7),
            base: None,
        },
        ReadViewSourceSpec::BranchView {
            head: branch(7),
            base: Some(BranchViewBase::Current(branch(9))),
        },
        ReadViewSourceSpec::BranchView {
            head: branch(7),
            base: Some(BranchViewBase::Snapshot {
                branch: branch(9),
                snapshot: snapshot.clone(),
            }),
        },
        ReadViewSourceSpec::Snapshot { snapshot },
    ];
    for (case, view) in fixture["cases"].as_array().unwrap().iter().zip(views) {
        let message = SyncMessage::Fetch {
            id: 17,
            query: Query::from("files").select(["owner"]).limit(3).offset(1),
            binding: BTreeMap::from([("scope".into(), Value::String("example".into()))]),
            at: Some(8),
            include_deleted: true,
            tier: ReadTier::Remote,
            wait: None,
            view,
        };
        let expected = hex::decode(case["hex"].as_str().unwrap()).unwrap();
        assert_eq!(
            encode_message(&message).unwrap(),
            expected,
            "{}",
            case["name"]
        );
        let decoded = decode_message(&expected).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
    let stored = SyncMessage::Stored { tx: 128 };
    let expected = hex::decode(fixture["stored"].as_str().unwrap()).unwrap();
    assert_eq!(encode_message(&stored).unwrap(), expected);
    assert!(matches!(
        decode_message(&expected).unwrap(),
        SyncMessage::Stored { tx: 128 }
    ));
}
