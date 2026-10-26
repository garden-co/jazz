//! A byte-level test is necessary here: a new message must not renumber the
//! existing SQLite messages, and its versioned role must have a frozen encoding.
use jazz::db::SyncMessage;
use jazz::ids::AuthorSubject;
use jazz::query::Query;
use jazz::records::Value;
use std::collections::BTreeMap;

fn requests() -> [(&'static str, SyncMessage); 2] {
    let query = Query::from("files").select(["owner"]).limit(3).offset(1);
    let binding = BTreeMap::from([("scope".to_owned(), Value::String("example".into()))]);
    [
        (
            "ordinary_fetch",
            SyncMessage::Fetch {
                view: jazz::protocol::ReadViewSourceSpec::Current,
                tier: jazz::db::ReadTier::Remote,
                wait: None,
                id: 17,
                query: query.clone(),
                binding: binding.clone(),
                at: Some(8),
                include_deleted: true,
            },
        ),
        (
            "scoped_fetch_v1",
            SyncMessage::ScopedFetchV1 {
                id: 17,
                query,
                binding,
                author: AuthorSubject::authenticated("https://issuer.example", "alice")
                    .unwrap()
                    .canonical()
                    .to_owned(),
                claims: BTreeMap::from([(
                    jazz::query::provider_claim_key("owner"),
                    Value::String("alice".into()),
                )]),
                include_deleted: true,
            },
        ),
    ]
}

/// Alice's versioned request has exact pinned bytes; the older unscoped request
/// keeps its tag and adds the explicit Current view. Both frames round-trip.
#[test]
fn sqlite_scoped_fetch_v1_has_a_frozen_append_only_wire_encoding() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v2/scoped-fetch.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.scoped-fetch.v1");
    assert_eq!(fixture["version"], 2);
    for (name, request) in requests() {
        let encoded = jazz::db::encode_message(&request).unwrap();
        let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, fixture[name].as_str().unwrap(), "{name}");
        let decoded = jazz::db::decode_message(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert_eq!(jazz::db::encode_message(&decoded).unwrap(), encoded);
    }
}
