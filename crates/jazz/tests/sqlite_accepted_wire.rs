//! Byte-level tests are needed to pin the accepted-evidence storage and wire
//! contracts independently of their Rust serializer implementation.
use jazz::db::{AcceptedContent, AcceptedRow, SyncMessage};
use jazz::ids::AuthorSubject;
use jazz::query::Query;
use jazz::records::Value;
use std::collections::BTreeMap;

fn content() -> AcceptedContent {
    AcceptedContent {
        seq: 127,
        node: [0x11; 16],
        tx: (1 << 52) + 3,
    }
}
fn messages() -> Vec<(&'static str, SyncMessage)> {
    let query = Query::from("notes").select(["text"]).limit(3).offset(1);
    let scope = Some((
        AuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .canonical()
            .to_owned(),
        BTreeMap::from([(
            jazz::query::provider_claim_key("access"),
            Value::String("reader".into()),
        )]),
    ));
    vec![
        (
            "accepted_rows_v1",
            SyncMessage::AcceptedRowsV1 {
                rows: vec![AcceptedRow {
                    table: "notes".into(),
                    id: [0x22; 16],
                    key: vec![],
                    row_seq: 128,
                    content: content(),
                }],
            },
        ),
        (
            "accepted_fetch_v1",
            SyncMessage::AcceptedFetchV1 {
                id: 17,
                query: query.clone(),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: None,
            },
        ),
        (
            "accepted_scoped_fetch_v1",
            SyncMessage::AcceptedFetchV1 {
                id: 18,
                query,
                binding: BTreeMap::new(),
                at: Some(128),
                include_deleted: true,
                scope,
            },
        ),
        (
            "accepted_answer_v1",
            SyncMessage::AcceptedAnswerV1 {
                id: 18,
                through: 128,
                rows: vec![[0x22; 16]],
            },
        ),
    ]
}

/// Alice's accepted messages append tags 21–23. Exact bytes and decode/re-encode
/// round trips pin scope, content origin and authority cut field order.
#[test]
fn accepted_v1_pins_wire_and_storage_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v3/accepted.json")).unwrap();
    let generated = serde_json::json!({
        "version":3,"encoding":"jazz.sqlite.sync.v3",
        "roles":["jazz.sqlite.sync.accepted-row.v1","jazz.sqlite.sync.accepted-fetch.v1","jazz.sqlite.sync.accepted-answer.v1","jazz.sqlite.accepted-row.v1"],
        "messages": messages().iter().map(|(name,message)| ((*name).to_owned(),hex::encode(jazz::db::encode_message(message).unwrap()))).collect::<BTreeMap<_,_>>(),
        "durableContent":hex::encode(postcard::to_allocvec(&content()).unwrap()),
        "emptySettledRowsFrame":"020000005b5d00"
    });
    assert_eq!(generated, fixture);
    for (name, message) in messages() {
        let bytes = hex::decode(fixture["messages"][name].as_str().unwrap()).unwrap();
        let decoded = jazz::db::decode_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
    let bytes = hex::decode(fixture["durableContent"].as_str().unwrap()).unwrap();
    assert_eq!(
        postcard::take_from_bytes::<AcceptedContent>(&bytes).unwrap(),
        (content(), &[][..])
    );
}
