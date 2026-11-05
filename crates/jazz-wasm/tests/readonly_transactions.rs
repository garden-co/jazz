//! Exercise public read-only transaction receipts through the browser binding.
#![cfg(target_arch = "wasm32")]
use jazz::db::WriteState;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DurabilityTier, Fate};
use jazz_wasm::WasmDb;
use serde::Serialize;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

#[derive(Serialize)]
struct Config {
    identity: Identity,
    row_id_seed: Option<u64>,
    history_complete: bool,
    initial_sync_flush_every: Option<u32>,
}
#[derive(Serialize)]
struct Identity {
    node: NodeUuid,
    author: AuthorSubject,
}
fn open(authority: bool) -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .build();
    let config = Config {
        identity: Identity {
            node: NodeUuid::from_bytes([0x68; 16]),
            author: AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
        },
        row_id_seed: Some(1),
        history_complete: authority,
        initial_sync_flush_every: None,
    };
    WasmDb::open_memory_as_backend(
        serde_json::to_vec(&schema).unwrap(),
        postcard::to_allocvec(&config).unwrap(),
    )
    .unwrap()
}

/// Alice's authority can globally accept an unchanged empty read; a standalone
/// local store may only report local persistence. Both allocate unique receipts.
#[wasm_bindgen_test]
async fn read_only_receipts_respect_authority_and_keep_unique_ids() {
    for authority in [false, true] {
        let db = open(authority);
        let mut previous = None;
        for _ in 0..2 {
            let id = OpenTransactionId::new().to_string();
            db.begin_transaction(id.clone(), "serializable".into(), None, None)
                .unwrap();
            let rows = db
                .all(
                    postcard::to_allocvec(&jazz::query::Query::from("notes")).unwrap(),
                    js_sys::JSON::parse(r#"{"tier":"local-only"}"#).unwrap(),
                    Some(id.clone()),
                    None,
                    JsValue::NULL,
                )
                .unwrap();
            assert_eq!(js_sys::Uint8Array::new(&rows).to_vec(), vec![0]);
            let write = db
                .commit_transaction(id, Some("serializable".into()))
                .unwrap();
            let state: WriteState =
                serde_wasm_bindgen::from_value(write.write_state().unwrap()).unwrap();
            assert_eq!(
                state.fate,
                if authority {
                    Fate::Accepted
                } else {
                    Fate::Pending
                }
            );
            assert_eq!(
                state.durability,
                if authority {
                    DurabilityTier::Global
                } else {
                    DurabilityTier::Local
                }
            );
            assert_ne!(previous, Some(write.tx_id()));
            previous = Some(write.tx_id());
            JsFuture::from(write.wait("local".into(), None).unwrap())
                .await
                .unwrap();
            let accepted = JsFuture::from(write.wait("global".into(), None).unwrap()).await;
            assert_eq!(accepted.is_ok(), authority);
        }
        JsFuture::from(db.close()).await.unwrap();
    }
}
