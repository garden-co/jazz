//! Explicit independent bytes pin the owner reservation and publish boundary.
mod common;
use jazz::db::{Db, DbConfig, DbIdentity, RemoteLinkHint, SyncMessage, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

/// Alice's reservation messages use appended tags 33–35. The commit nested in
/// publication is the independently pinned existing serializable-origins frame.
#[test]
fn owner_initialization_v1_pins_wire_and_durable_fields() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/owner-initialization.json"
    ))
    .unwrap();
    for name in ["reserve", "reserved", "refused", "publish"] {
        let bytes = hex::decode(fixture["messages"][name].as_str().unwrap()).unwrap();
        let message = jazz::db::decode_message(&bytes).unwrap();
        match (&message, name) {
            (
                SyncMessage::OwnerInitializationReserveV1 {
                    id,
                    nonce,
                    node,
                    minimum,
                },
                "reserve",
            ) => {
                assert_eq!(*id, 127);
                assert_eq!(*nonce, [0x11; 16]);
                assert_eq!(*node, [0x22; 16]);
                assert_eq!(*minimum, fixture["stored"]["tx"].as_u64().unwrap());
            }
            (SyncMessage::OwnerInitializationReservedV1 { id, nonce, result }, "reserved") => {
                assert_eq!(*id, 127);
                assert_eq!(*nonce, [0x11; 16]);
                assert_eq!(
                    result.as_ref().unwrap(),
                    &([0x22; 16], fixture["stored"]["tx"].as_u64().unwrap())
                );
            }
            (SyncMessage::OwnerInitializationReservedV1 { id, nonce, result }, "refused") => {
                assert_eq!(*id, 128);
                assert_eq!(*nonce, [0x11; 16]);
                assert_eq!(
                    serde_json::to_value(result).unwrap(),
                    serde_json::json!({"Err":{"Other":"denied"}})
                );
            }
            (SyncMessage::OwnerInitializationPublishV1 { nonce, commit }, "publish") => {
                assert_eq!(*nonce, [0x11; 16]);
                let old: serde_json::Value =
                    serde_json::from_str(include_str!("../fixtures/sqlite_initialization_v1.json"))
                        .unwrap();
                assert_eq!(hex::encode(commit), old["commitHex"].as_str().unwrap());
            }
            _ => panic!("changed message tag: {name}"),
        }
        assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    }
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    assert_eq!(
        hex::encode(schema.version_id().as_bytes()),
        fixture["stored"]["schemaHex"]
    );
    let alice =
        AuthorSubject::from_canonical(fixture["stored"]["subject"].as_str().unwrap()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let owner = block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([50; 16]),
                author: alice,
            },
        ),
        &path,
        Default::default(),
    ))
    .unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.require_owner_durability_flush();
    let (mut tab, down) = duplex();
    owner.accept_subscriber(down, alice);
    tab.send(SyncMessage::Schema {
        version: *schema.version_id().as_bytes(),
    })
    .unwrap();
    tab.send(
        jazz::db::decode_message(
            &hex::decode(fixture["messages"]["reserve"].as_str().unwrap()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    block_on(owner.tick()).unwrap();
    assert!(
        !std::iter::from_fn(|| tab.try_recv())
            .any(|m| matches!(m, SyncMessage::OwnerInitializationReservedV1 { .. }))
    );
    let barrier = owner.prepare_owner_durability().unwrap().unwrap();
    owner.confirm_owner_durability(barrier).unwrap();
    owner.flush_owner_durability().unwrap();
    let reply = std::iter::from_fn(|| tab.try_recv())
        .find(|m| matches!(m, SyncMessage::OwnerInitializationReservedV1 { .. }))
        .unwrap();
    assert_eq!(
        hex::encode(jazz::db::encode_message(&reply).unwrap()),
        fixture["messages"]["reserved"]
    );
    block_on(owner.close()).unwrap();
    drop(owner);
    // Raw SQL inspection is necessary to pin actual durable types and bytes.
    let sql = rusqlite::Connection::open(path).unwrap();
    let stored=sql.query_row("SELECT node,tx,nonce,subject,schema_id,state FROM _jazz_owner_initialization_v1",[],|r|Ok(serde_json::json!({"nodeHex":hex::encode(r.get::<_,Vec<u8>>(0)?),"tx":r.get::<_,i64>(1)?,"nonceHex":hex::encode(r.get::<_,Vec<u8>>(2)?),"subject":r.get::<_,String>(3)?,"schemaHex":hex::encode(r.get::<_,Vec<u8>>(4)?),"state":r.get::<_,i64>(5)?}))).unwrap();
    for field in ["nodeHex", "tx", "nonceHex", "subject", "schemaHex", "state"] {
        assert_eq!(stored[field], fixture["stored"][field], "{field}");
    }
    let types=sql.query_row("SELECT typeof(node),typeof(tx),typeof(nonce),typeof(subject),typeof(schema_id),typeof(state) FROM _jazz_owner_initialization_v1",[],|r|(0..6).map(|i|r.get::<_,String>(i)).collect::<Result<Vec<_>,_>>()).unwrap();
    assert_eq!(
        serde_json::to_value(types).unwrap(),
        fixture["stored"]["types"]
    );
}
