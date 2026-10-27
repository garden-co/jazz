//! Independently authored bytes pin the mutable authority policy descriptor.
mod common;
use jazz::db::{
    CatalogueHeadV1, Db, DbConfig, DbIdentity, PolicyDescriptorV1, SyncMessage, block_on,
};
use jazz::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, PhysicalColumnIdentity, PhysicalIdentityManifest,
    PhysicalTableIdentity, SchemaVersion,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;

/// Alice's policy bytes remain distinct from immutable structural catalogue-v1
/// bytes. Raw SQL inspection verifies the stored envelope, not just a roundtrip.
#[test]
fn policy_descriptor_v1_pins_wire_and_storage_bytes() {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    );
    let identities = PhysicalIdentityManifest {
        tables: BTreeMap::from([(
            "notes".into(),
            PhysicalTableIdentity {
                id: GlobalPhysicalTableId(uuid::Uuid::from_bytes([0x11; 16])),
                columns: BTreeMap::from([(
                    "text".into(),
                    PhysicalColumnIdentity {
                        id: GlobalPhysicalColumnId(uuid::Uuid::from_bytes([0x22; 16])),
                        enum_variants: BTreeMap::new(),
                    },
                )]),
            },
        )]),
    };
    let root = *blake3::Hasher::new_derive_key("jazz.sqlite.catalogue-namespace.v1")
        .update(&postcard::to_allocvec(&(schema.version_id(), &identities)).unwrap())
        .finalize()
        .as_bytes();
    let descriptor = PolicyDescriptorV1 {
        head: CatalogueHeadV1 {
            root,
            through: 0,
            revision: 128,
            current: schema.version_id(),
        },
        schema: SchemaVersion::new(schema.clone()),
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/policy-descriptor.json"
    ))
    .unwrap();
    let blob = hex::decode(fixture["descriptor"].as_str().unwrap()).unwrap();
    let message = hex::decode(fixture["message"].as_str().unwrap()).unwrap();
    assert_eq!(postcard::to_allocvec(&descriptor).unwrap(), blob);
    assert_eq!(
        jazz::db::encode_message(&SyncMessage::PolicyDescriptorV1(descriptor.clone())).unwrap(),
        message
    );
    let (decoded, rest) = postcard::take_from_bytes::<PolicyDescriptorV1>(&blob).unwrap();
    assert!(rest.is_empty());
    assert_eq!(decoded, descriptor);
    let SyncMessage::PolicyDescriptorV1(decoded) = jazz::db::decode_message(&message).unwrap()
    else {
        panic!("policy descriptor tag changed")
    };
    assert_eq!(decoded, descriptor);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([3; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &path,
        Default::default(),
    ))
    .unwrap();
    db.set_authority();
    db.install_authority_catalogue(&CatalogueSnapshot {
        genesis_physical_identities: identities,
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 128,
            schema: schema.version_id(),
        },
    })
    .unwrap();
    block_on(db.close()).unwrap();
    drop(db);
    let sql = rusqlite::Connection::open(&path).unwrap();
    let stored: Vec<u8> = sql
        .query_row(
            "SELECT value FROM _jazz_policy_descriptor_v1 WHERE slot=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored, blob);
}
