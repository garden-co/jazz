//! Byte fixtures pin durable catalogue records and appended sync tags.
mod common;
use jazz::db::{
    CatalogueHeadV1, CatalogueSchemaV1, Db, DbConfig, DbIdentity, SyncMessage, block_on,
};
use jazz::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, PhysicalColumnIdentity, PhysicalIdentityManifest,
    PhysicalTableIdentity, SchemaVersion,
};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;
fn descriptor() -> CatalogueSchemaV1 {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("text", ColumnType::Text))
            .build(),
    );
    CatalogueSchemaV1 {
        schema: SchemaVersion::new(schema),
        identities: PhysicalIdentityManifest {
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
        },
    }
}
fn head(record: &CatalogueSchemaV1) -> CatalogueHeadV1 {
    let root = *blake3::Hasher::new_derive_key("jazz.sqlite.catalogue-namespace.v1")
        .update(&postcard::to_allocvec(&(record.schema.id, &record.identities)).unwrap())
        .finalize()
        .as_bytes();
    CatalogueHeadV1 {
        root,
        through: 0,
        revision: 128,
        current: record.schema.id,
    }
}
fn bytes<T: serde::Serialize>(value: &T) -> String {
    hex::encode(postcard::to_allocvec(value).unwrap())
}
/// Alice's metadata, coverage request and answer have stable tags 24–26.
/// The raw durable head/schema/table blobs and namespace digest are also pinned.
#[test]
fn catalogue_v1_pins_wire_and_storage_bytes() {
    let record = descriptor();
    let head = head(&record);
    let messages = [
        (
            "metadata",
            SyncMessage::CatalogueMetadataV1 {
                head: head.clone(),
                schemas: vec![record.clone()],
            },
        ),
        (
            "fetch",
            SyncMessage::CatalogueFetchV1 {
                id: 127,
                table: "notes".into(),
                opened: record.schema.id,
                scope: None,
            },
        ),
        (
            "scopedFetch",
            SyncMessage::CatalogueFetchV1 {
                id: 128,
                table: "notes".into(),
                opened: record.schema.id,
                scope: Some((
                    AuthorSubject::authenticated("https://issuer.example", "alice")
                        .unwrap()
                        .canonical()
                        .into(),
                    BTreeMap::from([(
                        jazz::query::provider_claim_key("access"),
                        Value::String("reader".into()),
                    )]),
                )),
            },
        ),
        (
            "covered",
            SyncMessage::CatalogueCoveredV1 {
                id: 128,
                head: head.clone(),
                opened: record.schema.id,
            },
        ),
    ];
    let generated = serde_json::json!({"version":3,"encoding":"jazz.sqlite.sync.v3","roles":["jazz.sqlite.catalogue-head.v1","jazz.sqlite.catalogue-schema.v1","jazz.sqlite.catalogue-table.v1","jazz.sqlite.catalogue-namespace.v1","jazz.sqlite.sync.catalogue-metadata.v1","jazz.sqlite.sync.catalogue-fetch.v1","jazz.sqlite.sync.catalogue-covered.v1"],"head":bytes(&head),"schema":bytes(&record),"table":bytes(&record.identities.tables["notes"]),"messages":messages.iter().map(|(name,message)|((*name).to_owned(),hex::encode(jazz::db::encode_message(message).unwrap()))).collect::<BTreeMap<_,_>>()});
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v3/catalogue.json")).unwrap();
    assert_eq!(generated, fixture);
    for (name, message) in messages {
        let bytes = hex::decode(fixture["messages"][name].as_str().unwrap()).unwrap();
        let decoded = jazz::db::decode_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value::<SyncMessage>(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
}
/// Bob's published catalogue is persisted exactly in the documented codecs.
/// SQL inspection is needed to test actual durable bytes, rather than only serde.
#[test]
fn installed_catalogue_uses_the_pinned_durable_blobs() {
    let record = descriptor();
    let head = head(&record);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bob.sqlite");
    let config = DbConfig::new(
        record.schema.schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([3; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = block_on(Db::open_at(config, &path, Default::default())).unwrap();
    db.set_authority();
    db.install_authority_catalogue(&CatalogueSnapshot {
        genesis_physical_identities: record.identities.clone(),
        schemas: vec![record.schema.clone()],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 128,
            schema: record.schema.id,
        },
    })
    .unwrap();
    drop(db);
    let sql = rusqlite::Connection::open(&path).unwrap();
    for (table, expected) in [
        ("head", bytes(&head)),
        ("schema", bytes(&record)),
        ("table", bytes(&record.identities.tables["notes"])),
    ] {
        let actual: Vec<u8> = sql
            .query_row(
                &format!("SELECT value FROM _jazz_catalogue_{table}_v1"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hex::encode(actual), expected);
    }
}
