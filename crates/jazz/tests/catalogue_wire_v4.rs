//! The catalogue network extension has its own semantic codec and independent bytes.
mod common;
use jazz::db::{
    CatalogueHeadV1, CatalogueSchemaV1, PolicyDescriptorV1, SyncMessage, decode_message,
    encode_message,
};
use jazz::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId};
use jazz::protocol::{
    PhysicalColumnIdentity, PhysicalIdentityManifest, PhysicalTableIdentity, SchemaVersion,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;

/// Alice and Bob agree on the v4 catalogue fields, including a scoped request,
/// through independently specified bytes. Both encoding and decoding must match
/// the typed semantic values, not merely round-trip the serializer's own output.
#[test]
fn catalogue_v4_messages_match_independent_field_fixtures() {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    );
    let record = CatalogueSchemaV1 {
        schema: SchemaVersion::new(schema.without_permissions()),
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
    };
    let head = CatalogueHeadV1 {
        root: hex::decode("2915a6f28499cdc57591b4cf3b110e902448fabdedb3066c364fe031739470b0")
            .unwrap()
            .try_into()
            .unwrap(),
        through: 0,
        revision: 128,
        current: schema.version_id(),
    };
    let messages = [
        (
            "CatalogueMetadataV1",
            SyncMessage::CatalogueMetadataV1 {
                head: head.clone(),
                schemas: vec![record],
            },
        ),
        (
            "CatalogueFetchV1",
            SyncMessage::CatalogueFetchV1 {
                id: 127,
                table: "notes".into(),
                opened: schema.version_id(),
                scope: None,
            },
        ),
        (
            "CatalogueCoveredV1",
            SyncMessage::CatalogueCoveredV1 {
                id: 128,
                head: head.clone(),
                opened: schema.version_id(),
            },
        ),
        (
            "PolicyDescriptorV1",
            SyncMessage::PolicyDescriptorV1(PolicyDescriptorV1 {
                head,
                schema: SchemaVersion::new(schema.clone()),
            }),
        ),
    ];
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_catalogue_wire_v4.json")).unwrap();
    assert_eq!(fixture["encoding"], "jazz.sqlite.sync.v4");
    for (name, message) in messages {
        let bytes = hex::decode(fixture["messages"][name].as_str().unwrap()).unwrap();
        assert_eq!(encode_message(&message).unwrap(), bytes);
        assert_eq!(
            serde_json::to_value(decode_message(&bytes).unwrap()).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
    let scoped = SyncMessage::CatalogueFetchV1 {
        id: 128,
        table: "notes".into(),
        opened: schema.version_id(),
        scope: Some((
            AuthorSubject::authenticated("https://issuer.example", "alice")
                .unwrap()
                .canonical()
                .into(),
            BTreeMap::from([(
                jazz::query::provider_claim_key("access"),
                jazz::records::Value::String("reader".into()),
            )]),
        )),
    };
    let bytes = hex::decode(fixture["scopedFetch"].as_str().unwrap()).unwrap();
    assert_eq!(encode_message(&scoped).unwrap(), bytes);
    assert_eq!(
        serde_json::to_value(decode_message(&bytes).unwrap()).unwrap(),
        serde_json::to_value(scoped).unwrap()
    );
}

/// Bob refuses Alice's historical semantic layouts before interpreting message
/// fields. Current Hello/Error carrier frames are a separate protocol boundary.
#[test]
fn v4_refuses_historical_and_unknown_semantic_versions() {
    let historical: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_sync_wire_v1.json")).unwrap();
    for message in historical["messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(historical["fates"].as_array().unwrap())
    {
        assert!(
            decode_message(&hex::decode(message["frame_hex"].as_str().unwrap()).unwrap()).is_err()
        );
    }
    for invalid in [
        b"JSQL\x02\x08\x00".as_slice(),
        b"JSQL\x03\x08\x00",
        b"JSQL\x05\x08\x00",
        b"JSQL",
        b"JSQL\x04",
        b"JSQL\x04JSQL\x04\x08\x00",
    ] {
        assert!(decode_message(invalid).is_err());
    }
    assert!(
        matches!(decode_message(b"JSQL\x04\x08\x00").unwrap(),SyncMessage::Rows { images } if images.is_empty())
    );
}
