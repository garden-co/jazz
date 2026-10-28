//! Independent byte fixtures for the authority's additive catalogue storage.
mod common;
use jazz::db::{
    CatalogueHeadV1, CatalogueSchemaV1, Db, DbConfig, DbIdentity, ErrorCode, PolicyDescriptorV1,
    block_on,
};
use jazz::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, PhysicalColumnIdentity, PhysicalIdentityManifest,
    PhysicalTableIdentity, SchemaVersion,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{collections::BTreeMap, path::Path};

fn snapshot(select: bool) -> CatalogueSnapshot {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(if select {
                        PolicyExpr::True
                    } else {
                        PolicyExpr::False
                    })),
            )
            .build(),
    );
    CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest {
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
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 128,
            schema: schema.version_id(),
        },
    }
}
fn open(snapshot: &CatalogueSnapshot, path: &Path) -> Db {
    let db = block_on(Db::open_at(
        DbConfig::new(
            snapshot.schemas[0].schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([3; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        Default::default(),
    ))
    .unwrap();
    db.set_authority();
    db
}
fn blobs(path: &Path) -> BTreeMap<String, Vec<u8>> {
    let sql = rusqlite::Connection::open(path).unwrap();
    ["head", "schema", "table", "policy"]
        .into_iter()
        .map(|role| {
            let name = if role == "policy" {
                "_jazz_policy_descriptor_v1".into()
            } else {
                format!("_jazz_catalogue_{role}_v1")
            };
            let bytes = sql
                .query_row(&format!("SELECT value FROM {name}"), [], |r| r.get(0))
                .unwrap();
            (role.into(), bytes)
        })
        .collect()
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("../fixtures/sqlite_catalogue_storage_v1.json")).unwrap()
}

/// Bob installs Alice's authored snapshot. SQL inspection pins the actual
/// durable bytes, including its policy-independent namespace, against an
/// independently assembled fixture instead of the production serializer.
#[test]
fn catalogue_storage_uses_the_named_v1_bytes() {
    let snapshot = snapshot(true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bob.sqlite");
    let bob = open(&snapshot, &path);
    bob.install_authority_catalogue(&snapshot).unwrap();
    let actual = blobs(&path);
    let expected = fixture();
    assert_eq!(expected["format"], "jazz.sqlite.catalogue-storage.v1");
    for (name, bytes) in &actual {
        assert_eq!(
            hex::encode(bytes),
            expected[name].as_str().unwrap(),
            "{name}"
        );
    }
    let head: CatalogueHeadV1 = postcard::from_bytes(&actual["head"]).unwrap();
    let record: CatalogueSchemaV1 = postcard::from_bytes(&actual["schema"]).unwrap();
    let policy: PolicyDescriptorV1 = postcard::from_bytes(&actual["policy"]).unwrap();
    assert_eq!(head.current, snapshot.current_write_schema.schema);
    assert_eq!(head.revision, 128);
    assert_eq!(head.through, 0);
    assert_eq!(
        record.schema.schema,
        snapshot.schemas[0].schema.without_permissions()
    );
    assert_eq!(record.identities, snapshot.genesis_physical_identities);
    assert_eq!(policy.head, head);
    assert_eq!(policy.schema, snapshot.schemas[0]);
}

/// Bob must retain every accepted metadata byte when Alice tries conflicting
/// policies at the same revision. A newer revision permits the policy change
/// without changing structural identities, and a stale reinstall is refused.
#[test]
fn policy_revision_is_atomic_and_separate_from_structure() {
    let first = snapshot(true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bob.sqlite");
    let bob = open(&first, &path);
    bob.install_authority_catalogue(&first).unwrap();
    let before = blobs(&path);
    let mut changed = snapshot(false);
    assert_eq!(changed.schemas[0].id, first.schemas[0].id);
    assert_eq!(
        bob.install_authority_catalogue(&changed).unwrap_err().code,
        ErrorCode::Protocol
    );
    assert_eq!(blobs(&path), before);
    changed.current_write_schema.revision += 1;
    bob.install_authority_catalogue(&changed).unwrap();
    let after = blobs(&path);
    assert_eq!(after["schema"], before["schema"]);
    assert_eq!(after["table"], before["table"]);
    let policy: PolicyDescriptorV1 = postcard::from_bytes(&after["policy"]).unwrap();
    assert_eq!(policy.schema, changed.schemas[0]);
    assert_eq!(policy.head.revision, 129);
    assert_eq!(
        bob.install_authority_catalogue(&first).unwrap_err().code,
        ErrorCode::NotObserved
    );
    assert_eq!(blobs(&path), after);
    drop(bob);
    let reopened = open(&changed, &path);
    reopened.install_authority_catalogue(&changed).unwrap();
    assert_eq!(blobs(&path), after);
}

/// Alice's authority refuses malformed or suffixed persisted records without
/// replacing them. Direct SQL corruption is necessary to test this durable
/// boundary; Bob's incoming snapshot must not silently repair accepted evidence.
#[test]
fn malformed_catalogue_storage_fails_closed() {
    for role in ["head", "schema", "table", "policy"] {
        for suffix in [false, true] {
            let snapshot = snapshot(true);
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("alice.sqlite");
            let alice = open(&snapshot, &path);
            alice.install_authority_catalogue(&snapshot).unwrap();
            let mut bytes = blobs(&path)[role].clone();
            drop(alice);
            if suffix {
                bytes.push(0);
            } else {
                bytes = vec![255];
            }
            let name = if role == "policy" {
                "_jazz_policy_descriptor_v1".into()
            } else {
                format!("_jazz_catalogue_{role}_v1")
            };
            rusqlite::Connection::open(&path)
                .unwrap()
                .execute(&format!("UPDATE {name} SET value=?1"), [bytes.clone()])
                .unwrap();
            let reopened = open(&snapshot, &path);
            let result = if role == "table" {
                reopened.table_identity("notes").map(drop)
            } else {
                reopened.install_authority_catalogue(&snapshot)
            };
            assert_eq!(
                result.unwrap_err().code,
                ErrorCode::Storage,
                "{role} suffix={suffix}"
            );
            assert_eq!(blobs(&path)[role], bytes);
        }
    }
}

/// Bob's failed policy write must roll back Alice's already staged newer head.
/// A SQLite trigger supplies a real storage failure at the last publication
/// step; after its removal the same publication succeeds normally.
#[test]
fn failed_policy_write_rolls_back_the_catalogue_head() {
    let first = snapshot(true);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bob.sqlite");
    let bob = open(&first, &path);
    bob.install_authority_catalogue(&first).unwrap();
    let before = blobs(&path);
    let sql = rusqlite::Connection::open(&path).unwrap();
    sql.execute_batch("CREATE TRIGGER refuse_policy BEFORE UPDATE ON _jazz_policy_descriptor_v1 BEGIN SELECT RAISE(ABORT, 'injected policy failure'); END").unwrap();
    let mut changed = snapshot(false);
    changed.current_write_schema.revision += 1;
    assert_eq!(
        bob.install_authority_catalogue(&changed).unwrap_err().code,
        ErrorCode::Storage
    );
    assert_eq!(blobs(&path), before);
    sql.execute_batch("DROP TRIGGER refuse_policy").unwrap();
    bob.install_authority_catalogue(&changed).unwrap();
    assert_ne!(blobs(&path)["head"], before["head"]);
}
