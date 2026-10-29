//! The store's catalogue as rows (`_jazz_schemas`, `_jazz_lenses`,
//! `_jazz_permissions`): what a deploy publishes reads back as it was
//! written, a reopened store admits every version again from its own rows,
//! a change that fails leaves nothing behind, and every permissions
//! revision is kept.

use crate::common;

use std::collections::BTreeMap;

use jazz::catalogue::{CatalogueChange, CatalogueLens, CataloguePut, Permissions};
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    PolicyExpr, RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate,
    RelationValueRef, Schema, SchemaHash, TableName, TablePolicies,
};
use jazz::tools::schema_lens::runtime::compile_lens;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// `notes(column: text)`, without policies.
fn notes(column: &str) -> Schema {
    SchemaBuilder::new()
        .table(TableSchemaBuilder::new("notes").column(column, ColumnType::Text))
        .build()
}

/// `schema` with `policies` on every table.
fn with_policies(schema: &Schema, policies: TablePolicies) -> JazzSchema {
    let mut schema = schema.clone();
    for table in schema.values_mut() {
        table.policies = policies.clone();
    }
    compile_schema(&schema)
}

/// Reads notes whose body names the session's subject: a relation policy
/// with a literal and a session reference, which must survive as text.
fn subject_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::ExistsRel {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "notes".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::Or(vec![
                    RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: None,
                            column: "body".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::SessionRef(vec![
                            "claims".to_owned(),
                            "sub".to_owned(),
                        ]),
                    },
                    RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: None,
                            column: "body".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(
                            serde_json::json!({"type": "Text", "value": "public"}),
                        ),
                    },
                ]),
            },
        })
        .with_insert(PolicyExpr::True)
}

fn rename(from: &Schema, to: &Schema) -> Lens {
    Lens::new(
        SchemaHash::compute(from),
        SchemaHash::compute(to),
        LensTransform::with_ops(vec![LensOp::RenameColumn {
            table: "notes".into(),
            old_name: "text".into(),
            new_name: "body".into(),
        }]),
    )
}

fn open_at(schema: JazzSchema, path: &std::path::Path) -> Db {
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    db
}

/// Publish `v2` with its lens from `v1` and make it current with
/// `policies` at `revision`.
fn deploy(v1: &Schema, v2: &Schema, policies: TablePolicies, revision: u64) -> CatalogueChange {
    let lens = rename(v1, v2);
    let migration = compile_lens(&lens, v1, v2).unwrap();
    let current = with_policies(v2, policies);
    let permissions: Permissions = current
        .public_schema()
        .iter()
        .map(|(name, table)| (*name, table.policies.clone()))
        .collect();
    CatalogueChange {
        put: CataloguePut {
            schemas: vec![(v1.clone(), 1), (v2.clone(), 2)],
            lenses: vec![CatalogueLens {
                source: lens.source_hash,
                target: lens.target_hash,
                published_at: 2,
                forward: lens.forward.clone(),
            }],
            permissions: vec![(revision, SchemaHash::compute(v2), permissions, 3)],
        },
        admit: vec![(compile_schema(v2), vec![migration.clone()])],
        activate: Some((revision, current, vec![migration])),
    }
}

#[test]
fn a_deploy_reads_back_as_it_was_published() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let (v1, v2) = (notes("text"), notes("body"));
    let db = open_at(compile_schema(&v1), &path);
    block_on(db.apply_catalogue(deploy(&v1, &v2, subject_policies(), 1))).unwrap();

    let rows = db.catalogue().unwrap();
    let v1_row = rows.schema(&SchemaHash::compute(&v1)).unwrap();
    let v2_row = rows.schema(&SchemaHash::compute(&v2)).unwrap();
    assert_eq!(v1_row.schema, v1);
    assert_eq!(v2_row.schema, v2);
    assert_eq!(v1_row.version, Some(compile_schema(&v1).version_id()));
    assert_eq!(v2_row.version, Some(compile_schema(&v2).version_id()));
    assert!(v1_row.admitted < v2_row.admitted);
    assert_eq!(rows.genesis().unwrap().hash, SchemaHash::compute(&v1));
    assert_eq!(
        (v1_row.published_at, v2_row.published_at),
        (Some(1), Some(2))
    );
    let lens = rename(&v1, &v2);
    assert_eq!(rows.lenses.len(), 1);
    assert_eq!(rows.lenses[0].forward, lens.forward);
    let head = rows.head().unwrap();
    assert_eq!(head.revision, 1);
    assert_eq!(head.schema, SchemaHash::compute(&v2));
    assert_eq!(
        head.permissions,
        BTreeMap::from([(TableName::new("notes"), subject_policies())])
    );
    assert!(head.activated_seq.is_some());
    assert_eq!(
        db.current_schema().public_schema()[&TableName::new("notes")].policies,
        subject_policies()
    );
}

#[test]
fn a_reopened_store_admits_its_versions_from_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let (v1, v2) = (notes("text"), notes("body"));
    {
        let db = open_at(compile_schema(&v1), &path);
        block_on(db.apply_catalogue(deploy(&v1, &v2, allow_all_policies(), 1))).unwrap();
    }
    // Opened under its current schema: v1 is admitted again from its row,
    // so a lens from it still applies, and no revision is added.
    let db = open_at(with_policies(&v2, allow_all_policies()), &path);
    let v3 = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .nullable_column("tag", ColumnType::Text),
        )
        .build();
    let from_v1 = Lens::new(
        SchemaHash::compute(&v1),
        SchemaHash::compute(&v3),
        LensTransform::with_ops(vec![
            LensOp::RenameColumn {
                table: "notes".into(),
                old_name: "text".into(),
                new_name: "body".into(),
            },
            LensOp::AddColumn {
                table: "notes".into(),
                column: "tag".into(),
                column_type: ColumnType::Text,
                default: jazz::tools::Value::Null,
            },
        ]),
    );
    block_on(db.apply_catalogue(CatalogueChange {
        admit: vec![(
            compile_schema(&v3),
            vec![compile_lens(&from_v1, &v1, &v3).unwrap()],
        )],
        ..CatalogueChange::default()
    }))
    .unwrap();
    let rows = db.catalogue().unwrap();
    assert_eq!(
        rows.schemas
            .iter()
            .filter(|schema| schema.version.is_some())
            .count(),
        3
    );
    assert_eq!(rows.permissions.len(), 1);
    assert_eq!(rows.genesis().unwrap().hash, SchemaHash::compute(&v1));
}

#[test]
fn a_failed_change_leaves_the_catalogue_and_schema_as_they_were() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let (v1, v2) = (notes("text"), notes("body"));
    let db = open_at(compile_schema(&v1), &path);
    let before = db.catalogue().unwrap();
    // A required column added without a default cannot be admitted: the
    // rows published before the admission roll back with it.
    let required = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .column("count", ColumnType::BigInt),
        )
        .build();
    let mut change = deploy(&v1, &v2, allow_all_policies(), 1);
    change.admit.push((compile_schema(&required), Vec::new()));
    change.activate = Some((
        1,
        with_policies(&required, allow_all_policies()),
        Vec::new(),
    ));
    assert!(block_on(db.apply_catalogue(change)).is_err());
    assert_eq!(db.catalogue().unwrap(), before);
    assert_eq!(
        db.current_schema().version_id(),
        compile_schema(&v1).version_id()
    );
    drop(db);

    let db = open_at(compile_schema(&v1), &path);
    assert_eq!(db.catalogue().unwrap(), before);
    block_on(db.apply_catalogue(deploy(&v1, &v2, allow_all_policies(), 1))).unwrap();
    assert_eq!(db.catalogue().unwrap().head().unwrap().revision, 1);
}

#[test]
fn every_permissions_revision_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let (v1, v2) = (notes("text"), notes("body"));
    let db = open_at(compile_schema(&v1), &path);
    block_on(db.apply_catalogue(deploy(&v1, &v2, allow_all_policies(), 1))).unwrap();
    let first = db.catalogue().unwrap().head().unwrap().clone();
    // Only the write policy changes: reads take effect where they did.
    let writes_only = TablePolicies {
        insert: TablePolicies::default().insert,
        ..allow_all_policies()
    };
    block_on(db.apply_catalogue(CatalogueChange {
        activate: Some((2, with_policies(&v2, writes_only.clone()), Vec::new())),
        ..CatalogueChange::default()
    }))
    .unwrap();
    // Reads change too.
    block_on(db.apply_catalogue(CatalogueChange {
        activate: Some((3, with_policies(&v2, subject_policies()), Vec::new())),
        ..CatalogueChange::default()
    }))
    .unwrap();
    let rows = db.catalogue().unwrap();
    let revisions: Vec<_> = rows.permissions.iter().map(|row| row.revision).collect();
    assert_eq!(revisions, vec![1, 2, 3]);
    assert_eq!(rows.permissions[0], first);
    assert_eq!(rows.permissions[1].activated_seq, first.activated_seq);
    assert_eq!(
        rows.permissions[1].permissions,
        BTreeMap::from([(TableName::new("notes"), writes_only)])
    );
    assert_eq!(
        rows.permissions[2].permissions,
        BTreeMap::from([(TableName::new("notes"), subject_policies())])
    );
}
