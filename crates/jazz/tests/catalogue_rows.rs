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
    ColumnDescriptor, EnumCaseDescriptor, PolicyExpr, RelationCmpOp, RelationColumnRef,
    RelationExpr, RelationPredicate, RelationValueRef, RowDescriptor, Schema, SchemaHash,
    TableName, TablePolicies, TableSchema,
};
use jazz::tools::schema_lens::runtime::compile_lens;
use jazz::tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz::tools::{
    CmpOp, ColumnMergeStrategy, ColumnType, Operation, PolicyValue, SchemaBuilder,
    TableSchemaBuilder, Value,
};

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
                default: Value::Null,
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

/// Publish `put` alone (no admission) and read the catalogue back.
fn put_and_reopen(put: CataloguePut) -> jazz::catalogue::CatalogueRows {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    {
        let db = open_at(compile_schema(&notes("text")), &path);
        block_on(db.apply_catalogue(CatalogueChange {
            put,
            ..CatalogueChange::default()
        }))
        .unwrap();
    }
    open_at(compile_schema(&notes("text")), &path)
        .catalogue()
        .unwrap()
}

fn column(name: &str, column_type: ColumnType) -> ColumnDescriptor {
    ColumnDescriptor::new(name, column_type)
}

/// Every shape a definition can take reads back equal after a reopen:
/// column types, defaults, references, merge strategies, indexes, branch
/// keys and column order in schemas; every lens op, drafts included; and
/// nested, relation and session policies.
#[test]
fn every_definition_shape_reads_back_equal_after_a_reopen() {
    let case = |name: &str| EnumCaseDescriptor {
        name: name.to_owned(),
        fields: vec![column("note", ColumnType::Text).nullable()],
    };
    let wide = TableSchema {
        columns: RowDescriptor::new(vec![
            // Declared out of name order: column order is kept.
            column("zeta", ColumnType::Text),
            column("count", ColumnType::BigInt).merge_strategy(ColumnMergeStrategy::Counter),
            column("body", ColumnType::Text).merge_strategy(ColumnMergeStrategy::Markdown),
            column(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            )
            .merge_strategy(ColumnMergeStrategy::GSet),
            column(
                "mood",
                ColumnType::Enum {
                    variants: vec!["glad".into(), "sad".into()],
                },
            )
            .default(Value::Text("glad".into())),
            column(
                "state",
                ColumnType::EnumPayload {
                    cases: vec![case("open"), case("closed")],
                },
            )
            .nullable(),
            column(
                "doc",
                ColumnType::Json {
                    schema: Some(serde_json::json!({"type": "object", "required": ["a"]})),
                },
            )
            .nullable(),
            column("blob", ColumnType::Bytea).default(Value::Bytea(vec![0, 1, 0xff])),
            column("score", ColumnType::Double).default(Value::Double(-0.0)),
            column("ratio", ColumnType::Double).default(Value::Double(1e-300)),
            column("at", ColumnType::Timestamp).default(Value::Timestamp(u64::MAX)),
            column("owner", ColumnType::Uuid)
                .references("people")
                .nullable(),
            column("branch", ColumnType::Text),
        ]),
        indexed_columns: Some(vec!["zeta".into(), "owner".into()]),
        composite_indexes: vec![vec!["owner".into(), "at".into()]],
        policies: TablePolicies::default(),
        branch_by: vec!["branch".into()],
    };
    let people = TableSchemaBuilder::new("people").column("name", ColumnType::Text);
    let mut schema = SchemaBuilder::new().table(people).build();
    schema.insert(TableName::new("wide"), wide);

    let mut nested = PolicyExpr::True;
    for depth in 0..20 {
        nested = if depth % 2 == 0 {
            PolicyExpr::And(vec![
                nested,
                PolicyExpr::IsNotNull {
                    column: "zeta".into(),
                },
            ])
        } else {
            PolicyExpr::Or(vec![PolicyExpr::Not(Box::new(nested)), PolicyExpr::False])
        };
    }
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::Or(vec![
            nested,
            PolicyExpr::SessionCmp {
                path: vec!["claims".into(), "weight".into()],
                op: CmpOp::Ge,
                value: Value::Double(0.5),
            },
            PolicyExpr::InList {
                column: "mood".into(),
                values: vec![PolicyValue::Literal(Value::Text("glad".into()))],
            },
            PolicyExpr::Inherits {
                operation: Operation::Select,
                via_column: "owner".into(),
                max_depth: Some(3),
            },
        ]))
        .with_insert(subject_policies().select.using.unwrap());
    let permissions = BTreeMap::from([
        (TableName::new("wide"), policies),
        (TableName::new("notes"), subject_policies()),
    ]);

    let ops = vec![
        LensOp::RenameTable {
            old_name: "a".into(),
            new_name: "b".into(),
        },
        LensOp::AddColumn {
            table: "b".into(),
            column: "n".into(),
            column_type: ColumnType::Double,
            default: Value::Double(2.5),
        },
        LensOp::RemoveColumn {
            table: "b".into(),
            column: "old".into(),
            column_type: ColumnType::Bytea,
            default: Value::Bytea(vec![7]),
        },
        LensOp::RenameColumn {
            table: "b".into(),
            old_name: "x".into(),
            new_name: "y".into(),
        },
        LensOp::AddTable {
            table: "c".into(),
            schema: schema[&TableName::new("people")].clone(),
        },
        LensOp::RemoveTable {
            table: "d".into(),
            schema: schema[&TableName::new("wide")].clone(),
        },
    ];
    let mut forward = LensTransform::with_ops(ops);
    forward.draft_ops = vec![1, 4];
    let (source, target) = (
        SchemaHash::compute(&notes("text")),
        SchemaHash::compute(&schema),
    );

    let rows = put_and_reopen(CataloguePut {
        schemas: vec![(schema.clone(), 7)],
        lenses: vec![CatalogueLens {
            source,
            target,
            published_at: 8,
            forward: forward.clone(),
        }],
        permissions: vec![(4, target, permissions.clone(), 9)],
    });
    assert_eq!(rows.schema(&target).unwrap().schema, schema);
    assert_eq!(rows.lenses[0].forward, forward);
    assert_eq!(rows.head().unwrap().permissions, permissions);
}

/// JSON has no NaN or infinity: a definition holding one is refused when
/// it is written, with why, instead of making the store unopenable.
#[test]
fn a_non_finite_double_is_refused_when_written() {
    let infinite = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("text", ColumnType::Text)
                .column_with_default("score", ColumnType::Double, Value::Double(f64::INFINITY)),
        )
        .build();
    let dir = tempfile::tempdir().unwrap();
    let error = match block_on(Db::open_at(
        DbConfig::new(
            compile_schema(&infinite),
            DbIdentity {
                node: NodeUuid::from_bytes([0x5e; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        &dir.path().join("store.sqlite"),
        FileOptions::default(),
    )) {
        Ok(_) => panic!("a schema with an infinite default opened"),
        Err(error) => error,
    };
    assert_eq!(error.code, jazz::db::ErrorCode::Schema);
    assert!(error.to_string().contains("non-finite"), "{error}");

    let db = open_at(
        compile_schema(&notes("text")),
        &dir.path().join("other.sqlite"),
    );
    let before = db.catalogue().unwrap();
    for default in [f64::NAN, f64::NEG_INFINITY] {
        let lens = CatalogueLens {
            source: SchemaHash::compute(&notes("text")),
            target: SchemaHash::compute(&notes("body")),
            published_at: 1,
            forward: LensTransform::with_ops(vec![LensOp::AddColumn {
                table: "notes".into(),
                column: "score".into(),
                column_type: ColumnType::Double,
                default: Value::Double(default),
            }]),
        };
        let error = block_on(db.apply_catalogue(CatalogueChange {
            put: CataloguePut {
                lenses: vec![lens],
                ..CataloguePut::default()
            },
            ..CatalogueChange::default()
        }))
        .unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::Schema);
    }
    assert_eq!(db.catalogue().unwrap(), before);
}
