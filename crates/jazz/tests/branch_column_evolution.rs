//! A later schema version may add a `branchBy` column with an immutable
//! typed default (INV-BVIEW-19): rows written before it, and selectors of
//! older versions, read as if they named that default.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ExactWriteTarget, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::BranchSelector;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

/// `docs(space, body)` branched by `space`.
fn v1() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("space", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .branch_by("space")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// v1 plus `region`, defaulting to "eu", as a second branch column.
fn v2_with(default: &str, ty: ColumnType) -> JazzSchema {
    let value = match ty {
        ColumnType::Text => jazz::tools::Value::Text(default.to_owned()),
        _ => jazz::tools::Value::Integer(0),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("space", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column_with_default("region", ty, value)
                    .branch_by("space")
                    .branch_by("region")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn v2() -> JazzSchema {
    v2_with("eu", ColumnType::Text)
}

fn open(schema: JazzSchema) -> Db {
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn space(name: &str) -> BranchSelector {
    BranchSelector::new([("space", Value::String(name.into()))])
}

fn space_region(name: &str, region: &str) -> BranchSelector {
    BranchSelector::new([
        ("space", Value::String(name.into())),
        ("region", Value::String(region.into())),
    ])
}

fn write(db: &Db, branch: BranchSelector, body: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([("body".to_owned(), Value::String(body.into()))]),
        InsertOptions {
            target: ExactWriteTarget::Branch(branch),
            ..Default::default()
        },
    ))
    .unwrap();
}

fn bodies(db: &Db, branch: BranchSelector) -> Vec<(Option<Value>, Option<Value>)> {
    let query = db.prepare_query(&db.table("docs")).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&query, ReadOpts::default().branch_view(branch, None)))
        .unwrap()
        .iter()
        .map(|row| (row.cell("body"), row.cell("region")))
        .collect();
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

fn text(value: &str) -> Option<Value> {
    Some(Value::String(value.into()))
}

/// alice's store holds a doc in space `a`; a later version adds `region`
/// (default "eu") as a branch column. The doc reads in `(a, eu)`, a write
/// to `(a, us)` is a branch of its own, and a session still on v1, naming
/// only `space`, reads and writes `(a, eu)`.
///
/// ```text
/// v1: write(space a) ─► activate v2 ─► read(a, eu) = [before]
///                                      write(a, us), write(a, eu)
/// v1 view: write(space a) ──────────► read(a, eu) = [before, european, from v1]
/// ```
#[test]
fn an_added_branch_column_reads_older_rows_and_selectors_at_its_default() {
    let db = open(v1());
    write(&db, space("a"), "before");
    block_on(db.activate_schema(v2())).unwrap();

    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
    write(&db, space_region("a", "us"), "american");
    write(&db, space_region("a", "eu"), "european");
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu")), (text("european"), text("eu"))]
    );
    assert_eq!(
        bodies(&db, space_region("a", "us")),
        vec![(text("american"), text("us"))]
    );

    // A session on v1 names only `space`: its selector is the default's.
    let old = block_on(db.register_schema_view(v1())).unwrap();
    write(&old, space("a"), "from v1");
    let query = old.prepare_query(&old.table("docs")).unwrap();
    let mut seen: Vec<_> =
        block_on(old.all(&query, ReadOpts::default().branch_view(space("a"), None)))
            .unwrap()
            .iter()
            .map(|row| row.cell("body"))
            .collect();
    seen.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(
        seen,
        vec![text("before"), text("european"), text("from v1")]
    );
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![
            (text("before"), text("eu")),
            (text("european"), text("eu")),
            (text("from v1"), text("eu")),
        ]
    );
}

/// The only branch-column evolution is adding one with a default: a
/// version that adds one without a default, turns an existing column into
/// one, changes a branch column's default or type, or (made current) drops
/// one is refused as not supported yet, and the store is left as it was.
#[test]
fn every_other_branch_column_change_is_not_supported_yet() {
    let db = open(v1());
    write(&db, space("a"), "before");
    let refused = |schema: JazzSchema, what: &str| {
        let error = block_on(db.activate_schema(schema)).unwrap_err();
        assert!(
            error.to_string().contains("not supported"),
            "{what}: {error}"
        );
    };
    let table = |columns: &dyn Fn(TableSchemaBuilder) -> TableSchemaBuilder| {
        compile_schema(
            &SchemaBuilder::new()
                .table(columns(
                    TableSchemaBuilder::new("docs")
                        .column("space", ColumnType::Text)
                        .column("body", ColumnType::Text)
                        .branch_by("space")
                        .policies(allow_all_policies()),
                ))
                .build(),
        )
    };
    refused(
        table(&|t| t.column("region", ColumnType::Text).branch_by("region")),
        "an added branch column without a default",
    );
    refused(
        table(&|t| t.branch_by("body")),
        "an existing column made a branch column",
    );
    block_on(db.activate_schema(v2())).unwrap();
    refused(v2_with("us", ColumnType::Text), "a changed default");
    refused(v2_with("", ColumnType::Integer), "a changed type");
    refused(v1(), "a dropped branch column");

    // Planted positive: the store still reads as v2 did.
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
}

/// Every admitted version reads the same stored rows, so a new version is
/// checked against each of them, not only the current one: with v2 (region
/// defaulting to "eu") admitted beside a current v1, a version with region
/// defaulting to "us" is refused whether admitted or made current, and so
/// is one keeping region as an ordinary column.
#[test]
fn a_branch_column_s_default_agrees_with_every_admitted_version() {
    let db = open(v1());
    write(&db, space("a"), "before");
    block_on(db.admit_schema_with_lenses(v2(), &[])).unwrap();
    let error =
        block_on(db.admit_schema_with_lenses(v2_with("us", ColumnType::Text), &[])).unwrap_err();
    assert!(
        error.to_string().contains("not supported"),
        "admitted: {error}"
    );
    let error = block_on(db.activate_schema(v2_with("us", ColumnType::Text))).unwrap_err();
    assert!(
        error.to_string().contains("not supported"),
        "activated: {error}"
    );
    let ordinary = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("space", ColumnType::Text)
                    .column("body", ColumnType::Text)
                    .column_with_default(
                        "region",
                        ColumnType::Text,
                        jazz::tools::Value::Text("eu".to_owned()),
                    )
                    .branch_by("space")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let error = block_on(db.admit_schema_with_lenses(ordinary, &[])).unwrap_err();
    assert!(
        error.to_string().contains("not supported"),
        "ordinary: {error}"
    );

    // Planted positive: the admitted v2 itself can still be made current.
    block_on(db.activate_schema(v2())).unwrap();
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
}

/// A schema view is admitted under the same rules: with v2 (region
/// defaulting to "eu") current, a view whose region defaults to "us" is
/// refused instead of reading the "eu" rows as `(a, us)`.
#[test]
fn a_schema_view_keys_rows_as_every_admitted_version_does() {
    let db = open(v1());
    write(&db, space("a"), "before");
    block_on(db.activate_schema(v2())).unwrap();
    let error = block_on(db.register_schema_view(v2_with("us", ColumnType::Text)))
        .err()
        .expect("a view keyed by us");
    assert!(error.to_string().contains("not supported"), "{error}");

    // Planted positive: a view of v1 still reads the doc in its space.
    let old = block_on(db.register_schema_view(v1())).unwrap();
    assert_eq!(bodies(&old, space("a")), vec![(text("before"), None)]);
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("jazz-branch-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

/// The store keeps how it keys rows: after a reopen, a version that
/// changes a branch column's default is refused, whether it is admitted
/// beside the opening one or opens the store itself.
#[test]
fn a_reopened_store_keeps_the_branch_columns_it_admitted() {
    let path = scratch("reopen");
    let open_at = |schema: JazzSchema| {
        block_on(Db::open_at(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            jazz::db::FileOptions::default(),
        ))
    };
    {
        let db = open_at(v1()).unwrap();
        write(&db, space("a"), "before");
        block_on(db.admit_schema_with_lenses(v2(), &[])).unwrap();
    }
    {
        let db = open_at(v1()).unwrap();
        let error = block_on(db.admit_schema_with_lenses(v2_with("us", ColumnType::Text), &[]))
            .unwrap_err();
        assert!(
            error.to_string().contains("not supported"),
            "admitted: {error}"
        );
    }
    let error = open_at(v2_with("us", ColumnType::Text))
        .err()
        .expect("opening as us");
    assert!(
        error.to_string().contains("not supported"),
        "opened: {error}"
    );

    // Planted positive: the store opens as v2 and reads the doc at "eu".
    let db = open_at(v2()).unwrap();
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
}

fn add_region(default: &str) -> jazz::protocol::MigrationLens {
    jazz::protocol::MigrationLens::new(
        v1().version_id(),
        v2().version_id(),
        vec![jazz::protocol::TableLens {
            source_table: "docs".into(),
            target_table: "docs".into(),
            ops: vec![jazz::protocol::LensOp::AddColumn {
                column: "region".into(),
                default: Value::String(default.into()),
            }],
        }],
    )
    .unwrap()
}

/// Rows are keyed by a branch column's declared default, so a lens may not
/// add the column with another one: the store would key the old rows by
/// the lens's default and then refuse its own schema on reopen.
#[test]
fn a_lens_may_not_key_rows_by_another_branch_default() {
    let path = scratch("lens");
    let open_at = |schema: JazzSchema| {
        block_on(Db::open_at(
            DbConfig::new(
                schema,
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap()
    };
    {
        let db = open_at(v1());
        write(&db, space("a"), "before");
        let error = block_on(db.activate_schema_with_lenses(v2(), &[add_region("us")]))
            .expect_err("a lens keying rows by us");
        assert!(error.to_string().contains("not supported"), "{error}");

        // Planted positive: a lens with the declared default.
        block_on(db.activate_schema_with_lenses(v2(), &[add_region("eu")])).unwrap();
    }
    let db = open_at(v2());
    assert_eq!(
        bodies(&db, space_region("a", "eu")),
        vec![(text("before"), text("eu"))]
    );
}

/// Mixed versions agree on the branch: a client still on v1 writes to
/// space `a`, and a server on v2 holds it in `(a, eu)`.
///
/// ```text
/// alice(v1) ──insert(space a)──► server(v2) ──► read(a, eu) = [from alice]
/// ```
#[test]
fn a_client_on_the_old_version_writes_into_the_default_branch() {
    let server = block_on(Db::open_history_complete(DbConfig::new(
        v1(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(server.activate_schema(v2())).unwrap();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client = block_on(Db::open(DbConfig::new(
        v1(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: alice,
        },
    )))
    .unwrap();
    let (client_side, server_side) = jazz::db::duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice);

    write(&client, space("a"), "from alice");
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert_eq!(
        bodies(&server, space_region("a", "eu")),
        vec![(text("from alice"), text("eu"))]
    );
    assert!(bodies(&server, space_region("a", "us")).is_empty());
}
