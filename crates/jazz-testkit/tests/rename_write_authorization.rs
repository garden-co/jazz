use jazz_testkit as support;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, UpdateOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{LensOp, MigrationLens, SchemaVersion, TableLens};
use jazz::query::Query;
use jazz::records::Value;
use jazz::row_input;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::schema_lens::{Lens, LensTransform};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::duplex_transport::duplex;
use support::{
    publish_allow_all_permissions, push_catalogue_in_memory, wait_for_remote_query_ready,
};

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn v1_schema() -> JazzSchema {
    compile_public_schema(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("users")
                .column("email", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(owner_write_policies(PolicyExpr::True)),
        ),
    )
}

fn v2_schema() -> JazzSchema {
    let owner = owner_policy();
    compile_public_schema(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("people")
                .column("email", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::False)
                        .with_update(Some(owner.clone()), owner.clone())
                        .with_delete(owner),
                ),
        ),
    )
}

fn compile_public_schema(builder: SchemaBuilder) -> JazzSchema {
    jazz::schema::JazzSchema::new(&builder.build()).expect("test public schema compiles")
}

fn owner_policy() -> PolicyExpr {
    PolicyExpr::eq_session(
        "owner",
        vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ],
    )
}

fn owner_write_policies(select: PolicyExpr) -> TablePolicies {
    let owner = owner_policy();
    TablePolicies::new()
        .with_select(select)
        .with_insert(owner.clone())
        .with_update(Some(owner.clone()), owner.clone())
        .with_delete(owner)
}

fn open_authority(schema: JazzSchema) -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: node(0x90),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .expect("open authority")
}

/// A client of `authority` writing as `author`, subscribed to `table` so it
/// holds the rows it later updates.
fn open_writer(
    authority: &Db,
    byte: u8,
    author: AuthorSubject,
    schema: JazzSchema,
    table: &str,
) -> (Db, jazz::db::Upstream, jazz::db::SubscriptionStream) {
    // The app ships v1, v2 and the rename between them (SPEC 10.7).
    let v1 = SchemaVersion::new(v1_schema());
    let v2 = SchemaVersion::new(v2_schema());
    let lens = rename_lens(&v1, &v2);
    let writer = block_on(Db::open(
        DbConfig::new(
            schema,
            DbIdentity {
                node: node(byte),
                author,
            },
        )
        .with_migrations(vec![v1.schema, v2.schema], vec![lens]),
    ))
    .expect("open writer");
    let (client_side, authority_side) = duplex();
    let upstream = block_on(writer.connect_upstream(client_side));
    authority.accept_subscriber(authority_side, author).unwrap();
    let query = writer
        .prepare_query(&Query::from(table))
        .expect("prepare writer query");
    let stream = block_on(writer.subscribe(&query, ReadOpts::default())).expect("subscribe");
    pump(authority, &writer);
    (writer, upstream, stream)
}

fn pump(authority: &Db, writer: &Db) {
    for _ in 0..8 {
        block_on(writer.tick()).expect("writer tick");
        block_on(authority.tick()).expect("authority tick");
    }
    block_on(writer.tick()).expect("writer tick");
}

/// The authority's verdict on `tx`: ticks both sides until the writer's
/// Global wait resolves.
fn global_fate(authority: &Db, writer: &Db, tx: u64) -> Result<u64, jazz::db::Error> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let mut wait = Box::pin(writer.wait_for_transaction(tx, DurabilityTier::Global));
    for _ in 0..100 {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            return result;
        }
        block_on(writer.tick()).expect("writer tick");
        block_on(authority.tick()).expect("authority tick");
    }
    panic!("the authority never settled transaction {tx}");
}

fn rename_lens(v1: &SchemaVersion, v2: &SchemaVersion) -> MigrationLens {
    MigrationLens::new(
        v1.id,
        v2.id,
        vec![TableLens {
            source_table: "users".to_string(),
            target_table: "people".to_string(),
            ops: vec![LensOp::RenameTable {
                from: "users".to_string(),
                to: "people".to_string(),
            }],
        }],
    )
    .expect("rename fixture lens is valid")
}

fn cells(email: &str, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("email".to_string(), Value::String(email.to_string())),
        (
            "owner".to_string(),
            Value::String(owner.principal_parts().1),
        ),
    ])
}

fn client_person_values(email: &str) -> HashMap<String, jazz::tools::Value> {
    row_input!("email" => email)
}

fn client_v1_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(TableSchemaBuilder::new("users").column("email", jazz::tools::ColumnType::Text))
        .build()
}

fn client_v2_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(TableSchemaBuilder::new("people").column("email", jazz::tools::ColumnType::Text))
        .build()
}

fn client_rename_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&client_v1_schema()),
        SchemaHash::compute(&client_v2_schema()),
        LensTransform::with_ops(vec![jazz::tools::LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        }]),
    )
}

/// Exercises write authorization for a v2 update whose parent version was
/// stored under the v1 table name.
///
/// ```text
/// writer(v1) --users insert--> authority(v1)
/// authority  --rename lens----> authority(v2)
/// writer(v2) --people update--> authority(v2) --policy over projected v1 parent--> accepted
/// ```
#[test]
fn renamed_table_update_policy_uses_projected_parent_version() {
    let alice = author(0xa1);
    let v1 = SchemaVersion::new(v1_schema());
    let v2 = SchemaVersion::new(v2_schema());
    let lens = rename_lens(&v1, &v2);

    let authority = open_authority(v1.schema.clone());
    let (writer_v1, _writer_v1_link, _writer_v1_rows) =
        open_writer(&authority, 0x10, alice, v1.schema.clone(), "users");
    let user_row = row(0x77);
    let insert = block_on(writer_v1.insert(
        "users",
        cells("alice@example.com", alice),
        jazz::db::InsertOptions {
            row_id: Some(user_row),
            ..Default::default()
        },
    ))
    .expect("writer stages v1 insert");
    global_fate(&authority, &writer_v1, insert.mergeable_tx_id())
        .expect("authority accepts the v1 insert");

    block_on(authority.activate_schema_with_lenses(v2.schema.clone(), &[lens]))
        .expect("activate v2 rename lineage");

    let mallory = author(0xa2);
    let (non_owner_writer_v2, _mallory_link, _mallory_rows) =
        open_writer(&authority, 0x11, mallory, v2.schema.clone(), "people");
    let rejected = block_on(non_owner_writer_v2.update(
        "people",
        user_row,
        cells("mallory+renamed@example.com", alice),
        UpdateOptions::default(),
    ))
    .expect("non-owner stages v2 update");
    let error = global_fate(&authority, &non_owner_writer_v2, rejected.mergeable_tx_id())
        .expect_err("the authority must reject a non-owner update of the renamed row");
    assert!(
        error.to_string().to_lowercase().contains("denied")
            || error.to_string().to_lowercase().contains("polic"),
        "unexpected rejection: {error}"
    );

    let (writer_v2, _writer_v2_link, _writer_v2_rows) =
        open_writer(&authority, 0x12, alice, v2.schema.clone(), "people");
    let update = block_on(writer_v2.update(
        "people",
        user_row,
        cells("alice+renamed@example.com", alice),
        UpdateOptions::default(),
    ))
    .expect("writer stages v2 update");
    global_fate(&authority, &writer_v2, update.mergeable_tx_id())
        .expect("the owner's v2 update is accepted under the projected v1 parent");
}

/// Exercises write authorization for a new row authored after the catalogue
/// evolves from `users` to `people`.
///
/// ```text
/// admin --publish v1 users----> server
/// admin --publish v2 people---> server
/// bob   --insert people-------> server --write policy on v2 table--> accepted
/// ```
#[tokio::test(flavor = "current_thread")]
async fn renamed_table_insert_after_schema_evolution_reaches_global() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await.expect("start test server");
            let v1 = client_v1_schema();
            let v2 = client_v2_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                std::slice::from_ref(&v1),
                &[],
            )
            .await
            .expect("push initial v1 catalogue");
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &v1,
            )
            .await;

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                &[v1.clone(), v2.clone()],
                &[client_rename_lens()],
            )
            .await
            .expect("push evolved v2 catalogue");
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &v2,
            )
            .await;

            let bob = jazz_testkit::connect(
                server.make_client_context_for_user(v2, "bob-sequential-rename-write-auth"),
            )
            .await
            .expect("connect bob");
            wait_for_remote_query_ready(&bob, "people", Duration::from_secs(30)).await;

            let (_, _, transaction_id) = bob
                .insert("people", client_person_values("bob@example.com"))
                .expect("bob creates v2 person");
            support::wait_for_global_txs(
                &bob,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}

/// B -> A -> C projects old rows and checks new B writes against C's policies.
#[tokio::test(flavor = "current_thread")]
async fn sibling_schema_paths_translate_rows_defaults_and_owner_policies() {
    tokio::task::LocalSet::new()
        .run_until(sibling_schema_paths_translate_rows_defaults_and_owner_policies_inner())
        .await;
}

async fn sibling_schema_paths_translate_rows_defaults_and_owner_policies_inner() {
    use jazz::query::Query;
    use jazz::tools::{ReadTier, TableName, Value as PublicValue};
    let schema = |table: &str, owner: &str, extra: Option<&str>| {
        let mut table = TableSchemaBuilder::new(table)
            .column("title", ColumnType::Text)
            .column(owner, ColumnType::Text);
        if let Some(extra) = extra {
            table = table.column(extra, ColumnType::Text);
        }
        SchemaBuilder::new().table(table).build()
    };
    let base = schema("notes", "owner", None);
    let left = schema("left_notes", "left_owner", Some("tag"));
    let right = schema("right_notes", "right_owner", Some("category"));
    let server = JazzServer::start_with_schema(base.clone()).await.unwrap();
    for (target, table, owner, extra) in [
        (&left, "left_notes", "left_owner", "tag"),
        (&right, "right_notes", "right_owner", "category"),
    ] {
        let lens = Lens::new(
            SchemaHash::compute(&base),
            SchemaHash::compute(target),
            LensTransform::with_ops(vec![
                jazz::tools::LensOp::RenameColumn {
                    table: "notes".into(),
                    old_name: "owner".into(),
                    new_name: owner.into(),
                },
                jazz::tools::LensOp::AddColumn {
                    table: "notes".into(),
                    column: extra.into(),
                    column_type: ColumnType::Text,
                    default: PublicValue::Text(extra.into()),
                },
                jazz::tools::LensOp::RenameTable {
                    old_name: "notes".into(),
                    new_name: table.into(),
                },
            ]),
        );
        push_catalogue_in_memory(
            server.server_state(),
            server.app_id(),
            "dev",
            std::slice::from_ref(target),
            &[lens],
        )
        .await
        .unwrap();
    }
    activate_legacy_fixture(&server, &left, support::allow_all_permissions(&left)).await;
    let alice = support::connect_ready_user(
        &server,
        &left,
        "alice",
        "left_notes",
        Duration::from_secs(30),
    )
    .await;
    let (id, _, tx) = alice
        .insert(
            "left_notes",
            row_input!("title" => "before", "left_owner" => "alice", "tag" => "left-only"),
        )
        .unwrap();
    support::wait_for_global_txs(&alice, &[tx.unwrap()]).await;

    let owner = PolicyExpr::eq_session(
        "right_owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    );
    activate_legacy_fixture(
        &server,
        &right,
        [(
            TableName::new("right_notes"),
            TablePolicies::new()
                .with_select(owner.clone())
                .with_insert(owner.clone())
                .with_update(Some(owner.clone()), owner.clone())
                .with_delete(owner),
        )],
    )
    .await;
    let reader = support::connect_ready_user(
        &server,
        &right,
        "alice",
        "right_notes",
        Duration::from_secs(30),
    )
    .await;
    let rows = support::wait_for_query(
        &reader,
        Query::from("right_notes").select(["title", "category"]),
        ReadTier::Remote,
        Duration::from_secs(30),
        "project B through A to C",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    assert_eq!(
        rows,
        vec![(
            id,
            vec![
                PublicValue::Text("before".into()),
                PublicValue::Text("category".into())
            ]
        )]
    );

    let tx = alice
        .update(
            "left_notes",
            id,
            row_input!("title" => "after").into_iter().collect(),
        )
        .unwrap()
        .unwrap();
    support::wait_for_global_txs(&alice, &[tx]).await;
    support::wait_for_query(
        &reader,
        Query::from("right_notes").select(["title"]),
        ReadTier::Remote,
        Duration::from_secs(30),
        "C observes authorized B update",
        |rows| (rows == vec![(id, vec![PublicValue::Text("after".into())])]).then_some(()),
    )
    .await;
    let (reverse_id, _, tx) = reader
        .insert(
            "right_notes",
            row_input!("title" => "reverse", "right_owner" => "alice", "category" => "right-only"),
        )
        .unwrap();
    support::wait_for_global_txs(&reader, &[tx.unwrap()]).await;
    // Select B again so its renamed table is governed by explicit current grants.
    let owner = PolicyExpr::eq_session(
        "left_owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    );
    activate_legacy_fixture(
        &server,
        &left,
        [(
            TableName::new("left_notes"),
            TablePolicies::new()
                .with_select(owner.clone())
                .with_insert(owner),
        )],
    )
    .await;
    // C -> A -> B drops C's extra column and supplies B's default.
    let reverse = support::wait_for_query(
        &alice,
        Query::from("left_notes").select(["title", "tag"]),
        ReadTier::Remote,
        Duration::from_secs(30),
        "project C through A to B",
        |rows| rows.into_iter().find(|(id, _)| *id == reverse_id),
    )
    .await;
    assert_eq!(
        reverse.1,
        vec![
            PublicValue::Text("reverse".into()),
            PublicValue::Text("tag".into())
        ]
    );
    let (_, _, denied) = reader
        .insert(
            "right_notes",
            row_input!("title" => "denied", "right_owner" => "bob", "category" => "category"),
        )
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(15),
            reader.wait_for_transaction(denied.unwrap(), jazz::tools::DurabilityTier::GlobalServer),
        )
        .await
        .expect("denied sibling write should receive a rejection")
        .is_err()
    );
    alice.shutdown().await.unwrap();
    reader.shutdown().await.unwrap();
    server.shutdown().await;
}

// These fixtures exercise historical graphs that public deploy now rejects.
// Select their policies directly in the runtime to retain projection coverage.
async fn activate_legacy_fixture(
    server: &JazzServer,
    schema: &jazz::tools::Schema,
    permissions: impl IntoIterator<Item = (jazz::tools::TableName, jazz::tools::TablePolicies)>,
) {
    let permissions = permissions.into_iter().collect::<HashMap<_, _>>();
    let mut source = schema.clone();
    for (name, table) in &mut source {
        table.policies = permissions.get(name).cloned().unwrap_or_default();
    }
    let runtime = server.server_state().runtime().unwrap();
    let revision = runtime
        .catalogue()
        .await
        .unwrap()
        .head()
        .map_or(0, |head| head.revision)
        + 1;
    runtime
        .activate_schema(
            revision,
            jazz::schema::JazzSchema::new(&source).unwrap(),
            permissions,
        )
        .await
        .unwrap();
}

/// A sibling path drops `a` before adding it again; the merged view uses its
/// default, while reading the authored schema still returns the original value.
#[tokio::test(flavor = "current_thread")]
async fn merged_schema_via_sibling_path_defaults_reintroduced_column_without_erasing_source() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::query::Query;
            use jazz::tools::{ReadTier, Value as PublicValue};

            let schema = |columns: &[&str]| {
                let mut table = TableSchemaBuilder::new("notes").column("title", ColumnType::Text);
                for column in columns {
                    table = table.column(*column, ColumnType::Text);
                }
                SchemaBuilder::new().table(table).build()
            };
            let base = schema(&[]);
            let left = schema(&["a"]);
            let right = schema(&["b"]);
            let merged = schema(&["a", "b"]);
            let add_column =
                |from: &jazz::tools::Schema, to: &jazz::tools::Schema, column: &str| {
                    Lens::new(
                        SchemaHash::compute(from),
                        SchemaHash::compute(to),
                        LensTransform::with_ops(vec![jazz::tools::LensOp::AddColumn {
                            table: "notes".into(),
                            column: column.into(),
                            column_type: ColumnType::Text,
                            default: PublicValue::Text(format!("default-{column}")),
                        }]),
                    )
                };
            let server = JazzServer::start_with_schema(base.clone()).await.unwrap();
            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                std::slice::from_ref(&left),
                &[add_column(&base, &left, "a")],
            )
            .await
            .unwrap();
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &left,
            )
            .await;
            let writer = support::connect_ready_user(
                &server,
                &left,
                "alice",
                "notes",
                Duration::from_secs(30),
            )
            .await;
            let (id, _, tx) = writer
                .insert(
                    "notes",
                    row_input!("title" => "original", "a" => "authored-a"),
                )
                .unwrap();
            support::wait_for_global_txs(&writer, &[tx.unwrap()]).await;

            // Only 0 -> 1, 0 -> 2, and 2 -> 3 exist: reading 1 as 3 must
            // traverse 1 -> 0 -> 2 -> 3, with no direct value-preserving edge.
            for (from, to, column) in [(&base, &right, "b"), (&right, &merged, "a")] {
                push_catalogue_in_memory(
                    server.server_state(),
                    server.app_id(),
                    "dev",
                    std::slice::from_ref(to),
                    &[add_column(from, to, column)],
                )
                .await
                .unwrap();
            }
            activate_legacy_fixture(&server, &merged, support::allow_all_permissions(&merged))
                .await;
            let reader = support::connect_ready_user(
                &server,
                &merged,
                "alice",
                "notes",
                Duration::from_secs(30),
            )
            .await;
            let rows = support::wait_for_query(
                &reader,
                Query::from("notes").select(["title", "a", "b"]),
                ReadTier::Remote,
                Duration::from_secs(30),
                "read authored row through sibling and merged schemas",
                |rows| (rows.len() == 1).then_some(rows),
            )
            .await;
            assert_eq!(
                rows,
                vec![(
                    id,
                    vec![
                        PublicValue::Text("original".into()),
                        PublicValue::Text("default-a".into()),
                        PublicValue::Text("default-b".into()),
                    ]
                )]
            );

            let source_rows = support::wait_for_query(
                &writer,
                Query::from("notes").select(["title", "a"]),
                ReadTier::Remote,
                Duration::from_secs(30),
                "original schema retains the authored column value after projection",
                |rows| (rows.len() == 1).then_some(rows),
            )
            .await;
            assert_eq!(
                source_rows,
                vec![(
                    id,
                    vec![
                        PublicValue::Text("original".into()),
                        PublicValue::Text("authored-a".into()),
                    ]
                )]
            );

            reader.shutdown().await.unwrap();
            writer.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}
