use std::collections::HashMap;
use std::time::Duration;

use jazz::query::Query;

use super::support::wait_for_edge_txs;
use super::support::{connect_ready_client, wait_for_rows};
use super::{pe, permissions};
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, SchemaBuilder, TablePolicies, TableSchema,
    TableSchemaBuilder, Value,
};
use jazz::tools::{Session, WriteContext};
use jazz_server::JazzServer;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn make_notes_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn note_input(title: &str) -> HashMap<String, Value> {
    jazz::row_input!("title" => title)
}

fn provenance_values(title: &str, created_by: &str, updated_by: &str) -> Vec<Value> {
    vec![
        title.into(),
        canonical_user_principal(created_by),
        canonical_user_principal(updated_by),
    ]
}

fn canonical_user_principal(user_id: &str) -> Value {
    structured_author(user_id, None)
}

fn structured_author(user_id: &str, account: Option<jazz::account_registry::AccountId>) -> Value {
    Value::Row {
        id: None,
        values: vec![
            account
                .map(|id| Value::Uuid(ObjectId::from_uuid(id.0)))
                .unwrap_or(Value::Null),
            Value::Row {
                id: None,
                values: vec![
                    Value::Text("urn:jazz:test".into()),
                    Value::Text(user_id.into()),
                ],
            },
        ],
    }
}

async fn connect_author(
    server: &JazzServer,
    schema: &jazz::tools::Schema,
    user_id: &str,
) -> (JazzClient, Value) {
    let (context, client) = jazz_testkit::TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .as_user()
        .ready_on("notes", READY_TIMEOUT)
        .connect_with_context()
        .await;
    let author = structured_author(
        user_id,
        Some(context.account_id.expect("enrolled public author")),
    );
    (client, author)
}

async fn create_note_as(client: &JazzClient, title: &str) -> ObjectId {
    client
        .insert("notes", note_input(title))
        .expect("create note with session-authored provenance")
        .0
}

async fn create_note_without_session(client: &JazzClient, title: &str) -> ObjectId {
    client
        .insert("notes", note_input(title))
        .expect("create note without attribution")
        .0
}

/// A backend connection normally has system authority. Its explicit
/// `for_session` context must survive `begin_transaction`, so the staged write
/// uses both the provider UUID claim for the policy and the canonical logical
/// author for provenance.
#[tokio::test]
async fn backend_session_transaction_preserves_raw_claims_and_logical_author() {
    tokio::task::LocalSet::new()
        .run_until(backend_session_transaction_preserves_raw_claims_and_logical_author_inner())
        .await;
}

async fn backend_session_transaction_preserves_raw_claims_and_logical_author_inner() {
    let session_policy = pe::all_of([
        pe::eq("owner", pe::session(vec!["claims", "sub"])),
        pe::eq("$createdBy", pe::session("user")),
    ]);
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("owner", ColumnType::Uuid)
                .policies(permissions(|p| {
                    p.allow_read().always();
                    p.allow_insert().where_(session_policy);
                })),
        )
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;
    let account = jazz::account_registry::AccountId(uuid::Uuid::from_u128(0xa11ce));
    let mut session = Session::new("urn:jazz:test", super::ALICE_ID);
    session.account_id = Some(account);
    let transaction = backend
        .for_session(session.clone())
        .begin_transaction()
        .expect("begin backend session transaction");
    let owner = ObjectId::from_uuid(uuid::Uuid::parse_str(super::ALICE_ID).unwrap());
    let (note_id, _, staged) = transaction
        .insert(
            "notes",
            jazz::row_input!("title" => "session transaction", "owner" => Value::Uuid(owner)),
        )
        .expect("raw UUID user_id and logical author policy allow staged insert");
    assert_eq!(staged, None);
    let staged_rows = transaction
        .query(
            Query::from("notes").select(["title", "$createdBy", "$updatedBy"]),
            jazz::tools::ReadTier::LocalFirst,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("transaction reads retain the explicit session author");
    assert_eq!(
        staged_rows[0].1,
        vec![
            "session transaction".into(),
            structured_author(super::ALICE_ID, Some(account)),
            structured_author(super::ALICE_ID, Some(account)),
        ],
        "staged provenance must not use the backend SYSTEM author"
    );
    let transaction_id = transaction.commit().expect("commit session transaction");
    wait_for_edge_txs(&backend, &[transaction_id]).await;

    let rows = wait_for_rows(
        &backend,
        Query::from("notes").select(["title", "$createdBy", "$updatedBy"]),
        "backend observes canonical session provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![
            "session transaction".into(),
            structured_author(super::ALICE_ID, Some(account)),
            structured_author(super::ALICE_ID, Some(account)),
        ],
        "backend SYSTEM identity must not replace the explicit session author"
    );

    backend.shutdown().await.expect("shutdown backend");
    server.shutdown().await;
}

async fn create_note_with_backend_attribution(
    backend: &JazzClient,
    attributed_author: &str,
    title: &str,
) -> ObjectId {
    let write_context = WriteContext {
        attribution: Some(attributed_author.to_string()),
        ..Default::default()
    };
    let (note_id, _, transaction_id) = backend
        .with_write_context(write_context)
        .insert("notes", note_input(title))
        .expect("create note with backend attribution");
    wait_for_edge_txs(
        backend,
        &[transaction_id.expect("backend attributed insert should commit immediately")],
    )
    .await;

    note_id
}

/// Verifies that `$createdBy` policies scope read/update/delete access to the
/// creator when every mutation comes from an ordinary session client.
///
/// Actors: `alice` creates one note, `bob` creates another and then tries to
/// mutate Alice's row.
///
/// ```text
/// alice client ──create──────────────► server ──query──► alice sees alice row
/// bob client ───create───────────────► server ──query──► bob sees bob row
/// bob client ───update/delete alice row───────► server ──policy check──► ✗
/// ```
#[tokio::test]
async fn created_by_policies_scope_crud_to_creators() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_scope_crud_to_creators_inner())
        .await;
}

async fn created_by_policies_scope_crud_to_creators_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice, alice_author) = connect_author(&server, &schema, super::ALICE_ID).await;
    let (bob, bob_author) = connect_author(&server, &schema, super::BOB_ID).await;
    let alice_note = create_note_as(&alice, "alice note").await;
    let bob_note = create_note_as(&bob, "bob note").await;

    let query = Query::from("notes")
        .select(["title", "$createdBy", "$updatedBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![
            Value::Text("alice note".into()),
            alice_author.clone(),
            alice_author.clone()
        ]
    );

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == bob_note).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![
            Value::Text("bob note".into()),
            bob_author.clone(),
            bob_author.clone()
        ]
    );

    for operation in ["update", "delete"] {
        let result = match operation {
            "update" => bob.update(
                "notes",
                alice_note,
                vec![("title".to_string(), "bob edit".into())],
            ),
            "delete" => bob.delete("notes", alice_note),
            _ => unreachable!(),
        };
        if let Ok(transaction_id) = result {
            let error = bob
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation has a transaction"),
                    jazz::tools::DurabilityTier::EdgeServer,
                )
                .await
                .expect_err("Bob must not mutate Alice's note");
            assert!(
                error.to_string().contains("authorization_denied"),
                "{error}"
            );
        }
    }

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice row survives bob's rejected mutations",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == alice_note
                && rows[0].1
                    == vec![
                        Value::Text("alice note".into()),
                        alice_author.clone(),
                        alice_author.clone(),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 1);

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob still cannot see alice's row",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == bob_note
                && rows[0].1
                    == vec![
                        Value::Text("bob note".into()),
                        bob_author.clone(),
                        bob_author.clone(),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 1);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that backend/server writes with no attribution stamp
/// `jazz:system`, so `$createdBy` policies fail closed for ordinary users.
///
/// Actors: a backend client writes one derived row without a session, then
/// `alice` writes her own note through a normal user session.
///
/// ```text
/// backend client ─create(no session)──► server ──$createdBy = jazz:system
/// alice client ──create(as alice)─────► server ──$createdBy = alice
/// alice query ────────────────────────► sees only alice row
/// bob query ──────────────────────────► sees nothing
/// ```
#[tokio::test]
async fn created_by_policies_hide_server_generated_rows_without_attribution() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_hide_server_generated_rows_without_attribution_inner())
        .await;
}

async fn created_by_policies_hide_server_generated_rows_without_attribution_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice, alice_author) = connect_author(&server, &schema, super::ALICE_ID).await;
    let (bob, _bob_author) = connect_author(&server, &schema, super::BOB_ID).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, "alice note").await;
    let query = Query::from("notes")
        .select(["title", "$createdBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only explicitly attributed user-owned rows",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::from("alice note"), alice_author.clone(),]
    );
    assert!(
        alice_rows.iter().all(|(id, _)| *id != system_note),
        "server-generated row should stay hidden from alice under $createdBy policy"
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob does not see the server-generated system row by default",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

    assert_ne!(system_note, alice_note);

    backend.shutdown().await.expect("shutdown backend");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that the system issuer in structured `$createdBy` metadata can
/// explicitly allow ordinary users to read server-generated rows.
///
/// Actors: a backend client writes one system-authored row without a session,
/// and `alice` writes one user-authored row through her session.
///
/// ```text
/// backend client ─create(no session)──► server ──system issuer + originating node
/// alice client ──create(as alice)─────► server ──$createdBy = alice
/// alice query ────────────────────────► sees system row + alice row
/// bob query ──────────────────────────► sees only system row
/// ```
#[tokio::test]
async fn created_by_policies_can_allow_reads_from_system_author() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_can_allow_reads_from_system_author_inner())
        .await;
}

async fn created_by_policies_can_allow_reads_from_system_author_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
    let system_author_policy = pe::eq("$createdBy.identity.issuer", "urn:jazz:system");
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(pe::any_of([
                    created_by_policy.clone(),
                    system_author_policy,
                ]));
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice, alice_author) = connect_author(&server, &schema, super::ALICE_ID).await;
    let (bob, _bob_author) = connect_author(&server, &schema, super::BOB_ID).await;
    let backend_node = uuid::Uuid::new_v4();
    let backend = connect_ready_client(
        &server,
        &schema,
        &backend_node.to_string(),
        "notes",
        READY_TIMEOUT,
    )
    .await;
    let system_author = Value::Row {
        id: None,
        values: vec![
            Value::Uuid(ObjectId::from_uuid(
                jazz::account_registry::SYSTEM_ACCOUNT_ID.0,
            )),
            Value::Row {
                id: None,
                values: vec![
                    Value::Text("urn:jazz:system".into()),
                    Value::Text(backend_node.to_string()),
                ],
            },
        ],
    };

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, "alice note").await;
    let query = Query::from("notes")
        .select(["title", "$createdBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees both her own row and the allowed system-authored row",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == alice_note)
                && rows.iter().any(|(id, _)| *id == system_note))
            .then_some(rows)
        },
    )
    .await;
    let alice_owned = alice_rows
        .iter()
        .find(|(id, _)| *id == alice_note)
        .expect("alice-owned row should be visible");
    assert_eq!(
        alice_owned.1,
        vec![Value::from("alice note"), alice_author.clone()]
    );
    let system_owned = alice_rows
        .iter()
        .find(|(id, _)| *id == system_note)
        .expect("system-authored row should be visible");
    assert_eq!(
        system_owned.1,
        vec![Value::from("server-generated"), system_author.clone()]
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob sees only the allowed system-authored row",
        |rows| (rows.len() == 1 && rows[0].0 == system_note).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![Value::from("server-generated"), system_author]
    );

    backend.shutdown().await.expect("shutdown backend");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that backend writes can keep backend permissions while stamping
/// row authorship as `alice`, so `$createdBy` policies treat the row as hers.
///
/// Actors: a backend runtime creates one row with `alice` attribution and both
/// users query under a creator-only policy.
///
/// ```text
/// backend runtime ─create(attribution=alice)──► server ──$createdBy = alice
/// alice query ────────────────────────────────► sees attributed row
/// bob query ──────────────────────────────────► sees nothing
/// ```
#[tokio::test]
async fn created_by_policies_allow_backend_attribution_to_specific_user() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_allow_backend_attribution_to_specific_user_inner())
        .await;
}

async fn created_by_policies_allow_backend_attribution_to_specific_user_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().never();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice_context, alice) = jazz_testkit::TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::ALICE_ID)
        .as_user()
        .ready_on("notes", READY_TIMEOUT)
        .connect_with_context()
        .await;
    let alice_account = alice_context.account_id.expect("enrolled Alice account");
    let alice_author = structured_author(super::ALICE_ID, Some(alice_account));
    let mut alice_session = Session::new("urn:jazz:test", super::ALICE_ID);
    alice_session.account_id = Some(alice_account);
    let attribution = alice_session
        .author_subject()
        .expect("Alice author")
        .canonical()
        .to_owned();
    let (bob, _bob_author) = connect_author(&server, &schema, super::BOB_ID).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let error = bob
        .with_write_context(WriteContext {
            attribution: Some(attribution.to_owned()),
            ..Default::default()
        })
        .insert("notes", note_input("forged attribution"))
        .expect_err("ordinary users cannot attribute writes to Alice");
    assert!(
        error
            .to_string()
            .contains("attribution requires a trusted serving node"),
        "{error}"
    );

    let attributed_note =
        create_note_with_backend_attribution(&backend, &attribution, "backend for alice").await;
    let query = Query::from("notes").select(["title", "$createdBy", "$updatedBy"]);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees the backend-attributed row as her own",
        |rows| (rows.len() == 1 && rows[0].0 == attributed_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![
            "backend for alice".into(),
            alice_author.clone(),
            alice_author
        ]
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob cannot see alice-attributed backend row",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

    backend.shutdown().await.expect("shutdown backend");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Attribution survives transaction staging and commit without replacing
/// backend authorization, even when every user mutation policy denies writes.
#[tokio::test]
async fn backend_attribution_survives_transactions_and_later_mutations() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(make_notes_schema(
                    "notes",
                    permissions(|p| {
                        p.allow_read().always();
                        p.allow_insert().never();
                        p.allow_update().never();
                        p.allow_delete().never();
                    }),
                ))
                .build();
            let server = JazzServer::builder()
                .with_schema(schema.clone())
                .start()
                .await
                .expect("start test server");
            let (context, alice) = jazz_testkit::TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id(super::ALICE_ID)
                .as_user()
                .ready_on("notes", READY_TIMEOUT)
                .connect_with_context()
                .await;
            let account = context.account_id.expect("enrolled account");
            let author = structured_author(super::ALICE_ID, Some(account));
            let mut session = Session::new("urn:jazz:test", super::ALICE_ID);
            session.account_id = Some(account);
            let backend =
                connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;
            let attributed = backend.with_write_context(WriteContext {
                attribution: Some(session.author_subject().unwrap().canonical().to_owned()),
                ..Default::default()
            });
            let transaction = attributed
                .begin_transaction()
                .expect("begin attributed transaction");
            let (id, _, _) = transaction
                .insert("notes", note_input("staged"))
                .expect("stage insert");
            let query = Query::from("notes").select(["title", "$createdBy", "$updatedBy"]);
            let staged = transaction
                .query(query.clone(), jazz::tools::ReadTier::LocalFirst)
                .await
                .map(jazz::tools::test_support::ordinary_rows)
                .expect("read staged attribution");
            assert_eq!(
                staged[0].1,
                vec!["staged".into(), author.clone(), author.clone()]
            );
            wait_for_edge_txs(
                &backend,
                &[transaction.commit().expect("commit attribution")],
            )
            .await;
            let update = attributed
                .update("notes", id, vec![("title".into(), "updated".into())])
                .expect("attributed update")
                .unwrap();
            wait_for_edge_txs(&backend, &[update]).await;
            let updated = wait_for_rows(
                &alice,
                query.clone(),
                "Alice sees attributed update",
                |rows| {
                    (rows.len() == 1 && rows[0].1[0] == Value::Text("updated".into()))
                        .then_some(rows)
                },
            )
            .await;
            assert_eq!(
                updated[0].1,
                vec!["updated".into(), author.clone(), author.clone()]
            );
            let upsert = attributed
                .upsert("notes", *id.uuid(), note_input("upserted"))
                .expect("attributed upsert");
            wait_for_edge_txs(&backend, &[upsert.expect("upsert transaction")]).await;
            let rows = wait_for_rows(&alice, query, "Alice sees attributed mutations", |rows| {
                (rows.len() == 1 && rows[0].1[0] == Value::Text("upserted".into())).then_some(rows)
            })
            .await;
            assert_eq!(rows[0].1, vec!["upserted".into(), author.clone(), author]);
            wait_for_edge_txs(
                &backend,
                &[attributed
                    .delete("notes", id)
                    .expect("attributed delete")
                    .unwrap()],
            )
            .await;
            wait_for_rows(
                &alice,
                Query::from("notes"),
                "attributed delete settles",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            backend.shutdown().await.expect("shutdown backend");
            alice.shutdown().await.expect("shutdown Alice");
            server.shutdown().await;
        })
        .await;
}

/// Verifies that a `$updatedBy` select policy moves visibility to the latest
/// editor and preserves creator timestamps across edits using only session
/// clients.
///
/// Actors: `alice` creates the row, `bob` performs the later update.
///
/// ```text
/// alice client ──create(shared=true)──► server ──query──► alice and bob see row
/// bob client ───update(shared=false)─► server ──$updatedBy = bob
///                                       ├── alice query──► row hidden
///                                       └── bob query────► row visible
/// ```
#[tokio::test]
async fn updated_by_select_policy_moves_visibility_to_last_editor() {
    tokio::task::LocalSet::new()
        .run_until(updated_by_select_policy_moves_visibility_to_last_editor_inner())
        .await;
}

async fn updated_by_select_policy_moves_visibility_to_last_editor_inner() {
    let updated_by_policy = pe::eq("$updatedBy", pe::session("user"));
    let shared_policy = pe::eq("shared", true);
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("shared", ColumnType::Boolean)
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::any_of([shared_policy, updated_by_policy]));
                    p.allow_insert().always();
                    p.allow_update().always();
                })),
        )
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice, alice_author) = connect_author(&server, &schema, super::ALICE_ID).await;
    let (bob, bob_author) = connect_author(&server, &schema, super::BOB_ID).await;
    let query = Query::from("notes").select([
        "title",
        "shared",
        "$createdBy",
        "$updatedBy",
        "$createdAt",
        "$updatedAt",
    ]);
    // The shared flag bootstraps the row into Bob's local state before the
    // `$updatedBy` handoff on the later update.
    let note_id = alice
        .insert(
            "notes",
            jazz::row_input!("title" => "draft", "shared" => true),
        )
        .expect("alice creates shared draft")
        .0;

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees initial provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows[0].1[0], Value::from("draft"));
    assert_eq!(initial_rows[0].1[1], Value::from(true));
    assert_eq!(initial_rows[0].1[2], alice_author.clone());
    assert_eq!(initial_rows[0].1[3], alice_author.clone());
    let Value::Timestamp(initial_created_at) = initial_rows[0].1[4] else {
        panic!("$createdAt should decode as timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial_rows[0].1[5] else {
        panic!("$updatedAt should decode as timestamp")
    };

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees the shared draft before takeover",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1[0], Value::from("draft"));
    assert_eq!(bob_rows[0].1[1], Value::from(true));
    assert_eq!(bob_rows[0].1[2], alice_author.clone());
    assert_eq!(bob_rows[0].1[3], alice_author.clone());

    let bob_update = bob
        .update(
            "notes",
            note_id,
            vec![
                ("title".to_string(), "revised by bob".into()),
                ("shared".to_string(), false.into()),
            ],
        )
        .expect("bob becomes latest updater")
        .expect("ordinary Bob update commits immediately");
    wait_for_edge_txs(&bob, &[bob_update]).await;

    let alice_rows = tokio::time::timeout(
        READY_TIMEOUT,
        wait_for_rows(
            &alice,
            query.clone(),
            "alice no longer sees bob-updated row",
            |rows| rows.is_empty().then_some(rows),
        ),
    )
    .await
    .expect("Alice's removal query must not stall after Bob's update reaches edge");
    assert!(alice_rows.is_empty());

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees row after becoming latest updater",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1[0], Value::from("revised by bob"));
    assert_eq!(bob_rows[0].1[1], Value::from(false));
    assert_eq!(bob_rows[0].1[2], alice_author.clone());
    assert_eq!(bob_rows[0].1[3], bob_author.clone());
    let Value::Timestamp(updated_created_at) = bob_rows[0].1[4] else {
        panic!("updated $createdAt should decode as timestamp")
    };
    let Value::Timestamp(updated_updated_at) = bob_rows[0].1[5] else {
        panic!("updated $updatedAt should decode as timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert!(updated_updated_at >= initial_updated_at);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that provenance magic columns expose user-authored principals and
/// insert timestamps in ordinary end-to-end queries.
///
/// Actors: `alice` and `bob`, each writing one note through their own session
/// client and reading through an unrestricted query.
///
/// ```text
/// alice client ──create────► server ──► unrestricted query
/// bob client ───create────► server ──► unrestricted query
/// ```
#[tokio::test]
async fn provenance_columns_expose_user_principals_and_insert_timestamps() {
    tokio::task::LocalSet::new()
        .run_until(provenance_columns_expose_user_principals_and_insert_timestamps_inner())
        .await;
}

async fn provenance_columns_expose_user_principals_and_insert_timestamps_inner() {
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().always();
                p.allow_insert().always();
                p.allow_update().always();
                p.allow_delete().always();
            }),
        ))
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await
        .expect("start test server");
    let (alice, alice_author) = connect_author(&server, &schema, super::ALICE_ID).await;
    let (bob, bob_author) = connect_author(&server, &schema, super::BOB_ID).await;

    let alice_note = create_note_as(&alice, "alice note").await;
    let bob_note = create_note_as(&bob, "bob note").await;

    let query = Query::from("notes")
        .select([
            "title",
            "$createdBy",
            "$updatedBy",
            "$createdAt",
            "$updatedAt",
        ])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let rows = wait_for_rows(
        &alice,
        query,
        "alice sees provenance columns for both user rows",
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    let alice_row = rows
        .iter()
        .find(|(id, _)| *id == alice_note)
        .expect("alice-authored row should be present");
    assert_eq!(alice_row.1[0], Value::from("alice note"));
    assert_eq!(alice_row.1[1], alice_author.clone());
    assert_eq!(alice_row.1[2], alice_author.clone());
    let Value::Timestamp(alice_created_at) = alice_row.1[3] else {
        panic!("alice $createdAt should decode as timestamp")
    };
    let Value::Timestamp(alice_updated_at) = alice_row.1[4] else {
        panic!("alice $updatedAt should decode as timestamp")
    };
    assert_eq!(alice_created_at, alice_updated_at);

    let bob_row = rows
        .iter()
        .find(|(id, _)| *id == bob_note)
        .expect("bob-authored row should be present");
    assert_eq!(bob_row.1[0], Value::from("bob note"));
    assert_eq!(bob_row.1[1], bob_author.clone());
    assert_eq!(bob_row.1[2], bob_author.clone());
    let Value::Timestamp(bob_created_at) = bob_row.1[3] else {
        panic!("bob $createdAt should decode as timestamp")
    };
    let Value::Timestamp(bob_updated_at) = bob_row.1[4] else {
        panic!("bob $updatedAt should decode as timestamp")
    };
    assert_eq!(bob_created_at, bob_updated_at);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// A backend explicitly acting as Alice must enforce her read permissions
/// before merging updates, both ordinarily and inside a transaction.
/// Its underlying SYSTEM identity must not grant access to Bob's hidden note.
#[tokio::test]
async fn backend_session_updates_enforce_local_read_permissions() {
    tokio::task::LocalSet::new()
        .run_until(backend_session_updates_enforce_local_read_permissions_inner())
        .await;
}

async fn backend_session_updates_enforce_local_read_permissions_inner() {
    let owner_policy = pe::eq("owner", pe::session(vec!["claims", "sub"]));
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read().where_(owner_policy.clone());
                    p.allow_insert().always();
                    p.allow_update()
                        .where_old(owner_policy.clone())
                        .where_new(owner_policy);
                })),
        )
        .build();
    let backend = JazzClient::test_client(schema).await;
    let (note_id, _, _) = backend
        .insert(
            "notes",
            jazz::row_input!("title" => "Bob's note", "owner" => super::BOB_ID),
        )
        .expect("backend creates Bob's note");
    let mut session = Session::new("urn:jazz:test", super::ALICE_ID);
    session.account_id = Some(jazz::account_registry::AccountId(uuid::Uuid::from_u128(
        0xa11ce,
    )));
    let alice = backend.for_session(session);
    let patch = vec![("title".to_owned(), Value::Text("Alice's edit".to_owned()))];
    let error = alice
        .update("notes", note_id, patch.clone())
        .expect_err("explicit Alice session cannot merge Bob's hidden note");
    assert!(error.to_string().contains("read policy denied"), "{error}");
    let transaction = alice.begin_transaction().expect("begin Alice transaction");
    let error = transaction
        .update("notes", note_id, patch)
        .expect_err("transaction retains explicit Alice session permissions");
    assert!(error.to_string().contains("read policy denied"), "{error}");
    transaction.rollback().expect("rollback Alice transaction");
    let rows = backend
        .query(
            Query::from("notes").select(["title"]),
            jazz::tools::ReadTier::LocalFirst,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("backend reads unchanged note");
    assert_eq!(
        rows,
        vec![(note_id, vec![Value::Text("Bob's note".to_owned())])]
    );
    backend.shutdown().await.expect("shutdown backend");
}
