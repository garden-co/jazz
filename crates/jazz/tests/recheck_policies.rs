//! Old policy and permission-advice reports rechecked against the SQLite
//! engine: each test pins the behaviour its issue asked for.
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::common;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    UpdateOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};

use common::{compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn me(column: &str) -> PolicyExpr {
    session_eq(column, &ME)
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn row(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn authority(schema: JazzSchema) -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

/// A replica connected to `server` as `author`.
fn replica(schema: JazzSchema, server: &Db, node: u8, author: AuthorSubject) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    let (client_side, server_side) = jazz::db::duplex();
    block_on(db.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
    db.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    db
}

fn seed(server: &Db, table: &str, id: RowUuid, pairs: Vec<(&str, Value)>) {
    block_on(server.insert(
        table,
        cells(pairs),
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.tick()).unwrap();
}

/// Poll `future`, ticking `dbs` between polls, until it completes.
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn pump(dbs: &[&Db]) {
    for _ in 0..8 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

fn advise(client: &Db, server: &Db, action: PermissionAdviceAction) -> PermissionAdvice {
    let id = client.request_permission_advice(action, None);
    drive(&[client, server, client], client.permission_advice(id))
}

fn ids(rows: &[jazz::db::CurrentRow]) -> Vec<RowUuid> {
    let mut ids: Vec<RowUuid> = rows.iter().map(|row| row.row_uuid()).collect();
    ids.sort();
    ids
}

/// The tiers an app reads a row it has not loaded yet with: the server's
/// answer, or the local store once the server answered the first load.
fn first_read_tiers() -> [ReadOpts; 2] {
    [
        ReadOpts::for_read_tier(ReadTier::Remote),
        ReadOpts {
            first_load: jazz::db::FirstLoad::WaitForRemote { timeout_ms: 60_000 },
            ..ReadOpts::for_read_tier(ReadTier::LocalFirst)
        },
    ]
}

/// A read through the bindings' entry point, as `db.all` in TypeScript
/// issues it, optionally scoped to a request's session.
fn serialized_read(
    db: &Db,
    server: &Db,
    query: &Query,
    opts: ReadOpts,
    scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
) -> Vec<jazz::db::CurrentRow> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let read = drive(
        &[db, server, db],
        db.all_serialized_query(&bytes, opts, None, scope, None),
    );
    match read.unwrap() {
        SerializedReadResult::Rows(rows) => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn all_ids(db: &Db, table: &str) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    ids(&block_on(db.all(&prepared, ReadOpts::default())).unwrap())
}

// ---- reads through a relational read policy -------------------------------

/// Todos readable by their owner, or by a user a share row names.
fn shared_todos() -> JazzSchema {
    let writes = |read: PolicyExpr| {
        TablePolicies::new()
            .with_select(read)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(writes(PolicyExpr::or(vec![
                        me("owner"),
                        exists("todo_shares", vec![outer_eq("todo", "id"), me("user_id")]),
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("todo_shares")
                    .fk_column("todo", "todos")
                    .column("user_id", ColumnType::Text)
                    .policies(writes(me("user_id"))),
            )
            .build(),
    )
}

/// Alice's todo, shared with bob, and one she keeps to herself.
fn seed_shared_todo(server: &Db) -> RowUuid {
    let shared = row(1, 1);
    seed(
        server,
        "todos",
        shared,
        vec![
            ("owner", subject(alice())),
            ("title", Value::String("shared".into())),
        ],
    );
    seed(
        server,
        "todos",
        row(1, 2),
        vec![
            ("owner", subject(alice())),
            ("title", Value::String("private".into())),
        ],
    );
    seed(
        server,
        "todo_shares",
        row(2, 1),
        vec![("todo", uuid(shared)), ("user_id", subject(bob()))],
    );
    shared
}

/// #3779: a backend's request-scoped read returns every row the user can
/// read through an `exists` policy, without the scope having read the
/// share table first.
#[test]
fn a_backend_request_scope_reads_rows_shared_through_another_table() {
    let server = authority(shared_todos());
    let shared = seed_shared_todo(&server);
    let backend = replica(shared_todos(), &server, 0xbe, AuthorSubject::SYSTEM);
    for opts in first_read_tiers() {
        let tier = opts.tier;
        let rows = serialized_read(
            &backend,
            &server,
            &Query::from("todos"),
            opts,
            Some((bob(), BTreeMap::new())),
        );
        assert_eq!(ids(&rows), [shared], "{tier:?}");
    }
}

/// #4031: a new session's first read of a row it can read through a
/// membership returns it before the session has loaded the membership, and
/// the session may then write it.
#[test]
fn a_new_sessions_first_policy_filtered_read_returns_its_rows() {
    let server = authority(shared_todos());
    let shared = seed_shared_todo(&server);
    for opts in first_read_tiers() {
        let tier = opts.tier;
        let bob_db = replica(shared_todos(), &server, 0xb0, bob());
        let query = Query::from("todos").filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(Value::Uuid(shared.0)),
        ));
        let rows = serialized_read(&bob_db, &server, &query, opts, None);
        assert_eq!(ids(&rows), [shared], "{tier:?}");
        let write = block_on(bob_db.update(
            "todos",
            shared,
            cells(vec![("title", Value::String(format!("{tier:?}")))]),
            UpdateOptions::default(),
        ));
        assert!(write.is_ok(), "{tier:?}: {write:?}");
        pump(&[&bob_db, &server, &bob_db]);
    }
}

// ---- permission advice ------------------------------------------------------

/// Groups their owner may update, and docs that inherit update from their
/// group.
fn grouped_docs() -> JazzSchema {
    let inherited = PolicyExpr::inherits(Operation::Update, "group");
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("groups")
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(me("owner"))
                            .with_insert(me("owner"))
                            .with_update(Some(me("owner")), me("owner")),
                    ),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .fk_column("group", "groups")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::inherits(Operation::Select, "group"))
                            .with_insert(inherited.clone())
                            .with_update(Some(inherited.clone()), inherited),
                    ),
            )
            .build(),
    )
}

/// #3556: update advice for a patch that moves a row into a parent no other
/// row references answers as the update would be decided.
#[test]
fn update_advice_follows_the_patchs_new_parent() {
    let server = authority(grouped_docs());
    for n in 1..=3 {
        seed(
            &server,
            "groups",
            row(1, n),
            vec![("owner", subject(alice()))],
        );
    }
    seed(
        &server,
        "groups",
        row(1, 9),
        vec![("owner", subject(bob()))],
    );
    for (n, group) in [(1, 1), (2, 2)] {
        seed(
            &server,
            "docs",
            row(2, n),
            vec![
                ("group", uuid(row(1, group))),
                ("title", Value::String(format!("d{n}"))),
            ],
        );
    }
    let alice_db = replica(grouped_docs(), &server, 0xa1, alice());
    let move_to = |group: u8| PermissionAdviceAction::Update {
        table: "docs".to_owned(),
        row: row(2, 1),
        patch: cells(vec![("group", uuid(row(1, group)))]),
    };
    use PermissionAdvice::{Allowed, Denied};
    assert_eq!(advise(&alice_db, &server, move_to(1)), Allowed);
    assert_eq!(advise(&alice_db, &server, move_to(2)), Allowed);
    assert_eq!(advise(&alice_db, &server, move_to(3)), Allowed);
    assert_eq!(advise(&alice_db, &server, move_to(9)), Denied);
}

/// Folders their owner may update; a subfolder may be created by whoever
/// may update an ancestor up to two levels up.
fn folders() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("owner", ColumnType::Text)
                    .nullable_fk_column("parent", "folders")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::or(vec![
                                PolicyExpr::and(vec![
                                    PolicyExpr::IsNull {
                                        column: "parent".to_owned(),
                                    },
                                    me("owner"),
                                ]),
                                PolicyExpr::inherits_with_depth(Operation::Update, "parent", 2),
                            ]))
                            .with_update(Some(me("owner")), me("owner")),
                    ),
            )
            .build(),
    )
}

/// #3769: insert advice under a bounded recursive `allowedTo` answers
/// Allowed or Denied, not Unknown.
#[test]
fn insert_advice_decides_a_bounded_recursive_inherit() {
    let server = authority(folders());
    seed(
        &server,
        "folders",
        row(1, 1),
        vec![
            ("owner", subject(alice())),
            ("parent", Value::Nullable(None)),
        ],
    );
    let alice_db = replica(folders(), &server, 0xa1, alice());
    let bob_db = replica(folders(), &server, 0xb0, bob());
    let child = |owner: AuthorSubject| PermissionAdviceAction::Insert {
        table: "folders".to_owned(),
        cells: cells(vec![
            ("owner", subject(owner)),
            ("parent", Value::Nullable(Some(Box::new(uuid(row(1, 1)))))),
        ]),
    };
    assert_eq!(
        advise(&alice_db, &server, child(alice())),
        PermissionAdvice::Allowed
    );
    assert_eq!(
        advise(&bob_db, &server, child(bob())),
        PermissionAdvice::Denied
    );
}

// ---- inherited select policies on writes ------------------------------------

/// Messages readable by members of their room; a reaction may be added to
/// a message its author can read.
fn rooms() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("room_members")
                    .fk_column("room", "rooms")
                    .column("user_id", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(me("user_id"))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(exists(
                        "room_members",
                        vec![outer_eq("room", "room"), me("user_id")],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("reactions")
                    .fk_column("message", "messages")
                    .column("emoji", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::inherits(Operation::Select, "message")),
                    ),
            )
            .build(),
    )
}

/// #1875: `allowedTo.read(message)` inherits the message's correlated
/// `exists` read policy: a member may react, a stranger may not.
#[test]
fn allowed_to_read_inherits_a_correlated_exists_read_policy() {
    let server = authority(rooms());
    seed(
        &server,
        "rooms",
        row(1, 1),
        vec![("name", Value::String("lobby".into()))],
    );
    seed(
        &server,
        "room_members",
        row(2, 1),
        vec![("room", uuid(row(1, 1))), ("user_id", subject(alice()))],
    );
    seed(
        &server,
        "messages",
        row(3, 1),
        vec![
            ("room", uuid(row(1, 1))),
            ("body", Value::String("hi".into())),
        ],
    );
    let alice_db = replica(rooms(), &server, 0xa1, alice());
    let bob_db = replica(rooms(), &server, 0xb0, bob());
    let reaction = || PermissionAdviceAction::Insert {
        table: "reactions".to_owned(),
        cells: cells(vec![
            ("message", uuid(row(3, 1))),
            ("emoji", Value::String("+1".into())),
        ]),
    };
    assert_eq!(
        advise(&alice_db, &server, reaction()),
        PermissionAdvice::Allowed
    );
    assert_eq!(
        advise(&bob_db, &server, reaction()),
        PermissionAdvice::Denied
    );
    for (db, n) in [(&alice_db, 1), (&bob_db, 2)] {
        block_on(db.insert(
            "reactions",
            cells(vec![
                ("message", uuid(row(3, 1))),
                ("emoji", Value::String("+1".into())),
            ]),
            InsertOptions {
                row_id: Some(row(4, n)),
                ..Default::default()
            },
        ))
        .unwrap();
        pump(&[db, &server, db]);
    }
    assert_eq!(all_ids(&server, "reactions"), [row(4, 1)]);
}

// ---- claims compared with nullable and required columns ---------------------

/// Documents readable by their (optional) owner account, or by members of
/// their organization, both compared with the session's account claim.
fn documents(ownership_first: bool) -> JazzSchema {
    let account = || PolicyValue::SessionRef(vec!["claims".to_owned(), "account".to_owned()]);
    let owned = PolicyExpr::Cmp {
        column: "owner_account".to_owned(),
        op: CmpOp::Eq,
        value: account(),
    };
    let member = PolicyExpr::Exists {
        table: "memberships".to_owned(),
        condition: Box::new(PolicyExpr::and(vec![
            outer_eq("organization", "organization"),
            PolicyExpr::Cmp {
                column: "account".to_owned(),
                op: CmpOp::Eq,
                value: account(),
            },
        ])),
    };
    let read = if ownership_first {
        PolicyExpr::or(vec![owned, member])
    } else {
        PolicyExpr::or(vec![member, owned])
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("organizations")
                    .column("name", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("organization", "organizations")
                    .column("account", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .nullable_column("owner_account", ColumnType::Uuid)
                    .fk_column("organization", "organizations")
                    .policies(TablePolicies::new().with_select(read)),
            )
            .build(),
    )
}

/// #3981: a read policy comparing one account claim with a nullable and a
/// required UUID column answers the same rows in either branch order.
#[test]
fn an_account_claim_compares_with_nullable_and_required_uuids_in_either_order() {
    let account = uuid::Uuid::from_bytes([0xac; 16]);
    let org = uuid::Uuid::from_bytes([0x06; 16]);
    let other = uuid::Uuid::from_bytes([0x07; 16]);
    for ownership_first in [true, false] {
        let db = authority(documents(ownership_first));
        for organization in [org, other] {
            block_on(db.insert(
                "organizations",
                cells(vec![("name", Value::String(organization.to_string()))]),
                InsertOptions {
                    row_id: Some(RowUuid(organization)),
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        seed(
            &db,
            "memberships",
            row(1, 1),
            vec![
                ("organization", Value::Uuid(org)),
                ("account", Value::Uuid(account)),
            ],
        );
        let document = |n: u8, owner: Option<uuid::Uuid>, organization: uuid::Uuid| {
            seed(
                &db,
                "documents",
                row(2, n),
                vec![
                    (
                        "owner_account",
                        Value::Nullable(owner.map(|o| Box::new(Value::Uuid(o)))),
                    ),
                    ("organization", Value::Uuid(organization)),
                ],
            );
            row(2, n)
        };
        let personal = document(1, Some(account), other);
        let organizational = document(2, None, org);
        document(3, None, other);
        document(4, Some(other), other);
        let prepared = db
            .prepare_query(&Query::from("documents"))
            .unwrap()
            .with_identity_claims(
                alice(),
                BTreeMap::from([(
                    jazz::query::provider_claim_key("account"),
                    Value::String(account.to_string()),
                )]),
            );
        let rows = block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice()));
        let mut expected = vec![personal, organizational];
        expected.sort();
        assert_eq!(
            ids(&rows.unwrap()),
            expected,
            "ownership first: {ownership_first}"
        );
    }
}

// ---- serializable reads under an owner policy -------------------------------

fn owned_notes() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(me("owner"))
                            .with_insert(me("owner"))
                            .with_update(Some(me("owner")), me("owner"))
                            .with_delete(me("owner")),
                    ),
            )
            .build(),
    )
}

/// #3891: a serializable transaction that reads a whole owner-scoped table
/// and writes is accepted while another session without claims is
/// subscribed to the same table.
#[test]
fn a_whole_table_serializable_read_under_an_owner_policy_is_not_a_false_conflict() {
    let server = authority(owned_notes());
    seed(
        &server,
        "notes",
        row(1, 1),
        vec![
            ("owner", subject(alice())),
            ("title", Value::String("mine".into())),
        ],
    );
    seed(
        &server,
        "notes",
        row(1, 2),
        vec![
            ("owner", subject(bob())),
            ("title", Value::String("bob's".into())),
        ],
    );
    let bob_db = replica(owned_notes(), &server, 0xb0, bob());
    let prepared = bob_db.prepare_query(&Query::from("notes")).unwrap();
    let _bob_stream = block_on(bob_db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let alice_db = replica(owned_notes(), &server, 0xa1, alice());
    pump(&[&bob_db, &alice_db, &server, &bob_db, &alice_db]);

    let open = jazz::tools::OpenTransactionId::new();
    block_on(alice_db.begin_serializable(open)).unwrap();
    let all = alice_db.prepare_query(&Query::from("notes")).unwrap();
    let read = drive(
        &[&alice_db, &server, &bob_db, &alice_db],
        alice_db.tx_ref(open).all_prepared(&all),
    )
    .unwrap();
    assert_eq!(ids(&read), [row(1, 1)]);
    drive(
        &[&alice_db, &server, &alice_db],
        alice_db.tx_ref(open).insert(
            "notes",
            cells(vec![
                ("owner", subject(alice())),
                ("title", Value::String("second".into())),
            ]),
            Default::default(),
        ),
    )
    .unwrap();
    let receipt = drive(
        &[&alice_db, &server, &alice_db],
        alice_db.commit_serializable_handle(open),
    )
    .unwrap();
    let settled = drive(
        &[&alice_db, &server, &bob_db, &alice_db],
        alice_db.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
    );
    assert!(settled.is_ok(), "{settled:?}");
    assert_eq!(all_ids(&server, "notes").len(), 3);
}
