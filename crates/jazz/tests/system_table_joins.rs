//! Correlations on a system table's two-column address (a table identity
//! and a row id): schema validation accepts them although the address
//! columns cannot be declared references, and a replica serving a read
//! loads the rows of one address, matched on both columns at once.
//!
//! The tables are ordinary tables; the members table carries the
//! system-table prefix, which is what scopes the validation rule.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::{BTreeMap, BTreeSet};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, Schema, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};

use common::{compile_schema, session_eq};

const MEMBERS: &str = "__e2ee_members";

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["user".to_owned()])
}

fn this(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), column.to_owned()])
}

fn cmp(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(conditions)),
    }
}

fn inherits_from(operation: Operation) -> PolicyExpr {
    PolicyExpr::InheritsFrom {
        operation,
        table_column: "scopeId".to_owned(),
        id_column: "identifier".to_owned(),
        tables: vec!["projects".to_owned()],
    }
}

/// The members of a space see each other: `MEMBER(session)`.
fn member_of_same_space() -> PolicyExpr {
    exists(
        MEMBERS,
        vec![
            cmp("scopeId", this("scopeId")),
            cmp("identifier", this("identifier")),
            cmp("accountId", me()),
        ],
    )
}

/// Each shape of correlation an address policy takes: `M(projects)` on the
/// space table (primary `identifier = this.id`), `MEMBER` on the members
/// table (primary `scopeId = this.scopeId`), the creator clause (a row-id
/// join on `identifier`) and `InheritsFrom` (the project's own policies
/// reached through `identifier`).
fn space_tables() -> SchemaBuilder {
    let member_of_this_project = exists(
        MEMBERS,
        vec![
            cmp(
                "scopeId",
                PolicyValue::TableIdentity {
                    table: "projects".to_owned(),
                },
            ),
            cmp("identifier", this("id")),
            cmp("accountId", me()),
        ],
    );
    let creator = exists(
        "projects",
        vec![
            cmp("id", this("identifier")),
            session_eq("$createdBy.account", &["user"]),
            PolicyExpr::InsertedByThisTransaction,
        ],
    );
    let manages = PolicyExpr::And(vec![
        inherits_from(Operation::Select),
        inherits_from(Operation::Update),
    ]);
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .column_with_default("hidden", ColumnType::Boolean, false.into())
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::And(vec![
                            member_of_this_project.clone(),
                            PolicyExpr::eq_literal("hidden", false.into()),
                        ]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(member_of_this_project), PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new(MEMBERS)
                .column("scopeId", ColumnType::Uuid)
                .column("identifier", ColumnType::Uuid)
                .column("accountId", ColumnType::Uuid)
                .composite_index(["scopeId", "identifier", "accountId"])
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::Or(vec![
                            cmp("accountId", me()),
                            member_of_same_space(),
                        ]))
                        .with_insert(PolicyExpr::Or(vec![
                            manages.clone(),
                            PolicyExpr::And(vec![cmp("accountId", me()), creator]),
                        ]))
                        .with_delete(PolicyExpr::Or(vec![cmp("accountId", me()), manages])),
                ),
        )
}

/// An address correlation with one side on a system table validates
/// without a declared reference, in every shape the address policies take.
/// In the same schema, an undeclared `uuid` equality between two
/// application tables is still refused, as it was: only the system-table
/// class is relaxed.
///
/// ```text
/// projects ◄── identifier ── __e2ee_members   ✓ (no reference declared)
/// projects ◄── projectId ─── tasks            ✗ (declare the reference)
/// ```
#[test]
fn an_address_correlation_validates_only_with_a_system_table_on_one_side() {
    let schema: Schema = space_tables().build();
    JazzSchema::new(&schema).expect("every address correlation validates");

    let with_tasks: Schema = space_tables()
        .table(
            TableSchemaBuilder::new("tasks")
                .column("projectId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(exists("projects", vec![cmp("id", this("projectId"))])),
                ),
        )
        .build();
    let error = format!("{:?}", JazzSchema::new(&with_tasks).unwrap_err());
    assert!(
        error.contains("tasks") && error.contains("projectId") && error.contains("projects"),
        "{error}"
    );
    let message = JazzSchema::new(&with_tasks).unwrap_err().to_string();
    assert!(
        message.contains(
            "column tasks.projectId is matched against the ids of projects, but it is not \
             declared as a reference"
        ),
        "{message}"
    );
}

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn account(who: AuthorSubject) -> Value {
    Value::Uuid(who.account_id().expect("a test account").0)
}

fn person(n: u8) -> Value {
    account(AuthorSubject::for_test_bytes([n; 16]))
}

/// A replica serving `MEMBER` (correlated on `scopeId` and `identifier`)
/// loads and watches the member rows of one space, matched on both columns
/// at once: not every space's of the same table, which share `scopeId`.
/// Alice, a member of two projects, reads her own member row of the first;
/// her replica receives the other members of that project (what her read
/// rests on) and nothing of the second project's, then or later.
///
/// ```text
/// server: P1 {alice, bob}, P2 {alice, carol}
/// alice ──subscribe members(identifier = P1, accountId = alice)──► server
///       ◄── alice@P1 (result), bob@P1 (support), not carol@P2 / alice@P2
/// server: + dave@P1, + erin@P2  ──► alice gets dave@P1 only
/// ```
#[test]
#[ignore = "needs multi-column sync support from main"]
fn a_two_column_correlation_loads_and_watches_one_space() {
    let server = block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(&space_tables().build()),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let projects = Value::Uuid(server.table_identity("projects").unwrap().unwrap().0);
    let (p1, p2) = (
        RowUuid::from_bytes([0x11; 16]),
        RowUuid::from_bytes([0x22; 16]),
    );
    let insert = |table: &str, cells: BTreeMap<String, Value>| {
        block_on(server.insert(table, cells, InsertOptions::default())).unwrap();
    };
    // The member rows alone decide who reads them: no project row is read.
    let member = |project: RowUuid, who: Value| {
        BTreeMap::from([
            ("scopeId".to_owned(), projects.clone()),
            ("identifier".to_owned(), Value::Uuid(project.0)),
            ("accountId".to_owned(), who),
        ])
    };
    for (project, who) in [
        (p1, account(alice())),
        (p1, person(0xb0)),
        (p2, account(alice())),
        (p2, person(0xc0)),
    ] {
        insert(MEMBERS, member(project, who));
    }

    let replica = block_on(Db::open(DbConfig::new(
        compile_schema(&space_tables().build()),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    let (client_side, server_side) = duplex();
    let _upstream = block_on(replica.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let mine = replica
        .prepare_query(
            &Query::from(MEMBERS)
                .filter(eq(col("identifier"), lit(Value::Uuid(p1.0))))
                .filter(eq(col("accountId"), lit(account(alice())))),
        )
        .unwrap();
    let _stream = block_on(replica.subscribe(&mine, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..20 {
            block_on(replica.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    // The member rows the replica holds, as (project, account).
    let held = || {
        let every = replica.prepare_query(&Query::from(MEMBERS)).unwrap();
        block_on(replica.all(&every, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.cell("identifier").expect("identifier"),
                    row.cell("accountId").expect("accountId"),
                )
            })
            .map(|(project, who)| (format!("{project:?}"), format!("{who:?}")))
            .collect::<BTreeSet<_>>()
    };
    let pair = |project: RowUuid, who: Value| {
        (format!("{:?}", Value::Uuid(project.0)), format!("{who:?}"))
    };
    pump();
    assert_eq!(
        held(),
        BTreeSet::from([pair(p1, account(alice())), pair(p1, person(0xb0))])
    );

    insert(MEMBERS, member(p1, person(0xd0)));
    insert(MEMBERS, member(p2, person(0xe0)));
    pump();
    assert_eq!(
        held(),
        BTreeSet::from([
            pair(p1, account(alice())),
            pair(p1, person(0xb0)),
            pair(p1, person(0xd0)),
        ])
    );
}

fn open_as(schema: &Schema, node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        compile_schema(schema),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn open_authority(schema: &Schema) -> Db {
    let authority = block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(schema),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    authority.set_authority();
    authority
}

fn insert_row(db: &Db, table: &str, id: Option<RowUuid>, cells: &[(&str, Value)]) {
    block_on(
        db.insert(
            table,
            cells
                .iter()
                .map(|(column, value)| ((*column).to_owned(), value.clone()))
                .collect(),
            InsertOptions {
                row_id: id,
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

/// `who` joins the space `identifier` of the table whose identity is
/// `scope`.
fn add_member(db: &Db, scope: &Value, identifier: RowUuid, who: Value) {
    insert_row(
        db,
        MEMBERS,
        None,
        &[
            ("scopeId", scope.clone()),
            ("identifier", Value::Uuid(identifier.0)),
            ("accountId", who),
        ],
    );
}

/// The member rows a replica holds, as (scope, space, account).
fn members_held(db: &Db) -> BTreeSet<(String, String, String)> {
    let every = db.prepare_query(&Query::from(MEMBERS)).unwrap();
    block_on(db.all(&every, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .into_iter()
        .map(|row| {
            let cell = |column: &str| format!("{:?}", row.cell(column).expect(column));
            (cell("scopeId"), cell("identifier"), cell("accountId"))
        })
        .collect()
}

fn member_held(scope: &Value, identifier: RowUuid, who: Value) -> (String, String, String) {
    (
        format!("{scope:?}"),
        format!("{:?}", Value::Uuid(identifier.0)),
        format!("{who:?}"),
    )
}

fn row_ids(db: &Db, table: &str) -> Vec<RowUuid> {
    let every = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&every, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

/// A relay that takes a subscription before it knows its catalogue
/// identities still ships the rows the read rests on once it learns them,
/// then and later. Alice reads projects through a relay that has not yet
/// heard from the authority, so the `M(projects)` membership her read
/// rests on names a table identity the relay cannot resolve. Once the relay
/// confirms its names, alice holds the project and its member rows, as a
/// subscription opened after confirmation would, and a member added later
/// reaches her too.
///
/// ```text
/// authority: P1, members {alice@P1, bob@P1}
/// alice ──subscribe projects──► relay (identities unknown)
/// relay ◄──catalogue, rows── authority
/// alice ◄── P1, alice@P1, bob@P1
/// authority: + carol@P1 ──► relay ──► alice
/// ```
#[test]
fn a_relay_subscribed_before_confirming_its_names_ships_the_membership() {
    let schema = space_tables().build();
    let authority = open_authority(&schema);
    let projects = Value::Uuid(authority.table_identity("projects").unwrap().unwrap().0);
    let p1 = RowUuid::from_bytes([0x11; 16]);
    insert_row(
        &authority,
        "projects",
        Some(p1),
        &[("name", Value::String("P1".into()))],
    );
    add_member(&authority, &projects, p1, account(alice()));
    add_member(&authority, &projects, p1, person(0xb0));

    let relay = open_as(&schema, 0xed, AuthorSubject::SYSTEM);
    let (relay_side, authority_side) = duplex();
    let _relay_upstream = block_on(relay.connect_upstream(relay_side));
    authority
        .accept_subscriber(authority_side, AuthorSubject::SYSTEM)
        .unwrap();

    let client = open_as(&schema, 0xa1, alice());
    let (client_side, relay_client_side) = duplex();
    let _client_upstream = block_on(client.connect_upstream(client_side));
    relay.accept_subscriber(relay_client_side, alice()).unwrap();
    let all_projects = client.prepare_query(&Query::from("projects")).unwrap();
    let _stream = block_on(client.subscribe(&all_projects, ReadOpts::default())).unwrap();

    // The relay takes alice's subscription before the authority has told it
    // anything.
    block_on(client.tick()).unwrap();
    block_on(relay.tick()).unwrap();
    assert_eq!(
        relay.table_identity("projects").unwrap_err().code,
        jazz::db::ErrorCode::NotObserved
    );

    let pump = || {
        for _ in 0..20 {
            block_on(client.tick()).unwrap();
            block_on(relay.tick()).unwrap();
            block_on(authority.tick()).unwrap();
        }
    };
    pump();
    assert_eq!(
        relay.table_identity("projects").unwrap(),
        authority.table_identity("projects").unwrap()
    );
    assert_eq!(row_ids(&client, "projects"), vec![p1]);
    assert_eq!(
        members_held(&client),
        BTreeSet::from([
            member_held(&projects, p1, account(alice())),
            member_held(&projects, p1, person(0xb0)),
        ])
    );

    add_member(&authority, &projects, p1, person(0xc0));
    pump();
    assert_eq!(
        members_held(&client),
        BTreeSet::from([
            member_held(&projects, p1, account(alice())),
            member_held(&projects, p1, person(0xb0)),
            member_held(&projects, p1, person(0xc0)),
        ])
    );
}

/// `M(T)` support follows the members of one table's spaces only: a space
/// of another table whose row has the same id is not the read's support.
/// Alice is a member of project X and of team X, which share an id; she
/// reads projects. Her replica receives project X's members and nothing of
/// team X's, although she may read those too, then or later.
///
/// ```text
/// server: project X {alice, bob}, team X {alice, carol}
/// alice ──subscribe projects──► server
///       ◄── X, alice@project X, bob@project X; not alice@team X, carol@team X
/// server: + dave@project X, + erin@team X ──► alice gets dave@project X only
/// ```
#[test]
#[ignore = "needs multi-column sync support from main"]
fn table_identity_correlation_follows_one_tables_spaces() {
    let member_of_this_team = exists(
        MEMBERS,
        vec![
            cmp(
                "scopeId",
                PolicyValue::TableIdentity {
                    table: "teams".to_owned(),
                },
            ),
            cmp("identifier", this("id")),
            cmp("accountId", me()),
        ],
    );
    let schema = space_tables()
        .table(
            TableSchemaBuilder::new("teams")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(member_of_this_team)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    let server = open_authority(&schema);
    let projects = Value::Uuid(server.table_identity("projects").unwrap().unwrap().0);
    let teams = Value::Uuid(server.table_identity("teams").unwrap().unwrap().0);
    let x = RowUuid::from_bytes([0x33; 16]);
    insert_row(
        &server,
        "projects",
        Some(x),
        &[("name", Value::String("project X".into()))],
    );
    insert_row(
        &server,
        "teams",
        Some(x),
        &[("name", Value::String("team X".into()))],
    );
    add_member(&server, &projects, x, account(alice()));
    add_member(&server, &projects, x, person(0xb0));
    add_member(&server, &teams, x, account(alice()));
    add_member(&server, &teams, x, person(0xc0));

    let replica = open_as(&schema, 0xa1, alice());
    let (client_side, server_side) = duplex();
    let _upstream = block_on(replica.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let all_projects = replica.prepare_query(&Query::from("projects")).unwrap();
    let _stream = block_on(replica.subscribe(&all_projects, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..20 {
            block_on(replica.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    assert_eq!(row_ids(&replica, "projects"), vec![x]);
    assert_eq!(
        members_held(&replica),
        BTreeSet::from([
            member_held(&projects, x, account(alice())),
            member_held(&projects, x, person(0xb0)),
        ])
    );

    add_member(&server, &projects, x, person(0xd0));
    add_member(&server, &teams, x, person(0xe0));
    pump();
    assert_eq!(
        members_held(&replica),
        BTreeSet::from([
            member_held(&projects, x, account(alice())),
            member_held(&projects, x, person(0xb0)),
            member_held(&projects, x, person(0xd0)),
        ])
    );
}

/// A correlation whose two sides store equal values as different cells
/// still loads and follows the rows it matches. Grants are matched to
/// documents on team and level, an enum column on both sides declared in
/// opposite orders, so each store numbers `high` differently in each
/// table. Alice reads a `high` document through one grant; a second
/// matching grant written later reaches her replica too, and one of
/// another team does not.
///
/// ```text
/// server: doc(team a, level high), g1(team a, level high, alice)
/// alice ──subscribe docs──► server ◄── doc, g1
/// server: + g2(team a, level high, alice), + g3(team b, level high, alice)
///       ──► alice gets g2 only
/// ```
#[test]
fn a_correlation_on_differently_stored_columns_follows_its_rows() {
    let level = |variants: [&str; 2]| ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    };
    let granted = exists(
        "grants",
        vec![
            cmp("team", this("team")),
            cmp("level", this("level")),
            cmp("accountId", me()),
        ],
    );
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("team", ColumnType::Text)
                .column("level", level(["low", "high"]))
                .policies(
                    TablePolicies::new()
                        .with_select(granted)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new("grants")
                .column("team", ColumnType::Text)
                .column("level", level(["high", "low"]))
                .column("accountId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(cmp("accountId", me()))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build();
    let server = open_authority(&schema);
    let doc = RowUuid::from_bytes([0x44; 16]);
    let (g1, g2, g3) = (
        RowUuid::from_bytes([0x51; 16]),
        RowUuid::from_bytes([0x52; 16]),
        RowUuid::from_bytes([0x53; 16]),
    );
    let grant = |id: RowUuid, team: &str| {
        insert_row(
            &server,
            "grants",
            Some(id),
            &[
                ("team", Value::String(team.into())),
                ("level", Value::String("high".into())),
                ("accountId", account(alice())),
            ],
        );
    };
    insert_row(
        &server,
        "docs",
        Some(doc),
        &[
            ("team", Value::String("a".into())),
            ("level", Value::String("high".into())),
        ],
    );
    grant(g1, "a");

    let replica = open_as(&schema, 0xa1, alice());
    let (client_side, server_side) = duplex();
    let _upstream = block_on(replica.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let docs = replica.prepare_query(&Query::from("docs")).unwrap();
    let _stream = block_on(replica.subscribe(&docs, ReadOpts::default())).unwrap();
    let pump = || {
        for _ in 0..20 {
            block_on(replica.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    pump();
    assert_eq!(row_ids(&replica, "docs"), vec![doc]);
    assert_eq!(row_ids(&replica, "grants"), vec![g1]);

    grant(g2, "a");
    grant(g3, "b");
    pump();
    assert_eq!(
        row_ids(&replica, "grants")
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([g1, g2])
    );
}
