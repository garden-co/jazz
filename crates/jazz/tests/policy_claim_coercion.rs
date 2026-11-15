//! A session claim compares with a column the same way whichever policy
//! form names it: a row's own column (`session_eq`), a legacy
//! `table.exists.where(...)` (`PolicyExpr::Exists`) or a relation-style
//! `policy.exists(policy.table.where(...))` (`PolicyExpr::ExistsRel`).
//!
//! Claims reach policies as JSON (a JWT, or a JS object through a binding),
//! so a UUID or an integer can arrive as its text. Each form must coerce
//! such a claim to the column's type, for reads and for write checks, and
//! must still deny a claim that names someone else.
//!
//! ```
//! alice (claims.who = "<uuid>" | 42 | "42")
//!   ──read / insert──► docs_<form>_<column>
//!                        │
//!                        └── policy: members.<column> = session.claims.who
//! ```

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationValueRef,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use serde_json::json;

use common::{compile_schema, exists, session_eq};

const WHO: [&str; 2] = ["claims", "who"];
const NUMBER: i32 = 42;

/// Alice's account row id, also stored in `members.user_uuid`.
fn alice_uuid() -> uuid::Uuid {
    uuid::Uuid::from_bytes([0xa1; 16])
}

/// How a policy names the claim comparison.
#[derive(Clone, Copy, Debug)]
enum Form {
    /// The row's own column: `{ owner: session.claims.who }`.
    Direct,
    /// `members.exists.where({ <column>: session.claims.who })`.
    Exists,
    /// `policy.exists(policy.members.where({ <column>: session.claims.who }))`.
    Relation,
}

const FORMS: [Form; 3] = [Form::Direct, Form::Exists, Form::Relation];

/// The column a claim is compared with.
#[derive(Clone, Copy, Debug)]
enum Column {
    /// The member's row id.
    RowId,
    Uuid,
    Text,
    Int,
}

const COLUMNS: [Column; 4] = [Column::RowId, Column::Uuid, Column::Text, Column::Int];

impl Column {
    fn member_column(self) -> &'static str {
        match self {
            Column::RowId => "id",
            Column::Uuid => "user_uuid",
            Column::Text => "user_text",
            Column::Int => "user_int",
        }
    }

    fn doc_column(self) -> Option<&'static str> {
        match self {
            Column::RowId => None,
            Column::Uuid => Some("owner_uuid"),
            Column::Text => Some("owner_text"),
            Column::Int => Some("owner_int"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Column::RowId => "id",
            Column::Uuid => "uuid",
            Column::Text => "text",
            Column::Int => "int",
        }
    }
}

fn table(form: Form, column: Column) -> String {
    format!("docs_{form:?}_{}", column.name()).to_lowercase()
}

fn policy(form: Form, column: Column) -> Option<PolicyExpr> {
    Some(match form {
        Form::Direct => session_eq(column.doc_column()?, &WHO),
        Form::Exists => exists("members", vec![session_eq(column.member_column(), &WHO)]),
        Form::Relation => PolicyExpr::ExistsRel {
            rel: RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "members".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("members".to_owned()),
                        column: column.member_column().to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::SessionRef(
                        WHO.iter().map(|s| (*s).to_owned()).collect(),
                    ),
                },
            },
        },
    })
}

fn cases() -> Vec<(Form, Column, PolicyExpr)> {
    FORMS
        .into_iter()
        .flat_map(|form| COLUMNS.into_iter().map(move |column| (form, column)))
        .filter_map(|(form, column)| Some((form, column, policy(form, column)?)))
        .collect()
}

fn owner_cells(uuid: uuid::Uuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner_uuid".to_owned(), Value::Uuid(uuid)),
        ("owner_text".to_owned(), Value::String(uuid.to_string())),
        ("owner_int".to_owned(), Value::I32(NUMBER)),
    ])
}

fn open() -> Db {
    let mut schema = SchemaBuilder::new().table(
        TableSchemaBuilder::new("members")
            .column("user_uuid", ColumnType::Uuid)
            .column("user_text", ColumnType::Text)
            .column("user_int", ColumnType::Integer)
            .policies(TablePolicies::new().with_select(PolicyExpr::True)),
    );
    for (form, column, policy) in cases() {
        schema = schema.table(
            TableSchemaBuilder::new(&table(form, column))
                .column("owner_uuid", ColumnType::Uuid)
                .column("owner_text", ColumnType::Text)
                .column("owner_int", ColumnType::Integer)
                .policies(
                    TablePolicies::new()
                        .with_select(policy.clone())
                        .with_insert(policy),
                ),
        );
    }
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema.build()),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let alice = alice_uuid();
    let mut member = owner_cells(alice);
    member = member
        .into_iter()
        .map(|(name, value)| (name.replace("owner_", "user_"), value))
        .collect();
    block_on(db.insert(
        "members",
        member,
        InsertOptions {
            row_id: Some(RowUuid(alice)),
            ..Default::default()
        },
    ))
    .unwrap();
    for (form, column, _) in cases() {
        block_on(db.insert(&table(form, column), owner_cells(alice), Default::default())).unwrap();
    }
    db
}

/// How a claim reaches the policy.
#[derive(Clone, Debug)]
enum Claim {
    /// Admitted as JSON, as a JWT or a JS claims object is.
    Json(serde_json::Value),
    /// Bound directly by a Rust host.
    Raw(Value),
}

/// Claims naming alice, in every spelling a host may send, per column.
fn alice_claims(column: Column) -> Vec<Claim> {
    let uuid = alice_uuid();
    match column {
        Column::RowId | Column::Uuid | Column::Text => vec![
            Claim::Json(json!(uuid.to_string())),
            Claim::Raw(Value::String(uuid.to_string())),
            Claim::Raw(Value::Uuid(uuid)),
        ],
        Column::Int => vec![
            Claim::Json(json!(NUMBER)),
            Claim::Json(json!(NUMBER.to_string())),
            Claim::Raw(Value::String(NUMBER.to_string())),
            Claim::Raw(Value::I64(NUMBER as i64)),
        ],
    }
}

/// The same spellings, naming mallory.
fn mallory_claims(column: Column) -> Vec<Claim> {
    let uuid = uuid::Uuid::from_bytes([0xbb; 16]);
    match column {
        Column::RowId | Column::Uuid | Column::Text => vec![
            Claim::Json(json!(uuid.to_string())),
            Claim::Raw(Value::String(uuid.to_string())),
            Claim::Raw(Value::Uuid(uuid)),
            Claim::Json(json!("not a uuid")),
        ],
        Column::Int => vec![
            Claim::Json(json!(NUMBER + 1)),
            Claim::Json(json!((NUMBER + 1).to_string())),
            Claim::Json(json!(format!("{NUMBER}.0x"))),
            Claim::Raw(Value::I64(NUMBER as i64 + 1)),
        ],
    }
}

fn session(db: &Db, n: u8, claim: &Claim) -> AuthorSubject {
    session_with(db, n, "who", claim)
}

/// A session whose claim `name` is `claim`.
fn session_with(db: &Db, n: u8, name: &str, claim: &Claim) -> AuthorSubject {
    let mut bytes = [0x60; 16];
    bytes[15] = n;
    let author = AuthorSubject::for_test_bytes(bytes);
    let claims = match claim {
        Claim::Json(value) => {
            jazz::binding_args::policy_claims(&author, json!({ name: value })).unwrap()
        }
        Claim::Raw(value) => {
            BTreeMap::from([(jazz::query::provider_claim_key(name), value.clone())])
        }
    };
    db.set_identity_claims(author, claims);
    author
}

fn reads(db: &Db, form: Form, column: Column, who: AuthorSubject) -> usize {
    let prepared = db.prepare_query(&Query::from(table(form, column))).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), who))
        .unwrap()
        .len()
}

fn writes(db: &Db, form: Form, column: Column, who: AuthorSubject) -> bool {
    match block_on(db.insert(
        &table(form, column),
        owner_cells(alice_uuid()),
        InsertOptions {
            identity: WriteIdentity::Session(who),
            ..Default::default()
        },
    )) {
        Ok(_) => true,
        Err(error) if error.is_authorization_denied() => false,
        Err(error) => panic!("{form:?} {column:?}: {}", error.message),
    }
}

/// Every claim spelling that names alice reads her document and passes the
/// insert check, in every policy form.
#[test]
fn a_claim_naming_the_member_grants_in_every_policy_form() {
    let db = open();
    let mut failures = Vec::new();
    let mut n = 0;
    for (form, column, _) in cases() {
        for claim in alice_claims(column) {
            n += 1;
            let alice = session(&db, n, &claim);
            let read = reads(&db, form, column, alice);
            let wrote = writes(&db, form, column, alice);
            if read == 0 || !wrote {
                failures.push(format!(
                    "{form:?} / {column:?} / {claim:?}: read {read}, insert allowed {wrote}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "denied:\n{}", failures.join("\n"));
}

/// A claim naming someone else (or nothing the column could hold) grants
/// nothing, in every policy form: coercion never widens access.
#[test]
fn a_claim_naming_someone_else_grants_nothing_in_every_policy_form() {
    let db = open();
    let mut failures = Vec::new();
    let mut n = 0;
    for (form, column, _) in cases() {
        for claim in mallory_claims(column) {
            n += 1;
            let mallory = session(&db, n, &claim);
            let read = reads(&db, form, column, mallory);
            let wrote = writes(&db, form, column, mallory);
            if read != 0 || wrote {
                failures.push(format!(
                    "{form:?} / {column:?} / {claim:?}: read {read}, insert allowed {wrote}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "granted:\n{}", failures.join("\n"));
}

fn added(stream: &mut jazz::db::SubscriptionStream) -> usize {
    let mut added = 0;
    while let Some(event) = stream.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta { added: rows, .. } = event {
            added += rows.len();
        }
    }
    added
}

/// Live results follow the converted claim too: fan-out pins a policy's
/// rows by the value the policy compares, so a write of the row a text
/// claim names wakes the session's results in every form.
///
/// ```
/// carol (claims.who = "<carol's uuid>") ──subscribe──► docs_<form>_uuid
/// system ──insert members(carol) / docs(owner carol)──► carol's result grows
/// ```
#[test]
fn a_live_result_wakes_when_the_row_a_text_claim_names_arrives() {
    let db = open();
    let carol_id = uuid::Uuid::from_bytes([0xc0; 16]);
    let mut streams = Vec::new();
    for (n, form) in FORMS.into_iter().enumerate() {
        let carol = session(
            &db,
            200 + n as u8,
            &Claim::Json(json!(carol_id.to_string())),
        );
        let prepared = db
            .prepare_query(&Query::from(table(form, Column::Uuid)))
            .unwrap();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), carol)).unwrap();
        block_on(db.tick()).unwrap();
        assert_eq!(added(&mut stream), 0, "{form:?}: carol sees nothing yet");
        streams.push((form, stream));
    }
    block_on(db.insert(
        "members",
        BTreeMap::from([
            ("user_uuid".to_owned(), Value::Uuid(carol_id)),
            ("user_text".to_owned(), Value::String(carol_id.to_string())),
            ("user_int".to_owned(), Value::I32(7)),
        ]),
        Default::default(),
    ))
    .unwrap();
    block_on(db.insert(
        &table(Form::Direct, Column::Uuid),
        owner_cells(carol_id),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    // Direct: her own document shows; EXISTS and relation: she is a
    // member now, so alice's document shows.
    for (form, stream) in &mut streams {
        assert_eq!(added(stream), 1, "{form:?}: carol's result wakes");
    }
}

/// A query predicate on a claim (`where({ col: session.claims.who })`)
/// converts the claim as a policy does.
#[test]
fn a_query_filter_on_a_claim_converts_it_as_a_policy_does() {
    use jazz::query::{claim, col, eq};
    let db = open();
    let key = jazz::query::provider_claim_key("who");
    for column in COLUMNS {
        for spelling in alice_claims(column) {
            let alice = session(&db, 250, &spelling);
            let query = Query::from("members").filter(eq(col(column.member_column()), claim(&key)));
            let prepared = db.prepare_query(&query).unwrap();
            let rows =
                block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();
            assert_eq!(rows.len(), 1, "{column:?} / {spelling:?}");
        }
    }
}

/// A table readable as `policy` says, with one row.
fn open_with(policy: PolicyExpr) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(TablePolicies::new().with_select(policy)),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(db.insert(
        "notes",
        BTreeMap::from([("body".to_owned(), Value::String("hi".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    db
}

fn visible(db: &Db, who: AuthorSubject) -> usize {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), who))
        .unwrap()
        .len()
}

fn claims_path(name: &str) -> Vec<String> {
    vec!["claims".to_owned(), name.to_owned()]
}

/// A deny list in a claim (`not(session.claims.blocked contains <uuid>)`)
/// denies whatever way the issuer spells the uuid, and the positive form
/// grants alike: an array claim's elements compare as the literal's type.
///
/// ```
/// mallory (claims.blocked = ["<HER UUID>"]) ──read──► notes ✗
/// ```
#[test]
fn a_uuid_in_an_array_claim_matches_whatever_its_spelling() {
    use jazz::tools::{ObjectId, Value as PublicValue};
    let mallory_id = uuid::Uuid::from_bytes([0xbb; 16]);
    let blocked = || PolicyExpr::SessionContains {
        path: claims_path("blocked"),
        value: PublicValue::Uuid(ObjectId::from_uuid(mallory_id)),
    };
    let deny = open_with(PolicyExpr::not(blocked()));
    let allow = open_with(blocked());
    let spellings = [
        mallory_id.to_string(),
        mallory_id.to_string().to_uppercase(),
        mallory_id.simple().to_string(),
    ];
    for (n, spelling) in spellings.iter().enumerate() {
        let claim = Claim::Json(json!([spelling]));
        let on_deny = session_with(&deny, n as u8, "blocked", &claim);
        assert_eq!(visible(&deny, on_deny), 0, "{spelling}: still blocked");
        let on_allow = session_with(&allow, n as u8, "blocked", &claim);
        assert_eq!(visible(&allow, on_allow), 1, "{spelling}: listed");
    }
    let someone_else = Claim::Json(json!([uuid::Uuid::from_bytes([0xcc; 16]).to_string()]));
    let other = session_with(&deny, 9, "blocked", &someone_else);
    assert_eq!(
        visible(&deny, other),
        1,
        "a list without her lets others read"
    );
}

/// A claim in a mixed-type literal list (`role in [1, "admin"]`) compares
/// with each literal as that literal's type, so negating the list denies
/// exactly the claims the list grants.
#[test]
fn a_claim_in_a_mixed_literal_list_compares_with_each_literal_as_its_type() {
    use jazz::tools::Value as PublicValue;
    let listed = || PolicyExpr::SessionInList {
        path: claims_path("role"),
        values: vec![
            PublicValue::Integer(1),
            PublicValue::Text("admin".to_owned()),
        ],
    };
    let allow = open_with(listed());
    let deny = open_with(PolicyExpr::not(listed()));
    let cases = [
        (json!("admin"), true),
        (json!(1), true),
        (json!("1"), true),
        (json!("01"), false),
        (json!("editor"), false),
    ];
    for (n, (claim, listed)) in cases.into_iter().enumerate() {
        let claim = Claim::Json(claim);
        let on_allow = session_with(&allow, n as u8, "role", &claim);
        let on_deny = session_with(&deny, n as u8, "role", &claim);
        assert_eq!(
            visible(&allow, on_allow),
            listed as usize,
            "{claim:?} in the list"
        );
        assert_eq!(
            visible(&deny, on_deny),
            !listed as usize,
            "{claim:?} not in it"
        );
    }
}

/// A timestamp never reads a string claim as its milliseconds: only
/// Integer and BigInt columns take decimal strings.
#[test]
fn a_timestamp_compares_only_with_a_number_claim() {
    use jazz::tools::{CmpOp, Value as PublicValue};
    let db = open_with(PolicyExpr::SessionCmp {
        path: claims_path("since"),
        op: CmpOp::Eq,
        value: PublicValue::Timestamp(1_700_000_000_000),
    });
    let number = session_with(&db, 1, "since", &Claim::Json(json!(1_700_000_000_000_u64)));
    assert_eq!(visible(&db, number), 1);
    let text = session_with(&db, 2, "since", &Claim::Json(json!("1700000000000")));
    assert_eq!(visible(&db, text), 0);
}
