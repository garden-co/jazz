//! A team's first edit of a release plan under a many-branch sign-off policy:
//! the authority compiles the update policy and evaluates it for the edit.
//!
//! In BigLabel terms: the base table is a release plan, each policy factor is a sign-off desk, and each desk grants
//! the edit to either its leads or its deputies. The update policy is an AND of
//! `desks` ORs, each OR choosing between two correlated `exists` grants, so it
//! normalizes to `2^desks` branches of `desks` joins each. The grant tables
//! stay empty, so the measurement is policy compilation plus an evaluation over empty grants,
//! which denies the edit. The policy runs as one statement of correlated
//! `EXISTS` branches.
//!
//! Each sample opens a fresh database, so no compiled statement is reused
//! between samples. Compare the 16- and 64-branch rungs: linear scaling
//! is a 4x (plus the per-branch join count) step, not 16x.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, UpdateOptions, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, PolicyExpr, PolicyValue};
use jazz::tools::{ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder};

const PLANS: &str = "release_plans";
const GRANT_SIDES: [&str; 2] = ["leads", "deputies"];

/// The compiled sign-off schema. Built once per benchmark, outside timing.
pub struct PolicyGraphFixture {
    schema: JazzSchema,
}

impl PolicyGraphFixture {
    pub fn new(desks: usize) -> Self {
        assert!(desks > 0, "the sign-off policy needs at least one desk");
        Self {
            schema: schema(desks),
        }
    }

    /// Untimed per-sample input: a fresh authority holding one release plan,
    /// with nothing compiled for the editor yet.
    pub fn open_node(&self) -> Db {
        let db = block_on(Db::open(DbConfig::new(
            self.schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xd1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open sign-off policy database");
        block_on(db.insert(
            PLANS,
            BTreeMap::from([("title".to_owned(), Value::String("Spring release".into()))]),
            InsertOptions {
                row_id: Some(plan()),
                ..Default::default()
            },
        ))
        .expect("seed release plan");
        db
    }
}

/// The timed operation: the editor's first update of the release plan, which
/// the authority checks against the `2^desks`-branch update policy. Returns
/// whether the edit was denied (no desk has granted anything).
pub fn open_update_support(db: &mut Db) -> bool {
    block_on(db.update(
        PLANS,
        plan(),
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("Spring release (v2)".into()),
        )]),
        UpdateOptions {
            identity: WriteIdentity::Session(editor()),
            ..Default::default()
        },
    ))
    .is_err()
}

fn plan() -> RowUuid {
    RowUuid::from_bytes([1; 16])
}

fn editor() -> AuthorSubject {
    let mut bytes = [0_u8; 16];
    bytes[15] = 0xd2;
    AuthorSubject::for_test_bytes(bytes)
}

fn member_is_session_user() -> PolicyExpr {
    PolicyExpr::eq_session("member", vec!["claims".into(), "user_id".into()])
}

/// `exists grant where grant.plan = outer.id and grant.member = session`.
fn grant_exists(table: &str) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "plan".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "id".into()]),
            },
            member_is_session_user(),
        ])),
    }
}

fn open_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn schema(desks: usize) -> JazzSchema {
    let mut builder = SchemaBuilder::new();
    let mut sign_offs = Vec::new();
    for desk in 0..desks {
        let grants = GRANT_SIDES.map(|side| format!("desk_{desk}_{side}"));
        for table in &grants {
            builder = builder.table(
                TableSchemaBuilder::new(table)
                    .fk_column("plan", PLANS)
                    .column("member", ColumnType::Text)
                    .policies(open_policies()),
            );
        }
        sign_offs.push(PolicyExpr::or(
            grants.iter().map(|table| grant_exists(table)).collect(),
        ));
    }
    let update = PolicyExpr::and(sign_offs);
    builder = builder.table(
        TableSchemaBuilder::new(PLANS)
            .column("title", ColumnType::Text)
            .policies(
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(PolicyExpr::True)
                    .with_update(Some(update.clone()), update)
                    .with_delete(PolicyExpr::True),
            ),
    );
    JazzSchema::new(&builder.build()).expect("BigLabel sign-off policy schema compiles")
}
