//! A team's first edit of a release plan under a many-branch sign-off policy:
//! the authority compiles the update authorization-support view and hydrates
//! every one of its subscriptions.
//!
//! Moved from `crates/jazz/benches/authorization_support_branches.rs`
//! (`update_support_branches`), reframed in BigLabel terms: the base table is
//! a release plan, each policy factor is a sign-off desk, and each desk grants
//! the edit to either its leads or its deputies. The update policy is an AND of
//! `desks` ORs, each OR choosing between two correlated `exists` grants, so it
//! normalizes to `2^desks` branches of `desks` joins each. Schema, policy,
//! timed work and node identities are unchanged; the grant tables stay empty,
//! as before, so the measurement is compilation plus empty hydration.
//!
//! Each sample opens a fresh node, so no compiled program or Groove graph is
//! reused between samples. Compare the 16- and 64-branch rungs: linear scaling
//! is a 4x (plus the per-branch join count) step, not 16x.

use std::collections::BTreeMap;

use jazz::db::block_on;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::NodeState;
use jazz::peer::PeerState;
use jazz::protocol::{PermissionAdviceAction, SubscriptionKey};
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

    /// Untimed per-sample input: a fresh node with nothing compiled yet.
    pub fn open_node(&self) -> NodeState {
        let families = self.schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        block_on(NodeState::new_with_shared_test_catalogue(
            NodeUuid::from_bytes([0xd1; 16]),
            self.schema.clone(),
            MemoryStorage::new(&refs).expect("valid memory storage families"),
        ))
        .expect("open sign-off policy node")
    }
}

/// The timed operation: compile the release plan's update support scope and
/// hydrate each of its subscriptions, as an authority does for a session's
/// first update of the table. Returns the number of hydrated subscriptions.
pub fn open_update_support(node: &mut NodeState) -> usize {
    let editor = editor();
    let scope = node
        .authorization_support_scope(
            editor,
            &PermissionAdviceAction::Update {
                table: PLANS.to_owned(),
                row: RowUuid::from_bytes([1; 16]),
                patch: BTreeMap::new(),
            },
        )
        .expect("release plan update support scope compiles");
    let mut peer = PeerState::client_link(editor);
    for (shape, binding) in &scope.subscriptions {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: scope.options.read_view_key(),
        };
        block_on(peer.rehydrate_authorization_support_query_for_identity(
            node,
            editor,
            BTreeMap::new(),
            subscription,
            shape,
            binding,
            scope.options.clone(),
        ))
        .expect("release plan update support hydrates");
    }
    scope.subscriptions.len()
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
