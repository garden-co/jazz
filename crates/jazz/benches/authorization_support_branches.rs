//! Compile and hydrate one update authorization-support view as the number
//! of policy branches grows.
//!
//! The update policy is an AND of `factors` ORs, each OR choosing between two
//! correlated `exists` grants, so it normalizes to `2^factors` branches of
//! `factors` joins each. Each iteration opens a fresh node, so no compiled
//! program or Groove graph is reused between samples. Compare the 16- and
//! 64-branch rungs: linear scaling is a 4x (plus the per-branch join count)
//! step, not 16x.
//!
//! ```text
//! cargo bench -p jazz --features testing --bench authorization_support_branches
//! ```

mod schema_fixture;
mod support;

use std::collections::BTreeMap;

use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::NodeState;
use jazz::peer::PeerState;
use jazz::protocol::{PermissionAdviceAction, SubscriptionKey};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, PolicyExpr, PolicyValue};
use jazz::tools::{ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use support::BenchFutureExt as _;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    divan::main();
}

fn writer() -> AuthorSubject {
    AuthorSubject::for_test_uuid(uuid::uuid!("00000000-0000-0000-0000-0000000000d2"))
}

fn member_is_session_user() -> PolicyExpr {
    PolicyExpr::eq_session("member", vec!["claims".into(), "user_id".into()])
}

/// `exists grant where grant.target = outer.id and grant.member = session`.
fn grant_exists(table: &str) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "target".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "id".into()]),
            },
            member_is_session_user(),
        ])),
    }
}

fn branching_schema(factors: usize) -> JazzSchema {
    let open = || schema_fixture::all_operations(PolicyExpr::True);
    let mut builder = SchemaBuilder::new();
    let mut conjuncts = Vec::new();
    for factor in 0..factors {
        let sides = ["a", "b"].map(|side| format!("grant_{factor}_{side}"));
        for table in &sides {
            builder = builder.table(
                TableSchemaBuilder::new(table)
                    .fk_column("target", "base")
                    .column("member", ColumnType::Text)
                    .policies(open()),
            );
        }
        conjuncts.push(PolicyExpr::or(
            sides.iter().map(|table| grant_exists(table)).collect(),
        ));
    }
    let update = PolicyExpr::and(conjuncts);
    builder = builder.table(
        TableSchemaBuilder::new("base")
            .column("value", ColumnType::Text)
            .policies(
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(PolicyExpr::True)
                    .with_update(Some(update.clone()), update)
                    .with_delete(PolicyExpr::True),
            ),
    );
    schema_fixture::compile(builder)
}

fn open_node(schema: &JazzSchema) -> NodeState {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    NodeState::new_with_shared_test_catalogue(
        NodeUuid::from_bytes([0xd1; 16]),
        schema.clone(),
        MemoryStorage::new(&refs).expect("memory storage families"),
    )
    .expect("open node")
}

/// Compile the update support scope and hydrate each of its subscriptions,
/// as an authority does for a session's first update of the table.
fn open_update_support(node: &mut NodeState) -> usize {
    let writer = writer();
    let scope = node
        .authorization_support_scope(
            writer,
            &PermissionAdviceAction::Update {
                table: "base".to_owned(),
                row: RowUuid::from_bytes([1; 16]),
                patch: BTreeMap::new(),
            },
        )
        .expect("update support scope compiles");
    let mut peer = PeerState::client_link(writer);
    for (shape, binding) in &scope.subscriptions {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: scope.options.read_view_key(),
        };
        peer.rehydrate_authorization_support_query_for_identity(
            node,
            writer,
            BTreeMap::new(),
            subscription,
            shape,
            binding,
            scope.options.clone(),
        )
        .expect("update support hydrates");
    }
    scope.subscriptions.len()
}

#[divan::bench(args = [4, 6], sample_count = 5)]
fn update_support_branches(bencher: divan::Bencher<'_, '_>, factors: usize) {
    let schema = branching_schema(factors);
    bencher
        .with_inputs(|| open_node(&schema))
        .bench_local_values(|mut node| divan::black_box(open_update_support(&mut node)));
}
