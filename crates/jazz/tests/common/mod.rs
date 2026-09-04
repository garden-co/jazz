#![allow(dead_code)]

use jazz::schema::JazzSchema;
use jazz::tools::{PolicyExpr, Schema, TablePolicies};

pub fn compile_schema(source: &Schema) -> JazzSchema {
    jazz::schema::JazzSchema::new(source).expect("integration-test public schema compiles")
}

pub fn allow_all_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

pub fn allow_all_writes() -> TablePolicies {
    TablePolicies::new()
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

pub fn read_and_allow_all_writes(read: PolicyExpr) -> TablePolicies {
    allow_all_writes().with_select(read)
}

pub fn session_eq(column: &str, path: &[&str]) -> PolicyExpr {
    PolicyExpr::eq_session(
        column,
        path.iter().map(|segment| (*segment).to_owned()).collect(),
    )
}

pub fn outer_eq(column: &str, outer_column: &str) -> PolicyExpr {
    session_eq(column, &["__jazz_outer_row", outer_column])
}

pub fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::and(conditions)),
    }
}

/// Give each admitted fixture scope a distinct ordinary usage handle.
pub fn direct_subscription(
    schema: &JazzSchema,
    table: &str,
    identity: jazz::ids::AuthorSubject,
) -> jazz::protocol::SubscriptionKey {
    let shape = jazz::query::Query::from(table).validate(schema).unwrap();
    jazz::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        // Fixture handles are distinct for the upstream SYSTEM subscription
        // and the downstream user's subscription, even with identical queries.
        // These fixtures have one immutable claims snapshot per identity.
        binding_id: jazz::query::BindingId(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            identity.canonical().as_bytes(),
        )),
        read_view: Default::default(),
    }
}

/// Register the receiving half of an existing direct-node whole-table fixture.
/// These crash/FIFO fixtures deliberately exercise NodeState below JazzServer:
/// they need to interrupt persistence or schedule individual protocol frames.
/// Registration remains explicit and precedes delivery, just as on a real link.
/// The delegated snapshot models the fixture's already-admitted scope, not a
/// capability that an ordinary network client may assert for itself.
pub async fn register_direct_receiver(
    node: &mut jazz::node::NodeState<jazz_storage_rocksdb::RocksDbStorage>,
    schema: &JazzSchema,
    table: &str,
    scope: jazz::protocol::DelegatedSessionBinding,
) {
    use jazz::protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SyncMessage};
    use jazz::query::Query;

    let shape = Query::from(table).validate(schema).unwrap();
    let opts = RegisterShapeOptions::default();
    let subscription = direct_subscription(schema, table, scope.identity);
    for message in [
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        },
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: Some(scope),
        }),
    ] {
        let outcome = node.apply_sync_message(message).await.unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap();
    }
}

pub async fn direct_query_update(
    node: &mut jazz::node::NodeState<jazz_storage_rocksdb::RocksDbStorage>,
    peer: &mut jazz::peer::PeerState,
    schema: &JazzSchema,
    table: &str,
) -> jazz::protocol::SyncMessage {
    let shape = jazz::query::Query::from(table).validate(schema).unwrap();
    let binding = shape.bind(std::collections::BTreeMap::new()).unwrap();
    let subscription = direct_subscription(schema, table, peer.identity());
    let initial = peer.subscription_result_sets(subscription).is_none();
    peer.set_subscription_policy_binding(subscription, (peer.identity(), Default::default()));
    if initial
        && let Some(update) = peer
            .rehydrate_query_for_subscription_with_opts(
                node,
                subscription,
                &shape,
                &binding,
                Default::default(),
            )
            .await
            .unwrap()
    {
        return update;
    }
    peer.query_update_for_subscription(node, subscription, &shape, &binding)
        .await
        .unwrap()
}
