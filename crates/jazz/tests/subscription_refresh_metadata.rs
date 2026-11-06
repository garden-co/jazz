//! Recompilation refreshes cached metadata before later subscription changes.
use crate::common::compile_schema;
use jazz::db::block_on;
use jazz::ids::AuthorSubject;
use jazz::query::Query;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}

fn observed_rows(stream: &mut jazz::db::SubscriptionStream, previous: usize) -> usize {
    let mut count = previous;
    while let Some(event) = stream.try_next_event() {
        match event {
            jazz::db::SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    count = 0;
                }
                count = count
                    .checked_sub(removed.len())
                    .expect("removed visible rows")
                    + added.len();
            }
            jazz::db::SubscriptionEvent::Rejected { reason } => {
                panic!("subscription rejected: {reason}")
            }
            _ => {}
        }
    }
    count
}

fn support_schema(policy: jazz::tools::PolicyExpr) -> jazz::tools::public_schema::Schema {
    use crate::common::{allow_all_policies, read_and_allow_all_writes};
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("support")
                .column("principal", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .fk_column("support", "support")
                .policies(read_and_allow_all_writes(policy)),
        )
        .build()
}

fn support_server(policy: jazz::tools::PolicyExpr) -> jazz::db::Db {
    use jazz::db::{Db, DbConfig, DbIdentity};
    use jazz::ids::{NodeUuid, RowUuid};
    use jazz::records::Value;
    use std::collections::BTreeMap;
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&support_schema(policy)),
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    for (id, name) in [(5, "first"), (6, "second")] {
        db.seed_settled_mergeable_for_bootstrap(
            "support",
            RowUuid::from_bytes([id; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("principal".into(), Value::String(name.into()))]),
        )
        .unwrap();
    }
    db.seed_settled_mergeable_for_bootstrap(
        "notes",
        RowUuid::from_bytes([4; 16]),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("body".into(), Value::String("secret".into())),
            (
                "support".into(),
                Value::Uuid(uuid::Uuid::from_bytes([6; 16])),
            ),
        ]),
    )
    .unwrap();
    db.set_identity_claims(
        alice(),
        BTreeMap::from([
            ("\0claims:first".into(), Value::String("first".into())),
            ("\0claims:second".into(), Value::String("second".into())),
        ]),
    );
    db
}

/// Alice's optional include becomes an authorization dependency after owner
/// activation. Revoking it must remove the root, not only refresh the include.
#[test]
fn owner_policy_activation_rebuilds_output_only_include_state() {
    use crate::common::{exists, session_eq};
    use jazz::db::ReadOpts;
    use jazz::ids::RowUuid;
    use jazz::tools::PolicyExpr;
    let server = support_server(PolicyExpr::True);
    let query = server
        .prepare_query(&Query::from("notes").include("support"))
        .unwrap();
    let mut stream =
        block_on(server.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
    assert_eq!(observed_rows(&mut stream, 0), 1);
    let changed = support_schema(exists(
        "support",
        vec![session_eq("principal", &["claims", "second"])],
    ));
    block_on(server.activate_schema(compile_schema(&changed))).unwrap();
    assert_eq!(observed_rows(&mut stream, 1), 1);
    block_on(server.delete("support", RowUuid::from_bytes([6; 16]), Default::default())).unwrap();
    assert_eq!(
        block_on(server.all_for_identity(&query, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        observed_rows(&mut stream, 1),
        0,
        "policy support is no longer output-only"
    );
}
