//! Ordinary reads execute under the handle's current authenticated governance.
use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn schema(read: bool) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(read_and_allow_all_writes(if read {
                    PolicyExpr::True
                } else {
                    PolicyExpr::False
                })),
        )
        .build()
}
fn open(node: u8, author: AuthorSubject, read: bool, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema(read)),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    } else {
        db.become_replica();
    }
    db
}
fn seed(server: &Db) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([4; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        )
        .unwrap();
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

/// Alice's initially public root becomes dependent on another root row.
/// Deleting that support must re-evaluate every dependent root, not only its ID.
#[test]
fn owner_policy_activation_rebuilds_self_table_recheck_state() {
    use crate::common::{exists, read_and_allow_all_writes, session_eq};
    use jazz::db::ReadOpts;
    use jazz::ids::RowUuid;
    use jazz::query::{col, eq, lit};
    use jazz::records::Value;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use std::collections::BTreeMap;
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([5; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("support".into()))]),
        )
        .unwrap();
    server.set_identity_claims(
        alice(),
        BTreeMap::from([("\0claims:support".into(), Value::String("support".into()))]),
    );
    let query = server
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), lit(Value::String("secret".into())))),
        )
        .unwrap();
    let mut stream =
        block_on(server.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
    assert_eq!(observed_rows(&mut stream, 0), 1);
    let changed = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "notes",
                        vec![session_eq("body", &["claims", "support"])],
                    ))),
            )
            .build(),
    );
    block_on(server.activate_schema(changed)).unwrap();
    assert_eq!(observed_rows(&mut stream, 1), 1);
    block_on(server.delete("notes", RowUuid::from_bytes([5; 16]), Default::default())).unwrap();
    assert_eq!(
        block_on(server.all_for_identity(&query, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        observed_rows(&mut stream, 1),
        0,
        "self-table support revocation must re-evaluate roots"
    );
}

mod optional_include_nested {

    use jazz::db::{
        CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream,
        block_on,
    };
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::{ArraySubquery, Query, col, eq, lit};
    use jazz::records::Value;
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
    use std::collections::BTreeMap;

    fn children(row: &CurrentRow) -> Value {
        row.cell("children").unwrap()
    }
}
