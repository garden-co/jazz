//! Live groups preserve each author and claim source through refresh and cancellation.
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

/// Alice's registry-following, empty and fixed claims are distinct even when
/// today's policy is public. Bob's author must also survive that shared SQL.
#[test]
fn subscription_groups_keep_author_and_claims_provenance_after_policy_revision() {
    use crate::common::{read_and_allow_all_writes, session_eq};
    use jazz::db::ReadOpts;
    use jazz::records::Value;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use std::collections::BTreeMap;
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    let bob = AuthorSubject::for_test_bytes([0x91; 16]);
    let claims = |value: &str| {
        BTreeMap::from([("\0claims:body".to_owned(), Value::String(value.to_owned()))])
    };
    server.set_identity_claims(alice(), claims("secret"));
    server.set_identity_claims(bob, claims("other"));
    let prepared = server.prepare_query(&Query::from("notes")).unwrap();
    let variants = [
        (alice(), prepared.clone()),
        (
            alice(),
            prepared
                .clone()
                .with_identity_claims(alice(), BTreeMap::new()),
        ),
        (
            alice(),
            prepared
                .clone()
                .with_identity_claims(alice(), claims("secret")),
        ),
        (bob, prepared),
    ];
    let mut streams = variants
        .iter()
        .map(|(author, query)| {
            block_on(server.subscribe_for_identity(query, ReadOpts::default(), *author)).unwrap()
        })
        .collect::<Vec<_>>();
    let mut counts = [0; 4];
    for (i, stream) in streams.iter_mut().enumerate() {
        counts[i] = observed_rows(stream, 0);
        assert_eq!(counts[i], 1);
    }
    let restricted = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "body",
                        &["claims", "body"],
                    ))),
            )
            .build(),
    );
    block_on(server.activate_schema(restricted)).unwrap();
    for (i, expected) in [1, 0, 1, 0].into_iter().enumerate() {
        counts[i] = observed_rows(&mut streams[i], counts[i]);
        assert_eq!(counts[i], expected, "after revision, scope {i}");
    }
    server.set_identity_claims(alice(), claims("other"));
    server.set_identity_claims(bob, claims("secret"));
    for (i, expected) in [0, 0, 1, 1].into_iter().enumerate() {
        counts[i] = observed_rows(&mut streams[i], counts[i]);
        assert_eq!(counts[i], expected, "after drift, scope {i}");
        let (author, query) = &variants[i];
        let mut fresh =
            block_on(server.subscribe_for_identity(query, ReadOpts::default(), *author)).unwrap();
        assert_eq!(
            observed_rows(&mut fresh, 0),
            expected,
            "new member, scope {i}"
        );
    }
    drop(streams);
    block_on(server.tick()).unwrap();
    for (author, query) in variants {
        let mut fresh =
            block_on(server.subscribe_for_identity(&query, ReadOpts::default(), author)).unwrap();
        let expected = block_on(server.all_for_identity(&query, ReadOpts::default(), author))
            .unwrap()
            .len();
        assert_eq!(observed_rows(&mut fresh, 0), expected);
    }
}

/// Alice's registry changes must rekey her existing group before a new member
/// joins; equal current scope and SQL must share the refreshed result.
#[test]
fn registry_recompilation_rekeys_same_scope_sharing() {
    use jazz::db::ReadOpts;
    use jazz::query::{claim, col, eq, provider_claim_key};
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    let claims =
        |body: &str| BTreeMap::from([(provider_claim_key("body"), Value::String(body.to_owned()))]);
    server.set_identity_claims(alice(), claims("secret"));
    let query = server
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), claim(provider_claim_key("body")))),
        )
        .unwrap();
    let mut stream =
        block_on(server.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
    let mut previous = observed_rows(&mut stream, 0);
    assert_eq!(previous, 1);
    for (body, expected) in [("other", 0), ("secret", 1)] {
        server.set_identity_claims(alice(), claims(body));
        previous = observed_rows(&mut stream, previous);
        assert_eq!(
            previous, expected,
            "Alice's maintained result follows her registry"
        );
        let before = server.fanout_stats();
        assert_eq!(before.groups, 1);
        let mut fresh =
            block_on(server.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
        let after = server.fanout_stats();
        assert_eq!(
            after.groups, before.groups,
            "current scope reuses the existing group"
        );
        assert_eq!(after.shared_joins, before.shared_joins + 1);
        let oracle =
            block_on(server.all_for_identity(&query, ReadOpts::default(), alice())).unwrap();
        assert_eq!(oracle.len(), expected);
        assert_eq!(observed_rows(&mut fresh, 0), oracle.len());
        drop(fresh);
        block_on(server.tick()).unwrap();
        assert_eq!(server.fanout_stats().groups, 1);
    }
}

fn public_extended_schema() -> jazz::schema::JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .nullable_column("extra", ColumnType::Text)
                    .policies(read_and_allow_all_writes(PolicyExpr::True)),
            )
            .build(),
    )
}

fn public_row(
    stream: &mut jazz::db::SubscriptionStream,
    mut previous: Option<jazz::db::CurrentRow>,
) -> jazz::db::CurrentRow {
    while let Some(event) = stream.try_next_event() {
        match event {
            jazz::db::SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } => {
                assert!(removed.is_empty(), "public policy retains Alice's row");
                if reset {
                    previous = None;
                }
                for output in added.into_iter().chain(updated) {
                    assert_eq!(output.row.row_uuid(), RowUuid::from_bytes([4; 16]));
                    previous = Some(output.row);
                }
            }
            jazz::db::SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
            _ => {}
        }
    }
    previous.expect("Alice's public row remains present")
}

/// Alice's two groups converge after Bob extends a public schema. Cancelling
/// either original subscription must preserve sharing and updates for survivors.
/// The retained-policy denial case remains separately unchanged in B+F coverage.
#[test]
fn public_converged_groups_keep_survivor_index_after_either_cancellation() {
    use jazz::db::ReadOpts;
    for drop_root in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, true);
        seed(&server);
        let view = block_on(server.register_schema_view(public_extended_schema())).unwrap();
        let root_query = server.prepare_query(&Query::from("notes")).unwrap();
        let view_query = view.prepare_query(&Query::from("notes")).unwrap();
        let mut root = Some(
            block_on(server.subscribe_for_identity(&root_query, ReadOpts::default(), alice()))
                .unwrap(),
        );
        let mut retained = Some(
            block_on(view.subscribe_for_identity(&view_query, ReadOpts::default(), alice()))
                .unwrap(),
        );
        assert_eq!(server.fanout_stats().groups, 2);
        let root_before = public_row(root.as_mut().unwrap(), None);
        let retained_before = public_row(retained.as_mut().unwrap(), None);
        block_on(server.activate_schema(public_extended_schema())).unwrap();
        public_row(root.as_mut().unwrap(), Some(root_before));
        public_row(retained.as_mut().unwrap(), Some(retained_before));
        let before = server.fanout_stats();
        let mut survivor =
            block_on(server.subscribe_for_identity(&root_query, ReadOpts::default(), alice()))
                .unwrap();
        assert_eq!(server.fanout_stats().shared_joins, before.shared_joins + 1);
        assert_eq!(server.fanout_stats().groups, 2);
        let survivor_before = public_row(&mut survivor, None);
        if drop_root {
            drop(root.take());
        } else {
            drop(retained.take());
        }
        block_on(server.tick()).unwrap();
        let before = server.fanout_stats();
        // The fresh member keeps the indexed group alive. The other original
        // group is removed only in the cancellation order that drops its owner.
        assert!((1..=2).contains(&before.groups));
        let mut newest =
            block_on(view.subscribe_for_identity(&view_query, ReadOpts::default(), alice()))
                .unwrap();
        let after = server.fanout_stats();
        assert_eq!(
            after.groups, before.groups,
            "drop_root={drop_root}: no duplicate group"
        );
        assert_eq!(
            after.shared_joins,
            before.shared_joins + 1,
            "drop_root={drop_root}: the surviving group's index is intact"
        );
        let newest_before = public_row(&mut newest, None);
        let oracle =
            block_on(view.all_for_identity(&view_query, ReadOpts::default(), alice())).unwrap();
        assert_eq!(oracle.len(), 1);
        assert_eq!(newest_before.cell("body"), oracle[0].cell("body"));
        // HashMap traversal chooses the indexed representative. Keeping the
        // fresh members alive and dropping BOTH originals guarantees that the
        // unindexed group is collected, whichever original was dropped first.
        drop(root.take());
        drop(retained.take());
        block_on(server.tick()).unwrap();
        let before_both = server.fanout_stats();
        assert_eq!(before_both.groups, 1, "only the indexed survivor remains");
        let mut after_both =
            block_on(view.subscribe_for_identity(&view_query, ReadOpts::default(), alice()))
                .unwrap();
        let after_both_stats = server.fanout_stats();
        assert_eq!(
            after_both_stats.groups, 1,
            "collecting the unindexed group preserves sharing"
        );
        assert_eq!(after_both_stats.shared_joins, before_both.shared_joins + 1);
        let after_both_before = public_row(&mut after_both, None);
        assert_eq!(after_both_before.cell("body"), oracle[0].cell("body"));
        block_on(server.update(
            "notes",
            RowUuid::from_bytes([4; 16]),
            BTreeMap::from([("body".into(), Value::String("changed".into()))]),
            Default::default(),
        ))
        .unwrap();
        let survivor_after = public_row(&mut survivor, Some(survivor_before));
        let newest_after = public_row(&mut newest, Some(newest_before));
        let oracle =
            block_on(view.all_for_identity(&view_query, ReadOpts::default(), alice())).unwrap();
        assert_eq!(oracle.len(), 1);
        assert_eq!(
            oracle[0].cell("body"),
            Some(Value::String("changed".into()))
        );
        assert_eq!(survivor_after.cell("body"), oracle[0].cell("body"));
        assert_eq!(newest_after.cell("body"), oracle[0].cell("body"));
        let after_both_row = public_row(&mut after_both, Some(after_both_before));
        assert_eq!(after_both_row.cell("body"), oracle[0].cell("body"));
    }
}
