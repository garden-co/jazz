//! E17 governance-only regressions, retaining the qualified B+F joint.
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([18; 16])
}
fn policies(p: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(p)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}
fn schema(p: PolicyExpr, extra: bool) -> JazzSchema {
    let mut t = TableSchemaBuilder::new("notes")
        .column("body", ColumnType::Text)
        .policies(policies(p));
    if extra {
        t = t.nullable_column("extra", ColumnType::Text);
    }
    JazzSchema::new(&SchemaBuilder::new().table(t).build()).unwrap()
}
fn open(s: JazzSchema) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        s,
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db
}
fn seed(db: &Db, t: &str, n: u8, cells: BTreeMap<String, Value>) {
    db.seed_settled_mergeable_for_bootstrap(
        t,
        RowUuid::from_bytes([n; 16]),
        AuthorSubject::SYSTEM,
        cells,
    )
    .unwrap();
}
fn claims(v: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("\0claims:body".into(), Value::String(v.into()))])
}
fn count(s: &mut SubscriptionStream, mut n: usize) -> usize {
    while let Some(e) = s.try_next_event() {
        match e {
            SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    n = 0
                }
                n = n.checked_sub(removed.len()).unwrap() + added.len();
            }
            SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
            _ => {}
        }
    }
    n
}

/// Alice's root and retained-name groups converge after Bob activates the
/// retained structure. Either group's removal must preserve sharing for survivors.
#[test]
fn review_converged_group_keys_survive_either_cancellation() {
    for drop_root in [false, true] {
        let db = open(schema(PolicyExpr::True, false));
        seed(
            &db,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        let view = block_on(db.register_schema_view(schema(PolicyExpr::True, true))).unwrap();
        let rootq = db.prepare_query(&Query::from("notes")).unwrap();
        let viewq = view.prepare_query(&Query::from("notes")).unwrap();
        let mut a = Some(
            block_on(db.subscribe_for_identity(&rootq, ReadOpts::default(), alice())).unwrap(),
        );
        let mut b = Some(
            block_on(view.subscribe_for_identity(&viewq, ReadOpts::default(), alice())).unwrap(),
        );
        assert_eq!(db.fanout_stats().groups, 2);
        assert_eq!(count(a.as_mut().unwrap(), 0), 1);
        assert_eq!(count(b.as_mut().unwrap(), 0), 1);
        block_on(db.activate_schema(schema(PolicyExpr::True, true))).unwrap();
        assert_eq!(count(a.as_mut().unwrap(), 1), 1);
        assert_eq!(count(b.as_mut().unwrap(), 1), 1);
        let mut fresh =
            block_on(db.subscribe_for_identity(&rootq, ReadOpts::default(), alice())).unwrap();
        assert_eq!(count(&mut fresh, 0), 1);
        if drop_root {
            drop(a.take());
        } else {
            drop(b.take());
        }
        block_on(db.tick()).unwrap();
        let mut newest =
            block_on(view.subscribe_for_identity(&viewq, ReadOpts::default(), alice())).unwrap();
        assert_eq!(count(&mut newest, 0), 1);
        block_on(db.activate_schema(schema(PolicyExpr::False, true))).unwrap();
        assert_eq!(count(&mut fresh, 1), 0);
        assert_eq!(count(&mut newest, 1), 0);
        if let Some(a) = a.as_mut() {
            assert_eq!(count(a, 1), 0)
        }
        if let Some(b) = b.as_mut() {
            assert_eq!(count(b, 1), 0)
        }
    }
}

fn two_reader_activation(reverse: bool) {
    let db = open(schema(PolicyExpr::True, false));
    seed(
        &db,
        "notes",
        1,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    );
    db.set_identity_claims(alice(), claims("secret"));
    db.set_identity_claims(bob(), claims("other"));
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let authors = if reverse {
        [bob(), alice()]
    } else {
        [alice(), bob()]
    };
    let mut streams = authors
        .iter()
        .map(|author| {
            block_on(db.subscribe_for_identity(&query, ReadOpts::default(), *author)).unwrap()
        })
        .collect::<Vec<_>>();
    let mut counts = [0; 2];
    for (i, stream) in streams.iter_mut().enumerate() {
        counts[i] = count(stream, 0);
        assert_eq!(counts[i], 1);
    }
    let restricted = || {
        schema(
            crate::common::session_eq("body", &["claims", "body"]),
            false,
        )
    };
    for restrict in [true, false, true] {
        block_on(db.activate_schema(if restrict {
            restricted()
        } else {
            schema(PolicyExpr::True, false)
        }))
        .unwrap();
        for (i, author) in authors.iter().enumerate() {
            let expected = usize::from(!restrict || *author == alice());
            let fresh =
                block_on(db.all_for_identity(&query, ReadOpts::default(), *author)).unwrap();
            assert_eq!(fresh.len(), expected, "fresh oracle author={author:?}");
            counts[i] = count(&mut streams[i], counts[i]);
            assert_eq!(
                counts[i], expected,
                "reader={author:?} reverse={reverse} restrictive={restrict}"
            );
        }
    }
    db.set_identity_claims(alice(), claims("other"));
    db.set_identity_claims(bob(), claims("secret"));
    for (i, author) in authors.iter().enumerate() {
        let fresh = block_on(db.all_for_identity(&query, ReadOpts::default(), *author)).unwrap();
        assert_eq!(fresh.len(), usize::from(*author == bob()));
        counts[i] = count(&mut streams[i], counts[i]);
        assert_eq!(counts[i], fresh.len(), "claim change author={author:?}");
    }
}

/// Alice opens a public query before Bob; later policy activation must not
/// publish Alice's authorised row to Bob, including after another public phase.
#[test]
fn e17_governance_two_readers_alice_opens_first() {
    two_reader_activation(false);
}

/// Bob opens first; policy activation must not deny Alice because the original
/// shared execution retained Bob alone. Fresh same-scope reads remain the oracle.
#[test]
fn e17_governance_two_readers_bob_opens_first() {
    two_reader_activation(true);
}
