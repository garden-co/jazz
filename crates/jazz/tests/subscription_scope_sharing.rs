//! Live subscription groups keep each member's claim source: a query that
//! follows Alice's claims registry and one that carries fixed request claims
//! for Alice may compile to identical SQL today, but they must not share a
//! result that later diverges, before or after a registry refresh.
use jazz::db::{
    Db, DbConfig, DbIdentity, PreparedQuery, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, claim, col, eq, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}

fn open() -> Db {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(policies),
        )
        .build();
    let db = block_on(Db::open(DbConfig::new(
        JazzSchema::new(&schema).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db.seed_settled_mergeable_for_bootstrap(
        "notes",
        RowUuid::from_bytes([4; 16]),
        AuthorSubject::SYSTEM,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    )
    .unwrap();
    db
}

fn claims(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("body"), Value::String(body.into()))])
}

/// Notes whose body equals the reader's `body` claim.
fn by_claim(db: &Db) -> PreparedQuery {
    db.prepare_query(
        &Query::from("notes").filter(eq(col("body"), claim(provider_claim_key("body")))),
    )
    .unwrap()
}

fn subscribe(db: &Db, query: &PreparedQuery) -> SubscriptionStream {
    block_on(db.subscribe_for_identity(query, ReadOpts::default(), alice())).unwrap()
}

fn oracle(db: &Db, query: &PreparedQuery) -> usize {
    block_on(db.all_for_identity(query, ReadOpts::default(), alice()))
        .unwrap()
        .len()
}

fn count(stream: &mut SubscriptionStream, mut n: usize) -> usize {
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    n = 0;
                }
                n = n.checked_sub(removed.len()).expect("removed visible rows") + added.len();
            }
            SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
            _ => {}
        }
    }
    n
}

/// Alice's registry-following and fixed-claims subscriptions compile to the
/// same SQL while her registry matches the fixed claims. Revoking her
/// registry claim must remove the row from the registry-following one and
/// keep it in the fixed one, whichever subscribed first.
#[test]
fn registry_and_fixed_claims_do_not_share_across_registry_change() {
    for fixed_first in [true, false] {
        let db = open();
        db.set_identity_claims(alice(), claims("secret"));
        let registry = by_claim(&db);
        let fixed = by_claim(&db).with_identity_claims(alice(), claims("secret"));
        let (mut registry_stream, mut fixed_stream) = if fixed_first {
            let f = subscribe(&db, &fixed);
            (subscribe(&db, &registry), f)
        } else {
            let r = subscribe(&db, &registry);
            (r, subscribe(&db, &fixed))
        };
        assert_eq!(count(&mut registry_stream, 0), 1);
        assert_eq!(count(&mut fixed_stream, 0), 1);

        db.set_identity_claims(alice(), claims("revoked"));
        assert_eq!(oracle(&db, &registry), 0);
        assert_eq!(oracle(&db, &fixed), 1);
        assert_eq!(
            count(&mut registry_stream, 1),
            0,
            "fixed_first={fixed_first}: registry-following member keeps a revoked row"
        );
        assert_eq!(
            count(&mut fixed_stream, 1),
            1,
            "fixed_first={fixed_first}: fixed-claims member loses its row"
        );
    }
}

/// After a refresh, a new member whose claims match the group's current
/// claims shares it. A fixed-claims member whose SQL matches the group's
/// original SQL stays out (the claim-source split already guarantees that;
/// see `stale_key_does_not_admit_a_same_scope_member` for re-keying).
#[test]
fn refreshed_group_is_not_joined_by_its_stale_key() {
    let db = open();
    db.set_identity_claims(alice(), claims("other"));
    let registry = by_claim(&db);
    let mut registry_stream = subscribe(&db, &registry);
    assert_eq!(count(&mut registry_stream, 0), 0);

    db.set_identity_claims(alice(), claims("secret"));
    assert_eq!(count(&mut registry_stream, 0), 1);

    let fixed = by_claim(&db).with_identity_claims(alice(), claims("other"));
    assert_eq!(oracle(&db, &fixed), 0);
    let mut fixed_stream = subscribe(&db, &fixed);
    assert_eq!(
        count(&mut fixed_stream, 0),
        0,
        "fixed claims 'other' joined the refreshed 'secret' result"
    );

    // A fresh registry-following member shares the refreshed group.
    let before = db.fanout_stats();
    let mut again = subscribe(&db, &registry);
    assert_eq!(count(&mut again, 0), 1);
    assert_eq!(db.fanout_stats().shared_joins, before.shared_joins + 1);
}

/// Registry-following, other-subject, fixed and empty claim scopes live in
/// one store at once, read as Alice explicitly or as the store's own author
/// (Alice). Only Alice's matching fixed scope survives her revocation.
#[test]
fn claim_scopes_in_one_store_follow_their_own_source() {
    let bob = AuthorSubject::for_test_bytes([18; 16]);
    for default_author in [false, true] {
        let db = block_on(Db::open(DbConfig::new(
            open().current_schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([9; 16]),
                author: alice(),
            },
        )))
        .unwrap();
        db.set_authority();
        db.seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([4; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        )
        .unwrap();
        db.set_identity_claims(alice(), claims("secret"));
        let base = by_claim(&db);
        let queries = [
            base.clone(),
            base.clone().with_identity_claims(bob, claims("other")),
            base.clone().with_identity_claims(alice(), claims("secret")),
            base.with_identity_claims(alice(), BTreeMap::new()),
        ];
        let read = |query: &PreparedQuery| {
            if default_author {
                block_on(db.all(query, ReadOpts::default())).unwrap().len()
            } else {
                oracle(&db, query)
            }
        };
        let mut streams = queries
            .iter()
            .map(|query| {
                if default_author {
                    block_on(db.subscribe(query, ReadOpts::default())).unwrap()
                } else {
                    subscribe(&db, query)
                }
            })
            .collect::<Vec<_>>();
        let mut counts = [0; 4];
        for (i, expected) in [1, 1, 1, 0].into_iter().enumerate() {
            counts[i] = count(&mut streams[i], 0);
            assert_eq!(counts[i], expected, "default={default_author}, scope={i}");
        }
        db.set_identity_claims(bob, claims("secret"));
        db.set_identity_claims(alice(), claims("revoked"));
        for (i, expected) in [0, 0, 1, 0].into_iter().enumerate() {
            assert_eq!(read(&queries[i]), expected);
            assert_eq!(
                count(&mut streams[i], counts[i]),
                expected,
                "default={default_author}, scope={i}"
            );
        }
    }
}

/// Within one claim source, two queries reading different claims can
/// compile to the same SQL and parameters once those claims' values swap.
/// A refreshed group must then be found by what it now compiles to: a
/// member that matches only its original key must not join it and see rows
/// its own claims exclude.
#[test]
fn stale_key_does_not_admit_a_same_scope_member() {
    let db = open();
    let registry = |body: &str, alt: &str| {
        BTreeMap::from([
            (provider_claim_key("body"), Value::String(body.into())),
            (provider_claim_key("alt"), Value::String(alt.into())),
        ])
    };
    let by = |key: &str| {
        db.prepare_query(
            &Query::from("notes").filter(eq(col("body"), claim(provider_claim_key(key)))),
        )
        .unwrap()
    };
    let (by_body, by_alt) = (by("body"), by("alt"));
    db.set_identity_claims(alice(), registry("other", "secret"));
    let mut body_stream = subscribe(&db, &by_body);
    assert_eq!(count(&mut body_stream, 0), 0);

    // Swap the values: `by_body` now compiles to what `by_alt` did, and
    // `by_alt` to what `by_body` was opened with.
    db.set_identity_claims(alice(), registry("secret", "other"));
    assert_eq!(count(&mut body_stream, 0), 1);
    assert_eq!(oracle(&db, &by_alt), 0);
    let mut alt_stream = subscribe(&db, &by_alt);
    assert_eq!(
        count(&mut alt_stream, 0),
        0,
        "alt claim 'other' joined the refreshed 'secret' result through its stale key"
    );
}
