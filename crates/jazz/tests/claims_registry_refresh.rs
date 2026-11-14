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
            SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
            _ => {}
        }
    }
    n
}
/// Alice executes a query carrying Bob's request claims. Bob's claims are ignored
/// by execution, so Alice's registry remains live and must drive revocation.
#[test]
fn cross_subject_request_claims_follow_effective_registry() {
    let db = open(schema(
        PolicyExpr::eq_session("body", vec!["claims".into(), "body".into()]),
        false,
    ));
    seed(
        &db,
        "notes",
        1,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    );
    db.set_identity_claims(alice(), claims("secret"));
    let q = db
        .prepare_query(&Query::from("notes"))
        .unwrap()
        .with_identity_claims(bob(), claims("unused"));
    let mut sub = block_on(db.subscribe_for_identity(&q, ReadOpts::default(), alice())).unwrap();
    assert_eq!(count(&mut sub, 0), 1);
    db.set_identity_claims(alice(), claims("revoked"));
    assert_eq!(
        block_on(db.all_for_identity(&q, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        count(&mut sub, 1),
        0,
        "subscription must follow the effective author registry, not unrelated fixed claims"
    );
}
#[derive(Clone, Copy, Debug)]
enum ClaimScope {
    Registry,
    OtherSubject,
    Fixed,
    Empty,
}

// Every call opens its own store and exactly one subscription. This isolates
// claim-source invalidation from the separate equal-SQL group-sharing defect.
fn isolated_scope(default_author: bool, scope: ClaimScope) {
    use jazz::query::{claim, col, eq, provider_claim_key};
    let db = block_on(Db::open(DbConfig::new(
        schema(PolicyExpr::True, false),
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    db.set_authority();
    seed(
        &db,
        "notes",
        1,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    );
    db.set_identity_claims(alice(), claims("secret"));
    let prepared = db
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), claim(provider_claim_key("body")))),
        )
        .unwrap();
    let query = match scope {
        ClaimScope::Registry => prepared,
        ClaimScope::OtherSubject => prepared.with_identity_claims(bob(), claims("unused")),
        ClaimScope::Fixed => prepared.with_identity_claims(alice(), claims("secret")),
        ClaimScope::Empty => prepared.with_identity_claims(alice(), BTreeMap::new()),
    };
    let mut stream = if default_author {
        block_on(db.subscribe(&query, ReadOpts::default()))
    } else {
        block_on(db.subscribe_for_identity(&query, ReadOpts::default(), alice()))
    }
    .unwrap();
    let before = usize::from(!matches!(scope, ClaimScope::Empty));
    assert_eq!(count(&mut stream, 0), before);
    db.set_identity_claims(bob(), claims("unrelated"));
    assert_eq!(
        count(&mut stream, before),
        before,
        "unrelated registry change: default={default_author}, scope={scope:?}"
    );
    db.set_identity_claims(alice(), claims("revoked"));
    let expected = usize::from(matches!(scope, ClaimScope::Fixed));
    let fresh = if default_author {
        block_on(db.all(&query, ReadOpts::default()))
    } else {
        block_on(db.all_for_identity(&query, ReadOpts::default(), alice()))
    }
    .unwrap();
    assert_eq!(
        fresh.len(),
        expected,
        "default={default_author}, scope={scope:?}"
    );
    assert_eq!(
        count(&mut stream, before),
        expected,
        "default={default_author}, scope={scope:?}"
    );
}

/// alice's default-author query ignores bob's supplied scope, so alice's
/// registered-claim revocation must remove the live row just as a fresh read does.
#[test]
fn unrelated_request_claims_follow_default_author_registry() {
    isolated_scope(true, ClaimScope::OtherSubject);
}

/// alice's unscoped explicit and default-author reads both follow her registry.
#[test]
fn unscoped_reads_follow_registry_changes() {
    for default_author in [false, true] {
        isolated_scope(default_author, ClaimScope::Registry);
    }
}

/// alice's matching explicit claims remain fixed when her registry changes,
/// for both explicit-identity and default-author reads in separate stores.
#[test]
fn matching_fixed_claims_ignore_registry_changes() {
    for default_author in [false, true] {
        isolated_scope(default_author, ClaimScope::Fixed);
    }
}

/// alice's explicitly empty claim scope remains empty despite registry updates,
/// for both explicit-identity and default-author reads in separate stores.
#[test]
fn explicit_empty_claims_ignore_registry_changes() {
    for default_author in [false, true] {
        isolated_scope(default_author, ClaimScope::Empty);
    }
}
