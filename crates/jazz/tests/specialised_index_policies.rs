//! A search returns only rows its reader may read (SPEC 20.7, `INV-IX-3`),
//! for each real kind, under a real read policy: alice owns places; bob
//! reads his own and those a share row names him in. Rows bob may not read
//! are absent; a vector search's hidden rows do not take readable rows'
//! places among its `k`, and only readable candidates count against the
//! candidate cap; a live search follows the policy's evidence (a share row
//! added or removed).

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{IndexOp, MatchMode, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder, VectorMetric,
};

use crate::common::{compile_schema, exists, outer_eq, session_eq};

const ME: [&str; 2] = ["identity", "subject"];

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xb0; 16])
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

/// `places(owner, name, location, embedding)` with a text, a geo and a
/// vector index, readable by their owner or a user a `place_shares` row
/// names; `place_shares` readable by the user it names.
fn schema() -> JazzSchema {
    let writes = |read: PolicyExpr| {
        TablePolicies::new()
            .with_select(read)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("places")
                    .column("owner", ColumnType::Text)
                    .column("name", ColumnType::Text)
                    .column("location", ColumnType::Point)
                    .column("embedding", ColumnType::Vector { dimensions: 2 })
                    .index_only(["owner"])
                    .policies(writes(PolicyExpr::or(vec![
                        session_eq("owner", &ME),
                        exists(
                            "place_shares",
                            vec![outer_eq("place", "id"), session_eq("user_id", &ME)],
                        ),
                    ])))
                    .text_index("search", [("name", 1)], "jazz-text/1:en", false)
                    .geo_index("near", "location")
                    .vector_index("similar", "embedding", VectorMetric::L2),
            )
            .table(
                TableSchemaBuilder::new("place_shares")
                    .fk_column("place", "places")
                    .column("user_id", ColumnType::Text)
                    .policies(writes(session_eq("user_id", &ME))),
            )
            .build(),
    )
}

fn open() -> Db {
    jazz::use_test_kinds(false);
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn place(db: &Db, id: u8, owner: AuthorSubject, embedding: [f32; 2]) {
    block_on(
        db.insert(
            "places",
            BTreeMap::from([
                ("owner".to_owned(), subject(owner)),
                ("name".to_owned(), Value::String(format!("Cafe {id}"))),
                (
                    "location".to_owned(),
                    Value::Bytes(
                        jazz::records::encode_point_f64le(13.4 + f64::from(id) * 0.001, 52.5)
                            .unwrap()
                            .to_vec(),
                    ),
                ),
                (
                    "embedding".to_owned(),
                    Value::Bytes(embedding.iter().flat_map(|x| x.to_le_bytes()).collect()),
                ),
            ]),
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

fn share(db: &Db, id: u8, place: u8, with: AuthorSubject) {
    block_on(db.insert(
        "place_shares",
        BTreeMap::from([
            ("place".to_owned(), Value::Uuid(row(place).0)),
            ("user_id".to_owned(), subject(with)),
        ]),
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice owns places 1-4, all at the query's point and nearest the query
/// vector; bob owns 5 and 6, farther away; alice shares 2 with bob.
fn seeded() -> Db {
    let db = open();
    for id in 1..=4 {
        place(&db, id, alice(), [0.0, f32::from(id) * 0.1]);
    }
    place(&db, 5, bob(), [5.0, 5.0]);
    place(&db, 6, bob(), [6.0, 6.0]);
    share(&db, 100, 2, bob());
    db
}

fn ids_as(db: &Db, query: &Query, who: AuthorSubject) -> Result<Vec<u8>, jazz::db::Error> {
    let prepared = db.prepare_query(query)?;
    Ok(
        block_on(db.all_for_identity(&prepared, ReadOpts::default(), who))?
            .iter()
            .map(|row| row.row_uuid().0.as_bytes()[0])
            .collect(),
    )
}

fn text() -> Query {
    Query::from("places").search(
        "search",
        IndexOp::Match {
            text: "cafe".into(),
            mode: MatchMode::Any,
        },
    )
}

fn near() -> Query {
    Query::from("places").search(
        "near",
        IndexOp::WithinRadius {
            lat: 52.5,
            lng: 13.4,
            meters: 5_000.0,
        },
    )
}

fn nearest(k: u32) -> Query {
    Query::from("places").search(
        "similar",
        IndexOp::Nearest {
            vector: vec![0.0, 0.0],
            k,
        },
    )
}

/// Each kind returns bob only the rows he may read: his own and the one
/// shared with him. Alice's private rows match too, and are absent.
#[test]
fn every_kind_returns_only_readable_rows() {
    let db = seeded();
    assert_eq!(ids_as(&db, &text(), bob()).unwrap(), [2, 5, 6]);
    assert_eq!(ids_as(&db, &near(), bob()).unwrap(), [2, 5, 6]);
    assert_eq!(ids_as(&db, &nearest(10), bob()).unwrap(), [2, 5, 6]);
    assert_eq!(ids_as(&db, &text(), alice()).unwrap(), [1, 2, 3, 4]);
}

/// A vector search keeps the `k` nearest rows its reader may read: alice's
/// hidden rows are nearer than bob's own, and do not take their places.
#[test]
fn hidden_rows_do_not_displace_readable_ones_among_the_k_nearest() {
    let db = seeded();
    assert_eq!(ids_as(&db, &nearest(2), bob()).unwrap(), [2, 5]);
    assert_eq!(ids_as(&db, &nearest(1), bob()).unwrap(), [2]);
    assert_eq!(ids_as(&db, &nearest(2), alice()).unwrap(), [1, 2]);
}

/// Only candidates the reader may read count against the candidate cap
/// (lowered to 3 here): bob's three readable rows fit; alice's four do not.
#[test]
fn only_readable_candidates_count_against_the_cap() {
    let db = seeded();
    jazz::set_vector_cap_for_test(Some(3));
    let bob_reads = ids_as(&db, &nearest(10), bob());
    let alice_reads = ids_as(&db, &nearest(10), alice());
    jazz::set_vector_cap_for_test(None);
    assert_eq!(bob_reads.unwrap(), [2, 5, 6]);
    assert_eq!(
        alice_reads
            .expect_err("over the cap")
            .reason
            .map(|r| r.as_str()),
        Some("vector_candidates_over_cap")
    );
}

/// A live search follows its policy's evidence: when alice shares a place
/// with bob, it joins his result; when the share goes, so does the place.
#[test]
fn a_live_search_follows_the_policys_evidence() {
    let db = seeded();
    for (n, query) in [text(), near(), nearest(10)].into_iter().enumerate() {
        let share_id = 101 + n as u8;
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), bob())).unwrap();
        block_on(db.tick()).unwrap();
        let mut seen = |stream: &mut jazz::db::SubscriptionStream| {
            let (mut added, mut removed) = (Vec::new(), Vec::new());
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        added: a,
                        removed: r,
                        ..
                    } => {
                        added.extend(a.iter().map(|row| row.row_uuid().0.as_bytes()[0]));
                        removed.extend(r.iter().map(|row| row.row_uuid.0.as_bytes()[0]));
                    }
                    SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
                    SubscriptionEvent::Closed => {}
                }
            }
            (added, removed)
        };
        let _ = seen(&mut stream);
        share(&db, share_id, 3, bob());
        block_on(db.tick()).unwrap();
        assert_eq!(seen(&mut stream), (vec![3], vec![]), "{query:?}");
        block_on(db.delete("place_shares", row(share_id), Default::default())).unwrap();
        block_on(db.tick()).unwrap();
        assert_eq!(seen(&mut stream), (vec![], vec![3]), "{query:?}");
    }
}
