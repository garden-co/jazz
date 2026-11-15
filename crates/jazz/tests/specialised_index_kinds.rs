//! The real kinds of specialised indexes (SPEC 20.10) through a store:
//! text (analyzer `jazz-text/1:en`, field weights, prefix), geo (radius and
//! box, antimeridian included) and vector (cosine), searched with the
//! public query builders. Their pure parts (analysis, impacts, cells,
//! covers, scores) are tested in `jazz-index`; these tests pin what a
//! search through a store returns.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{IndexOp, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder, VectorMetric};

use crate::common::{allow_all_policies, compile_schema};

/// `places(name, about?, lat, lng, embedding)` with `search` over name
/// (weight 4) and about (1), `near` over lat/lng and `similar` over the
/// embedding (cosine).
fn schema_with(name_weight: u8) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("places")
                    .column("name", ColumnType::Text)
                    .nullable_column("about", ColumnType::Text)
                    .column("lat", ColumnType::Double)
                    .column("lng", ColumnType::Double)
                    .column("embedding", ColumnType::Vector { dimensions: 3 })
                    .policies(allow_all_policies())
                    .text_index(
                        "search",
                        [("name", name_weight), ("about", 1)],
                        "jazz-text/1:en",
                        true,
                    )
                    .geo_index("near", "lat", "lng")
                    .vector_index("similar", "embedding", VectorMetric::Cosine),
            )
            .build(),
    )
}

fn config(schema: JazzSchema) -> DbConfig {
    jazz::use_test_kinds(false);
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn open() -> Db {
    block_on(Db::open(config(schema_with(4)))).unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn f32le(v: [f32; 3]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

fn place(
    db: &Db,
    id: u8,
    name: &str,
    about: Option<&str>,
    lat: f64,
    lng: f64,
    embedding: [f32; 3],
) {
    block_on(db.insert(
        "places",
        BTreeMap::from([
            ("name".to_owned(), Value::String(name.to_owned())),
            (
                "about".to_owned(),
                Value::Nullable(about.map(|a| Box::new(Value::String(a.to_owned())))),
            ),
            ("lat".to_owned(), Value::F64(lat)),
            ("lng".to_owned(), Value::F64(lng)),
            ("embedding".to_owned(), Value::Bytes(f32le(embedding))),
        ]),
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Berlin, Potsdam, Hamburg, a café in Suva (Fiji, 178.4°E) and one in
/// Apia (Samoa, 171.8°W).
fn seed(db: &Db) {
    place(
        db,
        1,
        "Berlin",
        Some("capital, running club"),
        52.52,
        13.405,
        [1.0, 0.0, 0.0],
    );
    place(
        db,
        2,
        "Potsdam",
        Some("palaces near Berlin"),
        52.39,
        13.065,
        [0.9, 0.1, 0.0],
    );
    place(
        db,
        3,
        "Hamburg",
        Some("harbour"),
        53.55,
        9.993,
        [0.0, 1.0, 0.0],
    );
    place(db, 4, "Café Suva", None, -18.14, 178.44, [0.0, 0.0, 1.0]);
    place(
        db,
        5,
        "Apia runners",
        Some("café by the sea"),
        -13.83,
        -171.76,
        [0.0, 0.2, 1.0],
    );
}

fn ids(db: &Db, query: &Query) -> Vec<u8> {
    try_ids(db, query).unwrap()
}

fn try_ids(db: &Db, query: &Query) -> Result<Vec<u8>, jazz::db::Error> {
    let prepared = db.prepare_query(query)?;
    Ok(block_on(db.all(&prepared, ReadOpts::default()))?
        .iter()
        .map(|row| row.row_uuid().0.as_bytes()[0])
        .collect())
}

fn text(words: &str) -> Query {
    Query::from("places").search("search", IndexOp::Match { text: words.into() })
}

/// Text matches every query term in some field, after analysis
/// (lowercasing, diacritic folding, English stemming); a field's weight
/// ranks a match in the name above one in the description; with `prefix`
/// the last word also matches as the start of a term.
#[test]
fn text_search_analyzes_weighs_and_completes_prefixes() {
    let db = open();
    seed(&db);
    // "Berlin" is in Berlin's name (4) and Potsdam's description (1).
    assert_eq!(ids(&db, &text("berlin")), [1, 2]);
    // Folding: "cafe" finds "Café", in a name (Suva) above a description.
    assert_eq!(ids(&db, &text("CAFE")), [4, 5]);
    // Stemming: "runner" and "running" both stem to a term starting "run".
    assert_eq!(ids(&db, &text("run")), [5, 1]);
    // Every term must match: AND.
    assert_eq!(ids(&db, &text("berlin palaces")), [2]);
    assert_eq!(ids(&db, &text("berlin harbour")), Vec::<u8>::new());
    // A prefix completes the last word only.
    assert_eq!(ids(&db, &text("harb")), [3]);
    let error = try_ids(&db, &text("¡¿!")).expect_err("no terms");
    assert!(error.message.contains("no terms after analysis"), "{error}");
    // An edit re-indexes the row.
    block_on(db.update(
        "places",
        row(3),
        BTreeMap::from([("about".to_owned(), Value::Nullable(None))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(ids(&db, &text("harbour")), Vec::<u8>::new());
}

/// A radius search returns the points within it, nearest first; a box
/// returns the points inside it by id, also across the antimeridian.
/// Out-of-range regions are refused.
#[test]
fn geo_search_finds_points_in_a_radius_or_a_box() {
    let db = open();
    seed(&db);
    let radius = |lat: f64, lng: f64, meters: f64| {
        Query::from("places").search("near", IndexOp::WithinRadius { lat, lng, meters })
    };
    // Potsdam is ~26 km from central Berlin, Hamburg ~255 km.
    assert_eq!(ids(&db, &radius(52.5, 13.4, 50_000.0)), [1, 2]);
    assert_eq!(ids(&db, &radius(52.4, 13.0, 50_000.0)), [2, 1]);
    assert_eq!(ids(&db, &radius(52.5, 13.4, 300_000.0)), [1, 2, 3]);
    let boxed = |south: f64, west: f64, north: f64, east: f64| {
        Query::from("places").search(
            "near",
            IndexOp::WithinBox {
                south,
                west,
                north,
                east,
            },
        )
    };
    assert_eq!(ids(&db, &boxed(50.0, 5.0, 55.0, 15.0)), [1, 2, 3]);
    assert_eq!(ids(&db, &boxed(52.0, 10.0, 55.0, 15.0)), [1, 2]);
    // From 170°E across the antimeridian to 170°W.
    assert_eq!(ids(&db, &boxed(-20.0, 170.0, -10.0, -170.0)), [4, 5]);
    let error = try_ids(&db, &radius(52.5, 13.4, 2_000_000.0)).expect_err("too far");
    assert!(error.message.contains("radius"), "{error}");
}

/// A vector search keeps the `k` rows most similar to the query by
/// cosine, best first, among the rows the store holds.
#[test]
fn vector_search_keeps_the_k_most_similar_rows() {
    let db = open();
    seed(&db);
    let nearest = |vector: [f32; 3], k: u32| {
        Query::from("places").search(
            "similar",
            IndexOp::Nearest {
                vector: vector.to_vec(),
                k,
            },
        )
    };
    assert_eq!(ids(&db, &nearest([1.0, 0.0, 0.0], 2)), [1, 2]);
    // Cosine ignores length.
    assert_eq!(ids(&db, &nearest([0.0, 0.0, 7.0], 2)), [4, 5]);
    let error = try_ids(&db, &nearest([0.0, 0.0, 0.0], 2)).expect_err("zero vector");
    assert_eq!(error.code, jazz::db::ErrorCode::Query, "{error}");
    let error = try_ids(
        &db,
        &Query::from("places").search(
            "similar",
            IndexOp::Nearest {
                vector: vec![1.0, 0.0],
                k: 2,
            },
        ),
    )
    .expect_err("wrong dimension");
    assert!(
        error.message.contains("2 dimensions; the index has 3"),
        "{error}"
    );
}

/// An index outlives its store's process: a reopened store searches it
/// without rebuilding, and one reopened with a changed declaration
/// rebuilds it before answering (a client builds at once).
#[test]
fn indexes_persist_across_reopens_and_rebuild_when_declared_anew() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("places.sqlite");
    let db = block_on(Db::open_at(
        config(schema_with(4)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    seed(&db);
    drop(db);
    let db = block_on(Db::open_at(
        config(schema_with(4)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(ids(&db, &text("berlin")), [1, 2]);
    drop(db);
    // Weighting the description above the name reorders the same rows.
    let db = block_on(Db::open_at(
        config(schema_with(1)),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(
        ids(&db, &text("berlin")),
        [1, 2],
        "equal weights: ties by id"
    );
    let rows: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row("SELECT count(*) FROM _jazz_ix WHERE state = 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(rows, 3, "every index is built");
}
