//! `point` columns (SPEC 20.1): a WGS84 position carried as its 16
//! `point-f64le/1` bytes (longitude, then latitude), read back exactly,
//! refused at admission when the length is wrong or a coordinate is
//! non-finite or out of range.
use std::collections::BTreeMap;

use crate::common;

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};

use common::{allow_all_policies, compile_schema};

fn open_db() -> Db {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("location", ColumnType::Point),
        ColumnDescriptor::new("pickup", ColumnType::Point).nullable(),
    ]);
    let schema = compile_schema(&Schema::from([(
        TableName::new("places"),
        TableSchema::with_policies(columns, allow_all_policies()),
    )]));
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
    )))
    .unwrap()
}

/// `[lng, lat]` as `point-f64le/1`, without validation (so tests can write
/// invalid points).
fn point(lng: f64, lat: f64) -> Value {
    let mut bytes = lng.to_le_bytes().to_vec();
    bytes.extend_from_slice(&lat.to_le_bytes());
    Value::Bytes(bytes)
}

fn insert(db: &Db, byte: u8, location: Value, pickup: Value) -> Result<(), String> {
    let cells: BTreeMap<String, Value> = [
        ("name".to_owned(), Value::String("place".to_owned())),
        ("location".to_owned(), location),
        ("pickup".to_owned(), pickup),
    ]
    .into();
    block_on(db.insert(
        "places",
        cells,
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([byte; 16])),
            ..Default::default()
        },
    ))
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// alice writes points and reads back the same bits, the edges of the
/// ranges and `-0.0` included; a null nullable point stays null.
#[test]
fn point_cells_round_trip_bit_exactly() {
    let db = open_db();
    let location = point(-122.419_416, 37.774_929);
    let pickup = Value::Nullable(Some(Box::new(point(180.0, -90.0))));
    insert(&db, 1, location.clone(), pickup.clone()).unwrap();
    insert(&db, 2, point(-0.0, 5e-324), Value::Nullable(None)).unwrap();

    let prepared = db.prepare_query(&Query::from("places")).unwrap();
    let mut rows = db.read(&prepared).unwrap();
    rows.sort_by_key(|row| row.row_uuid().as_bytes()[0]);
    let cell = |row: usize, name: &str| {
        rows[row]
            .cells()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1
            .cloned()
    };
    assert_eq!(cell(0, "location"), Some(location));
    assert_eq!(cell(0, "pickup"), Some(pickup));
    assert_eq!(cell(1, "location"), Some(point(-0.0, 5e-324)));
    assert_eq!(cell(1, "pickup"), Some(Value::Nullable(None)));
}

/// mallory's writes with the wrong length, a non-finite coordinate or one
/// out of range (including latitude and longitude swapped) are refused at
/// admission and leave nothing behind.
#[test]
fn point_admission_refuses_wrong_length_non_finite_and_out_of_range() {
    let db = open_db();
    let null = || Value::Nullable(None);
    for (location, why) in [
        (Value::Bytes(vec![0; 15]), "too short"),
        (Value::Bytes(vec![0; 24]), "too long"),
        (point(f64::NAN, 0.0), "NaN longitude"),
        (point(0.0, f64::INFINITY), "infinite latitude"),
        (point(180.000_000_1, 0.0), "longitude over 180"),
        (point(0.0, -90.000_000_1), "latitude under -90"),
        (point(37.774_929, -122.419_416), "lat and lng swapped"),
        (Value::String("abc".to_owned()), "not bytes"),
    ] {
        assert!(insert(&db, 3, location, null()).is_err(), "{why}");
    }
    assert!(
        insert(
            &db,
            4,
            point(0.0, 0.0),
            Value::Nullable(Some(Box::new(point(0.0, 91.0))))
        )
        .is_err(),
        "nullable points are checked too"
    );
    let prepared = db.prepare_query(&Query::from("places")).unwrap();
    assert!(db.read(&prepared).unwrap().is_empty());
}

/// Outside a geo index search (SPEC 20.4), a filter compares points as whole
/// values, bit for bit: `-0.0` and `0.0` differ.
#[test]
fn point_filters_compare_whole_values_bit_for_bit() {
    use jazz::query::{Operand, Predicate};
    let db = open_db();
    let null = || Value::Nullable(None);
    insert(&db, 1, point(13.4, 52.5), null()).unwrap();
    insert(&db, 2, point(0.0, 0.0), null()).unwrap();
    insert(&db, 3, point(-0.0, 0.0), null()).unwrap();
    let ids = |literal: Value| {
        let query = Query::from("places").filter(Predicate::Eq(
            Operand::Column("location".to_owned()),
            Operand::Literal(literal),
        ));
        let prepared = db.prepare_query(&query).unwrap();
        let mut ids: Vec<u8> = db
            .read(&prepared)
            .unwrap()
            .iter()
            .map(|row| row.row_uuid().as_bytes()[0])
            .collect();
        ids.sort();
        ids
    };
    assert_eq!(ids(point(13.4, 52.5)), [1]);
    assert_eq!(ids(point(0.0, 0.0)), [2]);
    assert_eq!(ids(point(-0.0, 0.0)), [3]);
}
