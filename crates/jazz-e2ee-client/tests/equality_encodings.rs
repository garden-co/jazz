//! Equality search on encrypted JSON and array columns (`E2EE_EQUALITY_FORMAT.md`,
//! value encodings): JSON by its exact stored text, an array element by
//! element. A query finds exactly the rows plain equality would, locally and
//! at the server, which compares tokens only.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{Db, DbConfig, ReadOpts, ReadTier, block_on};
use jazz::e2ee::{E2eeDeclarations, EncryptionEntry};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_e2ee_client::MemoryStore;

fn tags_type() -> ColumnType {
    ColumnType::Array {
        element: Box::new(ColumnType::Text),
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("name", ColumnType::Text)
                    .policies(allow_all()),
            )
            .table(
                TableSchemaBuilder::new("records")
                    .fk_column("projectId", "projects")
                    .column("label", ColumnType::Text)
                    .column("meta", ColumnType::Bytea)
                    .nullable_column("__e2ee_eq_meta", ColumnType::Bytea)
                    .column("tags", ColumnType::Bytea)
                    .nullable_column("__e2ee_eq_tags", ColumnType::Bytea)
                    .policies(allow_all()),
            )
            .build(),
    )
    .unwrap()
}

fn declarations() -> E2eeDeclarations {
    E2eeDeclarations {
        app_id: app_id(),
        environment: ENVIRONMENT.to_owned(),
        settings: Default::default(),
        tables: BTreeMap::from([(
            "records".to_owned(),
            EncryptionEntry {
                space: "projectId".to_owned(),
                scope: "projects".to_owned(),
                columns: BTreeMap::from([
                    (
                        "meta".to_owned(),
                        encrypted(ColumnType::Json { schema: None }, false),
                    ),
                    ("tags".to_owned(), encrypted(tags_type(), false)),
                ]),
                equality: vec!["meta".to_owned(), "tags".to_owned()],
            },
        )]),
    }
}

fn tags(items: &[&str]) -> Value {
    Value::Array(items.iter().map(|item| text(item)).collect())
}

/// Contract: an equality query on an encrypted JSON column and on an
/// encrypted array(text) column finds exactly the rows plain equality
/// would, in the device's own store and at the server: JSON by its exact
/// text (the same object with its keys in another order is another value),
/// arrays element by element (the same elements in another order, or a
/// prefix, do not match).
///
/// ```text
/// alice ──seal, tokens──► server       query eq(meta, text) ──► tokens ──► same rows
/// ```
#[test]
fn equality_on_json_and_array_columns_finds_what_plain_equality_would() {
    let server = block_on(Db::open(DbConfig::new(schema(), identity(0)))).unwrap();
    server.set_authority().unwrap();
    let client = open_client_with(Rc::new(MemoryStore::new()), declarations());
    let db = block_on(Db::open(
        DbConfig::new(schema(), identity(1)).with_e2ee(client.hooks()),
    ))
    .unwrap();
    let (_link, _) = link(&db, &server);
    pump(&server, &db);
    assert!(client.prepare(&db).unwrap());
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);

    let rows = [
        (r#"{"a":1,"b":2}"#, ["x", "y"].as_slice()),
        (r#"{"b":2,"a":1}"#, ["y", "x"].as_slice()),
        (r#"{"a":1,"b":2}"#, ["x"].as_slice()),
        (r#"null"#, [].as_slice()),
    ];
    let mut ids = Vec::new();
    for (index, (meta, items)) in rows.iter().enumerate() {
        let cells = BTreeMap::from([
            ("projectId".to_owned(), Value::Uuid(space)),
            ("label".to_owned(), text(&format!("row{index}"))),
            ("meta".to_owned(), text(meta)),
            ("tags".to_owned(), tags(items)),
        ]);
        ids.push(
            block_on(db.insert("records", cells, Default::default()))
                .unwrap()
                .row_uuid(),
        );
    }
    pump(&server, &db);

    let in_space = || Query::from("records").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    let found = |query: Query, tier: ReadTier| -> Vec<jazz::ids::RowUuid> {
        let prepared = db.prepare_query(&query).unwrap();
        let mut rows = drive(
            &server,
            &[&db],
            db.all(&prepared, ReadOpts::for_read_tier(tier)),
        )
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expect = |picked: &[usize]| {
        let mut expected: Vec<_> = picked.iter().map(|index| ids[*index]).collect();
        expected.sort();
        expected
    };
    for tier in [ReadTier::LocalOnly, ReadTier::Remote] {
        let by_meta = |text_value: &str| {
            found(
                in_space().filter(eq(col("meta"), lit(text(text_value)))),
                tier,
            )
        };
        assert_eq!(by_meta(r#"{"a":1,"b":2}"#), expect(&[0, 2]), "{tier:?}");
        assert_eq!(by_meta(r#"{"b":2,"a":1}"#), expect(&[1]), "{tier:?}");
        assert_eq!(by_meta(r#"null"#), expect(&[3]), "{tier:?}");
        assert!(by_meta(r#"{"a":1}"#).is_empty(), "{tier:?}");

        let by_tags =
            |items: &[&str]| found(in_space().filter(eq(col("tags"), lit(tags(items)))), tier);
        assert_eq!(by_tags(&["x", "y"]), expect(&[0]), "{tier:?}");
        assert_eq!(by_tags(&["y", "x"]), expect(&[1]), "{tier:?}");
        assert_eq!(by_tags(&["x"]), expect(&[2]), "{tier:?}");
        assert_eq!(by_tags(&[]), expect(&[3]), "{tier:?}");
        assert!(by_tags(&["x", "y", "z"]).is_empty(), "{tier:?}");
    }
}
