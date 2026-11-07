//! Array relations: a relation subscription keeps its own rows
//! beside a plain one over the same root, siblings whose nested relations
//! differ in shape share one record type, and a synced forward array
//! relation ships the rows its arrays list.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, SubscriptionEvent, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("menus")
                    .column("name", ColumnType::Text)
                    .array_fk_column("featured", "categories")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("categories")
                    .fk_column("menu_id", "menus")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("images")
                    .fk_column("category_id", "categories")
                    .column("url", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn seed(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    let cells = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, cells)
        .unwrap();
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

/// Menu 1 with categories 1 (no images) and 2 (two), featuring 2 then 1.
fn menu(db: &Db) {
    seed(
        db,
        "menus",
        id(1, 1),
        &[
            ("name", text("menu")),
            (
                "featured",
                Value::Array(vec![Value::Uuid(id(2, 2).0), Value::Uuid(id(2, 1).0)]),
            ),
        ],
    );
    for n in 1..=2 {
        seed(
            db,
            "categories",
            id(2, n),
            &[
                ("menu_id", Value::Uuid(id(1, 1).0)),
                ("name", text(&format!("category {n}"))),
            ],
        );
    }
    for n in 1..=2 {
        seed(
            db,
            "images",
            id(3, n),
            &[
                ("category_id", Value::Uuid(id(2, 2).0)),
                ("url", text(&format!("image {n}"))),
            ],
        );
    }
}

fn categories_with_images() -> Query {
    Query::from("menus").array_subquery(
        ArraySubquery::new("categories", "categories", "menu_id", "id").nested(ArraySubquery::new(
            "images",
            "images",
            "category_id",
            "id",
        )),
    )
}

fn cell<'a>(row: &'a jazz::db::CurrentRow, name: &str) -> &'a Value {
    row.cells()
        .find_map(|(n, v)| (n == name).then_some(v).flatten())
        .unwrap_or_else(|| panic!("no `{name}` cell"))
}

/// The rows a stream's deltas delivered, each as last delivered.
fn delivered(events: impl Iterator<Item = SubscriptionEvent>) -> Vec<jazz::db::CurrentRow> {
    let mut rows = BTreeMap::new();
    for event in events {
        if let SubscriptionEvent::Delta { added, updated, .. } = event {
            for row in added.into_iter().chain(updated) {
                rows.insert(row.row.row_uuid(), row.row);
            }
        }
    }
    rows.into_values().collect()
}

#[test]
fn a_relation_subscription_beside_a_plain_one_keeps_its_relations() {
    let db = open(0);
    let plain = db.prepare_query(&Query::from("menus")).unwrap();
    let _plain = block_on(db.subscribe(&plain, ReadOpts::default())).unwrap();
    let nested = db.prepare_query(&categories_with_images()).unwrap();
    let mut stream = block_on(db.subscribe(&nested, ReadOpts::default())).unwrap();
    menu(&db);
    block_on(db.tick()).unwrap();
    let rows = delivered(std::iter::from_fn(|| stream.try_next_event()));
    assert_eq!(rows.len(), 1);
    let Value::Array(categories) = cell(&rows[0], "categories") else {
        panic!("categories are an array");
    };
    assert_eq!(categories.len(), 2);
}

#[test]
fn siblings_with_empty_and_full_nested_relations_share_one_type() {
    let db = open(0);
    menu(&db);
    let prepared = db.prepare_query(&categories_with_images()).unwrap();
    let roots = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let rows = db.nest_relations(&prepared, roots).unwrap();
    let Value::Array(categories) = cell(&rows[0], "categories") else {
        panic!("categories are an array");
    };
    let images = categories
        .iter()
        .map(|category| {
            let Value::Record(record) = category else {
                panic!("a category is a record");
            };
            match record.to_values().unwrap().last() {
                Some(Value::Array(images)) => images.len(),
                other => panic!("images are an array, got {other:?}"),
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(images, [0, 2]);
}

#[test]
fn a_synced_forward_array_relation_ships_the_rows_it_lists() {
    let server = open(0);
    menu(&server);
    let client = open(1);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let query = Query::from("menus").array_subquery(ArraySubquery::new(
        "featured_rows",
        "categories",
        "id",
        "featured",
    ));
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    for _ in 0..4 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let roots = block_on(client.all(&prepared, opts)).unwrap();
    let rows = client.nest_relations(&prepared, roots).unwrap();
    let Value::Array(featured) = cell(&rows[0], "featured_rows") else {
        panic!("featured rows are an array");
    };
    let ids: Vec<_> = featured
        .iter()
        .map(|row| match row {
            Value::Record(record) => match record.to_values().unwrap()[0] {
                Value::Uuid(id) => RowUuid(id),
                ref other => panic!("row id, got {other:?}"),
            },
            other => panic!("a record, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, [id(2, 2), id(2, 1)], "in the array's order");
}

fn images_of(row: &jazz::db::CurrentRow, category: usize) -> Vec<Value> {
    let Value::Array(categories) = cell(row, "categories") else {
        panic!("categories are an array");
    };
    let Value::Record(record) = &categories[category] else {
        panic!("a category is a record");
    };
    match record.to_values().unwrap().last() {
        Some(Value::Array(images)) => images.clone(),
        other => panic!("images are an array: {other:?}"),
    }
}

/// A transaction's relation read nests inside its overlay, so it sees the
/// rows the transaction staged.
#[test]
fn a_relation_read_in_a_transaction_sees_staged_children() {
    let db = open(1);
    menu(&db);
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(db.tx_ref(tx).insert(
        "images",
        BTreeMap::from([
            ("category_id".to_owned(), Value::Uuid(id(2, 1).0)),
            ("url".to_owned(), text("staged")),
        ]),
        Default::default(),
    ))
    .unwrap();
    let prepared = db.prepare_query(&categories_with_images()).unwrap();
    let rows = block_on(db.tx_ref(tx).all_prepared(&prepared)).unwrap();
    assert_eq!(
        images_of(&rows[0], 0).len(),
        1,
        "the staged image is nested"
    );
    db.abandon_transaction_handle(tx).unwrap();
}

/// A serializable relation read holds its related rows too: a change to one
/// before the commit is a conflict.
#[test]
fn a_change_to_a_related_row_conflicts_with_a_serializable_relation_read() {
    let db = open(1);
    menu(&db);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let prepared = db.prepare_query(&categories_with_images()).unwrap();
    block_on(db.tx_ref(tx).all_prepared(&prepared)).unwrap();
    block_on(db.update(
        "images",
        id(3, 1),
        BTreeMap::from([("url".to_owned(), text("moved"))]),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tx_ref(tx).update(
        "menus",
        id(1, 1),
        BTreeMap::from([("name".to_owned(), text("renamed"))]),
        Default::default(),
    ))
    .unwrap();
    let error = block_on(db.commit_serializable_handle(tx)).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict, "{error}");
}

/// A required relation nested under another leaves out the child rows it
/// finds nothing for, as it would its root rows.
#[test]
fn a_nested_required_relation_drops_children_without_rows() {
    let db = open(1);
    menu(&db);
    let query = Query::from("menus").array_subquery(
        ArraySubquery::new("categories", "categories", "menu_id", "id").nested(
            ArraySubquery::new("images", "images", "category_id", "id")
                .requirement(ArraySubqueryRequirement::AtLeastOne),
        ),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let roots = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let rows = db.nest_relations(&prepared, roots).unwrap();
    let Value::Array(categories) = cell(&rows[0], "categories") else {
        panic!("categories are an array");
    };
    assert_eq!(categories.len(), 1, "category 1 has no images");
    assert_eq!(images_of(&rows[0], 0).len(), 2);
}
