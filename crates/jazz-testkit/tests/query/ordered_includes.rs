//! Ordered reverse includes whose sort key is not part of the included
//! projection (#2739).
//!
//! Each case compares an include ordered by an *unselected* column with the
//! same include that also selects that column. The two must agree on the
//! included row ids and their order, and the unselected sort column must not
//! leak into the included payload. Local reads, remote reads, and maintained
//! subscriptions all compile the same collector graph, so each is exercised.

use std::time::Duration;

use crate::support::{QueryRows, TestingClient, wait_for_rows};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, ReadTier, Schema, SchemaBuilder, SubscriptionStreamItem,
    TableSchema, Value,
};
use jazz_server::JazzServer;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(10);

macro_rules! local_tokio_test {
    ($(#[$attr:meta])* async fn $name:ident() $body:block) => {
        $(#[$attr])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            tokio::task::LocalSet::new()
                .run_until(async $body)
                .await;
        }
    };
}

fn menu_schema() -> Schema {
    use jazz::tools::test_support::AllowAll;
    SchemaBuilder::new()
        .table(TableSchema::builder("menus").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("categories")
                .fk_column("menu_id", "menus")
                .column("name", ColumnType::Text)
                .column("order_key", ColumnType::Text)
                .nullable_column("priority", ColumnType::Integer),
        )
        .table(
            TableSchema::builder("images")
                .fk_column("category_id", "categories")
                .column("url", ColumnType::Text),
        )
        .allow_all()
        .build()
}

/// Category ids, in insertion order. `order_key` ties between 13 and 14 so a
/// slice has to fall back to the row-id tie-breaker; `priority` is NULL for 12.
const CATEGORIES: [(u128, &str, &str, Option<i32>); 4] = [
    (11, "Cakes", "c", Some(2)),
    (12, "Apps", "a", None),
    (13, "Bread", "b", Some(1)),
    (14, "Buns", "b", Some(1)),
];

struct Fixture {
    server: JazzServer,
    writer: JazzClient,
    reader: JazzClient,
    menu_id: ObjectId,
}

impl Fixture {
    async fn start() -> Self {
        let schema = menu_schema();
        let server = JazzServer::start_with_schema(schema.clone())
            .await
            .expect("start test server");
        let writer = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("ordered-includes-writer")
            .ready_on("menus", READY_TIMEOUT)
            .connect()
            .await;
        let reader = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema)
            .with_user_id("ordered-includes-reader")
            .ready_on("menus", READY_TIMEOUT)
            .connect()
            .await;

        let menu_id = writer
            .insert_with_id(
                "menus",
                uuid::Uuid::from_u128(1),
                row_input!("name" => "Menu"),
            )
            .expect("insert menu")
            .0;
        for (id, name, order_key, priority) in CATEGORIES {
            writer
                .insert_with_id(
                    "categories",
                    uuid::Uuid::from_u128(id),
                    row_input!(
                        "menu_id" => menu_id,
                        "name" => name,
                        "order_key" => order_key,
                        "priority" => priority.map_or(Value::Null, Value::Integer),
                    ),
                )
                .expect("insert category");
        }
        for (id, category) in [(21, 12), (22, 13), (23, 13)] {
            writer
                .insert_with_id(
                    "images",
                    uuid::Uuid::from_u128(id),
                    row_input!(
                        "category_id" => ObjectId::from_uuid(uuid::Uuid::from_u128(category)),
                        "url" => format!("https://img.example/{id}"),
                    ),
                )
                .expect("insert image");
        }

        // Wait until the server has everything, using a query shape that is
        // not under test.
        wait_for_rows(
            &reader,
            Query::from("menus").array_subquery(
                ArraySubquery::new("categories", "categories", "menu_id", "id")
                    .nested(ArraySubquery::new("images", "images", "category_id", "id")),
            ),
            "reader sees every category and image",
            |rows| {
                rows.iter()
                    .any(|(id, values)| {
                        *id == menu_id
                            && values[1].as_array().is_some_and(|categories| {
                                categories.len() == CATEGORIES.len()
                                    && categories
                                        .iter()
                                        .filter_map(|category| category.as_row())
                                        .map(|category| {
                                            category
                                                .last()
                                                .and_then(Value::as_array)
                                                .map_or(0, <[Value]>::len)
                                        })
                                        .sum::<usize>()
                                        == 3
                            })
                    })
                    .then_some(())
            },
        )
        .await;

        Self {
            server,
            writer,
            reader,
            menu_id,
        }
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.reader.shutdown().await.expect("shutdown reader");
        self.server.shutdown().await;
    }
}

/// One ordered include shape. `select` is the visible projection; `order` is
/// deliberately left out of it.
#[derive(Clone)]
struct Case {
    label: &'static str,
    select: &'static [&'static str],
    order: &'static [(&'static str, OrderDirection)],
    offset: usize,
    limit: Option<usize>,
    nested_images: bool,
    /// The expected id order, when it does not depend on commit timestamps.
    expected: Option<&'static [u128]>,
}

impl Case {
    fn include(&self, select: &[&str]) -> ArraySubquery {
        let mut include = ArraySubquery::new("categories", "categories", "menu_id", "id")
            .select(select.iter().copied());
        for (column, direction) in self.order {
            include = include.order_by(*column, *direction);
        }
        if self.offset > 0 {
            include = include.offset(self.offset);
        }
        if let Some(limit) = self.limit {
            include = include.limit(limit);
        }
        if self.nested_images {
            include = include.nested(
                ArraySubquery::new("images", "images", "category_id", "id")
                    .select(["url"])
                    .order_by("url", OrderDirection::Desc),
            );
        }
        include
    }

    fn query(&self) -> Query {
        // The fixture holds exactly one menu, so no root filter is needed.
        Query::from("menus")
            .select(["name"])
            .array_subquery(self.include(self.select))
    }

    /// The same include with every sort column also selected. This is the
    /// shape that already worked before #2739 and serves as the oracle.
    fn control_query(&self) -> Query {
        let mut select = self.select.to_vec();
        select.extend(self.order.iter().map(|(column, _)| *column));
        // The fixture holds exactly one menu, so no root filter is needed.
        Query::from("menus")
            .select(["name"])
            .array_subquery(self.include(&select))
    }
}

fn cases() -> Vec<Case> {
    use OrderDirection::{Asc, Desc};
    vec![
        Case {
            label: "unselected text sort, asc",
            select: &["name"],
            order: &[("order_key", Asc)],
            offset: 0,
            limit: None,
            nested_images: false,
            expected: Some(&[12, 13, 14, 11]),
        },
        Case {
            label: "unselected text sort, desc",
            select: &["name"],
            order: &[("order_key", Desc)],
            offset: 0,
            limit: None,
            nested_images: false,
            expected: Some(&[11, 13, 14, 12]),
        },
        Case {
            label: "unselected sort with a nested include",
            select: &["name"],
            order: &[("order_key", Asc)],
            offset: 0,
            limit: None,
            nested_images: true,
            expected: Some(&[12, 13, 14, 11]),
        },
        Case {
            label: "unselected nullable sort column",
            select: &["name"],
            order: &[("priority", Asc)],
            offset: 0,
            limit: None,
            nested_images: false,
            expected: None,
        },
        Case {
            label: "unselected nullable sort column, desc, nested",
            select: &["name"],
            order: &[("priority", Desc)],
            offset: 0,
            limit: None,
            nested_images: true,
            expected: None,
        },
        Case {
            label: "unselected $createdAt sort",
            select: &["name"],
            order: &[("$createdAt", Asc)],
            offset: 0,
            limit: None,
            nested_images: false,
            expected: None,
        },
        Case {
            label: "slice over a tied unselected sort key",
            select: &["name"],
            order: &[("order_key", Asc)],
            offset: 1,
            limit: Some(2),
            nested_images: false,
            expected: Some(&[13, 14]),
        },
        Case {
            label: "slice with two unselected sort keys",
            select: &["name"],
            order: &[("priority", Desc), ("order_key", Asc)],
            offset: 1,
            limit: Some(2),
            nested_images: true,
            expected: None,
        },
    ]
}

/// `(category id, visible payload)` for each included category.
fn included(values: &[Value]) -> Vec<(u128, Vec<Value>)> {
    values[1]
        .as_array()
        .expect("categories include should be an array")
        .iter()
        .map(|category| {
            (
                category
                    .row_id()
                    .expect("included category keeps its row id")
                    .uuid()
                    .as_u128(),
                category
                    .as_row()
                    .expect("included category should be a row")
                    .to_vec(),
            )
        })
        .collect()
}

fn single_menu(rows: &QueryRows, menu_id: ObjectId) -> &[Value] {
    assert_eq!(rows.len(), 1, "exactly the filtered menu is returned");
    assert_eq!(rows[0].0, menu_id);
    &rows[0].1
}

/// Checks `actual` against the control read: same ids in the same order, and
/// each payload equals the control payload minus the sort columns.
fn assert_matches_control(
    case: &Case,
    surface: &str,
    actual: &[(u128, Vec<Value>)],
    control: &[(u128, Vec<Value>)],
) {
    let ids = actual.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let control_ids = control.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    assert_eq!(
        ids, control_ids,
        "{}: {surface} order matches the selected-sort control",
        case.label
    );
    if let Some(expected) = case.expected {
        assert_eq!(ids, expected, "{}: {surface} order", case.label);
    }
    for ((id, payload), (_, control_payload)) in actual.iter().zip(control) {
        let expected_len = case.select.len() + usize::from(case.nested_images);
        assert_eq!(
            payload.len(),
            expected_len,
            "{}: {surface} category {id} exposes only its selected fields: {payload:?}",
            case.label
        );
        assert_eq!(
            payload[..case.select.len()],
            control_payload[..case.select.len()],
            "{}: {surface} category {id} visible values",
            case.label
        );
        if case.nested_images {
            assert_eq!(
                payload.last(),
                control_payload.last(),
                "{}: {surface} category {id} nested images",
                case.label
            );
        }
    }
}

async fn try_read(client: &JazzClient, query: Query, tier: ReadTier) -> Result<QueryRows, String> {
    match tokio::time::timeout(READ_TIMEOUT, client.query(query, tier)).await {
        Ok(Ok(results)) => Ok(jazz::tools::test_support::ordinary_rows(results)),
        Ok(Err(error)) => Err(format!("read failed: {error}")),
        Err(_) => Err(format!("read did not settle within {READ_TIMEOUT:?}")),
    }
}

async fn read(client: &JazzClient, query: Query, tier: ReadTier, what: &str) -> QueryRows {
    try_read(client, query, tier)
        .await
        .unwrap_or_else(|error| panic!("{what}: {error}"))
}

/// Runs every case on one read surface and reports all failing cases at once.
async fn assert_all_cases(client: &JazzClient, tier: ReadTier, surface: &str, menu_id: ObjectId) {
    let mut failures = Vec::new();
    for case in cases() {
        let what = format!("{} ({surface})", case.label);
        let control = read(client, case.control_query(), tier, &what).await;
        match try_read(client, case.query(), tier).await {
            Ok(rows) => assert_matches_control(
                &case,
                surface,
                &included(single_menu(&rows, menu_id)),
                &included(single_menu(&control, menu_id)),
            ),
            Err(error) => failures.push(format!("{what}: {error}")),
        }
    }
    assert!(
        failures.is_empty(),
        "failing cases:\n{}",
        failures.join("\n")
    );
}

local_tokio_test! {
/// A local one-shot read of an include ordered by an unselected column.
///
/// Actors: the writer owns every row and reads from its own local store.
async fn ordered_include_by_unselected_column_reads_locally() {
    let fixture = Fixture::start().await;
    assert_all_cases(&fixture.writer, ReadTier::LocalFirst, "local read", fixture.menu_id).await;
    fixture.shutdown().await;
}
}

local_tokio_test! {
/// A remote read of an include ordered by an unselected column must be
/// accepted by the server rather than rejected and left hanging.
///
/// Actors: the writer seeds rows; the reader reads them at the remote tier.
async fn ordered_include_by_unselected_column_reads_remotely() {
    let fixture = Fixture::start().await;
    assert_all_cases(&fixture.reader, ReadTier::Remote, "remote read", fixture.menu_id).await;
    fixture.shutdown().await;
}
}

local_tokio_test! {
/// A maintained subscription to an include ordered by an unselected column
/// delivers its initial snapshot and then keeps the hidden order current.
///
/// Actors: the reader subscribes; the writer changes a hidden sort key.
async fn ordered_include_by_unselected_column_subscription_tracks_hidden_key() {
    let fixture = Fixture::start().await;
    let case = cases().remove(0);
    let control = read(&fixture.reader, case.control_query(), ReadTier::Remote, case.label).await;
    let control = included(single_menu(&control, fixture.menu_id));

    let mut stream = fixture
        .reader
        .subscribe(case.query())
        .await
        .expect("subscribe to ordered include");
    let mut current = next_menu_snapshot(&mut stream, "initial snapshot").await;
    assert_matches_control(&case, "subscription", &current, &control);

    // Moving the first category to the end changes only an unselected column.
    fixture
        .writer
        .update(
            "categories",
            ObjectId::from_uuid(uuid::Uuid::from_u128(12)),
            vec![("order_key".to_owned(), Value::Text("z".to_owned()))],
        )
        .expect("move category to the end");
    let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
    while current.iter().map(|(id, _)| *id).collect::<Vec<_>>() != [13, 14, 11, 12] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "subscription never reordered by the hidden key; last: {current:?}"
        );
        current = next_menu_snapshot(&mut stream, "reorder update").await;
    }
    assert!(current.iter().all(|(_, payload)| payload.len() == 1));
    fixture.shutdown().await;
}
}

/// Waits for the next delta that carries the menu row and returns its include.
async fn next_menu_snapshot(
    stream: &mut jazz::tools::SubscriptionStream,
    what: &str,
) -> Vec<(u128, Vec<Value>)> {
    loop {
        let item = tokio::time::timeout(READ_TIMEOUT, stream.next())
            .await
            .unwrap_or_else(|_| panic!("{what}: no subscription delta within {READ_TIMEOUT:?}"))
            .unwrap_or_else(|| panic!("{what}: subscription stream closed"));
        let delta = match item {
            SubscriptionStreamItem::Delta(delta) => delta,
            SubscriptionStreamItem::Rejected { reason } => {
                panic!("{what}: subscription rejected: {reason:?}")
            }
        };
        let row = delta
            .added
            .iter()
            .map(|added| &added.row)
            .chain(
                delta
                    .updated
                    .iter()
                    .filter_map(|updated| updated.row.as_ref()),
            )
            .next();
        if let Some(row) = row {
            let values = row
                .fields
                .iter()
                .map(|field| field.value.clone())
                .collect::<Vec<_>>();
            return included(&values);
        }
    }
}
