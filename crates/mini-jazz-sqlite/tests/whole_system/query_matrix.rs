use super::*;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::BTreeSet;

#[derive(Clone)]
struct MatrixRow {
    id: &'static str,
    title: &'static str,
    category: &'static str,
    body: &'static str,
    tag: Option<&'static str>,
    active: bool,
    project: Option<&'static str>,
    created_by: &'static str,
    created_rank: i64,
    updated_rank: i64,
}

#[derive(Clone)]
struct ConditionCase {
    label: &'static str,
    conditions: Vec<JsonValue>,
}

#[derive(Clone)]
struct OrderCase {
    label: &'static str,
    order_by: Vec<(&'static str, &'static str)>,
}

#[derive(Clone)]
struct WindowCase {
    label: &'static str,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[test]
fn built_query_matrix_matches_expected_rows_for_supported_operations() {
    let mut runtime =
        Runtime::open_trusted_with_schema(Storage::Memory, "matrix-node", query_matrix_schema())
            .unwrap();
    let model_rows = seed_query_matrix_rows(&mut runtime);
    let condition_cases = query_condition_cases();
    let order_cases = query_order_cases();
    let window_cases = query_window_cases();

    for condition_case in &condition_cases {
        for order_case in &order_cases {
            for window_case in &window_cases {
                let query = built_query(
                    "query_items",
                    condition_case.conditions.clone(),
                    &order_case.order_by,
                    window_case.limit,
                    window_case.offset,
                );
                let actual = runtime
                    .query(query)
                    .unwrap_or_else(|err| {
                        panic!(
                            "query failed for conditions={} order={} window={}: {err}",
                            condition_case.label, order_case.label, window_case.label
                        )
                    })
                    .into_iter()
                    .map(|row| row.id)
                    .collect::<Vec<_>>();
                let expected = expected_matrix_ids(
                    &model_rows,
                    &condition_case.conditions,
                    &order_case.order_by,
                    window_case.limit,
                    window_case.offset,
                );

                assert_eq!(
                    actual, expected,
                    "conditions={} order={} window={}",
                    condition_case.label, order_case.label, window_case.label
                );
            }
        }
    }
}

#[test]
fn large_built_query_export_does_not_exceed_sqlite_variable_limit() {
    const ROW_COUNT: usize = 40_000;

    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "large-query-upstream", schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "large-query-peer", "alice", schema).unwrap();

    for index in 0..ROW_COUNT {
        upstream
            .insert_row(
                "tasks",
                &format!("task-{index:05}"),
                BTreeMap::from([
                    ("title".to_owned(), json!(format!("Task {index:05}"))),
                    ("done".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "done", "op": "eq", "value": false}],
        "orderBy": [["$createdAt", "desc"]]
    }))
    .unwrap();

    peer.apply_bundle(&upstream.export_query(query.clone()).unwrap())
        .unwrap();

    assert_eq!(peer.query(query).unwrap().len(), ROW_COUNT);
}

#[test]
fn built_query_offset_page_export_supports_peer_pagination() {
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "page-upstream", schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "page-peer", "alice", schema).unwrap();

    for index in 0..40 {
        upstream
            .insert_row(
                "tasks",
                &format!("task-{index:02}"),
                BTreeMap::from([
                    ("title".to_owned(), json!(format!("Task {index:02}"))),
                    ("done".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "done", "op": "eq", "value": false}],
        "orderBy": [["title", "asc"]],
        "limit": 10,
        "offset": 20,
    }))
    .unwrap();

    peer.apply_bundle(&upstream.export_query(query.clone()).unwrap())
        .unwrap();

    assert_eq!(
        query_ids(&peer, query),
        (20..30)
            .map(|index| format!("task-{index:02}"))
            .collect::<Vec<_>>()
    );
}

#[test]
fn built_query_page_refresh_removes_stale_rows_and_adds_new_boundary_rows() {
    let schema = support::tasks_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "refresh-upstream", schema.clone())
            .unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "refresh-peer", "alice", schema).unwrap();

    for (id, title) in [
        ("task-alpha", "Alpha"),
        ("task-beta", "Beta"),
        ("task-gamma", "Gamma"),
    ] {
        upstream
            .insert_row(
                "tasks",
                id,
                BTreeMap::from([
                    ("title".to_owned(), json!(title)),
                    ("done".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "done", "op": "eq", "value": false}],
        "orderBy": [["title", "asc"]],
        "limit": 2,
    }))
    .unwrap();

    peer.apply_bundle(&upstream.export_query(query.clone()).unwrap())
        .unwrap();
    assert_eq!(
        query_ids(&peer, query.clone()),
        vec!["task-alpha", "task-beta"]
    );
    let mut subscription = peer.subscribe_query(query.clone()).unwrap();

    upstream
        .update_row(
            "tasks",
            "task-gamma",
            BTreeMap::from([("title".to_owned(), json!("Aardvark"))]),
        )
        .unwrap();
    for refresh in upstream
        .export_query_read_refreshes(&peer.observed_query_reads().unwrap())
        .unwrap()
    {
        peer.apply_bundle(&refresh).unwrap();
    }

    let update = peer.subscription_delta(&mut subscription).unwrap();
    assert_eq!(
        update
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-gamma", "task-alpha"]
    );
    assert!(update.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Added { id, .. } if id == "task-gamma")
    ));
    assert!(update.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Removed { id, .. } if id == "task-beta")
    ));
}

#[test]
fn policy_filtered_built_query_subscription_tracks_rows_entering_and_leaving() {
    let mut runtime = Runtime::open_trusted_with_schema(
        Storage::Memory,
        "policy-query-node",
        policy_query_schema(),
    )
    .unwrap();
    seed_policy_query_rows(&mut runtime);
    let query = active_tasks_by_title_query(3);

    let mut subscription = support::run_as_user(&mut runtime, "alice", |runtime| {
        runtime.subscribe_query(query).unwrap()
    });
    assert_eq!(
        subscription_ids(subscription.initial_rows()),
        vec!["task-beta", "task-delta"]
    );

    support::run_attributing_to_user(&mut runtime, "alice", |runtime| {
        runtime
            .update_row(
                "tasks",
                "task-alpha",
                BTreeMap::from([("project".to_owned(), json!("project-alice"))]),
            )
            .unwrap();
    });
    let entered = support::run_as_user(&mut runtime, "alice", |runtime| {
        runtime.subscription_delta(&mut subscription).unwrap()
    });
    assert_eq!(
        entered
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-alpha", "task-beta", "task-delta"]
    );
    assert!(entered.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Added { id, .. } if id == "task-alpha")
    ));

    support::run_attributing_to_user(&mut runtime, "alice", |runtime| {
        runtime
            .update_row(
                "tasks",
                "task-beta",
                BTreeMap::from([("project".to_owned(), json!("project-bob"))]),
            )
            .unwrap();
    });
    let left = support::run_as_user(&mut runtime, "alice", |runtime| {
        runtime.subscription_delta(&mut subscription).unwrap()
    });
    assert_eq!(
        left.all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-alpha", "task-delta"]
    );
    assert!(left.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Removed { id, .. } if id == "task-beta")
    ));
}

#[test]
fn policy_filtered_query_refresh_replaces_ordered_page_boundary_after_incremental_update() {
    let schema = policy_query_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "upstream", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();
    seed_policy_query_rows(&mut upstream);
    let query = active_tasks_by_title_query(2);

    let initial = support::run_as_user(&mut upstream, "alice", |runtime| {
        runtime.export_query(query.clone()).unwrap()
    });
    peer.apply_bundle(&initial).unwrap();
    assert_eq!(
        query_ids(&peer, query.clone()),
        vec!["task-beta", "task-delta"]
    );

    let mut subscription = peer.subscribe_query(query.clone()).unwrap();
    support::run_attributing_to_user(&mut upstream, "alice", |runtime| {
        runtime
            .update_row(
                "tasks",
                "task-alpha",
                BTreeMap::from([("project".to_owned(), json!("project-alice"))]),
            )
            .unwrap();
    });

    let observed = peer.observed_query_reads().unwrap();
    let refreshes = support::run_as_user(&mut upstream, "alice", |runtime| {
        runtime.export_query_read_refreshes(&observed).unwrap()
    });
    for refresh in refreshes {
        peer.apply_bundle(&refresh).unwrap();
    }

    let update = peer.subscription_delta(&mut subscription).unwrap();
    assert_eq!(
        update
            .all
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["task-alpha", "task-beta"]
    );
    assert!(update.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Added { id, .. } if id == "task-alpha")
    ));
    assert!(update.delta.iter().any(
        |delta| matches!(delta, SubscriptionRowDelta::Removed { id, .. } if id == "task-delta")
    ));
}

#[test]
fn policy_filtered_created_at_page_refresh_keeps_visible_rows_ahead_of_hidden_storage() {
    let schema = policy_query_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "upstream", schema.clone()).unwrap();
    let mut peer = Runtime::open_with_schema(Storage::Memory, "peer", "alice", schema).unwrap();

    support::run_attributing_to_user(&mut upstream, "alice", |runtime| {
        runtime
            .insert_row(
                "projects",
                "project-alice",
                BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "tasks",
                "task-visible-old",
                BTreeMap::from([
                    ("title".to_owned(), json!("Visible old")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        runtime
            .insert_row(
                "tasks",
                "task-visible-new",
                BTreeMap::from([
                    ("title".to_owned(), json!("Visible new")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
    });
    support::run_attributing_to_user(&mut upstream, "bob", |runtime| {
        runtime
            .insert_row(
                "projects",
                "project-bob",
                BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        runtime
            .insert_row(
                "tasks",
                "task-hidden-newer",
                BTreeMap::from([
                    ("title".to_owned(), json!("Hidden newer")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-bob")),
                ]),
            )
            .unwrap();
    });

    peer.apply_bundle(&upstream.export_table_history("projects").unwrap())
        .unwrap();
    peer.apply_bundle(&upstream.export_table_history("tasks").unwrap())
        .unwrap();
    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "active", "op": "eq", "value": true}],
        "orderBy": [["$createdAt", "desc"]],
        "limit": 2,
    }))
    .unwrap();
    let initial = support::run_as_user(&mut upstream, "alice", |runtime| {
        runtime.export_query(query.clone()).unwrap()
    });
    peer.apply_bundle(&initial).unwrap();
    assert_eq!(
        query_ids(&peer, query.clone()),
        vec!["task-visible-new", "task-visible-old"]
    );

    support::run_attributing_to_user(&mut upstream, "alice", |runtime| {
        std::thread::sleep(std::time::Duration::from_millis(2));
        runtime
            .insert_row(
                "tasks",
                "task-visible-newest",
                BTreeMap::from([
                    ("title".to_owned(), json!("Visible newest")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
    });
    let observed = peer.observed_query_reads().unwrap();
    let refreshes = support::run_as_user(&mut upstream, "alice", |runtime| {
        runtime.export_query_read_refreshes(&observed).unwrap()
    });
    for refresh in refreshes {
        peer.apply_bundle(&refresh).unwrap();
    }

    assert_eq!(
        query_ids(&peer, query),
        vec!["task-visible-newest", "task-visible-new"]
    );
}

#[test]
fn built_query_page_export_does_not_collect_every_matching_row_as_repair_data() {
    let schema = SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    });
    let mut upstream =
        Runtime::open_with_schema(Storage::Memory, "upstream", "alice", schema).unwrap();

    for (id, title) in [
        ("task-alpha", "Alpha"),
        ("task-beta", "Beta"),
        ("task-gamma", "Gamma"),
    ] {
        upstream
            .insert_row(
                "tasks",
                id,
                BTreeMap::from([
                    ("title".to_owned(), json!(title)),
                    ("done".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
    }

    let query = BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "done", "op": "eq", "value": false}],
        "orderBy": [["title", "asc"]],
        "limit": 2,
    }))
    .unwrap();
    let task_ids = upstream
        .export_query(query)
        .unwrap()
        .history
        .into_iter()
        .filter(|record| record.table == "tasks")
        .map(|record| record.row_id)
        .collect::<Vec<_>>();

    assert_eq!(task_ids, vec!["task-alpha", "task-beta"]);
}

#[test]
fn policy_filtered_built_query_export_does_not_include_hidden_repair_rows() {
    let schema = policy_query_schema();
    let mut upstream =
        Runtime::open_trusted_with_schema(Storage::Memory, "upstream", schema).unwrap();
    seed_policy_query_rows(&mut upstream);

    let bundle = support::run_as_user(&mut upstream, "alice", |runtime| {
        runtime
            .export_query(active_tasks_by_title_query(2))
            .unwrap()
    });
    let task_ids = bundle
        .history
        .iter()
        .filter(|record| record.table == "tasks")
        .map(|record| record.row_id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(task_ids, BTreeSet::from(["task-beta", "task-delta"]));
}

#[test]
fn built_query_scope_export_refreshes_system_timestamp_and_system_text_conditions() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();

    alice
        .insert_row(
            "notes",
            "note-visible",
            BTreeMap::from([
                ("body".to_owned(), json!("system query")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice
        .update_row(
            "notes",
            "note-visible",
            BTreeMap::from([("body".to_owned(), json!("system query updated"))]),
        )
        .unwrap();

    for query in [
        built_query(
            "notes",
            vec![condition("$createdAt", "ne", json!(0))],
            &[],
            None,
            None,
        ),
        built_query(
            "notes",
            vec![condition("$updatedAt", "ne", json!(0))],
            &[],
            None,
            None,
        ),
        built_query(
            "notes",
            vec![condition("id", "contains", json!("note-"))],
            &[],
            None,
            None,
        ),
        built_query(
            "notes",
            vec![condition("$createdBy", "contains", json!("ali"))],
            &[],
            None,
            None,
        ),
    ] {
        let mut peer =
            Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema.clone())
                .unwrap();

        peer.apply_bundle(&alice.export_query(query.clone()).unwrap())
            .unwrap();
        assert_eq!(query_ids(&peer, query), vec!["note-visible"]);
    }
}

fn query_matrix_schema() -> SchemaDef {
    SchemaDef::new()
        .table("projects", |table| {
            table.text("name");
        })
        .table("query_items", |table| {
            table.text("title");
            table.text("category");
            table.text("body");
            table.optional_text("tag");
            table.bool("active");
            table.optional_ref("project", "projects");
            table.index("active_title", ["active", "title"]);
            table.index("category_title", ["category", "title"]);
        })
}

fn seed_query_matrix_rows(runtime: &mut Runtime) -> Vec<MatrixRow> {
    runtime
        .insert_row(
            "projects",
            "project-a",
            BTreeMap::from([("name".to_owned(), json!("Alpha project"))]),
        )
        .unwrap();
    runtime
        .insert_row(
            "projects",
            "project-b",
            BTreeMap::from([("name".to_owned(), json!("Beta project"))]),
        )
        .unwrap();

    let mut rows = vec![
        MatrixRow {
            id: "item-1",
            title: "Alpha",
            category: "work",
            body: "ship alpha",
            tag: Some("red"),
            active: true,
            project: Some("project-a"),
            created_by: "alice",
            created_rank: 1,
            updated_rank: 1,
        },
        MatrixRow {
            id: "item-2",
            title: "Bravo",
            category: "work",
            body: "ship beta",
            tag: Some("blue"),
            active: true,
            project: Some("project-a"),
            created_by: "bob",
            created_rank: 2,
            updated_rank: 2,
        },
        MatrixRow {
            id: "item-3",
            title: "Charlie",
            category: "home",
            body: "cook dinner",
            tag: None,
            active: false,
            project: None,
            created_by: "alice",
            created_rank: 3,
            updated_rank: 3,
        },
        MatrixRow {
            id: "item-4",
            title: "Delta",
            category: "home",
            body: "ship delta",
            tag: Some("red"),
            active: true,
            project: Some("project-b"),
            created_by: "carol",
            created_rank: 4,
            updated_rank: 4,
        },
        MatrixRow {
            id: "item-5",
            title: "Echo",
            category: "ops",
            body: "review docs",
            tag: None,
            active: false,
            project: Some("project-b"),
            created_by: "bob",
            created_rank: 5,
            updated_rank: 5,
        },
        MatrixRow {
            id: "item-6",
            title: "Foxtrot",
            category: "ops",
            body: "ship foxtrot",
            tag: Some("green"),
            active: true,
            project: None,
            created_by: "alice",
            created_rank: 6,
            updated_rank: 6,
        },
        MatrixRow {
            id: "item-7",
            title: "Golf",
            category: "work",
            body: "plan launch",
            tag: Some("blue"),
            active: false,
            project: Some("project-a"),
            created_by: "carol",
            created_rank: 7,
            updated_rank: 7,
        },
        MatrixRow {
            id: "item-8",
            title: "Hotel",
            category: "home",
            body: "ship hotel",
            tag: Some("yellow"),
            active: true,
            project: Some("project-b"),
            created_by: "bob",
            created_rank: 8,
            updated_rank: 8,
        },
    ];

    for row in &rows {
        support::run_attributing_to_user(runtime, row.created_by, |runtime| {
            runtime
                .insert_row(
                    "query_items",
                    row.id,
                    BTreeMap::from([
                        ("title".to_owned(), json!(row.title)),
                        ("category".to_owned(), json!(row.category)),
                        ("body".to_owned(), json!(row.body)),
                        ("tag".to_owned(), option_json(row.tag)),
                        ("active".to_owned(), json!(row.active)),
                        ("project".to_owned(), option_json(row.project)),
                    ]),
                )
                .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    support::run_attributing_to_user(runtime, "bob", |runtime| {
        runtime
            .update_row(
                "query_items",
                "item-2",
                BTreeMap::from([("body".to_owned(), json!("ship beta updated"))]),
            )
            .unwrap();
    });
    rows[1].body = "ship beta updated";
    rows[1].updated_rank = 9;
    std::thread::sleep(std::time::Duration::from_millis(2));

    support::run_attributing_to_user(runtime, "alice", |runtime| {
        runtime
            .update_row(
                "query_items",
                "item-6",
                BTreeMap::from([("body".to_owned(), json!("ship foxtrot updated"))]),
            )
            .unwrap();
    });
    rows[5].body = "ship foxtrot updated";
    rows[5].updated_rank = 10;

    rows
}

fn query_condition_cases() -> Vec<ConditionCase> {
    vec![
        condition_case("all rows", vec![]),
        condition_case(
            "active eq true",
            vec![condition("active", "eq", json!(true))],
        ),
        condition_case(
            "active ne false",
            vec![condition("active", "ne", json!(false))],
        ),
        condition_case(
            "category in work/home",
            vec![condition("category", "in", json!(["work", "home"]))],
        ),
        condition_case(
            "body contains ship",
            vec![condition("body", "contains", json!("ship"))],
        ),
        condition_case("tag eq null", vec![condition("tag", "eq", JsonValue::Null)]),
        condition_case("tag ne null", vec![condition("tag", "ne", JsonValue::Null)]),
        condition_case(
            "tag in red/null",
            vec![condition("tag", "in", json!(["red", null]))],
        ),
        condition_case(
            "project eq project-a",
            vec![condition("project", "eq", json!("project-a"))],
        ),
        condition_case(
            "project in project-a/null",
            vec![condition("project", "in", json!(["project-a", null]))],
        ),
        condition_case(
            "id in selected",
            vec![condition(
                "id",
                "in",
                json!(["item-2", "item-5", "missing"]),
            )],
        ),
        condition_case(
            "id contains item",
            vec![condition("id", "contains", json!("item-"))],
        ),
        condition_case(
            "createdBy eq alice",
            vec![condition("$createdBy", "eq", json!("alice"))],
        ),
        condition_case(
            "createdBy ne bob",
            vec![condition("$createdBy", "ne", json!("bob"))],
        ),
        condition_case(
            "createdBy in alice/carol",
            vec![condition("$createdBy", "in", json!(["alice", "carol"]))],
        ),
        condition_case(
            "title eq Bravo",
            vec![condition("title", "eq", json!("Bravo"))],
        ),
        condition_case(
            "title ne Delta",
            vec![condition("title", "ne", json!("Delta"))],
        ),
        condition_case(
            "createdAt ne zero",
            vec![condition("$createdAt", "ne", json!(0))],
        ),
        condition_case(
            "updatedAt ne zero",
            vec![condition("$updatedAt", "ne", json!(0))],
        ),
        condition_case(
            "eq in contains",
            vec![
                condition("active", "eq", json!(true)),
                condition("category", "in", json!(["work", "ops"])),
                condition("body", "contains", json!("ship")),
            ],
        ),
        condition_case(
            "ne ne in",
            vec![
                condition("tag", "ne", JsonValue::Null),
                condition("project", "ne", JsonValue::Null),
                condition("active", "in", json!([true])),
            ],
        ),
        condition_case(
            "system text and field ops",
            vec![
                condition("active", "eq", json!(true)),
                condition("$createdBy", "ne", json!("bob")),
                condition("id", "contains", json!("item")),
            ],
        ),
    ]
}

fn query_order_cases() -> Vec<OrderCase> {
    vec![
        order_case("default", vec![]),
        order_case("title asc", vec![("title", "asc")]),
        order_case("title desc", vec![("title", "desc")]),
        order_case(
            "active asc title asc",
            vec![("active", "asc"), ("title", "asc")],
        ),
        order_case("tag asc id asc", vec![("tag", "asc"), ("id", "asc")]),
        order_case(
            "project desc title asc",
            vec![("project", "desc"), ("title", "asc")],
        ),
        order_case("id asc", vec![("id", "asc")]),
        order_case(
            "createdBy asc title asc",
            vec![("$createdBy", "asc"), ("title", "asc")],
        ),
        order_case("createdAt asc", vec![("$createdAt", "asc")]),
        order_case("createdAt desc", vec![("$createdAt", "desc")]),
        order_case(
            "updatedAt desc id asc",
            vec![("$updatedAt", "desc"), ("id", "asc")],
        ),
        order_case(
            "category asc title desc",
            vec![("category", "asc"), ("title", "desc")],
        ),
    ]
}

fn query_window_cases() -> Vec<WindowCase> {
    vec![
        window_case("all", None, None),
        window_case("limit 1", Some(1), None),
        window_case("limit 3", Some(3), None),
        window_case("offset 1", None, Some(1)),
        window_case("limit 2 offset 1", Some(2), Some(1)),
        window_case("limit 5 offset 2", Some(5), Some(2)),
    ]
}

fn condition_case(label: &'static str, conditions: Vec<JsonValue>) -> ConditionCase {
    ConditionCase { label, conditions }
}

fn order_case(label: &'static str, order_by: Vec<(&'static str, &'static str)>) -> OrderCase {
    OrderCase { label, order_by }
}

fn window_case(label: &'static str, limit: Option<usize>, offset: Option<usize>) -> WindowCase {
    WindowCase {
        label,
        limit,
        offset,
    }
}

fn condition(column: &'static str, op: &'static str, value: JsonValue) -> JsonValue {
    json!({"column": column, "op": op, "value": value})
}

fn built_query(
    table: &str,
    conditions: Vec<JsonValue>,
    order_by: &[(&str, &str)],
    limit: Option<usize>,
    offset: Option<usize>,
) -> BuiltQuery {
    let mut query = json!({
        "table": table,
        "conditions": conditions,
        "orderBy": order_by
            .iter()
            .map(|(column, direction)| json!([column, direction]))
            .collect::<Vec<_>>(),
    });
    if let Some(limit) = limit {
        query["limit"] = json!(limit);
    }
    if let Some(offset) = offset {
        query["offset"] = json!(offset);
    }
    BuiltQuery::from_json_value(query).unwrap()
}

fn expected_matrix_ids(
    rows: &[MatrixRow],
    conditions: &[JsonValue],
    order_by: &[(&str, &str)],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<String> {
    let mut rows = rows
        .iter()
        .filter(|row| {
            conditions
                .iter()
                .all(|condition| row_matches(row, condition))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| compare_matrix_rows(left, right, order_by));
    rows.into_iter()
        .skip(offset.unwrap_or(0))
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| row.id.to_owned())
        .collect()
}

fn row_matches(row: &MatrixRow, condition: &JsonValue) -> bool {
    let column = condition["column"].as_str().unwrap();
    let op = condition["op"].as_str().unwrap();
    let expected = &condition["value"];
    let actual = matrix_value(row, column);
    match op {
        "eq" => actual == *expected,
        "ne" => actual != *expected,
        "in" => expected.as_array().unwrap().contains(&actual),
        "contains" => actual
            .as_str()
            .unwrap()
            .contains(expected.as_str().unwrap()),
        _ => unreachable!("unsupported test op {op}"),
    }
}

fn compare_matrix_rows(left: &MatrixRow, right: &MatrixRow, order_by: &[(&str, &str)]) -> Ordering {
    if order_by.is_empty() {
        return right
            .created_rank
            .cmp(&left.created_rank)
            .then_with(|| left.created_rank.cmp(&right.created_rank));
    }
    for (column, direction) in order_by {
        let ordering = sqlite_value_cmp(&matrix_value(left, column), &matrix_value(right, column));
        let ordering = match *direction {
            "asc" => ordering,
            "desc" => ordering.reverse(),
            _ => unreachable!("unsupported test order direction {direction}"),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.created_rank.cmp(&right.created_rank)
}

fn sqlite_value_cmp(left: &JsonValue, right: &JsonValue) -> Ordering {
    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            left.as_i64().unwrap().cmp(&right.as_i64().unwrap())
        }
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        _ => panic!("mixed test value types: {left:?} {right:?}"),
    }
}

fn matrix_value(row: &MatrixRow, column: &str) -> JsonValue {
    match column {
        "id" => json!(row.id),
        "$createdBy" => json!(row.created_by),
        "$createdAt" => json!(row.created_rank),
        "$updatedAt" => json!(row.updated_rank),
        "title" => json!(row.title),
        "category" => json!(row.category),
        "body" => json!(row.body),
        "tag" => option_json(row.tag),
        "active" => json!(row.active),
        "project" => option_json(row.project),
        _ => unreachable!("unknown test column {column}"),
    }
}

fn option_json(value: Option<&str>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| json!(value))
}

fn policy_query_schema() -> SchemaDef {
    SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("tasks", |table| {
            table.text("title");
            table.bool("active");
            table.ref_("project", "projects");
            table.read_if_ref_readable("project");
            table.index("active_title", ["active", "title"]);
        })
}

fn seed_policy_query_rows(runtime: &mut Runtime) {
    support::run_attributing_to_user(runtime, "alice", |runtime| {
        runtime
            .insert_row(
                "projects",
                "project-alice",
                BTreeMap::from([("title".to_owned(), json!("Alice project"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "tasks",
                "task-beta",
                BTreeMap::from([
                    ("title".to_owned(), json!("Beta")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
        runtime
            .insert_row(
                "tasks",
                "task-delta",
                BTreeMap::from([
                    ("title".to_owned(), json!("Delta")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
        runtime
            .insert_row(
                "tasks",
                "task-closed",
                BTreeMap::from([
                    ("title".to_owned(), json!("Closed")),
                    ("active".to_owned(), json!(false)),
                    ("project".to_owned(), json!("project-alice")),
                ]),
            )
            .unwrap();
    });
    support::run_attributing_to_user(runtime, "bob", |runtime| {
        runtime
            .insert_row(
                "projects",
                "project-bob",
                BTreeMap::from([("title".to_owned(), json!("Bob project"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "tasks",
                "task-alpha",
                BTreeMap::from([
                    ("title".to_owned(), json!("Alpha")),
                    ("active".to_owned(), json!(true)),
                    ("project".to_owned(), json!("project-bob")),
                ]),
            )
            .unwrap();
    });
}

fn active_tasks_by_title_query(limit: usize) -> BuiltQuery {
    BuiltQuery::from_json_value(json!({
        "table": "tasks",
        "conditions": [{"column": "active", "op": "eq", "value": true}],
        "orderBy": [["title", "asc"]],
        "limit": limit,
    }))
    .unwrap()
}

fn subscription_ids(rows: &[mini_jazz_sqlite::RowView]) -> Vec<&str> {
    rows.iter().map(|row| row.id.as_str()).collect()
}

fn query_ids(runtime: &Runtime, query: BuiltQuery) -> Vec<String> {
    runtime
        .query(query)
        .unwrap()
        .into_iter()
        .map(|row| row.id)
        .collect()
}
