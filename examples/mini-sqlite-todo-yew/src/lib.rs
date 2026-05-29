#[cfg(target_arch = "wasm32")]
pub mod browser_runtime;
pub mod browser_worker;
pub mod query_builder;
pub mod todo_query {
    use crate::query_builder::QueryBuilder;
    use mini_jazz_sqlite::{BuiltQuery, QueryDirection};
    use serde_json::json;

    pub const TODO_PAGE_SIZE: usize = 10;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum TodoDoneFilter {
        #[default]
        All,
        Open,
        Done,
    }

    impl TodoDoneFilter {
        pub fn from_value(value: &str) -> Self {
            match value {
                "open" => Self::Open,
                "done" => Self::Done,
                _ => Self::All,
            }
        }

        pub fn value(self) -> &'static str {
            match self {
                Self::All => "all",
                Self::Open => "open",
                Self::Done => "done",
            }
        }
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum TodoSortField {
        #[default]
        Date,
        Title,
    }

    impl TodoSortField {
        pub fn from_value(value: &str) -> Self {
            match value {
                "title" => Self::Title,
                _ => Self::Date,
            }
        }

        pub fn value(self) -> &'static str {
            match self {
                Self::Date => "date",
                Self::Title => "title",
            }
        }

        pub fn column(self) -> &'static str {
            match self {
                Self::Date => "$createdAt",
                Self::Title => "title",
            }
        }
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum TodoSortDirection {
        Asc,
        #[default]
        Desc,
    }

    impl TodoSortDirection {
        pub fn from_value(value: &str) -> Self {
            match value {
                "asc" => Self::Asc,
                _ => Self::Desc,
            }
        }

        pub fn value(self) -> &'static str {
            match self {
                Self::Asc => "asc",
                Self::Desc => "desc",
            }
        }

        pub fn query_direction(self) -> QueryDirection {
            match self {
                Self::Asc => QueryDirection::Asc,
                Self::Desc => QueryDirection::Desc,
            }
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct TodoQueryState {
        pub title_search: String,
        pub done_filter: TodoDoneFilter,
        pub sort_field: TodoSortField,
        pub sort_direction: TodoSortDirection,
        pub page: usize,
    }

    impl TodoQueryState {
        pub fn page_query(&self) -> BuiltQuery {
            self.query_with_window(TODO_PAGE_SIZE, TODO_PAGE_SIZE * self.page)
        }

        pub fn next_page_probe_query(&self) -> BuiltQuery {
            self.query_with_window(1, TODO_PAGE_SIZE * (self.page + 1))
        }

        pub fn page_hydration_query(&self) -> BuiltQuery {
            self.query_with_window(TODO_PAGE_SIZE + 1, TODO_PAGE_SIZE * self.page)
        }

        fn query_with_window(&self, limit: usize, offset: usize) -> BuiltQuery {
            let mut builder = QueryBuilder::table("todos");
            let title_search = self.title_search.trim();
            if !title_search.is_empty() {
                builder = builder.contains("title", title_search);
            }
            builder = match self.done_filter {
                TodoDoneFilter::All => builder,
                TodoDoneFilter::Open => builder.eq("done", json!(false)),
                TodoDoneFilter::Done => builder.eq("done", json!(true)),
            };
            builder
                .order_by(
                    self.sort_field.column(),
                    self.sort_direction.query_direction(),
                )
                .limit(limit)
                .offset(offset)
                .build()
        }
    }
}
pub mod todo_schema;
pub mod todo_display {
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct TodoDisplayState {
        pub ready: bool,
        pub generating: bool,
        pub syncing: bool,
        pub error: String,
        pub generated: u64,
        pub total_to_generate: u64,
    }

    pub fn controls_locked(state: &TodoDisplayState) -> bool {
        !state.ready || state.generating
    }

    pub fn status_text(state: &TodoDisplayState) -> String {
        if !state.error.is_empty() {
            "Error".to_owned()
        } else if state.generating {
            format!(
                "Generating {} / {} in main memory...",
                format_count(state.generated),
                format_count(state.total_to_generate)
            )
        } else if state.ready {
            "Main memory runtime synced with OPFS worker".to_owned()
        } else {
            "Opening runtimes...".to_owned()
        }
    }

    fn format_count(value: u64) -> String {
        let text = value.to_string();
        let mut out = String::new();
        for (index, ch) in text.chars().rev().enumerate() {
            if index > 0 && index % 3 == 0 {
                out.push(',');
            }
            out.push(ch);
        }
        out.chars().rev().collect()
    }
}
pub mod worker_bridge;

#[cfg(test)]
mod tests {
    use crate::query_builder::QueryBuilder;
    use crate::todo_display::{controls_locked, status_text, TodoDisplayState};
    use crate::todo_query::{
        TodoDoneFilter, TodoQueryState, TodoSortDirection, TodoSortField, TODO_PAGE_SIZE,
    };
    use crate::todo_schema::todo_schema;
    use mini_jazz_sqlite::{BuiltQuery, QueryConditionOp, QueryDirection, Runtime, Storage};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn syncing_changes_do_not_show_a_loading_state() {
        let state = TodoDisplayState {
            ready: true,
            syncing: true,
            ..TodoDisplayState::default()
        };

        assert_eq!(
            status_text(&state),
            "Main memory runtime synced with OPFS worker"
        );
        assert!(!controls_locked(&state));
    }

    #[test]
    fn todo_schema_lives_in_the_example_app() {
        let mut runtime =
            Runtime::open_with_schema(Storage::Memory, "todo-example", "alice", todo_schema())
                .unwrap();
        runtime
            .insert_row(
                "projects",
                "todo-list",
                BTreeMap::from([("title".to_owned(), json!("Todo list"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "todos",
                "todo-1",
                BTreeMap::from([
                    ("title".to_owned(), json!("Use app schema")),
                    ("done".to_owned(), json!(false)),
                    ("project".to_owned(), json!("todo-list")),
                ]),
            )
            .unwrap();

        let rows = runtime
            .query(BuiltQuery {
                table: "todos".to_owned(),
                conditions: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "todo-1");
    }

    #[test]
    fn queried_todos_expose_created_at_for_display() {
        let mut runtime =
            Runtime::open_with_schema(Storage::Memory, "todo-created-at", "alice", todo_schema())
                .unwrap();
        runtime
            .insert_row(
                "projects",
                "todo-list",
                BTreeMap::from([("title".to_owned(), json!("Todo list"))]),
            )
            .unwrap();
        runtime
            .insert_row(
                "todos",
                "todo-1",
                BTreeMap::from([
                    ("title".to_owned(), json!("Show date")),
                    ("done".to_owned(), json!(false)),
                    ("project".to_owned(), json!("todo-list")),
                ]),
            )
            .unwrap();

        let rows = runtime
            .query(TodoQueryState::default().page_query())
            .unwrap();

        assert!(rows[0].created_at > 0);
    }

    #[test]
    fn page_hydration_keeps_next_page_probe_available() {
        let mut worker =
            Runtime::open_with_schema(Storage::Memory, "worker-pages", "alice", todo_schema())
                .unwrap();
        let mut main =
            Runtime::open_with_schema(Storage::Memory, "main-pages", "alice", todo_schema())
                .unwrap();

        for index in 0..30 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Todo {index:02}"))),
                        ("done".to_owned(), json!(false)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }

        let page_two = TodoQueryState {
            sort_field: TodoSortField::Title,
            sort_direction: TodoSortDirection::Asc,
            page: 1,
            ..TodoQueryState::default()
        };
        main.apply_bundle(&worker.export_query(page_two.page_query()).unwrap())
            .unwrap();

        let page_one = TodoQueryState {
            sort_field: TodoSortField::Title,
            sort_direction: TodoSortDirection::Asc,
            page: 0,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(page_one.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            main.query(page_one.page_query()).unwrap().len(),
            TODO_PAGE_SIZE
        );
        assert_eq!(
            main.query(page_one.next_page_probe_query()).unwrap().len(),
            1
        );
    }

    #[test]
    fn page_hydration_filters_before_limiting() {
        let mut worker = Runtime::open_with_schema(
            Storage::Memory,
            "worker-filter-pages",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let mut main =
            Runtime::open_with_schema(Storage::Memory, "main-filter-pages", "alice", todo_schema())
                .unwrap();

        for index in 0..30 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Todo {index:02}"))),
                        ("done".to_owned(), json!(index < 3)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }

        let open_page = TodoQueryState {
            done_filter: TodoDoneFilter::Open,
            sort_field: TodoSortField::Title,
            sort_direction: TodoSortDirection::Asc,
            page: 0,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(open_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let rows = main.query(open_page.page_query()).unwrap();

        assert_eq!(rows.len(), TODO_PAGE_SIZE);
        assert_eq!(rows[0].id, "todo-03");
        assert_eq!(rows[9].id, "todo-12");
        assert_eq!(
            main.query(open_page.next_page_probe_query()).unwrap().len(),
            1
        );
    }

    #[test]
    fn created_at_page_hydration_filters_before_limiting() {
        let mut worker = Runtime::open_with_schema(
            Storage::Memory,
            "worker-created-at-filter-pages",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let mut main = Runtime::open_with_schema(
            Storage::Memory,
            "main-created-at-filter-pages",
            "alice",
            todo_schema(),
        )
        .unwrap();

        for index in 0..27 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-open-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Open {index:02}"))),
                        ("done".to_owned(), json!(false)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
        for index in 0..3 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-done-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Done {index:02}"))),
                        ("done".to_owned(), json!(true)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }

        let open_page = TodoQueryState {
            done_filter: TodoDoneFilter::Open,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(open_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let rows = main.query(open_page.page_query()).unwrap();

        assert_eq!(rows.len(), TODO_PAGE_SIZE);
        assert!(rows.iter().all(|row| {
            row.values.get("done").and_then(serde_json::Value::as_bool) == Some(false)
        }));
        assert_eq!(
            main.query(open_page.next_page_probe_query()).unwrap().len(),
            1
        );
    }

    #[test]
    fn page_hydration_refills_after_filter_membership_changes() {
        let mut worker = Runtime::open_with_schema(
            Storage::Memory,
            "worker-filter-refill",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let mut main = Runtime::open_with_schema(
            Storage::Memory,
            "main-filter-refill",
            "alice",
            todo_schema(),
        )
        .unwrap();

        for index in 0..30 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Todo {index:02}"))),
                        ("done".to_owned(), json!(false)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }

        let all_page = TodoQueryState::default();
        main.apply_bundle(
            &worker
                .export_query(all_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let visible_ids = main
            .query(all_page.page_query())
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(visible_ids.len(), TODO_PAGE_SIZE);

        for id in &visible_ids {
            worker
                .update_row(
                    "todos",
                    id,
                    BTreeMap::from([("done".to_owned(), json!(true))]),
                )
                .unwrap();
            main.update_row(
                "todos",
                id,
                BTreeMap::from([("done".to_owned(), json!(true))]),
            )
            .unwrap();
        }

        let open_page = TodoQueryState {
            done_filter: TodoDoneFilter::Open,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(open_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let rows = main.query(open_page.page_query()).unwrap();

        assert_eq!(rows.len(), TODO_PAGE_SIZE);
        assert!(rows.iter().all(|row| {
            row.values.get("done").and_then(serde_json::Value::as_bool) == Some(false)
        }));
        assert_eq!(
            main.query(open_page.next_page_probe_query()).unwrap().len(),
            1
        );
    }

    #[test]
    fn page_hydration_refills_after_synced_filter_membership_changes() {
        let mut worker = Runtime::open_with_schema(
            Storage::Memory,
            "worker-synced-filter-refill",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let mut main = Runtime::open_with_schema(
            Storage::Memory,
            "main-synced-filter-refill",
            "alice",
            todo_schema(),
        )
        .unwrap();

        for index in 0..30 {
            worker
                .insert_row(
                    "todos",
                    &format!("todo-{index:02}"),
                    BTreeMap::from([
                        ("title".to_owned(), json!(format!("Todo {index:02}"))),
                        ("done".to_owned(), json!(false)),
                        ("project".to_owned(), json!("todo-list")),
                    ]),
                )
                .unwrap();
        }

        let all_page = TodoQueryState::default();
        main.apply_bundle(
            &worker
                .export_query(all_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let visible_ids = main
            .query(all_page.page_query())
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(visible_ids.len(), TODO_PAGE_SIZE);

        for id in &visible_ids {
            main.update_row(
                "todos",
                id,
                BTreeMap::from([("done".to_owned(), json!(true))]),
            )
            .unwrap();
        }
        let changed_ids = QueryBuilder::table("todos")
            .in_values("id", json!(visible_ids))
            .build();
        worker
            .apply_bundle(&main.export_query(changed_ids).unwrap())
            .unwrap();

        let open_page = TodoQueryState {
            done_filter: TodoDoneFilter::Open,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(open_page.page_hydration_query())
                .unwrap(),
        )
        .unwrap();

        let rows = main.query(open_page.page_query()).unwrap();

        assert_eq!(rows.len(), TODO_PAGE_SIZE);
        assert!(rows.iter().all(|row| {
            row.values.get("done").and_then(serde_json::Value::as_bool) == Some(false)
        }));
    }

    #[test]
    fn done_filter_survives_page_reload_and_filter_flips() {
        let mut worker = Runtime::open_with_schema(
            Storage::Memory,
            "worker-filter-reload-flips",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let mut main = Runtime::open_with_schema(
            Storage::Memory,
            "main-filter-reload-flips",
            "alice",
            todo_schema(),
        )
        .unwrap();

        let mut pending_ids = Vec::new();
        for index in 0..5_000 {
            let id = format!("todo-{index:04}");
            main.insert_row(
                "todos",
                &id,
                BTreeMap::from([
                    ("title".to_owned(), json!(format!("Todo {index:04}"))),
                    ("done".to_owned(), json!(false)),
                    ("project".to_owned(), json!("todo-list")),
                ]),
            )
            .unwrap();
            pending_ids.push(id);
            if pending_ids.len() == 100 {
                let changed_ids = QueryBuilder::table("todos")
                    .in_values("id", json!(std::mem::take(&mut pending_ids)))
                    .build();
                worker
                    .apply_bundle(&main.export_query(changed_ids).unwrap())
                    .unwrap();
            }
        }
        if !pending_ids.is_empty() {
            let changed_ids = QueryBuilder::table("todos")
                .in_values("id", json!(pending_ids))
                .build();
            worker
                .apply_bundle(&main.export_query(changed_ids).unwrap())
                .unwrap();
        }

        let page_five = TodoQueryState {
            page: 4,
            ..TodoQueryState::default()
        };
        main.apply_bundle(
            &worker
                .export_query(page_five.page_hydration_query())
                .unwrap(),
        )
        .unwrap();
        let page_five_ids = main
            .query(page_five.page_query())
            .unwrap()
            .into_iter()
            .take(3)
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(page_five_ids.len(), 3);

        for id in &page_five_ids {
            main.update_row(
                "todos",
                id,
                BTreeMap::from([("done".to_owned(), json!(true))]),
            )
            .unwrap();
            let changed_id = QueryBuilder::table("todos")
                .in_values("id", json!([id]))
                .build();
            worker
                .apply_bundle(&main.export_query(changed_id).unwrap())
                .unwrap();
            for query in [page_five.page_query(), page_five.next_page_probe_query()] {
                main.apply_bundle(&worker.export_query(query).unwrap())
                    .unwrap();
            }
        }

        let mut reloaded = Runtime::open_with_schema(
            Storage::Memory,
            "reloaded-main-filter-flips",
            "alice",
            todo_schema(),
        )
        .unwrap();
        let default_page = TodoQueryState::default();
        reloaded
            .apply_bundle(
                &worker
                    .export_query(default_page.page_hydration_query())
                    .unwrap(),
            )
            .unwrap();

        let done_page = TodoQueryState {
            done_filter: TodoDoneFilter::Done,
            ..TodoQueryState::default()
        };
        let mut done_subscription = reloaded.subscribe_query(done_page.page_query()).unwrap();
        assert!(done_subscription.initial_rows().is_empty());
        reloaded
            .apply_bundle(
                &worker
                    .export_query(done_page.page_hydration_query())
                    .unwrap(),
            )
            .unwrap();
        let done_delta = reloaded.subscription_delta(&mut done_subscription).unwrap();
        assert_eq!(done_delta.all.len(), 3);

        let open_page = TodoQueryState {
            done_filter: TodoDoneFilter::Open,
            ..TodoQueryState::default()
        };
        let mut open_subscription = reloaded.subscribe_query(open_page.page_query()).unwrap();
        reloaded
            .apply_bundle(
                &worker
                    .export_query(open_page.page_hydration_query())
                    .unwrap(),
            )
            .unwrap();
        let open_delta = reloaded.subscription_delta(&mut open_subscription).unwrap();
        assert_eq!(open_delta.all.len(), TODO_PAGE_SIZE);

        let mut done_subscription = reloaded.subscribe_query(done_page.page_query()).unwrap();
        reloaded
            .apply_bundle(
                &worker
                    .export_query(done_page.page_hydration_query())
                    .unwrap(),
            )
            .unwrap();
        let done_ids = reloaded
            .subscription_delta(&mut done_subscription)
            .unwrap()
            .all
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();

        assert_eq!(done_ids, page_five_ids);
    }

    #[test]
    fn todo_query_state_builds_search_done_and_page_query() {
        let state = TodoQueryState {
            title_search: "  needle  ".to_owned(),
            done_filter: TodoDoneFilter::Open,
            page: 3,
            ..TodoQueryState::default()
        };

        let query = state.page_query();

        assert_eq!(query.table, "todos");
        assert_eq!(query.conditions.len(), 2);
        assert_eq!(query.conditions[0].column, "title");
        assert_eq!(query.conditions[0].op, QueryConditionOp::Contains);
        assert_eq!(query.conditions[0].value, json!("needle"));
        assert_eq!(query.conditions[1].column, "done");
        assert_eq!(query.conditions[1].op, QueryConditionOp::Eq);
        assert_eq!(query.conditions[1].value, json!(false));
        assert_eq!(query.order_by[0].column, "$createdAt");
        assert_eq!(query.order_by[0].direction, QueryDirection::Desc);
        assert_eq!(query.limit, Some(TODO_PAGE_SIZE));
        assert_eq!(query.offset, Some(TODO_PAGE_SIZE * 3));
    }

    #[test]
    fn todo_query_state_can_filter_done_items() {
        let query = TodoQueryState {
            done_filter: TodoDoneFilter::Done,
            ..TodoQueryState::default()
        }
        .page_query();

        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "done");
        assert_eq!(query.conditions[0].op, QueryConditionOp::Eq);
        assert_eq!(query.conditions[0].value, json!(true));
    }

    #[test]
    fn todo_query_state_can_sort_by_title_ascending() {
        let query = TodoQueryState {
            sort_field: TodoSortField::Title,
            sort_direction: TodoSortDirection::Asc,
            ..TodoQueryState::default()
        }
        .page_query();

        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].column, "title");
        assert_eq!(query.order_by[0].direction, QueryDirection::Asc);
    }

    #[test]
    fn todo_query_state_defaults_to_date_descending() {
        let query = TodoQueryState::default().page_query();

        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].column, "$createdAt");
        assert_eq!(query.order_by[0].direction, QueryDirection::Desc);
    }

    #[test]
    fn todo_query_state_builds_next_page_probe_query() {
        let query = TodoQueryState {
            title_search: "needle".to_owned(),
            page: 2,
            ..TodoQueryState::default()
        }
        .next_page_probe_query();

        assert_eq!(query.table, "todos");
        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "title");
        assert_eq!(query.limit, Some(1));
        assert_eq!(query.offset, Some(TODO_PAGE_SIZE * 3));
        assert_eq!(query.order_by[0].column, "$createdAt");
    }

    #[test]
    fn todo_query_state_builds_page_hydration_query_with_probe_row() {
        let query = TodoQueryState {
            title_search: "needle".to_owned(),
            page: 2,
            ..TodoQueryState::default()
        }
        .page_hydration_query();

        assert_eq!(query.table, "todos");
        assert_eq!(query.conditions.len(), 1);
        assert_eq!(query.conditions[0].column, "title");
        assert_eq!(query.limit, Some(TODO_PAGE_SIZE + 1));
        assert_eq!(query.offset, Some(TODO_PAGE_SIZE * 2));
        assert_eq!(query.order_by[0].column, "$createdAt");
    }
}
