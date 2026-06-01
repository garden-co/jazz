use js_sys::{Date, Function, Promise};
use mini_jazz_sqlite::{BuiltQuery, RowView, SubscriptionDelta};
use mini_sqlite_todo_yew::browser_runtime::{
    BrowserRuntime, BrowserRuntimeConfig, BrowserRuntimeStatus, SubscriptionId,
};
use mini_sqlite_todo_yew::query_builder::QueryBuilder;
use mini_sqlite_todo_yew::todo_query::{
    TodoDoneFilter, TodoQueryState, TodoSortDirection, TodoSortField,
};
use mini_sqlite_todo_yew::todo_schema::todo_schema;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::{closure::Closure, prelude::*, JsCast};
use wasm_bindgen_futures::{spawn_local, JsFuture};
use yew::Callback;

const PROJECT_ID: &str = "todo-list";
const SYNC_BATCH_SIZE: u64 = 100;
const TOTAL_TO_GENERATE: u64 = 100_000;

#[derive(Clone, Debug, PartialEq)]
pub struct Todo {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TodoState {
    pub ready: bool,
    pub generating: bool,
    pub syncing: bool,
    pub query: TodoQueryState,
    pub todos: Vec<Todo>,
    pub has_next_page: bool,
    pub error: String,
    pub generated: u64,
    pub total_to_generate: u64,
    pub current_rows: u64,
}

impl Default for TodoState {
    fn default() -> Self {
        Self {
            ready: false,
            generating: false,
            syncing: false,
            query: TodoQueryState::default(),
            todos: Vec::new(),
            has_next_page: false,
            error: String::new(),
            generated: 0,
            total_to_generate: TOTAL_TO_GENERATE,
            current_rows: 0,
        }
    }
}

impl TodoState {
    pub fn with_error(error: String) -> Self {
        Self {
            error,
            ..Self::default()
        }
    }
}

#[derive(Clone)]
pub struct TodoRuntime {
    inner: Rc<RefCell<Inner>>,
}

struct Inner {
    browser: BrowserRuntime,
    state: TodoState,
    set_state: Callback<TodoState>,
    project_ensured: bool,
    page_subscription: Option<SubscriptionId>,
    next_page_subscription: Option<SubscriptionId>,
}

impl TodoRuntime {
    pub fn open(set_state: Callback<TodoState>) -> Result<Self, String> {
        let runtime_slot = Rc::new(RefCell::new(None::<TodoRuntime>));
        let browser = BrowserRuntime::open(
            BrowserRuntimeConfig {
                db_name: "mini-jazz-sqlite-yew-serde-worker.sqlite3".to_owned(),
                main_node_id: "browser-yew-main".to_owned(),
                worker_node_id: "browser-yew-worker".to_owned(),
                user: "alice".to_owned(),
                schema: todo_schema(),
                hydrate_queries: vec![
                    project_query(),
                    TodoQueryState::default().page_hydration_query(),
                ],
                native_sync_url: native_sync_url(),
            },
            Callback::from({
                let runtime_slot = runtime_slot.clone();
                move |status| {
                    if let Some(runtime) = runtime_slot.borrow().as_ref() {
                        runtime.handle_status(status);
                    }
                }
            }),
        )?;

        let runtime = Self {
            inner: Rc::new(RefCell::new(Inner {
                browser,
                state: TodoState::default(),
                set_state,
                project_ensured: false,
                page_subscription: None,
                next_page_subscription: None,
            })),
        };
        *runtime_slot.borrow_mut() = Some(runtime.clone());

        let (page_subscription_id, next_page_subscription_id) =
            runtime.subscribe_current_queries()?;
        runtime.with_inner(|inner| {
            inner.page_subscription = Some(page_subscription_id);
            inner.next_page_subscription = Some(next_page_subscription_id);
            Ok(())
        })?;

        Ok(runtime)
    }

    pub fn add(&self, title: String) {
        let (browser, current_rows) = self.browser_and_current_rows();
        let id = format!("todo-{}-{}", Date::now() as u64, current_rows);
        if let Err(error) = (|| {
            browser.insert_row(
                "todos",
                &id,
                row_values([
                    ("title", json!(title)),
                    ("done", json!(false)),
                    ("project", json!(PROJECT_ID)),
                ]),
            )?;
            browser.refresh_subscriptions()?;
            browser.sync_queries(vec![todo_ids_query(vec![id])])?;
            Ok(())
        })() {
            self.set_error(error);
        }
    }

    pub fn toggle(&self, id: String, done: bool) {
        let browser = self.browser();
        if let Err(error) = (|| {
            browser.update_row("todos", &id, row_values([("done", json!(done))]))?;
            browser.refresh_subscriptions()?;
            browser.sync_queries(vec![todo_ids_query(vec![id])])?;
            Ok(())
        })() {
            self.set_error(error);
        }
    }

    pub fn delete(&self, id: String) {
        let browser = self.browser();
        if let Err(error) = (|| {
            browser.delete_row("todos", &id)?;
            browser.refresh_subscriptions()?;
            browser.sync_queries(vec![todo_ids_query(vec![id])])?;
            Ok(())
        })() {
            self.set_error(error);
        }
    }

    pub fn generate_100k(&self) {
        if self
            .with_inner(|inner| Ok(inner.controls_locked()))
            .unwrap_or(true)
        {
            return;
        }
        self.run_mut(|inner| {
            inner.state.generating = true;
            inner.state.generated = 0;
            inner.emit();
            Ok(())
        });

        let runtime = self.clone();
        spawn_local(async move {
            if let Err(error) = runtime.generate_100k_inner().await {
                runtime.set_error(error);
            }
        });
    }

    pub fn set_title_search(&self, title_search: String) {
        self.update_query(|query| {
            query.title_search = title_search;
            query.page = 0;
        });
    }

    pub fn set_done_filter(&self, done_filter: TodoDoneFilter) {
        self.update_query(|query| {
            query.done_filter = done_filter;
            query.page = 0;
        });
    }

    pub fn set_sort_field(&self, sort_field: TodoSortField) {
        self.update_query(|query| {
            query.sort_field = sort_field;
            query.page = 0;
        });
    }

    pub fn set_sort_direction(&self, sort_direction: TodoSortDirection) {
        self.update_query(|query| {
            query.sort_direction = sort_direction;
            query.page = 0;
        });
    }

    pub fn previous_page(&self) {
        self.update_query(|query| {
            query.page = query.page.saturating_sub(1);
        });
    }

    pub fn next_page(&self) {
        if !self
            .with_inner(|inner| Ok(inner.state.has_next_page))
            .unwrap_or(false)
        {
            return;
        }
        self.update_query(|query| {
            query.page = query.page.saturating_add(1);
        });
    }

    async fn generate_100k_inner(&self) -> Result<(), String> {
        let id_seed = Date::now() as u64;
        let mut batch_ids = Vec::new();
        let browser = self.browser();
        for index in 0..TOTAL_TO_GENERATE {
            let id = format!("todo-{id_seed}-{index}");
            browser.insert_row(
                "todos",
                &id,
                row_values([
                    ("title", json!(format!("Todo {}", index + 1))),
                    ("done", json!(false)),
                    ("project", json!(PROJECT_ID)),
                ]),
            )?;
            batch_ids.push(id);

            if (index + 1) % SYNC_BATCH_SIZE == 0 {
                let query = todo_ids_query(std::mem::take(&mut batch_ids));
                browser.sync_queries(vec![query])?;
            }

            if (index + 1) % 1000 == 0 {
                self.with_inner(|inner| {
                    inner.state.generated = index + 1;
                    inner.emit();
                    Ok(())
                })?;
                let _ = JsFuture::from(next_tick()).await;
            }
        }

        if !batch_ids.is_empty() {
            browser.sync_queries(vec![todo_ids_query(batch_ids)])?;
        }
        browser.refresh_subscriptions()?;

        self.with_inner(|inner| {
            inner.state.generating = false;
            inner.emit();
            Ok(())
        })
    }

    fn handle_status(&self, status: BrowserRuntimeStatus) {
        let should_ensure_project = self
            .with_inner(|inner| {
                inner.state.ready = status.ready;
                inner.state.syncing = status.syncing;
                inner.state.error = status.error;
                inner.state.current_rows = status.worker_storage_stats.current_rows.max(0) as u64;
                let should_ensure_project = status.ready && !inner.project_ensured;
                if should_ensure_project {
                    inner.project_ensured = true;
                }
                inner.emit();
                Ok(should_ensure_project)
            })
            .unwrap_or(false);

        if should_ensure_project {
            self.ensure_project();
        }
    }

    fn handle_todo_delta(&self, delta: SubscriptionDelta) {
        self.run_mut(|inner| {
            inner.state.todos = todos_from_rows(delta.all);
            inner.state.has_next_page = inner.has_next_page()?;
            inner.emit();
            Ok(())
        });
    }

    fn handle_next_page_delta(&self, delta: SubscriptionDelta) {
        self.run_mut(|inner| {
            inner.state.has_next_page = !delta.all.is_empty();
            inner.emit();
            Ok(())
        });
    }

    fn ensure_project(&self) {
        let browser = self.browser();
        if let Err(error) = (|| {
            let exists = browser
                .query(project_query())?
                .iter()
                .any(|row| row.id == PROJECT_ID);
            if !exists {
                browser.insert_row(
                    "projects",
                    PROJECT_ID,
                    row_values([("title", json!("Todo list"))]),
                )?;
                browser.sync_queries_after_render(vec![project_query()]);
            }
            Ok(())
        })() {
            self.set_error(error);
        }
    }

    fn run_mut(&self, f: impl FnOnce(&mut Inner) -> Result<(), String>) {
        if let Err(error) = self.with_inner(f) {
            self.set_error(error);
        }
    }

    fn with_inner<T>(&self, f: impl FnOnce(&mut Inner) -> Result<T, String>) -> Result<T, String> {
        f(&mut self.inner.borrow_mut())
    }

    fn browser(&self) -> BrowserRuntime {
        self.inner.borrow().browser.clone()
    }

    fn browser_and_current_rows(&self) -> (BrowserRuntime, u64) {
        let inner = self.inner.borrow();
        (inner.browser.clone(), inner.state.current_rows)
    }

    fn set_error(&self, error: String) {
        let mut inner = self.inner.borrow_mut();
        inner.state.generating = false;
        inner.state.syncing = false;
        inner.state.error = error;
        inner.emit();
    }

    fn update_query(&self, update: impl FnOnce(&mut TodoQueryState)) {
        if let Err(error) = self.replace_page_subscription(update) {
            self.set_error(error);
        }
    }

    fn replace_page_subscription(
        &self,
        update: impl FnOnce(&mut TodoQueryState),
    ) -> Result<(), String> {
        let (
            browser,
            old_page_subscription,
            old_next_page_subscription,
            query,
            next_query,
            hydrate_query,
        ) = self.with_inner(|inner| {
            update(&mut inner.state.query);
            inner.state.has_next_page = false;
            let old_page_subscription = inner.page_subscription.take();
            let old_next_page_subscription = inner.next_page_subscription.take();
            let query = inner.state.query.page_query();
            let next_query = inner.state.query.next_page_probe_query();
            let hydrate_query = inner.state.query.page_hydration_query();
            inner.emit();
            Ok((
                inner.browser.clone(),
                old_page_subscription,
                old_next_page_subscription,
                query,
                next_query,
                hydrate_query,
            ))
        })?;
        if let Some(id) = old_page_subscription {
            browser.unsubscribe(id);
        }
        if let Some(id) = old_next_page_subscription {
            browser.unsubscribe(id);
        }
        let subscription_id = self.subscribe_page(query.clone())?;
        let next_subscription_id = self.subscribe_next_page(next_query.clone())?;
        self.with_inner(|inner| {
            inner.page_subscription = Some(subscription_id);
            inner.next_page_subscription = Some(next_subscription_id);
            Ok(())
        })?;
        browser.hydrate_query_after_render(hydrate_query);
        Ok(())
    }

    fn subscribe_current_queries(&self) -> Result<(SubscriptionId, SubscriptionId), String> {
        let query = self.with_inner(|inner| Ok(inner.state.query.page_query()))?;
        let next_query = self.with_inner(|inner| Ok(inner.state.query.next_page_probe_query()))?;
        Ok((
            self.subscribe_page(query)?,
            self.subscribe_next_page(next_query)?,
        ))
    }

    fn subscribe_page(&self, query: BuiltQuery) -> Result<SubscriptionId, String> {
        let runtime = self.clone();
        self.browser().subscribe(
            query,
            Callback::from(move |delta| {
                runtime.handle_todo_delta(delta);
            }),
        )
    }

    fn subscribe_next_page(&self, query: BuiltQuery) -> Result<SubscriptionId, String> {
        let runtime = self.clone();
        self.browser().subscribe(
            query,
            Callback::from(move |delta| {
                runtime.handle_next_page_delta(delta);
            }),
        )
    }
}

impl Inner {
    fn emit(&self) {
        self.set_state.emit(self.state.clone());
    }

    fn controls_locked(&self) -> bool {
        !self.state.ready || self.state.generating
    }

    fn has_next_page(&self) -> Result<bool, String> {
        Ok(!self
            .browser
            .query(self.state.query.next_page_probe_query())?
            .is_empty())
    }
}

fn native_sync_url() -> Option<String> {
    Some("ws://127.0.0.1:8787/sync".to_owned())
}

fn project_query() -> BuiltQuery {
    QueryBuilder::table("projects").build()
}

fn todo_ids_query(ids: Vec<String>) -> BuiltQuery {
    QueryBuilder::table("todos")
        .in_values("id", json!(ids))
        .build()
}

fn row_values<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn todos_from_rows(rows: Vec<RowView>) -> Vec<Todo> {
    rows.into_iter()
        .map(|row| Todo {
            id: row.id,
            created_at: row.created_at,
            title: row
                .values
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            done: row
                .values
                .get("done")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
        .collect()
}

fn next_tick() -> Promise {
    Promise::new(&mut |resolve: Function, _reject: Function| {
        let timeout_resolve = resolve.clone();
        let callback = Closure::once(move || {
            let _ = timeout_resolve.call0(&JsValue::UNDEFINED);
        });
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                0,
            );
        } else {
            let _ = resolve.call0(&JsValue::UNDEFINED);
        }
        callback.forget();
    })
}
