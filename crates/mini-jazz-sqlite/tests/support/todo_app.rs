use mini_jazz_sqlite::sync::{Bundle, QueryReadRecord};
use mini_jazz_sqlite::{Result, RowView, Runtime};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use super::top_created_query;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TodoView {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub project_id: String,
    pub project_title: Option<String>,
    pub created_by: String,
    pub tx_id: String,
}

pub trait FixtureRuntimeExt {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String>;
    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String>;
    fn delete_todo(&mut self, id: &str) -> Result<String>;
    fn open_todos(&self) -> Result<Vec<TodoView>>;
    fn open_todos_require_project(&self) -> Result<Vec<TodoView>>;
    fn newest_open_todos(&self, limit: usize) -> Result<Vec<TodoView>>;
    fn export_query_scope_open_todos(&self) -> Result<Bundle>;
    fn export_query_scope_newest_open_todos(&self, limit: usize) -> Result<Bundle>;
}

impl FixtureRuntimeExt for Runtime {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String> {
        self.insert_row(
            "projects",
            id,
            BTreeMap::from([("title".to_owned(), json!(title))]),
        )
    }

    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String> {
        self.insert_row(
            "todos",
            id,
            BTreeMap::from([
                ("title".to_owned(), json!(title)),
                ("done".to_owned(), json!(done)),
                ("project".to_owned(), json!(project_id)),
            ]),
        )
    }

    fn delete_todo(&mut self, id: &str) -> Result<String> {
        self.delete_row("todos", id)
    }

    fn open_todos(&self) -> Result<Vec<TodoView>> {
        open_todo_rows(self.read_rows("todos")?, self.read_rows("projects")?)
    }

    fn open_todos_require_project(&self) -> Result<Vec<TodoView>> {
        Ok(self
            .open_todos()?
            .into_iter()
            .filter(|todo| todo.project_title.is_some())
            .collect())
    }

    fn newest_open_todos(&self, limit: usize) -> Result<Vec<TodoView>> {
        let todos = self.query(top_created_query(
            "todos",
            "done",
            JsonValue::Bool(false),
            limit,
        ))?;
        open_todo_rows_with_sort(todos, self.read_rows("projects")?, false)
    }

    fn export_query_scope_open_todos(&self) -> Result<Bundle> {
        let todos = self.open_todos()?;
        let mut bundle = self.export_query_where_eq_with_ref_include(
            "todos",
            "done",
            JsonValue::Bool(false),
            "project",
        )?;
        extend_with_project_scope(&mut bundle, &todos)?;
        Ok(bundle)
    }

    fn export_query_scope_newest_open_todos(&self, limit: usize) -> Result<Bundle> {
        let todos = self.newest_open_todos(limit)?;
        let mut bundle = self.export_query_with_ref_includes(
            top_created_query("todos", "done", JsonValue::Bool(false), limit),
            &["project"],
        )?;
        extend_with_project_scope(&mut bundle, &todos)?;
        Ok(bundle)
    }
}

fn open_todo_rows(todos: Vec<RowView>, projects: Vec<RowView>) -> Result<Vec<TodoView>> {
    open_todo_rows_with_sort(todos, projects, true)
}

fn open_todo_rows_with_sort(
    todos: Vec<RowView>,
    projects: Vec<RowView>,
    sort_by_id: bool,
) -> Result<Vec<TodoView>> {
    let projects = projects
        .into_iter()
        .map(|row| {
            (
                row.id,
                row.values
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut todos = todos
        .into_iter()
        .filter(|row| row.values.get("done") == Some(&JsonValue::Bool(false)))
        .map(|row| {
            let title = row
                .values
                .get("title")
                .and_then(JsonValue::as_str)
                .expect("todo missing title")
                .to_owned();
            let project_id = row
                .values
                .get("project")
                .and_then(JsonValue::as_str)
                .expect("todo missing project")
                .to_owned();
            let project_title = projects.get(&project_id).cloned().flatten();
            Ok(TodoView {
                id: row.id,
                title,
                done: false,
                project_id,
                project_title,
                created_by: row.created_by,
                tx_id: row.tx_id,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if sort_by_id {
        todos.sort_by(|left, right| left.id.cmp(&right.id));
    }
    Ok(todos)
}

fn extend_with_project_scope(bundle: &mut Bundle, todos: &[TodoView]) -> Result<()> {
    let Some(branch_id) = bundle
        .query_reads
        .first()
        .map(|read| read.branch_id.clone())
    else {
        return Ok(());
    };
    let mut added_absence = false;
    for todo in todos.iter().filter(|todo| todo.project_title.is_none()) {
        bundle.query_reads.push(QueryReadRecord {
            branch_id: branch_id.clone(),
            table: "projects".to_owned(),
            field: "id".to_owned(),
            op: "absent".to_owned(),
            value: JsonValue::String(todo.project_id.clone()),
        });
        added_absence = true;
    }
    if added_absence {
        bundle.policy_fingerprint = "legacy".to_owned();
    }
    Ok(())
}
