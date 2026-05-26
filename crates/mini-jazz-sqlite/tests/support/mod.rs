use mini_jazz_sqlite::sync::{Bundle, QueryReadRecord};
use mini_jazz_sqlite::{Result, RowView, Runtime, SchemaDef, Storage};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;

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

pub struct Harness {
    dir: TempDir,
}

impl Harness {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().expect("create test harness directory"),
        }
    }

    pub fn path(&self, file_name: &str) -> PathBuf {
        self.dir.path().join(file_name)
    }

    pub fn memory(&self, node_id: &str, principal: &str) -> Result<Runtime> {
        Runtime::open(Storage::Memory, node_id, principal)
    }

    pub fn durable(&self, file_name: &str, node_id: &str, principal: &str) -> Result<Runtime> {
        Runtime::open(Storage::File(self.path(file_name)), node_id, principal)
    }

    pub fn memory_with_schema(
        &self,
        node_id: &str,
        principal: &str,
        schema: SchemaDef,
    ) -> Result<Runtime> {
        Runtime::open_with_schema(Storage::Memory, node_id, principal, schema)
    }

    pub fn durable_with_schema(
        &self,
        file_name: &str,
        node_id: &str,
        principal: &str,
        schema: SchemaDef,
    ) -> Result<Runtime> {
        Runtime::open_with_schema(
            Storage::File(self.path(file_name)),
            node_id,
            principal,
            schema,
        )
    }
}

pub fn apply(source_bundle: Bundle, target: &mut Runtime) -> Result<()> {
    target.apply_bundle(&source_bundle)
}

pub fn refresh_observed_queries(source: &Runtime, target: &mut Runtime) -> Result<()> {
    for refresh in source.export_query_read_refreshes(&target.observed_query_reads()?)? {
        target.apply_bundle(&refresh)?;
    }
    Ok(())
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
        let todos = self.read_rows_where_eq_top_created_at_desc(
            "todos",
            "done",
            JsonValue::Bool(false),
            limit,
        )?;
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
        extend_with_project_scope(self, &mut bundle, &todos)?;
        Ok(bundle)
    }

    fn export_query_scope_newest_open_todos(&self, limit: usize) -> Result<Bundle> {
        let todos = self.newest_open_todos(limit)?;
        let mut bundle = self.export_query_where_eq_top_created_at_desc_with_ref_include(
            "todos",
            "done",
            JsonValue::Bool(false),
            limit,
            "project",
        )?;
        extend_with_project_scope(self, &mut bundle, &todos)?;
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

fn extend_with_project_scope(
    _runtime: &Runtime,
    bundle: &mut Bundle,
    todos: &[TodoView],
) -> Result<()> {
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

pub fn tasks_schema() -> SchemaDef {
    SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    })
}

pub fn notes_schema() -> SchemaDef {
    SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    })
}

pub fn folders_schema() -> SchemaDef {
    SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
        table.read_if_created_by_principal();
    })
}
