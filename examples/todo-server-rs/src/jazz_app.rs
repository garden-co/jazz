// jazz_app.rs: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:699e59c15aba1594d609960183f0700bbce87edd058e057e53e212c3f8a6f53c";

/// A row of `projects`.
#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub id: ::jazz_app::ObjectId,
    pub name: String,
}

/// A new row of `projects`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewProject {
    pub name: String,
}

/// Changes to a row of `projects`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProjectUpdate {
    pub name: Option<String>,
}

impl ::jazz_app::Table for Project {
    const NAME: &'static str = "projects";
    type New = NewProject;
    type Update = ProjectUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let name = row.take::<String>("name")?;
        Ok(Self {
            id: row.id(),
            name,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewProject {
    type Row = Project;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        Ok(cells)
    }
}

impl ::jazz_app::Update for ProjectUpdate {
    type Row = Project;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<Vec<(String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = Vec::new();
        if let Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        Ok(cells)
    }
}

/// Queries, columns and relations of `projects`.
pub mod projects {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `projects`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Project> {
        ::jazz_app::Query::all()
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Project, String> {
        ::jazz_app::Column::new("name")
    }

    /// Relation `todosViaProject`: the `todos` rows whose `project` references this row.
    pub fn todos_via_project() -> ::jazz_app::Many<Project, Todo> {
        ::jazz_app::Many::new("project")
    }
}

/// A row of `todos`.
#[derive(Debug, Clone, PartialEq)]
pub struct Todo {
    pub id: ::jazz_app::ObjectId,
    pub title: String,
    pub done: bool,
    pub description: Option<String>,
    pub parent: Option<::jazz_app::ObjectId>,
    pub project: Option<::jazz_app::ObjectId>,
}

/// A new row of `todos`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewTodo {
    pub title: String,
    pub done: bool,
    pub description: Option<String>,
    pub parent: Option<::jazz_app::ObjectId>,
    pub project: Option<::jazz_app::ObjectId>,
}

/// Changes to a row of `todos`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TodoUpdate {
    pub title: Option<String>,
    pub done: Option<bool>,
    pub description: Option<Option<String>>,
    pub parent: Option<Option<::jazz_app::ObjectId>>,
    pub project: Option<Option<::jazz_app::ObjectId>>,
}

impl ::jazz_app::Table for Todo {
    const NAME: &'static str = "todos";
    type New = NewTodo;
    type Update = TodoUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let title = row.take::<String>("title")?;
        let done = row.take::<bool>("done")?;
        let description = row.take::<Option<String>>("description")?;
        let parent = row.take::<Option<::jazz_app::ObjectId>>("parent")?;
        let project = row.take::<Option<::jazz_app::ObjectId>>("project")?;
        Ok(Self {
            id: row.id(),
            title,
            done,
            description,
            parent,
            project,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewTodo {
    type Row = Todo;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.done;
        cells.insert("done".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.description;
        cells.insert("description".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.parent;
        cells.insert("parent".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.project;
        cells.insert("project".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        Ok(cells)
    }
}

impl ::jazz_app::Update for TodoUpdate {
    type Row = Todo;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<Vec<(String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = Vec::new();
        if let Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.done {
            cells.push(("done".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.description {
            cells.push(("description".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.parent {
            cells.push(("parent".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.project {
            cells.push(("project".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        Ok(cells)
    }
}

/// Queries, columns and relations of `todos`.
pub mod todos {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `todos`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Todo> {
        ::jazz_app::Query::all()
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Todo, String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `done`.
    pub fn done() -> ::jazz_app::Column<Todo, bool> {
        ::jazz_app::Column::new("done")
    }

    /// Column `description`.
    pub fn description() -> ::jazz_app::Column<Todo, Option<String>> {
        ::jazz_app::Column::new("description")
    }

    /// Column `parent`.
    pub fn parent() -> ::jazz_app::Column<Todo, Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("parent")
    }

    /// Column `project`.
    pub fn project() -> ::jazz_app::Column<Todo, Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("project")
    }

    /// Relation `parentRelation`: the `todos` row `parent` references.
    pub fn parent_relation() -> ::jazz_app::One<Todo, Todo> {
        ::jazz_app::One::new("parent")
    }

    /// Relation `projectRelation`: the `projects` row `project` references.
    pub fn project_relation() -> ::jazz_app::One<Todo, Project> {
        ::jazz_app::One::new("project")
    }

    /// Relation `todosViaParent`: the `todos` rows whose `parent` references this row.
    pub fn todos_via_parent() -> ::jazz_app::Many<Todo, Todo> {
        ::jazz_app::Many::new("parent")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "projects": {
      "columns": { "name": "string" },
      "relations": { "todosViaProject": { "reverse": "todos", "relation": "projectRelation" } }
    },
    "todos": {
      "columns": {
        "title": "string",
        "done": "boolean",
        "description": "string?",
        "parent": "uuid?",
        "project": "uuid?"
      },
      "relations": {
        "parentRelation": { "rel": "todos", "column": "parent" },
        "projectRelation": { "rel": "projects", "column": "project" },
        "todosViaParent": { "reverse": "todos", "relation": "parentRelation" }
      }
    }
  }
}
"#;

/// Opens the app. A backend (`Config::backend`, with its
/// `jazz.permissions.json`) first publishes the app's permissions bundle
/// and pins the session it acts in to it.
pub async fn open(config: ::jazz_app::Config) -> ::jazz_app::Result<::jazz_app::Jazz> {
    ::jazz_app::Jazz::open(::jazz_app::AppFiles::new(APP, None), config).await
}
